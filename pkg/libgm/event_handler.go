package libgm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

const (
	invalidProviderIDLogWindow = time.Minute
	invalidProviderIDLogBurst  = 4
)

type IncomingRPCMessage struct {
	*gmproto.IncomingRPCMessage

	IsOld bool

	Pair *gmproto.RPCPairData
	Gaia *gmproto.RPCGaiaData

	Message          *gmproto.RPCMessageData
	DecryptedData    []byte
	DecryptedMessage proto.Message
	// PayloadSource identifies the strictly validated provider payload channel.
	// Correlated replies require authenticated primary ciphertext. The sole
	// unauthenticated exception is the exact historic GET_UPDATES logout control,
	// which is an event only and never satisfies a response waiter.
	PayloadSource    PayloadSource
	SecondaryMessage *gmproto.EncryptedData2Container
	DurableOutcome   DurableOutcome
	DurableError     error
}

// PayloadSource identifies the validated protobuf field that produced a decoded
// payload. The distinction is security-sensitive: field 11 (EncryptedData2)
// carries account-change events, not correlated RPC replies; LogoutControl is
// the exact legacy unauthenticated logout marker and is likewise never a reply.
type PayloadSource uint8

const (
	PayloadSourceNone PayloadSource = iota
	PayloadSourceEncryptedData
	PayloadSourceEncryptedData2
	PayloadSourceLogoutControl
	PayloadSourceUnencryptedData
)

// DurableEnvelope is delivered synchronously on the connection polling
// goroutine. The handler must durably commit raw bytes, projection, media jobs,
// outbox events, and ACK-pending state before returning nil.
type DurableEnvelope struct {
	ResponseID  string
	Raw         []byte
	Decoded     *IncomingRPCMessage
	DecodeError error
	Request     DurableRequest
}

// DurableRequest is trusted local metadata captured when an RPC waiter is
// registered. It routes response-owned durable state even when a valid page is
// empty; provider response contents are never used to guess the target.
type DurableRequest struct {
	Action         gmproto.ActionType
	ConversationID string
	// Cursor is the deterministic cursor sent in the request. It is trusted
	// local metadata and lets the durable boundary reject a provider cursor
	// that does not advance before acknowledging the response.
	Cursor []byte
}

type DurableOutcome uint8

const (
	DurableOutcomeUnknown DurableOutcome = iota
	DurableOutcomeCommitted
	DurableOutcomePoisoned
	// DurableOutcomeDuplicatePoisoned is an exact redelivery of an already
	// committed poison. It remains ACK-eligible but must not emit another
	// connection quarantine signal.
	DurableOutcomeDuplicatePoisoned
)

var (
	ErrDurablePersistence = errors.New("durable envelope persistence failed")
	ErrDurablePoisoned    = errors.New("provider response durably quarantined")
)

type durablePersistenceError struct{ cause error }

func (failure *durablePersistenceError) Error() string { return ErrDurablePersistence.Error() }
func (failure *durablePersistenceError) Unwrap() error { return failure.cause }
func (failure *durablePersistenceError) Is(target error) bool {
	return target == ErrDurablePersistence
}

type DurableEnvelopeHandler func(context.Context, DurableEnvelope) (DurableOutcome, error)
type ACKObserver func(context.Context, []string) error

// ACKBatchSender performs exactly one bounded provider ACK request. A durable
// gateway coordinator invokes it while holding its database serialization
// locks; non-durable clients continue to send ACKs directly.
type ACKBatchSender func(context.Context, []string) error

// ACKCoordinationResult separates a provider transport failure from durable
// coordination failures. RetryIDs are requeued in original order. IDs omitted
// from both AdmittedIDs and RetryIDs were durably filtered as no longer ACK
// eligible and must not reach provider I/O.
type ACKCoordinationResult struct {
	AdmittedIDs   []string
	RetryIDs      []string
	ProviderError error
}

// ACKCoordinator revalidates a candidate batch at the durable boundary and
// serializes the bounded provider request with conflicting envelope commits.
type ACKCoordinator func(context.Context, []string, ACKBatchSender) (ACKCoordinationResult, error)

// DurableFailureObserver receives secret-free typed persistence failures. It
// must be nonblocking; production runtimes feed it into a bounded channel.
type DurableFailureObserver func(error)

// QueueDurableACKs refills the actor-owned ACK queue from durable pending
// state after restart. The caller must have already verified its lease fence.
func (c *Client) QueueDurableACKs(ids []string) error {
	if len(ids) > 256 {
		return errors.New("too many durable ACKs")
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !domain.ValidProviderResponseID(id) {
			return errors.New("invalid durable ACK")
		}
		if _, exists := seen[id]; exists {
			return errors.New("duplicate durable ACK")
		}
		seen[id] = struct{}{}
	}
	for _, id := range ids {
		c.sessionHandler.queueMessageAck(id)
	}
	return nil
}

func (c *Client) SetDurableEnvelopeHandler(handler DurableEnvelopeHandler) {
	c.durableMu.Lock()
	c.durableEnvelope = handler
	c.durableMu.Unlock()
}

func (c *Client) SetACKObserver(observer ACKObserver) {
	c.durableMu.Lock()
	c.ackObserver = observer
	c.durableMu.Unlock()
}

func (c *Client) SetACKCoordinator(coordinator ACKCoordinator) {
	c.durableMu.Lock()
	c.ackCoordinator = coordinator
	c.durableMu.Unlock()
}

func (c *Client) SetDurableFailureObserver(observer DurableFailureObserver) {
	c.durableMu.Lock()
	c.durableFailure = observer
	c.durableMu.Unlock()
}

func (c *Client) durableHandlers() (DurableEnvelopeHandler, ACKObserver, DurableFailureObserver) {
	c.durableMu.RLock()
	defer c.durableMu.RUnlock()
	return c.durableEnvelope, c.ackObserver, c.durableFailure
}

func (c *Client) durableACKCoordinator() ACKCoordinator {
	c.durableMu.RLock()
	defer c.durableMu.RUnlock()
	return c.ackCoordinator
}

func (c *Client) shouldLogInvalidProviderResponseID(now time.Time) bool {
	c.invalidIDLogMu.Lock()
	defer c.invalidIDLogMu.Unlock()
	if c.invalidIDLogAt.IsZero() || now.Sub(c.invalidIDLogAt) >= invalidProviderIDLogWindow || now.Before(c.invalidIDLogAt) {
		c.invalidIDLogAt = now
		c.invalidIDLogHits = 0
	}
	if c.invalidIDLogHits >= invalidProviderIDLogBurst {
		return false
	}
	c.invalidIDLogHits++
	return true
}

var responseType = map[gmproto.ActionType]proto.Message{
	gmproto.ActionType_IS_BUGLE_DEFAULT:           &gmproto.IsBugleDefaultResponse{},
	gmproto.ActionType_GET_UPDATES:                &gmproto.UpdateEvents{},
	gmproto.ActionType_LIST_CONVERSATIONS:         &gmproto.ListConversationsResponse{},
	gmproto.ActionType_NOTIFY_DITTO_ACTIVITY:      &gmproto.NotifyDittoActivityResponse{},
	gmproto.ActionType_GET_CONVERSATION_TYPE:      &gmproto.GetConversationTypeResponse{},
	gmproto.ActionType_GET_CONVERSATION:           &gmproto.GetConversationResponse{},
	gmproto.ActionType_LIST_MESSAGES:              &gmproto.ListMessagesResponse{},
	gmproto.ActionType_SEND_MESSAGE:               &gmproto.SendMessageResponse{},
	gmproto.ActionType_SEND_REACTION:              &gmproto.SendReactionResponse{},
	gmproto.ActionType_DELETE_MESSAGE:             &gmproto.DeleteMessageResponse{},
	gmproto.ActionType_GET_PARTICIPANTS_THUMBNAIL: &gmproto.GetThumbnailResponse{},
	gmproto.ActionType_GET_CONTACTS_THUMBNAIL:     &gmproto.GetThumbnailResponse{},
	gmproto.ActionType_LIST_CONTACTS:              &gmproto.ListContactsResponse{},
	gmproto.ActionType_LIST_TOP_CONTACTS:          &gmproto.ListTopContactsResponse{},
	gmproto.ActionType_GET_OR_CREATE_CONVERSATION: &gmproto.GetOrCreateConversationResponse{},
	gmproto.ActionType_UPDATE_CONVERSATION:        &gmproto.UpdateConversationResponse{},
	gmproto.ActionType_GET_FULL_SIZE_IMAGE:        &gmproto.GetFullSizeImageResponse{},
}

type correlatedRPCResponsePolicy struct {
	prototype    proto.Message
	emptyPayload bool
}

// correlatedRPCResponses is intentionally an explicit policy inventory rather
// than an alias of responseType. responseType describes protobuf decoding;
// this table describes which public response-waiting Client methods may use a
// raw-only durable record. Keeping the two lists independent makes a newly
// added waiter fail closed until its durability policy is reviewed.
var correlatedRPCResponses = map[gmproto.ActionType]correlatedRPCResponsePolicy{
	gmproto.ActionType_IS_BUGLE_DEFAULT:           {prototype: &gmproto.IsBugleDefaultResponse{}},
	gmproto.ActionType_NOTIFY_DITTO_ACTIVITY:      {prototype: &gmproto.NotifyDittoActivityResponse{}},
	gmproto.ActionType_GET_CONVERSATION_TYPE:      {prototype: &gmproto.GetConversationTypeResponse{}},
	gmproto.ActionType_GET_CONVERSATION:           {prototype: &gmproto.GetConversationResponse{}},
	gmproto.ActionType_SEND_MESSAGE:               {prototype: &gmproto.SendMessageResponse{}},
	gmproto.ActionType_SEND_REACTION:              {prototype: &gmproto.SendReactionResponse{}},
	gmproto.ActionType_DELETE_MESSAGE:             {prototype: &gmproto.DeleteMessageResponse{}},
	gmproto.ActionType_MESSAGE_READ:               {emptyPayload: true},
	gmproto.ActionType_GET_PARTICIPANTS_THUMBNAIL: {prototype: &gmproto.GetThumbnailResponse{}},
	gmproto.ActionType_GET_CONTACTS_THUMBNAIL:     {prototype: &gmproto.GetThumbnailResponse{}},
	gmproto.ActionType_LIST_CONTACTS:              {prototype: &gmproto.ListContactsResponse{}},
	gmproto.ActionType_LIST_TOP_CONTACTS:          {prototype: &gmproto.ListTopContactsResponse{}},
	gmproto.ActionType_GET_OR_CREATE_CONVERSATION: {prototype: &gmproto.GetOrCreateConversationResponse{}},
	gmproto.ActionType_UPDATE_CONVERSATION:        {prototype: &gmproto.UpdateConversationResponse{}},
	gmproto.ActionType_GET_FULL_SIZE_IMAGE:        {prototype: &gmproto.GetFullSizeImageResponse{}},
}

// IsKnownCorrelatedRPCResponse reports whether the authenticated action and
// decoded protobuf form a known request/response pair whose raw response may
// be durably recorded without an inbox projection. Pagination and update
// responses are intentionally excluded because the gateway must atomically
// project their contents before ACK eligibility.
func IsKnownCorrelatedRPCResponse(action gmproto.ActionType, response proto.Message, source PayloadSource, authenticatedPayload ...[]byte) bool {
	if source != PayloadSourceEncryptedData {
		return false
	}
	if response == nil || action == gmproto.ActionType_UNSPECIFIED || action == gmproto.ActionType_GET_UPDATES ||
		action == gmproto.ActionType_LIST_CONVERSATIONS || action == gmproto.ActionType_LIST_MESSAGES {
		policy, known := correlatedRPCResponses[action]
		if !known || !policy.emptyPayload || response != nil {
			return false
		}
		for _, payload := range authenticatedPayload {
			if len(payload) != 0 {
				return false
			}
		}
		return true
	}
	policy, known := correlatedRPCResponses[action]
	return known && policy.prototype != nil && reflect.TypeOf(policy.prototype) == reflect.TypeOf(response)
}

func (c *Client) decryptInternalMessage(data *gmproto.IncomingRPCMessage) (*IncomingRPCMessage, error) {
	msg := &IncomingRPCMessage{
		IncomingRPCMessage: data,
	}
	switch data.BugleRoute {
	case gmproto.BugleRoute_PairEvent:
		msg.Pair = &gmproto.RPCPairData{}
		err := proto.Unmarshal(data.GetMessageData(), msg.Pair)
		if err != nil {
			logSafeBytes(c.Logger.Trace(), "provider_frame", msg.GetMessageData()).Msg("Errored pair event content")
			return nil, fmt.Errorf("failed to decode pair event: %w", err)
		}
	case gmproto.BugleRoute_GaiaEvent:
		msg.Gaia = &gmproto.RPCGaiaData{}
		err := proto.Unmarshal(data.GetMessageData(), msg.Gaia)
		if err != nil {
			logSafeBytes(c.Logger.Trace(), "provider_frame", msg.GetMessageData()).Msg("Errored gaia event content")
			return nil, fmt.Errorf("failed to decode gaia event: %w", err)
		}
	case gmproto.BugleRoute_DataEvent:
		auth := c.AuthData.Snapshot()
		defer auth.ClearSecrets()
		if auth.RequestCrypto == nil {
			return nil, fmt.Errorf("missing session encryption keys")
		}
		msg.Message = &gmproto.RPCMessageData{}
		err := proto.Unmarshal(data.GetMessageData(), msg.Message)
		if err != nil {
			logSafeBytes(c.Logger.Trace(), "provider_frame", msg.GetMessageData()).Msg("Errored data event content")
			return nil, fmt.Errorf("failed to decode data event: %w", err)
		}
		if (msg.Message.EncryptedData != nil && msg.Message.EncryptedData2 != nil) ||
			(msg.Message.UnencryptedData != nil && (msg.Message.EncryptedData != nil || msg.Message.EncryptedData2 != nil)) {
			return msg, errors.New("data event has ambiguous ciphertext fields")
		}
		if msg.Message.EncryptedData != nil {
			msg.DecryptedData, err = auth.RequestCrypto.Decrypt(msg.Message.EncryptedData)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt data event: %w", err)
			}
			msg.PayloadSource = PayloadSourceEncryptedData
			responseStruct, ok := responseType[msg.Message.GetAction()]
			if ok {
				msg.DecryptedMessage = responseStruct.ProtoReflect().New().Interface()
			}
			if msg.DecryptedMessage != nil {
				err = proto.Unmarshal(msg.DecryptedData, msg.DecryptedMessage)
				if err != nil {
					logSafeBytes(c.Logger.Trace(), "decrypted_payload", msg.DecryptedData).Msg("Errored decrypted data event content")
					// Decryption authenticated this response. Preserve its bounded
					// request metadata so an exact registered waiter can receive a
					// subsequent durable persistence failure without waiting 60s.
					return msg, fmt.Errorf("failed to decode decrypted data event: %w", err)
				}
			}
		} else if msg.Message.EncryptedData2 != nil {
			msg.DecryptedData, err = auth.RequestCrypto.Decrypt(msg.Message.EncryptedData2)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt field 2 in data event: %w", err)
			}
			msg.PayloadSource = PayloadSourceEncryptedData2
			msg.SecondaryMessage = &gmproto.EncryptedData2Container{}
			err = proto.Unmarshal(msg.DecryptedData, msg.SecondaryMessage)
			if err != nil {
				logSafeBytes(c.Logger.Trace(), "decrypted_payload", msg.DecryptedData).Msg("Errored decrypted data event content")
				// Decryption authenticated this response. Preserve only its bounded
				// request metadata so the durable path can complete the exact waiter.
				return msg, fmt.Errorf("failed to decode decrypted field 2 data event: %w", err)
			}
		} else if IsLegacyLogoutControl(msg.Message) {
			msg.PayloadSource = PayloadSourceLogoutControl
		} else if isGaiaPairingResponse(msg.Message) {
			// The GAIA pairing handshake runs before session encryption keys exist, so its
			// CLIENT_INIT/CLIENT_FINISHED responses are legitimately unencrypted. This is
			// scoped to those two actions only: every other unauthenticated plaintext data
			// event still falls through to the rejection below.
			msg.PayloadSource = PayloadSourceUnencryptedData
		} else {
			return msg, errors.New("data event has no authenticated ciphertext")
		}
	default:
		return nil, fmt.Errorf("unknown bugle route %d", data.BugleRoute)
	}
	return msg, nil
}

func (c *Client) deduplicateHash(id string, hash [32]byte) bool {
	const recentUpdatesLen = len(c.recentUpdates)
	for i := c.recentUpdatesPtr + recentUpdatesLen - 1; i >= c.recentUpdatesPtr; i-- {
		if c.recentUpdates[i%recentUpdatesLen].id == id {
			if c.recentUpdates[i%recentUpdatesLen].hash == hash {
				return true
			} else {
				break
			}
		}
	}
	c.recentUpdates[c.recentUpdatesPtr] = updateDedupItem{id: id, hash: hash}
	c.recentUpdatesPtr = (c.recentUpdatesPtr + 1) % recentUpdatesLen
	return false
}

func (c *Client) logContent(res *IncomingRPCMessage, thingID string, contentHash []byte) {
	if c.Logger.Trace().Enabled() && (res.DecryptedData != nil || res.DecryptedMessage != nil) {
		evt := c.Logger.Trace().Bool("is_old", res.IsOld)
		if res.DecryptedMessage != nil {
			evt.Str("proto_name", string(res.DecryptedMessage.ProtoReflect().Descriptor().FullName()))
		}
		if res.DecryptedData != nil {
			evt.Int("decrypted_payload_size", len(res.DecryptedData))
			if contentHash != nil {
				logSafeProviderID(evt, "provider_id", thingID)
				evt.Hex("data_hash", contentHash)
			}
		}
		evt.Msg("Got event")
	}
}

func (c *Client) deduplicateUpdate(id string, msg *IncomingRPCMessage) bool {
	if msg.DecryptedData != nil {
		contentHash := sha256.Sum256(msg.DecryptedData)
		if c.deduplicateHash(id, contentHash) {
			evt := c.Logger.Trace().Hex("data_hash", contentHash[:])
			logSafeProviderID(evt, "provider_id", id)
			evt.
				Bool("is_old", msg.IsOld).
				Msg("Ignoring duplicate update")
			return true
		}
		c.logContent(msg, id, contentHash[:])
	}
	return false
}

func (c *Client) HandleRPCMsg(rawMsg *gmproto.IncomingRPCMessage) {
	c.HandleRPCMsgContext(context.Background(), rawMsg)
}

func (c *Client) HandleRPCMsgContext(ctx context.Context, rawMsg *gmproto.IncomingRPCMessage) {
	c.handleRPCMsgEnvelopeContext(ctx, rawMsg, nil)
}

// handleRPCMsgEnvelopeContext preserves the exact provider frame when called
// by the long-poll reader. Direct callers fall back to deterministic protobuf
// bytes for compatibility with non-stream integrations.
func (c *Client) handleRPCMsgEnvelopeContext(ctx context.Context, rawMsg *gmproto.IncomingRPCMessage, rawEnvelope []byte) {
	if rawMsg == nil {
		return
	}
	if !domain.ValidProviderResponseID(rawMsg.ResponseID) {
		if c.shouldLogInvalidProviderResponseID(time.Now()) {
			c.Logger.Warn().Int("provider_response_id_size", len(rawMsg.ResponseID)).Msg("Discarded provider frame with invalid response ID")
		}
		return
	}
	raw := append([]byte(nil), rawEnvelope...)
	var marshalErr error
	if len(raw) == 0 {
		raw, marshalErr = proto.MarshalOptions{Deterministic: true}.Marshal(rawMsg)
	}
	msg, err := c.decryptInternalMessage(rawMsg)
	if marshalErr != nil && err == nil {
		err = marshalErr
	}
	handler, _, failureObserver := c.durableHandlers()
	durableOutcome := DurableOutcomeUnknown
	dataEventProvenanceClassified := false
	consumeRestartBacklog := false
	if handler != nil {
		request := c.sessionHandler.durableRequestFor(msg)
		if err == nil && msg != nil && msg.BugleRoute == gmproto.BugleRoute_DataEvent && request.Action == gmproto.ActionType_UNSPECIFIED {
			// Restart backlog provenance is part of the durable event contract, so
			// classify an unsolicited data frame before its transaction. A matched
			// RPC response never consumes the backlog counter and still exits through
			// receiveResponse below.
			dataEventProvenanceClassified = true
			consumeRestartBacklog = c.getSkipCount() > 0
			if consumeRestartBacklog {
				msg.IsOld = true
			}
		}
		var handleErr error
		durableOutcome, handleErr = handler(ctx, DurableEnvelope{
			ResponseID: rawMsg.ResponseID, Raw: append([]byte(nil), raw...), Decoded: msg, DecodeError: err, Request: request,
		})
		if handleErr != nil || (durableOutcome != DurableOutcomeCommitted && durableOutcome != DurableOutcomePoisoned && durableOutcome != DurableOutcomeDuplicatePoisoned) {
			evt := c.Logger.Warn()
			logSafeProviderID(evt, "provider_response_id", rawMsg.ResponseID)
			evt.Msg("Durable envelope commit failed; withholding ACK")
			if handleErr == nil {
				handleErr = ErrDurablePersistence
			}
			failure := &durablePersistenceError{cause: handleErr}
			if failureObserver != nil {
				failureObserver(failure)
			}
			c.sessionHandler.receiveDurableFailure(msg, failure)
			return
		}
		if durableOutcome == DurableOutcomeCommitted && consumeRestartBacklog {
			// Consume only after persistence succeeds. A failed durable attempt may
			// be presented again on the same connection and must remain a replay.
			_ = c.decrementSkipCount()
		}
		if msg != nil {
			msg.DurableOutcome = durableOutcome
		}
	}
	if err != nil {
		evt := logSafeError(c.Logger.Error(), err)
		logSafeProviderID(evt, "provider_response_id", rawMsg.ResponseID)
		evt.Msg("Failed to decode incoming RPC message")
		if handler != nil {
			c.sessionHandler.queueMessageAck(rawMsg.ResponseID)
			if durableOutcome == DurableOutcomePoisoned || durableOutcome == DurableOutcomeDuplicatePoisoned {
				matched := msg != nil && c.sessionHandler.receiveDurablePoison(msg)
				if durableOutcome == DurableOutcomePoisoned && !matched && failureObserver != nil {
					failureObserver(ErrDurablePoisoned)
				}
			}
		}
		return
	}

	if durableOutcome == DurableOutcomePoisoned || durableOutcome == DurableOutcomeDuplicatePoisoned {
		if handler != nil {
			c.sessionHandler.queueMessageAck(msg.ResponseID)
		}
		matched := c.sessionHandler.receiveDurablePoison(msg)
		if durableOutcome == DurableOutcomePoisoned && !matched && failureObserver != nil {
			failureObserver(ErrDurablePoisoned)
		}
		return
	}
	if handler != nil {
		c.sessionHandler.queueMessageAck(msg.ResponseID)
	}
	if c.sessionHandler.receiveResponse(msg) {
		return
	}
	logEvt := c.Logger.Debug().Stringer("bugle_route", msg.BugleRoute)
	logSafeProviderID(logEvt, "provider_response_id", msg.ResponseID)
	if msg.Message != nil {
		logEvt.Stringer("message_action", msg.Message.Action)
	}
	logEvt.Msg("Received message")
	switch msg.BugleRoute {
	case gmproto.BugleRoute_PairEvent:
		c.handlePairingEvent(msg)
	case gmproto.BugleRoute_GaiaEvent:
		c.handleGaiaPairingEvent(msg)
	case gmproto.BugleRoute_DataEvent:
		if !dataEventProvenanceClassified && c.decrementSkipCount() {
			msg.IsOld = true
		}
		c.handleUpdatesEvent(msg)
	}
}

type WrappedMessage struct {
	*gmproto.Message
	IsOld bool
	Data  []byte
}

// AuthenticatedSettings proves that Settings came from the primary encrypted
// GET_UPDATES payload. Gateway consumers use this provenance-bearing event for
// durable line discovery; the legacy raw *gmproto.Settings event remains
// additive for existing connector consumers.
type AuthenticatedSettings struct {
	Settings *gmproto.Settings
	IsOld    bool
}

var hackyLoggedOutBytes = []byte{0x72, 0x00}

// IsLegacyLogoutControl validates the complete observed shape of the historic
// unauthenticated logout notification. It is deliberately not a correlated
// response: a session ID, any ciphertext, or any marker variation rejects it.
func IsLegacyLogoutControl(message *gmproto.RPCMessageData) bool {
	return message != nil && message.GetAction() == gmproto.ActionType_GET_UPDATES && message.GetSessionID() == "" &&
		message.EncryptedData == nil && message.EncryptedData2 == nil && bytes.Equal(message.UnencryptedData, hackyLoggedOutBytes)
}

// isGaiaPairingResponse reports whether a data event is a GAIA pairing handshake
// response (CLIENT_INIT/CLIENT_FINISHED). These are exchanged before session
// encryption keys exist, so they are legitimately unencrypted; every other
// plaintext data event is still rejected as unauthenticated.
func isGaiaPairingResponse(message *gmproto.RPCMessageData) bool {
	if message == nil || message.UnencryptedData == nil {
		return false
	}
	switch message.GetAction() {
	case gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_INIT, gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_FINISHED:
		return true
	default:
		return false
	}
}

func (c *Client) handleUpdatesEvent(msg *IncomingRPCMessage) {
	if msg.PayloadSource == PayloadSourceLogoutControl {
		if IsLegacyLogoutControl(msg.Message) {
			c.triggerEvent(&events.GaiaLoggedOut{})
		}
		return
	}
	if msg.PayloadSource == PayloadSourceEncryptedData2 {
		accountChange := msg.SecondaryMessage.GetAccountChange()
		if strings.ContainsRune(accountChange.GetAccount(), '@') {
			c.triggerEvent(&events.AccountChange{
				AccountChangeOrSomethingEvent: accountChange,
				IsFake:                        true,
			})
		}
		return
	}
	if msg.PayloadSource != PayloadSourceEncryptedData {
		return
	}
	switch msg.Message.Action {
	case gmproto.ActionType_GET_UPDATES:
		if !msg.IsOld {
			c.bumpNextDataReceiveCheck(c.dataReceiveCheckInterval)
		}
		data, ok := msg.DecryptedMessage.(*gmproto.UpdateEvents)
		if !ok {
			c.Logger.Error().
				Type("data_type", msg.DecryptedMessage).
				Bool("is_old", msg.IsOld).
				Msg("Unexpected data type in GET_UPDATES event")
			return
		}

		switch evt := data.Event.(type) {
		case *gmproto.UpdateEvents_UserAlertEvent:
			c.logContent(msg, "", nil)
			if msg.IsOld {
				return
			}
			c.triggerEvent(evt.UserAlertEvent)

		case *gmproto.UpdateEvents_SettingsEvent:
			logSafeBytes(c.Logger.Debug().Bool("is_old", msg.IsOld), "decrypted_payload", msg.DecryptedData).
				Msg("Got settings event")
			c.triggerEvent(&AuthenticatedSettings{Settings: evt.SettingsEvent, IsOld: msg.IsOld})
			c.triggerEvent(evt.SettingsEvent)

		case *gmproto.UpdateEvents_ConversationEvent:
			for _, part := range evt.ConversationEvent.GetData() {
				if c.deduplicateUpdate(part.GetConversationID(), msg) {
					return
				} else if msg.IsOld {
					evt := c.Logger.Debug()
					logSafeProviderID(evt, "provider_conversation_id", part.ConversationID)
					evt.Msg("Ignoring old conversation event")
					continue
				}
				c.triggerEvent(part)
			}

		case *gmproto.UpdateEvents_MessageEvent:
			for _, part := range evt.MessageEvent.GetData() {
				if c.deduplicateUpdate(part.GetMessageID(), msg) {
					return
				}
				c.triggerEvent(&WrappedMessage{
					Message: part,
					IsOld:   msg.IsOld,
					Data:    msg.DecryptedData,
				})
			}

		case *gmproto.UpdateEvents_TypingEvent:
			c.logContent(msg, "", nil)
			if msg.IsOld {
				return
			}
			c.triggerEvent(evt.TypingEvent.GetData())

		case *gmproto.UpdateEvents_BrowserPresenceCheckEvent:
			c.Logger.Trace().Msg("Got browser presence check, sending ack")
			c.startBrowserPresenceACK()

		case *gmproto.UpdateEvents_AccountChange:
			c.logContent(msg, "", nil)
			c.triggerEvent(&events.AccountChange{
				AccountChangeOrSomethingEvent: evt.AccountChange,
			})

		default:
			logSafeBytes(logSafeBytes(c.Logger.Warn(), "provider_frame", msg.GetMessageData()), "decrypted_payload", msg.DecryptedData).
				Msg("Got unknown event type")
		}
	default:
		evt := logSafeBytes(c.Logger.Debug(), "provider_frame", msg.GetMessageData())
		logSafeProviderID(evt, "provider_request_id", msg.Message.SessionID)
		evt.Stringer("action_type", msg.Message.Action).
			Bool("is_old", msg.IsOld).
			Msg("Got unexpected response")
	}
}

func logSafeBytes(event *zerolog.Event, field string, value []byte) *zerolog.Event {
	digest := sha256.Sum256(value)
	return event.Int(field+"_size", len(value)).Hex(field+"_sha256", digest[:])
}

func logSafeError(event *zerolog.Event, err error) *zerolog.Event {
	classification := "provider_operation"
	switch {
	case errors.Is(err, context.Canceled):
		classification = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		classification = "deadline"
	}
	return event.Str("error_class", classification)
}

func logSafeProviderID(event *zerolog.Event, field, value string) {
	logSafeBytes(event, field, []byte(value))
}
