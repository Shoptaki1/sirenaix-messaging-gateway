package libgm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/util"
)

// ErrPhoneNotResponding is returned when the phone doesn't respond to a request within responseHardTimeout.
// The request was already accepted by the server, so the phone may still process it if it comes back online.
var ErrPhoneNotResponding = errors.New("phone did not respond to request")

// ErrConnectionClosed is returned for requests that were still waiting for a response when the client was disconnected.
var ErrConnectionClosed = errors.New("client disconnected before response was received")

// pingShortCircuitTimeout is how long to wait for a response before poking the ditto pinger
// (which will send a PhoneNotResponding event if its ping doesn't get a response quickly either).
const pingShortCircuitTimeout = 5 * time.Second

// responseHardTimeout is how long to wait for the phone to respond to a request before giving up.
const responseHardTimeout = 60 * time.Second

const (
	ackBatchLimit = 256
	ackQueueLimit = 1024
	// ProviderACKRequestTimeout is the hard upper bound for the single
	// no-transparent-retry provider ACK request.
	ProviderACKRequestTimeout = 5 * time.Second
)

type SessionHandler struct {
	client *Client

	responseWaiters     map[string]responseWaiter
	responseWaitersLock sync.Mutex

	ackMapLock  sync.Mutex
	ackMap      []string
	ackSet      map[string]struct{}
	ackTickerMu sync.Mutex
	ackTicker   lifecycleTicker

	sessionID string
}

type responseWaiter struct {
	channel chan<- *IncomingRPCMessage
	request DurableRequest
}

func (s *SessionHandler) ResetSessionID() {
	s.sessionID = uuid.NewString()
}

func (s *SessionHandler) sendMessageNoResponse(ctx context.Context, params SendMessageParams) error {
	requestID, payload, err := s.buildMessage(params)
	if err != nil {
		return err
	}

	url := util.SendMessageURL
	if s.client.AuthData.HasCookies() {
		url = util.SendMessageURLGoogle
	}
	s.client.Logger.Debug().
		Stringer("message_action", params.Action).
		Str("message_id", requestID).
		Msg("Sending request to phone (not expecting response)")
	_, err = typedHTTPResponse[*gmproto.OutgoingRPCResponse](
		s.client.doSendHTTPRequest(ctx, url, payload, params.DisableHTTPRetry),
	)
	return err
}

func (s *SessionHandler) sendAsyncMessage(ctx context.Context, params SendMessageParams) (<-chan *IncomingRPCMessage, error) {
	ch, _, err := s.sendAsyncMessageWithID(ctx, params)
	return ch, err
}

func (s *SessionHandler) sendAsyncMessageWithID(ctx context.Context, params SendMessageParams) (chan *IncomingRPCMessage, string, error) {
	requestID, payload, err := s.buildMessage(params)
	if err != nil {
		return nil, "", err
	}

	ch := s.waitResponse(requestID, params)
	url := util.SendMessageURL
	if s.client.AuthData.HasCookies() {
		url = util.SendMessageURLGoogle
	}
	s.client.Logger.Debug().
		Stringer("message_action", params.Action).
		Str("message_id", requestID).
		Msg("Sending request to phone")
	_, err = typedHTTPResponse[*gmproto.OutgoingRPCResponse](
		s.client.doSendHTTPRequest(ctx, url, payload, params.DisableHTTPRetry),
	)
	if err != nil {
		s.cancelResponse(requestID, ch)
		return nil, "", err
	}
	return ch, requestID, nil
}

func typedResponse[T proto.Message](resp *IncomingRPCMessage, err error) (casted T, retErr error) {
	if err != nil {
		retErr = err
		return
	}
	if resp == nil {
		retErr = ErrConnectionClosed
		return
	}
	if resp.DurableError != nil {
		retErr = resp.DurableError
		return
	}
	var ok bool
	casted, ok = resp.DecryptedMessage.(T)
	if !ok {
		retErr = fmt.Errorf("unexpected provider response type %T (response ID redacted), expected %T", resp.DecryptedMessage, casted)
	}
	return
}

func (s *SessionHandler) waitResponse(requestID string, params SendMessageParams) chan *IncomingRPCMessage {
	ch := make(chan *IncomingRPCMessage, 1)
	request := DurableRequest{Action: params.Action}
	if listMessages, ok := params.Data.(*gmproto.ListMessagesRequest); ok {
		request.ConversationID = listMessages.GetConversationID()
		request.Cursor = marshalDurableRequestCursor(listMessages.GetCursor())
	} else if listConversations, ok := params.Data.(*gmproto.ListConversationsRequest); ok {
		request.Cursor = marshalDurableRequestCursor(listConversations.GetCursor())
	}
	s.responseWaitersLock.Lock()
	s.responseWaiters[requestID] = responseWaiter{channel: ch, request: request}
	s.responseWaitersLock.Unlock()
	return ch
}

func marshalDurableRequestCursor(cursor *gmproto.Cursor) []byte {
	if cursor == nil {
		return nil
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(cursor)
	if err != nil || len(encoded) == 0 || len(encoded) > 4<<10 {
		return nil
	}
	return encoded
}

func (s *SessionHandler) cancelResponse(requestID string, ch chan *IncomingRPCMessage) {
	s.responseWaitersLock.Lock()
	defer s.responseWaitersLock.Unlock()
	// Only close the channel if the response hasn't been claimed yet: receiveResponse
	// removes the waiter from the map (under the lock) before sending to the channel,
	// so closing an already-claimed channel here could panic that send.
	if _, ok := s.responseWaiters[requestID]; ok {
		delete(s.responseWaiters, requestID)
		close(ch)
	}
}

func (s *SessionHandler) cancelAllResponseWaiters() {
	s.responseWaitersLock.Lock()
	defer s.responseWaitersLock.Unlock()
	for requestID, waiter := range s.responseWaiters {
		delete(s.responseWaiters, requestID)
		close(waiter.channel)
	}
}

func (s *SessionHandler) durableRequestFor(msg *IncomingRPCMessage) DurableRequest {
	if msg == nil || msg.PayloadSource != PayloadSourceEncryptedData || msg.Message == nil || msg.Message.SessionID == "" {
		return DurableRequest{}
	}
	s.responseWaitersLock.Lock()
	defer s.responseWaitersLock.Unlock()
	waiter, ok := s.responseWaiters[msg.Message.SessionID]
	if !ok || waiter.request.Action != msg.Message.Action {
		return DurableRequest{}
	}
	request := waiter.request
	request.Cursor = append([]byte(nil), request.Cursor...)
	return request
}

func (s *SessionHandler) receiveResponse(msg *IncomingRPCMessage) bool {
	if msg == nil || msg.Message == nil {
		return false
	}
	// Correlated responses are either encrypted (normal RPCs) or unencrypted (the GAIA
	// pairing handshake, which runs before session keys exist). Rejecting everything but
	// encrypted here dropped the pairing responses, so the GAIA switch below never ran.
	if msg.PayloadSource != PayloadSourceEncryptedData && msg.PayloadSource != PayloadSourceUnencryptedData {
		return false
	}
	if s.client.AuthData.HasCookies() {
		switch msg.Message.Action {
		case gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_INIT, gmproto.ActionType_CREATE_GAIA_PAIRING_CLIENT_FINISHED:
		default:
			// Very hacky way to ignore weird messages that come before real responses
			// TODO figure out how to properly handle these
			if msg.Message.UnencryptedData != nil && msg.Message.EncryptedData == nil {
				return false
			}
		}
	}
	requestID := msg.Message.SessionID
	s.responseWaitersLock.Lock()
	waiter, ok := s.responseWaiters[requestID]
	if !ok || waiter.request.Action != msg.Message.Action {
		s.responseWaitersLock.Unlock()
		return false
	}
	delete(s.responseWaiters, requestID)
	s.responseWaitersLock.Unlock()
	s.client.emitLifecycleActivity(lifecycleActivityPhoneResponse)
	evt := s.client.Logger.Debug()
	logSafeProviderID(evt, "provider_request_id", requestID)
	logSafeProviderID(evt, "provider_response_id", msg.ResponseID)
	if msg.Message != nil {
		evt.Stringer("message_action", msg.Message.Action)
	}
	if s.client.Logger.GetLevel() == zerolog.TraceLevel {
		if msg.DecryptedData != nil {
			logSafeBytes(evt, "decrypted_payload", msg.DecryptedData)
		}
		if msg.DecryptedMessage != nil {
			evt.Str("proto_name", string(msg.DecryptedMessage.ProtoReflect().Descriptor().FullName()))
		}
	}
	evt.Msg("Received response")
	waiter.channel <- msg
	return true
}

// receiveDurablePoison completes only the exact authenticated request ID after
// its raw response has been durably quarantined. It deliberately ignores the
// provider-supplied action and discards all response data, so a mismatched or
// partially decoded response can unblock its caller without being accepted.
func (s *SessionHandler) receiveDurablePoison(msg *IncomingRPCMessage) bool {
	if msg == nil || msg.PayloadSource != PayloadSourceEncryptedData || msg.Message == nil || msg.Message.SessionID == "" {
		return false
	}
	requestID := msg.Message.SessionID
	s.responseWaitersLock.Lock()
	waiter, ok := s.responseWaiters[requestID]
	if !ok {
		s.responseWaitersLock.Unlock()
		return false
	}
	delete(s.responseWaiters, requestID)
	s.responseWaitersLock.Unlock()
	zeroBytes(msg.DecryptedData)
	msg.DecryptedData = nil
	msg.DecryptedMessage = nil
	msg.DurableOutcome = DurableOutcomePoisoned
	msg.DurableError = ErrDurablePoisoned
	s.client.emitLifecycleActivity(lifecycleActivityPhoneResponse)
	evt := s.client.Logger.Debug()
	logSafeProviderID(evt, "provider_request_id", requestID)
	logSafeProviderID(evt, "provider_response_id", msg.ResponseID)
	evt.Msg("Received durably quarantined response")
	waiter.channel <- msg
	return true
}

// receiveDurableFailure completes only a matching authenticated request/action
// after its durable store failed. It does not expose response bytes that were
// not committed and therefore cannot be trusted by the application caller.
func (s *SessionHandler) receiveDurableFailure(msg *IncomingRPCMessage, failure error) bool {
	if msg == nil || msg.PayloadSource != PayloadSourceEncryptedData || msg.Message == nil || msg.Message.SessionID == "" || failure == nil {
		return false
	}
	requestID := msg.Message.SessionID
	s.responseWaitersLock.Lock()
	waiter, ok := s.responseWaiters[requestID]
	if !ok || waiter.request.Action != msg.Message.Action {
		s.responseWaitersLock.Unlock()
		return false
	}
	delete(s.responseWaiters, requestID)
	s.responseWaitersLock.Unlock()
	zeroBytes(msg.DecryptedData)
	msg.DecryptedData = nil
	msg.DecryptedMessage = nil
	msg.DurableOutcome = DurableOutcomeUnknown
	msg.DurableError = failure
	s.client.emitLifecycleActivity(lifecycleActivityPhoneResponse)
	evt := s.client.Logger.Debug()
	logSafeProviderID(evt, "provider_request_id", requestID)
	logSafeProviderID(evt, "provider_response_id", msg.ResponseID)
	evt.Msg("Received response whose durable commit failed")
	waiter.channel <- msg
	return true
}

func (s *SessionHandler) sendMessageWithParams(ctx context.Context, params SendMessageParams) (*IncomingRPCMessage, error) {
	ch, requestID, err := s.sendAsyncMessageWithID(ctx, params)
	if err != nil {
		return nil, err
	}

	shortCircuitTimer := time.NewTimer(pingShortCircuitTimeout)
	defer shortCircuitTimer.Stop()
	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, ErrConnectionClosed
		}
		return resp, nil
	case <-ctx.Done():
		s.cancelResponse(requestID, ch)
		return nil, ctx.Err()
	case <-shortCircuitTimer.C:
		// Notify the pinger in order to trigger an event that the phone isn't responding
		select {
		case s.client.pingShortCircuit <- struct{}{}:
		default:
		}
	}
	hardTimer := time.NewTimer(responseHardTimeout - pingShortCircuitTimeout)
	defer hardTimer.Stop()
	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, ErrConnectionClosed
		}
		return resp, nil
	case <-ctx.Done():
		s.cancelResponse(requestID, ch)
		return nil, ctx.Err()
	case <-hardTimer.C:
		s.cancelResponse(requestID, ch)
		return nil, fmt.Errorf("%w in %s", ErrPhoneNotResponding, responseHardTimeout)
	}
}

func (s *SessionHandler) sendMessage(ctx context.Context, actionType gmproto.ActionType, encryptedData proto.Message) (*IncomingRPCMessage, error) {
	return s.sendMessageWithParams(ctx, SendMessageParams{
		Action: actionType,
		Data:   encryptedData,
	})
}

type SendMessageParams struct {
	Action gmproto.ActionType
	Data   proto.Message

	RequestID        string
	OmitTTL          bool
	CustomTTL        int64
	DontEncrypt      bool
	MessageType      gmproto.MessageType
	DisableHTTPRetry bool
}

func (c *Client) doSendHTTPRequest(ctx context.Context, url string, payload proto.Message, disableRetry bool) (*http.Response, error) {
	if disableRetry {
		return c.makeProtobufHTTPRequestContextNoRetry(ctx, url, payload, ContentTypePBLite)
	}
	return c.makeProtobufHTTPRequestContext(ctx, url, payload, ContentTypePBLite, false)
}

func (s *SessionHandler) buildMessage(params SendMessageParams) (string, proto.Message, error) {
	var err error
	auth := s.client.AuthData.Snapshot()
	defer auth.ClearSecrets()
	sessionID := s.client.sessionHandler.sessionID

	requestID := params.RequestID
	if requestID == "" {
		requestID = uuid.NewString()
	}

	if params.MessageType == 0 {
		params.MessageType = gmproto.MessageType_BUGLE_MESSAGE
	}

	message := &gmproto.OutgoingRPCMessage{
		Mobile: auth.Mobile,
		Data: &gmproto.OutgoingRPCMessage_Data{
			RequestID:  requestID,
			BugleRoute: gmproto.BugleRoute_DataEvent,
			MessageTypeData: &gmproto.OutgoingRPCMessage_Data_Type{
				EmptyArr:    &gmproto.EmptyArr{},
				MessageType: params.MessageType,
			},
		},
		Auth: &gmproto.OutgoingRPCMessage_Auth{
			RequestID: requestID,
			// Copy the token: this message outlives the auth snapshot, whose deferred
			// ClearSecrets() zeroes the source slice in place. Referencing it directly
			// sent an all-zero TachyonAuthToken, so SendMessage came back UNAUTHENTICATED.
			TachyonAuthToken: append([]byte(nil), auth.TachyonAuthToken...),
			ConfigVersion:    util.ConfigMessage,
		},
		DestRegistrationIDs: []string{},
	}
	if auth.DestRegID != uuid.Nil {
		message.DestRegistrationIDs = append(message.DestRegistrationIDs, auth.DestRegID.String())
	}
	if params.CustomTTL != 0 {
		message.TTL = params.CustomTTL
	} else if !params.OmitTTL {
		message.TTL = auth.TachyonTTL
	}
	var encryptedData, unencryptedData []byte
	if params.Data != nil {
		var serializedData []byte
		serializedData, err = proto.Marshal(params.Data)
		if err != nil {
			return "", nil, err
		}
		if params.DontEncrypt {
			unencryptedData = serializedData
		} else {
			encryptedData, err = auth.RequestCrypto.Encrypt(serializedData)
			if err != nil {
				return "", nil, err
			}
		}
	}
	message.Data.MessageData, err = proto.Marshal(&gmproto.OutgoingRPCData{
		RequestID:            requestID,
		Action:               params.Action,
		UnencryptedProtoData: unencryptedData,
		EncryptedProtoData:   encryptedData,
		SessionID:            sessionID,
	})
	if err != nil {
		return "", nil, err
	}

	return requestID, message, err
}

func (s *SessionHandler) queueMessageAck(messageID string) {
	if !domain.ValidProviderResponseID(messageID) {
		return
	}
	s.ackMapLock.Lock()
	defer s.ackMapLock.Unlock()
	if s.ackSet == nil {
		s.ackSet = make(map[string]struct{}, ackQueueLimit)
	}
	if _, duplicate := s.ackSet[messageID]; duplicate {
		return
	}
	if len(s.ackMap) >= ackQueueLimit {
		// The authoritative pending set is durable. A successful ACK observer
		// or actor restart refills this bounded in-memory window.
		s.client.Logger.Warn().Int("ack_queue_size", len(s.ackMap)).Msg("Durable ACK memory window is full")
		return
	}
	s.ackSet[messageID] = struct{}{}
	s.ackMap = append(s.ackMap, messageID)
}

func (s *SessionHandler) ackLoop(ctx context.Context, newTicker func(time.Duration) lifecycleTicker) {
	ticker := newTicker(5 * time.Second)
	s.ackTickerMu.Lock()
	s.ackTicker = ticker
	s.ackTickerMu.Unlock()
	defer func() {
		ticker.Stop()
		s.ackTickerMu.Lock()
		if s.ackTicker == ticker {
			s.ackTicker = nil
		}
		s.ackTickerMu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			s.sendAckRequestContext(ctx)
		}
	}
}

func (s *SessionHandler) hasACKTicker() bool {
	s.ackTickerMu.Lock()
	defer s.ackTickerMu.Unlock()
	return s.ackTicker != nil
}

func (s *SessionHandler) sendAckRequest() {
	s.sendAckRequestContext(context.Background())
}

func (s *SessionHandler) sendAckRequestContext(ctx context.Context) {
	s.ackMapLock.Lock()
	batchSize := min(len(s.ackMap), ackBatchLimit)
	dataToAck := append([]string(nil), s.ackMap[:batchSize]...)
	s.ackMap = append(s.ackMap[:0], s.ackMap[batchSize:]...)
	for _, id := range dataToAck {
		delete(s.ackSet, id)
	}
	s.ackMapLock.Unlock()
	if len(dataToAck) == 0 {
		return
	}
	if coordinator := s.client.durableACKCoordinator(); coordinator != nil {
		result, coordinateErr := coordinator(ctx, append([]string(nil), dataToAck...), s.sendProviderACKBatch)
		if validateErr := validateACKCoordinationResult(dataToAck, result); validateErr != nil && coordinateErr == nil {
			coordinateErr = validateErr
		}
		if coordinateErr != nil {
			retryIDs := result.RetryIDs
			if len(retryIDs) == 0 {
				retryIDs = dataToAck
			}
			s.requeueMessageACKs(retryIDs)
			s.client.Logger.Warn().Int("ack_count", len(dataToAck)).Msg("Failed to coordinate durable provider ACK batch; retaining pending ACKs")
			_, _, failureObserver := s.client.durableHandlers()
			if failureObserver != nil {
				failureObserver(&durablePersistenceError{cause: coordinateErr})
			}
			return
		}
		if result.ProviderError != nil {
			s.requeueMessageACKs(result.RetryIDs)
			logSafeError(s.client.Logger.Error(), result.ProviderError).Int("ack_count", len(result.AdmittedIDs)).Msg("Failed to send provider ACK batch")
		}
		return
	}
	err := s.sendProviderACKBatch(ctx, dataToAck)
	if err != nil {
		logSafeError(s.client.Logger.Error(), err).Int("ack_count", len(dataToAck)).Msg("Failed to send provider ACK batch")
		s.requeueMessageACKs(dataToAck)
		return
	}
	s.client.Logger.Trace().Int("ack_count", len(dataToAck)).Msg("Sent provider ACK batch")
	_, observer, failureObserver := s.client.durableHandlers()
	if observer != nil {
		if observeErr := observer(ctx, append([]string(nil), dataToAck...)); observeErr != nil {
			s.client.Logger.Warn().Int("ack_count", len(dataToAck)).Msg("Failed to persist provider ACK result; retaining pending ACKs")
			s.requeueMessageACKs(dataToAck)
			if failureObserver != nil {
				failureObserver(&durablePersistenceError{cause: observeErr})
			}
		}
	}
}

func (s *SessionHandler) sendProviderACKBatch(ctx context.Context, dataToAck []string) error {
	if len(dataToAck) == 0 || len(dataToAck) > ackBatchLimit {
		return errors.New("invalid provider ACK batch")
	}
	auth := s.client.AuthData.Snapshot()
	defer auth.ClearSecrets()
	ackMessages := make([]*gmproto.AckMessageRequest_Message, len(dataToAck))
	for i, reqID := range dataToAck {
		ackMessages[i] = &gmproto.AckMessageRequest_Message{
			RequestID: reqID,
			Device:    auth.Browser,
		}
	}
	payload := &gmproto.AckMessageRequest{
		AuthData: &gmproto.AuthMessage{
			RequestID:        uuid.NewString(),
			TachyonAuthToken: auth.TachyonAuthToken,
			Network:          auth.AuthNetwork(),
			ConfigVersion:    util.ConfigMessage,
		},
		EmptyArr: &gmproto.EmptyArr{},
		Acks:     ackMessages,
	}
	url := util.AckMessagesURL
	if auth.HasCookies() {
		url = util.AckMessagesURLGoogle
	}
	requestCtx, cancel := context.WithTimeout(ctx, ProviderACKRequestTimeout)
	defer cancel()
	_, err := typedHTTPResponse[*gmproto.OutgoingRPCResponse](
		s.client.makeProtobufHTTPRequestContextNoRetry(requestCtx, url, payload, ContentTypePBLite),
	)
	return err
}

func validateACKCoordinationResult(candidates []string, result ACKCoordinationResult) error {
	candidateSet := make(map[string]struct{}, len(candidates))
	for _, id := range candidates {
		candidateSet[id] = struct{}{}
	}
	admittedSet := make(map[string]struct{}, len(result.AdmittedIDs))
	for listIndex, ids := range [][]string{result.AdmittedIDs, result.RetryIDs} {
		seen := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			if _, exists := candidateSet[id]; !exists {
				return errors.New("durable ACK coordinator returned an unknown response ID")
			}
			if _, duplicate := seen[id]; duplicate {
				return errors.New("durable ACK coordinator returned a duplicate response ID")
			}
			seen[id] = struct{}{}
			if listIndex == 0 {
				admittedSet[id] = struct{}{}
			} else if result.ProviderError != nil {
				if _, admitted := admittedSet[id]; !admitted {
					return errors.New("provider ACK retry was not admitted")
				}
			}
		}
	}
	if result.ProviderError != nil && len(result.RetryIDs) == 0 {
		return errors.New("provider ACK failure omitted retry IDs")
	}
	return nil
}

func (s *SessionHandler) requeueMessageACKs(ids []string) {
	s.ackMapLock.Lock()
	defer s.ackMapLock.Unlock()
	if s.ackSet == nil {
		s.ackSet = make(map[string]struct{}, ackQueueLimit)
	}
	prefix := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, duplicate := s.ackSet[id]; !duplicate && len(prefix)+len(s.ackMap) < ackQueueLimit {
			s.ackSet[id] = struct{}{}
			prefix = append(prefix, id)
		}
	}
	if len(prefix) > 0 {
		queue := make([]string, 0, len(prefix)+len(s.ackMap))
		queue = append(queue, prefix...)
		queue = append(queue, s.ackMap...)
		s.ackMap = queue
	}
}
