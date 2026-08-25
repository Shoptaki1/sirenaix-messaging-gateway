package libgm

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	libevents "go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"go.mau.fi/util/pblite"
	"google.golang.org/protobuf/proto"
)

func TestProviderFramesAndDecryptedMessagesNeverEnterLogsAtAnyLevel(t *testing.T) {
	sentinel := []byte("SIRENAIX_PROVIDER_SECRET_SENTINEL")
	encodedSentinel := base64.StdEncoding.EncodeToString(sentinel)
	const responseIDCanary = "OPAQUE_PROVIDER_RESPONSE_CANARY"
	const requestIDCanary = "OPAQUE_PROVIDER_REQUEST_CANARY"
	for _, level := range []zerolog.Level{zerolog.TraceLevel, zerolog.DebugLevel, zerolog.WarnLevel} {
		t.Run(level.String(), func(t *testing.T) {
			var output bytes.Buffer
			client := NewClient(NewAuthData(), nil, zerolog.New(&output).Level(level))
			client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
				ResponseID: "response-malformed", BugleRoute: gmproto.BugleRoute_PairEvent, MessageData: sentinel,
			})
			client.handleUpdatesEvent(&IncomingRPCMessage{
				IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: "response-settings"},
				Message:            &gmproto.RPCMessageData{Action: gmproto.ActionType_GET_UPDATES}, DecryptedData: sentinel,
				DecryptedMessage: &gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_SettingsEvent{SettingsEvent: &gmproto.Settings{}}},
			})
			response := &IncomingRPCMessage{
				IncomingRPCMessage: &gmproto.IncomingRPCMessage{ResponseID: responseIDCanary},
				Message:            &gmproto.RPCMessageData{Action: gmproto.ActionType_LIST_MESSAGES, SessionID: requestIDCanary},
				PayloadSource:      PayloadSourceEncryptedData,
				DecryptedData:      sentinel, DecryptedMessage: &gmproto.ListMessagesResponse{},
			}
			_ = client.sessionHandler.waitResponse(requestIDCanary, SendMessageParams{Action: gmproto.ActionType_LIST_MESSAGES})
			if !client.sessionHandler.receiveResponse(response) {
				t.Fatal("response waiter was not notified")
			}
			logs := output.String()
			if bytes.Contains([]byte(logs), sentinel) || strings.Contains(logs, encodedSentinel) ||
				strings.Contains(logs, responseIDCanary) || strings.Contains(logs, requestIDCanary) {
				t.Fatalf("provider secret entered %s logs: %s", level, logs)
			}
		})
	}
}

func TestSettingsEventCarriesAuthenticatedPrimaryCiphertextProvenance(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	settings := &gmproto.Settings{SIMCards: []*gmproto.SIMCard{{SIMParticipant: &gmproto.SIMParticipant{ID: "line-a"}}}}
	var received []any
	client.SetEventHandler(func(event any) { received = append(received, event) })
	message := func(source PayloadSource) *IncomingRPCMessage {
		return &IncomingRPCMessage{
			Message: &gmproto.RPCMessageData{Action: gmproto.ActionType_GET_UPDATES}, PayloadSource: source,
			DecryptedMessage: &gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_SettingsEvent{SettingsEvent: settings}},
		}
	}

	client.handleUpdatesEvent(message(PayloadSourceNone))
	if len(received) != 0 {
		t.Fatalf("unauthenticated settings emitted events: %#v", received)
	}
	client.handleUpdatesEvent(message(PayloadSourceEncryptedData))
	if len(received) != 2 {
		t.Fatalf("authenticated settings events = %#v", received)
	}
	authenticated, ok := received[0].(*AuthenticatedSettings)
	if !ok || authenticated.Settings != settings || authenticated.IsOld {
		t.Fatalf("authenticated settings wrapper = %#v", received[0])
	}
	if legacy, ok := received[1].(*gmproto.Settings); !ok || legacy != settings {
		t.Fatalf("legacy settings event = %#v", received[1])
	}
}

func TestRestartBacklogProvenanceIsSetBeforeDurablePersistence(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.setSkipCount(1)
	updates, err := proto.Marshal(&gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_MessageEvent{
		MessageEvent: &gmproto.MessageEvent{Data: []*gmproto.Message{{MessageID: "provider-message-a"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(updates)
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{Action: gmproto.ActionType_GET_UPDATES, EncryptedData: encrypted})
	if err != nil {
		t.Fatal(err)
	}
	var captured DurableEnvelope
	var oldAtPersistence bool
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		captured = envelope
		oldAtPersistence = envelope.Decoded != nil && envelope.Decoded.IsOld
		return DurableOutcomeCommitted, nil
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-restart-backlog", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	if captured.Decoded == nil || !oldAtPersistence {
		t.Fatalf("durable restart envelope provenance = %+v, want IsOld before persistence", captured.Decoded)
	}
	if remaining := client.getSkipCount(); remaining != 0 {
		t.Fatalf("restart backlog skip count = %d, want 0", remaining)
	}
}

func TestRestartBacklogCountIsRetainedWhenDurablePersistenceFails(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.setSkipCount(1)
	updates, err := proto.Marshal(&gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_MessageEvent{
		MessageEvent: &gmproto.MessageEvent{Data: []*gmproto.Message{{MessageID: "provider-message-a"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(updates)
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{Action: gmproto.ActionType_GET_UPDATES, EncryptedData: encrypted})
	if err != nil {
		t.Fatal(err)
	}
	oldAtPersistence := false
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		oldAtPersistence = envelope.Decoded != nil && envelope.Decoded.IsOld
		return DurableOutcomeUnknown, errors.New("database unavailable")
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-restart-retry", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	if !oldAtPersistence {
		t.Fatal("restart backlog lost old provenance at failed durable attempt")
	}
	if remaining := client.getSkipCount(); remaining != 1 {
		t.Fatalf("failed durable attempt consumed restart backlog count: %d", remaining)
	}
}

func productionCorrelatedResponseFixtures() []struct {
	action   gmproto.ActionType
	response proto.Message
	rawOnly  bool
} {
	// This is deliberately independent of the decoder map. It inventories every
	// public Client method that registers a response waiter in methods.go.
	return []struct {
		action   gmproto.ActionType
		response proto.Message
		rawOnly  bool
	}{
		{gmproto.ActionType_LIST_CONVERSATIONS, &gmproto.ListConversationsResponse{}, false},
		{gmproto.ActionType_LIST_MESSAGES, &gmproto.ListMessagesResponse{}, false},
		{gmproto.ActionType_IS_BUGLE_DEFAULT, &gmproto.IsBugleDefaultResponse{}, true},
		{gmproto.ActionType_NOTIFY_DITTO_ACTIVITY, &gmproto.NotifyDittoActivityResponse{}, true},
		{gmproto.ActionType_GET_CONVERSATION_TYPE, &gmproto.GetConversationTypeResponse{}, true},
		{gmproto.ActionType_GET_CONVERSATION, &gmproto.GetConversationResponse{}, true},
		{gmproto.ActionType_SEND_MESSAGE, &gmproto.SendMessageResponse{}, true},
		{gmproto.ActionType_SEND_REACTION, &gmproto.SendReactionResponse{}, true},
		{gmproto.ActionType_DELETE_MESSAGE, &gmproto.DeleteMessageResponse{}, true},
		{gmproto.ActionType_MESSAGE_READ, nil, true}, // The phone replies with an authenticated empty payload.
		{gmproto.ActionType_GET_PARTICIPANTS_THUMBNAIL, &gmproto.GetThumbnailResponse{}, true},
		{gmproto.ActionType_GET_CONTACTS_THUMBNAIL, &gmproto.GetThumbnailResponse{}, true},
		{gmproto.ActionType_LIST_CONTACTS, &gmproto.ListContactsResponse{}, true},
		{gmproto.ActionType_LIST_TOP_CONTACTS, &gmproto.ListTopContactsResponse{}, true},
		{gmproto.ActionType_GET_OR_CREATE_CONVERSATION, &gmproto.GetOrCreateConversationResponse{}, true},
		{gmproto.ActionType_UPDATE_CONVERSATION, &gmproto.UpdateConversationResponse{}, true},
		{gmproto.ActionType_GET_FULL_SIZE_IMAGE, &gmproto.GetFullSizeImageResponse{}, true},
	}
}

func TestDurabilityPolicyClassifiesEveryProductionCorrelatedResponseAction(t *testing.T) {
	for _, fixture := range productionCorrelatedResponseFixtures() {
		fixture := fixture
		t.Run(fixture.action.String(), func(t *testing.T) {
			var response proto.Message
			if fixture.response != nil {
				response = fixture.response.ProtoReflect().New().Interface()
			}
			if got := IsKnownCorrelatedRPCResponse(fixture.action, response, PayloadSourceEncryptedData); got != fixture.rawOnly {
				t.Fatalf("production response %s raw-only policy = %v, want %v", fixture.action, got, fixture.rawOnly)
			}
			if IsKnownCorrelatedRPCResponse(fixture.action, response, PayloadSourceNone) {
				t.Fatalf("unauthenticated production response %s was accepted by raw-only policy", fixture.action)
			}
		})
	}
	for _, action := range []gmproto.ActionType{
		gmproto.ActionType_GET_UPDATES,
		gmproto.ActionType(32767),
	} {
		if IsKnownCorrelatedRPCResponse(action, nil, PayloadSourceEncryptedData) {
			t.Fatalf("projection-bearing or unknown action %s was classified as raw-only", action)
		}
	}
}

func TestProductionCorrelatedResponsesReachExactWaiterAfterDurableCommit(t *testing.T) {
	for _, fixture := range productionCorrelatedResponseFixtures() {
		fixture := fixture
		t.Run(fixture.action.String(), func(t *testing.T) {
			client := NewClient(NewAuthData(), nil, zerolog.Nop())
			client.setSkipCount(1)
			client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
				if envelope.Decoded != nil && envelope.Decoded.IsOld {
					t.Fatal("correlated RPC response was classified as restart backlog")
				}
				return DurableOutcomeCommitted, nil
			})
			var payload []byte
			var err error
			if fixture.response != nil {
				payload, err = proto.Marshal(fixture.response)
				if err != nil {
					t.Fatal(err)
				}
			}
			encrypted, err := client.AuthData.RequestCrypto.Encrypt(payload)
			if err != nil {
				t.Fatal(err)
			}
			requestID := "request-" + fixture.action.String()
			waiter := client.sessionHandler.waitResponse(requestID, SendMessageParams{Action: fixture.action})
			rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
				SessionID: requestID, Action: fixture.action, EncryptedData: encrypted,
			})
			if err != nil {
				t.Fatal(err)
			}
			client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
				ResponseID: "response-" + fixture.action.String(), BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
			})
			if remaining := client.getSkipCount(); remaining != 1 {
				t.Fatalf("correlated response consumed restart backlog count: %d", remaining)
			}
			select {
			case response := <-waiter:
				if response == nil || response.DurableOutcome != DurableOutcomeCommitted || response.DurableError != nil {
					t.Fatalf("waiter response = %+v", response)
				}
				if fixture.response == nil {
					if response.DecryptedMessage != nil || len(response.DecryptedData) != 0 {
						t.Fatalf("empty response exposed payload: %T %x", response.DecryptedMessage, response.DecryptedData)
					}
				} else if reflect.TypeOf(response.DecryptedMessage) != reflect.TypeOf(fixture.response) {
					t.Fatalf("decoded response type = %T, want %T", response.DecryptedMessage, fixture.response)
				}
			default:
				client.sessionHandler.cancelResponse(requestID, waiter)
				t.Fatal("durably committed response did not complete waiter")
			}
		})
	}
}

func TestDataResponsesWithoutAuthenticatedCiphertextPoisonExactWaiters(t *testing.T) {
	actions := make([]gmproto.ActionType, 0, len(productionCorrelatedResponseFixtures())+1)
	for _, fixture := range productionCorrelatedResponseFixtures() {
		actions = append(actions, fixture.action)
	}
	actions = append(actions, gmproto.ActionType_GET_UPDATES)
	for _, action := range actions {
		action := action
		for _, mode := range []string{"missing", "plaintext"} {
			mode := mode
			t.Run(action.String()+"/"+mode, func(t *testing.T) {
				client := NewClient(NewAuthData(), nil, zerolog.Nop())
				var captured DurableEnvelope
				failures := make(chan error, 1)
				client.SetDurableFailureObserver(func(err error) { failures <- err })
				client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
					captured = envelope
					return DurableOutcomePoisoned, nil
				})
				requestID := "request-unauth-" + action.String() + "-" + mode
				waiter := client.sessionHandler.waitResponse(requestID, SendMessageParams{Action: action})
				message := &gmproto.RPCMessageData{SessionID: requestID, Action: action}
				if mode == "plaintext" {
					message.UnencryptedData = []byte("untrusted-wrapper-payload")
				}
				rpcBytes, err := proto.Marshal(message)
				if err != nil {
					t.Fatal(err)
				}
				client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
					ResponseID: "response-unauth-" + action.String() + "-" + mode, BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
				})
				if captured.DecodeError == nil || captured.Decoded == nil || captured.Decoded.PayloadSource != PayloadSourceNone {
					t.Fatalf("unauthenticated %s durable envelope = %+v", action, captured)
				}
				select {
				case response := <-waiter:
					t.Fatalf("unauthenticated %s wrapper metadata completed waiter: %+v", action, response)
				default:
				}
				client.sessionHandler.cancelResponse(requestID, waiter)
				select {
				case failure := <-failures:
					if !errors.Is(failure, ErrDurablePoisoned) {
						t.Fatalf("unauthenticated %s failure = %v", action, failure)
					}
				default:
					t.Fatalf("unauthenticated %s poison did not signal connection quarantine", action)
				}
			})
		}
	}
}

func TestLegacyUnencryptedLogoutControlRunsOnlyAfterDurableCommitAndNeverCompletesWaiter(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	var captured DurableEnvelope
	eventsSeen := make(chan any, 1)
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		captured = envelope
		select {
		case event := <-eventsSeen:
			t.Fatalf("logout event escaped before durable commit: %T", event)
		default:
		}
		return DurableOutcomeCommitted, nil
	})
	client.SetEventHandler(func(event any) { eventsSeen <- event })
	waiter := client.sessionHandler.waitResponse("request-logout-hint", SendMessageParams{Action: gmproto.ActionType_GET_UPDATES})
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		Action: gmproto.ActionType_GET_UPDATES, UnencryptedData: append([]byte(nil), hackyLoggedOutBytes...),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-logout-hint", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	if captured.DecodeError != nil || captured.Decoded == nil || captured.Decoded.PayloadSource != PayloadSourceLogoutControl || captured.Request.Action != gmproto.ActionType_UNSPECIFIED {
		t.Fatalf("logout control durable envelope = %+v", captured)
	}
	select {
	case response := <-waiter:
		t.Fatalf("logout control completed an RPC waiter: %+v", response)
	default:
	}
	client.sessionHandler.cancelResponse("request-logout-hint", waiter)
	select {
	case event := <-eventsSeen:
		if _, ok := event.(*libevents.GaiaLoggedOut); !ok {
			t.Fatalf("logout control event = %T", event)
		}
	default:
		t.Fatal("durably committed logout control did not emit GaiaLoggedOut")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if !reflect.DeepEqual(client.sessionHandler.ackMap, []string{"response-logout-hint"}) {
		t.Fatalf("logout control ACK queue = %v", client.sessionHandler.ackMap)
	}
}

func TestLegacyUnencryptedLogoutControlPersistenceFailureWithholdsEventAndACK(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	eventsSeen := make(chan any, 1)
	client.SetEventHandler(func(event any) { eventsSeen <- event })
	databaseFailure := errors.New("logout inbox unavailable")
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		if envelope.Decoded == nil || envelope.Decoded.PayloadSource != PayloadSourceLogoutControl || envelope.DecodeError != nil {
			t.Fatalf("logout control durable envelope = %+v", envelope)
		}
		return DurableOutcomeUnknown, databaseFailure
	})
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		Action: gmproto.ActionType_GET_UPDATES, UnencryptedData: append([]byte(nil), hackyLoggedOutBytes...),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-logout-failed", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case event := <-eventsSeen:
		t.Fatalf("uncommitted logout control emitted event: %T", event)
	default:
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 0 {
		t.Fatalf("uncommitted logout control ACK queue = %v", client.sessionHandler.ackMap)
	}
}

func TestLegacyUnencryptedLogoutControlPreservesNonDurableLibgmEvent(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	eventsSeen := make(chan any, 1)
	client.SetEventHandler(func(event any) { eventsSeen <- event })
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		Action: gmproto.ActionType_GET_UPDATES, UnencryptedData: append([]byte(nil), hackyLoggedOutBytes...),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-logout-upstream", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case event := <-eventsSeen:
		if _, ok := event.(*libevents.GaiaLoggedOut); !ok {
			t.Fatalf("non-durable logout event = %T", event)
		}
	default:
		t.Fatal("non-durable libgm did not preserve exact logout event")
	}
}

func TestLegacyUnencryptedLogoutControlRejectsEveryShapeMutation(t *testing.T) {
	mutations := map[string]func(*gmproto.RPCMessageData){
		"marker":     func(message *gmproto.RPCMessageData) { message.UnencryptedData = []byte{0x72, 0x01} },
		"extra data": func(message *gmproto.RPCMessageData) { message.UnencryptedData = []byte{0x72, 0x00, 0x01} },
		"action":     func(message *gmproto.RPCMessageData) { message.Action = gmproto.ActionType_LIST_MESSAGES },
		"session":    func(message *gmproto.RPCMessageData) { message.SessionID = "request-collision" },
		"primary":    func(message *gmproto.RPCMessageData) { message.EncryptedData = []byte("ciphertext") },
		"secondary":  func(message *gmproto.RPCMessageData) { message.EncryptedData2 = []byte("ciphertext") },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			client := NewClient(NewAuthData(), nil, zerolog.Nop())
			message := &gmproto.RPCMessageData{Action: gmproto.ActionType_GET_UPDATES, UnencryptedData: append([]byte(nil), hackyLoggedOutBytes...)}
			mutate(message)
			if IsLegacyLogoutControl(message) {
				t.Fatal("mutated logout wrapper passed exact predicate")
			}
			var captured DurableEnvelope
			client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
				captured = envelope
				return DurableOutcomePoisoned, nil
			})
			eventsSeen := make(chan any, 1)
			client.SetEventHandler(func(event any) { eventsSeen <- event })
			waiter := client.sessionHandler.waitResponse("request-collision", SendMessageParams{Action: gmproto.ActionType_GET_UPDATES})
			rpcBytes, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
				ResponseID: "response-logout-mutated-" + strings.ReplaceAll(name, " ", "-"), BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
			})
			if captured.DecodeError == nil || captured.Decoded == nil || captured.Decoded.PayloadSource != PayloadSourceNone ||
				captured.Request.Action != gmproto.ActionType_UNSPECIFIED {
				t.Fatalf("mutated logout durable envelope = %+v", captured)
			}
			select {
			case event := <-eventsSeen:
				t.Fatalf("mutated logout emitted event: %T", event)
			default:
			}
			select {
			case response := <-waiter:
				t.Fatalf("mutated logout completed waiter: %+v", response)
			default:
			}
			client.sessionHandler.cancelResponse("request-collision", waiter)
		})
	}
}

func TestMessageReadRequiresAuthenticatedEmptyPrimaryCiphertext(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		if envelope.DecodeError != nil || envelope.Decoded == nil || envelope.Decoded.PayloadSource != PayloadSourceEncryptedData {
			t.Fatalf("authenticated empty primary envelope = %+v", envelope)
		}
		return DurableOutcomeCommitted, nil
	})
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(nil)
	if err != nil {
		t.Fatal(err)
	}
	requestID := "request-read-primary"
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: requestID, Action: gmproto.ActionType_MESSAGE_READ, EncryptedData: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter := client.sessionHandler.waitResponse(requestID, SendMessageParams{Action: gmproto.ActionType_MESSAGE_READ})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-read-primary", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case response := <-waiter:
		if response == nil || response.DurableError != nil || response.PayloadSource != PayloadSourceEncryptedData || response.DecryptedMessage != nil || len(response.DecryptedData) != 0 {
			t.Fatalf("authenticated empty primary response = %+v", response)
		}
	default:
		client.sessionHandler.cancelResponse(requestID, waiter)
		t.Fatal("authenticated empty primary did not complete waiter")
	}
}

func TestDurableEnvelopeHandlerRunsBeforeACKEligibilityIncludingPoison(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	var handled DurableEnvelope
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		handled = envelope
		return DurableOutcomePoisoned, nil
	})
	raw := &gmproto.IncomingRPCMessage{ResponseID: "response-a", BugleRoute: gmproto.BugleRoute(999)}
	client.HandleRPCMsgContext(context.Background(), raw)
	if handled.ResponseID != "response-a" || len(handled.Raw) == 0 || handled.DecodeError == nil {
		t.Fatalf("handled envelope = %+v", handled)
	}
	client.sessionHandler.ackMapLock.Lock()
	acks := append([]string(nil), client.sessionHandler.ackMap...)
	client.sessionHandler.ackMapLock.Unlock()
	if len(acks) != 1 || acks[0] != "response-a" {
		t.Fatalf("queued ACKs = %v", acks)
	}
}

func TestNewUnmatchedDurablePoisonSignalsPermanentFailureAfterQueuingACK(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.SetDurableEnvelopeHandler(func(context.Context, DurableEnvelope) (DurableOutcome, error) {
		return DurableOutcomePoisoned, nil
	})
	failures := make(chan error, 1)
	client.SetDurableFailureObserver(func(err error) { failures <- err })
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "unsolicited-poison", BugleRoute: gmproto.BugleRoute(999), MessageData: []byte("malformed"),
	})
	select {
	case err := <-failures:
		if !errors.Is(err, ErrDurablePoisoned) {
			t.Fatalf("unmatched poison failure = %v", err)
		}
	default:
		t.Fatal("new unmatched durable poison did not signal the runtime")
	}
	client.sessionHandler.ackMapLock.Lock()
	acks := append([]string(nil), client.sessionHandler.ackMap...)
	client.sessionHandler.ackMapLock.Unlock()
	if !reflect.DeepEqual(acks, []string{"unsolicited-poison"}) {
		t.Fatalf("unmatched poison ACK queue = %v", acks)
	}
}

func TestDuplicateUnmatchedDurablePoisonConvergesWithoutRepeatedFailureSignal(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.SetDurableEnvelopeHandler(func(context.Context, DurableEnvelope) (DurableOutcome, error) {
		return DurableOutcomeDuplicatePoisoned, nil
	})
	failures := make(chan error, 1)
	client.SetDurableFailureObserver(func(err error) { failures <- err })
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "duplicate-unsolicited-poison", BugleRoute: gmproto.BugleRoute(999), MessageData: []byte("malformed"),
	})
	select {
	case err := <-failures:
		t.Fatalf("duplicate poison repeated permanent failure signal: %v", err)
	default:
	}
	client.sessionHandler.ackMapLock.Lock()
	acks := append([]string(nil), client.sessionHandler.ackMap...)
	client.sessionHandler.ackMapLock.Unlock()
	if !reflect.DeepEqual(acks, []string{"duplicate-unsolicited-poison"}) {
		t.Fatalf("duplicate poison ACK queue = %v", acks)
	}
}

func TestInvalidProviderResponseIDIsDiscardedBeforeDurabilityWaiterAndACK(t *testing.T) {
	for name, responseID := range map[string]string{
		"oversize":     strings.Repeat("r", 257),
		"nul":          "response\x00id",
		"control":      "response\nid",
		"bidi":         "response\u202eid",
		"private":      "response\ue000id",
		"noncharacter": "response\ufdd0id",
	} {
		t.Run(name, func(t *testing.T) {
			client := NewClient(NewAuthData(), nil, zerolog.Nop())
			handled := 0
			client.SetDurableEnvelopeHandler(func(context.Context, DurableEnvelope) (DurableOutcome, error) {
				handled++
				return DurableOutcomeCommitted, nil
			})
			responseBytes, err := proto.Marshal(&gmproto.ListConversationsResponse{})
			if err != nil {
				t.Fatal(err)
			}
			encrypted, err := client.AuthData.RequestCrypto.Encrypt(responseBytes)
			if err != nil {
				t.Fatal(err)
			}
			rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
				SessionID: "request-invalid-response-id", Action: gmproto.ActionType_LIST_CONVERSATIONS, EncryptedData: encrypted,
			})
			if err != nil {
				t.Fatal(err)
			}
			waiter := client.sessionHandler.waitResponse("request-invalid-response-id", SendMessageParams{Action: gmproto.ActionType_LIST_CONVERSATIONS})
			client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
				ResponseID: responseID, BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
			})
			if handled != 0 {
				t.Fatalf("invalid response reached durable handler %d times", handled)
			}
			client.sessionHandler.ackMapLock.Lock()
			acks := len(client.sessionHandler.ackMap)
			client.sessionHandler.ackMapLock.Unlock()
			if acks != 0 {
				t.Fatalf("invalid response queued %d ACKs", acks)
			}
			select {
			case <-waiter:
				t.Fatal("invalid response completed authenticated pagination waiter")
			default:
			}
			client.sessionHandler.cancelResponse("request-invalid-response-id", waiter)
		})
	}
}

func TestInvalidProviderResponseIDWarningsAreSampledAndNeverIncludeOpaqueID(t *testing.T) {
	var output bytes.Buffer
	client := NewClient(NewAuthData(), nil, zerolog.New(&output).Level(zerolog.WarnLevel))
	canary := strings.Repeat("OPAQUE_INVALID_PROVIDER_RESPONSE_ID_", 12)
	for range 100 {
		client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
			ResponseID: canary, BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: []byte("ignored"),
		})
	}
	logs := output.String()
	if strings.Contains(logs, canary) {
		t.Fatal("invalid opaque provider response ID entered logs")
	}
	count := strings.Count(logs, "Discarded provider frame with invalid response ID")
	if count == 0 || count > invalidProviderIDLogBurst {
		t.Fatalf("invalid response warning count = %d, want 1..%d", count, invalidProviderIDLogBurst)
	}
}

func TestDurableACKRefillUsesCanonicalProviderResponseIDBoundary(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	if err := client.QueueDurableACKs([]string{strings.Repeat("r", 256)}); err != nil {
		t.Fatalf("256-byte ACK ID rejected: %v", err)
	}
	for _, responseID := range []string{strings.Repeat("r", 257), "response\x00id", "response\nid", " response"} {
		if err := client.QueueDurableACKs([]string{responseID}); err == nil {
			t.Fatalf("invalid ACK response ID %q accepted", responseID)
		}
	}
}

func TestDurableEnvelopeCarriesTrustedListMessagesRequestTarget(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	responseBytes, err := proto.Marshal(&gmproto.ListMessagesResponse{Cursor: &gmproto.Cursor{
		LastItemID: "empty-page", LastItemTimestamp: 1724400000024,
	}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(responseBytes)
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: "request-list-messages", Action: gmproto.ActionType_LIST_MESSAGES, EncryptedData: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	requestCursor := &gmproto.Cursor{LastItemID: "request-cursor", LastItemTimestamp: 1724400000011}
	waiter := client.sessionHandler.waitResponse("request-list-messages", SendMessageParams{
		Action: gmproto.ActionType_LIST_MESSAGES,
		Data:   &gmproto.ListMessagesRequest{ConversationID: "conversation-requested", Cursor: requestCursor},
	})
	var got DurableRequest
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		got = envelope.Request
		return DurableOutcomePoisoned, nil
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-empty", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	wantCursor, _ := proto.MarshalOptions{Deterministic: true}.Marshal(requestCursor)
	if got.Action != gmproto.ActionType_LIST_MESSAGES || got.ConversationID != "conversation-requested" || !bytes.Equal(got.Cursor, wantCursor) {
		t.Fatalf("durable request metadata = %+v", got)
	}
	select {
	case response := <-waiter:
		if response.DurableOutcome != DurableOutcomePoisoned {
			t.Fatalf("response durable outcome = %v", response.DurableOutcome)
		}
	default:
		t.Fatal("response was not delivered after its durable commit")
	}
}

func TestDecodedSameActionDurablePoisonNeverExposesResponseData(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	responseBytes, err := proto.Marshal(&gmproto.ListMessagesResponse{Messages: []*gmproto.Message{{MessageID: "must-not-escape"}}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(responseBytes)
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: "request-semantic-poison", Action: gmproto.ActionType_LIST_MESSAGES, EncryptedData: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter := client.sessionHandler.waitResponse("request-semantic-poison", SendMessageParams{
		Action: gmproto.ActionType_LIST_MESSAGES, Data: &gmproto.ListMessagesRequest{ConversationID: "conversation-a"},
	})
	client.SetDurableEnvelopeHandler(func(context.Context, DurableEnvelope) (DurableOutcome, error) {
		return DurableOutcomePoisoned, nil
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-semantic-poison", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case response := <-waiter:
		data, responseErr := typedResponse[*gmproto.ListMessagesResponse](response, nil)
		if !errors.Is(responseErr, ErrDurablePoisoned) || data != nil || response.DecryptedData != nil || response.DecryptedMessage != nil {
			t.Fatalf("semantic poison exposed response: data=%+v raw=%x decoded=%T err=%v", data, response.DecryptedData, response.DecryptedMessage, responseErr)
		}
	case <-time.After(time.Second):
		t.Fatal("semantic poison did not complete exact waiter")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 1 || client.sessionHandler.ackMap[0] != "response-semantic-poison" {
		t.Fatalf("semantic poison ACK queue = %v", client.sessionHandler.ackMap)
	}
}

func TestDurablePersistenceFailureReleasesExactRPCWaiterWithoutACK(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	responseBytes, err := proto.Marshal(&gmproto.ListMessagesResponse{})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(responseBytes)
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: "request-durable-db", Action: gmproto.ActionType_LIST_MESSAGES, EncryptedData: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter := client.sessionHandler.waitResponse("request-durable-db", SendMessageParams{
		Action: gmproto.ActionType_LIST_MESSAGES,
		Data:   &gmproto.ListMessagesRequest{ConversationID: "conversation-a"},
	})
	databaseFailure := errors.New("database transaction unavailable")
	client.SetDurableEnvelopeHandler(func(context.Context, DurableEnvelope) (DurableOutcome, error) {
		return DurableOutcomeUnknown, databaseFailure
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-durable-db", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case response := <-waiter:
		_, responseErr := typedResponse[*gmproto.ListMessagesResponse](response, nil)
		if !errors.Is(responseErr, ErrDurablePersistence) || !errors.Is(responseErr, databaseFailure) {
			t.Fatalf("durable waiter error = %v", responseErr)
		}
	default:
		t.Fatal("durable persistence failure left the exact RPC waiter blocked")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 0 {
		t.Fatalf("durable persistence failure queued ACKs = %v", client.sessionHandler.ackMap)
	}
}

func TestDecodeAndPersistenceFailureReleasesAuthenticatedExactWaiterWithoutACK(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	encrypted, err := client.AuthData.RequestCrypto.Encrypt([]byte{0xff, 0xff, 0xff})
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: "request-malformed-durable", Action: gmproto.ActionType_LIST_MESSAGES, EncryptedData: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter := client.sessionHandler.waitResponse("request-malformed-durable", SendMessageParams{
		Action: gmproto.ActionType_LIST_MESSAGES, Data: &gmproto.ListMessagesRequest{ConversationID: "conversation-a"},
	})
	databaseFailure := errors.New("persist poison transaction unavailable")
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		if envelope.Decoded == nil || envelope.DecodeError == nil || envelope.Request.ConversationID != "conversation-a" {
			t.Fatalf("partially decoded authenticated envelope = %+v", envelope)
		}
		return DurableOutcomeUnknown, databaseFailure
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-malformed-durable", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case response := <-waiter:
		_, responseErr := typedResponse[*gmproto.ListMessagesResponse](response, nil)
		if !errors.Is(responseErr, ErrDurablePersistence) || !errors.Is(responseErr, databaseFailure) {
			t.Fatalf("durable waiter error = %v", responseErr)
		}
	case <-time.After(time.Second):
		t.Fatal("combined decode and persistence failure left exact waiter blocked")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 0 {
		t.Fatalf("combined failure queued ACKs = %v", client.sessionHandler.ackMap)
	}
}

func TestAuthenticatedPartialDecodeDurablePoisonReleasesExactWaiterAndACKs(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	encrypted, err := client.AuthData.RequestCrypto.Encrypt([]byte{0xff, 0xff, 0xff})
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: "request-malformed-poison", Action: gmproto.ActionType_LIST_MESSAGES, EncryptedData: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter := client.sessionHandler.waitResponse("request-malformed-poison", SendMessageParams{
		Action: gmproto.ActionType_LIST_MESSAGES, Data: &gmproto.ListMessagesRequest{ConversationID: "conversation-a"},
	})
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		if envelope.Decoded == nil || envelope.DecodeError == nil || envelope.Request.ConversationID != "conversation-a" {
			t.Fatalf("partially decoded authenticated envelope = %+v", envelope)
		}
		return DurableOutcomePoisoned, nil
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-malformed-poison", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case response := <-waiter:
		_, responseErr := typedResponse[*gmproto.ListMessagesResponse](response, nil)
		if !errors.Is(responseErr, ErrDurablePoisoned) || response.DurableOutcome != DurableOutcomePoisoned {
			t.Fatalf("durable poison waiter = outcome %v, error %v", response.DurableOutcome, responseErr)
		}
	case <-time.After(time.Second):
		t.Fatal("successful durable poison left exact RPC waiter blocked")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 1 || client.sessionHandler.ackMap[0] != "response-malformed-poison" {
		t.Fatalf("successful durable poison ACK queue = %v", client.sessionHandler.ackMap)
	}
}

func TestSecondaryOrDualCiphertextNeverSatisfiesCorrelatedWaiter(t *testing.T) {
	actions := make([]gmproto.ActionType, 0, len(productionCorrelatedResponseFixtures())+1)
	for _, fixture := range productionCorrelatedResponseFixtures() {
		actions = append(actions, fixture.action)
	}
	actions = append(actions, gmproto.ActionType_GET_UPDATES)
	for _, action := range actions {
		action := action
		for _, mode := range []string{"secondary", "dual", "primary-plaintext"} {
			mode := mode
			t.Run(action.String()+"/"+mode, func(t *testing.T) {
				client := NewClient(NewAuthData(), nil, zerolog.Nop())
				container, err := proto.Marshal(&gmproto.EncryptedData2Container{})
				if err != nil {
					t.Fatal(err)
				}
				secondary, err := client.AuthData.RequestCrypto.Encrypt(container)
				if err != nil {
					t.Fatal(err)
				}
				message := &gmproto.RPCMessageData{SessionID: "request-" + action.String() + "-" + mode, Action: action}
				if mode == "secondary" || mode == "dual" {
					message.EncryptedData2 = secondary
				}
				if mode == "dual" {
					message.EncryptedData, err = client.AuthData.RequestCrypto.Encrypt(nil)
					if err != nil {
						t.Fatal(err)
					}
				} else if mode == "primary-plaintext" {
					message.EncryptedData, err = client.AuthData.RequestCrypto.Encrypt(nil)
					if err != nil {
						t.Fatal(err)
					}
					message.UnencryptedData = []byte("ambiguous-wrapper-plaintext")
				}
				rpcBytes, err := proto.Marshal(message)
				if err != nil {
					t.Fatal(err)
				}
				waiter := client.sessionHandler.waitResponse(message.SessionID, SendMessageParams{
					Action: action,
				})
				failures := make(chan error, 1)
				client.SetDurableFailureObserver(func(err error) { failures <- err })
				client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
					if envelope.Decoded == nil || envelope.Request.Action != gmproto.ActionType_UNSPECIFIED || envelope.Request.ConversationID != "" || len(envelope.Request.Cursor) != 0 {
						t.Fatalf("%s durable envelope trusted correlated wrapper metadata: %+v", mode, envelope)
					}
					if mode == "secondary" && (envelope.DecodeError != nil || envelope.Decoded.PayloadSource != PayloadSourceEncryptedData2) {
						t.Fatalf("secondary durable envelope = %+v", envelope)
					}
					if mode != "secondary" && (envelope.DecodeError == nil || envelope.Decoded.PayloadSource != PayloadSourceNone) {
						t.Fatalf("ambiguous %s durable envelope = %+v", mode, envelope)
					}
					return DurableOutcomePoisoned, nil
				})
				client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
					ResponseID: "response-" + action.String() + "-" + mode, BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
				})
				select {
				case response := <-waiter:
					t.Fatalf("%s ciphertext satisfied correlated waiter: %+v", mode, response)
				default:
				}
				client.sessionHandler.cancelResponse(message.SessionID, waiter)
				select {
				case failure := <-failures:
					if !errors.Is(failure, ErrDurablePoisoned) {
						t.Fatalf("%s failure = %v", mode, failure)
					}
				default:
					t.Fatalf("%s poison did not signal connection quarantine", mode)
				}
			})
		}
	}
}

func TestAuthenticatedSecondaryAccountChangeRunsOnlyAfterDurableCommit(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	container, err := proto.Marshal(&gmproto.EncryptedData2Container{AccountChange: &gmproto.AccountChangeOrSomethingEvent{
		Account: "account@example.test", Enabled: true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(container)
	if err != nil {
		t.Fatal(err)
	}
	var committed bool
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		if envelope.DecodeError != nil || envelope.Decoded == nil || envelope.Decoded.PayloadSource != PayloadSourceEncryptedData2 ||
			envelope.Request.Action != gmproto.ActionType_UNSPECIFIED || envelope.Request.ConversationID != "" || len(envelope.Request.Cursor) != 0 {
			t.Fatalf("secondary account-change durable envelope = %+v", envelope)
		}
		committed = true
		return DurableOutcomeCommitted, nil
	})
	eventsSeen := make(chan *libevents.AccountChange, 1)
	client.SetEventHandler(func(event any) {
		accountChange, ok := event.(*libevents.AccountChange)
		if ok {
			if !committed {
				t.Error("secondary account change escaped before durable commit")
			}
			eventsSeen <- accountChange
		}
	})
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		Action: gmproto.ActionType_GET_UPDATES, EncryptedData2: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-account-change", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case event := <-eventsSeen:
		if event.AccountChangeOrSomethingEvent.GetAccount() != "account@example.test" {
			t.Fatalf("account change event = %+v", event)
		}
	default:
		t.Fatal("authenticated secondary account change was not emitted")
	}
}

func TestAuthenticatedActionMismatchDurablePoisonReleasesExactWaiterWithoutData(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	responseBytes, err := proto.Marshal(&gmproto.ListConversationsResponse{})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(responseBytes)
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: "request-action-mismatch", Action: gmproto.ActionType_LIST_CONVERSATIONS, EncryptedData: encrypted,
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter := client.sessionHandler.waitResponse("request-action-mismatch", SendMessageParams{
		Action: gmproto.ActionType_LIST_MESSAGES, Data: &gmproto.ListMessagesRequest{ConversationID: "conversation-a"},
	})
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		if envelope.Request.Action != gmproto.ActionType_UNSPECIFIED {
			t.Fatalf("mismatched action inherited trusted request metadata: %+v", envelope.Request)
		}
		return DurableOutcomePoisoned, nil
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-action-mismatch", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case response := <-waiter:
		_, responseErr := typedResponse[*gmproto.ListMessagesResponse](response, nil)
		if !errors.Is(responseErr, ErrDurablePoisoned) || response.DurableOutcome != DurableOutcomePoisoned {
			t.Fatalf("mismatched poison waiter = outcome %v, error %v", response.DurableOutcome, responseErr)
		}
		if response.DecryptedMessage != nil || len(response.DecryptedData) != 0 {
			t.Fatal("mismatched provider response data crossed the durable poison boundary")
		}
	case <-time.After(time.Second):
		t.Fatal("durably poisoned action mismatch left exact RPC waiter blocked")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 1 || client.sessionHandler.ackMap[0] != "response-action-mismatch" {
		t.Fatalf("mismatched durable poison ACK queue = %v", client.sessionHandler.ackMap)
	}
}

func TestUnsolicitedDurablePersistenceFailureSignalsObserverWithoutACK(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	updates, err := proto.Marshal(&gmproto.UpdateEvents{})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(updates)
	if err != nil {
		t.Fatal(err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{Action: gmproto.ActionType_GET_UPDATES, EncryptedData: encrypted})
	if err != nil {
		t.Fatal(err)
	}
	databaseFailure := errors.New("inbox database unavailable")
	failures := make(chan error, 1)
	client.SetDurableFailureObserver(func(failure error) { failures <- failure })
	client.SetDurableEnvelopeHandler(func(context.Context, DurableEnvelope) (DurableOutcome, error) {
		return DurableOutcomeUnknown, databaseFailure
	})
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-unsolicited-db", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	})
	select {
	case failure := <-failures:
		if !errors.Is(failure, ErrDurablePersistence) || !errors.Is(failure, databaseFailure) {
			t.Fatalf("observer failure = %v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("unsolicited durable failure was not signaled")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 0 {
		t.Fatalf("unsolicited failure queued ACKs = %v", client.sessionHandler.ackMap)
	}
}

func TestLongPollDurabilityPreservesExactProviderFrameBytes(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	want := []byte(`[{"exact":"provider-frame","spacing":true}]`)
	var got []byte
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		got = append([]byte(nil), envelope.Raw...)
		return DurableOutcomeCommitted, nil
	})
	client.handleRPCMsgEnvelopeContext(context.Background(), &gmproto.IncomingRPCMessage{
		ResponseID: "response-exact", BugleRoute: gmproto.BugleRoute(999),
	}, want)
	if !bytes.Equal(got, want) {
		t.Fatalf("durable raw frame = %q, want %q", got, want)
	}
}

func TestRealLongPollAssemblyPreservesExactProviderFrameBytes(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	message := &gmproto.IncomingRPCMessage{ResponseID: "response-assembled", BugleRoute: gmproto.BugleRoute(999)}
	want, err := pblite.Marshal(&gmproto.LongPollingPayload{Data: message})
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		got = append([]byte(nil), envelope.Raw...)
		return DurableOutcomeCommitted, nil
	})
	reader := &sequenceReadCloser{chunks: [][]byte{[]byte("[["), want, []byte("]]")}}
	if clean := client.readLongPollContext(context.Background(), ptrLogger(zerolog.Nop()), reader, false); !clean {
		t.Fatal("long-poll reader did not close cleanly")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("assembled durable raw = %q, want %q", got, want)
	}
}

func TestLongPollParserHandlesEveryFragmentBoundaryAndCoalescedFrames(t *testing.T) {
	first, err := pblite.Marshal(&gmproto.LongPollingPayload{Data: &gmproto.IncomingRPCMessage{ResponseID: "response-first", BugleRoute: gmproto.BugleRoute(999)}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := pblite.Marshal(&gmproto.LongPollingPayload{Data: &gmproto.IncomingRPCMessage{ResponseID: "response-second", BugleRoute: gmproto.BugleRoute(999)}})
	if err != nil {
		t.Fatal(err)
	}
	wire := append([]byte("[["), first...)
	wire = append(wire, ',')
	wire = append(wire, second...)
	wire = append(wire, ']', ']')
	for split := 1; split < len(wire); split++ {
		client := NewClient(NewAuthData(), nil, zerolog.Nop())
		var rawFrames [][]byte
		client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
			rawFrames = append(rawFrames, append([]byte(nil), envelope.Raw...))
			return DurableOutcomeCommitted, nil
		})
		reader := &retainingSequenceReadCloser{chunks: [][]byte{wire[:split], wire[split:]}}
		if clean := client.readLongPollContext(context.Background(), ptrLogger(zerolog.Nop()), reader, false); !clean {
			t.Fatalf("split %d did not close cleanly", split)
		}
		if len(rawFrames) != 2 || !bytes.Equal(rawFrames[0], first) || !bytes.Equal(rawFrames[1], second) {
			t.Fatalf("split %d frames = %q", split, rawFrames)
		}
	}
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	var got []byte
	client.SetDurableEnvelopeHandler(func(_ context.Context, envelope DurableEnvelope) (DurableOutcome, error) {
		got = append([]byte(nil), envelope.Raw...)
		return DurableOutcomeCommitted, nil
	})
	oneByte := &maxReadCloser{reader: bytes.NewReader(append(append(append([]byte("[["), first...), ']'), ']')), maximum: 1}
	if clean := client.readLongPollContext(context.Background(), ptrLogger(zerolog.Nop()), oneByte, false); !clean || !bytes.Equal(got, first) {
		t.Fatalf("one-byte reads = clean %v raw %q", clean, got)
	}
}

func TestLongPollParserRejectsPartialEOFAndOversizedFrame(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	partial := &maxReadCloser{reader: bytes.NewReader([]byte(`[[[{"partial":true}`)), maximum: 3}
	if clean := client.readLongPollContext(context.Background(), ptrLogger(zerolog.Nop()), partial, false); clean {
		t.Fatal("partial frame EOF accepted as clean")
	}
	oversized := append([]byte("[[["), bytes.Repeat([]byte(" "), maxLongPollEnvelopeBytes+1)...)
	if clean := client.readLongPollContext(context.Background(), ptrLogger(zerolog.Nop()), io.NopCloser(bytes.NewReader(oversized)), false); clean {
		t.Fatal("oversized fragmented frame accepted")
	}
}

func TestLongPollScannerProcessesNearLimitTinyChunksLinearly(t *testing.T) {
	frame := append([]byte(`{"value":"`), bytes.Repeat([]byte{'a'}, maxLongPollEnvelopeBytes-32)...)
	frame = append(frame, []byte(`"}`)...)
	started := time.Now()
	emitted := 0
	scanner := longPollFrameScanner{}
	for index := range frame {
		if err := scanner.Feed(frame[index:index+1], func(raw []byte) error {
			emitted++
			if !bytes.Equal(raw, frame) {
				t.Fatal("scanner changed exact provider frame")
			}
			return nil
		}); err != nil {
			t.Fatalf("Feed byte %d: %v", index, err)
		}
	}
	if emitted != 1 {
		t.Fatalf("emitted frames = %d", emitted)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("near-limit one-byte scan took %s; scanner may be superlinear", elapsed)
	}
}

type retainingSequenceReadCloser struct {
	chunks [][]byte
}

func (reader *retainingSequenceReadCloser) Read(destination []byte) (int, error) {
	if len(reader.chunks) == 0 {
		return 0, io.EOF
	}
	count := copy(destination, reader.chunks[0])
	reader.chunks[0] = reader.chunks[0][count:]
	if len(reader.chunks[0]) == 0 {
		reader.chunks = reader.chunks[1:]
	}
	return count, nil
}
func (*retainingSequenceReadCloser) Close() error { return nil }

type maxReadCloser struct {
	reader  *bytes.Reader
	maximum int
}

func (reader *maxReadCloser) Read(destination []byte) (int, error) {
	if len(destination) > reader.maximum {
		destination = destination[:reader.maximum]
	}
	return reader.reader.Read(destination)
}
func (*maxReadCloser) Close() error { return nil }

type sequenceReadCloser struct{ chunks [][]byte }

func (reader *sequenceReadCloser) Read(destination []byte) (int, error) {
	if len(reader.chunks) == 0 {
		return 0, io.EOF
	}
	chunk := reader.chunks[0]
	reader.chunks = reader.chunks[1:]
	return copy(destination, chunk), nil
}

func (*sequenceReadCloser) Close() error { return nil }

func ptrLogger(logger zerolog.Logger) *zerolog.Logger { return &logger }

func TestDurableEnvelopePersistenceFailureSuppressesACKAndProjection(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.SetDurableEnvelopeHandler(func(context.Context, DurableEnvelope) (DurableOutcome, error) {
		return DurableOutcomeUnknown, errors.New("database down")
	})
	events := 0
	client.SetEventHandler(func(any) { events++ })
	client.HandleRPCMsgContext(context.Background(), &gmproto.IncomingRPCMessage{ResponseID: "response-a", BugleRoute: gmproto.BugleRoute(999)})
	client.sessionHandler.ackMapLock.Lock()
	acks := len(client.sessionHandler.ackMap)
	client.sessionHandler.ackMapLock.Unlock()
	if acks != 0 || events != 0 {
		t.Fatalf("ACKs=%d projected events=%d", acks, events)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestFailedACKBatchIsRequeuedWithoutLoss(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network unavailable")
	})}
	client.sessionHandler.queueMessageAck("response-a")
	client.sessionHandler.queueMessageAck("response-b")
	client.sessionHandler.sendAckRequestContext(context.Background())
	client.sessionHandler.ackMapLock.Lock()
	acks := append([]string(nil), client.sessionHandler.ackMap...)
	client.sessionHandler.ackMapLock.Unlock()
	if len(acks) != 2 || acks[0] != "response-a" || acks[1] != "response-b" {
		t.Fatalf("ACKs after failure = %v", acks)
	}
}

func TestDurableACKCoordinatorFiltersStaleQueuedConflictBeforeProviderIO(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	providerCalls := 0
	client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		providerCalls++
		return nil, errors.New("provider ACK must not run")
	})}
	client.SetACKCoordinator(func(_ context.Context, candidates []string, _ ACKBatchSender) (ACKCoordinationResult, error) {
		if !reflect.DeepEqual(candidates, []string{"response-a"}) {
			t.Fatalf("ACK candidates = %v", candidates)
		}
		// The durable reservation became conflicted after response A was
		// queued in memory, so the coordinator admits nothing.
		return ACKCoordinationResult{}, nil
	})
	client.sessionHandler.queueMessageAck("response-a")
	client.sessionHandler.sendAckRequestContext(context.Background())
	if providerCalls != 0 {
		t.Fatalf("stale conflicted ACK reached provider %d times", providerCalls)
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 0 || len(client.sessionHandler.ackSet) != 0 {
		t.Fatalf("durably filtered ACK remained queued: deque=%v set=%v", client.sessionHandler.ackMap, client.sessionHandler.ackSet)
	}
}

func TestDurableACKCoordinatorProviderFailureRetriesOnceWithoutTerminalFailure(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	providerCalls := 0
	providerErr := errors.New("provider ACK unavailable")
	client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		providerCalls++
		return nil, providerErr
	})}
	failures := make(chan error, 1)
	client.SetDurableFailureObserver(func(err error) { failures <- err })
	client.SetACKCoordinator(func(ctx context.Context, candidates []string, send ACKBatchSender) (ACKCoordinationResult, error) {
		err := send(ctx, candidates)
		return ACKCoordinationResult{
			AdmittedIDs: append([]string(nil), candidates...), RetryIDs: append([]string(nil), candidates...), ProviderError: err,
		}, nil
	})
	client.sessionHandler.queueMessageAck("response-a")
	client.sessionHandler.sendAckRequestContext(context.Background())
	if providerCalls != 1 {
		t.Fatalf("provider ACK attempts = %d, want one no-retry request", providerCalls)
	}
	select {
	case err := <-failures:
		t.Fatalf("provider transport failure became durable terminal failure: %v", err)
	default:
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if !reflect.DeepEqual(client.sessionHandler.ackMap, []string{"response-a"}) {
		t.Fatalf("provider ACK failure retry queue = %v", client.sessionHandler.ackMap)
	}
}

func TestACKObserverRunsOnlyAfterProviderSuccess(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.http = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{ContentTypePBLite}}, Body: io.NopCloser(bytes.NewReader([]byte("[]"))), Request: request}, nil
	})}
	var mu sync.Mutex
	var observed []string
	client.SetACKObserver(func(_ context.Context, ids []string) error {
		mu.Lock()
		observed = append(observed, ids...)
		mu.Unlock()
		return nil
	})
	client.sessionHandler.queueMessageAck("response-a")
	client.sessionHandler.sendAckRequestContext(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 1 || observed[0] != "response-a" {
		t.Fatalf("observed ACKs = %v", observed)
	}
}

func TestACKSuccessBeforeLocalMarkFailureRemainsPendingForRedelivery(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.http = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{ContentTypePBLite}}, Body: io.NopCloser(bytes.NewReader([]byte("[]"))), Request: request}, nil
	})}
	client.SetACKObserver(func(context.Context, []string) error { return errors.New("database mark failed") })
	client.sessionHandler.queueMessageAck("response-a")
	client.sessionHandler.sendAckRequestContext(context.Background())
	client.sessionHandler.ackMapLock.Lock()
	pending := append([]string(nil), client.sessionHandler.ackMap...)
	client.sessionHandler.ackMapLock.Unlock()
	if len(pending) != 1 || pending[0] != "response-a" {
		t.Fatalf("pending ACKs after local mark failure = %v", pending)
	}
}

func TestACKSuccessLocalMarkFailureSignalsDurableObserverAndRetainsPending(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.http = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{ContentTypePBLite}}, Body: io.NopCloser(bytes.NewReader([]byte("[]"))), Request: request}, nil
	})}
	markFailure := errors.New("ACK repository unavailable")
	failures := make(chan error, 1)
	client.SetACKObserver(func(context.Context, []string) error { return markFailure })
	client.SetDurableFailureObserver(func(failure error) { failures <- failure })
	client.sessionHandler.queueMessageAck("response-a")
	client.sessionHandler.sendAckRequestContext(context.Background())
	select {
	case failure := <-failures:
		if !errors.Is(failure, ErrDurablePersistence) || !errors.Is(failure, markFailure) {
			t.Fatalf("ACK mark observer failure = %v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("persistent ACK mark failure was only logged")
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 1 || client.sessionHandler.ackMap[0] != "response-a" {
		t.Fatalf("pending ACKs after signaled local mark failure = %v", client.sessionHandler.ackMap)
	}
}

func TestACKQueueBatchesTwoHundredFiftySevenWithoutLossOrLivelock(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	providerCalls := 0
	client.http = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		providerCalls++
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{ContentTypePBLite}}, Body: io.NopCloser(bytes.NewReader([]byte("[]"))), Request: request}, nil
	})}
	var batches [][]string
	client.SetACKObserver(func(_ context.Context, ids []string) error {
		batches = append(batches, append([]string(nil), ids...))
		return nil
	})
	for index := 0; index < 257; index++ {
		client.sessionHandler.queueMessageAck(fmt.Sprintf("response-%03d", index))
	}
	client.sessionHandler.sendAckRequestContext(context.Background())
	client.sessionHandler.sendAckRequestContext(context.Background())
	if providerCalls != 2 || len(batches) != 2 || len(batches[0]) != 256 || len(batches[1]) != 1 {
		t.Fatalf("provider calls=%d ACK batches=%v", providerCalls, []int{len(batches[0]), len(batches[1])})
	}
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 0 || len(client.sessionHandler.ackSet) != 0 {
		t.Fatalf("ACK queue not drained: deque=%d set=%d", len(client.sessionHandler.ackMap), len(client.sessionHandler.ackSet))
	}
}

func TestFailedFirstACKBatchRetainsBatchAheadOfRemainder(t *testing.T) {
	client := NewClient(NewAuthData(), nil, zerolog.Nop())
	client.http = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network unavailable")
	})}
	for index := 0; index < 300; index++ {
		client.sessionHandler.queueMessageAck(fmt.Sprintf("response-%03d", index))
	}
	client.sessionHandler.sendAckRequestContext(context.Background())
	client.sessionHandler.ackMapLock.Lock()
	defer client.sessionHandler.ackMapLock.Unlock()
	if len(client.sessionHandler.ackMap) != 300 || len(client.sessionHandler.ackSet) != 300 ||
		client.sessionHandler.ackMap[0] != "response-000" || client.sessionHandler.ackMap[299] != "response-299" {
		t.Fatalf("failed ACK queue = first=%q last=%q deque=%d set=%d", client.sessionHandler.ackMap[0], client.sessionHandler.ackMap[len(client.sessionHandler.ackMap)-1], len(client.sessionHandler.ackMap), len(client.sessionHandler.ackSet))
	}
}

func TestMutatingSendDoesNotTransparentlyRetryProviderFiveHundreds(t *testing.T) {
	for name, invoke := range map[string]func(*Client) error{
		"send message": func(client *Client) error {
			_, err := client.SendMessage(context.Background(), &gmproto.SendMessageRequest{})
			return err
		},
		"get or create conversation": func(client *Client) error {
			_, err := client.GetOrCreateConversation(context.Background(), &gmproto.GetOrCreateConversationRequest{})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := NewClient(NewAuthData(), nil, zerolog.Nop())
			calls := 0
			client.http = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{"Content-Type": []string{"text/plain"}}, Body: io.NopCloser(bytes.NewReader([]byte("provider failure"))), Request: request}, nil
			})}
			if err := invoke(client); err == nil {
				t.Fatal("mutating request unexpectedly succeeded")
			}
			if calls != 1 {
				t.Fatalf("provider calls = %d, want 1", calls)
			}
		})
	}
}
