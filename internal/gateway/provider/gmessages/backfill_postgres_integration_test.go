//go:build postgres_integration

package gmessages

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/rs/zerolog"
	"go.mau.fi/util/pblite"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/internal/gateway/connectionactor"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/store/postgres"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type postgresIntegrationSealer struct{}

func (postgresIntegrationSealer) Seal(context.Context, session.Scope, []byte) (session.Envelope, error) {
	return session.Envelope{}, nil
}

type postgresBackfillProvider struct{ client *libgm.Client }

func (*postgresBackfillProvider) Connect(context.Context) error    { return nil }
func (*postgresBackfillProvider) Disconnect(context.Context) error { return nil }
func (provider *postgresBackfillProvider) gatewayBackfillClient() gatewayBackfillClient {
	return provider.client
}
func (provider *postgresBackfillProvider) gatewayMessagingClient() gatewayMessagingClient {
	return provider.client
}

type postgresNoMedia struct{}

func (postgresNoMedia) Open(context.Context, domain.TenantID, domain.MediaID) (io.ReadCloser, media.Record, error) {
	return nil, media.Record{}, media.ErrNotFound
}

type postgresBackfillExecutor struct {
	ownership connectionactor.ProviderOwnership
	provider  connectionactor.Provider
}

func (executor postgresBackfillExecutor) Execute(
	ctx context.Context, key connectionactor.Key, operation connectionactor.ProviderOperation,
) error {
	if key != executor.ownership.Key {
		return connectionactor.ErrProviderUnavailable
	}
	return operation(connectionactor.ContextWithProviderOwnership(ctx, executor.ownership), executor.provider)
}

type postgresBackfillTransport struct {
	mu                   sync.Mutex
	client               *libgm.Client
	messageCalls         int
	messageDeliveries    int
	secondMessageRequest *gmproto.ListMessagesRequest
	rpcCalls             int
	actions              []gmproto.ActionType
	sendStatuses         []gmproto.SendMessageResponse_Status
}

func (transport *postgresBackfillTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read provider request: %w", err)
	}
	var outgoing gmproto.OutgoingRPCMessage
	if err = pblite.Unmarshal(body, &outgoing); err != nil {
		return nil, fmt.Errorf("decode provider request: %w", err)
	}
	var rpcData gmproto.OutgoingRPCData
	if err = proto.Unmarshal(outgoing.GetData().GetMessageData(), &rpcData); err != nil {
		return nil, fmt.Errorf("decode provider request metadata: %w", err)
	}
	plaintext, err := transport.client.AuthData.RequestCrypto.Decrypt(rpcData.GetEncryptedProtoData())
	if err != nil {
		return nil, fmt.Errorf("decrypt provider request: %w", err)
	}

	transport.mu.Lock()
	action := rpcData.GetAction()
	transport.actions = append(transport.actions, action)
	responseID := "worker-conversations"
	var response proto.Message
	deliveries := 1
	switch action {
	case gmproto.ActionType_GET_CONVERSATION:
		var getRequest gmproto.GetConversationRequest
		if err = proto.Unmarshal(plaintext, &getRequest); err != nil {
			transport.mu.Unlock()
			return nil, fmt.Errorf("decode get-conversation request: %w", err)
		}
		transport.rpcCalls++
		responseID = fmt.Sprintf("worker-rpc-%d", transport.rpcCalls)
		response = &gmproto.GetConversationResponse{Conversation: &gmproto.Conversation{
			ConversationID: getRequest.GetConversationID(), DefaultOutgoingID: "outgoing-default",
		}}
	case gmproto.ActionType_GET_OR_CREATE_CONVERSATION:
		var createRequest gmproto.GetOrCreateConversationRequest
		if err = proto.Unmarshal(plaintext, &createRequest); err != nil {
			transport.mu.Unlock()
			return nil, fmt.Errorf("decode get-or-create request: %w", err)
		}
		if len(createRequest.GetNumbers()) != 1 || createRequest.GetNumbers()[0].GetNumber() != "+12025550123" {
			transport.mu.Unlock()
			return nil, errors.New("new-conversation request did not preserve the E.164 recipient")
		}
		transport.rpcCalls++
		responseID = fmt.Sprintf("worker-rpc-%d", transport.rpcCalls)
		response = &gmproto.GetOrCreateConversationResponse{
			Status:       gmproto.GetOrCreateConversationResponse_SUCCESS,
			Conversation: &gmproto.Conversation{ConversationID: "conversation-created", DefaultOutgoingID: "outgoing-default"},
		}
	case gmproto.ActionType_SEND_MESSAGE:
		var sendRequest gmproto.SendMessageRequest
		if err = proto.Unmarshal(plaintext, &sendRequest); err != nil {
			transport.mu.Unlock()
			return nil, fmt.Errorf("decode send request: %w", err)
		}
		if !domain.ValidProviderConversationID(sendRequest.GetConversationID()) || sendRequest.GetTmpID() == "" {
			transport.mu.Unlock()
			return nil, errors.New("send request omitted its durable route or temporary ID")
		}
		transport.rpcCalls++
		responseID = fmt.Sprintf("worker-rpc-%d", transport.rpcCalls)
		status := gmproto.SendMessageResponse_SUCCESS
		if len(transport.sendStatuses) > 0 {
			status = transport.sendStatuses[0]
			transport.sendStatuses = transport.sendStatuses[1:]
		}
		response = &gmproto.SendMessageResponse{Status: status}
	case gmproto.ActionType_LIST_CONVERSATIONS:
		var listRequest gmproto.ListConversationsRequest
		if err = proto.Unmarshal(plaintext, &listRequest); err != nil {
			transport.mu.Unlock()
			return nil, fmt.Errorf("decode conversation request: %w", err)
		}
		response = &gmproto.ListConversationsResponse{
			Conversations: []*gmproto.Conversation{{ConversationID: "conversation-worker"}},
			Cursor:        &gmproto.Cursor{LastItemID: "conversation-page-1", LastItemTimestamp: 1724400000040},
		}
	case gmproto.ActionType_LIST_MESSAGES:
		var listRequest gmproto.ListMessagesRequest
		if err = proto.Unmarshal(plaintext, &listRequest); err != nil {
			transport.mu.Unlock()
			return nil, fmt.Errorf("decode message request: %w", err)
		}
		transport.messageCalls++
		switch transport.messageCalls {
		case 1:
			responseID = "worker-empty-message-page"
			response = &gmproto.ListMessagesResponse{Cursor: &gmproto.Cursor{
				LastItemID: "message-page-1", LastItemTimestamp: 1724400000041,
			}}
		case 2:
			responseID = "worker-poison-message-page"
			transport.secondMessageRequest = proto.Clone(&listRequest).(*gmproto.ListMessagesRequest)
			response = &gmproto.ListMessagesResponse{
				Messages: []*gmproto.Message{{MessageID: "message-crossed", ConversationID: "conversation-other"}},
				Cursor:   &gmproto.Cursor{LastItemID: "message-page-2", LastItemTimestamp: 1724400000042},
			}
			deliveries = 2
		default:
			responseID = "worker-stale-fence"
			response = &gmproto.ListMessagesResponse{}
		}
	default:
		transport.mu.Unlock()
		return nil, fmt.Errorf("unexpected provider action %s", action)
	}
	transport.messageDeliveries += deliveries
	transport.mu.Unlock()

	responseBytes, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("encode provider response: %w", err)
	}
	encrypted, err := transport.client.AuthData.RequestCrypto.Encrypt(responseBytes)
	if err != nil {
		return nil, fmt.Errorf("encrypt provider response: %w", err)
	}
	rpcBytes, err := proto.Marshal(&gmproto.RPCMessageData{
		SessionID: rpcData.GetRequestID(), Action: action, EncryptedData: encrypted,
	})
	if err != nil {
		return nil, fmt.Errorf("encode provider response metadata: %w", err)
	}
	incoming := &gmproto.IncomingRPCMessage{
		ResponseID: responseID, BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: rpcBytes,
	}
	for index := 0; index < deliveries; index++ {
		transport.client.HandleRPCMsgContext(request.Context(), incoming)
	}
	httpBody, err := pblite.Marshal(&gmproto.OutgoingRPCResponse{})
	if err != nil {
		return nil, fmt.Errorf("encode provider HTTP response: %w", err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{libgm.ContentTypePBLite}},
		Body:       io.NopCloser(bytes.NewReader(httpBody)),
		Request:    request,
	}, nil
}

func TestPostgresIntegrationBackfillCursorRoutingAndDurablePoisonOutcome(t *testing.T) {
	adminDSN := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if adminDSN == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	adminDB, err := sql.Open("postgres", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer adminDB.Close()
	schema := fmt.Sprintf("sirenaix_backfill_it_%d", time.Now().UnixNano())
	if _, err = adminDB.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = adminDB.ExecContext(context.Background(), "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE")
	})
	parsed, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entries, err := postgres.Migrations.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		contents, readErr := postgres.Migrations.ReadFile("migrations/" + entry.Name())
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, err = db.ExecContext(ctx, string(contents)); err != nil {
			t.Fatalf("apply %s: %v", entry.Name(), err)
		}
	}
	repository, err := postgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	const tenantID, connectionID = domain.TenantID("tenant-backfill"), domain.ConnectionID("connection-backfill")
	if err = repository.SaveTenant(ctx, domain.Tenant{ID: tenantID, Name: "Backfill tenant"}); err != nil {
		t.Fatal(err)
	}
	if err = repository.SaveConnection(ctx, tenantID, postgres.ConnectionRecord{Connection: domain.Connection{
		ID: connectionID, TenantID: tenantID, State: domain.ConnectionStateConnected,
	}, ProviderDeviceFingerprint: bytes.Repeat([]byte{7}, 32)}); err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := repository.AcquireConnectionLease(ctx, tenantID, connectionID, "actor-owner", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("lease = (%+v, %v, %v)", lease, acquired, err)
	}
	inbox, _ := ingress.NewService(repository)
	sink, _ := NewDurableSink(DurableSinkConfig{Inbox: inbox, ACKs: repository, Sealer: postgresIntegrationSealer{}})
	ownership := connectionactor.ProviderOwnership{
		Key: connectionactor.Key{TenantID: tenantID, ConnectionID: connectionID}, OwnerID: "actor-owner", FencingToken: lease.FencingToken,
	}

	emptyCursor := &gmproto.Cursor{LastItemID: "empty-boundary", LastItemTimestamp: 1724400000024}
	outcome, err := sink.PersistEnvelopeOutcome(ctx, ownership, libgm.DurableEnvelope{
		ResponseID: "response-empty", Raw: []byte("response-empty"),
		Request: libgm.DurableRequest{Action: gmproto.ActionType_LIST_MESSAGES, ConversationID: "conversation-empty"},
		Decoded: &libgm.IncomingRPCMessage{PayloadSource: libgm.PayloadSourceEncryptedData, DecryptedMessage: &gmproto.ListMessagesResponse{Cursor: emptyCursor}},
	})
	if err != nil || outcome != libgm.DurableOutcomeCommitted {
		t.Fatalf("empty page = (%v, %v)", outcome, err)
	}
	wantCursor, _ := proto.MarshalOptions{Deterministic: true}.Marshal(emptyCursor)
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, "conversation-empty"); cursorErr != nil || !bytes.Equal(cursor, wantCursor) {
		t.Fatalf("empty page child cursor = %x, %v", cursor, cursorErr)
	}
	if parent, parentErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, providerPageCursorID); parentErr != nil || len(parent) != 0 {
		t.Fatalf("empty page changed parent cursor = %x, %v", parent, parentErr)
	}

	seed, err := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantID, ConnectionID: connectionID, OwnerID: ownership.OwnerID, FencingToken: ownership.FencingToken,
		ProviderResponseID: "seed-attachment", Raw: []byte("seed-attachment"),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{ProviderMessageID: "message-media", ConversationID: "conversation-poison"}}},
		Media:      []ingress.MediaLocator{{ProviderMessageID: "message-media", Locator: "gmessages:original", Position: 0, MIMEType: "image/png", DeclaredSize: 7}},
	})
	if err != nil || !seed.ACKEligible {
		t.Fatalf("seed attachment = (%+v, %v)", seed, err)
	}
	poisonEnvelope := libgm.DurableEnvelope{
		ResponseID: "response-attachment-poison", Raw: []byte("response-attachment-poison"),
		Request: libgm.DurableRequest{Action: gmproto.ActionType_LIST_MESSAGES, ConversationID: "conversation-poison"},
		Decoded: &libgm.IncomingRPCMessage{PayloadSource: libgm.PayloadSourceEncryptedData, DecryptedMessage: &gmproto.ListMessagesResponse{
			Messages: []*gmproto.Message{{
				MessageID: "message-media", ConversationID: "conversation-poison",
				MessageInfo: []*gmproto.MessageInfo{{Data: &gmproto.MessageInfo_MediaContent{MediaContent: &gmproto.MediaContent{
					MediaID: "changed", MimeType: "image/png", Size: 7,
				}}}},
			}},
			Cursor: &gmproto.Cursor{LastItemID: "poison-boundary", LastItemTimestamp: 1724400000030},
		}},
	}
	for attempt := 0; attempt < 2; attempt++ {
		outcome, err = sink.PersistEnvelopeOutcome(ctx, ownership, poisonEnvelope)
		expected := libgm.DurableOutcomePoisoned
		if attempt > 0 {
			expected = libgm.DurableOutcomeDuplicatePoisoned
		}
		if err != nil || outcome != expected {
			t.Fatalf("poison attempt %d = (%v, %v)", attempt, outcome, err)
		}
	}
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, "conversation-poison"); cursorErr != nil || len(cursor) != 0 {
		t.Fatalf("attachment poison advanced child = %x, %v", cursor, cursorErr)
	}
	if parent, parentErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, providerPageCursorID); parentErr != nil || len(parent) != 0 {
		t.Fatalf("attachment poison advanced parent = %x, %v", parent, parentErr)
	}

	crossOutcome, err := sink.PersistEnvelopeOutcome(ctx, ownership, libgm.DurableEnvelope{
		ResponseID: "response-cross-conversation", Raw: []byte("response-cross-conversation"),
		Request: libgm.DurableRequest{Action: gmproto.ActionType_LIST_MESSAGES, ConversationID: "conversation-requested"},
		Decoded: &libgm.IncomingRPCMessage{PayloadSource: libgm.PayloadSourceEncryptedData, DecryptedMessage: &gmproto.ListMessagesResponse{
			Messages: []*gmproto.Message{{MessageID: "message-wrong", ConversationID: "conversation-other"}},
			Cursor:   &gmproto.Cursor{LastItemID: "wrong-boundary", LastItemTimestamp: 1724400000031},
		}},
	})
	if err != nil || crossOutcome != libgm.DurableOutcomePoisoned {
		t.Fatalf("cross-conversation page = (%v, %v)", crossOutcome, err)
	}
	if cursor, cursorErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, "conversation-requested"); cursorErr != nil || len(cursor) != 0 {
		t.Fatalf("cross-conversation poison advanced target = %x, %v", cursor, cursorErr)
	}

	client := libgm.NewClient(libgm.NewAuthData(), nil, zerolog.Nop())
	transport := &postgresBackfillTransport{client: client, sendStatuses: []gmproto.SendMessageResponse_Status{
		gmproto.SendMessageResponse_SUCCESS, gmproto.SendMessageResponse_SUCCESS, gmproto.SendMessageResponse_UNKNOWN,
	}}
	client.SetPostgresIntegrationTransport(transport)
	client.SetDurableEnvelopeHandler(func(handlerCtx context.Context, envelope libgm.DurableEnvelope) (libgm.DurableOutcome, error) {
		return sink.PersistEnvelopeOutcome(handlerCtx, ownership, envelope)
	})
	provider := &postgresBackfillProvider{client: client}
	executor := postgresBackfillExecutor{ownership: ownership, provider: provider}

	if result, seedErr := inbox.Process(ctx, ingress.Envelope{
		TenantID: tenantID, ConnectionID: connectionID, OwnerID: ownership.OwnerID, FencingToken: ownership.FencingToken,
		ProviderResponseID: "seed-existing-conversation", Raw: []byte("seed-existing-conversation"),
		Projection: ingress.Projection{Conversations: []ingress.ProjectedConversation{{
			ConversationID: "conversation-existing", DefaultOutgoingID: "outgoing-default",
		}}},
	}); seedErr != nil || !result.ACKEligible {
		t.Fatalf("seed existing conversation = (%+v, %v)", result, seedErr)
	}
	commandIDs := []string{"message-existing", "message-created", "message-ambiguous"}
	commands, err := messaging.NewService(messaging.Config{
		Store: repository,
		NewID: func() string {
			id := commandIDs[0]
			commandIDs = commandIDs[1:]
			return id
		},
		Now: func() time.Time { return time.Unix(1724400000, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	existingMessage, err := commands.Submit(ctx, tenantID, "idem-existing", messaging.SendInput{
		ConnectionID: connectionID, ConversationID: "conversation-existing", Text: "existing route",
	})
	if err != nil {
		t.Fatal(err)
	}
	createdMessage, err := commands.Submit(ctx, tenantID, "idem-created", messaging.SendInput{
		ConnectionID: connectionID, Recipient: "+12025550123", RouteMode: messaging.RouteModePhoneDefault, Text: "new route",
	})
	if err != nil {
		t.Fatal(err)
	}
	createdClaim, claimed, err := repository.ClaimNext(ctx, messaging.LaneKey{
		TenantID: tenantID, ConnectionID: connectionID, ConversationID: "new:+12025550123",
	}, ownership.OwnerID)
	if err != nil || !claimed {
		t.Fatalf("claim created-conversation message = (%+v, %v, %v)", createdClaim, claimed, err)
	}
	if owned, beginErr := repository.BeginProviderIO(ctx, createdClaim, ownership.OwnerID); beginErr != nil || !owned {
		t.Fatalf("begin created-conversation provider I/O = (%v, %v)", owned, beginErr)
	}
	createdMessage = createdClaim.Message
	sender, err := NewActorSender(ActorSenderConfig{Executor: executor, Lines: repository, Media: postgresNoMedia{}, Routes: repository})
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name    string
		message messaging.OutboundMessage
	}{{"existing", existingMessage}, {"new", createdMessage}} {
		result, sendErr := sender.SendOnce(ctx, messaging.ProviderSendCommand{Message: fixture.message, FencingToken: ownership.FencingToken})
		if sendErr != nil || !result.Accepted {
			t.Fatalf("%s ActorSender durable provider path = (%+v, %v)", fixture.name, result, sendErr)
		}
	}
	ambiguousMessage, err := commands.Submit(ctx, tenantID, "idem-ambiguous", messaging.SendInput{
		ConnectionID: connectionID, ConversationID: "conversation-existing", Text: "unknown relay response",
	})
	if err != nil {
		t.Fatal(err)
	}
	ambiguousResult, err := sender.SendOnce(ctx, messaging.ProviderSendCommand{Message: ambiguousMessage, FencingToken: ownership.FencingToken})
	if err != nil || ambiguousResult.Accepted || ambiguousResult.FailureReason != "" {
		t.Fatalf("unknown durable send response was not preserved as ambiguous: result=%+v error=%v", ambiguousResult, err)
	}
	storedCreated, err := repository.GetMessage(ctx, tenantID, createdMessage.ID)
	if err != nil || storedCreated.ConversationID != "conversation-created" {
		t.Fatalf("created conversation route = (%+v, %v)", storedCreated, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "SELECT set_config('sirenaix.tenant_id', $1, true)", string(tenantID)); err != nil {
		t.Fatal(err)
	}
	var durableRPCs, poisonedRPCs int
	if err = tx.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE poisoned)
        FROM provider_inbox WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id LIKE 'worker-rpc-%'`, tenantID, connectionID).
		Scan(&durableRPCs, &poisonedRPCs); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if durableRPCs != 6 || poisonedRPCs != 0 {
		t.Fatalf("durable correlated RPC rows = %d poisoned=%d, want six clean Get/GetOrCreate/Send responses", durableRPCs, poisonedRPCs)
	}
	transport.mu.Lock()
	wantActions := []gmproto.ActionType{gmproto.ActionType_GET_CONVERSATION, gmproto.ActionType_SEND_MESSAGE,
		gmproto.ActionType_GET_OR_CREATE_CONVERSATION, gmproto.ActionType_SEND_MESSAGE,
		gmproto.ActionType_GET_CONVERSATION, gmproto.ActionType_SEND_MESSAGE}
	gotActions := append([]gmproto.ActionType(nil), transport.actions...)
	transport.mu.Unlock()
	if !reflect.DeepEqual(gotActions, wantActions) {
		t.Fatalf("ActorSender provider actions = %v, want %v", gotActions, wantActions)
	}
	transport.mu.Lock()
	deliveriesBeforeWorker := transport.messageDeliveries
	transport.mu.Unlock()

	workerConfig := ActorBackfillWorkerConfig{
		Executor: executor,
		Cursors:  repository, Checkpoints: repository, ConversationPageSize: 10, MessagePageSize: 10,
	}
	worker, err := NewActorBackfillWorker(workerConfig)
	if err != nil {
		t.Fatal(err)
	}
	if processed, runErr := worker.RunConnection(ctx, ownership.Key); runErr != nil || !processed {
		t.Fatalf("stage worker checkpoint = (%v, %v)", processed, runErr)
	}
	checkpoint, err := repository.LoadBackfillCheckpoint(ctx, tenantID, connectionID)
	if err != nil || checkpoint == nil || len(checkpoint.Items) != 1 || checkpoint.Items[0].ConversationID != "conversation-worker" || checkpoint.Items[0].State != "pending" {
		t.Fatalf("staged worker checkpoint = (%+v, %v)", checkpoint, err)
	}
	if parent, parentErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, providerPageCursorID); parentErr != nil || len(parent) != 0 {
		t.Fatalf("staged worker checkpoint advanced parent = %x, %v", parent, parentErr)
	}
	if processed, runErr := worker.RunConnection(ctx, ownership.Key); runErr != nil || !processed {
		t.Fatalf("commit empty child page = (%v, %v)", processed, runErr)
	}
	wantWorkerCursor, _ := proto.MarshalOptions{Deterministic: true}.Marshal(&gmproto.Cursor{
		LastItemID: "message-page-1", LastItemTimestamp: 1724400000041,
	})
	if child, childErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, "conversation-worker"); childErr != nil || !bytes.Equal(child, wantWorkerCursor) {
		t.Fatalf("worker child cursor = %x, %v", child, childErr)
	}
	if parent, parentErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, providerPageCursorID); parentErr != nil || len(parent) != 0 {
		t.Fatalf("empty worker page advanced parent = %x, %v", parent, parentErr)
	}

	// Reconstructing the worker simulates a process restart. The next request
	// must resume from the committed child cursor and an exact poison
	// redelivery must converge without advancing either checkpoint.
	worker, err = NewActorBackfillWorker(workerConfig)
	if err != nil {
		t.Fatal(err)
	}
	if processed, runErr := worker.RunConnection(ctx, ownership.Key); runErr != nil || !processed {
		t.Fatalf("persist exact-redelivered child poison = (%v, %v)", processed, runErr)
	}
	checkpoint, err = repository.LoadBackfillCheckpoint(ctx, tenantID, connectionID)
	if err != nil || checkpoint == nil || len(checkpoint.Items) != 1 || checkpoint.Items[0].State != "poisoned" {
		t.Fatalf("poisoned worker checkpoint = (%+v, %v)", checkpoint, err)
	}
	if processed, runErr := worker.RunConnection(ctx, ownership.Key); !errors.Is(runErr, messaging.ErrBackfillPoisoned) || processed {
		t.Fatalf("blocked poisoned checkpoint = (%v, %v)", processed, runErr)
	}
	transport.mu.Lock()
	secondRequest := transport.secondMessageRequest
	deliveries := transport.messageDeliveries
	transport.mu.Unlock()
	if secondRequest == nil || !proto.Equal(secondRequest.GetCursor(), &gmproto.Cursor{
		LastItemID: "message-page-1", LastItemTimestamp: 1724400000041,
	}) {
		t.Fatalf("restart message request cursor = %+v", secondRequest)
	}
	if deliveries-deliveriesBeforeWorker != 4 {
		t.Fatalf("provider response deliveries = %d after baseline %d, want conversation, empty, plus two exact poison deliveries", deliveries, deliveriesBeforeWorker)
	}
	if child, childErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, "conversation-worker"); childErr != nil || !bytes.Equal(child, wantWorkerCursor) {
		t.Fatalf("poison changed worker child cursor = %x, %v", child, childErr)
	}
	if parent, parentErr := repository.LoadCommittedCursor(ctx, tenantID, connectionID, providerPageCursorID); parentErr != nil || len(parent) != 0 {
		t.Fatalf("poison changed worker parent cursor = %x, %v", parent, parentErr)
	}

	if err = repository.QuarantineBackfillConnection(ctx, tenantID, connectionID, "provider-protocol"); err != nil {
		t.Fatalf("persist connection-local quarantine: %v", err)
	}
	health, err := repository.GetConnectionHealth(ctx, tenantID, connectionID)
	if err != nil {
		t.Fatal(err)
	}
	if health.ConnectionState != domain.ConnectionStateSuspended || health.ActorState != "stopped" ||
		health.LeaseState != "inactive" || health.LastSafeReason != "provider-protocol" || health.FencingToken <= ownership.FencingToken {
		t.Fatalf("persisted quarantine health = %+v", health)
	}
	if renewed, renewErr := repository.RenewConnectionLease(ctx, tenantID, connectionID, ownership.OwnerID, ownership.FencingToken, time.Minute); renewErr != nil || renewed {
		t.Fatalf("quarantined connection renewed stale lease = (%v, %v)", renewed, renewErr)
	}
	if ackErr := sink.MarkACKed(ctx, ownership, []string{"worker-empty-message-page"}); !errors.Is(ackErr, ErrDurableFenceLost) {
		t.Fatalf("quarantined connection accepted stale fenced ACK = %v", ackErr)
	}
	_, fetchErr := client.FetchMessagesDurable(ctx, "conversation-after-quarantine", 1, nil)
	if !errors.Is(fetchErr, libgm.ErrDurablePersistence) || !errors.Is(fetchErr, postgres.ErrConnectionLeaseLost) {
		t.Fatalf("stale provider response durable cause = %v", fetchErr)
	}
}
