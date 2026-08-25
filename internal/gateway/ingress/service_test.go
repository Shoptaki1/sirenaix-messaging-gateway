package ingress_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type inboxStore struct {
	result  ingress.CommitResult
	err     error
	records []ingress.EnvelopeRecord
}

func TestCanonicalProviderCursorUsesSemanticTupleNotWireEncoding(t *testing.T) {
	cursor := &gmproto.Cursor{LastItemID: "conversation-a", LastItemTimestamp: 1700000000}
	canonicalWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(cursor)
	if err != nil {
		t.Fatal(err)
	}
	alternateWire := protowire.AppendTag(nil, 2, protowire.VarintType)
	alternateWire = protowire.AppendVarint(alternateWire, uint64(cursor.GetLastItemTimestamp()))
	alternateWire = protowire.AppendTag(alternateWire, 1, protowire.BytesType)
	alternateWire = protowire.AppendString(alternateWire, cursor.GetLastItemID())
	alternateWire = protowire.AppendTag(alternateWire, 99, protowire.VarintType)
	alternateWire = protowire.AppendVarint(alternateWire, 7)

	want, err := ingress.CanonicalProviderCursor(canonicalWire)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ingress.CanonicalProviderCursor(alternateWire)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("semantic cursor canonicalization differs: got=%x want=%x", got, want)
	}
	if err := ingress.ValidateProjection(ingress.Projection{
		Cursor: canonicalWire, CursorBase: alternateWire, CursorCandidate: canonicalWire,
		CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a",
	}, nil); err == nil {
		t.Fatal("semantically identical provider cursor was accepted as progress")
	}
}

func (store *inboxStore) CommitEnvelope(_ context.Context, record ingress.EnvelopeRecord) (ingress.CommitResult, error) {
	store.records = append(store.records, record)
	return store.result, store.err
}

func TestEnvelopeCommitsRawProjectionMediaJobsEventsAndACKPendingTogether(t *testing.T) {
	store := &inboxStore{result: ingress.CommitInserted}
	service, err := ingress.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte("provider-envelope")
	result, err := service.Process(context.Background(), ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7, ProviderResponseID: "response-a", Raw: raw,
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-message-a", ConversationID: "conversation-a", Direction: "inbound",
			Provenance: ingress.MessageProvenanceLive, ProviderStatus: "INCOMING_COMPLETE",
			ProviderOccurredAt: time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC), Actionable: true, Sender: "+12025550100",
			Text: "hello", Transport: "mms", State: domain.MessageStateDelivered,
		}}},
		Media: []ingress.MediaLocator{{ProviderMessageID: "provider-message-a", Locator: "gmessages:media-a"}},
	})
	if err != nil || !result.ACKEligible {
		t.Fatalf("Process() = (%+v, %v)", result, err)
	}
	if len(store.records) != 1 {
		t.Fatalf("commits=%d, want 1", len(store.records))
	}
	record := store.records[0]
	wantDigest := sha256.Sum256(raw)
	if record.Digest != wantDigest || string(record.Raw) != string(raw) || !record.ACKPending || record.Poisoned || len(record.Projection.Messages) != 1 || len(record.Media) != 1 || len(record.Events) == 0 {
		t.Fatalf("committed record = %+v", record)
	}
	var event map[string]any
	if err = json.Unmarshal(record.Events[0].CanonicalBody, &event); err != nil {
		t.Fatal(err)
	}
	if event["event_id"] != string(record.Events[0].ID) || event["type"] != "message.received" || event["version"] != float64(1) ||
		event["tenant_id"] != "tenant-a" || event["connection_id"] != "connection-a" || event["conversation_id"] != "conversation-a" ||
		event["provider_message_id"] != "provider-message-a" || event["direction"] != "inbound" || event["sender"] != "+12025550100" ||
		event["text"] != "hello" || event["transport"] != "mms" || event["status"] != "delivered" ||
		event["provenance"] != string(ingress.MessageProvenanceLive) || event["provider_status"] != "INCOMING_COMPLETE" || event["actionable"] != true ||
		event["occurred_at"] != "2026-08-25T12:00:00Z" || event["ingested_at"] == "" {
		t.Fatalf("message event contract = %#v", event)
	}
}

func TestEnvelopeTwoAttachmentsHaveStableDistinctEventIdentityAndOrder(t *testing.T) {
	store := &inboxStore{result: ingress.CommitInserted}
	service, _ := ingress.NewService(store)
	envelope := ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: "response-two-images", Raw: []byte("exact-two-images"),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{ProviderMessageID: "provider-a", ConversationID: "conversation-a"}}},
		Media: []ingress.MediaLocator{
			{ProviderMessageID: "provider-a", Locator: "gmessages:first", Position: 0},
			{ProviderMessageID: "provider-a", Locator: "gmessages:second", Position: 1},
		},
	}
	if _, err := service.Process(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	first := store.records[0]
	if len(first.Events) != 3 || first.Events[1].ID == first.Events[2].ID || first.Events[1].AggregateID == first.Events[2].AggregateID {
		t.Fatalf("two-image events = %+v", first.Events)
	}
	store.records = nil
	if _, err := service.Process(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	second := store.records[0]
	if first.Events[1].ID != second.Events[1].ID || first.Events[2].ID != second.Events[2].ID {
		t.Fatalf("attachment identities changed across replay: first=%+v second=%+v", first.Events, second.Events)
	}
}

func TestMediaPendingIdentityDoesNotDependOnProviderResponseID(t *testing.T) {
	store := &inboxStore{result: ingress.CommitInserted}
	service, _ := ingress.NewService(store)
	base := ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: "response-a", Raw: []byte("first-envelope"),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{ProviderMessageID: "provider-a", ConversationID: "conversation-a"}}},
		Media:      []ingress.MediaLocator{{ProviderMessageID: "provider-a", Locator: "gmessages:stable", Position: 0}},
	}
	if _, err := service.Process(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	firstID := store.records[0].Events[1].ID
	base.ProviderResponseID = "response-b"
	base.Raw = []byte("second-envelope")
	if _, err := service.Process(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if secondID := store.records[1].Events[1].ID; firstID != secondID {
		t.Fatalf("same provider attachment changed event identity across envelopes: %q != %q", firstID, secondID)
	}
}

func TestMediaPendingIdentityCannotCollideThroughProviderIDDelimiters(t *testing.T) {
	store := &inboxStore{result: ingress.CommitInserted}
	service, _ := ingress.NewService(store)
	envelope := ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: "response-colons", Raw: []byte("colon-frame"),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{
			{ProviderMessageID: "a", ConversationID: "conversation-a"},
			{ProviderMessageID: "a:1", ConversationID: "conversation-a"},
		}},
		Media: []ingress.MediaLocator{
			{ProviderMessageID: "a", Locator: "gmessages:first", Position: 1},
			{ProviderMessageID: "a:1", Locator: "gmessages:second", Position: 0},
		},
	}
	if _, err := service.Process(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	events := store.records[0].Events
	if len(events) != 4 || events[2].AggregateID == events[3].AggregateID || events[2].ID == events[3].ID {
		t.Fatalf("delimiter-safe media events = %+v", events)
	}
}

func TestEnvelopeDBFailureIsNeverACKEligible(t *testing.T) {
	store := &inboxStore{err: errors.New("database unavailable")}
	service, _ := ingress.NewService(store)
	result, err := service.Process(context.Background(), ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7, ProviderResponseID: "response-a", Raw: []byte("raw"),
	})
	if err == nil || result.ACKEligible {
		t.Fatalf("Process() = (%+v, %v), want error and no ACK", result, err)
	}
}

func TestTerminalPoisonRemainsACKWithheldWhenRecoveredFromDurableState(t *testing.T) {
	store := &inboxStore{result: ingress.CommitDuplicateACKWithheld}
	service, _ := ingress.NewService(store)
	result, err := service.Process(context.Background(), ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: "settings-terminal-restart", Raw: []byte("same-settings-frame"),
	})
	if err != nil || result.ACKEligible || !result.ACKWithheld || !result.Duplicate || !result.Poisoned {
		t.Fatalf("durably terminal replay = (%+v, %v)", result, err)
	}
}

func TestProviderResponseIDBoundaryRejectsBeforeDurableStore(t *testing.T) {
	store := &inboxStore{result: ingress.CommitInserted}
	service, err := ingress.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	valid := ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: strings.Repeat("r", 256), Raw: []byte("raw"),
	}
	if result, processErr := service.Process(context.Background(), valid); processErr != nil || !result.ACKEligible {
		t.Fatalf("256-byte response ID = (%+v, %v)", result, processErr)
	}
	for _, responseID := range []string{strings.Repeat("r", 257), "response\x00id", "response\nid", " response"} {
		invalid := valid
		invalid.ProviderResponseID = responseID
		if _, processErr := service.Process(context.Background(), invalid); !errors.Is(processErr, ingress.ErrInvalidProviderResponseID) {
			t.Fatalf("invalid response ID %q error = %v", responseID, processErr)
		}
	}
	if len(store.records) != 1 {
		t.Fatalf("invalid response IDs reached durable store: commits=%d", len(store.records))
	}
}

func TestEnvelopeRestartRedeliveryConvergesButConflictingBytesNeverACK(t *testing.T) {
	store := &inboxStore{result: ingress.CommitDuplicate}
	service, _ := ingress.NewService(store)
	envelope := ingress.Envelope{TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7, ProviderResponseID: "response-a", Raw: []byte("same")}
	result, err := service.Process(context.Background(), envelope)
	if err != nil || !result.ACKEligible || !result.Duplicate {
		t.Fatalf("exact duplicate = (%+v, %v)", result, err)
	}
	store.result = ingress.CommitConflict
	result, err = service.Process(context.Background(), envelope)
	if !errors.Is(err, ingress.ErrConflictingEnvelope) || result.ACKEligible {
		t.Fatalf("conflicting duplicate = (%+v, %v)", result, err)
	}
}

func TestMalformedEnvelopeIsDurablyPoisonedBeforeACK(t *testing.T) {
	store := &inboxStore{result: ingress.CommitInserted}
	service, _ := ingress.NewService(store)
	result, err := service.Process(context.Background(), ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7, ProviderResponseID: "poison-a", Raw: []byte{0xff},
		DecodeError: errors.New("unknown envelope"),
	})
	if err != nil || !result.ACKEligible || !result.Poisoned || len(store.records) != 1 || !store.records[0].Poisoned || store.records[0].PoisonReason != "decode_failed" {
		t.Fatalf("poison result=%+v err=%v records=%+v", result, err, store.records)
	}
}

func TestRepositoryDetectedPoisonIsACKEligibleButExplicitlyNotCommittedProjection(t *testing.T) {
	store := &inboxStore{result: ingress.CommitPoisoned}
	service, _ := ingress.NewService(store)
	result, err := service.Process(context.Background(), ingress.Envelope{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: "attachment-conflict", Raw: []byte("structurally-valid-provider-page"),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-message", ConversationID: "conversation-a",
		}}},
	})
	if err != nil || !result.ACKEligible || !result.Poisoned || result.Duplicate {
		t.Fatalf("repository poison result = (%+v, %v)", result, err)
	}
}

func TestPermanentlyOversizedProjectionIsPoisonedBeforeStoreProjection(t *testing.T) {
	for name, mutate := range map[string]func(*ingress.Envelope){
		"text": func(envelope *ingress.Envelope) {
			envelope.Projection.Messages = []ingress.ProjectedMessage{{ProviderMessageID: "message-a", ConversationID: "conversation-a", Text: string(make([]byte, 65537))}}
		},
		"cursor": func(envelope *ingress.Envelope) { envelope.Projection.Cursor = make([]byte, 4097) },
		"reserved parent cursor target": func(envelope *ingress.Envelope) {
			envelope.Projection.Cursor = []byte("child-cursor")
			envelope.Projection.CursorSource = ingress.CursorSourceListMessages
			envelope.Projection.CursorConversationID = ingress.ProviderPageCursorID
		},
		"media size": func(envelope *ingress.Envelope) {
			envelope.Projection.Messages = []ingress.ProjectedMessage{{ProviderMessageID: "message-a", ConversationID: "conversation-a"}}
			envelope.Media = []ingress.MediaLocator{{ProviderMessageID: "message-a", Locator: "gmessages:a", DeclaredSize: 25<<20 + 1}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := &inboxStore{result: ingress.CommitInserted}
			service, _ := ingress.NewService(store)
			envelope := ingress.Envelope{TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7, ProviderResponseID: "oversized", Raw: []byte("raw")}
			mutate(&envelope)
			result, err := service.Process(context.Background(), envelope)
			if err != nil || !result.ACKEligible || len(store.records) != 1 || !store.records[0].Poisoned || len(store.records[0].Projection.Messages) != 0 || len(store.records[0].Media) != 0 {
				t.Fatalf("Process() = (%+v, %v), record=%+v", result, err, store.records)
			}
		})
	}
}

func TestEnvelopeTenantAndConnectionAreRequired(t *testing.T) {
	service, _ := ingress.NewService(&inboxStore{result: ingress.CommitInserted})
	for _, envelope := range []ingress.Envelope{
		{ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7, ProviderResponseID: "response-a", Raw: []byte("raw")},
		{TenantID: "tenant-a", OwnerID: "owner-a", FencingToken: 7, ProviderResponseID: "response-a", Raw: []byte("raw")},
		{TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw")},
	} {
		if result, err := service.Process(context.Background(), envelope); !errors.Is(err, domain.ErrInvalidIdentifier) || result.ACKEligible {
			t.Fatalf("Process(%+v) = (%+v, %v)", envelope, result, err)
		}
	}
}
