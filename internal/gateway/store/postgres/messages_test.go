package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func providerCursorBytes(t *testing.T, id string, timestamp int64) []byte {
	t.Helper()
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&gmproto.Cursor{
		LastItemID: id, LastItemTimestamp: timestamp,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestCreateOutboundCommitsIdempotencyMessageStatusEventAndOutboxTogether(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_message_idempotency": {{err: sql.ErrNoRows}},
		"validate_message_route":  {{values: []any{"outgoing-a", "", "conversation-a"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"status-a", "event-a", "outbox-a"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	command := messaging.CreateOutbound{
		IdempotencyKey: "idem-a", RequestDigest: [32]byte{1, 2, 3},
		Message: messaging.OutboundMessage{
			ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
			Text: "hello", ProviderTmpID: "sx-temp", State: domain.MessageStateQueued, CreatedAt: time.Unix(1700000000, 0).UTC(),
		},
	}
	result, err := repository.CreateOutbound(context.Background(), command)
	if err != nil || result.Outcome != messaging.CreateInserted || result.Message.ID != "message-a" {
		t.Fatalf("CreateOutbound() = (%+v, %v)", result, err)
	}
	want := []string{"tenant_context", "lock_message_idempotency", "get_message_idempotency", "lock_message_route_connection", "validate_message_route", "insert_outbound_message", "ensure_message_lane", "insert_message_idempotency", "insert_message_status", "insert_gateway_event", "insert_event_outbox"}
	if got := tx.operationNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operation order = %v, want %v", got, want)
	}
	if query := strings.ToLower(tx.lastQuery("insert_message_idempotency")); !strings.Contains(query, "request_digest") || strings.Contains(query, "on conflict") {
		t.Fatalf("idempotency SQL = %s", query)
	}
	var event map[string]any
	call := tx.findCall("insert_gateway_event")
	if call == nil || json.Unmarshal(call.args[7].([]byte), &event) != nil || event["version"] != float64(1) ||
		event["tenant_id"] != "tenant-a" || event["connection_id"] != "connection-a" || event["conversation_id"] != "conversation-a" ||
		event["message_id"] != "message-a" || event["direction"] != "outbound" || event["text"] != "hello" ||
		event["status"] != "queued" || event["state"] != "queued" || event["occurred_at"] == "" {
		t.Fatalf("queued event contract = %#v", event)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestCreateOutboundSerializesMissingIdempotencyKeyBeforeLookingUpWinner(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_message_idempotency": {{err: sql.ErrNoRows}},
		"validate_message_route":  {{values: []any{"outgoing-a", "", "conversation-a"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "stable-id" }
	_, err := repository.CreateOutbound(context.Background(), messaging.CreateOutbound{
		IdempotencyKey: "same-key", RequestDigest: [32]byte{1},
		Message: messaging.OutboundMessage{
			ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
			Text: "hello", ProviderTmpID: "sx-temp", State: domain.MessageStateQueued, CreatedAt: time.Unix(1700000000, 0).UTC(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"tenant_context", "lock_message_idempotency", "get_message_idempotency"}
	got := tx.operationNames()
	if len(got) < len(wantPrefix) || !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("operation prefix = %v, want %v", got, wantPrefix)
	}
	query := strings.ToLower(tx.lastQuery("lock_message_idempotency"))
	if !strings.Contains(query, "pg_advisory_xact_lock") || !strings.Contains(query, "hashtextextended") {
		t.Fatalf("idempotency serialization SQL = %s", query)
	}
}

func TestCreateOutboundCommitsAuthenticatedKafkaAuditInSameTransaction(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_message_idempotency": {{err: sql.ErrNoRows}},
		"validate_message_route":  {{values: []any{"outgoing-a", "", "conversation-a"}}},
		"record_kafka_command":    {{values: []any{"command-a", "message-a"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"command-a", "status-a", "event-a", "outbox-a"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	audit := messaging.CommandAudit{
		Topic: "sirenaix.messaging.commands.v1", Partition: 2, Offset: 8,
		ProducerIdentity: "producer-a", CorrelationID: "correlation-a", PayloadDigest: [32]byte{7},
	}
	result, err := repository.CreateOutbound(context.Background(), messaging.CreateOutbound{
		IdempotencyKey: "idem-a", RequestDigest: [32]byte{1}, CommandAudit: &audit,
		Message: messaging.OutboundMessage{
			ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
			Text: "hello", ProviderTmpID: "sx-temp", State: domain.MessageStateQueued, CreatedAt: time.Unix(1700000000, 0).UTC(),
		},
	})
	if err != nil || result.Outcome != messaging.CreateInserted {
		t.Fatalf("CreateOutbound() = (%+v, %v)", result, err)
	}
	want := []string{"tenant_context", "lock_message_idempotency", "get_message_idempotency", "lock_message_route_connection", "validate_message_route", "insert_outbound_message", "ensure_message_lane", "insert_message_idempotency", "record_kafka_command", "insert_message_status", "insert_gateway_event", "insert_event_outbox"}
	if got := tx.operationNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operation order = %v, want %v", got, want)
	}
	query := strings.ToLower(tx.lastQuery("record_kafka_command"))
	for _, required := range []string{"kafka_commands", "payload_digest", "producer_identity", "correlation_id", "on conflict", "returning"} {
		if !strings.Contains(query, required) {
			t.Fatalf("Kafka audit SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestClaimNextUsesSkipLockedDBTimeAndConnectionFence(t *testing.T) {
	createdAt := time.Unix(1700000000, 0).UTC()
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"claim_next_message": {{values: []any{
			"message-a", "connection-a", "conversation-a", "", "", "", "hello", "sx-temp", "dispatching", createdAt,
			pq.StringArray{"media-a", "media-b"},
			"attempt-a", uint64(4), uint64(9),
		}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "attempt-a" }
	claim, ok, err := repository.ClaimNext(context.Background(), messaging.LaneKey{
		TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a",
	}, "dispatcher-a")
	if err != nil || !ok || claim.AttemptID != "attempt-a" || claim.LaneToken != 4 || claim.FencingToken != 9 || claim.Message.State != domain.MessageStateDispatching || !reflect.DeepEqual(claim.Message.MediaIDs, []domain.MediaID{"media-a", "media-b"}) {
		t.Fatalf("ClaimNext() = (%+v, %v, %v)", claim, ok, err)
	}
	query := strings.ToLower(tx.lastQuery("claim_next_message"))
	for _, required := range []string{
		"for update", "skip locked", "clock_timestamp()", "connection_leases", "fencing_token", "message_attempts", "message_status_history",
		"provider_io_ambiguous", "gateway_events", "event_outbox", "'version', 1", "'occurred_at'", "'tenant_id', $1",
		"'connection_id', recovered_message.connection_id", "'conversation_id', recovered_message.conversation_id", "'direction', 'outbound'", "'status', 'uncertain'",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("claim SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestListQueuedDispatchLanesIsBoundedDeterministicAndTenantScoped(t *testing.T) {
	tx := &fakeTransaction{rowsResult: map[string][][]any{
		"list_queued_dispatch_lanes": {{"connection-a", "conversation-a"}, {"connection-b", "recipient:+12025550123"}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	lanes, err := repository.ListQueuedDispatchLanes(context.Background(), "tenant-a", 64)
	if err != nil || len(lanes) != 2 || lanes[0].TenantID != "tenant-a" || lanes[0].ConnectionID != "connection-a" || lanes[0].ConversationID != "conversation-a" {
		t.Fatalf("ListQueuedDispatchLanes() = (%+v, %v)", lanes, err)
	}
	query := strings.ToLower(tx.lastQuery("list_queued_dispatch_lanes"))
	for _, required := range []string{"current_state = 'queued'", "ordering_key", "order by", "limit $2", "tenant_id = $1"} {
		if !strings.Contains(query, required) {
			t.Fatalf("lane discovery SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestListQueuedDispatchLanesAfterUsesBoundedRotatingCursor(t *testing.T) {
	tx := &fakeTransaction{rowsResult: map[string][][]any{"list_queued_dispatch_lanes_after": {{"connection-z", "conversation-healthy"}}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	lanes, err := repository.ListQueuedDispatchLanesAfter(context.Background(), "tenant-a", messaging.LaneKey{
		TenantID: "tenant-a", ConnectionID: "connection-y", ConversationID: "conversation-old",
	}, 64)
	if err != nil || len(lanes) != 1 || lanes[0].ConnectionID != "connection-z" {
		t.Fatalf("ListQueuedDispatchLanesAfter = (%+v, %v)", lanes, err)
	}
	query := strings.ToLower(tx.lastQuery("list_queued_dispatch_lanes_after"))
	for _, required := range []string{"(message.connection_id, message.ordering_key) > ($2, $3)", "order by message.connection_id, message.ordering_key", "limit $4"} {
		if !strings.Contains(query, required) {
			t.Fatalf("paginated lane SQL missing %q: %s", required, query)
		}
	}
}

func TestLoadCommittedCursorIsTenantScopedAndReturnsIndependentBytes(t *testing.T) {
	committed := []byte{10, 2, 'm', '1'}
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"load_committed_cursor": {{values: []any{committed}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	cursor, err := repository.LoadCommittedCursor(context.Background(), "tenant-a", "connection-a", "conversation-a")
	if err != nil || !bytes.Equal(cursor, committed) {
		t.Fatalf("LoadCommittedCursor() = (%x, %v)", cursor, err)
	}
	cursor[0] = 0
	if committed[0] == 0 {
		t.Fatal("returned cursor aliases driver-owned storage")
	}
	query := strings.ToLower(tx.lastQuery("load_committed_cursor"))
	for _, required := range []string{"tenant_id = $1", "connection_id = $2", "conversation_id = $3", "octet_length(committed_cursor) <= 4096"} {
		if !strings.Contains(query, required) {
			t.Fatalf("cursor SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestLoadCommittedCursorAcceptsOnlyCanonicalConversationOrParentSentinel(t *testing.T) {
	for _, valid := range []string{"conversation-a", domain.ProviderPageCursorID} {
		t.Run("valid "+valid, func(t *testing.T) {
			tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{"load_committed_cursor": {{err: sql.ErrNoRows}}}}
			repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
			if _, err := repository.LoadCommittedCursor(context.Background(), "tenant-a", "connection-a", valid); err != nil {
				t.Fatalf("LoadCommittedCursor(%q) = %v", valid, err)
			}
		})
	}
	for name, invalid := range map[string]string{
		"whitespace": " conversation-a", "control": "conversation\x00a", "reserved suffix": domain.ProviderPageCursorID + " ",
	} {
		t.Run(name, func(t *testing.T) {
			tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{"load_committed_cursor": {{err: sql.ErrNoRows}}}}
			repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
			if _, err := repository.LoadCommittedCursor(context.Background(), "tenant-a", "connection-a", invalid); !errors.Is(err, domain.ErrInvalidIdentifier) {
				t.Fatalf("LoadCommittedCursor(%q) = %v", invalid, err)
			}
			if len(tx.calls) != 0 {
				t.Fatalf("invalid cursor target reached transaction: %v", tx.operationNames())
			}
		})
	}
}

func TestBackfillCheckpointStagesLoadsAndAdvancesOnlyAfterEveryChild(t *testing.T) {
	t.Run("stage is lease fenced and compares the committed parent cursor", func(t *testing.T) {
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{"stage_backfill_page": {{values: []any{true}}}}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		repository.newID = func() string { return "checkpoint-a" }
		err := repository.StageBackfillPageFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 9, messaging.BackfillPage{
			BaseCursor: []byte("base"), NextCursor: []byte("next"),
			Items: []messaging.BackfillItem{{Ordinal: 0, ConversationID: "conversation-a", State: messaging.BackfillItemPending}},
		})
		if err != nil {
			t.Fatal(err)
		}
		query := strings.ToLower(tx.lastQuery("stage_backfill_page"))
		for _, required := range []string{"connection_leases", "owner_id = $3", "fencing_token = $4", "expires_at > clock_timestamp()", "committed_cursor", "provider_backfill_checkpoints"} {
			if !strings.Contains(query, required) {
				t.Fatalf("stage SQL missing %q: %s", required, query)
			}
		}
	})

	t.Run("load returns independent resumable item progress", func(t *testing.T) {
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
			"load_backfill_checkpoint": {{values: []any{
				"checkpoint-a", []byte("next"), false, false,
				pq.StringArray{"conversation-a", "conversation-b"}, pq.StringArray{"complete", "pending"}, pq.StringArray{"", ""},
			}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		checkpoint, err := repository.LoadBackfillCheckpoint(context.Background(), "tenant-a", "connection-a")
		if err != nil || checkpoint == nil || len(checkpoint.Items) != 2 || checkpoint.Items[1].ConversationID != "conversation-b" || checkpoint.Items[1].State != messaging.BackfillItemPending {
			t.Fatalf("LoadBackfillCheckpoint() = (%+v, %v)", checkpoint, err)
		}
		checkpoint.NextCursor[0] = 'X'
		if tx.rowResults["load_backfill_checkpoint"] != nil && bytes.Equal(checkpoint.NextCursor, []byte("next")) {
			t.Fatal("checkpoint bytes unexpectedly unchanged")
		}
	})

	t.Run("item and parent completion are token and lease fenced", func(t *testing.T) {
		markTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"mark_backfill_item": {{values: []any{true}}}}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{markTX}})
		if err := repository.MarkBackfillItemFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 9, "checkpoint-a", 1, messaging.BackfillItemComplete, ""); err != nil {
			t.Fatal(err)
		}
		markQuery := strings.ToLower(markTX.lastQuery("mark_backfill_item"))
		for _, required := range []string{"connection_leases", "checkpoint_id = $5", "item_states[$6 + 1] = 'pending'", "fencing_token = $4"} {
			if !strings.Contains(markQuery, required) {
				t.Fatalf("mark SQL missing %q: %s", required, markQuery)
			}
		}

		completeTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"complete_backfill_page": {{values: []any{true}}}}}
		repository = newRepository(&fakeBeginner{transactions: []*fakeTransaction{completeTX}})
		if err := repository.CompleteBackfillPageFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 9, "checkpoint-a"); err != nil {
			t.Fatal(err)
		}
		completeQuery := strings.ToLower(completeTX.lastQuery("complete_backfill_page"))
		for _, required := range []string{"for update", "item_states <@ array['complete']", "committed_cursor", "delete from provider_backfill_checkpoints", "delete from provider_cursor_history", "delete from provider_cursor_budgets", "fencing_token = $4"} {
			if !strings.Contains(completeQuery, required) {
				t.Fatalf("complete SQL missing %q: %s", required, completeQuery)
			}
		}
	})
}

func TestStageBackfillPageRejectsMoreItemsThanMigrationConstraint(t *testing.T) {
	repository := newRepository(&fakeBeginner{})
	items := make([]messaging.BackfillItem, messaging.MaxBackfillConversationsPerPage+1)
	for index := range items {
		items[index] = messaging.BackfillItem{Ordinal: index, ConversationID: "conversation", State: messaging.BackfillItemPending}
	}
	err := repository.StageBackfillPageFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7, messaging.BackfillPage{
		Terminal: true, Items: items,
	})
	if !errors.Is(err, domain.ErrInvalidIdentifier) {
		t.Fatalf("oversized checkpoint error = %v", err)
	}
}

func TestRenewProviderIORechecksLeaseAttemptAndLaneFence(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"renew_provider_io": {{values: []any{true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	claim := messaging.DispatchClaim{
		Message:   messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"},
		AttemptID: "attempt-a", LaneToken: 4, FencingToken: 9,
	}
	owned, err := repository.RenewProviderIO(context.Background(), claim, "dispatcher-a")
	if err != nil || !owned {
		t.Fatalf("RenewProviderIO() = (%v, %v)", owned, err)
	}
	query := strings.ToLower(tx.lastQuery("renew_provider_io"))
	for _, required := range []string{"connection_leases", "fencing_token", "provider_io_started", "lane_token", "claim_expires_at", "clock_timestamp()", "for update"} {
		if !strings.Contains(query, required) {
			t.Fatalf("renew-provider-I/O SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestDispatchQueriesUseCanonicalConnectionLeaseLaneAttemptMessageLockOrder(t *testing.T) {
	for name, query := range map[string]string{
		"claim":    claimNextMessageSQL,
		"begin":    beginProviderIOSQL,
		"renew":    renewProviderIOSQL,
		"complete": lockDispatchCompletionSQL,
		"release":  releaseBeforeDispatchSQL,
	} {
		t.Run(name, func(t *testing.T) {
			query = strings.ToLower(query)
			last := -1
			for _, marker := range []string{"locked_connection", "locked_lease", "locked_lane", "locked_attempt", "locked_message"} {
				position := strings.Index(query, marker)
				if position < 0 || position <= last {
					t.Fatalf("dispatch lock order missing/out of order at %q: %s", marker, query)
				}
				last = position
			}
		})
	}
}

func TestCreateOutboundRequiresRequestedLineToMatchConversationDefault(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_message_idempotency": {{err: sql.ErrNoRows}},
		"validate_message_route":  {{values: []any{"outgoing-primary", "outgoing-secondary", "conversation-a"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	result, err := repository.CreateOutbound(context.Background(), messaging.CreateOutbound{
		IdempotencyKey: "idem-line", RequestDigest: [32]byte{9},
		Message: messaging.OutboundMessage{
			ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a",
			ConversationID: "conversation-a", LineID: "line-secondary", Text: "hello",
			ProviderTmpID: "sx-temp", State: domain.MessageStateQueued,
		},
	})
	if !errors.Is(err, messaging.ErrInvalidRoute) || result.Outcome != 0 {
		t.Fatalf("CreateOutbound() = (%+v, %v), want ErrInvalidRoute", result, err)
	}
	if tx.findCall("insert_outbound_message") != nil {
		t.Fatal("mismatched route inserted a message")
	}
	query := strings.ToLower(tx.lastQuery("validate_message_route"))
	for _, required := range []string{"provider_default_outgoing_id", "provider_outgoing_id", "tenant_id", "connection_id", "line.active = true", "for share"} {
		if !strings.Contains(query, required) {
			t.Fatalf("route validation SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}

func TestRecordCreatedConversationIsAttemptAndLeaseFencedBeforeProviderSend(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_message_route_connection": {{values: []any{true}}},
		"record_created_conversation":   {{values: []any{true, false, true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	err := repository.RecordCreatedConversationFenced(
		context.Background(), "tenant-a", "connection-a", "message-a",
		"conversation-created", "outgoing-default", false, "owner-a", 9,
	)
	if err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(tx.lastQuery("record_created_conversation"))
	for _, required := range []string{"connection_leases", "fencing_token", "clock_timestamp()", "provider_io_started", "provider_default_outgoing_id", "update messages", "locked_lanes", "other_lane_work", "for update"} {
		if !strings.Contains(query, required) {
			t.Fatalf("created-conversation SQL missing %q: %s", required, query)
		}
	}
	if strings.Contains(query, "coalesce(nullif(conversations.ordering_key") || !strings.Contains(query, "ordering_key = excluded.ordering_key") {
		t.Fatalf("pre-existing conversation did not adopt in-flight canonical new-chat lane: %s", query)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRecordCreatedConversationRejectsReservedProviderConversationBeforeTransaction(t *testing.T) {
	database := &fakeBeginner{}
	err := newRepository(database).RecordCreatedConversationFenced(
		context.Background(), "tenant-a", "connection-a", "message-a",
		domain.ProviderPageCursorID, "outgoing-default", false, "owner-a", 9,
	)
	if !errors.Is(err, messaging.ErrInvalidRoute) || database.beginCalls != 0 {
		t.Fatalf("reserved RecordCreatedConversationFenced() = (%v, begin calls %d)", err, database.beginCalls)
	}
}

func TestStageBackfillPageRejectsReservedProviderConversationBeforeTransaction(t *testing.T) {
	for _, state := range []messaging.BackfillItemState{messaging.BackfillItemPending, messaging.BackfillItemPoisoned} {
		t.Run(string(state), func(t *testing.T) {
			database := &fakeBeginner{}
			err := newRepository(database).StageBackfillPageFenced(
				context.Background(), "tenant-a", "connection-a", "owner-a", 9,
				messaging.BackfillPage{Terminal: true, Items: []messaging.BackfillItem{{
					Ordinal: 0, ConversationID: domain.ProviderPageCursorID, State: state, SafeError: "reserved_provider_id",
				}}},
			)
			if !errors.Is(err, domain.ErrInvalidIdentifier) || database.beginCalls != 0 {
				t.Fatalf("reserved StageBackfillPageFenced(%s) = (%v, begin calls %d)", state, err, database.beginCalls)
			}
		})
	}
}

func TestRepositoryRejectsReservedProviderConversationUpsertsBeforeTransaction(t *testing.T) {
	for name, projection := range map[string]ingress.Projection{
		"conversation": {Conversations: []ingress.ProjectedConversation{{ConversationID: domain.ProviderPageCursorID}}},
		"message": {Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-message", ConversationID: domain.ProviderPageCursorID,
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			database := &fakeBeginner{}
			result, err := newRepository(database).CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
				TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 9,
				ProviderResponseID: "response-reserved-" + name, Raw: []byte("raw"), ACKPending: true,
				Projection: projection,
			})
			if !errors.Is(err, domain.ErrInvalidIdentifier) || result != 0 || database.beginCalls != 0 {
				t.Fatalf("reserved %s projection = (%v, %v, begin calls %d)", name, result, err, database.beginCalls)
			}
		})
	}
}

func TestCreateOutboundRejectsReservedProviderConversationBeforeTransaction(t *testing.T) {
	database := &fakeBeginner{}
	result, err := newRepository(database).CreateOutbound(context.Background(), messaging.CreateOutbound{
		IdempotencyKey: "reserved-route", RequestDigest: [32]byte{1},
		Message: messaging.OutboundMessage{
			ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a",
			ConversationID: domain.ProviderPageCursorID, Text: "hello", ProviderTmpID: "sx-temp", State: domain.MessageStateQueued,
		},
	})
	if !errors.Is(err, messaging.ErrInvalidRoute) || result.Outcome != 0 || database.beginCalls != 0 {
		t.Fatalf("reserved CreateOutbound() = (%+v, %v, begin calls %d)", result, err, database.beginCalls)
	}
}

func TestRecordCreatedConversationRejectsPreexistingQueuedOrInflightOldLane(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_message_route_connection": {{values: []any{true}}},
		"record_created_conversation":   {{values: []any{true, true, false}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	err := repository.RecordCreatedConversationFenced(
		context.Background(), "tenant-a", "connection-a", "message-new",
		"conversation-existing", "outgoing-default", false, "owner-a", 9,
	)
	if !errors.Is(err, messaging.ErrCanonicalLaneBusy) {
		t.Fatalf("RecordCreatedConversationFenced() error = %v, want ErrCanonicalLaneBusy", err)
	}
	if !tx.rolledBack || tx.committed {
		t.Fatalf("busy migration transaction committed=%v rolledBack=%v", tx.committed, tx.rolledBack)
	}
}

func TestReturnedConversationSubmissionUsesCanonicalRecipientLane(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_message_idempotency": {{err: sql.ErrNoRows}},
		"validate_message_route":  {{values: []any{"outgoing-a", "", "new:+12025550123"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "stable-id" }
	_, err := repository.CreateOutbound(context.Background(), messaging.CreateOutbound{
		IdempotencyKey: "conversation-followup", RequestDigest: [32]byte{9},
		Message: messaging.OutboundMessage{
			ID: "message-followup", TenantID: "tenant-a", ConnectionID: "connection-a",
			ConversationID: "provider-created", Text: "second", ProviderTmpID: "sx-second",
			State: domain.MessageStateQueued, CreatedAt: time.Unix(1700000000, 0).UTC(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	insert := tx.findCall("insert_outbound_message")
	if insert == nil || insert.args[4] != "new:+12025550123" {
		t.Fatalf("follow-up ordering lane = %+v", insert)
	}
}

func TestBeginProviderIORechecksCurrentLeaseAndLaneImmediatelyBeforeSend(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"begin_provider_io": {{values: []any{true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	claim := messaging.DispatchClaim{
		Message:   messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"},
		AttemptID: "attempt-a", LaneToken: 4, FencingToken: 9,
	}
	owned, err := repository.BeginProviderIO(context.Background(), claim, "dispatcher-a")
	if err != nil || !owned {
		t.Fatalf("BeginProviderIO() = (%v, %v)", owned, err)
	}
	query := strings.ToLower(tx.lastQuery("begin_provider_io"))
	for _, required := range []string{"from connections", "from connection_leases", "fencing_token", "lane_token", "phase = 'claimed'", "clock_timestamp()", "for update"} {
		if !strings.Contains(query, required) {
			t.Fatalf("begin-provider-I/O SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestCompleteDispatchAtomicallyRecordsHistoryEventsAfterLeaseExpiryWhenTokensAreUnchanged(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_dispatch_completion": {{values: []any{"dispatching"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"status-1", "event-1", "outbox-1", "status-2", "event-2", "outbox-2"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	claim := messaging.DispatchClaim{
		Message:   messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"},
		AttemptID: "attempt-a", OrderingKey: "conversation-a", OwnerID: "dispatcher-a", LaneToken: 4, FencingToken: 9,
	}
	err := repository.CompleteDispatch(context.Background(), claim, []domain.MessageState{
		domain.MessageStateProviderAccepted, domain.MessageStateAwaitingPhone,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"tenant_context", "lock_dispatch_completion",
		"insert_message_status", "insert_gateway_event", "insert_event_outbox",
		"insert_message_status", "insert_gateway_event", "insert_event_outbox",
		"update_dispatch_message", "finish_dispatch_attempt", "release_dispatch_lane",
	}
	if got := tx.operationNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operation order = %v, want %v", got, want)
	}
	query := strings.ToLower(tx.lastQuery("lock_dispatch_completion"))
	for _, required := range []string{"from connections", "from connection_leases", "fencing_token = $7", "lane_token = $4", "attempt_id = $6", "provider_io_started", "for update"} {
		if !strings.Contains(query, required) {
			t.Fatalf("post-I/O completion lacks token fence %q: %s", required, query)
		}
	}
	if strings.Contains(query, "expires_at > clock_timestamp()") || strings.Contains(query, "claim_expires_at > clock_timestamp()") {
		t.Fatalf("known provider result incorrectly depends on an unexpired lease: %s", query)
	}
	for _, eventCall := range tx.calls {
		if eventCall.operation != "insert_gateway_event" {
			continue
		}
		var body map[string]any
		if err := json.Unmarshal(eventCall.args[7].([]byte), &body); err != nil || body["version"] != float64(1) ||
			body["tenant_id"] != "tenant-a" || body["connection_id"] != "connection-a" || body["conversation_id"] != "conversation-a" ||
			body["message_id"] != "message-a" || body["direction"] != "outbound" || body["status"] == "" || body["state"] == "" || body["occurred_at"] == "" {
			t.Fatalf("dispatch event contract = %#v, error %v", body, err)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestReleaseBeforeDispatchIsOnlyClaimedPhaseRequeue(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"release_before_dispatch": {{values: []any{true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "status-requeued" }
	claim := messaging.DispatchClaim{
		Message:   messaging.OutboundMessage{ID: "message-a", TenantID: "tenant-a", ConnectionID: "connection-a", ConversationID: "conversation-a"},
		AttemptID: "attempt-a", OrderingKey: "conversation-a", OwnerID: "dispatcher-a", LaneToken: 4, FencingToken: 9,
	}
	if err := repository.ReleaseBeforeDispatch(context.Background(), claim, "fence_lost"); err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(tx.lastQuery("release_before_dispatch"))
	for _, required := range []string{"phase = 'claimed'", "current_state = 'queued'", "lane_token", "fencing_token", "message_status_history"} {
		if !strings.Contains(query, required) {
			t.Fatalf("pre-dispatch release SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestCreateOutboundSameDigestReturnsOriginalAndDifferentDigestConflicts(t *testing.T) {
	digest := [32]byte{7}
	existing := []any{digest[:], "message-original"}
	messageRow := []any{"message-original", "connection-a", "conversation-a", "outbound", "", "", "", "original", "", "sx-original", "", "queued", time.Unix(1700000000, 0).UTC(), pq.StringArray{"media-original"}}
	exactTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_message_idempotency": {{values: existing}}, "get_outbound_message": {{values: messageRow}},
	}}
	exactRepo := newRepository(&fakeBeginner{transactions: []*fakeTransaction{exactTX}})
	result, err := exactRepo.CreateOutbound(context.Background(), messaging.CreateOutbound{
		IdempotencyKey: "idem-a", RequestDigest: digest,
		Message: messaging.OutboundMessage{ID: "message-new", TenantID: "tenant-a", ConnectionID: "connection-a", State: domain.MessageStateQueued},
	})
	if err != nil || result.Outcome != messaging.CreateDuplicate || result.Message.ID != "message-original" {
		t.Fatalf("exact duplicate = (%+v, %v)", result, err)
	}
	if exactTX.findCall("insert_outbound_message") != nil {
		t.Fatal("exact duplicate inserted a replacement")
	}

	conflictTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"get_message_idempotency": {{values: existing}}}}
	conflictRepo := newRepository(&fakeBeginner{transactions: []*fakeTransaction{conflictTX}})
	result, err = conflictRepo.CreateOutbound(context.Background(), messaging.CreateOutbound{
		IdempotencyKey: "idem-a", RequestDigest: [32]byte{8},
		Message: messaging.OutboundMessage{ID: "message-new", TenantID: "tenant-a", ConnectionID: "connection-a", State: domain.MessageStateQueued},
	})
	if err != nil || result.Outcome != messaging.CreateConflict {
		t.Fatalf("conflict = (%+v, %v)", result, err)
	}
}

func TestCommitEnvelopeChecksFenceAndAtomicallyProjectsCursorMediaAndEvents(t *testing.T) {
	cursor := providerCursorBytes(t, "cursor-a", 1)
	cursorDigest := sha256.Sum256(cursor)
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":               {{values: []any{true}}},
		"get_provider_inbox":               {{err: sql.ErrNoRows}},
		"lock_existing_attachment":         {{err: sql.ErrNoRows}},
		"lock_projected_message":           {{err: sql.ErrNoRows}},
		"lock_provider_cursor_budget":      {{values: []any{0, false}}},
		"find_provider_cursor_history":     {{err: sql.ErrNoRows}},
		"insert_provider_cursor_history":   {{values: []any{cursorDigest[:], []byte(nil), sql.NullString{String: "response-a", Valid: true}}}},
		"increment_provider_cursor_budget": {{values: []any{1}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-a", "message-a", "status-a", "media-a", "job-a", "outbox-message", "outbox-media"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	receivedAt := time.Date(2026, 8, 25, 12, 30, 0, 0, time.UTC)
	locator := ingress.MediaLocator{ProviderMessageID: "provider-a", Locator: "gmessages:media-a", MIMEType: "image/png", DeclaredSize: 42}
	record := ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-a",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{3}, ACKPending: true, ReceivedAt: receivedAt,
		Projection: ingress.Projection{
			Conversations: []ingress.ProjectedConversation{{ConversationID: "conversation-a", DefaultOutgoingID: "outgoing-a", IsGroup: false}},
			Messages: []ingress.ProjectedMessage{{
				ProviderMessageID: "provider-a", ConversationID: "conversation-a", Direction: "inbound",
				Provenance: ingress.MessageProvenanceHistory, ProviderStatus: "INCOMING_DELIVERED", ProviderOccurredAt: receivedAt.Add(-time.Minute),
				Sender: "+12025550100", Text: "hello", Transport: "mms", State: domain.MessageStateDelivered,
			}},
			Cursor: cursor, CursorSource: ingress.CursorSourceListMessages,
			CursorConversationID: "conversation-a",
		},
		Media: []ingress.MediaLocator{locator},
		Events: []ingress.OutboxEvent{
			{ID: "event-a", Type: "message.imported", AggregateID: "provider-a", CanonicalBody: []byte(`{"type":"message.imported"}`)},
			{ID: "event-media", Type: "media.pending", AggregateID: ingress.AttachmentAggregateID(locator), CanonicalBody: []byte(`{"type":"media.pending"}`)},
		},
	}
	result, err := repository.CommitEnvelope(context.Background(), record)
	if err != nil || result != ingress.CommitInserted {
		t.Fatalf("CommitEnvelope() = (%v, %v)", result, err)
	}
	want := []string{
		"tenant_context", "verify_inbox_fence", "ensure_provider_cursor_budget", "lock_provider_cursor_budget",
		"get_provider_inbox", "insert_provider_inbox", "find_provider_cursor_history", "insert_provider_cursor_history", "increment_provider_cursor_budget", "prune_provider_cursor_history",
		"lock_existing_attachment", "upsert_provider_conversation", "lock_projected_message", "insert_projected_message", "insert_inbound_status", "insert_media_object", "insert_inbound_media_link", "insert_media_fetch_job",
		"insert_gateway_event", "insert_event_outbox", "insert_gateway_event", "insert_event_outbox", "advance_conversation_cursor",
	}
	if got := tx.operationNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("operation order = %v, want %v", got, want)
	}
	insertedEvent := tx.findCall("insert_gateway_event")
	if insertedEvent == nil {
		t.Fatal("message event was not inserted")
	}
	var body struct {
		EventID           string `json:"event_id"`
		Type              string `json:"type"`
		OccurredAt        string `json:"occurred_at"`
		TenantID          string `json:"tenant_id"`
		ConnectionID      string `json:"connection_id"`
		ConversationID    string `json:"conversation_id"`
		MessageID         string `json:"message_id"`
		ProviderMessageID string `json:"provider_message_id"`
		Direction         string `json:"direction"`
		Provenance        string `json:"provenance"`
		Actionable        bool   `json:"actionable"`
		Sender            string `json:"sender"`
		Text              string `json:"text"`
		Status            string `json:"status"`
		State             string `json:"state"`
		Version           int    `json:"version"`
		Media             []struct {
			ID       string `json:"media_id"`
			Position int    `json:"position"`
			Status   string `json:"status"`
			MIMEType string `json:"mime_type"`
			Size     int64  `json:"size"`
		} `json:"media"`
	}
	if err = json.Unmarshal(insertedEvent.args[7].([]byte), &body); err != nil {
		t.Fatal(err)
	}
	expectedDigest := sha256.Sum256([]byte("tenant-a\x00message-a\x00message.imported"))
	expectedEventID := fmt.Sprintf("evt_%x", expectedDigest[:16])
	if body.EventID != expectedEventID || body.Type != "message.imported" || body.Version != 1 || body.OccurredAt != receivedAt.Add(-time.Minute).Format(time.RFC3339Nano) ||
		body.TenantID != "tenant-a" || body.ConnectionID != "connection-a" || body.ConversationID != "conversation-a" ||
		body.MessageID != "message-a" || body.ProviderMessageID != "provider-a" || body.Direction != "inbound" || body.Sender != "+12025550100" ||
		body.Provenance != string(ingress.MessageProvenanceHistory) || body.Actionable ||
		body.Text != "hello" || body.Status != "delivered" || body.State != "delivered" || len(body.Media) != 1 ||
		body.Media[0].ID != "media-a" || body.Media[0].Position != 0 || body.Media[0].Status != "pending" || body.Media[0].MIMEType != "image/png" || body.Media[0].Size != 42 {
		t.Fatalf("stored message event contract = %+v", body)
	}
	var mediaCall *fakeCall
	for index := range tx.calls {
		if tx.calls[index].operation == "insert_gateway_event" && tx.calls[index].args[2] == "media.pending" {
			mediaCall = &tx.calls[index]
			break
		}
	}
	if mediaCall == nil || mediaCall.args[3] != "media" || mediaCall.args[4] != "media-a" {
		t.Fatalf("pending media durable event = %#v", mediaCall)
	}
	var pending struct {
		MediaID           string `json:"media_id"`
		MessageID         string `json:"message_id"`
		ProviderMessageID string `json:"provider_message_id"`
		ConversationID    string `json:"conversation_id"`
		MetadataPath      string `json:"metadata_path"`
		Status            string `json:"status"`
		Media             []struct {
			ID           string `json:"media_id"`
			Status       string `json:"status"`
			MetadataPath string `json:"metadata_path"`
			MIMEType     string `json:"mime_type"`
			Size         int64  `json:"size"`
		} `json:"media"`
		Data map[string]any `json:"data"`
	}
	if err = json.Unmarshal(mediaCall.args[7].([]byte), &pending); err != nil || pending.MediaID != "media-a" ||
		pending.MessageID != "message-a" || pending.ProviderMessageID != "provider-a" || pending.ConversationID != "conversation-a" ||
		pending.MetadataPath != "/v1/media/media-a" || pending.Status != "pending" ||
		len(pending.Media) != 1 || pending.Media[0].ID != "media-a" || pending.Media[0].Status != "pending" ||
		pending.Media[0].MetadataPath != "/v1/media/media-a" || pending.Media[0].MIMEType != "image/png" || pending.Media[0].Size != 42 ||
		pending.Data["provider_message_id"] != "provider-a" || pending.Data["attachment_index"] != float64(0) {
		t.Fatalf("pending media contract = %+v, error=%v", pending, err)
	}
	if mediaCall.args[6] != "conversation-a" {
		t.Fatalf("pending media partition conversation = %#v", mediaCall.args[6])
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestCommitEnvelopeSemanticMessageDedupeUsesStableInternalIdentity(t *testing.T) {
	providerTime := time.Date(2026, 8, 25, 11, 45, 0, 0, time.UTC)
	first := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":     {{values: []any{true}}},
		"get_provider_inbox":     {{err: sql.ErrNoRows}},
		"lock_projected_message": {{err: sql.ErrNoRows}},
	}}
	firstRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{first}})
	firstIDs := []string{"inbox-first", "message-internal", "status-first", "outbox-first"}
	firstRepository.newID = func() string { id := firstIDs[0]; firstIDs = firstIDs[1:]; return id }
	firstRecord := liveInboundEnvelopeRecord("response-one", "candidate-event-one", providerTime)
	if result, err := firstRepository.CommitEnvelope(context.Background(), firstRecord); err != nil || result != ingress.CommitInserted {
		t.Fatalf("first live message commit = (%v, %v)", result, err)
	}

	received := first.findCall("insert_gateway_event")
	if received == nil {
		t.Fatal("first live message did not emit a durable event")
	}
	expectedDigest := sha256.Sum256([]byte("tenant-a\x00message-internal\x00message.received"))
	expectedEventID := fmt.Sprintf("evt_%x", expectedDigest[:16])
	if received.args[1] != expectedEventID || received.args[2] != "message.received" ||
		received.args[3] != "message" || received.args[4] != "message-internal" {
		t.Fatalf("stable received identity = %#v, want event=%q aggregate=message-internal", received.args, expectedEventID)
	}
	var receivedBody struct {
		EventID      string `json:"event_id"`
		OccurredAt   string `json:"occurred_at"`
		IngestedAt   string `json:"ingested_at"`
		Direction    string `json:"direction"`
		Provenance   string `json:"provenance"`
		Actionable   bool   `json:"actionable"`
		ProviderStat string `json:"provider_status"`
	}
	if err := json.Unmarshal(received.args[7].([]byte), &receivedBody); err != nil {
		t.Fatal(err)
	}
	if receivedBody.EventID != expectedEventID || receivedBody.OccurredAt != providerTime.Format(time.RFC3339Nano) ||
		receivedBody.IngestedAt != firstRecord.ReceivedAt.Format(time.RFC3339Nano) || receivedBody.Direction != "inbound" ||
		receivedBody.Provenance != string(ingress.MessageProvenanceLive) || !receivedBody.Actionable ||
		receivedBody.ProviderStat != "INCOMING_COMPLETE" {
		t.Fatalf("received body = %+v", receivedBody)
	}
	if query := strings.ToLower(first.lastQuery("insert_gateway_event")); !strings.Contains(query, "on conflict") || !strings.Contains(query, "do nothing") {
		t.Fatalf("semantic event insertion is not concurrency-safe: %s", query)
	}

	// A different provider response arrives after a process restart with the
	// same provider message identity. It retries the same semantic received
	// event, and the durable uniqueness constraint suppresses its outboxes.
	second := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{
			"verify_inbox_fence":     {{values: []any{true}}},
			"get_provider_inbox":     {{err: sql.ErrNoRows}},
			"lock_projected_message": {{values: []any{"message-internal", "delivered", "inbound"}}},
		},
		execResults: map[string][]int64{"insert_gateway_event": {0}},
	}
	secondRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{second}})
	secondIDs := []string{"inbox-second", "status-second"}
	secondRepository.newID = func() string { id := secondIDs[0]; secondIDs = secondIDs[1:]; return id }
	secondRecord := liveInboundEnvelopeRecord("response-two", "candidate-event-two", providerTime)
	if result, err := secondRepository.CommitEnvelope(context.Background(), secondRecord); err != nil || result != ingress.CommitInserted {
		t.Fatalf("restart duplicate message commit = (%v, %v)", result, err)
	}
	retried := second.findCall("insert_gateway_event")
	if retried == nil || retried.args[1] != expectedEventID || retried.args[2] != "message.received" || retried.args[3] != "message" || retried.args[4] != "message-internal" {
		t.Fatalf("duplicate semantic message event = %#v", retried)
	}
	var retriedBody struct {
		Type       string `json:"type"`
		Actionable bool   `json:"actionable"`
	}
	if err := json.Unmarshal(retried.args[7].([]byte), &retriedBody); err != nil {
		t.Fatal(err)
	}
	if retriedBody.Type != "message.received" || !retriedBody.Actionable {
		t.Fatalf("semantic received retry changed contract: %+v", retriedBody)
	}
	if second.findCall("insert_event_outbox") != nil {
		t.Fatalf("restart duplicate emitted another outbox: %v", second.operationNames())
	}

	// Model the losing side of a concurrent first-seen race: its candidate
	// insert affects zero rows, then it must lock the committed winner and
	// retry the same received identity without producing another outbox.
	raceTX := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{
			"verify_inbox_fence": {{values: []any{true}}},
			"get_provider_inbox": {{err: sql.ErrNoRows}},
			"lock_projected_message": {
				{err: sql.ErrNoRows},
				{values: []any{"message-internal", "delivered", "inbound"}},
			},
		},
		execResults: map[string][]int64{"insert_projected_message": {0}, "insert_gateway_event": {0}},
	}
	raceRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{raceTX}})
	raceIDs := []string{"inbox-race", "message-loser", "status-race"}
	raceRepository.newID = func() string { id := raceIDs[0]; raceIDs = raceIDs[1:]; return id }
	raceRecord := liveInboundEnvelopeRecord("response-race", "candidate-event-race", providerTime)
	if result, err := raceRepository.CommitEnvelope(context.Background(), raceRecord); err != nil || result != ingress.CommitInserted {
		t.Fatalf("concurrent losing commit = (%v, %v), calls=%v", result, err, raceTX.operationNames())
	}
	if calls := raceTX.operationNames(); !reflect.DeepEqual(filterOperation(calls, "lock_projected_message"), []string{"lock_projected_message", "lock_projected_message"}) {
		t.Fatalf("concurrent loser did not reload winner: %v", calls)
	}
	if event := raceTX.findCall("insert_gateway_event"); event == nil || event.args[1] != expectedEventID || event.args[2] != "message.received" {
		t.Fatalf("concurrent loser event = %#v", event)
	}
	if raceTX.findCall("insert_event_outbox") != nil {
		t.Fatalf("concurrent loser emitted another outbox: %v", raceTX.operationNames())
	}
}

func TestCommitEnvelopeLiveCompletePromotesExistingNonactionableMessage(t *testing.T) {
	providerTime := time.Date(2026, 8, 25, 11, 45, 0, 0, time.UTC)
	for _, fixture := range []struct {
		name         string
		currentState domain.MessageState
		direction    string
	}{
		{name: "history row", currentState: domain.MessageStateDelivered, direction: "inbound"},
		{name: "pending MMS row", currentState: domain.MessageStateUncertain, direction: "unknown"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			tx := &fakeTransaction{
				rowResults: map[string][]fakeRowResult{
					"verify_inbox_fence":     {{values: []any{true}}},
					"get_provider_inbox":     {{err: sql.ErrNoRows}},
					"lock_projected_message": {{values: []any{"message-internal", string(fixture.currentState), fixture.direction}}},
				},
				rowsResult: map[string][][]any{"list_message_states": {{string(fixture.currentState)}}},
			}
			repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
			ids := []string{"inbox-promote", "status-promote", "outbox-promote"}
			repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
			record := liveInboundEnvelopeRecord("response-after-nonactionable", "candidate-after-nonactionable", providerTime)
			if result, err := repository.CommitEnvelope(context.Background(), record); err != nil || result != ingress.CommitInserted {
				t.Fatalf("live complete promotion = (%v, %v), calls=%v", result, err, tx.operationNames())
			}
			event := tx.findCall("insert_gateway_event")
			expectedDigest := sha256.Sum256([]byte("tenant-a\x00message-internal\x00message.received"))
			expectedEventID := fmt.Sprintf("evt_%x", expectedDigest[:16])
			if event == nil || event.args[1] != expectedEventID || event.args[2] != "message.received" || event.args[4] != "message-internal" {
				t.Fatalf("promotion event = %#v", event)
			}
			if tx.findCall("insert_event_outbox") == nil {
				t.Fatalf("new actionable event has no outbox: %v", tx.operationNames())
			}
		})
	}
}

func TestCommitEnvelopeDeliveredOrDisplayedCannotEstablishActionableInbound(t *testing.T) {
	for _, providerStatus := range []string{"INCOMING_DELIVERED", "INCOMING_DISPLAYED"} {
		t.Run(providerStatus, func(t *testing.T) {
			tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
				"verify_inbox_fence":     {{values: []any{true}}},
				"get_provider_inbox":     {{err: sql.ErrNoRows}},
				"lock_projected_message": {{err: sql.ErrNoRows}},
			}}
			repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
			ids := []string{"inbox-status-only", "message-status-only", "status-status-only", "outbox-status-only"}
			repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
			record := liveInboundEnvelopeRecord("response-"+strings.ToLower(providerStatus), "candidate-status-only", time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC))
			record.Projection.Messages[0].ProviderStatus = providerStatus
			if providerStatus == "INCOMING_DISPLAYED" {
				record.Projection.Messages[0].State = domain.MessageStateRead
			}
			if result, err := repository.CommitEnvelope(context.Background(), record); err != nil || result != ingress.CommitInserted {
				t.Fatalf("status-only commit = (%v, %v)", result, err)
			}
			event := tx.findCall("insert_gateway_event")
			var body map[string]any
			if event == nil || event.args[2] != "message.updated" || json.Unmarshal(event.args[7].([]byte), &body) != nil || body["actionable"] == true {
				t.Fatalf("status-only message became actionable: %#v / %#v", event, body)
			}
			for _, call := range tx.calls {
				if call.operation == "insert_gateway_event" && call.args[2] == "message.received" {
					t.Fatalf("status-only message emitted message.received: %#v", call)
				}
			}
		})
	}
}

func TestCommitEnvelopeRefinesOnlyUnknownProviderDirection(t *testing.T) {
	for _, knownDirection := range []string{"inbound", "outbound"} {
		t.Run(knownDirection, func(t *testing.T) {
			tx := &fakeTransaction{
				rowResults: map[string][]fakeRowResult{
					"verify_inbox_fence":     {{values: []any{true}}},
					"get_provider_inbox":     {{err: sql.ErrNoRows}},
					"lock_projected_message": {{values: []any{"message-direction", "uncertain", "unknown"}}},
				},
				rowsResult: map[string][][]any{"list_message_states": {{"uncertain"}}},
			}
			repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
			ids := []string{"inbox-direction", "outbox-direction"}
			repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
			record := ingress.EnvelopeRecord{
				TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-direction-" + knownDirection,
				OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-direction-" + knownDirection), ACKPending: true,
				Digest: sha256.Sum256([]byte("raw-direction-" + knownDirection)), ReceivedAt: time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC),
				Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
					ProviderMessageID: "provider-direction", ConversationID: "conversation-a", Direction: knownDirection,
					Provenance: ingress.MessageProvenanceLive, ProviderStatus: "provider-known-direction",
				}}},
				Events: []ingress.OutboxEvent{{ID: "candidate-direction", Type: "message.updated", AggregateID: "provider-direction", CanonicalBody: []byte(`{"type":"message.updated"}`)}},
			}
			if result, err := repository.CommitEnvelope(context.Background(), record); err != nil || result != ingress.CommitInserted {
				t.Fatalf("unknown to %s = (%v, %v)", knownDirection, result, err)
			}
			update := tx.findCall("update_projected_message")
			if update == nil || len(update.args) != 9 || update.args[8] != knownDirection {
				t.Fatalf("direction refinement args = %#v", update)
			}
			query := strings.ToLower(update.query)
			if !strings.Contains(query, "direction = case") || !strings.Contains(query, "direction = 'unknown'") || !strings.Contains(query, "$9 in ('inbound', 'outbound')") {
				t.Fatalf("direction refinement is not one-way: %s", query)
			}
			event := tx.findCall("insert_gateway_event")
			var body map[string]any
			if event == nil || json.Unmarshal(event.args[7].([]byte), &body) != nil || body["direction"] != knownDirection {
				t.Fatalf("refined event direction = %#v / %#v", event, body)
			}
			if !tx.committed {
				t.Fatal("direction refinement did not commit atomically")
			}
		})
	}
}

func TestCommitEnvelopeRejectsKnownProviderDirectionConflict(t *testing.T) {
	for _, fixture := range []struct{ stored, projected string }{
		{stored: "inbound", projected: "outbound"},
		{stored: "outbound", projected: "inbound"},
	} {
		t.Run(fixture.stored+" to "+fixture.projected, func(t *testing.T) {
			tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
				"verify_inbox_fence":     {{values: []any{true}}},
				"get_provider_inbox":     {{err: sql.ErrNoRows}},
				"lock_projected_message": {{values: []any{"message-direction", "uncertain", fixture.stored}}},
			}}
			repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
			repository.newID = func() string { return "inbox-conflict" }
			record := ingress.EnvelopeRecord{
				TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-conflict-" + fixture.projected,
				OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-conflict"), Digest: sha256.Sum256([]byte("raw-conflict")), ACKPending: true,
				Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
					ProviderMessageID: "provider-direction", ConversationID: "conversation-a", Direction: fixture.projected,
					Provenance: ingress.MessageProvenanceLive, ProviderStatus: "provider-conflict",
				}}},
				Events: []ingress.OutboxEvent{{ID: "candidate-conflict", Type: "message.updated", AggregateID: "provider-direction", CanonicalBody: []byte(`{"type":"message.updated"}`)}},
			}
			if _, err := repository.CommitEnvelope(context.Background(), record); err == nil || !strings.Contains(err.Error(), "direction conflicts") {
				t.Fatalf("known direction conflict error = %v", err)
			}
			if !tx.rolledBack || tx.findCall("update_projected_message") != nil || tx.findCall("insert_gateway_event") != nil || tx.findCall("insert_event_outbox") != nil {
				t.Fatalf("known direction conflict mutated durable state: %v", tx.operationNames())
			}
		})
	}
}

func TestCommitEnvelopeDoesNotInventFinalStateForInProgressProviderStatus(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":     {{values: []any{true}}},
		"get_provider_inbox":     {{err: sql.ErrNoRows}},
		"lock_projected_message": {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-in-progress", "message-in-progress"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	record := ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: "response-in-progress", Raw: []byte("raw-in-progress"), Digest: sha256.Sum256([]byte("raw-in-progress")),
		ACKPending: true, ReceivedAt: time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-in-progress", ConversationID: "conversation-a", Direction: "inbound",
			Provenance: ingress.MessageProvenanceLive, ProviderStatus: "INCOMING_AUTO_DOWNLOADING",
		}}},
	}
	if result, err := repository.CommitEnvelope(context.Background(), record); err != nil || result != ingress.CommitInserted {
		t.Fatalf("in-progress provider commit = (%v, %v)", result, err)
	}
	insert := tx.findCall("insert_projected_message")
	if insert == nil || insert.args[9] != string(domain.MessageStateUncertain) {
		t.Fatalf("in-progress provider status was invented as final: %#v", insert)
	}
}

func TestCommitEnvelopeAtomicallyReplacesAuthenticatedLineSnapshotUnderInboxFence(t *testing.T) {
	lines := []ingress.ProjectedLine{
		projectedSettingsLine("line-a", "participant-a", "+12025550101", 1),
		projectedSettingsLine("line-b", "participant-b", "+12025550102", 2),
	}
	record := ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "settings-response",
		OwnerID: "owner-current", FencingToken: 17, Raw: []byte("settings-raw"), Digest: sha256.Sum256([]byte("settings-raw")),
		ACKPending: true, ReceivedAt: time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC),
		Projection: ingress.Projection{LineSnapshot: true, Lines: lines},
	}
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "settings-inbox" }
	if result, err := repository.CommitEnvelope(context.Background(), record); err != nil || result != ingress.CommitInserted {
		t.Fatalf("settings commit = (%v, %v), calls=%v", result, err, tx.operationNames())
	}
	want := []string{"tenant_context", "verify_inbox_fence", "get_provider_inbox", "insert_provider_inbox", "upsert_line", "upsert_line", "retire_lines"}
	if got := tx.operationNames(); !reflect.DeepEqual(got, want) {
		t.Fatalf("atomic settings operations = %v, want %v", got, want)
	}
	first := tx.findCall("upsert_line")
	if first == nil || !reflect.DeepEqual(first.args, []any{
		"tenant-a", "line-a", "connection-a", "participant-a", "participant-a", "+12025550101", "Carrier", "Carrier",
		"#123456", true, int32(1), int32(7), string(LineDiscoveryAuthenticatedGoogleSettings),
	}) {
		t.Fatalf("durable line upsert args = %#v", first)
	}

	insertFailure := errors.New("line upsert unavailable")
	failing := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{
			"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
		},
		execErrors: map[string][]error{"upsert_line": {insertFailure}},
	}
	failingRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{failing}})
	failingRepository.newID = func() string { return "settings-inbox-failing" }
	if result, err := failingRepository.CommitEnvelope(context.Background(), record); result != 0 || !errors.Is(err, insertFailure) {
		t.Fatalf("failed atomic settings commit = (%v, %v)", result, err)
	}
	if !failing.rolledBack || failing.committed {
		t.Fatalf("failed settings transaction committed=%v rolledBack=%v", failing.committed, failing.rolledBack)
	}

	stale := &fakeTransaction{rowResults: map[string][]fakeRowResult{"verify_inbox_fence": {{values: []any{false}}}}}
	staleRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{stale}})
	if result, err := staleRepository.CommitEnvelope(context.Background(), record); result != 0 || !errors.Is(err, ErrConnectionLeaseLost) {
		t.Fatalf("stale settings commit = (%v, %v)", result, err)
	}
	if stale.findCall("insert_provider_inbox") != nil || stale.findCall("upsert_line") != nil || stale.findCall("retire_lines") != nil || !stale.rolledBack {
		t.Fatalf("stale settings mutated rows: %v", stale.operationNames())
	}
}

func TestCommitEnvelopeTerminalSettingsPoisonNeverEntersACKRecovery(t *testing.T) {
	raw := []byte("impossible-settings")
	digest := sha256.Sum256(raw)
	record := ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "settings-poison",
		OwnerID: "owner-a", FencingToken: 7, Raw: raw, Digest: digest, ACKPending: true, ACKWithheld: true,
		Poisoned: true, PoisonReason: ingress.PoisonReasonInvalidSettingsSnapshot,
		ReceivedAt: time.Date(2026, 8, 25, 16, 30, 0, 0, time.UTC),
	}
	first := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{first}})
	repository.newID = func() string { return "settings-poison-inbox" }
	if result, err := repository.CommitEnvelope(context.Background(), record); err != nil || result != ingress.CommitPoisoned {
		t.Fatalf("terminal settings poison = (%v, %v)", result, err)
	}
	insert := first.findCall("insert_provider_inbox")
	if insert == nil || len(insert.args) != 14 || insert.args[13] != true {
		t.Fatalf("terminal poison insert args = %#v", insert)
	}
	if query := strings.ToLower(insert.query); !strings.Contains(query, "not $14") {
		t.Fatalf("terminal poison can enter ACK recovery: %s", query)
	}

	replay := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}},
		"get_provider_inbox": {{values: []any{digest[:], "inbox", false, true, ingress.PoisonReasonInvalidSettingsSnapshot}}},
	}}
	replayRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{replay}})
	replayRepository.newID = func() string { t.Fatal("terminal exact replay must not reopen ACK or allocate"); return "" }
	if result, err := replayRepository.CommitEnvelope(context.Background(), record); err != nil || result != ingress.CommitDuplicateACKWithheld {
		t.Fatalf("terminal settings replay = (%v, %v)", result, err)
	}
	if replay.findCall("reopen_provider_ack") != nil {
		t.Fatalf("terminal settings replay reopened ACK: %v", replay.operationNames())
	}

	// A restarted binary may decode the same bytes differently after a protocol
	// update. The durable terminal reason, rather than the new in-memory
	// projection, remains authoritative and must keep the response out of ACK
	// recovery.
	recovered := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}},
		"get_provider_inbox": {{values: []any{digest[:], "inbox", false, true, ingress.PoisonReasonInvalidSettingsSnapshot}}},
	}}
	recoveredRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{recovered}})
	recoveredRepository.newID = func() string { t.Fatal("durably terminal replay must not allocate"); return "" }
	recoveredRecord := record
	recoveredRecord.ACKWithheld = false
	recoveredRecord.Poisoned = false
	recoveredRecord.PoisonReason = ""
	if result, err := recoveredRepository.CommitEnvelope(context.Background(), recoveredRecord); err != nil || result != ingress.CommitDuplicateACKWithheld {
		t.Fatalf("redecoded terminal settings replay = (%v, %v)", result, err)
	}
	if recovered.findCall("reopen_provider_ack") != nil {
		t.Fatalf("redecoded terminal Settings replay reopened ACK: %v", recovered.operationNames())
	}
}

func projectedSettingsLine(id, participant, phone string, number int32) ingress.ProjectedLine {
	return ingress.ProjectedLine{
		ID: domain.LineID(id), TenantID: "tenant-a", ConnectionID: "connection-a",
		ProviderParticipantID: participant, ProviderOutgoingID: participant, Phone: phone,
		DisplayName: "Carrier", CarrierName: "Carrier", ColorHex: "#123456", RCSEnabled: true,
		ProviderSIMNumber: number, ProviderSIMPayloadType: 7, DiscoverySource: ingress.LineDiscoveryAuthenticatedGoogleSettings,
	}
}

func liveInboundEnvelopeRecord(responseID, candidateEventID string, providerTime time.Time) ingress.EnvelopeRecord {
	receivedAt := providerTime.Add(time.Minute)
	return ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: responseID,
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-" + responseID), ACKPending: true,
		Digest: sha256.Sum256([]byte("raw-" + responseID)), ReceivedAt: receivedAt,
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-semantic", ConversationID: "conversation-a", Direction: "inbound",
			Provenance: ingress.MessageProvenanceLive, ProviderStatus: "INCOMING_COMPLETE", ProviderOccurredAt: providerTime,
			Actionable: true, Sender: "+12025550100", Text: "hello", Transport: "sms", State: domain.MessageStateDelivered,
		}}},
		Events: []ingress.OutboxEvent{{
			ID: domain.EventID(candidateEventID), Type: "message.received", AggregateID: "provider-semantic",
			CanonicalBody: []byte(`{"type":"message.received"}`), PartitionConversation: "conversation-a",
		}},
	}
}

func filterOperation(operations []string, wanted string) []string {
	filtered := make([]string, 0, len(operations))
	for _, operation := range operations {
		if operation == wanted {
			filtered = append(filtered, operation)
		}
	}
	return filtered
}

func TestRepositoryRejectsInvalidProviderResponseIDsBeforeTransaction(t *testing.T) {
	db := &fakeBeginner{}
	repository := newRepository(db)
	validRecord := ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", OwnerID: "owner-a", FencingToken: 7,
		ProviderResponseID: "response-a", Raw: []byte("raw"), Digest: sha256.Sum256([]byte("raw")), ACKPending: true,
	}
	for _, responseID := range []string{strings.Repeat("r", 257), "response\x00id", "response\nid", " response"} {
		record := validRecord
		record.ProviderResponseID = responseID
		if _, err := repository.CommitEnvelope(context.Background(), record); !errors.Is(err, domain.ErrInvalidIdentifier) {
			t.Fatalf("CommitEnvelope invalid response ID %q error = %v", responseID, err)
		}
		if _, err := repository.MarkProviderACKedFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7, []string{responseID}); !errors.Is(err, domain.ErrInvalidIdentifier) {
			t.Fatalf("MarkProviderACKedFenced invalid response ID %q error = %v", responseID, err)
		}
	}
	if db.beginCalls != 0 {
		t.Fatalf("invalid response IDs began %d transactions", db.beginCalls)
	}
}

func TestCommitEnvelopeRoutesEmptyListMessagesCursorToExplicitConversation(t *testing.T) {
	cursor := providerCursorBytes(t, "next-message-page", 2)
	cursorDigest := sha256.Sum256(cursor)
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":               {{values: []any{true}}},
		"get_provider_inbox":               {{err: sql.ErrNoRows}},
		"lock_provider_cursor_budget":      {{values: []any{0, false}}},
		"find_provider_cursor_history":     {{err: sql.ErrNoRows}},
		"insert_provider_cursor_history":   {{values: []any{cursorDigest[:], []byte(nil), sql.NullString{String: "response-empty-page", Valid: true}}}},
		"increment_provider_cursor_budget": {{values: []any{1}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "inbox-a" }
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-empty-page",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{3}, ACKPending: true,
		Projection: ingress.Projection{
			Cursor:               cursor,
			CursorSource:         ingress.CursorSourceListMessages,
			CursorConversationID: "conversation-a",
		},
	})
	if err != nil || result != ingress.CommitInserted {
		t.Fatalf("CommitEnvelope() = (%v, %v)", result, err)
	}
	advance := tx.findCall("advance_conversation_cursor")
	if advance == nil || advance.args[2] != "conversation-a" || !bytes.Equal(advance.args[3].([]byte), cursor) {
		t.Fatalf("empty ListMessages cursor target = %+v", advance)
	}
	if advance.args[2] == "_provider_page" {
		t.Fatal("empty ListMessages page corrupted the parent conversation-list cursor")
	}
}

func TestCommitEnvelopePersistsBoundedCursorHistoryAndPoisonsRestartCycle(t *testing.T) {
	cursorA, cursorB := providerCursorBytes(t, "cursor-a", 1), providerCursorBytes(t, "cursor-b", 2)
	digestA := sha256.Sum256(cursorA)
	digestB := sha256.Sum256(cursorB)
	first := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
		"lock_provider_cursor_budget":      {{values: []any{0, false}}},
		"find_provider_cursor_history":     {{err: sql.ErrNoRows}},
		"insert_provider_cursor_history":   {{values: []any{digestB[:], digestA[:], sql.NullString{String: "response-a-b", Valid: true}}}},
		"increment_provider_cursor_budget": {{values: []any{1}}},
	}}
	firstRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{first}})
	firstRepository.newID = func() string { return "inbox-first" }
	result, err := firstRepository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-a-b",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-a-b"), Digest: [32]byte{1}, ACKPending: true,
		Projection: ingress.Projection{Cursor: cursorB, CursorBase: cursorA, CursorCandidate: cursorB,
			CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a"},
	})
	if err != nil || result != ingress.CommitInserted {
		t.Fatalf("first cursor transition = (%v, %v)", result, err)
	}
	if first.findCall("seed_provider_cursor_history") == nil || first.findCall("prune_provider_cursor_history") == nil || first.findCall("advance_conversation_cursor") == nil {
		t.Fatalf("first cursor calls = %v", first.operationNames())
	}
	if call := first.findCall("increment_provider_cursor_budget"); call == nil || call.args[4] != ingress.MaxProviderCursorAdvances {
		t.Fatalf("new cursor edge budget call = %+v", call)
	}
	if query := strings.ToLower(first.lastQuery("prune_provider_cursor_history")); !strings.Contains(query, "offset 64") {
		t.Fatalf("cursor eviction is not bounded to 64: %s", query)
	}
	replay := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
		"lock_provider_cursor_budget":  {{values: []any{1, false}}},
		"find_provider_cursor_history": {{values: []any{digestA[:], sql.NullString{String: "response-a-b", Valid: true}}}},
	}}
	replayRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{replay}})
	replayRepository.newID = func() string { return "inbox-replay" }
	result, err = replayRepository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-a-b-retry",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-a-b-retry"), Digest: [32]byte{9}, ACKPending: true,
		Projection: ingress.Projection{Cursor: cursorB, CursorBase: cursorA, CursorCandidate: cursorB,
			CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a"},
	})
	if err != nil || result != ingress.CommitInserted || replay.findCall("advance_conversation_cursor") == nil {
		t.Fatalf("same cursor edge replay = (%v, %v), calls=%v", result, err, replay.operationNames())
	}
	if call := replay.findCall("increment_provider_cursor_budget"); call != nil {
		t.Fatalf("replayed cursor edge consumed budget: %+v", call)
	}

	// A new Repository represents a process restart. PostgreSQL reports the
	// previously committed A fingerprint as a conflict for B -> A.
	second := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
		"lock_provider_cursor_budget":  {{values: []any{1, false}}},
		"find_provider_cursor_history": {{values: []any{[]byte(nil), sql.NullString{}}}},
		"finalize_provider_poison":     {{err: sql.ErrNoRows}},
		"compact_provider_poison":      {{values: []any{"rejected"}}},
	}}
	secondRepository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{second}})
	ids := []string{"inbox-second", "outbox-second"}
	secondRepository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	result, err = secondRepository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-b-a",
		OwnerID: "owner-a", FencingToken: 8, Raw: []byte("raw-b-a"), Digest: [32]byte{2}, ACKPending: true,
		Projection: ingress.Projection{Cursor: cursorA, CursorBase: cursorB, CursorCandidate: cursorA,
			CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a"},
	})
	if err != nil || result != ingress.CommitPoisoned || second.findCall("advance_conversation_cursor") != nil {
		t.Fatalf("restart cursor cycle = (%v, %v), calls=%v", result, err, second.operationNames())
	}
	if call := second.findCall("finalize_provider_poison"); call == nil || call.args[4] != ingress.MaxPoisonedProviderInboxEntries ||
		call.args[5] != ingress.MaxPoisonedProviderInboxBytes {
		t.Fatalf("cursor poison did not use shared row/byte/rejected admission: %+v", call)
	}
	if call := second.findCall("compact_provider_poison"); call == nil || call.args[4] != ingress.MaxRejectedProviderResponses {
		t.Fatalf("cursor poison did not use bounded compact ACK audit: %+v", call)
	}
}

func TestCommitEnvelopePoisonsWhenDurableCursorAdvanceBudgetIsExhausted(t *testing.T) {
	base := providerCursorBytes(t, "cursor-base", 255)
	candidate := providerCursorBytes(t, "cursor-next", 256)
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":             {{values: []any{true}}},
		"get_provider_inbox":             {{err: sql.ErrNoRows}},
		"lock_provider_cursor_budget":    {{values: []any{ingress.MaxProviderCursorAdvances, false}}},
		"find_provider_cursor_history":   {{err: sql.ErrNoRows}},
		"exhaust_provider_cursor_budget": {{values: []any{true}}},
		"finalize_provider_poison":       {{values: []any{"inbox"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-over-budget", "outbox-over-budget"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-over-budget",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-over-budget"), Digest: [32]byte{8}, ACKPending: true,
		Projection: ingress.Projection{
			Cursor: candidate, CursorBase: base, CursorCandidate: candidate,
			CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a",
		},
	})
	if err != nil || result != ingress.CommitPoisoned {
		t.Fatalf("over-budget transition = (%v, %v)", result, err)
	}
	if tx.findCall("finalize_provider_poison") == nil || tx.findCall("insert_gateway_event") == nil ||
		tx.findCall("insert_event_outbox") == nil || tx.findCall("advance_conversation_cursor") != nil ||
		tx.findCall("prune_provider_cursor_history") != nil || tx.findCall("insert_provider_cursor_history") != nil {
		t.Fatalf("over-budget calls = %v", tx.operationNames())
	}
	wantPrefix := []string{
		"tenant_context", "verify_inbox_fence", "ensure_provider_cursor_budget", "lock_provider_cursor_budget",
		"get_provider_inbox", "insert_provider_inbox", "find_provider_cursor_history", "exhaust_provider_cursor_budget",
	}
	if got := tx.operationNames(); len(got) < len(wantPrefix) || !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("budget precheck order = %v, want prefix %v", got, wantPrefix)
	}
}

func TestCommitEnvelopeStickyCursorBudgetNeverAddsHistoryOrRepeatedAlerts(t *testing.T) {
	base := providerCursorBytes(t, "cursor-exhausted-base", 300)
	for index := range 300 {
		candidate := providerCursorBytes(t, fmt.Sprintf("cursor-rejected-%03d", index), int64(301+index))
		rawDigest := sha256.Sum256([]byte(fmt.Sprintf("raw-%03d", index)))
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
			"verify_inbox_fence":                {{values: []any{true}}},
			"get_provider_inbox":                {{err: sql.ErrNoRows}},
			"lock_provider_cursor_budget":       {{values: []any{ingress.MaxProviderCursorAdvances, true}}},
			"upsert_rejected_provider_response": {{values: []any{rawDigest[:], false, int64(1)}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		repository.newID = func() string { t.Fatal("sticky rejection must not allocate a raw inbox ID"); return "" }
		result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
			TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: fmt.Sprintf("response-rejected-%03d", index),
			OwnerID: "owner-a", FencingToken: 7, Raw: []byte(fmt.Sprintf("raw-%03d", index)), Digest: sha256.Sum256([]byte(fmt.Sprintf("raw-%03d", index))), ACKPending: true,
			Projection: ingress.Projection{Cursor: candidate, CursorBase: base, CursorCandidate: candidate,
				CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a"},
		})
		if err != nil || result != ingress.CommitPoisoned {
			t.Fatalf("rejected transition %d = (%v, %v)", index, result, err)
		}
		for _, forbidden := range []string{"insert_provider_inbox", "find_provider_cursor_history", "seed_provider_cursor_history", "insert_provider_cursor_history", "prune_provider_cursor_history", "advance_conversation_cursor", "insert_gateway_event", "insert_event_outbox"} {
			if tx.findCall(forbidden) != nil {
				t.Fatalf("sticky exhaustion transition %d performed %s: %v", index, forbidden, tx.operationNames())
			}
		}
		want := []string{"tenant_context", "verify_inbox_fence", "ensure_provider_cursor_budget", "lock_provider_cursor_budget", "get_provider_inbox", "upsert_rejected_provider_response"}
		if got := tx.operationNames(); !reflect.DeepEqual(got, want) {
			t.Fatalf("sticky exhaustion transition %d calls = %v, want %v", index, got, want)
		}
		query := strings.ToLower(tx.lastQuery("upsert_rejected_provider_response"))
		for _, required := range []string{"provider_response_reservations", "provider_rejected_responses", "count(*)", "< $6"} {
			if !strings.Contains(query, required) {
				t.Fatalf("sticky rejection is not globally reserved and capped (%q missing): %s", required, query)
			}
		}
		base = candidate
	}
}

func TestCommitEnvelopeRejectedReservationWinsAcrossActionAndScope(t *testing.T) {
	digest := sha256.Sum256([]byte("raw-rejected"))
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}},
		// The authoritative lookup must materialize an earlier rejected identity
		// even though this replay is a non-pagination response.
		"get_provider_inbox": {{values: []any{digest[:], "rejected", true, true, "provider_cursor_budget_exhausted"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-rejected-first",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-rejected"), Digest: digest, ACKPending: true,
	})
	if err != nil || result != ingress.CommitDuplicatePoisoned {
		t.Fatalf("cross-action exact replay = (%v, %v)", result, err)
	}
	if tx.findCall("insert_provider_inbox") != nil {
		t.Fatal("rejected-first response ID later entered the raw inbox")
	}
	query := strings.ToLower(tx.lastQuery("get_provider_inbox"))
	for _, required := range []string{"provider_response_reservations", "provider_rejected_responses", "disposition"} {
		if !strings.Contains(query, required) {
			t.Fatalf("authoritative response lookup missing %q: %s", required, query)
		}
	}
	if strings.Contains(query, "left join") && strings.Contains(query, "for update") {
		t.Fatalf("authoritative lookup locks the nullable side of an outer join: %s", query)
	}
}

func TestProviderInboxLookupUsesOnlyTheLockedIdentityAndBlocksConflictedRows(t *testing.T) {
	query := strings.ToLower(getProviderInboxSQL)
	for _, required := range []string{
		"with locked_reservation as materialized",
		"from locked_reservation",
		"and not conflicted",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("authoritative response lookup missing %q: %s", required, query)
		}
	}
	if strings.Contains(query, "from provider_response_reservations as reservation") {
		t.Fatalf("outer lookup can shadow the locked CTE and consume another same-tenant identity: %s", query)
	}
}

func TestCommitEnvelopeRejectedCapacityFailsClosedWithoutNewReservation(t *testing.T) {
	candidate := providerCursorBytes(t, "cursor-over-rejected-cap", 900)
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":          {{values: []any{true}}},
		"lock_provider_cursor_budget": {{values: []any{ingress.MaxProviderCursorAdvances, true}}},
		"get_provider_inbox":          {{err: sql.ErrNoRows}},
		// No row means the CTE observed the fixed reservation cap before it
		// inserted either the reservation or rejected ACK identity.
		"upsert_rejected_provider_response": {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-over-rejected-cap",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: sha256.Sum256([]byte("raw")), ACKPending: true,
		Projection: ingress.Projection{Cursor: candidate, CursorCandidate: candidate,
			CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a"},
	})
	if result != 0 || !errors.Is(err, ingress.ErrProviderResponseCapacity) {
		t.Fatalf("over-cap rejection = (%v, %v), want local capacity failure", result, err)
	}
	for _, forbidden := range []string{"insert_provider_inbox", "insert_provider_inbox_conflict", "insert_gateway_event", "insert_event_outbox"} {
		if tx.findCall(forbidden) != nil {
			t.Fatalf("over-cap rejection performed %s: %v", forbidden, tx.operationNames())
		}
	}
}

func TestProviderInboxConflictStorageIsOneBoundedRecordPerResponseID(t *testing.T) {
	query := strings.ToLower(insertProviderInboxConflictSQL)
	for _, required := range []string{
		"on conflict (tenant_id, connection_id, provider_response_id)",
		"occurrence_count", "least(", "conflicting_envelope_size",
		"octet_length($6::bytea)", "substring($6::bytea from 1 for 256)",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("bounded conflict SQL missing %q: %s", required, query)
		}
	}
	if strings.Contains(query, "provider_response_id, conflicting_digest) do nothing") {
		t.Fatalf("conflict SQL permits one raw row per attacker-chosen digest: %s", query)
	}
}

func TestPoisonInboxReservationEnforcesRowAndCumulativeRawByteCaps(t *testing.T) {
	query := strings.ToLower(insertProviderInboxSQL)
	for _, required := range []string{
		"provider_response_reservations", "poison_inbox.poisoned", "count(*)",
		"sum(octet_length(poison_inbox.raw_envelope))", "octet_length($6::bytea)",
		"< $12", "<= $13",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("poison inbox reservation SQL missing %q: %s", required, query)
		}
	}
}

func TestCommitEnvelopeStickyCursorBudgetRejectionConvergesAndConflictsWithoutRawInbox(t *testing.T) {
	digest := sha256.Sum256([]byte("raw-rejected"))
	record := ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-rejected",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw-rejected"), Digest: digest, ACKPending: true,
		Projection: ingress.Projection{Cursor: providerCursorBytes(t, "cursor-rejected", 301),
			CursorSource: ingress.CursorSourceListMessages, CursorConversationID: "conversation-a"},
	}
	for name, fixture := range map[string]struct {
		storedDigest [32]byte
		conflicted   bool
		occurrences  int64
		want         ingress.CommitResult
	}{
		"first":           {storedDigest: digest, occurrences: 1, want: ingress.CommitPoisoned},
		"exact replay":    {storedDigest: digest, occurrences: 2, want: ingress.CommitDuplicatePoisoned},
		"different bytes": {storedDigest: digest, conflicted: true, occurrences: 3, want: ingress.CommitConflict},
	} {
		t.Run(name, func(t *testing.T) {
			tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
				"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
				"lock_provider_cursor_budget":       {{values: []any{ingress.MaxProviderCursorAdvances, true}}},
				"upsert_rejected_provider_response": {{values: []any{fixture.storedDigest[:], fixture.conflicted, fixture.occurrences}}},
			}}
			candidate := record
			if fixture.conflicted {
				candidate.Raw = []byte("different")
				candidate.Digest = sha256.Sum256(candidate.Raw)
			}
			result, err := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}}).CommitEnvelope(context.Background(), candidate)
			if err != nil || result != fixture.want {
				t.Fatalf("rejected response = (%v, %v), want %v", result, err, fixture.want)
			}
			if tx.findCall("insert_provider_inbox") != nil {
				t.Fatal("sticky rejection persisted a raw provider envelope")
			}
			query := strings.ToLower(tx.lastQuery("upsert_rejected_provider_response"))
			if !strings.Contains(query, "acked_at = case") || !strings.Contains(query, "then null") ||
				!strings.Contains(query, "ack_pending = not excluded.conflicted") ||
				!strings.Contains(query, "provider_response_reservations.envelope_digest <> excluded.envelope_digest") {
				t.Fatalf("exact rejected redelivery is not made ACK-pending again: %s", query)
			}
		})
	}
}

func TestCommitEnvelopeReconcilesReceiptHistoryWithoutRegressingCurrentState(t *testing.T) {
	tx := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{
			"verify_inbox_fence":     {{values: []any{true}}},
			"get_provider_inbox":     {{err: sql.ErrNoRows}},
			"lock_projected_message": {{values: []any{"message-a", "sent", "outbound"}}},
		},
		rowsResult: map[string][][]any{
			"list_message_states": {{"queued"}, {"sent"}},
		},
	}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-a", "status-a", "outbox-transition", "outbox-updated"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-delivered",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{3}, ACKPending: true,
		ReceivedAt: time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-a", ProviderTmpID: "sx-temp", ConversationID: "conversation-a",
			Direction: "outbound", Provenance: ingress.MessageProvenanceLive, ProviderStatus: "OUTGOING_DELIVERED",
			ProviderOccurredAt: time.Date(2026, 8, 25, 14, 59, 0, 0, time.UTC), Transport: "rcs", State: domain.MessageStateDelivered,
		}}},
		Events: []ingress.OutboxEvent{{ID: "event-generic", Type: "message.updated", AggregateID: "provider-a", CanonicalBody: []byte(`{"type":"message.updated"}`)}},
	})
	if err != nil || result != ingress.CommitInserted {
		t.Fatalf("CommitEnvelope() = (%v, %v)", result, err)
	}
	update := tx.findCall("update_projected_message")
	if update == nil || update.args[4] != "provider-a" || update.args[5] != "rcs" || update.args[6] != "delivered" {
		t.Fatalf("reconciled message update = %+v", update)
	}
	for _, required := range []string{"for update", "provider_tmp_id", "provider_message_id"} {
		if query := strings.ToLower(tx.lastQuery("lock_projected_message")); !strings.Contains(query, required) {
			t.Fatalf("message receipt lock missing %q: %s", required, query)
		}
	}
	event := tx.findCall("insert_gateway_event")
	if event == nil || event.args[2] != "message.delivered" || event.args[3] != "message" || event.args[4] != "message-a" {
		t.Fatalf("receipt transition event = %+v", event)
	}
	var body map[string]any
	if err = json.Unmarshal(event.args[7].([]byte), &body); err != nil {
		t.Fatalf("decode receipt event body: %v", err)
	}
	if body["type"] != "message.delivered" || body["version"] != float64(1) || body["tenant_id"] != "tenant-a" ||
		body["connection_id"] != "connection-a" || body["conversation_id"] != "conversation-a" || body["direction"] != "outbound" ||
		body["status"] != "delivered" || body["state"] != "delivered" || body["message_id"] != "message-a" ||
		body["provider_message_id"] != "provider-a" || body["provider_status"] != "OUTGOING_DELIVERED" ||
		body["occurred_at"] != "2026-08-25T14:59:00Z" || body["ingested_at"] != "2026-08-25T15:00:00Z" {
		t.Fatalf("receipt event body = %#v", body)
	}
	eventCount := 0
	for index := range tx.calls {
		if tx.calls[index].operation == "insert_gateway_event" {
			eventCount++
		}
	}
	if eventCount != 2 {
		t.Fatalf("receipt emitted %d events, want transition plus non-actionable enrichment", eventCount)
	}
	updatedCount, outboxCount := 0, 0
	for _, call := range tx.calls {
		if call.operation == "insert_event_outbox" {
			outboxCount++
		}
		if call.operation != "insert_gateway_event" || call.args[2] != "message.updated" {
			continue
		}
		updatedCount++
		var enrichment map[string]any
		if err = json.Unmarshal(call.args[7].([]byte), &enrichment); err != nil || enrichment["actionable"] == true || enrichment["direction"] != "outbound" {
			t.Fatalf("outbound enrichment event = %#v, error=%v", enrichment, err)
		}
	}
	if updatedCount != 1 || outboxCount != 2 {
		t.Fatalf("receipt enrichment=%d outboxes=%d", updatedCount, outboxCount)
	}
}

func TestCommitEnvelopeOutOfOrderReceiptDoesNotEmitRegressingEvent(t *testing.T) {
	tx := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{
			"verify_inbox_fence":     {{values: []any{true}}},
			"get_provider_inbox":     {{err: sql.ErrNoRows}},
			"lock_projected_message": {{values: []any{"message-a", "read", "outbound"}}},
		},
		rowsResult: map[string][][]any{"list_message_states": {{"sent"}, {"delivered"}, {"read"}}},
	}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-a", "status-a", "outbox-updated"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-old",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{4}, ACKPending: true,
		ReceivedAt: time.Date(2026, 8, 25, 15, 1, 0, 0, time.UTC),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-a", ProviderTmpID: "sx-temp", ConversationID: "conversation-a",
			Direction: "outbound", Provenance: ingress.MessageProvenanceLive, ProviderStatus: "OUTGOING_COMPLETE", State: domain.MessageStateSent,
		}}},
		Events: []ingress.OutboxEvent{{ID: "generic", Type: "message.updated", AggregateID: "provider-a", CanonicalBody: []byte(`{"type":"message.updated"}`)}},
	})
	if err != nil || result != ingress.CommitInserted {
		t.Fatalf("CommitEnvelope() = (%v, %v)", result, err)
	}
	if event := tx.findCall("insert_gateway_event"); event == nil || event.args[2] != "message.updated" {
		t.Fatalf("out-of-order receipt enrichment event = %+v", event)
	}
	if update := tx.findCall("update_projected_message"); update == nil || update.args[6] != "read" {
		t.Fatalf("out-of-order receipt regressed current state: %+v", update)
	}
}

func TestCommitEnvelopeAssociatesEveryInboundAttachmentInProviderOrder(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":       {{values: []any{true}}},
		"get_provider_inbox":       {{err: sql.ErrNoRows}},
		"lock_existing_attachment": {{err: sql.ErrNoRows}, {err: sql.ErrNoRows}},
		"lock_projected_message":   {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-a", "message-a", "status-a", "media-a", "job-a", "media-b", "job-b"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	_, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-a",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{3}, ACKPending: true,
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-a", ConversationID: "conversation-a", State: domain.MessageStateDelivered,
		}}},
		Media: []ingress.MediaLocator{
			{ProviderMessageID: "provider-a", Locator: "gmessages:first", Position: 0},
			{ProviderMessageID: "provider-a", Locator: "gmessages:second", Position: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var links []*fakeCall
	for index := range tx.calls {
		if tx.calls[index].operation == "insert_inbound_media_link" {
			links = append(links, &tx.calls[index])
		}
	}
	if len(links) != 2 || links[0].args[1] != "message-a" || links[0].args[3] != 0 || links[1].args[3] != 1 {
		t.Fatalf("inbound attachment links = %+v", links)
	}
}

func TestCommitEnvelopeReusesAttachmentAcrossResponseIDsAndOutboundEcho(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":       {{values: []any{true}}},
		"get_provider_inbox":       {{err: sql.ErrNoRows}},
		"lock_existing_attachment": {{values: []any{"media-original", "ready", "outbound", []byte{}}}},
		"lock_projected_message":   {{values: []any{"message-outbound", "sent", "outbound"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-a", "status-a"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	locator := ingress.MediaLocator{ProviderMessageID: "provider-a", Locator: "gmessages:same", Position: 0}
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "different-response",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{5}, ACKPending: true,
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-a", ProviderTmpID: "sx-outbound", ConversationID: "conversation-a", State: domain.MessageStateSent,
		}}},
		Media:  []ingress.MediaLocator{locator},
		Events: []ingress.OutboxEvent{{ID: "media-stable", Type: "media.pending", AggregateID: ingress.AttachmentAggregateID(locator), CanonicalBody: []byte(`{"type":"media.pending"}`)}},
	})
	if err != nil || result != ingress.CommitInserted {
		t.Fatalf("CommitEnvelope() = (%v, %v)", result, err)
	}
	bound := tx.findCall("bind_outbound_attachment_identity")
	if bound == nil || len(bound.args) != 6 || bound.args[5] == nil {
		t.Fatalf("first outbound echo did not persist provider attachment identity: %+v", bound)
	}
	for _, operation := range []string{"insert_media_object", "insert_inbound_media_link", "insert_media_fetch_job", "insert_gateway_event"} {
		if call := tx.findCall(operation); call != nil {
			t.Fatalf("exact attachment redelivery performed %s: %+v", operation, call)
		}
	}
}

func TestCommitEnvelopeQuarantinesChangedLocatorAtOccupiedPositionAndCommitsForACK(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}},
		"get_provider_inbox": {{err: sql.ErrNoRows}},
		// A previous outbound echo already bound a different identity digest.
		"lock_existing_attachment": {{values: []any{"media-original", "ready", "outbound", make([]byte, 32)}}},
		"finalize_provider_poison": {{err: sql.ErrNoRows}},
		"compact_provider_poison":  {{values: []any{"rejected"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids := []string{"inbox-a", "outbox-a"}
	repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "locator-conflict-response",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{6}, ACKPending: true,
		ReceivedAt: time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC),
		Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{
			ProviderMessageID: "provider-a", ProviderTmpID: "sx-outbound", ConversationID: "conversation-a", State: domain.MessageStateDelivered,
		}}},
		Media: []ingress.MediaLocator{{ProviderMessageID: "provider-a", Locator: "gmessages:changed", Position: 0}},
	})
	if err != nil || result != ingress.CommitPoisoned {
		t.Fatalf("locator conflict = (%v, %v), want committed poison eligible for ACK", result, err)
	}
	poison := tx.findCall("finalize_provider_poison")
	if poison == nil || poison.args[3] != "media_identity_conflict" {
		t.Fatalf("durable locator conflict poison = %+v", poison)
	}
	if tx.findCall("lock_projected_message") != nil || tx.findCall("insert_media_object") != nil {
		t.Fatal("locator conflict projected provider-controlled data before quarantine")
	}
	event := tx.findCall("insert_gateway_event")
	if event == nil || event.args[2] != "inbox.poisoned" {
		t.Fatalf("locator conflict alert event = %+v", event)
	}
	var body map[string]any
	if err = json.Unmarshal(event.args[7].([]byte), &body); err != nil || body["version"] != float64(1) || body["tenant_id"] != "tenant-a" ||
		body["connection_id"] != "connection-a" || body["provider_response_id"] != "locator-conflict-response" ||
		body["reason"] != "media_identity_conflict" || body["occurred_at"] != "2026-08-25T16:00:00Z" {
		t.Fatalf("poison event contract = %#v, error %v", body, err)
	}
}

func TestFinalPoisonDispositionAtomicallyBoundsRawEvidenceOrMovesToBoundedACKAudit(t *testing.T) {
	query := strings.ToLower(finalizeProviderPoisonSQL + compactProviderPoisonSQL)
	for _, required := range []string{
		"provider_inbox", "poisoned = true", "count(*)", "sum(octet_length",
		"< $5", "<= $6", "delete from provider_inbox",
		"provider_rejected_responses", "provider_response_reservations",
		"disposition = 'rejected'", "rejected_usage.rows_used < $5",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("final poison admission missing %q: %s", required, query)
		}
	}
}

func TestCommitEnvelopeQuarantinesChangedKeyOrDeclaredMetadataAtOccupiedPosition(t *testing.T) {
	original := ingress.MediaLocator{ProviderMessageID: "provider-a", Locator: "gmessages:same", Position: 0, MIMEType: "image/png", DeclaredSize: 4,
		KeyEnvelope: session.Envelope{Version: 1, Provider: "gmessages-media", Ciphertext: bytes.Repeat([]byte{1}, 16), WrappedDEK: []byte{2}, Nonce: make([]byte, 12), KeyID: "kms-a", KeyVersion: 1}}
	originalIdentity := ingress.AttachmentIdentityDigest(original)
	for name, mutate := range map[string]func(*ingress.MediaLocator){
		"sealed key": func(locator *ingress.MediaLocator) { locator.KeyEnvelope.Ciphertext[0] = 9 },
		"MIME":       func(locator *ingress.MediaLocator) { locator.MIMEType = "image/jpeg" },
		"size":       func(locator *ingress.MediaLocator) { locator.DeclaredSize++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := original
			changed.KeyEnvelope = original.KeyEnvelope.Clone()
			mutate(&changed)
			tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
				"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
				"lock_existing_attachment": {{values: []any{"media-original", "pending", "inbound", originalIdentity[:]}}},
				"finalize_provider_poison": {{values: []any{"inbox"}}},
			}}
			repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
			ids := []string{"inbox-a", "outbox-a"}
			repository.newID = func() string { id := ids[0]; ids = ids[1:]; return id }
			result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
				TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-changed", OwnerID: "owner-a", FencingToken: 7,
				Raw: []byte("raw"), Digest: [32]byte{8}, ACKPending: true,
				Projection: ingress.Projection{Messages: []ingress.ProjectedMessage{{ProviderMessageID: "provider-a", ConversationID: "conversation-a"}}},
				Media:      []ingress.MediaLocator{changed},
			})
			if err != nil || result != ingress.CommitPoisoned || tx.findCall("finalize_provider_poison") == nil {
				t.Fatalf("changed attachment = (%v, %v), calls=%+v", result, err, tx.calls)
			}
		})
	}
}

func TestCommitEnvelopeCommitFailureReturnsNoACKEligibility(t *testing.T) {
	commitFailure := errors.New("commit failed")
	tx := &fakeTransaction{commitErr: commitFailure, rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "inbox-a" }
	_, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-a",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: [32]byte{1}, ACKPending: true,
	})
	if !errors.Is(err, commitFailure) || tx.committed {
		t.Fatalf("CommitEnvelope() error=%v committed=%v", err, tx.committed)
	}
}

func TestCommitEnvelopeExactDuplicateConvergesAndConflictIsQuarantined(t *testing.T) {
	exactDigest := [32]byte{1, 2}
	exactTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{values: []any{exactDigest[:], "inbox", true, false, ""}}},
	}}
	exactRepo := newRepository(&fakeBeginner{transactions: []*fakeTransaction{exactTX}})
	exact, err := exactRepo.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-a",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("raw"), Digest: exactDigest, ACKPending: true,
	})
	if err != nil || exact != ingress.CommitDuplicate {
		t.Fatalf("exact duplicate = (%v, %v)", exact, err)
	}

	conflictTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}}, "get_provider_inbox": {{values: []any{[]byte{9}, "inbox", true, false, ""}}},
		"finalize_provider_poison": {{values: []any{"inbox"}}},
	}}
	conflictRepo := newRepository(&fakeBeginner{transactions: []*fakeTransaction{conflictTX}})
	conflictRepo.newID = func() string { return "conflict-a" }
	conflict, err := conflictRepo.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-a",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("different"), Digest: [32]byte{2}, ACKPending: true,
	})
	if err != nil || conflict != ingress.CommitConflict || conflictTX.findCall("insert_provider_inbox_conflict") == nil {
		t.Fatalf("conflict = (%v, %v), calls=%v", conflict, err, conflictTX.operationNames())
	}
}

func TestCommitEnvelopeConflictAtRawPoisonCapacityCompactsToNonACKRejectedDisposition(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":       {{values: []any{true}}},
		"get_provider_inbox":       {{values: []any{bytes.Repeat([]byte{1}, 32), "inbox", true, false, ""}}},
		"finalize_provider_poison": {{err: sql.ErrNoRows}},
		"compact_provider_poison":  {{values: []any{"rejected"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "conflict-cap" }
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-conflict-cap",
		OwnerID: "owner-a", FencingToken: 7, Raw: bytes.Repeat([]byte{'x'}, ingress.MaxRawEnvelopeBytes),
		Digest: sha256.Sum256(bytes.Repeat([]byte{'x'}, ingress.MaxRawEnvelopeBytes)), ACKPending: true,
	})
	if err != nil || result != ingress.CommitConflict {
		t.Fatalf("capacity conflict = (%v, %v), calls=%v", result, err, tx.operationNames())
	}
	if call := tx.findCall("finalize_provider_poison"); call == nil || call.args[4] != ingress.MaxPoisonedProviderInboxEntries || call.args[5] != ingress.MaxPoisonedProviderInboxBytes {
		t.Fatalf("conflict did not use raw poison row/byte admission: %+v", call)
	}
	if call := tx.findCall("compact_provider_poison"); call == nil || call.args[3] != "response_id_digest_conflict" || call.args[4] != ingress.MaxRejectedProviderResponses {
		t.Fatalf("conflict did not compact under bounded rejected admission: %+v", call)
	}
	query := strings.ToLower(tx.lastQuery("compact_provider_poison"))
	for _, required := range []string{"delete from provider_inbox_conflicts", "delete from provider_inbox", "disposition = 'rejected'", "moved_reservation.conflicted", "not moved_reservation.conflicted", "moved_reservation.occurrence_count"} {
		if !strings.Contains(query, required) {
			t.Fatalf("conflict compaction missing %q: %s", required, query)
		}
	}
}

func TestCommitEnvelopeConflictCommitsAuthoritativeACKFenceWhenAllEvidenceCapsAreFull(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence":       {{values: []any{true}}},
		"get_provider_inbox":       {{values: []any{bytes.Repeat([]byte{1}, sha256.Size), "inbox", true, false, ""}}},
		"finalize_provider_poison": {{err: sql.ErrNoRows}},
		"compact_provider_poison":  {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "conflict-all-caps-full" }
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "response-all-caps-full",
		OwnerID: "owner-a", FencingToken: 7, Raw: bytes.Repeat([]byte{'x'}, ingress.MaxRawEnvelopeBytes),
		Digest: sha256.Sum256(bytes.Repeat([]byte{'x'}, ingress.MaxRawEnvelopeBytes)), ACKPending: true,
	})
	if err != nil || result != ingress.CommitConflict {
		t.Fatalf("all-caps conflict = (%v, %v), want committed conflict; calls=%v", result, err, tx.operationNames())
	}
	if !tx.committed || tx.rolledBack {
		t.Fatalf("authoritative conflict/ACK fence was not committed: committed=%v rolledBack=%v", tx.committed, tx.rolledBack)
	}
	if tx.findCall("insert_provider_inbox_conflict") == nil {
		t.Fatal("all-caps conflict did not persist the reservation conflict and ACK fence")
	}
}

func TestCommitEnvelopeExactDuplicateOfPoisonRemainsExplicitlyPoisoned(t *testing.T) {
	digest := [32]byte{7, 8}
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}},
		"get_provider_inbox": {{values: []any{digest[:], "inbox", true, true, "decode_failed"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "poison-response",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("same"), Digest: digest, ACKPending: true,
	})
	if err != nil || result != ingress.CommitDuplicatePoisoned {
		t.Fatalf("poison duplicate = (%v, %v)", result, err)
	}
}

func TestCommitEnvelopeExactRejectedRedeliveryReopensDurableACK(t *testing.T) {
	digest := sha256.Sum256([]byte("same-rejected"))
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"verify_inbox_fence": {{values: []any{true}}},
		"get_provider_inbox": {{values: []any{digest[:], "rejected", false, true, "provider_cursor_budget_exhausted"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	result, err := repository.CommitEnvelope(context.Background(), ingress.EnvelopeRecord{
		TenantID: "tenant-a", ConnectionID: "connection-a", ProviderResponseID: "rejected-after-ack",
		OwnerID: "owner-a", FencingToken: 7, Raw: []byte("same-rejected"), Digest: digest, ACKPending: true,
	})
	if err != nil || result != ingress.CommitDuplicatePoisoned {
		t.Fatalf("acked rejected redelivery = (%v, %v)", result, err)
	}
	query := strings.ToLower(tx.lastQuery("reopen_provider_ack"))
	for _, required := range []string{"provider_response_reservations", "provider_inbox", "provider_rejected_responses", "ack_pending = true", "acked_at = null", "not conflicted"} {
		if !strings.Contains(query, required) {
			t.Fatalf("ACK reopen SQL missing %q: %s", required, query)
		}
	}
}

func TestMarkProviderACKedFencedUsesCurrentLeaseAndBoundedIDs(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{"mark_provider_acked": {{values: []any{true}}}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	owned, err := repository.MarkProviderACKedFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7, []string{"response-a", "response-b"})
	if err != nil || !owned {
		t.Fatalf("MarkProviderACKedFenced() = (%v, %v)", owned, err)
	}
	query := strings.ToLower(tx.lastQuery("mark_provider_acked"))
	if !strings.Contains(query, "connection_leases") || !strings.Contains(query, "fencing_token") || !strings.Contains(query, "ack_pending = false") ||
		!strings.Contains(query, "provider_rejected_responses") || !strings.Contains(query, "provider_response_reservations") ||
		!strings.Contains(query, "not reservation.conflicted") || !strings.Contains(query, "union") {
		t.Fatalf("ACK SQL is not fenced/durable: %s", query)
	}
}

func TestCoordinateProviderACKsFiltersConflictBeforeIOAndSerializesMarkAfterWire(t *testing.T) {
	t.Run("conflict committed first", func(t *testing.T) {
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
			"coordinate_provider_ack_batch": {{values: []any{pq.StringArray{}, true}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		providerCalls := 0
		result, err := repository.CoordinateProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7,
			30*time.Second, []string{"response-a"}, func(context.Context, []string) error { providerCalls++; return nil })
		if err != nil || len(result.AdmittedIDs) != 0 || result.ProviderError != nil || providerCalls != 0 {
			t.Fatalf("filtered coordination = (%+v, %v), provider calls=%d", result, err, providerCalls)
		}
		if !tx.committed || tx.findCall("complete_coordinated_provider_ack") != nil {
			t.Fatalf("filtered ACK transaction calls=%v committed=%v", tx.operationNames(), tx.committed)
		}
	})

	t.Run("mixed eligible batch", func(t *testing.T) {
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
			"coordinate_provider_ack_batch":     {{values: []any{pq.StringArray{"response-a", "response-c"}, true}}},
			"complete_coordinated_provider_ack": {{values: []any{2}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		var providerIDs []string
		result, err := repository.CoordinateProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7,
			30*time.Second, []string{"response-a", "response-b", "response-c"}, func(sendCtx context.Context, ids []string) error {
				providerIDs = append([]string(nil), ids...)
				deadline, bounded := sendCtx.Deadline()
				if !bounded || time.Until(deadline) <= 0 || time.Until(deadline) > providerACKWireHardTimeout {
					t.Fatalf("direct ACK coordination deadline = %v, bounded=%v", deadline, bounded)
				}
				if tx.findCall("complete_coordinated_provider_ack") != nil {
					t.Fatal("ACK state was marked before provider I/O")
				}
				return nil
			})
		if err != nil || result.ProviderError != nil || !reflect.DeepEqual(result.AdmittedIDs, []string{"response-a", "response-c"}) ||
			!reflect.DeepEqual(providerIDs, result.AdmittedIDs) || !tx.committed {
			t.Fatalf("mixed coordination = (%+v, %v), provider=%v committed=%v", result, err, providerIDs, tx.committed)
		}
		query := strings.ToLower(tx.lastQuery("coordinate_provider_ack_batch"))
		for _, required := range []string{
			"locked_connection", "for update", "locked_lease", "expires_at > clock_timestamp()",
			"update connection_leases", "set expires_at = greatest", "interval '1 microsecond'",
			"locked_reservations", "order by reservation.provider_response_id", "not reservation.conflicted",
			"inbox.ack_pending", "rejected.ack_pending", "for update of inbox", "for update of rejected",
		} {
			if !strings.Contains(query, required) {
				t.Fatalf("coordinated ACK SQL missing %q: %s", required, query)
			}
		}
	})
}

func TestCoordinateProviderACKsRollsBackSendFenceAndCommitFailures(t *testing.T) {
	t.Run("provider send failure", func(t *testing.T) {
		providerErr := errors.New("provider unavailable")
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
			"coordinate_provider_ack_batch": {{values: []any{pq.StringArray{"response-a"}, true}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		result, err := repository.CoordinateProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7,
			30*time.Second, []string{"response-a"}, func(context.Context, []string) error { return providerErr })
		if err != nil || !errors.Is(result.ProviderError, providerErr) || !tx.rolledBack || tx.committed ||
			tx.findCall("complete_coordinated_provider_ack") != nil {
			t.Fatalf("provider failure = (%+v, %v), calls=%v committed=%v rollback=%v", result, err, tx.operationNames(), tx.committed, tx.rolledBack)
		}
	})

	t.Run("shorter caller deadline wins", func(t *testing.T) {
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
			"coordinate_provider_ack_batch": {{values: []any{pq.StringArray{"response-a"}, true}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		callerCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		started := time.Now()
		result, err := repository.CoordinateProviderACKsFenced(callerCtx, "tenant-a", "connection-a", "owner-a", 7,
			30*time.Second, []string{"response-a"}, func(sendCtx context.Context, _ []string) error {
				<-sendCtx.Done()
				return sendCtx.Err()
			})
		if err != nil || !errors.Is(result.ProviderError, context.DeadlineExceeded) || !tx.rolledBack ||
			time.Since(started) >= ProviderACKCoordinationHardTimeout {
			t.Fatalf("short caller deadline = (%+v, %v), rolledBack=%v elapsed=%v", result, err, tx.rolledBack, time.Since(started))
		}
	})

	t.Run("lease lost before send", func(t *testing.T) {
		tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
			"coordinate_provider_ack_batch": {{values: []any{pq.StringArray{}, false}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		providerCalls := 0
		_, err := repository.CoordinateProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7,
			30*time.Second, []string{"response-a"}, func(context.Context, []string) error { providerCalls++; return nil })
		if !errors.Is(err, ErrConnectionLeaseLost) || providerCalls != 0 || !tx.rolledBack {
			t.Fatalf("lost lease error=%v provider calls=%d rollback=%v", err, providerCalls, tx.rolledBack)
		}
	})

	t.Run("commit failure after provider success", func(t *testing.T) {
		commitErr := errors.New("commit unavailable")
		tx := &fakeTransaction{commitErr: commitErr, rowResults: map[string][]fakeRowResult{
			"coordinate_provider_ack_batch":     {{values: []any{pq.StringArray{"response-a"}, true}}},
			"complete_coordinated_provider_ack": {{values: []any{1}}},
		}}
		repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
		providerCalls := 0
		result, err := repository.CoordinateProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7,
			30*time.Second, []string{"response-a"}, func(context.Context, []string) error { providerCalls++; return nil })
		if !errors.Is(err, commitErr) || providerCalls != 1 || !reflect.DeepEqual(result.AdmittedIDs, []string{"response-a"}) || !tx.rolledBack {
			t.Fatalf("commit failure = (%+v, %v), provider calls=%d rollback=%v", result, err, providerCalls, tx.rolledBack)
		}
	})
}

func TestCoordinateProviderACKsCallbackPanicAlwaysReleasesTransaction(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"coordinate_provider_ack_batch": {{values: []any{pq.StringArray{"response-a"}, true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	func() {
		defer func() {
			if recovered := recover(); recovered != "provider callback panic" {
				t.Fatalf("callback panic = %v", recovered)
			}
		}()
		_, _ = repository.CoordinateProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7,
			30*time.Second, []string{"response-a"}, func(context.Context, []string) error { panic("provider callback panic") })
	}()
	if !tx.rolledBack || tx.committed {
		t.Fatalf("callback panic transaction = committed=%v rolledBack=%v", tx.committed, tx.rolledBack)
	}
}

func TestListPendingProviderACKsIsBoundedOrderedAndLeaseFenced(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"list_pending_provider_acks": {{values: []any{pq.StringArray{"response-a", "response-b"}, true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	ids, err := repository.ListPendingProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7, 256)
	if err != nil || !reflect.DeepEqual(ids, []string{"response-a", "response-b"}) {
		t.Fatalf("ListPendingProviderACKsFenced() = (%v, %v)", ids, err)
	}
	query := strings.ToLower(tx.lastQuery("list_pending_provider_acks"))
	for _, required := range []string{"connection_leases", "owner_id", "fencing_token", "clock_timestamp()", "ack_pending", "provider_rejected_responses", "order by", "limit"} {
		if !strings.Contains(query, required) {
			t.Fatalf("pending ACK SQL missing %q: %s", required, query)
		}
	}
	pendingStart := strings.Index(query, "pending as materialized")
	aggregateStart := strings.Index(query, "select coalesce(array")
	if pendingStart < 0 || aggregateStart <= pendingStart ||
		!strings.Contains(query[pendingStart:aggregateStart], "order by") ||
		!strings.Contains(query[pendingStart:aggregateStart], "limit $5") {
		t.Fatalf("pending ACK query materializes an unbounded overflow before aggregation: %s", query)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
	staleTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"list_pending_provider_acks": {{values: []any{pq.StringArray{}, false}}},
	}}
	stale := newRepository(&fakeBeginner{transactions: []*fakeTransaction{staleTX}})
	if _, err = stale.ListPendingProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "stale-owner", 6, 256); !errors.Is(err, ErrConnectionLeaseLost) {
		t.Fatalf("stale pending ACK recovery error = %v", err)
	}
	assertTenantTransaction(t, staleTX, "tenant-a", false)
}

func TestListPendingProviderACKsClassifiesLegacyInvalidIDAsProviderLocalCorruption(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"list_pending_provider_acks": {{values: []any{pq.StringArray{"legacy\x00response"}, true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	_, err := repository.ListPendingProviderACKsFenced(context.Background(), "tenant-a", "connection-a", "owner-a", 7, 256)
	if !errors.Is(err, ingress.ErrInvalidProviderResponseID) || errors.Is(err, ErrConnectionLeaseLost) {
		t.Fatalf("legacy invalid pending ACK error = %v, want provider-local invalid response ID", err)
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}
