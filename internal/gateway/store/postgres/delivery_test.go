package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	gatewaykafka "go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

func TestClaimWebhookDeliveriesMaterializesImmutableEventsAndUsesSkipLockedDBTime(t *testing.T) {
	tx := &fakeTransaction{rowsResult: map[string][][]any{
		"claim_webhook_deliveries": {{
			"tenant-a", "delivery-a", "endpoint-a", "event-a", "https://hooks.example/receive", "key-a", []byte(`{"event_id":"event-a"}`), 1,
			"worker-a", uint64(7), uint64(3),
			uint64(0), 1, "webhook", []byte{1}, []byte{2}, make([]byte, 12), "kms-a", 1,
		}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	deliveries, err := repository.Claim(context.Background(), "tenant-a", "worker-a", 20)
	if err != nil || len(deliveries) != 1 || deliveries[0].TenantID != "tenant-a" || deliveries[0].Attempt != 1 || deliveries[0].OwnerID != "worker-a" || deliveries[0].ClaimToken != 7 || deliveries[0].EndpointGeneration != 3 {
		t.Fatalf("Claim() = (%+v, %v)", deliveries, err)
	}
	query := strings.ToLower(tx.lastQuery("claim_webhook_deliveries"))
	for _, required := range []string{"event_outbox", "webhook_endpoints", "webhook_deliveries", "for update", "skip locked", "clock_timestamp()", "cycle_attempt_count", "claim_token", "webhook_attempts", "endpoint.active", "previous_secret_ciphertext = null", "previous_valid_until <= clock_timestamp()", "candidate_pairs", "limit $3", "completed_events", "for share of endpoint"} {
		if !strings.Contains(query, required) {
			t.Fatalf("webhook claim SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestAdmitWebhookClaimAtomicallyChecksEndpointGenerationAndLease(t *testing.T) {
	tx := &fakeTransaction{}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	owned, err := repository.AdmitClaim(context.Background(), webhook.Delivery{
		TenantID: "tenant-a", DeliveryID: "delivery-a", EndpointID: "endpoint-a", OwnerID: "worker-a",
		ClaimToken: 7, EndpointGeneration: 3,
	})
	if err != nil || !owned {
		t.Fatalf("AdmitClaim = (%v, %v)", owned, err)
	}
	query := strings.ToLower(tx.lastQuery("admit_webhook_claim"))
	for _, required := range []string{"http_started_at", "endpoint.active", "endpoint.generation", "endpoint_generation", "claim_token", "claim_expires_at > clock_timestamp()"} {
		if !strings.Contains(query, required) {
			t.Fatalf("admit SQL missing %q: %s", required, query)
		}
	}
}

func TestPurgeExpiredWebhookSecretsIsDirectTenantMaintenance(t *testing.T) {
	tx := &fakeTransaction{}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	if err := repository.PurgeExpiredSecrets(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(tx.lastQuery("purge_expired_webhook_secrets"))
	for _, required := range []string{
		"previous_secret_ciphertext = null", "previous_secret_wrapped_dek = null",
		"previous_secret_nonce = null", "previous_key_id = null",
		"previous_valid_until <= clock_timestamp()", "tenant_id = $1",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("purge SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestMediaMetadataCreateAndGetAreTenantScopedAndNeverTrustCallerPaths(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	createTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"check_webhook_endpoint_quota": {{values: []any{0}}}}}
	getTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_media_metadata": {{values: []any{"media-a", "objects/0123/abcdef", "image/png", int64(3), []byte(strings.Repeat("d", 32)), 1, 1, "image.png", "ready", now}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{createTX, getTX}})
	record := media.Record{
		ID: "media-a", TenantID: "tenant-a", ObjectKey: "objects/0123/abcdef", MIMEType: "image/png", Size: 3,
		SHA256: []byte(strings.Repeat("d", 32)), Width: 1, Height: 1, DisplayFilename: "image.png", State: "ready", CreatedAt: now,
	}
	if err := repository.Create(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.Get(context.Background(), "tenant-a", "media-a")
	if err != nil || loaded.ID != record.ID || loaded.TenantID != "tenant-a" || loaded.ObjectKey != record.ObjectKey {
		t.Fatalf("Get() = (%+v, %v)", loaded, err)
	}
	if !strings.Contains(strings.ToLower(createTX.lastQuery("create_media_metadata")), "on conflict") {
		t.Fatal("media completion cannot converge with an inbound pending media record")
	}
	getQuery := strings.ToLower(getTX.lastQuery("get_media_metadata"))
	if !strings.Contains(getQuery, "media_fetch_jobs") || !strings.Contains(getQuery, "job.state = 'failed'") {
		t.Fatalf("ready object with terminal failed verification remains serveable: %s", getQuery)
	}
	assertTenantTransaction(t, createTX, "tenant-a", true)
	assertTenantTransaction(t, getTX, "tenant-a", true)
}

func TestMediaFetchClaimAndReadyCompletionAreFencedAndOutboxed(t *testing.T) {
	claimTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"claim_media_fetch_job": {{values: []any{
			"job-a", "media-a", "connection-a", "provider-a", "https://allowed.example/object",
			"image/png", int64(4), "image.png",
			[]byte{1}, []byte{2}, make([]byte, 12), "kms-a", 1,
			[]byte{}, []byte{}, []byte{}, "", 0,
			2, "worker-a", uint64(7),
			"pending", "", "image/png", int64(0), []byte{}, 0, 0, "image.png", time.Unix(1700000000, 0).UTC(),
		}}},
	}}
	readyTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"complete_media_fetch_ready": {{values: []any{true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{claimTX, readyTX}})
	job, ok, err := repository.ClaimFetch(context.Background(), "tenant-a", "worker-a")
	if err != nil || !ok || job.JobID != "job-a" || job.ClaimToken != 7 || job.KeyEnvelope.KeyID != "kms-a" {
		t.Fatalf("Claim media fetch = (%+v, %v, %v)", job, ok, err)
	}
	claimSQL := strings.ToLower(claimTX.lastQuery("claim_media_fetch_job"))
	for _, required := range []string{"for update", "skip locked", "clock_timestamp()", "claim_token", "claim_expires_at", "key_ciphertext"} {
		if !strings.Contains(claimSQL, required) {
			t.Fatalf("media claim SQL missing %q: %s", required, claimSQL)
		}
	}
	record := media.Record{ID: "media-a", TenantID: "tenant-a", State: "ready", ObjectKey: "objects/0123/abcdef", MIMEType: "image/png", Size: 4, SHA256: make([]byte, 32)}
	if err = repository.CompleteReady(context.Background(), job, record, "event-a", []byte(`{"event_id":"event-a","type":"media.ready"}`)); err != nil {
		t.Fatal(err)
	}
	readySQL := strings.ToLower(readyTX.lastQuery("complete_media_fetch_ready"))
	for _, required := range []string{"claim_token", "owner_id", "gateway_events", "event_outbox", "media.ready"} {
		if !strings.Contains(readySQL, required) {
			t.Fatalf("media ready SQL missing %q: %s", required, readySQL)
		}
	}
	assertTenantTransaction(t, claimTX, "tenant-a", true)
	assertTenantTransaction(t, readyTX, "tenant-a", true)
}

func TestMediaFetchRetryAndTerminalFailureHaveDistinctDurableTransitions(t *testing.T) {
	retryTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"retry_media_fetch": {{values: []any{true}}}}}
	failedTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"fail_media_fetch": {{values: []any{true}}}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{retryTX, failedTX}})
	job := media.FetchJob{
		TenantID: "tenant-a", JobID: "job-a", MediaID: "media-a", ConnectionID: "connection-a",
		ProviderMessageID: "provider-a", OwnerID: "worker-a", ClaimToken: 7, AttemptCount: 2,
	}
	if err := repository.CompleteFailed(context.Background(), job, "fetch_failed", "", nil, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if query := strings.ToLower(retryTX.lastQuery("retry_media_fetch")); !strings.Contains(query, "state = 'pending'") || !strings.Contains(query, "available_at") || strings.Contains(query, "gateway_events") {
		t.Fatalf("retry SQL = %s", query)
	}
	if err := repository.CompleteFailed(context.Background(), job, "invalid_media", "event-failed", []byte(`{"event_id":"event-failed","type":"media.failed"}`), time.Time{}); err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(failedTX.lastQuery("fail_media_fetch"))
	for _, required := range []string{"state = 'failed'", "gateway_events", "event_outbox", "media.failed", "claim_token", "owner_id"} {
		if !strings.Contains(query, required) {
			t.Fatalf("terminal media SQL missing %q: %s", required, query)
		}
	}
	if strings.Contains(query, "from failed_job join failed_object") {
		t.Fatal("job-only quarantine incorrectly requires downgrading an imported ready object")
	}
}

func TestWebhookEndpointPersistenceEncryptsRotatesSoftDeletesAndReplaysWithoutErasingHistory(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	envelope := session.Envelope{
		Version: 1, Provider: "webhook", Ciphertext: make([]byte, 16), WrappedDEK: []byte{2},
		Nonce: make([]byte, 12), KeyID: "kms-a", KeyVersion: 1,
	}
	createTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"check_webhook_endpoint_quota": {{values: []any{0}}},
	}}
	rotateTX := &fakeTransaction{}
	deleteTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"delete_webhook_endpoint": {{values: []any{true, true}}}}}
	replayTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"replay_webhook_dlq": {{values: []any{true}}}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{createTX, rotateTX, deleteTX, replayTX}})
	if err := repository.CreateEndpoint(context.Background(), webhook.EndpointRecord{
		Endpoint: webhook.Endpoint{ID: "endpoint-a", TenantID: "tenant-a", Destination: "https://hooks.example/receive", KeyID: "key-a", Active: true, CreatedAt: now},
		Secret:   envelope,
	}, webhook.DefaultMaxEndpointsPerTenant); err != nil {
		t.Fatal(err)
	}
	if query := strings.ToLower(createTX.lastQuery("create_webhook_endpoint")); !strings.Contains(query, "secret_ciphertext") || strings.Contains(query, "plaintext") {
		t.Fatalf("endpoint create SQL = %s", query)
	}
	if err := repository.RotateEndpoint(context.Background(), webhook.EndpointRotation{
		TenantID: "tenant-a", EndpointID: "endpoint-a", KeyID: "key-b", Secret: envelope, PreviousValidUntil: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if query := strings.ToLower(rotateTX.lastQuery("rotate_webhook_endpoint")); !strings.Contains(query, "previous_secret_ciphertext = secret_ciphertext") || !strings.Contains(query, "previous_valid_until") {
		t.Fatalf("rotation SQL = %s", query)
	}
	deleted, err := repository.DeleteEndpoint(context.Background(), "tenant-a", "endpoint-a")
	if err != nil || !deleted.Deleting {
		t.Fatal(err)
	}
	deleteSQL := strings.ToLower(deleteTX.lastQuery("delete_webhook_endpoint"))
	for _, required := range []string{"active = false", "endpoint.generation + 1", "state = 'dead'", "claim_token = delivery.claim_token + 1", "webhook_dlq", "http_started_at is null", "claim_expires_at > clock_timestamp()"} {
		if !strings.Contains(deleteSQL, required) {
			t.Fatalf("endpoint deletion lacks %q: %s", required, deleteSQL)
		}
	}
	if err := repository.ReplayDLQ(context.Background(), "tenant-a", "dlq-a"); err != nil {
		t.Fatal(err)
	}
	replaySQL := strings.ToLower(replayTX.lastQuery("replay_webhook_dlq"))
	for _, required := range []string{"cycle_attempt_count = 0", "state = 'pending'", "replayed_at", "for update", "endpoint.active", "claim_token = delivery.claim_token + 1"} {
		if !strings.Contains(replaySQL, required) {
			t.Fatalf("replay SQL missing %q: %s", required, replaySQL)
		}
	}
}

func TestWebhookEndpointQuotaAndListingAreRaceSerializedAndBounded(t *testing.T) {
	quotaTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"check_webhook_endpoint_quota": {{values: []any{webhook.DefaultMaxEndpointsPerTenant}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{quotaTX}})
	envelope := session.Envelope{Version: 1, Provider: "webhook", Ciphertext: make([]byte, 16), WrappedDEK: []byte{2}, Nonce: make([]byte, 12), KeyID: "kms-a", KeyVersion: 1}
	err := repository.CreateEndpoint(context.Background(), webhook.EndpointRecord{
		Endpoint: webhook.Endpoint{ID: "endpoint-over", TenantID: "tenant-a", Destination: "https://hooks.example/receive", KeyID: "key-a", Active: true}, Secret: envelope,
	}, webhook.DefaultMaxEndpointsPerTenant)
	if !errors.Is(err, webhook.ErrEndpointQuotaExceeded) || quotaTX.findCall("create_webhook_endpoint") != nil {
		t.Fatalf("over-quota endpoint = %v", err)
	}
	lockQuery := strings.ToLower(quotaTX.lastQuery("lock_webhook_endpoint_quota"))
	if !strings.Contains(lockQuery, "from tenants") || !strings.Contains(lockQuery, "for update") {
		t.Fatalf("endpoint quota lock = %s", lockQuery)
	}

	now := time.Unix(1700000000, 0).UTC()
	listTX := &fakeTransaction{rowsResult: map[string][][]any{"list_webhook_endpoints": {
		{"endpoint-a", "https://a.example/hook", "key-a", true, now},
		{"endpoint-b", "https://b.example/hook", "key-b", true, now},
	}}}
	repository = newRepository(&fakeBeginner{transactions: []*fakeTransaction{listTX}})
	page, err := repository.ListEndpoints(context.Background(), "tenant-a", webhook.EndpointListOptions{After: "endpoint-before", Limit: 1})
	if err != nil || len(page.Endpoints) != 1 || page.NextCursor != "endpoint-a" {
		t.Fatalf("ListEndpoints = (%+v, %v)", page, err)
	}
	query := strings.ToLower(listTX.lastQuery("list_webhook_endpoints"))
	for _, required := range []string{"endpoint_id > $2", "order by endpoint_id", "limit $3"} {
		if !strings.Contains(query, required) {
			t.Fatalf("endpoint page SQL missing %q: %s", required, query)
		}
	}
}

func TestCompleteWebhookAttemptPersistsHistoryAndDLQAtomically(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"complete_webhook_attempt": {{values: []any{true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "dlq-a" }
	err := repository.CompleteAttempt(context.Background(), webhook.AttemptResult{
		TenantID: "tenant-a", DeliveryID: "delivery-a", Attempt: 8, Dead: true,
		OwnerID: "worker-a", ClaimToken: 11, StatusCode: 503, SafeError: "webhook returned HTTP 503",
	})
	if err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(tx.lastQuery("complete_webhook_attempt"))
	for _, required := range []string{"webhook_attempts", "webhook_deliveries", "webhook_dlq", "for update", "cycle_attempt_count", "clock_timestamp()", "claimed_by", "claim_token", "endpoint.active"} {
		if !strings.Contains(query, required) {
			t.Fatalf("webhook completion SQL missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestKafkaOutboxClaimAndMarkAreTenantScopedAndCrashReplayable(t *testing.T) {
	txClaim := &fakeTransaction{rowsResult: map[string][][]any{
		"claim_kafka_events": {{"event-a", "tenant-a", "connection-a", "conversation-a", []byte(`{"event_id":"event-a"}`)}},
	}}
	txMark := &fakeTransaction{}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{txClaim, txMark}})
	events, err := repository.ClaimEvents(context.Background(), "tenant-a", "worker-a", 20)
	if err != nil || len(events) != 1 || events[0].EventID != "event-a" {
		t.Fatalf("ClaimEvents() = (%+v, %v)", events, err)
	}
	claimSQL := strings.ToLower(txClaim.lastQuery("claim_kafka_events"))
	for _, required := range []string{"event_outbox", "kafka_event_deliveries", "for update", "skip locked", "clock_timestamp()"} {
		if !strings.Contains(claimSQL, required) {
			t.Fatalf("Kafka claim SQL missing %q: %s", required, claimSQL)
		}
	}
	if err = repository.MarkPublished(context.Background(), domain.TenantID("tenant-a"), "event-a"); err != nil {
		t.Fatal(err)
	}
	if got := txMark.operationNames(); !reflect.DeepEqual(got, []string{"tenant_context", "mark_kafka_event_published"}) {
		t.Fatalf("mark operations = %v", got)
	}
	assertTenantTransaction(t, txClaim, "tenant-a", true)
	assertTenantTransaction(t, txMark, "tenant-a", true)
}

func TestKafkaCommandDLQIsDistinctTenantScopedAndBounded(t *testing.T) {
	tx := &fakeTransaction{}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "kafka-dlq-a" }
	err := repository.Store(context.Background(), gatewaykafka.CommandDLQRecord{
		Topic: gatewaykafka.DefaultCommandsTopic, Partition: 2, Offset: 8, AuthorizedTenant: "tenant-a",
		CorrelationID: "correlation-a", OriginalPayload: []byte("{}"), SafeError: "command rejected",
	})
	if err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(tx.lastQuery("insert_kafka_command_dlq"))
	if !strings.Contains(query, "kafka_command_dlq") || strings.Contains(query, "webhook_dlq") {
		t.Fatalf("Kafka DLQ SQL = %s", query)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}
