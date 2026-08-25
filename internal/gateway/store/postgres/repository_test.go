package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contacts"
	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/pairing"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

func TestRepositoryGetConnectionSetsTransactionTenantAndFailsClosed(t *testing.T) {
	tx := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{
			"get_connection": {{values: []any{"connection-1", "Primary phone", "connected"}}},
		},
	}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
	repository := newRepository(db)

	connection, err := repository.GetConnection(context.Background(), "tenant-a", "connection-1")
	if err != nil {
		t.Fatalf("GetConnection() error = %v", err)
	}
	if connection != (domain.Connection{ID: "connection-1", TenantID: "tenant-a", Name: "Primary phone", State: domain.ConnectionStateConnected}) {
		t.Fatalf("GetConnection() = %#v", connection)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
	if call := tx.findCall("tenant_context"); call == nil || !strings.Contains(call.query, "set_config('sirenaix.tenant_id', $1, true)") {
		t.Fatalf("tenant context query = %#v, want transaction-local sirenaix.tenant_id", call)
	}

	missingTx := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{"get_connection": {{err: sql.ErrNoRows}}},
	}
	missingDB := &fakeBeginner{transactions: []*fakeTransaction{missingTx}}
	_, err = newRepository(missingDB).GetConnection(context.Background(), "tenant-b", "connection-1")
	if !errors.Is(err, contactsync.ErrConnectionNotFound) {
		t.Fatalf("missing GetConnection() error = %v, want ErrConnectionNotFound", err)
	}
	assertTenantTransaction(t, missingTx, "tenant-b", false)
}

func TestRepositoryUpsertProviderContactIsIdempotentAndPreservesServerMetadata(t *testing.T) {
	row := []any{"contact-1", "+12025550199", "VIP customer", "Alice Updated", `["label-vip"]`}
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"upsert_provider_contact": {{values: row}, {values: row}},
	}}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx, tx}}
	repository := newRepository(db)
	repository.newID = func() string { return "unused-after-convergence" }
	phone, _ := domain.ParseE164("+1 202 555 0199")
	update := contactsync.ProviderContactUpsert{
		TenantID: "tenant-a", ConnectionID: "connection-1", ProviderContactID: "provider-7",
		Phone: phone, ProviderDisplayName: "Alice Updated",
	}

	first, err := repository.UpsertProviderContact(context.Background(), update)
	if err != nil {
		t.Fatalf("first UpsertProviderContact() error = %v", err)
	}
	second, err := repository.UpsertProviderContact(context.Background(), update)
	if err != nil {
		t.Fatalf("second UpsertProviderContact() error = %v", err)
	}
	if first.ID != second.ID || first.ID != "contact-1" {
		t.Fatalf("idempotent IDs = %q and %q", first.ID, second.ID)
	}
	if second.ProviderDisplayName != "Alice Updated" || second.Alias != "VIP customer" || !reflect.DeepEqual(second.LabelIDs, []domain.LabelID{"label-vip"}) {
		t.Fatalf("upsert result lost server metadata: %#v", second)
	}
	call := tx.findCall("upsert_provider_contact")
	if call == nil || len(call.args) != 6 || call.args[5] != "Alice Updated" {
		t.Fatalf("provider-name refresh args = %#v", call)
	}
	if !strings.Contains(strings.ToLower(tx.lastQuery("upsert_provider_contact")), "on conflict") ||
		!strings.Contains(strings.ToLower(tx.lastQuery("upsert_provider_contact")), "provider_contact_sources.contact_id = excluded.contact_id") ||
		!strings.Contains(strings.ToLower(tx.lastQuery("upsert_provider_contact")), "provider_contact_sources.normalized_phone = excluded.normalized_phone") {
		t.Fatal("provider-source SQL does not atomically guard its immutable canonical identity")
	}
}

func TestRepositoryUpsertServerContactConvergesByTenantPhoneAndPreservesProviderMetadata(t *testing.T) {
	row := []any{"contact-existing", "+12025550199", "Potential Client", "Phone Alice", `["label-vip"]`}
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"upsert_server_contact": {{values: row}, {values: row}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx, tx}})
	phone, _ := domain.ParseE164("+1 (202) 555-0199")
	alias := "Potential Client"

	first, err := repository.UpsertServerContact(context.Background(), "tenant-a", "contact-new-a", phone, &alias)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.UpsertServerContact(context.Background(), "tenant-a", "contact-new-b", phone, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "contact-existing" || second.ID != first.ID || second.Alias != "Potential Client" ||
		second.ProviderDisplayName != "Phone Alice" || !reflect.DeepEqual(second.LabelIDs, []domain.LabelID{"label-vip"}) {
		t.Fatalf("converged server contact = %#v / %#v", first, second)
	}
	var calls []fakeCall
	for _, call := range tx.calls {
		if call.operation == "upsert_server_contact" {
			calls = append(calls, call)
		}
	}
	if len(calls) != 2 || !reflect.DeepEqual(calls[0].args, []any{"tenant-a", "contact-new-a", "+12025550199", "Potential Client", true}) ||
		!reflect.DeepEqual(calls[1].args, []any{"tenant-a", "contact-new-b", "+12025550199", "", false}) {
		t.Fatalf("upsert args = %#v", calls)
	}
	query := strings.ToLower(calls[0].query)
	if !strings.Contains(query, "on conflict (tenant_id, normalized_phone)") ||
		strings.Contains(query, "provider_display_name = excluded") || !strings.Contains(query, "case when $5") {
		t.Fatalf("server upsert does not preserve provider metadata/optional alias: %s", query)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryUpsertServerContactRejectsInvalidAliasBeforeSQL(t *testing.T) {
	repository := newRepository(&fakeBeginner{})
	phone, _ := domain.ParseE164("+12025550100")
	alias := strings.Repeat("a", contacts.MaxServerAliasBytes+1)
	if _, err := repository.UpsertServerContact(context.Background(), "tenant-a", "contact-a", phone, &alias); !errors.Is(err, contacts.ErrInvalidContact) {
		t.Fatalf("oversized alias error = %v", err)
	}
	if _, err := repository.UpsertServerContact(context.Background(), "tenant-a", " contact-a", phone, nil); !errors.Is(err, contacts.ErrInvalidContact) {
		t.Fatalf("unsafe created contact ID error = %v", err)
	}
}

func TestRepositoryUpsertProviderContactMapsImmutableIdentityConflictAndRollsBack(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"upsert_provider_contact": {{err: sql.ErrNoRows}},
	}}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
	repository := newRepository(db)
	phone, _ := domain.ParseE164("+12025550100")

	_, err := repository.UpsertProviderContact(context.Background(), contactsync.ProviderContactUpsert{
		TenantID: "tenant-a", ConnectionID: "connection-1", ProviderContactID: "provider-7", Phone: phone,
	})
	if !errors.Is(err, contactsync.ErrProviderIdentityConflict) {
		t.Fatalf("UpsertProviderContact() error = %v, want ErrProviderIdentityConflict", err)
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}

func TestRepositoryUpsertProviderContactRollsBackUnexpectedFailure(t *testing.T) {
	databaseFailure := errors.New("database unavailable")
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"upsert_provider_contact": {{err: databaseFailure}},
	}}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
	repository := newRepository(db)
	phone, _ := domain.ParseE164("+12025550100")

	_, err := repository.UpsertProviderContact(context.Background(), contactsync.ProviderContactUpsert{
		TenantID: "tenant-a", ConnectionID: "connection-1", ProviderContactID: "provider-7", Phone: phone,
	})
	if !errors.Is(err, databaseFailure) {
		t.Fatalf("UpsertProviderContact() error = %v, want wrapped database failure", err)
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}

func TestRepositoryReplaceLinesIsAtomicAndTenantScoped(t *testing.T) {
	upsertFailure := errors.New("duplicate provider route")
	tx := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{"ensure_connection": {{values: []any{1}}}},
		execErrors: map[string][]error{
			"upsert_line": {nil, upsertFailure},
		},
	}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
	repository := newRepository(db)
	phone1, _ := domain.ParseE164("+12025550101")
	phone2, _ := domain.ParseE164("+12025550102")
	lines := []LineRecord{
		{Line: domain.Line{ID: "line-1", TenantID: "tenant-a", ConnectionID: "connection-1", ProviderParticipantID: "participant-1", ProviderOutgoingID: "outgoing-1"}, Phone: phone1, DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings},
		{Line: domain.Line{ID: "line-2", TenantID: "tenant-a", ConnectionID: "connection-1", ProviderParticipantID: "participant-2", ProviderOutgoingID: "outgoing-2"}, Phone: phone2, DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings},
	}

	err := repository.ReplaceLines(context.Background(), "tenant-a", "connection-1", lines)
	if !errors.Is(err, upsertFailure) {
		t.Fatalf("ReplaceLines() error = %v, want wrapped insert failure", err)
	}
	if got := tx.operationNames(); !reflect.DeepEqual(got, []string{"tenant_context", "ensure_connection", "upsert_line", "upsert_line"}) {
		t.Fatalf("operation order = %v", got)
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}

func TestRepositoryReplaceAndListLinesPreserveAuthenticatedProviderMetadata(t *testing.T) {
	phone, _ := domain.ParseE164("+12025550101")
	record := LineRecord{
		Line:  domain.Line{ID: "line-1", TenantID: "tenant-a", ConnectionID: "connection-1", ProviderParticipantID: "participant-1", ProviderOutgoingID: "outgoing-1", DisplayName: "Carrier A"},
		Phone: phone, CarrierName: "Carrier A", ColorHex: "#123456", RCSEnabled: true,
		ProviderSIMNumber: 2, ProviderSIMPayloadType: 7, DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings,
	}
	write := &fakeTransaction{
		rowResults: map[string][]fakeRowResult{"ensure_connection": {{values: []any{1}}}},
	}
	read := &fakeTransaction{rowsResult: map[string][][]any{"list_lines": {{
		"line-1", "connection-1", "participant-1", "outgoing-1", "+12025550101", "Carrier A",
		"Carrier A", "#123456", true, int32(2), int32(7), string(LineDiscoveryAuthenticatedGoogleSettings),
	}}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{write, read}})
	if err := repository.ReplaceLines(context.Background(), "tenant-a", "connection-1", []LineRecord{record}); err != nil {
		t.Fatal(err)
	}
	call := write.findCall("upsert_line")
	if call == nil || !reflect.DeepEqual(call.args, []any{
		"tenant-a", "line-1", "connection-1", "participant-1", "outgoing-1", "+12025550101", "Carrier A",
		"Carrier A", "#123456", true, int32(2), int32(7), string(LineDiscoveryAuthenticatedGoogleSettings),
	}) {
		t.Fatalf("line upsert args = %#v", call)
	}
	query := strings.ToLower(call.query)
	for _, required := range []string{"on conflict (tenant_id, line_id) do update", "active = true", "where lines.connection_id = excluded.connection_id"} {
		if !strings.Contains(query, required) {
			t.Fatalf("line upsert SQL missing %q: %s", required, query)
		}
	}
	retire := write.findCall("retire_lines")
	if retire == nil || len(retire.args) != 3 || retire.args[0] != "tenant-a" || retire.args[1] != "connection-1" {
		t.Fatalf("line retirement call = %#v", retire)
	}
	retireQuery := strings.ToLower(retire.query)
	for _, required := range []string{"update lines", "active = false", "not (line_id = any($3::text[]))"} {
		if !strings.Contains(retireQuery, required) {
			t.Fatalf("line retirement SQL missing %q: %s", required, retireQuery)
		}
	}
	if strings.Contains(query+retireQuery, "delete from lines") {
		t.Fatalf("line snapshot replacement can delete FK targets: %s / %s", query, retireQuery)
	}
	lines, err := repository.ListLines(context.Background(), "tenant-a", "connection-1")
	if err != nil || len(lines) != 1 || lines[0] != record {
		t.Fatalf("listed lines = %#v, %v", lines, err)
	}
	if query := strings.ToLower(read.lastQuery("list_lines")); !strings.Contains(query, "active") || !strings.Contains(query, "active = true") {
		t.Fatalf("line listing exposes retired rows: %s", query)
	}
}

func TestRepositoryReplaceLinesFencedRequiresCurrentOwnedLease(t *testing.T) {
	phone, _ := domain.ParseE164("+12025550101")
	record := LineRecord{
		Line:  domain.Line{ID: "line-1", TenantID: "tenant-a", ConnectionID: "connection-1", ProviderParticipantID: "participant-1", ProviderOutgoingID: "outgoing-1"},
		Phone: phone, DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings,
	}
	owned := &fakeTransaction{rowResults: map[string][]fakeRowResult{"ensure_line_replace_fence": {{values: []any{1}}}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{owned}})
	if err := repository.ReplaceLinesFenced(context.Background(), "tenant-a", "connection-1", "owner-a", 7, []LineRecord{record}); err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(owned.lastQuery("ensure_line_replace_fence"))
	assertConnectionThenLeaseLockOrder(t, query)
	if !strings.Contains(query, "lease.owner_id = $3") || !strings.Contains(query, "lease.fencing_token = $4") || !strings.Contains(query, "lease.expires_at > clock_timestamp()") {
		t.Fatalf("line replacement fence is incomplete: %s", query)
	}
	if owned.findCall("retire_lines") == nil || owned.findCall("upsert_line") == nil || owned.findCall("delete_lines") != nil {
		t.Fatalf("owned line replacement operations = %v", owned.operationNames())
	}

	stale := &fakeTransaction{rowResults: map[string][]fakeRowResult{"ensure_line_replace_fence": {{err: sql.ErrNoRows}}}}
	repository = newRepository(&fakeBeginner{transactions: []*fakeTransaction{stale}})
	if err := repository.ReplaceLinesFenced(context.Background(), "tenant-a", "connection-1", "stale-owner", 6, []LineRecord{record}); !errors.Is(err, ErrConnectionLeaseLost) {
		t.Fatalf("stale line replacement error = %v", err)
	}
	if stale.findCall("retire_lines") != nil || stale.findCall("upsert_line") != nil || stale.findCall("delete_lines") != nil || !stale.rolledBack {
		t.Fatalf("stale line replacement mutated rows: %v", stale.operationNames())
	}

	overCap := make([]LineRecord, MaxAuthenticatedLineSnapshot+1)
	for index := range overCap {
		overCap[index] = record
		overCap[index].Line.ID = domain.LineID(fmt.Sprintf("line-%02d", index))
		overCap[index].Line.ProviderParticipantID = fmt.Sprintf("participant-%02d", index)
		overCap[index].Line.ProviderOutgoingID = fmt.Sprintf("outgoing-%02d", index)
	}
	overCapDB := &fakeBeginner{}
	if err := newRepository(overCapDB).ReplaceLinesFenced(context.Background(), "tenant-a", "connection-1", "owner-a", 7, overCap); !errors.Is(err, domain.ErrInvalidIdentifier) {
		t.Fatalf("over-cap fenced replacement error = %v", err)
	}
	if overCapDB.beginCalls != 0 {
		t.Fatalf("over-cap fenced replacement allocated a transaction")
	}
}

func TestRepositoryReplaceLinesFailsClosedForMissingConnection(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"ensure_connection": {{err: sql.ErrNoRows}},
	}}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}

	err := newRepository(db).ReplaceLines(context.Background(), "tenant-a", "missing", nil)
	if !errors.Is(err, contactsync.ErrConnectionNotFound) {
		t.Fatalf("ReplaceLines() error = %v, want ErrConnectionNotFound", err)
	}
	if tx.findCall("retire_lines") != nil || tx.findCall("upsert_line") != nil || tx.findCall("delete_lines") != nil {
		t.Fatal("ReplaceLines mutated rows before proving connection ownership")
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}

func TestRepositoryGetLineExcludesRetiredSnapshotRows(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_message_line": {{values: []any{
			"line-current", "connection-1", "participant-current", "outgoing-current", "Current SIM",
		}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	line, err := repository.GetLine(context.Background(), "tenant-a", "connection-1", "line-current")
	if err != nil || line.ID != "line-current" || line.ConnectionID != "connection-1" {
		t.Fatalf("GetLine() = (%+v, %v)", line, err)
	}
	if query := strings.ToLower(tx.lastQuery("get_message_line")); !strings.Contains(query, "active = true") {
		t.Fatalf("dispatch line lookup can expose a retired line: %s", query)
	}
}

func TestRepositoryRejectsInvalidEncryptedSessionBeforeSQL(t *testing.T) {
	valid := validTestEnvelope()
	tests := []struct {
		name    string
		session EncryptedSession
	}{
		{name: "ciphertext", session: EncryptedSession{WrappedDEK: valid.WrappedDEK, Nonce: valid.Nonce, KeyID: valid.KeyID, KeyVersion: 1}},
		{name: "wrapped DEK", session: EncryptedSession{Ciphertext: valid.Ciphertext, Nonce: valid.Nonce, KeyID: valid.KeyID, KeyVersion: 1}},
		{name: "nonce", session: EncryptedSession{Ciphertext: valid.Ciphertext, WrappedDEK: valid.WrappedDEK, KeyID: valid.KeyID, KeyVersion: 1}},
		{name: "key ID", session: EncryptedSession{Ciphertext: valid.Ciphertext, WrappedDEK: valid.WrappedDEK, Nonce: valid.Nonce, KeyVersion: 1}},
		{name: "key version", session: EncryptedSession{Ciphertext: valid.Ciphertext, WrappedDEK: valid.WrappedDEK, Nonce: valid.Nonce, KeyID: valid.KeyID}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := &fakeBeginner{}
			err := newRepository(db).SaveEncryptedSession(context.Background(), "tenant-a", "connection-1", test.session)
			if !errors.Is(err, ErrInvalidEncryptedSession) {
				t.Fatalf("SaveEncryptedSession() error = %v, want ErrInvalidEncryptedSession", err)
			}
			if db.beginCalls != 0 {
				t.Fatalf("invalid envelope started %d transactions", db.beginCalls)
			}
		})
	}
}

func TestRepositoryCreatesUnpairedConnectionWithoutInventedFingerprint(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"check_connection_quota": {{values: []any{false, DefaultMaxConnectionsPerTenant - 1, DefaultMaxConnectionsPerTenant, "active"}}},
	}}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
	record := ConnectionRecord{Connection: domain.Connection{ID: "connection-new", TenantID: "tenant-a", Name: "Front phone", State: domain.ConnectionStateUnpaired}}
	if err := newRepository(db).SaveConnection(context.Background(), "tenant-a", record); err != nil {
		t.Fatalf("SaveConnection: %v", err)
	}
	call := tx.findCall("create_unpaired_connection")
	if call == nil || call.args[4] != nil {
		t.Fatalf("unpaired fingerprint argument = %#v", call)
	}
	if strings.Contains(strings.ToLower(call.query), "on conflict") {
		t.Fatal("unpaired connection creation could overwrite an existing connection ID")
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryRejectsNewConnectionAtRaceSafeTenantQuota(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"check_connection_quota": {{values: []any{false, 12, 12, "active"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	err := repository.SaveConnection(context.Background(), "tenant-a", ConnectionRecord{Connection: domain.Connection{
		ID: "connection-over-quota", TenantID: "tenant-a", State: domain.ConnectionStateUnpaired,
	}})
	if !errors.Is(err, ErrConnectionQuotaExceeded) {
		t.Fatalf("SaveConnection() error = %v, want ErrConnectionQuotaExceeded", err)
	}
	if tx.findCall("create_unpaired_connection") != nil || tx.committed {
		t.Fatal("over-quota connection was written")
	}
	query := strings.ToLower(tx.lastQuery("lock_connection_quota"))
	if !strings.Contains(query, "from tenants") || !strings.Contains(query, "for update") {
		t.Fatalf("quota lock SQL = %s", query)
	}
	quotaQuery := strings.ToLower(tx.lastQuery("check_connection_quota"))
	if !strings.Contains(quotaQuery, "max_connections") {
		t.Fatalf("quota check ignores provisioned tenant limit: %s", quotaQuery)
	}
}

func TestRepositoryRejectsConnectionCreationForSuspendedTenant(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"check_connection_quota": {{values: []any{false, 0, 8, "suspended"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	err := repository.SaveConnection(context.Background(), "tenant-a", ConnectionRecord{Connection: domain.Connection{
		ID: "connection-new", TenantID: "tenant-a", State: domain.ConnectionStateUnpaired,
	}})
	if !errors.Is(err, ErrTenantSuspended) {
		t.Fatalf("SaveConnection() error = %v, want ErrTenantSuspended", err)
	}
	if tx.committed || tx.findCall("create_unpaired_connection") != nil {
		t.Fatal("connection was created for a suspended tenant")
	}
}

func TestOperationalQueueDepthsAreTenantScopedAndFixedFamilies(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"operational_queue_depths": {{values: []any{int64(2), int64(3), int64(4), int64(5)}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	depths, err := repository.OperationalQueueDepths(context.Background(), "tenant-a")
	if err != nil || depths.Messages != 2 || depths.Media != 3 || depths.Webhooks != 4 || depths.Kafka != 5 {
		t.Fatalf("OperationalQueueDepths() = (%+v, %v)", depths, err)
	}
	query := strings.ToLower(tx.lastQuery("operational_queue_depths"))
	for _, required := range []string{"messages", "media_fetch_jobs", "webhook_deliveries", "event_outbox", "tenant_id = $1", "destination = 'kafka'"} {
		if !strings.Contains(query, required) {
			t.Fatalf("queue depth query missing %q: %s", required, query)
		}
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestListConnectionsPageUsesBoundedTenantKeyset(t *testing.T) {
	tx := &fakeTransaction{rowsResult: map[string][][]any{"list_connections_page": {
		{"connection-a", "Phone A", "connected", []byte("fingerprint-a")},
		{"connection-b", "Phone B", "degraded", []byte("fingerprint-b")},
	}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	page, err := repository.ListConnectionsPage(context.Background(), "tenant-a", "connection-before", 1)
	if err != nil || len(page.Records) != 1 || page.Records[0].Connection.ID != "connection-a" || page.NextCursor != "connection-a" {
		t.Fatalf("ListConnectionsPage() = (%+v, %v)", page, err)
	}
	query := strings.ToLower(tx.lastQuery("list_connections_page"))
	for _, required := range []string{"tenant_id = $1", "connection_id > $2", "order by connection_id", "limit $3"} {
		if !strings.Contains(query, required) {
			t.Fatalf("connection page SQL missing %q: %s", required, query)
		}
	}
}

func TestRepositoryCommitPairedSessionSavesBeforeConnectedAtomically(t *testing.T) {
	tx := &fakeTransaction{execResults: map[string][]int64{"commit_connection_state": {1}}}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
	repository := newRepository(db)
	envelope := validTestEnvelope()
	if err := repository.CommitPairedSession(context.Background(), "tenant-a", "connection-1", "attempt-owner", envelope, make([]byte, 32)); err != nil {
		t.Fatalf("CommitPairedSession: %v", err)
	}
	if got := tx.operationNames(); !reflect.DeepEqual(got, []string{"tenant_context", "commit_encrypted_session", "commit_connection_state"}) {
		t.Fatalf("operation order = %v", got)
	}
	commitCall := tx.findCall("commit_connection_state")
	if commitCall == nil || !strings.Contains(strings.ToLower(commitCall.query), "pairing_attempt_id = $3") ||
		!reflect.DeepEqual(commitCall.args, []any{"tenant-a", "connection-1", "attempt-owner", make([]byte, 32)}) {
		t.Fatalf("commit did not fence the durable owner: %#v", commitCall)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryPairingLifecyclePersistsAndRestoresDurablePriorState(t *testing.T) {
	beginTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_active_tenant": {{values: []any{1}}},
		"begin_pairing":      {{values: []any{"reauthorization-required"}}},
	}}
	restoreTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"restore_pairing": {{values: []any{"reauthorization-required"}}}}}
	reconcileTX := &fakeTransaction{execResults: map[string][]int64{"reconcile_stale_pairings": {2}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{beginTX, restoreTX, reconcileTX}})
	prior, err := repository.BeginPairing(context.Background(), "tenant-a", "connection-1", "attempt-owner", 5*time.Minute)
	if err != nil || prior != domain.ConnectionStateReauthorizationRequired {
		t.Fatalf("BeginPairing: %v", err)
	}
	beginCall := beginTX.findCall("begin_pairing")
	if beginCall == nil || !reflect.DeepEqual(beginCall.args, []any{"tenant-a", "connection-1", "attempt-owner", int64(5 * time.Minute / time.Microsecond)}) ||
		!strings.Contains(strings.ToLower(beginCall.query), "pairing_started_at = clock_timestamp()") ||
		!strings.Contains(strings.ToLower(beginCall.query), "pairing_started_at < clock_timestamp() - ($4 * interval '1 microsecond')") {
		t.Fatalf("BeginPairing args = %#v", beginCall)
	}
	lockTenant := beginTX.findCall("lock_active_tenant")
	if lockTenant == nil || !strings.Contains(strings.ToLower(lockTenant.query), "status = 'active'") ||
		!strings.Contains(strings.ToLower(lockTenant.query), "for share") {
		t.Fatalf("BeginPairing lacks tenant lifecycle lock: %#v", lockTenant)
	}
	if operations := beginTX.operationNames(); !reflect.DeepEqual(operations, []string{"tenant_context", "lock_active_tenant", "begin_pairing"}) {
		t.Fatalf("BeginPairing lock order = %v", operations)
	}
	restored, err := repository.RestorePairing(context.Background(), "tenant-a", "connection-1", "attempt-owner")
	if err != nil || restored != domain.ConnectionStateReauthorizationRequired {
		t.Fatalf("RestorePairing = %q, %v", restored, err)
	}
	restoreCall := restoreTX.findCall("restore_pairing")
	if restoreCall == nil || !strings.Contains(strings.ToLower(restoreCall.query), "pairing_attempt_id = $3") ||
		!strings.Contains(strings.ToLower(restoreCall.query), "pairing_attempt_id = null") {
		t.Fatalf("RestorePairing did not fence and clear owner: %#v", restoreCall)
	}
	count, err := repository.ReconcileStalePairings(context.Background(), "tenant-a", 5*time.Minute)
	if err != nil || count != 2 {
		t.Fatalf("ReconcileStalePairings = %d, %v", count, err)
	}
	reconcileCall := reconcileTX.findCall("reconcile_stale_pairings")
	if reconcileCall == nil || !strings.Contains(strings.ToLower(reconcileCall.query), "pairing_started_at < clock_timestamp() - ($2 * interval '1 microsecond')") ||
		!reflect.DeepEqual(reconcileCall.args, []any{"tenant-a", int64(5 * time.Minute / time.Microsecond)}) ||
		!strings.Contains(strings.ToLower(reconcileCall.query), "pairing_attempt_id = null") {
		t.Fatalf("ReconcileStalePairings lacks strict cutoff/owner clear: %#v", reconcileCall)
	}
	assertTenantTransaction(t, beginTX, "tenant-a", true)
	assertTenantTransaction(t, restoreTX, "tenant-a", true)
	assertTenantTransaction(t, reconcileTX, "tenant-a", true)
}

func TestRepositoryRejectsInvalidReconciliationTTLBeforeSQL(t *testing.T) {
	db := &fakeBeginner{}
	repository := newRepository(db)
	for _, ttl := range []time.Duration{0, -time.Second, 24*time.Hour + time.Nanosecond} {
		if _, err := repository.ReconcileStalePairings(context.Background(), "tenant-a", ttl); !errors.Is(err, pairing.ErrInvalidConnectionState) {
			t.Fatalf("ReconcileStalePairings TTL %s = %v", ttl, err)
		}
	}
	if db.beginCalls != 0 {
		t.Fatalf("invalid reconciliation TTL started %d transactions", db.beginCalls)
	}
}

func TestRepositoryRejectsFreshPairingClaimAndFencesSupersededRestoreAndCommit(t *testing.T) {
	started := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claimTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_active_tenant":     {{values: []any{1}}},
		"begin_pairing":          {{err: sql.ErrNoRows}},
		"classify_pairing_start": {{values: []any{"pairing", sql.NullTime{Time: started, Valid: true}}}},
	}}
	restoreTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{"restore_pairing": {{err: sql.ErrNoRows}}}}
	commitTX := &fakeTransaction{execResults: map[string][]int64{"commit_connection_state": {0}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{claimTX, restoreTX, commitTX}})

	if _, err := repository.BeginPairing(context.Background(), "tenant-a", "connection-1", "attempt-new", 5*time.Minute); !errors.Is(err, pairing.ErrAttemptActive) {
		t.Fatalf("fresh BeginPairing = %v, want ErrAttemptActive", err)
	}
	if _, err := repository.RestorePairing(context.Background(), "tenant-a", "connection-1", "attempt-old"); !errors.Is(err, pairing.ErrAttemptSuperseded) {
		t.Fatalf("superseded RestorePairing = %v", err)
	}
	if err := repository.CommitPairedSession(context.Background(), "tenant-a", "connection-1", "attempt-old", validTestEnvelope(), make([]byte, 32)); !errors.Is(err, pairing.ErrAttemptSuperseded) {
		t.Fatalf("superseded CommitPairedSession = %v", err)
	}
	if !restoreTX.rolledBack || !commitTX.rolledBack {
		t.Fatal("superseded ownership operations did not roll back")
	}
}

func TestRepositoryRejectsPairingForSuspendedTenantBeforeConnectionLock(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_active_tenant": {{err: sql.ErrNoRows}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	if _, err := repository.BeginPairing(context.Background(), "tenant-a", "connection-1", "attempt-new", 5*time.Minute); !errors.Is(err, ErrTenantSuspended) {
		t.Fatalf("BeginPairing() error = %v, want ErrTenantSuspended", err)
	}
	if tx.findCall("begin_pairing") != nil || tx.committed {
		t.Fatal("pairing touched a connection for a suspended tenant")
	}
}

func TestRepositoryGenericTransitionCannotEnterProtectedPairingStates(t *testing.T) {
	db := &fakeBeginner{}
	repository := newRepository(db)
	for _, state := range []domain.ConnectionState{domain.ConnectionStatePairing, domain.ConnectionStateReauthorizationRequired} {
		if err := repository.TransitionConnection(context.Background(), "tenant-a", "connection-1", []domain.ConnectionState{domain.ConnectionStateConnected}, state); !errors.Is(err, pairing.ErrInvalidConnectionState) {
			t.Fatalf("TransitionConnection to %s = %v", state, err)
		}
	}
	if db.beginCalls != 0 {
		t.Fatalf("protected transitions reached SQL %d times", db.beginCalls)
	}
}

func TestRepositoryEncryptedSessionCASUsesRevision(t *testing.T) {
	envelope := validTestEnvelope()
	envelope.Revision = 7
	successTX := &fakeTransaction{execResults: map[string][]int64{"cas_encrypted_session": {1}}}
	conflictTX := &fakeTransaction{execResults: map[string][]int64{"cas_encrypted_session": {0}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{successTX, conflictTX}})
	swapped, err := repository.CompareAndSwapEncryptedSession(context.Background(), "tenant-a", "connection-1", 7, envelope)
	if err != nil || !swapped {
		t.Fatalf("successful CAS = %v, %v", swapped, err)
	}
	call := successTX.findCall("cas_encrypted_session")
	if call == nil || call.args[2] != uint64(7) {
		t.Fatalf("CAS args = %#v", call)
	}
	swapped, err = repository.CompareAndSwapEncryptedSession(context.Background(), "tenant-a", "connection-1", 7, envelope)
	if err != nil || swapped {
		t.Fatalf("conflicting CAS = %v, %v", swapped, err)
	}
}

func TestRepositoryRequiresExactPairedFingerprintLength(t *testing.T) {
	db := &fakeBeginner{}
	repository := newRepository(db)
	for _, size := range []int{16, 31, 33} {
		record := ConnectionRecord{Connection: domain.Connection{ID: "connection-1", TenantID: "tenant-a", State: domain.ConnectionStateConnected}, ProviderDeviceFingerprint: make([]byte, size)}
		if err := repository.SaveConnection(context.Background(), "tenant-a", record); !errors.Is(err, ErrInvalidFingerprint) {
			t.Fatalf("fingerprint size %d = %v", size, err)
		}
	}
	if db.beginCalls != 0 {
		t.Fatal("invalid fingerprint reached SQL")
	}
}

func validTestEnvelope() session.Envelope {
	return session.Envelope{Version: 1, Provider: "gmessages", Ciphertext: make([]byte, 16), WrappedDEK: []byte{2}, Nonce: make([]byte, 12), KeyID: "key", KeyVersion: 1}
}

func TestRepositoryAuthorizationFailureTransitionReturnsStableEventID(t *testing.T) {
	firstTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_connection_auth_state":     {{values: []any{"connected", sql.NullString{}}}},
		"validate_reauthorization_event": {{values: []any{"connection.reauthorization_required", "connection", "connection-1", "connection-1"}}},
	}}
	secondTX := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_connection_auth_state":     {{values: []any{"reauthorization-required", sql.NullString{String: "event-fixed", Valid: true}}}},
		"validate_reauthorization_event": {{values: []any{"connection.reauthorization_required", "connection", "connection-1", "connection-1"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{firstTX, secondTX}})
	repository.newID = func() string { return "event-fixed" }
	first, err := repository.MarkReauthorizationRequired(context.Background(), "tenant-a", "connection-1")
	if err != nil || !first.Transitioned || first.EventID != "event-fixed" {
		t.Fatalf("first transition = %#v, %v", first, err)
	}
	second, err := repository.MarkReauthorizationRequired(context.Background(), "tenant-a", "connection-1")
	if err != nil || second.Transitioned || second.EventID != first.EventID {
		t.Fatalf("repeat transition = %#v, %v", second, err)
	}
	if firstTX.findCall("mark_reauthorization_required") == nil || secondTX.findCall("mark_reauthorization_required") != nil {
		t.Fatal("idempotent transition performed the wrong updates")
	}
	if firstTX.findCall("fence_connection_lease_for_reauth") == nil || secondTX.findCall("fence_connection_lease_for_reauth") == nil {
		t.Fatal("reauthorization did not invalidate the current actor generation")
	}
	if firstTX.findCall("ensure_reauthorization_event") == nil || firstTX.findCall("ensure_reauthorization_outbox") == nil ||
		secondTX.findCall("ensure_reauthorization_event") == nil || secondTX.findCall("ensure_reauthorization_outbox") == nil {
		t.Fatalf("reauthorization event emission first=%v second=%v", firstTX.operationNames(), secondTX.operationNames())
	}
	for _, tx := range []*fakeTransaction{firstTX, secondTX} {
		eventQuery := strings.ToLower(tx.lastQuery("ensure_reauthorization_event"))
		validateQuery := strings.ToLower(tx.lastQuery("validate_reauthorization_event"))
		outboxQuery := strings.ToLower(tx.lastQuery("ensure_reauthorization_outbox"))
		if !strings.Contains(eventQuery, "on conflict (tenant_id, event_id) do nothing") ||
			!strings.Contains(validateQuery, "where tenant_id = $1 and event_id = $2") || !strings.Contains(validateQuery, "for update") ||
			!strings.Contains(outboxQuery, "on conflict (tenant_id, event_id, destination) do nothing") {
			t.Fatalf("reauthorization repair is not tenant-safe/idempotent: event=%q validate=%q outbox=%q", eventQuery, validateQuery, outboxQuery)
		}
		eventCall := tx.findCall("ensure_reauthorization_event")
		outboxCall := tx.findCall("ensure_reauthorization_outbox")
		if !reflect.DeepEqual(eventCall.args[:7], []any{
			"tenant-a", "event-fixed", "connection.reauthorization_required", "connection", "connection-1", "connection-1", "",
		}) || !reflect.DeepEqual(outboxCall.args, []any{"tenant-a", "event-fixed", "event-fixed"}) {
			t.Fatalf("reauthorization repair escaped stored tenant/event identity: event=%#v outbox=%#v", eventCall.args, outboxCall.args)
		}
	}
	var body map[string]any
	if err = json.Unmarshal(firstTX.findCall("ensure_reauthorization_event").args[7].([]byte), &body); err != nil ||
		body["event_id"] != "event-fixed" || body["type"] != "connection.reauthorization_required" || body["version"] != float64(1) ||
		body["tenant_id"] != "tenant-a" || body["connection_id"] != "connection-1" || body["status"] != "reauthorization-required" ||
		body["state"] != "reauthorization-required" || body["occurred_at"] == "" {
		t.Fatalf("reauthorization event body = %#v, error %v", body, err)
	}
}

func TestRepositoryLegacyReauthorizationRejectsMismatchedDurableEvent(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_connection_auth_state":     {{values: []any{"reauthorization-required", sql.NullString{String: "event-fixed", Valid: true}}}},
		"validate_reauthorization_event": {{values: []any{"message.updated", "message", "message-1", "connection-1"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})

	if _, err := repository.MarkReauthorizationRequired(context.Background(), "tenant-a", "connection-1"); err == nil {
		t.Fatal("legacy reauthorization accepted an event ID owned by another aggregate")
	}
	if tx.findCall("ensure_reauthorization_outbox") != nil {
		t.Fatal("mismatched legacy event was made externally visible")
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}

func TestRepositoryActorHealthWriteCannotOverwriteAuthoritativeConnectionState(t *testing.T) {
	tx := &fakeTransaction{execResults: map[string][]int64{"write_connection_health_fenced": {1}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	written, err := repository.WriteConnectionHealthFenced(context.Background(), "tenant-a", "connection-1", "owner-a", 7, ConnectionActorHealth{
		ActorState: "ready", ConnectionState: domain.ConnectionStateConnected, LastSafeReason: "none",
	})
	if err != nil || !written {
		t.Fatalf("WriteConnectionHealthFenced() = %v, %v", written, err)
	}
	if tx.findCall("write_connection_state_fenced") != nil {
		t.Fatal("actor health telemetry overwrote the authoritative connection state")
	}
	query := strings.ToLower(tx.lastQuery("write_connection_health_fenced"))
	if !strings.Contains(query, "for update") || !strings.Contains(query, "connection_actor_health.fencing_token <= excluded.fencing_token") {
		t.Fatal("actor health write does not serialize on the lease and reject an older conflicting token")
	}
	assertConnectionThenLeaseLockOrder(t, query)
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryFencedSessionWriteSerializesOnCurrentLease(t *testing.T) {
	tx := &fakeTransaction{execResults: map[string][]int64{"cas_encrypted_session_fenced": {1}}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	swapped, err := repository.CompareAndSwapEncryptedSessionFenced(context.Background(), "tenant-a", "connection-1", "owner-a", 7, 3, validTestEnvelope())
	if err != nil || !swapped {
		t.Fatalf("CompareAndSwapEncryptedSessionFenced() = %v, %v", swapped, err)
	}
	query := strings.ToLower(tx.lastQuery("cas_encrypted_session_fenced"))
	if !strings.Contains(query, "for update") {
		t.Fatal("fenced session CAS did not serialize with lease invalidation")
	}
	assertConnectionThenLeaseLockOrder(t, query)
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryFencedReauthorizationLocksConnectionBeforeLease(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"mark_reauthorization_required_fenced": {{values: []any{"event-fenced", true}}},
		"validate_reauthorization_event":       {{values: []any{"connection.reauthorization_required", "connection", "connection-1", "connection-1"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "event-fenced" }
	transitioned, err := repository.MarkReauthorizationRequiredFenced(context.Background(), "tenant-a", "connection-1", "owner-a", 7)
	if err != nil || !transitioned {
		t.Fatalf("MarkReauthorizationRequiredFenced() = %v, %v", transitioned, err)
	}
	query := strings.ToLower(tx.lastQuery("mark_reauthorization_required_fenced"))
	assertConnectionThenLeaseLockOrder(t, query)
	if !strings.Contains(query, "select tenant_id, connection_id, state") {
		t.Fatal("fenced reauthorization does not lock and return the prior connection state")
	}
	if tx.findCall("ensure_reauthorization_event") == nil || tx.findCall("ensure_reauthorization_outbox") == nil {
		t.Fatalf("fenced reauthorization omitted event outbox: %v", tx.operationNames())
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryFencedLegacyReauthorizationRepairsEventWithStoredID(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"mark_reauthorization_required_fenced": {{values: []any{"event-stored", false}}},
		"validate_reauthorization_event":       {{values: []any{"connection.reauthorization_required", "connection", "connection-1", "connection-1"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "event-unused" }

	transitioned, err := repository.MarkReauthorizationRequiredFenced(context.Background(), "tenant-a", "connection-1", "owner-a", 7)
	if err != nil || !transitioned {
		t.Fatalf("MarkReauthorizationRequiredFenced() = %v, %v", transitioned, err)
	}
	eventCall := tx.findCall("ensure_reauthorization_event")
	outboxCall := tx.findCall("ensure_reauthorization_outbox")
	if eventCall == nil || outboxCall == nil || eventCall.args[1] != "event-stored" || outboxCall.args[1] != "event-stored" {
		t.Fatalf("legacy fenced repair did not preserve stored event ID: event=%#v outbox=%#v", eventCall, outboxCall)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func assertConnectionThenLeaseLockOrder(t *testing.T, query string) {
	t.Helper()
	connectionLock := strings.Index(query, "from connections")
	leaseLock := strings.Index(query, "from connection_leases")
	if connectionLock < 0 || leaseLock < 0 || connectionLock >= leaseLock || strings.Count(query, "for update") < 2 || !strings.Contains(query, "join locked_connection") {
		t.Fatalf("query does not enforce connections -> connection_leases row-lock order:\n%s", query)
	}
}

func TestRepositoryActorHealthReadCorrelatesTelemetryToCurrentLeaseFence(t *testing.T) {
	updated := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"get_connection_health": {{values: []any{
			uint64(9), "ready", "owned", "reauthorization-required", (*time.Time)(nil), (*time.Time)(nil), (*time.Time)(nil),
			uint64(2), int64(0), "none", true, updated,
		}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	health, err := repository.GetConnectionHealth(context.Background(), "tenant-a", "connection-1")
	if err != nil || health.ConnectionState != domain.ConnectionStateReauthorizationRequired || !health.RequiresReauthorization {
		t.Fatalf("GetConnectionHealth() = %#v, %v", health, err)
	}
	query := strings.ToLower(tx.lastQuery("get_connection_health"))
	if !strings.Contains(query, "connections") || !strings.Contains(query, "h.fencing_token = l.fencing_token") {
		t.Fatal("actor health read did not join authoritative state to telemetry at the current lease fence")
	}
}

func TestQuarantineBackfillConnectionPersistsSuspendedHealthAndFencesActor(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"quarantine_backfill_connection": {{values: []any{true}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	err := repository.QuarantineBackfillConnection(context.Background(), "tenant-a", "connection-a", "provider-protocol")
	if err != nil {
		t.Fatal(err)
	}
	query := strings.ToLower(tx.lastQuery("quarantine_backfill_connection"))
	for _, required := range []string{
		"for update", "fencing_token + 1", "owner_id = null", "state = 'suspended'",
		"connection_actor_health", "actor_state", "'stopped'", "last_safe_reason",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("backfill quarantine SQL missing %q: %s", required, query)
		}
	}
	if call := tx.findCall("quarantine_backfill_connection"); call == nil || call.args[2] != "provider-protocol" {
		t.Fatalf("quarantine safe reason args = %+v", call)
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryAuthorizationFailureRejectsUnpairedCurrentState(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_connection_auth_state": {{values: []any{"unpaired", sql.NullString{}}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	if _, err := repository.MarkReauthorizationRequired(context.Background(), "tenant-a", "connection-1"); !errors.Is(err, pairing.ErrInvalidConnectionState) {
		t.Fatalf("MarkReauthorizationRequired error = %v", err)
	}
	if tx.findCall("mark_reauthorization_required") != nil {
		t.Fatal("unpaired connection was marked for reauthorization")
	}
}

func TestRepositoryAuthorizationFailureAcceptsSuspendedPairedState(t *testing.T) {
	tx := &fakeTransaction{rowResults: map[string][]fakeRowResult{
		"lock_connection_auth_state":     {{values: []any{"suspended", sql.NullString{}}}},
		"validate_reauthorization_event": {{values: []any{"connection.reauthorization_required", "connection", "connection-1", "connection-1"}}},
	}}
	repository := newRepository(&fakeBeginner{transactions: []*fakeTransaction{tx}})
	repository.newID = func() string { return "event-suspended" }

	transition, err := repository.MarkReauthorizationRequired(context.Background(), "tenant-a", "connection-1")
	if err != nil || !transition.Transitioned || transition.EventID != "event-suspended" {
		t.Fatalf("suspended transition = %#v, %v", transition, err)
	}
	if tx.findCall("mark_reauthorization_required") == nil {
		t.Fatal("suspended paired connection was not marked for reauthorization")
	}
}

func TestRepositoryRejectsEmptyIdentifiersBeforeSQL(t *testing.T) {
	db := &fakeBeginner{}
	repository := newRepository(db)
	phone, _ := domain.ParseE164("+12025550100")
	tests := []struct {
		name string
		call func() error
	}{
		{name: "tenant", call: func() error { _, err := repository.GetConnection(context.Background(), "", "connection-1"); return err }},
		{name: "connection", call: func() error { _, err := repository.GetConnection(context.Background(), "tenant-a", ""); return err }},
		{name: "contact", call: func() error { return repository.SetContactAlias(context.Background(), "tenant-a", "", "alias") }},
		{name: "label", call: func() error { return repository.AttachLabel(context.Background(), "tenant-a", "contact-1", "") }},
		{name: "provider contact", call: func() error {
			_, err := repository.UpsertProviderContact(context.Background(), contactsync.ProviderContactUpsert{TenantID: "tenant-a", ConnectionID: "connection-1", Phone: phone})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("operation accepted an empty identifier")
			}
		})
	}
	if db.beginCalls != 0 {
		t.Fatalf("invalid identifiers started %d transactions", db.beginCalls)
	}
}

func TestRepositoryLabelLinksAreTenantIsolated(t *testing.T) {
	tx := &fakeTransaction{}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
	err := newRepository(db).AttachLabel(context.Background(), "tenant-a", "contact-1", "label-1")
	if err != nil {
		t.Fatalf("AttachLabel() error = %v", err)
	}
	call := tx.findCall("attach_label")
	if call == nil || !reflect.DeepEqual(call.args, []any{"tenant-a", "contact-1", "label-1"}) {
		t.Fatalf("AttachLabel SQL args = %#v", call)
	}
	query := strings.ToLower(call.query)
	if !strings.Contains(query, "tenant_id") || !strings.Contains(query, "select") || !strings.Contains(query, "contacts") || !strings.Contains(query, "labels") {
		t.Fatal("AttachLabel does not prove contact and label ownership in the same tenant")
	}
	assertTenantTransaction(t, tx, "tenant-a", true)
}

func TestRepositoryAttachLabelFailsClosedWhenTenantObjectsAreMissing(t *testing.T) {
	tx := &fakeTransaction{execResults: map[string][]int64{"attach_label": {0}}}
	db := &fakeBeginner{transactions: []*fakeTransaction{tx}}

	err := newRepository(db).AttachLabel(context.Background(), "tenant-a", "contact-elsewhere", "label-elsewhere")
	if !errors.Is(err, ErrContactLabelLinkNotFound) {
		t.Fatalf("AttachLabel() error = %v, want ErrContactLabelLinkNotFound", err)
	}
	assertTenantTransaction(t, tx, "tenant-a", false)
}

func TestRepositoryDetachLabelAtomicallyDistinguishesObjectAndLinkState(t *testing.T) {
	tests := []struct {
		name          string
		row           []any
		wantErr       error
		wantCommitted bool
	}{
		{name: "existing link", row: []any{true, true, true}, wantCommitted: true},
		{name: "already absent link", row: []any{true, true, false}, wantCommitted: true},
		{name: "missing contact", row: []any{false, true, false}, wantErr: ErrContactNotFound},
		{name: "missing label", row: []any{true, false, false}, wantErr: ErrLabelNotFound},
		{name: "both objects missing", row: []any{false, false, false}, wantErr: ErrContactNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := &fakeTransaction{}
			row("detach_label", test.row)(tx)
			db := &fakeBeginner{transactions: []*fakeTransaction{tx}}

			err := newRepository(db).DetachLabel(context.Background(), "tenant-a", "contact-1", "label-1")
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("DetachLabel() error = %v, want %v", err, test.wantErr)
			}
			call := tx.findCall("detach_label")
			if call == nil || !reflect.DeepEqual(call.args, []any{"tenant-a", "contact-1", "label-1"}) {
				t.Fatalf("DetachLabel SQL args = %#v", call)
			}
			query := strings.ToLower(call.query)
			for _, required := range []string{"with", "delete from contact_labels", "contacts", "labels", "contact_exists", "label_exists", "deleted"} {
				if !strings.Contains(query, required) {
					t.Fatalf("DetachLabel query does not atomically report %q: %s", required, call.query)
				}
			}
			assertTenantTransaction(t, tx, "tenant-a", test.wantCommitted)
		})
	}
}

func TestRepositoryPracticalOperationsUseTenantTransactions(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		operation string
		prepare   func(*fakeTransaction)
		call      func(*Repository) error
	}{
		{name: "save tenant", operation: "save_tenant", call: func(r *Repository) error {
			return r.SaveTenant(context.Background(), domain.Tenant{ID: "tenant-a", Name: "Tenant A"})
		}},
		{name: "save connection", operation: "save_connection", prepare: row("check_connection_quota", []any{true, DefaultMaxConnectionsPerTenant, DefaultMaxConnectionsPerTenant, "active"}), call: func(r *Repository) error {
			return r.SaveConnection(context.Background(), "tenant-a", ConnectionRecord{Connection: domain.Connection{ID: "connection-1", TenantID: "tenant-a", Name: "Phone", State: domain.ConnectionStateConnected}, ProviderDeviceFingerprint: make([]byte, 32)})
		}},
		{name: "list connections", operation: "list_connections", prepare: rows("list_connections", []any{"connection-1", "Phone", "connected", []byte("0123456789abcdef")}), call: func(r *Repository) error { _, err := r.ListConnections(context.Background(), "tenant-a"); return err }},
		{name: "list lines", operation: "list_lines", prepare: rows("list_lines", []any{
			"line-1", "connection-1", "participant", "outgoing", "+12025550100", "SIM 1",
			"Carrier", "#123456", true, int32(1), int32(2), LineDiscoveryAuthenticatedGoogleSettings,
		}), call: func(r *Repository) error {
			_, err := r.ListLines(context.Background(), "tenant-a", "connection-1")
			return err
		}},
		{name: "save session", operation: "save_encrypted_session", call: func(r *Repository) error {
			return r.SaveEncryptedSession(context.Background(), "tenant-a", "connection-1", validTestEnvelope())
		}},
		{name: "load session", operation: "load_encrypted_session", prepare: row("load_encrypted_session", []any{uint64(1), 1, "gmessages", make([]byte, 16), []byte{2}, make([]byte, 12), "key", 1}), call: func(r *Repository) error {
			_, err := r.LoadEncryptedSession(context.Background(), "tenant-a", "connection-1")
			return err
		}},
		{name: "list contacts", operation: "list_contacts", prepare: rows("list_contacts", []any{"contact-1", "+12025550100", "Alias", "Provider", `[]`}), call: func(r *Repository) error {
			_, err := r.ListContacts(context.Background(), "tenant-a", ContactListOptions{Limit: 20})
			return err
		}},
		{name: "set alias", operation: "set_contact_alias", call: func(r *Repository) error {
			return r.SetContactAlias(context.Background(), "tenant-a", "contact-1", "Alias")
		}},
		{name: "clear alias", operation: "clear_contact_alias", call: func(r *Repository) error { return r.ClearContactAlias(context.Background(), "tenant-a", "contact-1") }},
		{name: "create label", operation: "create_label", call: func(r *Repository) error {
			label, _ := domain.NewLabel("label-1", "tenant-a", "Potential Client")
			return r.CreateLabel(context.Background(), "tenant-a", label)
		}},
		{name: "list labels", operation: "list_labels", prepare: rows("list_labels", []any{"label-1", "Potential Client", "potential-client"}), call: func(r *Repository) error { _, err := r.ListLabels(context.Background(), "tenant-a"); return err }},
		{name: "detach label", operation: "detach_label", prepare: row("detach_label", []any{true, true, true}), call: func(r *Repository) error {
			return r.DetachLabel(context.Background(), "tenant-a", "contact-1", "label-1")
		}},
		{name: "begin sync", operation: "begin_contact_sync", call: func(r *Repository) error {
			return r.BeginContactSyncRun(context.Background(), "tenant-a", ContactSyncRun{ID: "run-1", TenantID: "tenant-a", ConnectionID: "connection-1", Status: ContactSyncRunning, StartedAt: now})
		}},
		{name: "finish sync", operation: "finish_contact_sync", call: func(r *Repository) error {
			return r.FinishContactSyncRun(context.Background(), "tenant-a", "run-1", ContactSyncSucceeded, 4, 1, "", now.Add(time.Minute))
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx := &fakeTransaction{}
			if test.prepare != nil {
				test.prepare(tx)
			}
			db := &fakeBeginner{transactions: []*fakeTransaction{tx}}
			err := test.call(newRepository(db))
			if err != nil {
				t.Fatalf("operation error = %v", err)
			}
			if tx.findCall(test.operation) == nil {
				t.Fatalf("operation %q was not executed", test.operation)
			}
			assertTenantTransaction(t, tx, "tenant-a", true)
		})
	}
}

type fakeBeginner struct {
	transactions []*fakeTransaction
	beginCalls   int
}

func (db *fakeBeginner) BeginTx(context.Context, *sql.TxOptions) (transaction, error) {
	db.beginCalls++
	if len(db.transactions) == 0 {
		return nil, errors.New("unexpected transaction")
	}
	tx := db.transactions[0]
	db.transactions = db.transactions[1:]
	return tx, nil
}

type fakeCall struct {
	operation string
	query     string
	args      []any
}

type fakeRowResult struct {
	values []any
	err    error
}

type fakeTransaction struct {
	calls       []fakeCall
	execErrors  map[string][]error
	execResults map[string][]int64
	rowResults  map[string][]fakeRowResult
	rowsResult  map[string][][]any
	tenant      string
	committed   bool
	rolledBack  bool
	commitErr   error
}

func (tx *fakeTransaction) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	op := operationName(query)
	tx.calls = append(tx.calls, fakeCall{operation: op, query: query, args: append([]any(nil), args...)})
	if op == "tenant_context" {
		if len(args) != 1 {
			return nil, errors.New("tenant context needs one argument")
		}
		tx.tenant, _ = args[0].(string)
	} else if tx.tenant == "" {
		return nil, errors.New("data operation before tenant context")
	}
	if sequence := tx.execErrors[op]; len(sequence) > 0 {
		err := sequence[0]
		tx.execErrors[op] = sequence[1:]
		if err != nil {
			return nil, err
		}
	}
	if sequence := tx.execResults[op]; len(sequence) > 0 {
		result := sequence[0]
		tx.execResults[op] = sequence[1:]
		return fakeResult(result), nil
	}
	return fakeResult(1), nil
}

func (tx *fakeTransaction) QueryRowContext(_ context.Context, query string, args ...any) rowScanner {
	op := operationName(query)
	tx.calls = append(tx.calls, fakeCall{operation: op, query: query, args: append([]any(nil), args...)})
	if tx.tenant == "" {
		return fakeRowResult{err: errors.New("data operation before tenant context")}
	}
	sequence := tx.rowResults[op]
	if len(sequence) == 0 {
		return fakeRowResult{err: errors.New("unexpected row query: " + op)}
	}
	result := sequence[0]
	tx.rowResults[op] = sequence[1:]
	return result
}

func (tx *fakeTransaction) QueryContext(_ context.Context, query string, args ...any) (rowIterator, error) {
	op := operationName(query)
	tx.calls = append(tx.calls, fakeCall{operation: op, query: query, args: append([]any(nil), args...)})
	if tx.tenant == "" {
		return nil, errors.New("data operation before tenant context")
	}
	return &fakeRows{rows: tx.rowsResult[op]}, nil
}

func (tx *fakeTransaction) Commit() error {
	if tx.commitErr != nil {
		return tx.commitErr
	}
	tx.committed = true
	return nil
}
func (tx *fakeTransaction) Rollback() error { tx.rolledBack = true; return nil }

func (tx *fakeTransaction) findCall(operation string) *fakeCall {
	for index := range tx.calls {
		if tx.calls[index].operation == operation {
			return &tx.calls[index]
		}
	}
	return nil
}

func (tx *fakeTransaction) lastQuery(operation string) string {
	for index := len(tx.calls) - 1; index >= 0; index-- {
		if tx.calls[index].operation == operation {
			return tx.calls[index].query
		}
	}
	return ""
}

func (tx *fakeTransaction) operationNames() []string {
	names := make([]string, len(tx.calls))
	for index, call := range tx.calls {
		names[index] = call.operation
	}
	return names
}

func (row fakeRowResult) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	return assignValues(row.values, destinations)
}

type fakeRows struct {
	rows  [][]any
	index int
	err   error
}

func (rows *fakeRows) Next() bool { return rows.index < len(rows.rows) }
func (rows *fakeRows) Scan(destinations ...any) error {
	if rows.index >= len(rows.rows) {
		return errors.New("scan beyond rows")
	}
	err := assignValues(rows.rows[rows.index], destinations)
	rows.index++
	return err
}
func (rows *fakeRows) Close() error { return nil }
func (rows *fakeRows) Err() error   { return rows.err }

func assignValues(values []any, destinations []any) error {
	if len(values) != len(destinations) {
		return errors.New("scan destination count mismatch")
	}
	for index, value := range values {
		destination := reflect.ValueOf(destinations[index])
		if destination.Kind() != reflect.Pointer || destination.IsNil() {
			return errors.New("scan destination is not a pointer")
		}
		target := destination.Elem()
		source := reflect.ValueOf(value)
		if source.Type().AssignableTo(target.Type()) {
			target.Set(source)
		} else if source.Type().ConvertibleTo(target.Type()) {
			target.Set(source.Convert(target.Type()))
		} else {
			return errors.New("incompatible scan value")
		}
	}
	return nil
}

type fakeResult int64

func (result fakeResult) LastInsertId() (int64, error) { return 0, errors.New("unsupported") }
func (result fakeResult) RowsAffected() (int64, error) { return int64(result), nil }

func operationName(query string) string {
	start := strings.Index(query, "/* op:")
	if start < 0 {
		return ""
	}
	start += len("/* op:")
	end := strings.Index(query[start:], " */")
	if end < 0 {
		return ""
	}
	return query[start : start+end]
}

func row(operation string, values []any) func(*fakeTransaction) {
	return func(tx *fakeTransaction) {
		tx.rowResults = map[string][]fakeRowResult{operation: {{values: values}}}
	}
}

func rows(operation string, values ...[]any) func(*fakeTransaction) {
	return func(tx *fakeTransaction) {
		tx.rowsResult = map[string][][]any{operation: values}
	}
}

func assertTenantTransaction(t *testing.T, tx *fakeTransaction, tenant string, committed bool) {
	t.Helper()
	if tx.tenant != tenant {
		t.Errorf("transaction tenant = %q, want %q", tx.tenant, tenant)
	}
	if tx.committed != committed {
		t.Errorf("committed = %v, want %v", tx.committed, committed)
	}
	if tx.rolledBack == committed {
		t.Errorf("rolledBack = %v, want %v", tx.rolledBack, !committed)
	}
}
