package postgres

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestLoadSchemaMigrationsRequiresContiguousImmutableCatalog(t *testing.T) {
	migrations, err := LoadSchemaMigrations()
	if err != nil {
		t.Fatalf("LoadSchemaMigrations() error = %v", err)
	}
	if len(migrations) < 16 {
		t.Fatalf("migration count = %d, want at least 16", len(migrations))
	}
	foundTenantOperations := false
	for index, migration := range migrations {
		wantVersion := index + 1
		if migration.Version != wantVersion || !strings.HasPrefix(migration.Name, fmt.Sprintf("%04d_", wantVersion)) {
			t.Fatalf("migration[%d] identity = (%d, %q), want version/prefix %d/%q", index, migration.Version, migration.Name, wantVersion, fmt.Sprintf("%04d_", wantVersion))
		}
		wantDigest := sha256.Sum256(migration.Contents)
		if !bytes.Equal(migration.SHA256[:], wantDigest[:]) {
			t.Fatalf("migration %s digest does not cover the shipped bytes", migration.Name)
		}
		foundTenantOperations = foundTenantOperations || migration.Name == "0016_tenant_operations.sql"
	}
	if !foundTenantOperations {
		t.Fatal("tenant operations migration is missing")
	}

	bad := append([]SchemaMigration(nil), migrations...)
	bad[1].Version = 1
	if err := validateSchemaMigrationCatalog(bad); !errors.Is(err, ErrMigrationCatalog) {
		t.Fatalf("duplicate version error = %v, want ErrMigrationCatalog", err)
	}
	bad = append([]SchemaMigration(nil), migrations...)
	bad[1].Name = "0003_wrong.sql"
	if err := validateSchemaMigrationCatalog(bad); !errors.Is(err, ErrMigrationCatalog) {
		t.Fatalf("gap/name mismatch error = %v, want ErrMigrationCatalog", err)
	}
	bad = append([]SchemaMigration(nil), migrations...)
	bad[0].SHA256[0] ^= 0xff
	if err := validateSchemaMigrationCatalog(bad); !errors.Is(err, ErrMigrationCatalog) {
		t.Fatalf("digest mismatch error = %v, want ErrMigrationCatalog", err)
	}
}

func TestPrepareMigrationSQLRemovesOnlyCompleteShippedTransactionEnvelope(t *testing.T) {
	input, readErr := Migrations.ReadFile("migrations/0003_pairing_upgrade_support.sql")
	if readErr != nil {
		t.Fatal(readErr)
	}
	prepared, err := prepareMigrationSQL(input)
	if err != nil {
		t.Fatalf("prepareMigrationSQL() error = %v", err)
	}
	if strings.Contains(strings.ToUpper(prepared), "BEGIN;") || strings.Contains(strings.ToUpper(prepared), "COMMIT;") {
		t.Fatalf("prepared migration retained transaction control: %q", prepared)
	}
	if !strings.Contains(prepared, "pairing_attempt_id") {
		t.Fatalf("prepared migration lost body: %q", prepared)
	}
	for _, invalid := range [][]byte{
		[]byte("BEGIN; SELECT 1; COMMIT;"),
		[]byte("BEGIN;\nSELECT 1;"),
		[]byte("SELECT 1;\nCOMMIT;"),
		[]byte("SELECT 1; COMMIT;-- hidden on the same line\nSELECT 2;"),
		[]byte("BEGIN;\nCOMMIT;\nSELECT 1;"),
		[]byte("BEGIN;\nBEGIN;\nSELECT 1;\nCOMMIT;"),
		[]byte("START /* disguised */ TRANSACTION; SELECT 1; COMMIT;"),
		[]byte("BEGIN WORK; SELECT 1; END TRANSACTION;"),
		[]byte("SELECT 1; END /* disguised */ WORK;"),
		[]byte("ROLLBACK /* no semicolon */"),
	} {
		if _, err = prepareMigrationSQL(invalid); !errors.Is(err, ErrMigrationCatalog) {
			t.Fatalf("prepareMigrationSQL(%q) error = %v, want ErrMigrationCatalog", invalid, err)
		}
	}
}

func TestPrepareMigrationSQLRejectsEverySessionEndingOrDetachingMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{"abort", "ABORT; SELECT 1;"},
		{"abort work and no chain", "SELECT 1; ABORT /* disguised */ WORK AND NO CHAIN; SELECT 2;"},
		{"prepare transaction", "PREPARE TRANSACTION 'detached-migration';"},
		{"commented prepare transaction", "SELECT 1; PREPARE /* disguised */ TRANSACTION 'detached-migration'; SELECT 2;"},
		{"commit prepared", "COMMIT PREPARED 'detached-migration';"},
		{"rollback prepared", "ROLLBACK PREPARED 'detached-migration';"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := prepareMigrationSQL([]byte(test.sql)); !errors.Is(err, ErrMigrationCatalog) {
				t.Fatalf("prepareMigrationSQL(%q) error = %v, want ErrMigrationCatalog before database access", test.sql, err)
			}
		})
	}
}

func TestPrepareMigrationSQLIgnoresTransactionWordsInsideSQLLexicalBodies(t *testing.T) {
	input := []byte(`-- COMMIT;
CREATE TABLE transaction_words (value text);
INSERT INTO transaction_words VALUES ('BEGIN; COMMIT; ROLLBACK; ABORT; PREPARE TRANSACTION;'), ('it''s END;');
INSERT INTO transaction_words VALUES (E'escaped\' COMMIT;');
INSERT INTO transaction_words VALUES ($body$BEGIN; START TRANSACTION; COMMIT; END; ROLLBACK; ABORT; PREPARE TRANSACTION;$body$);
CREATE FUNCTION transaction_word_fixture() RETURNS void AS $$
BEGIN
    PERFORM 'COMMIT;';
END;
$$ LANGUAGE plpgsql;
/* outer ROLLBACK; /* nested COMMIT; */ still comment */
SELECT "COMMIT" FROM transaction_words;
`)
	prepared, err := prepareMigrationSQL(input)
	if err != nil {
		t.Fatalf("prepareMigrationSQL() rejected lexical bodies: %v", err)
	}
	if got := string(prepared); got != string(input) {
		t.Fatalf("prepareMigrationSQL() changed transaction-free SQL\n got: %q\nwant: %q", got, input)
	}
}

func TestValidateMigrationLedgerRejectsGapDriftNameMismatchAndDatabaseAhead(t *testing.T) {
	migrations, err := LoadSchemaMigrations()
	if err != nil {
		t.Fatal(err)
	}
	valid := []MigrationLedgerRow{
		{Version: 1, Name: migrations[0].Name, SHA256: migrations[0].SHA256[:]},
		{Version: 2, Name: migrations[1].Name, SHA256: migrations[1].SHA256[:]},
	}
	status, err := validateMigrationLedger(migrations, valid)
	if err != nil || status.Current != 2 || status.Latest != len(migrations) || len(status.Pending) != len(migrations)-2 {
		t.Fatalf("valid ledger status = (%+v, %v)", status, err)
	}

	cases := []struct {
		name string
		rows []MigrationLedgerRow
		want error
	}{
		{"gap", []MigrationLedgerRow{{Version: 2, Name: migrations[1].Name, SHA256: migrations[1].SHA256[:]}}, ErrMigrationGap},
		{"name", []MigrationLedgerRow{{Version: 1, Name: "0001_changed.sql", SHA256: migrations[0].SHA256[:]}}, ErrMigrationDrift},
		{"checksum", []MigrationLedgerRow{{Version: 1, Name: migrations[0].Name, SHA256: make([]byte, sha256.Size)}}, ErrMigrationDrift},
		{"ahead", []MigrationLedgerRow{{Version: len(migrations) + 1, Name: fmt.Sprintf("%04d_future.sql", len(migrations)+1), SHA256: make([]byte, sha256.Size)}}, ErrDatabaseAhead},
		{"duplicate", []MigrationLedgerRow{valid[0], valid[0]}, ErrMigrationDuplicate},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, gotErr := validateMigrationLedger(migrations, test.rows); !errors.Is(gotErr, test.want) {
				t.Fatalf("validateMigrationLedger() error = %v, want %v", gotErr, test.want)
			}
		})
	}
}

func TestMigrationConfigIsStrictlyBounded(t *testing.T) {
	config := MigrationRunnerConfig{}
	if err := config.setDefaultsAndValidate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
	if config.LockTimeout != 30*time.Second || config.StatementTimeout != 2*time.Minute {
		t.Fatalf("defaults = %+v", config)
	}
	for _, config := range []MigrationRunnerConfig{
		{LockTimeout: 99 * time.Millisecond, StatementTimeout: time.Second},
		{LockTimeout: 10 * time.Minute, StatementTimeout: time.Second},
		{LockTimeout: time.Second, StatementTimeout: 999 * time.Millisecond},
		{LockTimeout: time.Second, StatementTimeout: 31 * time.Minute},
	} {
		if err := config.setDefaultsAndValidate(); !errors.Is(err, ErrMigrationConfig) {
			t.Fatalf("config %+v error = %v, want ErrMigrationConfig", config, err)
		}
	}
}

func TestAdoptionCatalogVerifiesEveryTenantTableAndItsFailClosedPolicy(t *testing.T) {
	migrations, err := LoadSchemaMigrations()
	if err != nil {
		t.Fatal(err)
	}
	allProbes := ""
	for _, migration := range migrations {
		allProbes += migration.AdoptionProbe + "\n"
	}
	for _, table := range []string{
		"tenants", "connections", "lines", "connection_sessions", "contacts", "provider_contact_sources", "labels", "contact_labels", "contact_sync_runs",
		"connection_leases", "connection_actor_health", "conversations", "messages", "message_status_history", "message_idempotency", "message_lanes",
		"message_attempts", "gateway_events", "event_outbox", "provider_inbox", "provider_inbox_conflicts", "media_objects", "message_media", "media_fetch_jobs",
		"webhook_endpoints", "webhook_deliveries", "webhook_attempts", "webhook_dlq", "kafka_commands", "kafka_event_deliveries", "kafka_command_dlq",
		"provider_backfill_checkpoints", "provider_cursor_history", "provider_cursor_budgets", "provider_response_id_quarantine", "provider_rejected_responses",
		"provider_response_reservations", "provider_response_overflow_audits", "tenant_admin_events",
	} {
		if !strings.Contains(allProbes, "'"+table+"'") {
			t.Fatalf("adoption catalog does not verify tenant table %q", table)
		}
	}
	for _, failClosedMarker := range []string{"relrowsecurity", "relforcerowsecurity", "sirenaix.tenant_id", "_tenant_isolation"} {
		if !strings.Contains(allProbes, failClosedMarker) {
			t.Fatalf("adoption catalog does not verify %q", failClosedMarker)
		}
	}
	rlsProbe := tenantTablesAdoptionProbe("contacts")
	if strings.Contains(strings.ToUpper(rlsProbe), " LIKE ") {
		t.Fatal("adoption catalog uses substring matching for an RLS policy")
	}
	for _, exactMarker := range []string{"pg_get_expr(policy.polqual", "pg_get_expr(policy.polwithcheck", "regexp_replace", "tenant_id=NULLIF(current_setting(''sirenaix.tenant_id''::text,true),''''::text)"} {
		if !strings.Contains(rlsProbe, exactMarker) {
			t.Fatalf("adoption catalog does not require canonical policy expression marker %q", exactMarker)
		}
	}
	for _, structuralMarker := range []string{
		"provider_device_fingerprint", "reauthorization_event_id", "envelope_version", "provider", "pairing_prior_state", "pairing_started_at", "pairing_attempt_id", "revision",
		"connections_fingerprint_matches_state", "connections_reauthorization_event_required", "connections_pairing_metadata_required", "connection_sessions_revision_positive",
		"ordering_key", "claim_token", "generation", "endpoint_generation", "http_started_at", "attachment_identity_digest", "provider_identity_digest",
		"conflicting_envelope_size", "occurrence_count", "max_connections", "suspended_at", "tenant_suspend_prior_state", "tenants_suspension_timestamp",
	} {
		if !strings.Contains(allProbes, structuralMarker) {
			t.Fatalf("adoption catalog does not verify structural marker %q", structuralMarker)
		}
	}
}

func TestMigrationAdoptionRegistryIsExplicitlyExtensible(t *testing.T) {
	if len(migrationAdoptionProbes) < 17 {
		t.Fatalf("adoption registry entries = %d, want at least 17", len(migrationAdoptionProbes))
	}
	if got := migrationAdoptionProbe(len(migrationAdoptionProbes) + 100); got != "" {
		t.Fatalf("unknown migration adoption probe = %q, want empty fail-closed marker", got)
	}
}

func TestLineMetadataAdoptionProbeIsExactAndFailClosed(t *testing.T) {
	probe := lineMetadataAdoptionProbe()
	for _, marker := range []string{
		"carrier_name", "color_hex", "rcs_enabled", "provider_sim_number", "provider_sim_payload_type", "discovery_source", "active",
		"lines_carrier_name_boundary", "lines_color_hex_boundary", "lines_discovery_source_check", "messages_direction_check",
		"authenticated_google_settings", "unknown", "connoinherit", "relrowsecurity", "relforcerowsecurity",
		"lines", "messages", "connections", "gateway_events", "event_outbox",
	} {
		if !strings.Contains(probe, marker) {
			t.Fatalf("line metadata adoption probe lacks %q", marker)
		}
	}
	if strings.Contains(strings.ToUpper(probe), " LIKE ") {
		t.Fatal("line metadata adoption probe uses permissive substring matching")
	}
}
