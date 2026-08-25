package postgres

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
)

func TestInitialMigrationEnforcesTenantOwnershipAndEncryptedSessions(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0001_gateway.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := strings.ToLower(string(contents))

	tables := []string{
		"tenants", "connections", "lines", "connection_sessions", "contacts",
		"provider_contact_sources", "labels", "contact_labels", "contact_sync_runs",
	}
	requiredConstraints := map[string][]string{
		"tenants": {"primary key (tenant_id)"},
		"connections": {
			"primary key (tenant_id, connection_id)",
			"unique (tenant_id, provider_device_fingerprint)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
		"lines": {
			"primary key (tenant_id, line_id)",
			"unique (tenant_id, connection_id, line_id)",
			"unique (tenant_id, connection_id, provider_participant_id, provider_outgoing_id)",
			"foreign key (tenant_id, connection_id) references connections (tenant_id, connection_id)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
		"connection_sessions": {
			"primary key (tenant_id, connection_id)",
			"foreign key (tenant_id, connection_id) references connections (tenant_id, connection_id)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
		"contacts": {
			"primary key (tenant_id, contact_id)",
			"unique (tenant_id, normalized_phone)",
			"unique (tenant_id, contact_id, normalized_phone)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
		"provider_contact_sources": {
			"primary key (tenant_id, connection_id, provider_contact_id)",
			"foreign key (tenant_id, connection_id) references connections (tenant_id, connection_id)",
			"foreign key (tenant_id, contact_id, normalized_phone) references contacts (tenant_id, contact_id, normalized_phone)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
		"labels": {
			"primary key (tenant_id, label_id)",
			"unique (tenant_id, normalized_slug)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
		"contact_labels": {
			"primary key (tenant_id, contact_id, label_id)",
			"foreign key (tenant_id, contact_id) references contacts (tenant_id, contact_id)",
			"foreign key (tenant_id, label_id) references labels (tenant_id, label_id)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
		"contact_sync_runs": {
			"primary key (tenant_id, sync_run_id)",
			"unique (tenant_id, connection_id, sync_run_id)",
			"foreign key (tenant_id, connection_id) references connections (tenant_id, connection_id)",
			"foreign key (tenant_id) references tenants (tenant_id)",
		},
	}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			body := tableBody(t, sql, table)
			normalizedBody := normalizeSQL(body)
			if !strings.Contains(body, "tenant_id") {
				t.Fatalf("table %s is not tenant scoped", table)
			}
			if table != "tenants" && !regexp.MustCompile(`foreign key\s*\(\s*tenant_id`).MatchString(body) {
				t.Fatalf("table %s lacks a tenant-aware foreign key", table)
			}
			for _, constraint := range requiredConstraints[table] {
				if !strings.Contains(normalizedBody, constraint) {
					t.Errorf("table %s lacks constraint %q", table, constraint)
				}
			}
			for _, clause := range []string{
				"alter table " + table + " enable row level security",
				"alter table " + table + " force row level security",
				"create policy " + table + "_tenant_isolation",
			} {
				if !strings.Contains(sql, clause) {
					t.Errorf("migration lacks %q", clause)
				}
			}
			policy := policyBody(t, sql, table)
			predicate := "tenant_id = nullif(current_setting('sirenaix.tenant_id', true), '')"
			if !strings.Contains(policy, "using ("+predicate+")") {
				t.Errorf("policy %s_tenant_isolation lacks fail-closed USING predicate", table)
			}
			if !strings.Contains(policy, "with check ("+predicate+")") {
				t.Errorf("policy %s_tenant_isolation lacks fail-closed WITH CHECK predicate", table)
			}
		})
	}

	required := []string{
		"ciphertext", "wrapped_dek", "nonce", "key_id", "key_version",
		"provider_participant_id", "provider_outgoing_id", "normalized_phone",
	}
	if !hasIndexPrefix(sql, "provider_contact_sources", "tenant_id", "contact_id", "normalized_phone") {
		t.Error("provider_contact_sources lacks FK-supporting (tenant_id, contact_id, normalized_phone) index")
	}
	if !hasIndexPrefix(sql, "contact_labels", "tenant_id", "label_id") {
		t.Error("contact_labels lacks FK-supporting (tenant_id, label_id) index")
	}
	for _, fragment := range required {
		if !strings.Contains(sql, fragment) {
			t.Errorf("migration lacks required contract %q", fragment)
		}
	}

	sessionBody := tableBody(t, sql, "connection_sessions")
	for _, forbidden := range []string{"cookie", "token", "credential", "secret", "password", "private_key", "plaintext"} {
		if strings.Contains(sessionBody, forbidden) {
			t.Errorf("connection_sessions contains forbidden plaintext-secret name %q", forbidden)
		}
	}
	for _, forbidden := range []string{"provider_cookie", "access_token", "refresh_token", "provider_credential", "session_secret", "plaintext_session"} {
		if strings.Contains(sql, forbidden) {
			t.Errorf("migration contains forbidden plaintext-secret column %q", forbidden)
		}
	}
}

func TestPairingMigrationAddsOnlyEnvelopeAndTransitionMetadata(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0002_pairing_sessions.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := strings.ToLower(string(contents))
	for _, required := range []string{"envelope_version", "provider", "reauthorization_event_id", "drop not null"} {
		if !strings.Contains(sql, required) {
			t.Errorf("pairing migration lacks %q", required)
		}
	}
	for _, forbidden := range []string{"cookie", "token", "credential", "private_key", "plaintext", " dek "} {
		if strings.Contains(sql, forbidden) {
			t.Errorf("pairing migration contains secret-bearing column term %q", forbidden)
		}
	}
}

func TestPairingHardeningMigrationsPhaseReconciliationConstraintsAndValidation(t *testing.T) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	wantNames := []string{"0001_gateway.sql", "0002_pairing_sessions.sql", "0003_pairing_upgrade_support.sql", "0004_pairing_constraints.sql", "0005_validate_pairing_constraints.sql", "0006_connection_actor.sql", "0007_durable_messaging.sql", "0008_task7_review_hardening.sql", "0009_task7_delivery_admission.sql", "0010_task7_backfill_checkpoints.sql", "0011_task7_cursor_history.sql", "0012_task7_cursor_budget.sql", "0013_task7_provider_response_id_boundary.sql", "0014_task7_rejected_provider_responses.sql", "0015_task7_provider_response_reservations.sql"}
	if len(names) < len(wantNames) || strings.Join(names[:len(wantNames)], ",") != strings.Join(wantNames, ",") {
		t.Fatalf("shipped migration prefix = %v, want %v", names, wantNames)
	}
	if len(names) != 17 || names[15] != "0016_tenant_operations.sql" || names[16] != "0017_task8_line_metadata.sql" {
		t.Fatalf("integrated migration suffix = %v, want tenant operations then line metadata", names)
	}

	supportBytes, err := os.ReadFile(filepath.Join("migrations", wantNames[2]))
	if err != nil {
		t.Fatalf("read support migration: %v", err)
	}
	support := normalizeSQL(strings.ToLower(string(supportBytes)))
	for _, required := range []string{
		"pairing_attempt_id", "pairing_prior_state", "pairing_started_at", "revision bigint",
		"delete from connection_sessions", "state = 'pairing'", "state = 'unpaired'",
		"octet_length(provider_device_fingerprint) <> 32", "legacy-reauth-", "md5(", "set revision = 1",
	} {
		if !strings.Contains(support, required) {
			t.Errorf("support/reconciliation migration lacks %q", required)
		}
	}
	if strings.Contains(support, "add constraint") || strings.Contains(support, "validate constraint") {
		t.Fatal("support/reconciliation migration validates constraints in the unsafe first phase")
	}
	beginIndex := strings.Index(support, "begin;")
	connectionsNoForce := strings.Index(support, "alter table connections no force row level security")
	sessionsNoForce := strings.Index(support, "alter table connection_sessions no force row level security")
	firstDML := strings.Index(support, "delete from connection_sessions")
	lastDML := strings.LastIndex(support, "update connection_sessions")
	connectionsForce := strings.LastIndex(support, "alter table connections force row level security")
	sessionsForce := strings.LastIndex(support, "alter table connection_sessions force row level security")
	commitIndex := strings.LastIndex(support, "commit;")
	if beginIndex < 0 || connectionsNoForce < beginIndex || sessionsNoForce < beginIndex ||
		firstDML < connectionsNoForce || firstDML < sessionsNoForce ||
		connectionsForce < lastDML || sessionsForce < lastDML ||
		commitIndex < connectionsForce || commitIndex < sessionsForce {
		t.Fatalf("RLS owner boundary order is unsafe: begin=%d no-force=(%d,%d) dml=(%d,%d) force=(%d,%d) commit=%d",
			beginIndex, connectionsNoForce, sessionsNoForce, firstDML, lastDML, connectionsForce, sessionsForce, commitIndex)
	}
	for _, required := range []string{
		"alter table connections enable row level security",
		"alter table connection_sessions enable row level security",
	} {
		if !strings.Contains(support, required) {
			t.Errorf("support migration does not keep RLS enabled: missing %q", required)
		}
	}

	constraintBytes, err := os.ReadFile(filepath.Join("migrations", wantNames[3]))
	if err != nil {
		t.Fatalf("read constraint migration: %v", err)
	}
	constraints := normalizeSQL(strings.ToLower(string(constraintBytes)))
	for _, required := range []string{
		"connections_fingerprint_matches_state", "connections_reauthorization_event_required",
		"connections_pairing_metadata_required", "connection_sessions_revision_positive",
		"pairing_attempt_id is not null", "pairing_prior_state in ('unpaired', 'reauthorization-required')",
		"not valid",
	} {
		if !strings.Contains(constraints, required) {
			t.Errorf("constraint phase lacks %q", required)
		}
	}
	if strings.Contains(constraints, "validate constraint") {
		t.Fatal("constraint phase validates in the same migration")
	}

	validationBytes, err := os.ReadFile(filepath.Join("migrations", wantNames[4]))
	if err != nil {
		t.Fatalf("read validation migration: %v", err)
	}
	validation := normalizeSQL(strings.ToLower(string(validationBytes)))
	for _, name := range []string{
		"connections_fingerprint_matches_state", "connections_reauthorization_event_required",
		"connections_pairing_metadata_required", "connection_sessions_revision_positive",
	} {
		if !strings.Contains(validation, "validate constraint "+name) {
			t.Errorf("validation phase does not validate %s", name)
		}
	}
	if !strings.Contains(validation, "alter column revision set not null") {
		t.Error("validation phase does not finalize revision NOT NULL")
	}
}

func TestLineMetadataMigrationPersistsOnlyAuthenticatedProviderFacts(t *testing.T) {
	entries, err := os.ReadDir("migrations")
	if err != nil {
		t.Fatal(err)
	}
	var contents []byte
	for _, entry := range entries {
		candidate, readErr := os.ReadFile(filepath.Join("migrations", entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(strings.ToLower(string(candidate)), "lines_discovery_source_check") {
			contents = candidate
			break
		}
	}
	if len(contents) == 0 {
		t.Fatal("line metadata migration not found")
	}
	sql := normalizeSQL(strings.ToLower(string(contents)))
	if strings.Contains(sql, "begin;") || strings.Contains(sql, "commit;") {
		t.Fatal("line metadata migration must use the production runner transaction")
	}
	for _, required := range []string{
		"alter table lines add column carrier_name", "add column color_hex", "add column rcs_enabled",
		"add column provider_sim_number", "add column provider_sim_payload_type", "add column discovery_source", "add column active boolean not null default true",
		"authenticated_google_settings", "lines_discovery_source_check",
		"drop constraint messages_direction_check", "direction in ('inbound', 'outbound', 'unknown')",
		"insert into gateway_events", "connection.reauthorization_required", "on conflict (tenant_id, event_id) do nothing",
		"insert into event_outbox", "on conflict (tenant_id, event_id, destination) do nothing",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("line metadata migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"supports_explicit_secondary_send", "is_default_sim", "imei", "imsi"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("line metadata migration invents unsupported provider fact %q", forbidden)
		}
	}
	firstDML := strings.Index(sql, "insert into gateway_events")
	lastDML := strings.LastIndex(sql, "insert into event_outbox")
	for _, table := range []string{"connections", "gateway_events", "event_outbox"} {
		noForce := strings.Index(sql, "alter table "+table+" no force row level security")
		force := strings.LastIndex(sql, "alter table "+table+" force row level security")
		if noForce < 0 || noForce > firstDML || force < lastDML {
			t.Fatalf("legacy reauthorization repair has unsafe %s RLS boundary: no-force=%d first-dml=%d last-dml=%d force=%d", table, noForce, firstDML, lastDML, force)
		}
	}
}

func TestRejectedProviderResponseMigrationStoresOnlyBoundedACKAudit(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0014_task7_rejected_provider_responses.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := normalizeSQL(strings.ToLower(string(contents)))
	for _, required := range []string{
		"create table provider_rejected_responses", "primary key (tenant_id, connection_id, provider_response_id)",
		"octet_length(envelope_digest) = 32", "sirenaix_valid_provider_response_id(provider_response_id)",
		"ack_pending", "conflicted", "occurrence_count", "provider_cursor_budget_exhausted",
		"enable row level security", "force row level security", "current_setting('sirenaix.tenant_id', true)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("rejected response migration missing %q", required)
		}
	}
	for _, forbidden := range []string{"raw_envelope", "conflicting_raw_envelope", "provider_response_body"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("rejected response audit contains unbounded payload column %q", forbidden)
		}
	}
}

func TestProviderResponseReservationMigrationIsAtomicTenantScopedAndBoundsConflicts(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0015_task7_provider_response_reservations.sql"))
	if err != nil {
		t.Fatal(err)
	}
	query := normalizeSQL(strings.ToLower(string(contents)))
	for _, required := range []string{
		"begin;", "commit;", "create table provider_response_reservations",
		"create table provider_response_overflow_audits",
		"primary key (tenant_id, connection_id, provider_response_id)",
		"envelope_digest bytea", "disposition text", "conflicted boolean",
		"provider_response_reservations_tenant_isolation",
		"provider_response_overflow_audits_tenant_isolation",
		"alter table provider_response_reservations force row level security",
		"alter table provider_response_overflow_audits force row level security",
		"provider_inbox_conflicts_one_per_response",
		"unique (tenant_id, connection_id, provider_response_id)",
		"add column occurrence_count", "set occurrence_count = least(conflict_occurrences.occurrences", "no force row level security",
		"conflicting_envelope_size bigint", "provider_inbox_conflicts_size_boundary",
		"provider_inbox_conflicts_sample_boundary", "substring(conflicting_raw_envelope from 1 for 256)",
		"from provider_inbox_conflicts", "set conflicted = true", "ack_pending = false",
		"provider_rejected_responses_reason_check", "response_id_digest_conflict", "media_identity_conflict",
		"or excluded.conflicted",
		"delete from provider_rejected_responses as rejected", "reservation.disposition = 'inbox'", "not reservation.conflicted",
		"create temp table task7_provider_response_overflow", "row_number() over",
		"sum(octet_length(raw_envelope)) over", "33554432", "row_ordinal > 256",
		"delete from provider_response_reservations", "set owner_id = null",
		"state = 'suspended'", "last_safe_reason = 'provider-protocol'",
		"force row level security",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("provider response reservation migration missing %q", required)
		}
	}
	if strings.Index(query, "begin;") > strings.Index(query, "no force row level security") ||
		strings.LastIndex(query, "force row level security") > strings.LastIndex(query, "commit;") {
		t.Fatal("reservation migration does not contain its complete RLS boundary in one transaction")
	}
}

func TestTenantOperationsMigrationIsAuditedTenantScopedAndNonDestructive(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0016_tenant_operations.sql"))
	if err != nil {
		t.Fatal(err)
	}
	query := normalizeSQL(strings.ToLower(string(contents)))
	for _, required := range []string{
		"add column status", "status in ('active', 'suspended')", "add column max_connections",
		"max_connections between 1 and 128", "tenant_suspend_prior_state", "drop constraint connections_fingerprint_matches_state",
		"connections_tenant_suspension_state", "tenant_suspend_prior_state is null or state = 'suspended'",
		"create table tenant_admin_events", "action in ('provision', 'update', 'suspend', 'resume')",
		"primary key (tenant_id, event_id)", "foreign key (tenant_id) references tenants (tenant_id)",
		"enable row level security", "force row level security", "current_setting('sirenaix.tenant_id', true)",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("tenant operations migration missing %q", required)
		}
	}
	if strings.Contains(query, "delete from tenants") || strings.Contains(query, "on delete cascade") {
		t.Fatal("tenant operations migration added a destructive tenant path")
	}
}

func TestProviderCursorHistoryMigrationIsTenantScopedFencedAndBounded(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0011_task7_cursor_history.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(contents))
	for _, required := range []string{
		"create table provider_cursor_history", "primary key (tenant_id, connection_id, cursor_scope, cursor_digest)",
		"foreign key (tenant_id, connection_id)", "octet_length(cursor_digest) = 32",
		"base_cursor_digest bytea", "octet_length(base_cursor_digest) = 32",
		"shared-infrastructure",
		"enable row level security", "force row level security", "current_setting('sirenaix.tenant_id', true)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("cursor history migration missing %q", required)
		}
	}
}

func TestProviderCursorBudgetMigrationIsTenantScopedAndMatchesApplicationCap(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0012_task7_cursor_budget.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := normalizeSQL(strings.ToLower(string(contents)))
	for _, required := range []string{
		"create table provider_cursor_budgets", "primary key (tenant_id, connection_id, cursor_scope)",
		"foreign key (tenant_id, connection_id)", "accepted_advances <= " + strconv.Itoa(ingress.MaxProviderCursorAdvances),
		"exhausted boolean not null default false",
		"octet_length(last_provider_response_id) between 1 and " + strconv.Itoa(domain.MaxProviderResponseIDBytes),
		"enable row level security", "force row level security", "current_setting('sirenaix.tenant_id', true)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("cursor budget migration missing %q", required)
		}
	}
}

func TestProviderResponseIDMigrationDefendsEveryDurableSQLEntryPoint(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0013_task7_provider_response_id_boundary.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := normalizeSQL(strings.ToLower(string(contents)))
	for _, required := range []string{
		"begin;",
		"create table provider_response_id_quarantine",
		"primary key (tenant_id, quarantine_id)",
		"alter table provider_response_id_quarantine enable row level security",
		"alter table provider_response_id_quarantine force row level security",
		"alter table provider_inbox no force row level security",
		"alter table provider_inbox_conflicts no force row level security",
		"alter table provider_cursor_history no force row level security",
		"alter table provider_cursor_budgets no force row level security",
		"insert into provider_response_id_quarantine",
		"set state = 'suspended'",
		"set owner_id = null",
		"delete from provider_inbox",
		"alter table provider_inbox add constraint provider_inbox_response_id_boundary",
		"alter table provider_inbox_conflicts add constraint provider_inbox_conflicts_response_id_boundary",
		"alter table provider_cursor_history add constraint provider_cursor_history_response_id_boundary",
		"alter table provider_cursor_budgets add constraint provider_cursor_budgets_response_id_boundary",
		"octet_length(candidate) not between 1 and " + strconv.Itoa(domain.MaxProviderResponseIDBytes),
		"candidate <> btrim(candidate)",
		"not valid",
		"validate constraint provider_inbox_response_id_boundary",
		"validate constraint provider_inbox_conflicts_response_id_boundary",
		"validate constraint provider_cursor_history_response_id_boundary",
		"validate constraint provider_cursor_budgets_response_id_boundary",
		"alter table provider_inbox force row level security",
		"alter table provider_inbox_conflicts force row level security",
		"alter table provider_cursor_history force row level security",
		"alter table provider_cursor_budgets force row level security",
		"commit;",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("provider response ID migration missing %q", required)
		}
	}
}

func TestDurableMessagingMigrationTenantScopesEveryDataFamily(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0007_durable_messaging.sql"))
	if err != nil {
		t.Fatalf("read durable messaging migration: %v", err)
	}
	sql := strings.ToLower(string(contents))
	tables := []string{
		"gateway_events", "provider_inbox", "provider_inbox_conflicts", "conversations", "messages",
		"message_status_history", "message_idempotency", "message_attempts", "message_lanes",
		"media_objects", "message_media", "media_fetch_jobs", "event_outbox", "webhook_endpoints", "webhook_deliveries",
		"webhook_attempts", "webhook_dlq", "kafka_commands", "kafka_event_deliveries", "kafka_command_dlq",
	}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			body := normalizeSQL(tableBody(t, sql, table))
			if !strings.Contains(body, "tenant_id") || !strings.Contains(body, "primary key (tenant_id,") {
				t.Errorf("%s lacks composite tenant primary key", table)
			}
			if !strings.Contains(body, "foreign key (tenant_id)") {
				t.Errorf("%s lacks tenant foreign key", table)
			}
			for _, clause := range []string{
				"alter table " + table + " enable row level security",
				"alter table " + table + " force row level security",
				"create policy " + table + "_tenant_isolation",
			} {
				if !strings.Contains(sql, clause) {
					t.Errorf("migration lacks %q", clause)
				}
			}
			policy := policyBody(t, sql, table)
			predicate := "tenant_id = nullif(current_setting('sirenaix.tenant_id', true), '')"
			if !strings.Contains(policy, "using ("+predicate+")") || !strings.Contains(policy, "with check ("+predicate+")") {
				t.Errorf("%s policy is not fail closed", table)
			}
		})
	}
	for _, required := range []string{
		"unique (tenant_id, connection_id, provider_response_id)",
		"unique (tenant_id, idempotency_key)",
		"for update skip locked",
		"request_digest bytea", "raw_envelope bytea", "ack_pending boolean", "canonical_body bytea",
		"secret_ciphertext bytea", "key_ciphertext bytea", "thumbnail_key_ciphertext bytea",
		"kafka_command_dlq", "webhook_dlq", "provider_inbox_conflicts",
		"clock_timestamp()", "fencing_token bigint", "lane_token bigint",
	} {
		if !strings.Contains(normalizeSQL(sql), required) {
			t.Errorf("durable messaging migration lacks %q", required)
		}
	}
	if !strings.Contains(sql, "raise exception 'immutable gateway event'") {
		t.Error("source events are not protected from update/delete")
	}
}

func TestTask7ReviewMigrationTemporarilyLiftsForceRLSForOwnerBackfill(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0008_task7_review_hardening.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := normalizeSQL(strings.ToLower(string(contents)))
	noForce := strings.Index(sql, "alter table conversations no force row level security")
	backfill := strings.Index(sql, "update conversations set ordering_key = conversation_id")
	restore := strings.Index(sql, "alter table conversations force row level security")
	if noForce < 0 || backfill <= noForce || restore <= backfill {
		t.Fatalf("0008 owner-safe RLS phase ordering is missing: %s", sql)
	}
	for _, required := range []string{"do $task7_ordering_backfill$", "exception when others", "raise", "execute 'alter table conversations force row level security'"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("0008 does not atomically restore FORCE after failure; missing %q: %s", required, sql)
		}
	}

}

func TestBackfillCheckpointMigrationMatchesApplicationPageLimit(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0010_task7_backfill_checkpoints.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := normalizeSQL(strings.ToLower(string(contents)))
	match := regexp.MustCompile(`cardinality\(conversation_ids\)\s*<=\s*([0-9]+)`).FindStringSubmatch(sql)
	if len(match) != 2 {
		t.Fatal("backfill migration has no executable conversation cap")
	}
	migrationLimit, err := strconv.Atoi(match[1])
	if err != nil || migrationLimit != messaging.MaxBackfillConversationsPerPage {
		t.Fatalf("backfill cap drift: migration=%d application=%d error=%v", migrationLimit, messaging.MaxBackfillConversationsPerPage, err)
	}
}

func TestTask7DeliveryAdmissionMigrationPersistsWebhookAndAttachmentFences(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0009_task7_delivery_admission.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := normalizeSQL(strings.ToLower(string(contents)))
	for _, required := range []string{
		"alter table webhook_endpoints add column generation bigint",
		"alter table webhook_deliveries add column endpoint_generation bigint",
		"add column http_started_at timestamptz",
		"alter table media_fetch_jobs add column attachment_identity_digest bytea",
		"alter table message_media add column provider_identity_digest bytea",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("0009 lacks durable fence %q: %s", required, sql)
		}
	}
}

func TestTask7BackfillCheckpointMigrationIsTenantScopedAndBounded(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("migrations", "0010_task7_backfill_checkpoints.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ToLower(string(contents))
	for _, required := range []string{
		"create table provider_backfill_checkpoints", "primary key (tenant_id, connection_id)",
		"foreign key (tenant_id, connection_id)", "cardinality(conversation_ids) <= 100",
		"enable row level security", "force row level security", "current_setting('sirenaix.tenant_id', true)",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("backfill migration missing %q", required)
		}
	}
}

func normalizeSQL(value string) string {
	return strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(value, " "))
}

func policyBody(t *testing.T, sql, table string) string {
	t.Helper()
	re := regexp.MustCompile(`(?s)create policy ` + regexp.QuoteMeta(table) + `_tenant_isolation\s+on\s+` + regexp.QuoteMeta(table) + `\s+(.*?);`)
	match := re.FindStringSubmatch(sql)
	if len(match) != 2 {
		t.Fatalf("migration does not create tenant policy for %s", table)
	}
	return normalizeSQL(match[1])
}

func hasIndexPrefix(sql, table string, columns ...string) bool {
	columnList := strings.Join(columns, `\s*,\s*`)
	re := regexp.MustCompile(`create\s+index\s+\S+\s+on\s+` + regexp.QuoteMeta(table) + `\s*\(\s*` + columnList + `(?:\s*,|\s*\))`)
	return re.MatchString(sql)
}

func tableBody(t *testing.T, sql, table string) string {
	t.Helper()
	re := regexp.MustCompile(`(?s)create table ` + regexp.QuoteMeta(table) + `\s*\((.*?)\);`)
	match := re.FindStringSubmatch(sql)
	if len(match) != 2 {
		t.Fatalf("migration does not create table %s", table)
	}
	return match[1]
}
