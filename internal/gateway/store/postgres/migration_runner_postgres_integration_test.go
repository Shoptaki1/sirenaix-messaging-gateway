//go:build postgres_integration

package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

func TestPostgresIntegrationMigrationRunnerFreshConcurrentAdoptionAndRollback(t *testing.T) {
	dsn := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	t.Run("fresh and idempotent", func(t *testing.T) {
		db := isolatedMigrationDatabase(t, dsn)
		runner, err := NewMigrationRunner(db, MigrationRunnerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		result, err := runner.Up(context.Background(), MigrationUpOptions{})
		if err != nil || result.Applied != len(runner.migrations) || !resultIsCurrent(t, runner) {
			t.Fatalf("fresh Up() = (%+v, %v)", result, err)
		}
		second, err := runner.Up(context.Background(), MigrationUpOptions{})
		if err != nil || second.Applied != 0 || second.Current != len(runner.migrations) {
			t.Fatalf("idempotent Up() = (%+v, %v)", second, err)
		}
	})

	t.Run("concurrent runners serialize", func(t *testing.T) {
		db := isolatedMigrationDatabase(t, dsn)
		secondDB, err := sql.Open("postgres", dbDSN(t, db))
		if err != nil {
			t.Fatal(err)
		}
		defer secondDB.Close()
		first, _ := NewMigrationRunner(db, MigrationRunnerConfig{LockTimeout: 2 * time.Minute})
		second, _ := NewMigrationRunner(secondDB, MigrationRunnerConfig{LockTimeout: 2 * time.Minute})
		start := make(chan struct{})
		results := make(chan MigrationResult, 2)
		errorsFound := make(chan error, 2)
		var wait sync.WaitGroup
		for _, runner := range []*MigrationRunner{first, second} {
			wait.Add(1)
			go func(runner *MigrationRunner) {
				defer wait.Done()
				<-start
				result, runErr := runner.Up(context.Background(), MigrationUpOptions{})
				results <- result
				errorsFound <- runErr
			}(runner)
		}
		close(start)
		wait.Wait()
		close(results)
		close(errorsFound)
		applied := 0
		for runErr := range errorsFound {
			if runErr != nil {
				t.Fatalf("concurrent Up() error = %v", runErr)
			}
		}
		for result := range results {
			applied += result.Applied
		}
		if applied != len(first.migrations) || !resultIsCurrent(t, first) {
			t.Fatalf("concurrent applied total = %d, want %d", applied, len(first.migrations))
		}
	})

	t.Run("advisory lock wait is bounded", func(t *testing.T) {
		db := isolatedMigrationDatabase(t, dsn)
		locker, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer locker.Close()
		var locked bool
		if err = locker.QueryRowContext(context.Background(), `SELECT pg_try_advisory_lock($1)`, migrationAdvisoryLockKey).Scan(&locked); err != nil || !locked {
			t.Fatalf("hold migration lock = %v, %v", locked, err)
		}
		defer func() {
			var unlocked bool
			_ = locker.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLockKey).Scan(&unlocked)
		}()

		runner, _ := NewMigrationRunner(db, MigrationRunnerConfig{LockTimeout: 150 * time.Millisecond})
		started := time.Now()
		if _, err = runner.Up(context.Background(), MigrationUpOptions{}); !errors.Is(err, ErrMigrationLock) {
			t.Fatalf("locked Up() error = %v, want ErrMigrationLock", err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("migration lock wait = %v, want bounded wait", elapsed)
		}
	})

	t.Run("explicit verified adoption", func(t *testing.T) {
		db := isolatedMigrationDatabase(t, dsn)
		migrations, err := LoadSchemaMigrations()
		if err != nil {
			t.Fatal(err)
		}
		for _, migration := range migrations {
			if _, err = db.ExecContext(context.Background(), string(migration.Contents)); err != nil {
				t.Fatalf("apply legacy migration %s: %v", migration.Name, err)
			}
		}
		runner, _ := NewMigrationRunner(db, MigrationRunnerConfig{})
		if _, err = runner.Up(context.Background(), MigrationUpOptions{}); !errors.Is(err, ErrUntrackedSchema) {
			t.Fatalf("unconfirmed adoption error = %v, want ErrUntrackedSchema", err)
		}
		result, err := runner.Up(context.Background(), MigrationUpOptions{AdoptExisting: true})
		if err != nil || !result.Adopted || result.Current != len(migrations) || !resultIsCurrent(t, runner) {
			t.Fatalf("adoption Up() = (%+v, %v)", result, err)
		}
	})

	for _, mutation := range []struct {
		name       string
		applyCount int
		sql        string
	}{
		{name: "schema only through tenant operations", applyCount: 16},
		{name: "partial line metadata column", applyCount: 16, sql: `ALTER TABLE lines ADD COLUMN carrier_name text NOT NULL DEFAULT ''`},
		{name: "wrong active default", sql: `ALTER TABLE lines ALTER COLUMN active SET DEFAULT false`},
		{name: "missing line boundary", sql: `ALTER TABLE lines DROP CONSTRAINT lines_color_hex_boundary`},
		{name: "unvalidated line boundary", sql: `ALTER TABLE lines DROP CONSTRAINT lines_color_hex_boundary; ALTER TABLE lines ADD CONSTRAINT lines_color_hex_boundary CHECK (octet_length(color_hex) <= 64) NOT VALID`},
		{name: "broadened discovery source", sql: `ALTER TABLE lines DROP CONSTRAINT lines_discovery_source_check; ALTER TABLE lines ADD CONSTRAINT lines_discovery_source_check CHECK (discovery_source IN ('legacy_unknown', 'authenticated_google_settings', 'forged'))`},
		{name: "direction omits unknown", sql: `ALTER TABLE messages DROP CONSTRAINT messages_direction_check; ALTER TABLE messages ADD CONSTRAINT messages_direction_check CHECK (direction IN ('inbound', 'outbound'))`},
		{name: "line metadata rls not forced", sql: `ALTER TABLE event_outbox NO FORCE ROW LEVEL SECURITY`},
	} {
		t.Run("line metadata adoption rejects "+mutation.name, func(t *testing.T) {
			db := isolatedMigrationDatabase(t, dsn)
			migrations, err := LoadSchemaMigrations()
			if err != nil {
				t.Fatal(err)
			}
			applyCount := mutation.applyCount
			if applyCount == 0 {
				applyCount = len(migrations)
			}
			for _, migration := range migrations[:applyCount] {
				if _, err = db.ExecContext(context.Background(), string(migration.Contents)); err != nil {
					t.Fatalf("apply legacy migration %s: %v", migration.Name, err)
				}
			}
			if mutation.sql != "" {
				if _, err = db.ExecContext(context.Background(), mutation.sql); err != nil {
					t.Fatalf("mutate line metadata schema: %v", err)
				}
			}
			runner, runnerErr := NewMigrationRunner(db, MigrationRunnerConfig{})
			if runnerErr != nil {
				t.Fatal(runnerErr)
			}
			if _, err = runner.Up(context.Background(), MigrationUpOptions{AdoptExisting: true}); !errors.Is(err, ErrUnsafeAdoption) {
				t.Fatalf("unsafe line metadata adoption error = %v, want ErrUnsafeAdoption", err)
			}
			var ledgerRows int
			if err = db.QueryRow(`SELECT count(*) FROM sirenaix_schema_migrations`).Scan(&ledgerRows); err != nil || ledgerRows != 0 {
				t.Fatalf("unsafe line metadata adoption ledger rows = %d, error %v", ledgerRows, err)
			}
		})
	}

	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{
			name: "permissive or true policy",
			sql: `DROP POLICY contacts_tenant_isolation ON contacts;
CREATE POLICY contacts_tenant_isolation ON contacts
USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), '') OR TRUE)
WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), '') OR TRUE)`,
		},
		{
			name: "missing with check",
			sql: `DROP POLICY contacts_tenant_isolation ON contacts;
CREATE POLICY contacts_tenant_isolation ON contacts
USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))`,
		},
		{
			name: "wrong tenant setting",
			sql: `DROP POLICY contacts_tenant_isolation ON contacts;
CREATE POLICY contacts_tenant_isolation ON contacts
USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))
WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), ''))`,
		},
		{
			name: "additional permissive policy",
			sql:  `CREATE POLICY contacts_cross_tenant ON contacts USING (TRUE) WITH CHECK (TRUE)`,
		},
	} {
		t.Run("adoption rejects "+mutation.name, func(t *testing.T) {
			db := isolatedMigrationDatabase(t, dsn)
			migrations, err := LoadSchemaMigrations()
			if err != nil {
				t.Fatal(err)
			}
			for _, migration := range migrations {
				if _, err = db.ExecContext(context.Background(), string(migration.Contents)); err != nil {
					t.Fatalf("apply legacy migration %s: %v", migration.Name, err)
				}
			}
			if _, err = db.ExecContext(context.Background(), mutation.sql); err != nil {
				t.Fatalf("mutate policy: %v", err)
			}
			runner, runnerErr := NewMigrationRunner(db, MigrationRunnerConfig{})
			if runnerErr != nil {
				t.Fatal(runnerErr)
			}
			if _, err = runner.Up(context.Background(), MigrationUpOptions{AdoptExisting: true}); !errors.Is(err, ErrUnsafeAdoption) {
				t.Fatalf("unsafe policy adoption error = %v, want ErrUnsafeAdoption", err)
			}
			var ledgerRows int
			if err = db.QueryRow(`SELECT count(*) FROM sirenaix_schema_migrations`).Scan(&ledgerRows); err != nil || ledgerRows != 0 {
				t.Fatalf("unsafe adoption ledger rows = %d, error %v", ledgerRows, err)
			}
		})
	}

	t.Run("partial legacy schema is never adopted", func(t *testing.T) {
		db := isolatedMigrationDatabase(t, dsn)
		migrations, err := LoadSchemaMigrations()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(context.Background(), string(migrations[0].Contents)); err != nil {
			t.Fatalf("apply partial legacy schema: %v", err)
		}
		runner, _ := NewMigrationRunner(db, MigrationRunnerConfig{})
		if _, err = runner.Up(context.Background(), MigrationUpOptions{AdoptExisting: true}); !errors.Is(err, ErrUnsafeAdoption) {
			t.Fatalf("partial adoption error = %v, want ErrUnsafeAdoption", err)
		}
		var ledgerRows int
		if err = db.QueryRow(`SELECT count(*) FROM sirenaix_schema_migrations`).Scan(&ledgerRows); err != nil || ledgerRows != 0 {
			t.Fatalf("partial adoption ledger rows = %d, error %v", ledgerRows, err)
		}
	})

	t.Run("failed migration and ledger row roll back together", func(t *testing.T) {
		db := isolatedMigrationDatabase(t, dsn)
		runner, _ := NewMigrationRunner(db, MigrationRunnerConfig{})
		if _, err := runner.Up(context.Background(), MigrationUpOptions{}); err != nil {
			t.Fatal(err)
		}
		failingRunner, _ := NewMigrationRunner(db, MigrationRunnerConfig{StatementTimeout: time.Second})
		contents := []byte("CREATE TABLE task8_failure_marker (id integer);\nSELECT pg_sleep(2);\n")
		failingRunner.migrations = append(failingRunner.migrations, SchemaMigration{
			Version: len(runner.migrations) + 1, Name: fmt.Sprintf("%04d_failure_fixture.sql", len(runner.migrations)+1),
			Contents: contents, SHA256: sha256.Sum256(contents), AdoptionProbe: `SELECT to_regclass('task8_failure_marker') IS NOT NULL`,
		})
		started := time.Now()
		if _, err := failingRunner.Up(context.Background(), MigrationUpOptions{}); err == nil {
			t.Fatal("failing migration unexpectedly succeeded")
		}
		if elapsed := time.Since(started); elapsed > 3*time.Second {
			t.Fatalf("statement timeout took %v, want bounded failure", elapsed)
		}
		var markerExists bool
		if err := db.QueryRow(`SELECT to_regclass('task8_failure_marker') IS NOT NULL`).Scan(&markerExists); err != nil || markerExists {
			t.Fatalf("failed migration marker = %v, error %v", markerExists, err)
		}
		var ledgerCount int
		if err := db.QueryRow(`SELECT count(*) FROM sirenaix_schema_migrations`).Scan(&ledgerCount); err != nil || ledgerCount != len(failingRunner.migrations)-1 {
			t.Fatalf("ledger count = %d, error %v", ledgerCount, err)
		}
	})

	for _, mutation := range []struct {
		name     string
		contents string
	}{
		{"abort", "CREATE TABLE task8_transaction_before (id integer); ABORT; CREATE TABLE task8_transaction_after (id integer);"},
		{"commit", "CREATE TABLE task8_transaction_before (id integer); COMMIT; CREATE TABLE task8_transaction_after (id integer);"},
	} {
		t.Run("transaction control cannot create false applied state "+mutation.name, func(t *testing.T) {
			db := isolatedMigrationDatabase(t, dsn)
			runner, err := NewMigrationRunner(db, MigrationRunnerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = runner.Up(context.Background(), MigrationUpOptions{}); err != nil {
				t.Fatal(err)
			}
			baseCount := len(runner.migrations)
			contents := []byte(mutation.contents)
			runner.migrations = append(runner.migrations, SchemaMigration{
				Version:       baseCount + 1,
				Name:          fmt.Sprintf("%04d_transaction_control_fixture.sql", baseCount+1),
				Contents:      contents,
				SHA256:        sha256.Sum256(contents),
				AdoptionProbe: `SELECT false`,
			})
			if _, err = runner.Up(context.Background(), MigrationUpOptions{}); !errors.Is(err, ErrMigrationCatalog) {
				t.Fatalf("unsafe migration error = %v, want ErrMigrationCatalog", err)
			}
			var beforeExists, afterExists bool
			if err = db.QueryRow(`SELECT
				to_regclass('task8_transaction_before') IS NOT NULL,
				to_regclass('task8_transaction_after') IS NOT NULL`).Scan(&beforeExists, &afterExists); err != nil || beforeExists || afterExists {
				t.Fatalf("unsafe migration objects = (%v, %v), error %v", beforeExists, afterExists, err)
			}
			var ledgerCount int
			if err = db.QueryRow(`SELECT count(*) FROM sirenaix_schema_migrations`).Scan(&ledgerCount); err != nil || ledgerCount != baseCount {
				t.Fatalf("unsafe migration ledger rows = %d, want %d, error %v", ledgerCount, baseCount, err)
			}
		})
	}
}

func TestPostgresIntegrationTenantAdministrationIsIdempotentAuditedAndFenced(t *testing.T) {
	dsn := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	db := isolatedMigrationDatabase(t, dsn)
	runner, _ := NewMigrationRunner(db, MigrationRunnerConfig{})
	if _, err := runner.Up(context.Background(), MigrationUpOptions{}); err != nil {
		t.Fatal(err)
	}
	admin, err := NewTenantAdministrator(db, "integration/operator")
	if err != nil {
		t.Fatal(err)
	}
	input := TenantAdminInput{TenantID: "tenant-admin", Name: "Admin tenant", MaxConnections: 2}
	first, err := admin.Provision(context.Background(), input)
	if err != nil || first.Status != "active" || first.MaxConnections != 2 {
		t.Fatalf("Provision() = (%+v, %v)", first, err)
	}
	if _, err = admin.Provision(context.Background(), input); err != nil {
		t.Fatalf("idempotent Provision() error = %v", err)
	}
	repository, _ := New(db)
	if err = repository.SaveConnection(context.Background(), "tenant-admin", ConnectionRecord{Connection: domain.Connection{
		ID: "unpaired", TenantID: "tenant-admin", Name: "New phone", State: domain.ConnectionStateUnpaired,
	}}); err != nil {
		t.Fatal(err)
	}
	if err = repository.SaveConnection(context.Background(), "tenant-admin", ConnectionRecord{Connection: domain.Connection{
		ID: "connected", TenantID: "tenant-admin", Name: "Active phone", State: domain.ConnectionStateConnected,
	}, ProviderDeviceFingerprint: make([]byte, sha256.Size)}); err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := repository.AcquireConnectionLease(context.Background(), "tenant-admin", "connected", "owner-before-suspend", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("AcquireConnectionLease() = (%+v, %v, %v)", lease, acquired, err)
	}
	suspended, err := admin.Suspend(context.Background(), "tenant-admin")
	if err != nil || suspended.Status != "suspended" || suspended.ConnectionCount != 2 {
		t.Fatalf("Suspend() = (%+v, %v)", suspended, err)
	}
	if _, err = admin.Suspend(context.Background(), "tenant-admin"); err != nil {
		t.Fatalf("idempotent Suspend() error = %v", err)
	}
	connected, err := repository.GetConnection(context.Background(), "tenant-admin", "connected")
	if err != nil || connected.State != domain.ConnectionStateSuspended {
		t.Fatalf("suspended connection = (%+v, %v)", connected, err)
	}
	unpaired, err := repository.GetConnection(context.Background(), "tenant-admin", "unpaired")
	if err != nil || unpaired.State != domain.ConnectionStateSuspended {
		t.Fatalf("unpaired connection was not closed by suspension = (%+v, %v)", unpaired, err)
	}
	if renewed, renewErr := repository.RenewConnectionLease(context.Background(), "tenant-admin", "connected", "owner-before-suspend", lease.FencingToken, time.Minute); renewErr != nil || renewed {
		t.Fatalf("suspended lease renewed = (%v, %v)", renewed, renewErr)
	}
	resumed, err := admin.Resume(context.Background(), "tenant-admin")
	if err != nil || resumed.Status != "active" {
		t.Fatalf("Resume() = (%+v, %v)", resumed, err)
	}
	connected, err = repository.GetConnection(context.Background(), "tenant-admin", "connected")
	if err != nil || connected.State != domain.ConnectionStateConnected {
		t.Fatalf("resumed connection = (%+v, %v)", connected, err)
	}
	unpaired, err = repository.GetConnection(context.Background(), "tenant-admin", "unpaired")
	if err != nil || unpaired.State != domain.ConnectionStateUnpaired {
		t.Fatalf("resumed unpaired connection = (%+v, %v)", unpaired, err)
	}
	if err = repository.SaveConnection(context.Background(), "tenant-admin", ConnectionRecord{Connection: domain.Connection{
		ID: "over-quota", TenantID: "tenant-admin", Name: "Third phone", State: domain.ConnectionStateUnpaired,
	}}); !errors.Is(err, ErrConnectionQuotaExceeded) {
		t.Fatalf("third connection error = %v, want ErrConnectionQuotaExceeded", err)
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(tenantContextSQL, "tenant-admin"); err != nil {
		t.Fatal(err)
	}
	var audits int
	if err = tx.QueryRow(`SELECT count(*) FROM tenant_admin_events WHERE tenant_id = 'tenant-admin'`).Scan(&audits); err != nil || audits != 3 {
		t.Fatalf("tenant audit count = %d, error %v", audits, err)
	}
}

func TestPostgresIntegrationOperationalKafkaDepthExcludesWebhookDestinations(t *testing.T) {
	dsn := os.Getenv("SIRENAIX_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SIRENAIX_POSTGRES_TEST_DSN is not set")
	}
	db := isolatedMigrationDatabase(t, dsn)
	runner, err := NewMigrationRunner(db, MigrationRunnerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.Up(context.Background(), MigrationUpOptions{}); err != nil {
		t.Fatal(err)
	}
	admin, err := NewTenantAdministrator(db, "integration/operator")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Provision(context.Background(), TenantAdminInput{TenantID: "queue-tenant", Name: "Queue tenant", MaxConnections: 1}); err != nil {
		t.Fatal(err)
	}
	repository, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err = inTenantExec(context.Background(), repository, "queue-tenant", func(tx transaction) error {
		for _, eventID := range []string{"queue-kafka", "queue-webhook"} {
			if _, insertErr := tx.ExecContext(context.Background(), `INSERT INTO gateway_events
                (tenant_id, event_id, event_type, aggregate_type, aggregate_id, canonical_body)
                VALUES ('queue-tenant', $1, 'fixture.queued', 'fixture', $1, '{}'::bytea)`, eventID); insertErr != nil {
				return insertErr
			}
		}
		_, insertErr := tx.ExecContext(context.Background(), `INSERT INTO event_outbox
            (tenant_id, outbox_id, event_id, destination) VALUES
            ('queue-tenant', 'queue-kafka', 'queue-kafka', 'kafka'),
            ('queue-tenant', 'queue-webhook', 'queue-webhook', 'webhook')`)
		return insertErr
	}); err != nil {
		t.Fatal(err)
	}
	depths, err := repository.OperationalQueueDepths(context.Background(), "queue-tenant")
	if err != nil || depths.Kafka != 1 {
		t.Fatalf("OperationalQueueDepths() = (%+v, %v), want Kafka=1", depths, err)
	}
}

func resultIsCurrent(t *testing.T, runner *MigrationRunner) bool {
	t.Helper()
	status, err := runner.Status(context.Background())
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	return status.IsCurrent()
}

var migrationDatabaseDSNs sync.Map

func isolatedMigrationDatabase(t *testing.T, baseDSN string) *sql.DB {
	t.Helper()
	admin, err := sql.Open("postgres", baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	schema := "sirenaix_migration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(`CREATE SCHEMA ` + pq.QuoteIdentifier(schema)); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	dsn := parsed.String()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		admin.Close()
		t.Fatal(err)
	}
	migrationDatabaseDSNs.Store(db, dsn)
	t.Cleanup(func() {
		migrationDatabaseDSNs.Delete(db)
		db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.ExecContext(ctx, `DROP SCHEMA `+pq.QuoteIdentifier(schema)+` CASCADE`)
		admin.Close()
	})
	return db
}

func dbDSN(t *testing.T, db *sql.DB) string {
	t.Helper()
	value, ok := migrationDatabaseDSNs.Load(db)
	if !ok {
		t.Fatal("migration database DSN missing")
	}
	return value.(string)
}
