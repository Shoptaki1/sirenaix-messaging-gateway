package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const migrationAdvisoryLockKey int64 = 0x534952454e414958 // "SIRENAIX"

var (
	ErrMigrationConfig    = errors.New("invalid migration runner configuration")
	ErrMigrationCatalog   = errors.New("invalid embedded migration catalog")
	ErrMigrationGap       = errors.New("migration ledger contains a gap")
	ErrMigrationDuplicate = errors.New("migration ledger contains a duplicate")
	ErrMigrationDrift     = errors.New("migration ledger checksum or name drift")
	ErrDatabaseAhead      = errors.New("database schema is ahead of this binary")
	ErrMigrationPending   = errors.New("database schema migrations are pending")
	ErrMigrationLock      = errors.New("migration advisory lock was not acquired")
	ErrUntrackedSchema    = errors.New("existing schema has no migration ledger")
	ErrUnsafeAdoption     = errors.New("existing schema could not be safely adopted")
)

type SchemaMigration struct {
	Version       int
	Name          string
	Contents      []byte
	SHA256        [sha256.Size]byte
	AdoptionProbe string
}

type MigrationLedgerRow struct {
	Version int
	Name    string
	SHA256  []byte
}

type MigrationStatus struct {
	Tracked bool
	Current int
	Latest  int
	Pending []SchemaMigration
}

func (status MigrationStatus) IsCurrent() bool {
	return status.Tracked && status.Current == status.Latest && len(status.Pending) == 0
}

type MigrationRunnerConfig struct {
	LockTimeout      time.Duration
	StatementTimeout time.Duration
}

func (config *MigrationRunnerConfig) setDefaultsAndValidate() error {
	if config.LockTimeout == 0 {
		config.LockTimeout = 30 * time.Second
	}
	if config.StatementTimeout == 0 {
		config.StatementTimeout = 2 * time.Minute
	}
	if config.LockTimeout < 100*time.Millisecond || config.LockTimeout > 5*time.Minute ||
		config.StatementTimeout < time.Second || config.StatementTimeout > 30*time.Minute {
		return ErrMigrationConfig
	}
	return nil
}

type MigrationRunner struct {
	db         *sql.DB
	config     MigrationRunnerConfig
	migrations []SchemaMigration
}

func NewMigrationRunner(db *sql.DB, config MigrationRunnerConfig) (*MigrationRunner, error) {
	if db == nil || config.setDefaultsAndValidate() != nil {
		return nil, ErrMigrationConfig
	}
	migrations, err := LoadSchemaMigrations()
	if err != nil {
		return nil, err
	}
	return &MigrationRunner{db: db, config: config, migrations: migrations}, nil
}

var migrationNamePattern = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.sql$`)

func LoadSchemaMigrations() ([]SchemaMigration, error) {
	entries, err := Migrations.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("%w: read catalog", ErrMigrationCatalog)
	}
	migrations := make([]SchemaMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf("%w: directory in catalog", ErrMigrationCatalog)
		}
		match := migrationNamePattern.FindStringSubmatch(entry.Name())
		if len(match) != 2 {
			return nil, fmt.Errorf("%w: invalid migration name", ErrMigrationCatalog)
		}
		version, parseErr := strconv.Atoi(match[1])
		if parseErr != nil {
			return nil, fmt.Errorf("%w: invalid migration version", ErrMigrationCatalog)
		}
		contents, readErr := Migrations.ReadFile("migrations/" + entry.Name())
		if readErr != nil || len(bytes.TrimSpace(contents)) == 0 {
			return nil, fmt.Errorf("%w: unreadable migration", ErrMigrationCatalog)
		}
		migrations = append(migrations, SchemaMigration{
			Version: version, Name: entry.Name(), Contents: contents,
			SHA256: sha256.Sum256(contents), AdoptionProbe: migrationAdoptionProbe(version),
		})
	}
	sort.Slice(migrations, func(left, right int) bool { return migrations[left].Version < migrations[right].Version })
	if err = validateSchemaMigrationCatalog(migrations); err != nil {
		return nil, err
	}
	return migrations, nil
}

func validateSchemaMigrationCatalog(migrations []SchemaMigration) error {
	if len(migrations) == 0 {
		return ErrMigrationCatalog
	}
	for index, migration := range migrations {
		wantVersion := index + 1
		if migration.Version != wantVersion || !strings.HasPrefix(migration.Name, fmt.Sprintf("%04d_", wantVersion)) ||
			len(bytes.TrimSpace(migration.Contents)) == 0 || migration.SHA256 != sha256.Sum256(migration.Contents) ||
			strings.TrimSpace(migration.AdoptionProbe) == "" {
			return ErrMigrationCatalog
		}
		if _, err := prepareMigrationSQL(migration.Contents); err != nil {
			return err
		}
	}
	return nil
}

func prepareMigrationSQL(contents []byte) (string, error) {
	statements, err := scanTopLevelSQLStatements(contents)
	if err != nil {
		return "", ErrMigrationCatalog
	}
	controls := make([]int, 0, 2)
	for index, statement := range statements {
		if isTransactionControl(statement.tokens) {
			controls = append(controls, index)
		}
	}
	if len(controls) == 0 {
		return string(contents), nil
	}
	// Immutable, already-shipped migrations may contain a plain outer
	// BEGIN/COMMIT envelope. Retain byte compatibility by removing only that
	// exact envelope before the runner supplies its own atomic transaction.
	// Every variant or nested transaction command remains a catalog error.
	if len(controls) != 2 || controls[0] != 0 || controls[1] != len(statements)-1 ||
		!hasExactTokens(statements[0].tokens, "BEGIN") ||
		!hasExactTokens(statements[len(statements)-1].tokens, "COMMIT") ||
		!isKnownShippedTransactionEnvelope(contents) {
		return "", ErrMigrationCatalog
	}
	preparedBytes := append([]byte(nil), contents...)
	for _, statement := range []sqlStatement{statements[0], statements[len(statements)-1]} {
		for index := statement.start; index < statement.end; index++ {
			if preparedBytes[index] != '\n' && preparedBytes[index] != '\r' {
				preparedBytes[index] = ' '
			}
		}
	}
	prepared := strings.TrimSpace(string(preparedBytes))
	if prepared == "" {
		return "", ErrMigrationCatalog
	}
	return prepared, nil
}

func isKnownShippedTransactionEnvelope(contents []byte) bool {
	// These are SHA-256 digests of the canonical LF form of the three shipped
	// migrations that predate the runner's outer transaction. Normalizing only
	// checkout line endings keeps the exception byte-specific across platforms.
	normalized := bytes.ReplaceAll(contents, []byte("\r\n"), []byte("\n"))
	digest := sha256.Sum256(normalized)
	switch fmt.Sprintf("%x", digest) {
	case "1960a89ba21748afd597940f6b0765c8e3f45f1399c8db1b7be9468dd7bae11d",
		"e3cc236c18be08c6f6feab871dcb87fb866d7370dc4c9a6da1d4a61485320f39",
		"22e84cbe118f37a08f1a04ffd66d31007290e794387c6e916b05b5ce13e48216":
		return true
	default:
		return false
	}
}

type sqlStatement struct {
	start  int
	end    int
	tokens []string
}

func scanTopLevelSQLStatements(contents []byte) ([]sqlStatement, error) {
	statements := make([]sqlStatement, 0, 8)
	statementStart := 0
	tokens := make([]string, 0, 4)
	significant := false
	blockCommentDepth := 0
	var dollarDelimiter []byte
	var quote byte
	escapeString := false

	finish := func(end int) {
		if significant {
			statements = append(statements, sqlStatement{start: statementStart, end: end, tokens: append([]string(nil), tokens...)})
		}
		statementStart, tokens, significant = end, tokens[:0], false
	}

	for index := 0; index < len(contents); {
		if len(dollarDelimiter) > 0 {
			if bytes.HasPrefix(contents[index:], dollarDelimiter) {
				index += len(dollarDelimiter)
				dollarDelimiter = nil
				continue
			}
			index++
			continue
		}
		if blockCommentDepth > 0 {
			switch {
			case index+1 < len(contents) && contents[index] == '/' && contents[index+1] == '*':
				blockCommentDepth++
				index += 2
			case index+1 < len(contents) && contents[index] == '*' && contents[index+1] == '/':
				blockCommentDepth--
				index += 2
			default:
				index++
			}
			continue
		}
		if quote != 0 {
			if quote == '\'' && escapeString && contents[index] == '\\' && index+1 < len(contents) {
				index += 2
				continue
			}
			if contents[index] == quote {
				if index+1 < len(contents) && contents[index+1] == quote {
					index += 2
					continue
				}
				quote = 0
				escapeString = false
			}
			index++
			continue
		}

		switch {
		case index+1 < len(contents) && contents[index] == '-' && contents[index+1] == '-':
			index += 2
			for index < len(contents) && contents[index] != '\n' {
				index++
			}
		case index+1 < len(contents) && contents[index] == '/' && contents[index+1] == '*':
			blockCommentDepth = 1
			index += 2
		case contents[index] == '\'' || contents[index] == '"':
			quote = contents[index]
			escapeString = quote == '\'' && index > 0 && (contents[index-1] == 'e' || contents[index-1] == 'E') &&
				(index == 1 || !isSQLIdentifierByte(contents[index-2]))
			significant = true
			index++
		case contents[index] == '$':
			if delimiter := sqlDollarDelimiter(contents[index:]); len(delimiter) > 0 {
				dollarDelimiter = delimiter
				significant = true
				index += len(delimiter)
			} else {
				significant = true
				index++
			}
		case contents[index] == ';':
			finish(index + 1)
			index++
		case isSQLWordStart(contents[index]):
			start := index
			for index < len(contents) && isSQLIdentifierByte(contents[index]) {
				index++
			}
			tokens = append(tokens, strings.ToUpper(string(contents[start:index])))
			significant = true
		default:
			if contents[index] != ' ' && contents[index] != '\t' && contents[index] != '\r' && contents[index] != '\n' {
				significant = true
			}
			index++
		}
	}
	if quote != 0 || blockCommentDepth != 0 || len(dollarDelimiter) != 0 {
		return nil, ErrMigrationCatalog
	}
	finish(len(contents))
	return statements, nil
}

func sqlDollarDelimiter(contents []byte) []byte {
	if len(contents) < 2 || contents[0] != '$' {
		return nil
	}
	for index := 1; index < len(contents); index++ {
		if contents[index] == '$' {
			if index == 1 || isSQLWordStart(contents[1]) {
				return append([]byte(nil), contents[:index+1]...)
			}
			return nil
		}
		if !isSQLIdentifierByte(contents[index]) {
			return nil
		}
	}
	return nil
}

func isSQLWordStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isSQLIdentifierByte(value byte) bool {
	return isSQLWordStart(value) || value >= '0' && value <= '9' || value == '$'
}

func isTransactionControl(tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}
	switch tokens[0] {
	case "ABORT", "BEGIN", "COMMIT", "END", "ROLLBACK":
		return true
	case "PREPARE", "START":
		return len(tokens) > 1 && tokens[1] == "TRANSACTION"
	default:
		return false
	}
}

func hasExactTokens(tokens []string, want ...string) bool {
	if len(tokens) != len(want) {
		return false
	}
	for index := range tokens {
		if tokens[index] != want[index] {
			return false
		}
	}
	return true
}

func validateMigrationLedger(migrations []SchemaMigration, rows []MigrationLedgerRow) (MigrationStatus, error) {
	status := MigrationStatus{Tracked: true, Latest: len(migrations)}
	seen := make(map[int]struct{}, len(rows))
	seenNames := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		if _, duplicate := seen[row.Version]; duplicate {
			return status, ErrMigrationDuplicate
		}
		seen[row.Version] = struct{}{}
		if _, duplicate := seenNames[row.Name]; duplicate {
			return status, ErrMigrationDuplicate
		}
		seenNames[row.Name] = struct{}{}
		if row.Version > len(migrations) {
			return status, ErrDatabaseAhead
		}
		if row.Version != index+1 || row.Version < 1 {
			return status, ErrMigrationGap
		}
		expected := migrations[row.Version-1]
		if row.Name != expected.Name || !bytes.Equal(row.SHA256, expected.SHA256[:]) {
			return status, ErrMigrationDrift
		}
		status.Current = row.Version
	}
	status.Pending = append([]SchemaMigration(nil), migrations[status.Current:]...)
	return status, nil
}

const (
	createMigrationLedgerSQL = `CREATE TABLE IF NOT EXISTS sirenaix_schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    name text NOT NULL UNIQUE CHECK (name ~ '^[0-9]{4}_[a-z0-9_]+[.]sql$'),
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`
	readMigrationLedgerSQL   = `SELECT version, name, sha256 FROM sirenaix_schema_migrations ORDER BY version`
	insertMigrationLedgerSQL = `INSERT INTO sirenaix_schema_migrations (version, name, sha256) VALUES ($1, $2, $3)`
)

func (runner *MigrationRunner) Status(ctx context.Context) (MigrationStatus, error) {
	if runner == nil || runner.db == nil || ctx == nil {
		return MigrationStatus{}, ErrMigrationConfig
	}
	statusCtx, cancel := context.WithTimeout(ctx, runner.config.LockTimeout)
	defer cancel()
	conn, err := runner.db.Conn(statusCtx)
	if err != nil {
		return MigrationStatus{}, errors.New("read schema status")
	}
	defer conn.Close()
	tracked, schemaExists, err := migrationSchemaPresence(statusCtx, conn)
	if err != nil {
		return MigrationStatus{}, err
	}
	if !tracked {
		status := MigrationStatus{Tracked: false, Latest: len(runner.migrations), Pending: append([]SchemaMigration(nil), runner.migrations...)}
		if schemaExists {
			return status, ErrUntrackedSchema
		}
		return status, nil
	}
	rows, err := readMigrationLedger(statusCtx, conn)
	if err != nil {
		return MigrationStatus{}, err
	}
	return validateMigrationLedger(runner.migrations, rows)
}

func (runner *MigrationRunner) CheckCurrent(ctx context.Context) error {
	status, err := runner.Status(ctx)
	if err != nil {
		return err
	}
	if !status.IsCurrent() {
		return ErrMigrationPending
	}
	return nil
}

type MigrationUpOptions struct {
	AdoptExisting bool
}

type MigrationResult struct {
	Previous int
	Current  int
	Applied  int
	Adopted  bool
}

func (runner *MigrationRunner) Up(ctx context.Context, options MigrationUpOptions) (MigrationResult, error) {
	if runner == nil || runner.db == nil || ctx == nil {
		return MigrationResult{}, ErrMigrationConfig
	}
	conn, err := runner.db.Conn(ctx)
	if err != nil {
		return MigrationResult{}, errors.New("open migration connection")
	}
	defer conn.Close()
	if err = runner.acquireLock(ctx, conn); err != nil {
		return MigrationResult{}, err
	}
	defer runner.releaseLock(conn)

	tracked, schemaExists, err := migrationSchemaPresence(ctx, conn)
	if err != nil {
		return MigrationResult{}, err
	}
	if !tracked {
		if err = runner.ensureMigrationLedger(ctx, conn); err != nil {
			return MigrationResult{}, err
		}
	}
	rows, err := readMigrationLedger(ctx, conn)
	if err != nil {
		return MigrationResult{}, err
	}
	status, err := validateMigrationLedger(runner.migrations, rows)
	if err != nil {
		return MigrationResult{}, err
	}
	result := MigrationResult{Previous: status.Current, Current: status.Current}
	if schemaExists && len(rows) == 0 {
		if !options.AdoptExisting {
			return result, ErrUntrackedSchema
		}
		if err = runner.adoptExisting(ctx, conn); err != nil {
			return result, err
		}
		result.Current, result.Adopted = len(runner.migrations), true
		return result, nil
	}
	for _, migration := range status.Pending {
		if err = runner.applyOne(ctx, conn, migration); err != nil {
			return result, err
		}
		result.Applied++
		result.Current = migration.Version
	}
	return result, nil
}

func (runner *MigrationRunner) ensureMigrationLedger(ctx context.Context, conn *sql.Conn) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin migration ledger transaction")
	}
	defer tx.Rollback()
	if err = configureMigrationTransaction(ctx, tx, runner.config); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, createMigrationLedgerSQL); err != nil {
		return errors.New("create migration ledger")
	}
	if err = tx.Commit(); err != nil {
		return errors.New("commit migration ledger")
	}
	return nil
}

func migrationSchemaPresence(ctx context.Context, conn *sql.Conn) (tracked, schemaExists bool, err error) {
	if err = conn.QueryRowContext(ctx, `SELECT
    to_regclass(format('%I.%I', current_schema(), 'sirenaix_schema_migrations')) IS NOT NULL,
    to_regclass(format('%I.%I', current_schema(), 'tenants')) IS NOT NULL`).Scan(&tracked, &schemaExists); err != nil {
		return false, false, errors.New("inspect schema presence")
	}
	return tracked, schemaExists, nil
}

func readMigrationLedger(ctx context.Context, conn *sql.Conn) ([]MigrationLedgerRow, error) {
	rows, err := conn.QueryContext(ctx, readMigrationLedgerSQL)
	if err != nil {
		return nil, errors.New("read migration ledger")
	}
	defer rows.Close()
	var ledger []MigrationLedgerRow
	for rows.Next() {
		var row MigrationLedgerRow
		if err = rows.Scan(&row.Version, &row.Name, &row.SHA256); err != nil {
			return nil, errors.New("scan migration ledger")
		}
		row.SHA256 = append([]byte(nil), row.SHA256...)
		ledger = append(ledger, row)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.New("iterate migration ledger")
	}
	return ledger, nil
}

func (runner *MigrationRunner) acquireLock(ctx context.Context, conn *sql.Conn) error {
	lockCtx, cancel := context.WithTimeout(ctx, runner.config.LockTimeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var acquired bool
		if err := conn.QueryRowContext(lockCtx, `SELECT pg_try_advisory_lock($1)`, migrationAdvisoryLockKey).Scan(&acquired); err != nil {
			if lockCtx.Err() != nil {
				return ErrMigrationLock
			}
			return errors.New("acquire migration advisory lock")
		}
		if acquired {
			return nil
		}
		select {
		case <-lockCtx.Done():
			return ErrMigrationLock
		case <-ticker.C:
		}
	}
}

func (runner *MigrationRunner) releaseLock(conn *sql.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var ignored bool
	_ = conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLockKey).Scan(&ignored)
}

func (runner *MigrationRunner) applyOne(ctx context.Context, conn *sql.Conn, migration SchemaMigration) error {
	prepared, err := prepareMigrationSQL(migration.Contents)
	if err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin migration transaction")
	}
	defer tx.Rollback()
	if err = configureMigrationTransaction(ctx, tx, runner.config); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, prepared); err != nil {
		return fmt.Errorf("apply migration %04d: %w", migration.Version, errors.New("migration statement failed"))
	}
	if _, err = tx.ExecContext(ctx, insertMigrationLedgerSQL, migration.Version, migration.Name, migration.SHA256[:]); err != nil {
		return errors.New("record migration ledger")
	}
	if err = tx.Commit(); err != nil {
		return errors.New("commit migration transaction")
	}
	return nil
}

func configureMigrationTransaction(ctx context.Context, tx *sql.Tx, config MigrationRunnerConfig) error {
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, strconv.FormatInt(config.LockTimeout.Milliseconds(), 10)+"ms"); err != nil {
		return errors.New("configure migration lock timeout")
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('statement_timeout', $1, true)`, strconv.FormatInt(config.StatementTimeout.Milliseconds(), 10)+"ms"); err != nil {
		return errors.New("configure migration statement timeout")
	}
	return nil
}

func (runner *MigrationRunner) adoptExisting(ctx context.Context, conn *sql.Conn) error {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.New("begin schema adoption")
	}
	defer tx.Rollback()
	if err = configureMigrationTransaction(ctx, tx, runner.config); err != nil {
		return err
	}
	for _, migration := range runner.migrations {
		var present bool
		if err = tx.QueryRowContext(ctx, migration.AdoptionProbe).Scan(&present); err != nil || !present {
			return fmt.Errorf("%w: migration %04d marker missing", ErrUnsafeAdoption, migration.Version)
		}
		if _, err = tx.ExecContext(ctx, insertMigrationLedgerSQL, migration.Version, migration.Name, migration.SHA256[:]); err != nil {
			return errors.New("record adopted migration")
		}
	}
	if err = tx.Commit(); err != nil {
		return errors.New("commit schema adoption")
	}
	return nil
}

var adoptionObjectNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

func tenantTablesAdoptionProbe(tableNames ...string) string {
	if len(tableNames) == 0 {
		return ""
	}
	literals := make([]string, 0, len(tableNames))
	for _, tableName := range tableNames {
		if !adoptionObjectNamePattern.MatchString(tableName) {
			return ""
		}
		literals = append(literals, "'"+tableName+"'")
	}
	return `SELECT NOT EXISTS (
    SELECT 1
    FROM unnest(ARRAY[` + strings.Join(literals, ", ") + `]::text[]) AS required(table_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_class AS relation
        JOIN pg_policy AS policy ON policy.polrelid = relation.oid
        WHERE relation.oid = to_regclass(format('%I.%I', current_schema(), required.table_name))
          AND relation.relkind IN ('r', 'p')
          AND relation.relrowsecurity
          AND relation.relforcerowsecurity
          AND policy.polname = required.table_name || '_tenant_isolation'
          AND policy.polcmd = '*'
          AND policy.polpermissive
          AND policy.polroles = ARRAY[0]::oid[]
          AND regexp_replace(pg_get_expr(policy.polqual, policy.polrelid), '[[:space:]]', '', 'g')
              IN ('(tenant_id=NULLIF(current_setting(''sirenaix.tenant_id''::text,true),''''::text))',
                  'tenant_id=NULLIF(current_setting(''sirenaix.tenant_id''::text,true),''''::text)')
          AND regexp_replace(pg_get_expr(policy.polwithcheck, policy.polrelid), '[[:space:]]', '', 'g')
              IN ('(tenant_id=NULLIF(current_setting(''sirenaix.tenant_id''::text,true),''''::text))',
                  'tenant_id=NULLIF(current_setting(''sirenaix.tenant_id''::text,true),''''::text)')
          AND NOT EXISTS (
              SELECT 1 FROM pg_policy AS extra
              WHERE extra.polrelid = relation.oid AND extra.oid <> policy.oid
          )
    )
)`
}

func columnsAdoptionProbe(columnNames ...string) string {
	if len(columnNames) == 0 {
		return ""
	}
	rows := make([]string, 0, len(columnNames))
	for _, qualifiedName := range columnNames {
		tableName, columnName, found := strings.Cut(qualifiedName, ".")
		if !found || !adoptionObjectNamePattern.MatchString(tableName) || !adoptionObjectNamePattern.MatchString(columnName) {
			return ""
		}
		rows = append(rows, "('"+tableName+"', '"+columnName+"')")
	}
	return `SELECT NOT EXISTS (
    SELECT 1
    FROM (VALUES ` + strings.Join(rows, ", ") + `) AS required(table_name, column_name)
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.columns AS present
        WHERE present.table_schema = current_schema()
          AND present.table_name = required.table_name
          AND present.column_name = required.column_name
    )
)`
}

func combineAdoptionProbes(probes ...string) string {
	parts := make([]string, 0, len(probes))
	for _, probe := range probes {
		if strings.TrimSpace(probe) == "" {
			return ""
		}
		parts = append(parts, "COALESCE(("+probe+"), false)")
	}
	if len(parts) == 0 {
		return ""
	}
	return "SELECT " + strings.Join(parts, " AND ")
}

// lineMetadataAdoptionProbe deliberately verifies the complete shape of the
// Task 8 line/event upgrade. A partial preview schema must fail closed instead
// of being recorded as an applied production migration.
func lineMetadataAdoptionProbe() string {
	return combineAdoptionProbes(
		`SELECT NOT EXISTS (
    SELECT 1
    FROM (VALUES
        ('carrier_name', 'text', 'NO', chr(39) || chr(39) || '::text'),
        ('color_hex', 'text', 'NO', chr(39) || chr(39) || '::text'),
        ('rcs_enabled', 'bool', 'NO', 'false'),
        ('provider_sim_number', 'int4', 'NO', '0'),
        ('provider_sim_payload_type', 'int4', 'NO', '0'),
        ('discovery_source', 'text', 'NO', chr(39) || 'legacy_unknown' || chr(39) || '::text'),
        ('active', 'bool', 'NO', 'true')
    ) AS required(column_name, udt_name, is_nullable, column_default)
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS present
        WHERE present.table_schema = current_schema()
          AND present.table_name = 'lines'
          AND present.column_name = required.column_name
          AND present.udt_name = required.udt_name
          AND present.is_nullable = required.is_nullable
          AND regexp_replace(COALESCE(present.column_default, ''), '[[:space:]()]', '', 'g') = required.column_default
    )
)`,
		`SELECT NOT EXISTS (
    SELECT 1
    FROM (VALUES
        ('lines_carrier_name_boundary', 'checkoctet_lengthcarrier_name<=255'),
        ('lines_color_hex_boundary', 'checkoctet_lengthcolor_hex<=64'),
        ('lines_discovery_source_check', 'checkdiscovery_source=anyarray[''legacy_unknown''::text,''authenticated_google_settings''::text]'),
        ('messages_direction_check', 'checkdirection=anyarray[''inbound''::text,''outbound''::text,''unknown''::text]')
    ) AS required(constraint_name, normalized_definition)
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS present
        WHERE present.conname = required.constraint_name
          AND present.conrelid = CASE
              WHEN required.constraint_name = 'messages_direction_check'
                  THEN to_regclass(format('%I.%I', current_schema(), 'messages'))
              ELSE to_regclass(format('%I.%I', current_schema(), 'lines'))
          END
          AND present.contype = 'c'
          AND present.convalidated
          AND NOT present.connoinherit
          AND lower(regexp_replace(pg_get_constraintdef(present.oid), '[[:space:]()]', '', 'g')) = required.normalized_definition
    )
)`,
		tenantTablesAdoptionProbe("lines", "messages", "connections", "gateway_events", "event_outbox"),
	)
}

var migrationAdoptionProbes = map[int]string{
	1: tenantTablesAdoptionProbe(
		"tenants", "connections", "lines", "connection_sessions", "contacts", "provider_contact_sources", "labels", "contact_labels", "contact_sync_runs",
	),
	2: combineAdoptionProbes(
		columnsAdoptionProbe("connections.provider_device_fingerprint", "connections.reauthorization_event_id", "connection_sessions.envelope_version", "connection_sessions.provider"),
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'connections' AND column_name = 'provider_device_fingerprint' AND is_nullable = 'YES') AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'connection_sessions' AND column_name = 'envelope_version' AND is_nullable = 'NO' AND column_default IS NULL) AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'connection_sessions' AND column_name = 'provider' AND is_nullable = 'NO' AND column_default IS NULL)`,
	),
	3: columnsAdoptionProbe("connections.pairing_prior_state", "connections.pairing_started_at", "connections.pairing_attempt_id", "connection_sessions.revision"),
	4: `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_fingerprint_matches_state' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections'))) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_reauthorization_event_required' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections'))) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_pairing_metadata_required' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections'))) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connection_sessions_revision_positive' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connection_sessions')))`,
	5: `SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_fingerprint_matches_state' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections')) AND convalidated) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_reauthorization_event_required' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections')) AND convalidated) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_pairing_metadata_required' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections')) AND convalidated) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connection_sessions_revision_positive' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connection_sessions')) AND convalidated) AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'connection_sessions' AND column_name = 'revision' AND is_nullable = 'NO')`,
	6: tenantTablesAdoptionProbe("connection_leases", "connection_actor_health"),
	7: tenantTablesAdoptionProbe(
		"conversations", "messages", "message_status_history", "message_idempotency", "message_lanes", "message_attempts", "gateway_events", "event_outbox",
		"provider_inbox", "provider_inbox_conflicts", "media_objects", "message_media", "media_fetch_jobs", "webhook_endpoints", "webhook_deliveries",
		"webhook_attempts", "webhook_dlq", "kafka_commands", "kafka_event_deliveries", "kafka_command_dlq",
	),
	8: columnsAdoptionProbe("conversations.ordering_key", "webhook_deliveries.claim_token"),
	9: columnsAdoptionProbe(
		"webhook_endpoints.generation", "webhook_deliveries.endpoint_generation", "webhook_deliveries.http_started_at",
		"media_fetch_jobs.attachment_identity_digest", "message_media.provider_identity_digest",
	),
	10: tenantTablesAdoptionProbe("provider_backfill_checkpoints"),
	11: combineAdoptionProbes(
		tenantTablesAdoptionProbe("provider_cursor_history"),
		`SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connection_actor_health_last_safe_reason_check' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connection_actor_health')) AND pg_get_constraintdef(oid) LIKE '%session-conflict%')`,
	),
	12: tenantTablesAdoptionProbe("provider_cursor_budgets"),
	13: combineAdoptionProbes(
		tenantTablesAdoptionProbe("provider_response_id_quarantine"),
		`SELECT to_regprocedure(format('%I.sirenaix_valid_provider_response_id(text)', current_schema())) IS NOT NULL AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'provider_inbox_response_id_boundary' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'provider_inbox')) AND convalidated) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'provider_inbox_conflicts_response_id_boundary' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'provider_inbox_conflicts')) AND convalidated) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'provider_cursor_history_response_id_boundary' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'provider_cursor_history')) AND convalidated) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'provider_cursor_budgets_response_id_boundary' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'provider_cursor_budgets')) AND convalidated)`,
	),
	14: tenantTablesAdoptionProbe("provider_rejected_responses"),
	15: combineAdoptionProbes(
		tenantTablesAdoptionProbe("provider_response_reservations", "provider_response_overflow_audits"),
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'provider_inbox_conflicts' AND column_name = 'conflicting_envelope_size' AND is_nullable = 'NO') AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'provider_inbox_conflicts' AND column_name = 'occurrence_count' AND is_nullable = 'NO') AND (SELECT count(*) = 3 FROM pg_constraint WHERE conrelid = to_regclass(format('%I.%I', current_schema(), 'provider_inbox_conflicts')) AND conname IN ('provider_inbox_conflicts_size_boundary', 'provider_inbox_conflicts_sample_boundary', 'provider_inbox_conflicts_one_per_response')) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass(format('%I.%I', current_schema(), 'provider_rejected_responses')) AND conname = 'provider_rejected_responses_reason_check' AND pg_get_constraintdef(oid) LIKE '%response_id_digest_conflict%')`,
	),
	16: combineAdoptionProbes(
		tenantTablesAdoptionProbe("tenant_admin_events"),
		columnsAdoptionProbe("tenants.status", "tenants.max_connections", "tenants.suspended_at", "connections.tenant_suspend_prior_state"),
		`SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tenants_suspension_timestamp' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'tenants')) AND convalidated) AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_fingerprint_matches_state' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections')) AND convalidated AND pg_get_constraintdef(oid) LIKE '%tenant_suspend_prior_state%') AND EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'connections_tenant_suspension_state' AND conrelid = to_regclass(format('%I.%I', current_schema(), 'connections')) AND convalidated)`,
	),
	17: lineMetadataAdoptionProbe(),
}

func migrationAdoptionProbe(version int) string {
	return migrationAdoptionProbes[version]
}
