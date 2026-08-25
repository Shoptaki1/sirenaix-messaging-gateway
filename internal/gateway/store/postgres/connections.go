package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lib/pq"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/eventcontract"
	"go.mau.fi/mautrix-gmessages/internal/gateway/pairing"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

const DefaultMaxConnectionsPerTenant = 128
const MaxAuthenticatedLineSnapshot = 16

var (
	ErrConnectionQuotaExceeded = errors.New("tenant connection quota exceeded")
	ErrTenantSuspended         = errors.New("tenant is suspended")
)

type ConnectionPage struct {
	Records    []ConnectionRecord
	NextCursor domain.ConnectionID
}

const saveTenantSQL = `/* op:save_tenant */
INSERT INTO tenants (tenant_id, name)
VALUES ($1, $2)
ON CONFLICT (tenant_id) DO UPDATE
SET name = EXCLUDED.name, updated_at = now()`

func (repository *Repository) SaveTenant(ctx context.Context, tenant domain.Tenant) error {
	if tenant.ID == "" {
		return domain.ErrInvalidTenantID
	}
	return inTenantExec(ctx, repository, tenant.ID, func(tx transaction) error {
		if _, err := tx.ExecContext(ctx, saveTenantSQL, string(tenant.ID), tenant.Name); err != nil {
			return fmt.Errorf("save tenant: %w", err)
		}
		return nil
	})
}

const saveConnectionSQL = `/* op:save_connection */
INSERT INTO connections (
    tenant_id, connection_id, name, state, provider_device_fingerprint
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, connection_id) DO UPDATE
SET name = EXCLUDED.name,
    state = EXCLUDED.state,
    provider_device_fingerprint = EXCLUDED.provider_device_fingerprint,
    updated_at = now()`

const createUnpairedConnectionSQL = `/* op:create_unpaired_connection */
INSERT INTO connections (
    tenant_id, connection_id, name, state, provider_device_fingerprint
) VALUES ($1, $2, $3, $4, $5)`

const lockConnectionQuotaSQL = `/* op:lock_connection_quota */
SELECT tenant_id
FROM tenants
WHERE tenant_id = $1
FOR UPDATE`

const checkConnectionQuotaSQL = `/* op:check_connection_quota */
SELECT EXISTS (
           SELECT 1 FROM connections WHERE tenant_id = $1 AND connection_id = $2
       ), count(*),
       (SELECT max_connections FROM tenants WHERE tenant_id = $1),
       (SELECT status FROM tenants WHERE tenant_id = $1)
FROM connections
WHERE tenant_id = $1`

func (repository *Repository) SaveConnection(ctx context.Context, tenantID domain.TenantID, record ConnectionRecord) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if record.Connection.ID == "" {
		return domain.ErrInvalidIdentifier
	}
	if record.Connection.TenantID != tenantID {
		return domain.ErrTenantBoundary
	}
	if err := record.Connection.State.Validate(); err != nil {
		return err
	}
	if record.Connection.State == domain.ConnectionStatePairing || record.Connection.State == domain.ConnectionStateReauthorizationRequired {
		return pairing.ErrInvalidConnectionState
	}
	if !validFingerprintForState(record.Connection.State, record.ProviderDeviceFingerprint) {
		return ErrInvalidFingerprint
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		if _, err := tx.ExecContext(ctx, lockConnectionQuotaSQL, string(tenantID)); err != nil {
			return fmt.Errorf("lock connection quota: %w", err)
		}
		var exists bool
		var count, maxConnections int
		var tenantStatus string
		if err := tx.QueryRowContext(ctx, checkConnectionQuotaSQL, string(tenantID), string(record.Connection.ID)).Scan(&exists, &count, &maxConnections, &tenantStatus); err != nil {
			return fmt.Errorf("check connection quota: %w", err)
		}
		if tenantStatus == "suspended" {
			return ErrTenantSuspended
		}
		if tenantStatus != "active" {
			return errors.New("stored tenant status is invalid")
		}
		if maxConnections < 1 || maxConnections > DefaultMaxConnectionsPerTenant {
			return errors.New("stored tenant connection quota is invalid")
		}
		if !exists && count >= maxConnections {
			return ErrConnectionQuotaExceeded
		}
		var fingerprint any = record.ProviderDeviceFingerprint
		if len(record.ProviderDeviceFingerprint) == 0 {
			fingerprint = nil
		}
		query := saveConnectionSQL
		if record.Connection.State == domain.ConnectionStateUnpaired && fingerprint == nil {
			query = createUnpairedConnectionSQL
		}
		_, err := tx.ExecContext(ctx, query,
			string(tenantID), string(record.Connection.ID), record.Connection.Name,
			string(record.Connection.State), fingerprint,
		)
		if err != nil {
			return fmt.Errorf("save connection: %w", err)
		}
		return nil
	})
}

func validFingerprintForState(state domain.ConnectionState, fingerprint []byte) bool {
	switch state {
	case domain.ConnectionStateUnpaired:
		return len(fingerprint) == 0
	case domain.ConnectionStatePairing:
		return len(fingerprint) == 0 || len(fingerprint) == sha256.Size
	default:
		return len(fingerprint) == sha256.Size
	}
}

const getConnectionSQL = `/* op:get_connection */
SELECT connection_id, name, state
FROM connections
WHERE tenant_id = $1 AND connection_id = $2`

func (repository *Repository) GetConnection(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (domain.Connection, error) {
	if tenantID == "" {
		return domain.Connection{}, domain.ErrInvalidTenantID
	}
	if connectionID == "" {
		return domain.Connection{}, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (domain.Connection, error) {
		var id, name, state string
		err := tx.QueryRowContext(ctx, getConnectionSQL, string(tenantID), string(connectionID)).Scan(&id, &name, &state)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Connection{}, contactsync.ErrConnectionNotFound
		}
		if err != nil {
			return domain.Connection{}, fmt.Errorf("get connection: %w", err)
		}
		connection := domain.Connection{ID: domain.ConnectionID(id), TenantID: tenantID, Name: name, State: domain.ConnectionState(state)}
		if err := connection.State.Validate(); err != nil {
			return domain.Connection{}, fmt.Errorf("stored connection state: %w", err)
		}
		return connection, nil
	})
}

const listConnectionsSQL = `/* op:list_connections */
SELECT connection_id, name, state, provider_device_fingerprint
FROM connections
WHERE tenant_id = $1
ORDER BY connection_id
LIMIT $2`

func (repository *Repository) ListConnections(ctx context.Context, tenantID domain.TenantID) ([]ConnectionRecord, error) {
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]ConnectionRecord, error) {
		rows, err := tx.QueryContext(ctx, listConnectionsSQL, string(tenantID), DefaultMaxConnectionsPerTenant)
		if err != nil {
			return nil, fmt.Errorf("list connections: %w", err)
		}
		defer rows.Close()
		var records []ConnectionRecord
		for rows.Next() {
			var id, name, state string
			var fingerprint []byte
			if err := rows.Scan(&id, &name, &state, &fingerprint); err != nil {
				return nil, fmt.Errorf("scan connection: %w", err)
			}
			connectionState := domain.ConnectionState(state)
			if err := connectionState.Validate(); err != nil {
				return nil, fmt.Errorf("stored connection state: %w", err)
			}
			records = append(records, ConnectionRecord{
				Connection:                domain.Connection{ID: domain.ConnectionID(id), TenantID: tenantID, Name: name, State: connectionState},
				ProviderDeviceFingerprint: append([]byte(nil), fingerprint...),
			})
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate connections: %w", err)
		}
		return records, nil
	})
}

const listConnectionsPageSQL = `/* op:list_connections_page */
SELECT connection_id, name, state, provider_device_fingerprint
FROM connections
WHERE tenant_id = $1 AND connection_id > $2
ORDER BY connection_id
LIMIT $3`

func (repository *Repository) ListConnectionsPage(
	ctx context.Context, tenantID domain.TenantID, after domain.ConnectionID, limit int,
) (ConnectionPage, error) {
	if tenantID == "" || len(after) > 256 || limit < 1 || limit > 256 {
		return ConnectionPage{}, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (ConnectionPage, error) {
		rows, err := tx.QueryContext(ctx, listConnectionsPageSQL, string(tenantID), string(after), limit+1)
		if err != nil {
			return ConnectionPage{}, fmt.Errorf("list connection page: %w", err)
		}
		defer rows.Close()
		records := make([]ConnectionRecord, 0, limit+1)
		for rows.Next() {
			var id, name, state string
			var fingerprint []byte
			if err = rows.Scan(&id, &name, &state, &fingerprint); err != nil {
				return ConnectionPage{}, fmt.Errorf("scan connection page: %w", err)
			}
			connectionState := domain.ConnectionState(state)
			if id == "" || connectionState.Validate() != nil {
				return ConnectionPage{}, errors.New("invalid connection in page")
			}
			records = append(records, ConnectionRecord{
				Connection:                domain.Connection{ID: domain.ConnectionID(id), TenantID: tenantID, Name: name, State: connectionState},
				ProviderDeviceFingerprint: append([]byte(nil), fingerprint...),
			})
		}
		if err = rows.Err(); err != nil {
			return ConnectionPage{}, fmt.Errorf("iterate connection page: %w", err)
		}
		page := ConnectionPage{Records: records}
		if len(records) > limit {
			page.Records = records[:limit]
			page.NextCursor = page.Records[len(page.Records)-1].Connection.ID
		}
		return page, nil
	})
}

const upsertLineSQL = `/* op:upsert_line */
INSERT INTO lines (
    tenant_id, line_id, connection_id, provider_participant_id,
    provider_outgoing_id, normalized_phone, display_name,
    carrier_name, color_hex, rcs_enabled, provider_sim_number,
    provider_sim_payload_type, discovery_source, active
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, true)
ON CONFLICT (tenant_id, line_id) DO UPDATE
SET provider_participant_id = EXCLUDED.provider_participant_id,
    provider_outgoing_id = EXCLUDED.provider_outgoing_id,
    normalized_phone = EXCLUDED.normalized_phone,
    display_name = EXCLUDED.display_name,
    carrier_name = EXCLUDED.carrier_name,
    color_hex = EXCLUDED.color_hex,
    rcs_enabled = EXCLUDED.rcs_enabled,
    provider_sim_number = EXCLUDED.provider_sim_number,
    provider_sim_payload_type = EXCLUDED.provider_sim_payload_type,
    discovery_source = EXCLUDED.discovery_source,
    active = true,
    updated_at = clock_timestamp()
WHERE lines.connection_id = EXCLUDED.connection_id`

const retireLinesSQL = `/* op:retire_lines */
UPDATE lines
SET active = false, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND active = true
  AND NOT (line_id = ANY($3::text[]))`

const ensureConnectionSQL = `/* op:ensure_connection */
SELECT 1
FROM connections
WHERE tenant_id = $1 AND connection_id = $2
FOR UPDATE`

const ensureLineReplaceFenceSQL = `/* op:ensure_line_replace_fence */
WITH locked_connection AS MATERIALIZED (
    SELECT tenant_id, connection_id
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
),
owned_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS locked
      ON locked.tenant_id = lease.tenant_id AND locked.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $3 AND lease.fencing_token = $4
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
)
SELECT 1 FROM owned_lease`

func (repository *Repository) ReplaceLines(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, records []LineRecord) error {
	if err := validateLineReplacement(tenantID, connectionID, records); err != nil {
		return err
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		var owned int
		err := tx.QueryRowContext(ctx, ensureConnectionSQL, string(tenantID), string(connectionID)).Scan(&owned)
		if errors.Is(err, sql.ErrNoRows) {
			return contactsync.ErrConnectionNotFound
		}
		if err != nil {
			return fmt.Errorf("verify connection ownership: %w", err)
		}
		return replaceLineRows(ctx, tx, tenantID, connectionID, records)
	})
}

// ReplaceLinesFenced is the provider-runtime write path. Locking the
// connection before its lease prevents a retired actor generation from
// replacing a newer generation's authenticated Settings snapshot.
func (repository *Repository) ReplaceLinesFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken uint64, records []LineRecord) error {
	if ownerID == "" || fencingToken == 0 {
		return ErrInvalidLease
	}
	if err := validateLineReplacement(tenantID, connectionID, records); err != nil {
		return err
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		var owned int
		err := tx.QueryRowContext(ctx, ensureLineReplaceFenceSQL,
			string(tenantID), string(connectionID), ownerID, fencingToken,
		).Scan(&owned)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrConnectionLeaseLost
		}
		if err != nil {
			return fmt.Errorf("verify line replacement fence: %w", err)
		}
		return replaceLineRows(ctx, tx, tenantID, connectionID, records)
	})
}

func validateLineReplacement(tenantID domain.TenantID, connectionID domain.ConnectionID, records []LineRecord) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if connectionID == "" {
		return domain.ErrInvalidIdentifier
	}
	if len(records) > MaxAuthenticatedLineSnapshot {
		return domain.ErrInvalidIdentifier
	}
	connection := domain.Connection{ID: connectionID, TenantID: tenantID}
	for _, record := range records {
		if record.Line.ID == "" || record.Line.ProviderParticipantID == "" || record.Line.ProviderOutgoingID == "" || record.Phone.String() == "" ||
			!validLineMetadata(record.Line.DisplayName, 255) || !validLineMetadata(record.CarrierName, 255) || !validLineMetadata(record.ColorHex, 64) ||
			record.DiscoverySource != LineDiscoveryAuthenticatedGoogleSettings {
			return domain.ErrInvalidIdentifier
		}
		if err := record.Line.ValidateFor(connection); err != nil {
			return err
		}
	}
	return nil
}

func replaceLineRows(ctx context.Context, tx transaction, tenantID domain.TenantID, connectionID domain.ConnectionID, records []LineRecord) error {
	currentLineIDs := make([]string, 0, len(records))
	for _, record := range records {
		result, err := tx.ExecContext(ctx, upsertLineSQL,
			string(tenantID), string(record.Line.ID), string(connectionID),
			record.Line.ProviderParticipantID, record.Line.ProviderOutgoingID,
			record.Phone.String(), record.Line.DisplayName, record.CarrierName,
			record.ColorHex, record.RCSEnabled, record.ProviderSIMNumber,
			record.ProviderSIMPayloadType, string(record.DiscoverySource),
		)
		if err != nil {
			return fmt.Errorf("upsert line %q: %w", record.Line.ID, err)
		}
		updated, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("read line %q upsert count: %w", record.Line.ID, rowsErr)
		}
		if updated != 1 {
			return fmt.Errorf("line %q belongs to another connection", record.Line.ID)
		}
		currentLineIDs = append(currentLineIDs, string(record.Line.ID))
	}
	if _, err := tx.ExecContext(ctx, retireLinesSQL, string(tenantID), string(connectionID), pq.Array(currentLineIDs)); err != nil {
		return fmt.Errorf("retire absent lines: %w", err)
	}
	return nil
}

const listLinesSQL = `/* op:list_lines */
SELECT line_id, connection_id, provider_participant_id, provider_outgoing_id,
       normalized_phone, display_name, carrier_name, color_hex, rcs_enabled,
       provider_sim_number, provider_sim_payload_type, discovery_source
FROM lines
WHERE tenant_id = $1 AND connection_id = $2 AND active = true
ORDER BY line_id`

func (repository *Repository) ListLines(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) ([]LineRecord, error) {
	if connectionID == "" {
		return nil, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]LineRecord, error) {
		rows, err := tx.QueryContext(ctx, listLinesSQL, string(tenantID), string(connectionID))
		if err != nil {
			return nil, fmt.Errorf("list lines: %w", err)
		}
		defer rows.Close()
		var records []LineRecord
		for rows.Next() {
			var id, storedConnectionID, participantID, outgoingID, number, displayName string
			var record LineRecord
			if err := rows.Scan(&id, &storedConnectionID, &participantID, &outgoingID, &number, &displayName,
				&record.CarrierName, &record.ColorHex, &record.RCSEnabled, &record.ProviderSIMNumber,
				&record.ProviderSIMPayloadType, &record.DiscoverySource); err != nil {
				return nil, fmt.Errorf("scan line: %w", err)
			}
			phone, err := domain.ParseE164(number)
			if err != nil {
				return nil, fmt.Errorf("stored line phone: %w", err)
			}
			record.Line = domain.Line{
				ID: domain.LineID(id), TenantID: tenantID, ConnectionID: domain.ConnectionID(storedConnectionID),
				ProviderParticipantID: participantID, ProviderOutgoingID: outgoingID, DisplayName: displayName,
			}
			record.Phone = phone
			if record.DiscoverySource != LineDiscoveryLegacyUnknown && record.DiscoverySource != LineDiscoveryAuthenticatedGoogleSettings {
				return nil, errors.New("stored line discovery source is invalid")
			}
			records = append(records, record)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate lines: %w", err)
		}
		return records, nil
	})
}

func validLineMetadata(value string, limit int) bool {
	return len(value) <= limit && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

const saveEncryptedSessionSQL = `/* op:save_encrypted_session */
INSERT INTO connection_sessions (
    tenant_id, connection_id, envelope_version, provider, ciphertext, wrapped_dek, nonce, key_id, key_version
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, connection_id) DO UPDATE
SET envelope_version = EXCLUDED.envelope_version,
    provider = EXCLUDED.provider,
    ciphertext = EXCLUDED.ciphertext,
    wrapped_dek = EXCLUDED.wrapped_dek,
    nonce = EXCLUDED.nonce,
	key_id = EXCLUDED.key_id,
	key_version = EXCLUDED.key_version,
	revision = connection_sessions.revision + 1,
	updated_at = now()`

func (repository *Repository) SaveEncryptedSession(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, session EncryptedSession) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if connectionID == "" {
		return domain.ErrInvalidIdentifier
	}
	if session.Validate() != nil {
		return ErrInvalidEncryptedSession
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		_, err := tx.ExecContext(ctx, saveEncryptedSessionSQL, string(tenantID), string(connectionID),
			session.Version, session.Provider, session.Ciphertext, session.WrappedDEK, session.Nonce, session.KeyID, session.KeyVersion)
		if err != nil {
			return fmt.Errorf("save encrypted session: %w", err)
		}
		return nil
	})
}

const loadEncryptedSessionSQL = `/* op:load_encrypted_session */
SELECT revision, envelope_version, provider, ciphertext, wrapped_dek, nonce, key_id, key_version
FROM connection_sessions
WHERE tenant_id = $1 AND connection_id = $2`

func (repository *Repository) LoadEncryptedSession(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (EncryptedSession, error) {
	if connectionID == "" {
		return EncryptedSession{}, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (EncryptedSession, error) {
		var session EncryptedSession
		err := tx.QueryRowContext(ctx, loadEncryptedSessionSQL, string(tenantID), string(connectionID)).Scan(
			&session.Revision, &session.Version, &session.Provider, &session.Ciphertext, &session.WrappedDEK, &session.Nonce, &session.KeyID, &session.KeyVersion,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return EncryptedSession{}, ErrEncryptedSessionNotFound
		}
		if err != nil {
			return EncryptedSession{}, fmt.Errorf("load encrypted session: %w", err)
		}
		if session.Validate() != nil {
			return EncryptedSession{}, ErrInvalidEncryptedSession
		}
		return session, nil
	})
}

const compareAndSwapEncryptedSessionSQL = `/* op:cas_encrypted_session */
UPDATE connection_sessions
SET envelope_version = $4, provider = $5, ciphertext = $6, wrapped_dek = $7,
    nonce = $8, key_id = $9, key_version = $10, revision = revision + 1,
    updated_at = now()
WHERE tenant_id = $1 AND connection_id = $2 AND revision = $3`

func (repository *Repository) CompareAndSwapEncryptedSession(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, expectedRevision uint64, envelope session.Envelope) (bool, error) {
	if tenantID == "" || connectionID == "" || expectedRevision == 0 || envelope.Validate() != nil {
		return false, ErrInvalidEncryptedSession
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		result, err := tx.ExecContext(ctx, compareAndSwapEncryptedSessionSQL, string(tenantID), string(connectionID), expectedRevision,
			envelope.Version, envelope.Provider, envelope.Ciphertext, envelope.WrappedDEK, envelope.Nonce, envelope.KeyID, envelope.KeyVersion)
		if err != nil {
			return false, fmt.Errorf("rotate encrypted session: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("read encrypted session rotation result: %w", err)
		}
		return affected == 1, nil
	})
}

const transitionConnectionSQL = `/* op:transition_connection */
UPDATE connections
SET state = $3, updated_at = now()
WHERE tenant_id = $1 AND connection_id = $2 AND state = ANY($4)`

func (repository *Repository) TransitionConnection(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, from []domain.ConnectionState, to domain.ConnectionState) error {
	if tenantID == "" || connectionID == "" || len(from) == 0 || to.Validate() != nil ||
		to == domain.ConnectionStatePairing || to == domain.ConnectionStateReauthorizationRequired {
		return pairing.ErrInvalidConnectionState
	}
	allowed := make([]string, len(from))
	for index, state := range from {
		if state.Validate() != nil {
			return pairing.ErrInvalidConnectionState
		}
		allowed[index] = string(state)
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		result, err := tx.ExecContext(ctx, transitionConnectionSQL, string(tenantID), string(connectionID), string(to), pq.Array(allowed))
		if err != nil {
			return fmt.Errorf("transition connection state: %w", err)
		}
		if err := requireAffected(result, pairing.ErrInvalidConnectionState); err != nil {
			return err
		}
		return nil
	})
}

const beginPairingSQL = `/* op:begin_pairing */
UPDATE connections
SET state = 'pairing',
    pairing_prior_state = CASE WHEN state = 'pairing' THEN pairing_prior_state ELSE state END,
    pairing_attempt_id = $3, pairing_started_at = clock_timestamp(), updated_at = now()
WHERE tenant_id = $1 AND connection_id = $2
  AND (
    state IN ('unpaired', 'reauthorization-required')
    OR (state = 'pairing'
        AND pairing_started_at < clock_timestamp() - ($4 * interval '1 microsecond')
        AND pairing_prior_state IN ('unpaired', 'reauthorization-required'))
  )
RETURNING pairing_prior_state`

const lockActiveTenantSQL = `/* op:lock_active_tenant */
SELECT 1
FROM tenants
WHERE tenant_id = $1 AND status = 'active'
FOR SHARE`

const classifyPairingStartSQL = `/* op:classify_pairing_start */
SELECT state, pairing_started_at
FROM connections
WHERE tenant_id = $1 AND connection_id = $2
FOR UPDATE`

func (repository *Repository) BeginPairing(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, attemptID string, ttl time.Duration) (domain.ConnectionState, error) {
	if tenantID == "" || connectionID == "" || !pairing.ValidPairingID(attemptID) || !pairing.ValidAttemptTTL(ttl) {
		return "", pairing.ErrInvalidConnectionState
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (domain.ConnectionState, error) {
		var active int
		if err := tx.QueryRowContext(ctx, lockActiveTenantSQL, string(tenantID)).Scan(&active); errors.Is(err, sql.ErrNoRows) {
			return "", ErrTenantSuspended
		} else if err != nil {
			return "", fmt.Errorf("lock active tenant: %w", err)
		}
		var prior domain.ConnectionState
		err := tx.QueryRowContext(ctx, beginPairingSQL, string(tenantID), string(connectionID), attemptID, ttl.Microseconds()).Scan(&prior)
		if err == nil {
			return prior, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("begin pairing: %w", err)
		}
		var state domain.ConnectionState
		var currentStarted sql.NullTime
		if classifyErr := tx.QueryRowContext(ctx, classifyPairingStartSQL, string(tenantID), string(connectionID)).Scan(&state, &currentStarted); classifyErr != nil {
			if errors.Is(classifyErr, sql.ErrNoRows) {
				return "", pairing.ErrInvalidConnectionState
			}
			return "", fmt.Errorf("classify pairing start: %w", classifyErr)
		}
		if state == domain.ConnectionStatePairing {
			return "", pairing.ErrAttemptActive
		}
		return "", pairing.ErrInvalidConnectionState
	})
}

const restorePairingSQL = `/* op:restore_pairing */
UPDATE connections
SET state = pairing_prior_state, pairing_prior_state = NULL, pairing_started_at = NULL,
    pairing_attempt_id = NULL, updated_at = now()
WHERE tenant_id = $1 AND connection_id = $2 AND state = 'pairing'
  AND pairing_attempt_id = $3
  AND pairing_prior_state IN ('unpaired', 'reauthorization-required')
RETURNING state`

func (repository *Repository) RestorePairing(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, attemptID string) (domain.ConnectionState, error) {
	if tenantID == "" || connectionID == "" || !pairing.ValidPairingID(attemptID) {
		return "", pairing.ErrInvalidConnectionState
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (domain.ConnectionState, error) {
		var state domain.ConnectionState
		if err := tx.QueryRowContext(ctx, restorePairingSQL, string(tenantID), string(connectionID), attemptID).Scan(&state); errors.Is(err, sql.ErrNoRows) {
			return "", pairing.ErrAttemptSuperseded
		} else if err != nil {
			return "", fmt.Errorf("restore pairing: %w", err)
		}
		return state, nil
	})
}

const reconcileStalePairingsSQL = `/* op:reconcile_stale_pairings */
UPDATE connections
SET state = pairing_prior_state, pairing_prior_state = NULL, pairing_started_at = NULL,
    pairing_attempt_id = NULL, updated_at = now()
WHERE tenant_id = $1 AND state = 'pairing'
  AND pairing_started_at < clock_timestamp() - ($2 * interval '1 microsecond')
  AND pairing_prior_state IN ('unpaired', 'reauthorization-required')`

func (repository *Repository) ReconcileStalePairings(ctx context.Context, tenantID domain.TenantID, ttl time.Duration) (int, error) {
	if tenantID == "" || !pairing.ValidAttemptTTL(ttl) {
		return 0, pairing.ErrInvalidConnectionState
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (int, error) {
		result, err := tx.ExecContext(ctx, reconcileStalePairingsSQL, string(tenantID), ttl.Microseconds())
		if err != nil {
			return 0, fmt.Errorf("reconcile stale pairings: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read pairing reconciliation result: %w", err)
		}
		return int(affected), nil
	})
}

const commitEncryptedSessionSQL = `/* op:commit_encrypted_session */
INSERT INTO connection_sessions (
    tenant_id, connection_id, envelope_version, provider, ciphertext, wrapped_dek, nonce, key_id, key_version
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, connection_id) DO UPDATE
SET envelope_version = EXCLUDED.envelope_version,
    provider = EXCLUDED.provider,
    ciphertext = EXCLUDED.ciphertext,
    wrapped_dek = EXCLUDED.wrapped_dek,
    nonce = EXCLUDED.nonce,
    key_id = EXCLUDED.key_id,
    key_version = EXCLUDED.key_version,
	 revision = connection_sessions.revision + 1,
    updated_at = now()`

const commitConnectionStateSQL = `/* op:commit_connection_state */
UPDATE connections
SET state = 'connected', provider_device_fingerprint = $4,
	 reauthorization_event_id = NULL, pairing_prior_state = NULL, pairing_started_at = NULL,
	 pairing_attempt_id = NULL, updated_at = now()
WHERE tenant_id = $1 AND connection_id = $2 AND state = 'pairing'
  AND pairing_attempt_id = $3`

func (repository *Repository) CommitPairedSession(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, attemptID string, envelope session.Envelope, fingerprint []byte) error {
	if tenantID == "" || connectionID == "" || !pairing.ValidPairingID(attemptID) || envelope.Validate() != nil || len(fingerprint) != sha256.Size {
		return ErrInvalidEncryptedSession
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		if _, err := tx.ExecContext(ctx, commitEncryptedSessionSQL, string(tenantID), string(connectionID),
			envelope.Version, envelope.Provider, envelope.Ciphertext, envelope.WrappedDEK, envelope.Nonce, envelope.KeyID, envelope.KeyVersion); err != nil {
			return fmt.Errorf("commit encrypted session: %w", err)
		}
		result, err := tx.ExecContext(ctx, commitConnectionStateSQL, string(tenantID), string(connectionID), attemptID, fingerprint)
		if err != nil {
			return fmt.Errorf("commit connected state: %w", err)
		}
		return requireAffected(result, pairing.ErrAttemptSuperseded)
	})
}

const lockConnectionAuthStateSQL = `/* op:lock_connection_auth_state */
SELECT state, reauthorization_event_id
FROM connections
WHERE tenant_id = $1 AND connection_id = $2
FOR UPDATE`

const markReauthorizationRequiredSQL = `/* op:mark_reauthorization_required */
UPDATE connections
SET state = 'reauthorization-required', reauthorization_event_id = $3, updated_at = now()
WHERE tenant_id = $1 AND connection_id = $2`

const fenceConnectionLeaseForReauthorizationSQL = `/* op:fence_connection_lease_for_reauth */
UPDATE connection_leases
SET owner_id = NULL, fencing_token = fencing_token + 1,
    expires_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND owner_id IS NOT NULL`

func (repository *Repository) MarkReauthorizationRequired(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (pairing.AuthorizationTransition, error) {
	return repository.markReauthorizationRequired(ctx, tenantID, connectionID, nil)
}

func (repository *Repository) markReauthorizationRequired(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, afterConnectionLock func()) (pairing.AuthorizationTransition, error) {
	if tenantID == "" || connectionID == "" {
		return pairing.AuthorizationTransition{}, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (pairing.AuthorizationTransition, error) {
		var state string
		var eventID sql.NullString
		if err := tx.QueryRowContext(ctx, lockConnectionAuthStateSQL, string(tenantID), string(connectionID)).Scan(&state, &eventID); errors.Is(err, sql.ErrNoRows) {
			return pairing.AuthorizationTransition{}, contactsync.ErrConnectionNotFound
		} else if err != nil {
			return pairing.AuthorizationTransition{}, fmt.Errorf("lock connection auth state: %w", err)
		}
		if afterConnectionLock != nil {
			afterConnectionLock()
		}
		if state == string(domain.ConnectionStateReauthorizationRequired) && eventID.Valid && eventID.String != "" {
			if _, err := tx.ExecContext(ctx, fenceConnectionLeaseForReauthorizationSQL, string(tenantID), string(connectionID)); err != nil {
				return pairing.AuthorizationTransition{}, fmt.Errorf("fence connection actor for reauthorization: %w", err)
			}
			if err := insertReauthorizationEvent(ctx, tx, tenantID, connectionID, eventID.String); err != nil {
				return pairing.AuthorizationTransition{}, err
			}
			return pairing.AuthorizationTransition{EventID: eventID.String}, nil
		}
		switch domain.ConnectionState(state) {
		case domain.ConnectionStateConnected, domain.ConnectionStateDegraded, domain.ConnectionStateDisconnected,
			domain.ConnectionStateSuspended, domain.ConnectionStateReauthorizationRequired:
		default:
			return pairing.AuthorizationTransition{}, pairing.ErrInvalidConnectionState
		}
		newEventID := repository.newID()
		if newEventID == "" {
			return pairing.AuthorizationTransition{}, errors.New("generate authorization event ID")
		}
		if _, err := tx.ExecContext(ctx, markReauthorizationRequiredSQL, string(tenantID), string(connectionID), newEventID); err != nil {
			return pairing.AuthorizationTransition{}, fmt.Errorf("mark reauthorization required: %w", err)
		}
		if _, err := tx.ExecContext(ctx, fenceConnectionLeaseForReauthorizationSQL, string(tenantID), string(connectionID)); err != nil {
			return pairing.AuthorizationTransition{}, fmt.Errorf("fence connection actor for reauthorization: %w", err)
		}
		if err := insertReauthorizationEvent(ctx, tx, tenantID, connectionID, newEventID); err != nil {
			return pairing.AuthorizationTransition{}, err
		}
		return pairing.AuthorizationTransition{Transitioned: true, EventID: newEventID}, nil
	})
}

const ensureReauthorizationEventSQL = `/* op:ensure_reauthorization_event */
INSERT INTO gateway_events (
    tenant_id, event_id, event_type, aggregate_type, aggregate_id,
    connection_id, conversation_id, canonical_body
) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)
ON CONFLICT (tenant_id, event_id) DO NOTHING`

const validateReauthorizationEventSQL = `/* op:validate_reauthorization_event */
SELECT event_type, aggregate_type, aggregate_id, COALESCE(connection_id, '')
FROM gateway_events
WHERE tenant_id = $1 AND event_id = $2
FOR UPDATE`

const ensureReauthorizationOutboxSQL = `/* op:ensure_reauthorization_outbox */
INSERT INTO event_outbox (tenant_id, outbox_id, event_id, destination)
SELECT $1, $2 || ':' || destination, $3, destination
FROM unnest(ARRAY['webhook'::text, 'kafka'::text]) AS destination
ON CONFLICT (tenant_id, event_id, destination) DO NOTHING`

func insertReauthorizationEvent(ctx context.Context, tx transaction, tenantID domain.TenantID, connectionID domain.ConnectionID, eventID string) error {
	body, err := eventcontract.Marshal(eventcontract.Envelope{
		EventID: eventID, Type: "connection.reauthorization_required", OccurredAt: time.Now().UTC(),
		TenantID: string(tenantID), ConnectionID: string(connectionID),
		Status: string(domain.ConnectionStateReauthorizationRequired), State: string(domain.ConnectionStateReauthorizationRequired),
	})
	if err != nil {
		return fmt.Errorf("encode reauthorization event: %w", err)
	}
	if _, err = tx.ExecContext(ctx, ensureReauthorizationEventSQL,
		string(tenantID), eventID, "connection.reauthorization_required", "connection", string(connectionID),
		string(connectionID), "", body,
	); err != nil {
		return fmt.Errorf("ensure reauthorization event: %w", err)
	}
	var storedType, storedAggregateType, storedAggregateID, storedConnectionID string
	if err = tx.QueryRowContext(ctx, validateReauthorizationEventSQL, string(tenantID), eventID).Scan(
		&storedType, &storedAggregateType, &storedAggregateID, &storedConnectionID,
	); err != nil {
		return fmt.Errorf("validate reauthorization event: %w", err)
	}
	if storedType != "connection.reauthorization_required" || storedAggregateType != "connection" ||
		storedAggregateID != string(connectionID) || storedConnectionID != string(connectionID) {
		return errors.New("reauthorization event identity conflict")
	}
	if _, err = tx.ExecContext(ctx, ensureReauthorizationOutboxSQL, string(tenantID), eventID, eventID); err != nil {
		return fmt.Errorf("ensure reauthorization outbox: %w", err)
	}
	return nil
}

var _ pairing.Repository = (*Repository)(nil)
var _ session.EnvelopeStore = (*Repository)(nil)
