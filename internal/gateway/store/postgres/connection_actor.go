package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

const (
	minimumLeaseTTL = time.Second
	maximumLeaseTTL = 10 * time.Minute
)

var ErrInvalidLease = errors.New("invalid connection lease")

func validLeaseInput(tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, ttl time.Duration) bool {
	return tenantID != "" && connectionID != "" && len(ownerID) > 0 && len(ownerID) <= 256 && ttl >= minimumLeaseTTL && ttl <= maximumLeaseTTL
}

const acquireConnectionLeaseSQL = `/* op:acquire_connection_lease */
INSERT INTO connection_leases (tenant_id, connection_id, owner_id, fencing_token, expires_at)
VALUES ($1, $2, $3, 1, clock_timestamp() + ($4 * interval '1 microsecond'))
ON CONFLICT (tenant_id, connection_id) DO UPDATE
SET owner_id = EXCLUDED.owner_id,
    fencing_token = CASE
        WHEN connection_leases.owner_id = EXCLUDED.owner_id AND connection_leases.expires_at > clock_timestamp()
            THEN connection_leases.fencing_token
        ELSE connection_leases.fencing_token + 1
    END,
    expires_at = EXCLUDED.expires_at,
    updated_at = clock_timestamp()
WHERE connection_leases.owner_id = EXCLUDED.owner_id
   OR connection_leases.expires_at <= clock_timestamp()
RETURNING owner_id, fencing_token, expires_at`

func (repository *Repository) AcquireConnectionLease(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, ttl time.Duration) (ConnectionLease, bool, error) {
	if !validLeaseInput(tenantID, connectionID, ownerID, ttl) {
		return ConnectionLease{}, false, ErrInvalidLease
	}
	type result struct {
		lease    ConnectionLease
		acquired bool
	}
	acquired, err := inTenant(ctx, repository, tenantID, func(tx transaction) (result, error) {
		var lease ConnectionLease
		err := tx.QueryRowContext(ctx, acquireConnectionLeaseSQL, string(tenantID), string(connectionID), ownerID, ttl.Microseconds()).Scan(&lease.OwnerID, &lease.FencingToken, &lease.ExpiresAt)
		if errors.Is(err, sql.ErrNoRows) {
			return result{}, nil
		}
		if err != nil {
			return result{}, fmt.Errorf("acquire connection lease: %w", err)
		}
		return result{lease: lease, acquired: true}, nil
	})
	return acquired.lease, acquired.acquired, err
}

const renewConnectionLeaseSQL = `/* op:renew_connection_lease */
UPDATE connection_leases
SET expires_at = clock_timestamp() + ($5 * interval '1 microsecond'), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND owner_id = $3 AND fencing_token = $4
  AND expires_at > clock_timestamp()
RETURNING expires_at`

func (repository *Repository) RenewConnectionLease(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken uint64, ttl time.Duration) (bool, error) {
	if !validLeaseInput(tenantID, connectionID, ownerID, ttl) || fencingToken == 0 {
		return false, ErrInvalidLease
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		var expiresAt time.Time
		err := tx.QueryRowContext(ctx, renewConnectionLeaseSQL, string(tenantID), string(connectionID), ownerID, fencingToken, ttl.Microseconds()).Scan(&expiresAt)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("renew connection lease: %w", err)
		}
		return true, nil
	})
}

const releaseConnectionLeaseSQL = `/* op:release_connection_lease */
UPDATE connection_leases
SET owner_id = NULL, expires_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND owner_id = $3 AND fencing_token = $4
  AND expires_at > clock_timestamp()`

func (repository *Repository) ReleaseConnectionLease(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken uint64) (bool, error) {
	if tenantID == "" || connectionID == "" || ownerID == "" || fencingToken == 0 {
		return false, ErrInvalidLease
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		result, err := tx.ExecContext(ctx, releaseConnectionLeaseSQL, string(tenantID), string(connectionID), ownerID, fencingToken)
		if err != nil {
			return false, fmt.Errorf("release connection lease: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("read connection lease release: %w", err)
		}
		return count == 1, nil
	})
}

const writeConnectionHealthFencedSQL = `/* op:write_connection_health_fenced */
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
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2 AND lease.owner_id = $3
      AND lease.fencing_token = $4 AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
)
INSERT INTO connection_actor_health (
    tenant_id, connection_id, fencing_token, actor_state, connection_state,
    connected_at, last_frame_at, last_phone_response_at, reconnect_count,
    current_backoff_microseconds, last_safe_reason, requires_reauthorization, updated_at
)
SELECT $1, $2, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, clock_timestamp()
FROM owned_lease
ON CONFLICT (tenant_id, connection_id) DO UPDATE
SET fencing_token = EXCLUDED.fencing_token,
    actor_state = EXCLUDED.actor_state,
    connection_state = EXCLUDED.connection_state,
    connected_at = EXCLUDED.connected_at,
    last_frame_at = EXCLUDED.last_frame_at,
    last_phone_response_at = EXCLUDED.last_phone_response_at,
    reconnect_count = EXCLUDED.reconnect_count,
    current_backoff_microseconds = EXCLUDED.current_backoff_microseconds,
    last_safe_reason = EXCLUDED.last_safe_reason,
    requires_reauthorization = EXCLUDED.requires_reauthorization,
    updated_at = EXCLUDED.updated_at
WHERE connection_actor_health.fencing_token <= EXCLUDED.fencing_token`

func (repository *Repository) WriteConnectionHealthFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken uint64, health ConnectionActorHealth) (bool, error) {
	if tenantID == "" || connectionID == "" || ownerID == "" || fencingToken == 0 || !validActorHealth(health) {
		return false, ErrInvalidLease
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		result, err := tx.ExecContext(ctx, writeConnectionHealthFencedSQL, string(tenantID), string(connectionID), ownerID, fencingToken,
			health.ActorState, string(health.ConnectionState), health.ConnectedAt, health.LastFrameAt, health.LastPhoneResponseAt,
			health.ReconnectCount, health.CurrentBackoff.Microseconds(), health.LastSafeReason, health.RequiresReauthorization)
		if err != nil {
			return false, fmt.Errorf("write fenced connection health: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("read fenced connection health result: %w", err)
		}
		return count == 1, nil
	})
}

const getConnectionHealthSQL = `/* op:get_connection_health */
SELECT COALESCE(h.fencing_token, 0), COALESCE(h.actor_state, ''),
       CASE WHEN l.owner_id IS NOT NULL AND l.expires_at > clock_timestamp()
            THEN 'owned' ELSE 'inactive' END,
       c.state, h.connected_at, h.last_frame_at, h.last_phone_response_at,
       COALESCE(h.reconnect_count, 0), COALESCE(h.current_backoff_microseconds, 0),
       COALESCE(h.last_safe_reason, 'none'),
       c.state = 'reauthorization-required', COALESCE(h.updated_at, c.updated_at)
FROM connections AS c
LEFT JOIN connection_leases AS l
  ON l.tenant_id = c.tenant_id AND l.connection_id = c.connection_id
LEFT JOIN connection_actor_health AS h
  ON h.tenant_id = c.tenant_id AND h.connection_id = c.connection_id
 AND h.fencing_token = l.fencing_token
WHERE c.tenant_id = $1 AND c.connection_id = $2`

func (repository *Repository) GetConnectionHealth(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID) (ConnectionActorHealth, error) {
	if tenantID == "" || connectionID == "" {
		return ConnectionActorHealth{}, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (ConnectionActorHealth, error) {
		var health ConnectionActorHealth
		var backoffMicros int64
		err := tx.QueryRowContext(ctx, getConnectionHealthSQL, string(tenantID), string(connectionID)).Scan(
			&health.FencingToken, &health.ActorState, &health.LeaseState, &health.ConnectionState, &health.ConnectedAt, &health.LastFrameAt,
			&health.LastPhoneResponseAt, &health.ReconnectCount, &backoffMicros, &health.LastSafeReason,
			&health.RequiresReauthorization, &health.UpdatedAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return ConnectionActorHealth{}, sql.ErrNoRows
		}
		if err != nil {
			return ConnectionActorHealth{}, fmt.Errorf("get connection health: %w", err)
		}
		health.CurrentBackoff = time.Duration(backoffMicros) * time.Microsecond
		return health, nil
	})
}

const quarantineBackfillConnectionSQL = `/* op:quarantine_backfill_connection */
WITH locked_connection AS MATERIALIZED (
    SELECT tenant_id, connection_id
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
      AND state IN ('connected', 'degraded', 'disconnected', 'suspended')
    FOR UPDATE
),
locked_lease AS MATERIALIZED (
    SELECT lease.tenant_id, lease.connection_id
    FROM connection_leases AS lease
    JOIN locked_connection AS connection
      ON connection.tenant_id = lease.tenant_id AND connection.connection_id = lease.connection_id
    FOR UPDATE OF lease
),
fenced_lease AS (
    UPDATE connection_leases AS lease
    SET owner_id = NULL, fencing_token = lease.fencing_token + 1,
        expires_at = clock_timestamp(), updated_at = clock_timestamp()
    FROM locked_lease AS locked
    WHERE lease.tenant_id = locked.tenant_id AND lease.connection_id = locked.connection_id
    RETURNING lease.fencing_token
),
suspended_connection AS (
    UPDATE connections AS connection
    SET state = 'suspended', updated_at = clock_timestamp()
    FROM locked_connection AS locked
    WHERE connection.tenant_id = locked.tenant_id AND connection.connection_id = locked.connection_id
    RETURNING connection.tenant_id, connection.connection_id
),
updated_health AS (
    INSERT INTO connection_actor_health (
        tenant_id, connection_id, fencing_token, actor_state, connection_state,
        reconnect_count, current_backoff_microseconds, last_safe_reason,
        requires_reauthorization, updated_at
    )
    SELECT suspended.tenant_id, suspended.connection_id, lease.fencing_token,
           'stopped', 'suspended', 0, 0, $3, false, clock_timestamp()
    FROM suspended_connection AS suspended
    CROSS JOIN fenced_lease AS lease
    ON CONFLICT (tenant_id, connection_id) DO UPDATE
    SET fencing_token = EXCLUDED.fencing_token, actor_state = EXCLUDED.actor_state,
        connection_state = EXCLUDED.connection_state, last_safe_reason = EXCLUDED.last_safe_reason,
        current_backoff_microseconds = 0, requires_reauthorization = false,
        updated_at = EXCLUDED.updated_at
    RETURNING true
)
SELECT true FROM updated_health`

// QuarantineBackfillConnection is the durable connection-local circuit
// breaker for repeated provider/backfill poison. It stops the actor by fencing
// its lease and makes the operator-action boundary visible as suspended health.
func (repository *Repository) QuarantineBackfillConnection(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, safeReason string) error {
	if tenantID == "" || connectionID == "" || safeReason != "provider-protocol" {
		return domain.ErrInvalidIdentifier
	}
	_, err := inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		var quarantined bool
		if scanErr := tx.QueryRowContext(ctx, quarantineBackfillConnectionSQL,
			string(tenantID), string(connectionID), safeReason,
		).Scan(&quarantined); scanErr != nil {
			return false, fmt.Errorf("quarantine backfill connection: %w", scanErr)
		}
		if !quarantined {
			return false, errors.New("backfill connection quarantine was not persisted")
		}
		return true, nil
	})
	return err
}

const markReauthorizationRequiredFencedSQL = `/* op:mark_reauthorization_required_fenced */
WITH locked_connection AS MATERIALIZED (
    SELECT tenant_id, connection_id, state
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
      AND state IN ('connected', 'degraded', 'disconnected', 'suspended', 'reauthorization-required')
    FOR UPDATE
),
owned_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS locked
      ON locked.tenant_id = lease.tenant_id AND locked.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2 AND lease.owner_id = $3
      AND lease.fencing_token = $4 AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
)
UPDATE connections AS current_connection
SET state = 'reauthorization-required',
    reauthorization_event_id = COALESCE(NULLIF(current_connection.reauthorization_event_id, ''), $5),
    updated_at = clock_timestamp()
FROM owned_lease, locked_connection
WHERE current_connection.tenant_id = $1 AND current_connection.connection_id = $2
RETURNING current_connection.reauthorization_event_id,
          locked_connection.state <> 'reauthorization-required'`

func (repository *Repository) MarkReauthorizationRequiredFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken uint64) (bool, error) {
	if tenantID == "" || connectionID == "" || ownerID == "" || fencingToken == 0 {
		return false, ErrInvalidLease
	}
	eventID := repository.newID()
	if eventID == "" {
		return false, errors.New("generate authorization event ID")
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		var storedEventID string
		var newlyTransitioned bool
		err := tx.QueryRowContext(ctx, markReauthorizationRequiredFencedSQL, string(tenantID), string(connectionID), ownerID, fencingToken, eventID).Scan(&storedEventID, &newlyTransitioned)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("mark reauthorization required with fence: %w", err)
		}
		if storedEventID == "" {
			return false, errors.New("reauthorization transition returned an empty event ID")
		}
		if err = insertReauthorizationEvent(ctx, tx, tenantID, connectionID, storedEventID); err != nil {
			if !newlyTransitioned {
				return false, fmt.Errorf("repair legacy reauthorization event: %w", err)
			}
			return false, err
		}
		return true, nil
	})
}

func validActorHealth(health ConnectionActorHealth) bool {
	if health.ConnectionState.Validate() != nil || health.ReconnectCount > uint64(^uint64(0)>>1) || health.CurrentBackoff < 0 || health.CurrentBackoff > maximumLeaseTTL {
		return false
	}
	switch health.ActorState {
	case "acquiring", "connecting", "ready", "backoff", "stopped", "lease-lost":
	default:
		return false
	}
	switch health.LastSafeReason {
	case "none", "transient-network", "provider-auth", "provider-config", "provider-protocol", "shared-infrastructure", "lease-lost", "session-conflict", "shutdown":
		return true
	default:
		return false
	}
}

const compareAndSwapEncryptedSessionFencedSQL = `/* op:cas_encrypted_session_fenced */
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
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2 AND lease.owner_id = $3
      AND lease.fencing_token = $4 AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
)
UPDATE connection_sessions
SET envelope_version = $6, provider = $7, ciphertext = $8, wrapped_dek = $9,
    nonce = $10, key_id = $11, key_version = $12, revision = revision + 1,
    updated_at = clock_timestamp()
FROM owned_lease
WHERE tenant_id = $1 AND connection_id = $2 AND revision = $5`

func (repository *Repository) CompareAndSwapEncryptedSessionFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken, expectedRevision uint64, envelope session.Envelope) (bool, error) {
	if tenantID == "" || connectionID == "" || ownerID == "" || fencingToken == 0 || expectedRevision == 0 || envelope.Validate() != nil {
		return false, ErrInvalidEncryptedSession
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		result, err := tx.ExecContext(ctx, compareAndSwapEncryptedSessionFencedSQL, string(tenantID), string(connectionID), ownerID, fencingToken, expectedRevision,
			envelope.Version, envelope.Provider, envelope.Ciphertext, envelope.WrappedDEK, envelope.Nonce, envelope.KeyID, envelope.KeyVersion)
		if err != nil {
			return false, fmt.Errorf("write fenced encrypted session: %w", err)
		}
		count, err := result.RowsAffected()
		return count == 1, err
	})
}
