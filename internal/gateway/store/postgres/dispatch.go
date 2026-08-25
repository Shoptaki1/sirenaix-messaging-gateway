package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
)

const listQueuedDispatchLanesSQL = `/* op:list_queued_dispatch_lanes */
SELECT message.connection_id, message.ordering_key
FROM messages AS message
WHERE message.tenant_id = $1 AND message.current_state = 'queued'
GROUP BY message.connection_id, message.ordering_key
ORDER BY min(message.created_at), message.connection_id, message.ordering_key
LIMIT $2`

func (repository *Repository) ListQueuedDispatchLanes(ctx context.Context, tenantID domain.TenantID, limit int) ([]messaging.LaneKey, error) {
	if tenantID == "" || limit < 1 || limit > 256 {
		return nil, messaging.ErrInvalidRoute
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]messaging.LaneKey, error) {
		rows, err := tx.QueryContext(ctx, listQueuedDispatchLanesSQL, string(tenantID), limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		lanes := make([]messaging.LaneKey, 0, limit)
		for rows.Next() {
			var connectionID, orderingKey string
			if err = rows.Scan(&connectionID, &orderingKey); err != nil {
				return nil, err
			}
			if connectionID == "" || orderingKey == "" {
				return nil, errors.New("invalid dispatch lane returned by database")
			}
			lanes = append(lanes, messaging.LaneKey{
				TenantID: tenantID, ConnectionID: domain.ConnectionID(connectionID), ConversationID: orderingKey,
			})
		}
		return lanes, rows.Err()
	})
}

const listQueuedDispatchLanesAfterSQL = `/* op:list_queued_dispatch_lanes_after */
SELECT message.connection_id, message.ordering_key
FROM messages AS message
WHERE message.tenant_id = $1 AND message.current_state = 'queued'
  AND ($2 = '' OR (message.connection_id, message.ordering_key) > ($2, $3))
GROUP BY message.connection_id, message.ordering_key
ORDER BY message.connection_id, message.ordering_key
LIMIT $4`

// ListQueuedDispatchLanesAfter is the production supervisor's rotating cursor.
// It prevents a stable prefix of disconnected/unready actors from starving all
// later tenant lanes while keeping each poll and query bounded.
func (repository *Repository) ListQueuedDispatchLanesAfter(ctx context.Context, tenantID domain.TenantID, after messaging.LaneKey, limit int) ([]messaging.LaneKey, error) {
	if tenantID == "" || limit < 1 || limit > 256 || (after.TenantID != "" && after.TenantID != tenantID) {
		return nil, messaging.ErrInvalidRoute
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]messaging.LaneKey, error) {
		rows, err := tx.QueryContext(ctx, listQueuedDispatchLanesAfterSQL, string(tenantID), string(after.ConnectionID), after.ConversationID, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		lanes := make([]messaging.LaneKey, 0, limit)
		for rows.Next() {
			var connectionID, orderingKey string
			if err = rows.Scan(&connectionID, &orderingKey); err != nil {
				return nil, err
			}
			if connectionID == "" || orderingKey == "" {
				return nil, errors.New("invalid dispatch lane returned by database")
			}
			lanes = append(lanes, messaging.LaneKey{TenantID: tenantID, ConnectionID: domain.ConnectionID(connectionID), ConversationID: orderingKey})
		}
		return lanes, rows.Err()
	})
}

const claimNextMessageSQL = `/* op:claim_next_message */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $4 AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), locked_lane AS MATERIALIZED (
    SELECT lane.ordering_key, lane.claimed_message_id, lane.lane_token
    FROM message_lanes AS lane
    JOIN locked_lease ON true
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
      AND (lane.owner_id IS NULL OR lane.claim_expires_at <= clock_timestamp())
    FOR UPDATE OF lane SKIP LOCKED
), locked_attempt AS MATERIALIZED (
    SELECT attempt.attempt_id, attempt.message_id, attempt.phase
    FROM message_attempts AS attempt
    JOIN locked_lane AS lane
      ON lane.claimed_message_id = attempt.message_id AND lane.lane_token = attempt.lane_token
    WHERE attempt.tenant_id = $1 AND attempt.connection_id = $2
      AND attempt.ordering_key = $3
      AND attempt.phase IN ('claimed', 'provider_io_started')
    ORDER BY attempt.created_at DESC, attempt.attempt_id DESC
    FOR UPDATE OF attempt
    LIMIT 1
), locked_message AS MATERIALIZED (
    SELECT message.message_id
    FROM messages AS message
    JOIN locked_attempt AS attempt ON attempt.message_id = message.message_id
    WHERE message.tenant_id = $1 AND message.current_state = 'dispatching'
    FOR UPDATE OF message
), recovered_message AS (
    UPDATE messages AS message
    SET current_state = CASE
            WHEN locked_attempt.phase = 'claimed' THEN 'queued'
            ELSE 'uncertain'
        END,
        updated_at = clock_timestamp()
    FROM locked_attempt, locked_message
    WHERE message.tenant_id = $1 AND message.message_id = locked_attempt.message_id
      AND message.message_id = locked_message.message_id
    RETURNING message.message_id, message.connection_id, message.conversation_id,
              message.current_state, locked_attempt.phase
), recovered_attempt AS (
    UPDATE message_attempts AS attempt
    SET phase = 'complete', completed_at = clock_timestamp(),
        safe_result = CASE
            WHEN recovered_message.phase = 'claimed' THEN 'crash_before_provider_io'
            ELSE 'provider_io_ambiguous'
        END
    FROM recovered_message
    WHERE attempt.tenant_id = $1
      AND attempt.attempt_id IN (SELECT attempt_id FROM locked_attempt)
    RETURNING attempt.attempt_id
), recovery_status AS (
    INSERT INTO message_status_history (
        tenant_id, status_id, message_id, state, provider_status, safe_reason
    )
    SELECT $1, $7, recovered_message.message_id, recovered_message.current_state, '',
           CASE WHEN recovered_message.phase = 'claimed'
                THEN 'crash_before_provider_io' ELSE 'provider_io_ambiguous' END
    FROM recovered_message
    RETURNING message_id
), recovery_event AS (
    INSERT INTO gateway_events (
        tenant_id, event_id, event_type, aggregate_type, aggregate_id,
        connection_id, conversation_id, canonical_body
    )
    SELECT $1, $8::text, 'message.uncertain', 'message', recovered_message.message_id,
           recovered_message.connection_id, recovered_message.conversation_id,
           convert_to(jsonb_build_object(
               'event_id', $8::text, 'type', 'message.uncertain',
	           'version', 1,
	           'occurred_at', to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
	           'tenant_id', $1,
	           'connection_id', recovered_message.connection_id,
	           'conversation_id', recovered_message.conversation_id,
	           'message_id', recovered_message.message_id,
	           'direction', 'outbound',
	           'status', 'uncertain', 'state', 'uncertain'
           )::text, 'UTF8')
    FROM recovered_message
    CROSS JOIN (SELECT count(*) FROM recovered_attempt) AS attempt_barrier
    CROSS JOIN (SELECT count(*) FROM recovery_status) AS status_barrier
    WHERE recovered_message.current_state = 'uncertain'
    RETURNING event_id
), recovery_outbox AS (
    INSERT INTO event_outbox (tenant_id, outbox_id, event_id, destination)
    SELECT $1, destination.outbox_id, recovery_event.event_id, destination.kind
    FROM recovery_event
    CROSS JOIN (VALUES ($9::text, 'webhook'), ($10::text, 'kafka')) AS destination(outbox_id, kind)
    RETURNING outbox_id
), released_stale_lane AS (
    UPDATE message_lanes AS lane
    SET owner_id = NULL, claimed_message_id = NULL, claim_expires_at = NULL,
        updated_at = clock_timestamp()
    FROM locked_lane
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
      AND locked_lane.claimed_message_id IS NOT NULL
    RETURNING lane.ordering_key
), candidate AS MATERIALIZED (
    SELECT message.message_id
    FROM messages AS message
    JOIN locked_lane AS lane ON lane.ordering_key = message.ordering_key
    CROSS JOIN (SELECT count(*) FROM recovered_attempt) AS recovery_barrier
    CROSS JOIN (SELECT count(*) FROM recovery_outbox) AS outbox_barrier
    WHERE message.tenant_id = $1 AND message.connection_id = $2
      AND message.current_state = 'queued'
      AND NOT EXISTS (SELECT 1 FROM locked_attempt)
    ORDER BY message.created_at, message.message_id
    FOR UPDATE OF message SKIP LOCKED
    LIMIT 1
), claimed_lane AS (
    UPDATE message_lanes AS lane
    SET lane_token = lane.lane_token + 1, owner_id = $4,
        claimed_message_id = candidate.message_id,
        claim_expires_at = clock_timestamp() + interval '30 seconds',
        updated_at = clock_timestamp()
    FROM candidate
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
    RETURNING lane.lane_token, candidate.message_id
), updated_message AS (
    UPDATE messages AS message
    SET current_state = 'dispatching', updated_at = clock_timestamp()
    FROM claimed_lane
    WHERE message.tenant_id = $1 AND message.message_id = claimed_lane.message_id
    RETURNING message.message_id, message.connection_id, message.conversation_id,
              message.recipient, COALESCE(message.line_id, '') AS line_id,
              message.route_mode, message.body_text, message.provider_tmp_id,
              message.current_state, message.created_at
), inserted_attempt AS (
    INSERT INTO message_attempts (
        tenant_id, attempt_id, message_id, connection_id, ordering_key,
        owner_id, lane_token, fencing_token, phase
    )
    SELECT $1, $5, updated_message.message_id, $2, $3, $4,
           claimed_lane.lane_token, locked_lease.fencing_token, 'claimed'
    FROM updated_message, claimed_lane, locked_lease
    RETURNING attempt_id, lane_token, fencing_token
), inserted_status AS (
    INSERT INTO message_status_history (tenant_id, status_id, message_id, state, provider_status, safe_reason)
    SELECT $1, $6, updated_message.message_id, 'dispatching', '', ''
    FROM updated_message
    RETURNING status_id
)
SELECT updated_message.message_id, updated_message.connection_id, updated_message.conversation_id,
       updated_message.recipient, updated_message.line_id, updated_message.route_mode,
       updated_message.body_text, updated_message.provider_tmp_id, updated_message.current_state,
       updated_message.created_at,
       COALESCE(ARRAY(
           SELECT attachment.media_id
           FROM message_media AS attachment
           WHERE attachment.tenant_id = $1 AND attachment.message_id = updated_message.message_id
           ORDER BY attachment.position
       ), ARRAY[]::text[]) AS media_ids,
       inserted_attempt.attempt_id,
       inserted_attempt.lane_token, inserted_attempt.fencing_token
FROM updated_message, inserted_attempt, inserted_status`

func (repository *Repository) ClaimNext(ctx context.Context, lane messaging.LaneKey, ownerID string) (messaging.DispatchClaim, bool, error) {
	if lane.TenantID == "" || lane.ConnectionID == "" || lane.ConversationID == "" || ownerID == "" || len(ownerID) > 256 {
		return messaging.DispatchClaim{}, false, messaging.ErrInvalidRoute
	}
	type claimResult struct {
		claim messaging.DispatchClaim
		ok    bool
	}
	result, err := inTenant(ctx, repository, lane.TenantID, func(tx transaction) (claimResult, error) {
		attemptID, statusID := repository.newID(), repository.newID()
		recoveryStatusID, recoveryEventID := repository.newID(), repository.newID()
		recoveryWebhookOutboxID, recoveryKafkaOutboxID := repository.newID(), repository.newID()
		var claim messaging.DispatchClaim
		var messageID, connectionID, lineID, state string
		var mediaIDs pq.StringArray
		err := tx.QueryRowContext(ctx, claimNextMessageSQL,
			string(lane.TenantID), string(lane.ConnectionID), lane.ConversationID, ownerID, attemptID, statusID,
			recoveryStatusID, recoveryEventID, recoveryWebhookOutboxID, recoveryKafkaOutboxID,
		).Scan(
			&messageID, &connectionID, &claim.Message.ConversationID, &claim.Message.Recipient,
			&lineID, &claim.Message.RouteMode, &claim.Message.Text, &claim.Message.ProviderTmpID,
			&state, &claim.Message.CreatedAt, &mediaIDs, &claim.AttemptID, &claim.LaneToken, &claim.FencingToken,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return claimResult{}, nil
		}
		if err != nil {
			return claimResult{}, fmt.Errorf("claim next message: %w", err)
		}
		claim.Message.ID = domain.MessageID(messageID)
		claim.Message.TenantID = lane.TenantID
		claim.Message.ConnectionID = domain.ConnectionID(connectionID)
		claim.Message.LineID = domain.LineID(lineID)
		claim.Message.State = domain.MessageState(state)
		claim.Message.MediaIDs = toMediaIDs(mediaIDs)
		claim.OrderingKey = lane.ConversationID
		claim.OwnerID = ownerID
		if claim.Message.State.Validate() != nil || claim.AttemptID == "" || claim.LaneToken == 0 || claim.FencingToken == 0 {
			return claimResult{}, errors.New("invalid dispatch claim returned by database")
		}
		return claimResult{claim: claim, ok: true}, nil
	})
	return result.claim, result.ok, err
}

const beginProviderIOSQL = `/* op:begin_provider_io */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $4 AND lease.fencing_token = $5
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), locked_lane AS MATERIALIZED (
    SELECT lane.lane_token
    FROM message_lanes AS lane
    JOIN locked_lease ON true
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
      AND lane.owner_id = $4 AND lane.lane_token = $6 AND lane.claimed_message_id = $7
      AND lane.claim_expires_at > clock_timestamp()
    FOR UPDATE OF lane
), locked_attempt AS MATERIALIZED (
    SELECT attempt.attempt_id
    FROM message_attempts AS attempt
    JOIN locked_lane ON true
    WHERE attempt.tenant_id = $1 AND attempt.attempt_id = $8
      AND attempt.message_id = $7 AND attempt.owner_id = $4
      AND attempt.lane_token = $6 AND attempt.fencing_token = $5
      AND attempt.phase = 'claimed'
    FOR UPDATE OF attempt
), locked_message AS MATERIALIZED (
    SELECT message.message_id
    FROM messages AS message
    JOIN locked_attempt ON true
    WHERE message.tenant_id = $1 AND message.message_id = $7
      AND message.current_state = 'dispatching'
    FOR UPDATE OF message
), updated AS (
    UPDATE message_attempts AS attempt
    SET phase = 'provider_io_started', provider_io_started_at = clock_timestamp()
    FROM locked_message
    WHERE attempt.tenant_id = $1 AND attempt.attempt_id = $8
      AND attempt.message_id = $7 AND attempt.owner_id = $4
      AND attempt.lane_token = $6 AND attempt.fencing_token = $5
      AND attempt.phase = 'claimed'
    RETURNING attempt.attempt_id
)
SELECT EXISTS (SELECT 1 FROM updated)`

func (repository *Repository) BeginProviderIO(ctx context.Context, claim messaging.DispatchClaim, ownerID string) (bool, error) {
	orderingKey := dispatchOrderingKey(claim)
	if !validDispatchClaim(claim) || orderingKey == "" || ownerID == "" || len(ownerID) > 256 {
		return false, messaging.ErrInvalidCommand
	}
	return inTenant(ctx, repository, claim.Message.TenantID, func(tx transaction) (bool, error) {
		var owned bool
		err := tx.QueryRowContext(ctx, beginProviderIOSQL,
			string(claim.Message.TenantID), string(claim.Message.ConnectionID), orderingKey, ownerID,
			claim.FencingToken, claim.LaneToken, string(claim.Message.ID), claim.AttemptID,
		).Scan(&owned)
		if err != nil {
			return false, fmt.Errorf("begin provider I/O: %w", err)
		}
		return owned, nil
	})
}

const renewProviderIOSQL = `/* op:renew_provider_io */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $4 AND lease.fencing_token = $5
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), locked_lane AS MATERIALIZED (
    SELECT lane.ordering_key
    FROM message_lanes AS lane
    JOIN locked_lease ON true
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
      AND lane.owner_id = $4 AND lane.lane_token = $6
      AND lane.claimed_message_id = $7 AND lane.claim_expires_at > clock_timestamp()
    FOR UPDATE OF lane
), locked_attempt AS MATERIALIZED (
    SELECT attempt.attempt_id
    FROM message_attempts AS attempt
	JOIN locked_lane ON true
    WHERE attempt.tenant_id = $1 AND attempt.attempt_id = $8
      AND attempt.message_id = $7 AND attempt.connection_id = $2
      AND attempt.ordering_key = $3 AND attempt.owner_id = $4
      AND attempt.fencing_token = $5 AND attempt.lane_token = $6
      AND attempt.phase = 'provider_io_started'
    FOR UPDATE OF attempt
), locked_message AS MATERIALIZED (
    SELECT message.message_id
    FROM messages AS message
    JOIN locked_attempt ON true
    WHERE message.tenant_id = $1 AND message.message_id = $7
      AND message.current_state = 'dispatching'
    FOR UPDATE OF message
), renewed AS (
    UPDATE message_lanes AS lane
    SET claim_expires_at = clock_timestamp() + interval '30 seconds',
        updated_at = clock_timestamp()
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
      AND lane.owner_id = $4 AND lane.lane_token = $6
      AND lane.claimed_message_id = $7
      AND EXISTS (SELECT 1 FROM locked_message)
    RETURNING lane.ordering_key
)
SELECT EXISTS (SELECT 1 FROM renewed)`

func (repository *Repository) RenewProviderIO(ctx context.Context, claim messaging.DispatchClaim, ownerID string) (bool, error) {
	orderingKey := dispatchOrderingKey(claim)
	if !validDispatchClaim(claim) || orderingKey == "" || ownerID == "" || len(ownerID) > 256 {
		return false, messaging.ErrInvalidCommand
	}
	return inTenant(ctx, repository, claim.Message.TenantID, func(tx transaction) (bool, error) {
		var owned bool
		err := tx.QueryRowContext(ctx, renewProviderIOSQL,
			string(claim.Message.TenantID), string(claim.Message.ConnectionID), orderingKey, ownerID,
			claim.FencingToken, claim.LaneToken, string(claim.Message.ID), claim.AttemptID,
		).Scan(&owned)
		if err != nil {
			return false, fmt.Errorf("renew provider I/O: %w", err)
		}
		return owned, nil
	})
}

const lockDispatchCompletionSQL = `/* op:lock_dispatch_completion */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $8 AND lease.fencing_token = $7
    FOR UPDATE OF lease
), locked_lane AS MATERIALIZED (
    SELECT lane.lane_token
    FROM message_lanes AS lane
    JOIN locked_lease ON true
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
      AND lane.owner_id = $8 AND lane.lane_token = $4 AND lane.claimed_message_id = $5
    FOR UPDATE OF lane
), locked_attempt AS MATERIALIZED (
    SELECT attempt.attempt_id
    FROM message_attempts AS attempt
    JOIN locked_lane ON true
    WHERE attempt.tenant_id = $1 AND attempt.attempt_id = $6
      AND attempt.message_id = $5 AND attempt.owner_id = $8
      AND attempt.lane_token = $4 AND attempt.fencing_token = $7
      AND attempt.phase = 'provider_io_started'
    FOR UPDATE OF attempt
), locked_message AS MATERIALIZED (
    SELECT message.current_state
    FROM messages AS message
    JOIN locked_attempt ON true
    WHERE message.tenant_id = $1 AND message.message_id = $5
      AND message.current_state = 'dispatching'
    FOR UPDATE OF message
)
SELECT current_state FROM locked_message`

const updateDispatchMessageSQL = `/* op:update_dispatch_message */
UPDATE messages SET current_state = $3, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND message_id = $2`

const finishDispatchAttemptSQL = `/* op:finish_dispatch_attempt */
UPDATE message_attempts
SET phase = 'complete', safe_result = $3, completed_at = clock_timestamp()
WHERE tenant_id = $1 AND attempt_id = $2 AND phase = 'provider_io_started'`

const releaseDispatchLaneSQL = `/* op:release_dispatch_lane */
UPDATE message_lanes
SET owner_id = NULL, claimed_message_id = NULL, claim_expires_at = NULL, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND ordering_key = $3
  AND lane_token = $4 AND claimed_message_id = $5`

func (repository *Repository) CompleteDispatch(ctx context.Context, claim messaging.DispatchClaim, states []domain.MessageState, safeReason string) error {
	orderingKey := dispatchOrderingKey(claim)
	if !validDispatchClaim(claim) || orderingKey == "" || claim.OwnerID == "" || len(claim.OwnerID) > 256 || len(states) == 0 || len(safeReason) > 512 {
		return messaging.ErrInvalidCommand
	}
	for _, state := range states {
		if state.Validate() != nil || state == domain.MessageStateQueued || state == domain.MessageStateDispatching {
			return domain.ErrInvalidMessageState
		}
	}
	_, err := inTenant(ctx, repository, claim.Message.TenantID, func(tx transaction) (struct{}, error) {
		var current string
		if err := tx.QueryRowContext(ctx, lockDispatchCompletionSQL,
			string(claim.Message.TenantID), string(claim.Message.ConnectionID), orderingKey,
			claim.LaneToken, string(claim.Message.ID), claim.AttemptID, claim.FencingToken, claim.OwnerID,
		).Scan(&current); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return struct{}{}, messaging.ErrDispatchFenceLost
			}
			return struct{}{}, fmt.Errorf("lock dispatch completion: %w", err)
		}
		finalState := domain.DeriveMessageState(append([]domain.MessageState{domain.MessageState(current)}, states...))
		for _, state := range states {
			if _, err := tx.ExecContext(ctx, insertMessageStatusSQL,
				string(claim.Message.TenantID), repository.newID(), string(claim.Message.ID), string(state), "gmessages", safeReason,
			); err != nil {
				return struct{}{}, fmt.Errorf("insert dispatch status: %w", err)
			}
			eventID := repository.newID()
			body, marshalErr := outboundMessageEventBody(eventID, "message."+string(state), claim.Message, state, time.Now().UTC())
			if marshalErr != nil {
				return struct{}{}, fmt.Errorf("encode dispatch event: %w", marshalErr)
			}
			if _, err := tx.ExecContext(ctx, insertGatewayEventSQL,
				string(claim.Message.TenantID), eventID, "message."+string(state), "message", string(claim.Message.ID),
				string(claim.Message.ConnectionID), claim.Message.ConversationID, body,
			); err != nil {
				return struct{}{}, fmt.Errorf("insert dispatch event: %w", err)
			}
			if _, err := tx.ExecContext(ctx, insertEventOutboxSQL, string(claim.Message.TenantID), repository.newID(), eventID); err != nil {
				return struct{}{}, fmt.Errorf("insert dispatch outbox: %w", err)
			}
		}
		if err := requireOneRow(ctx, tx, updateDispatchMessageSQL, string(claim.Message.TenantID), string(claim.Message.ID), string(finalState)); err != nil {
			return struct{}{}, err
		}
		if err := requireOneRow(ctx, tx, finishDispatchAttemptSQL, string(claim.Message.TenantID), claim.AttemptID, safeReason); err != nil {
			return struct{}{}, err
		}
		if err := requireOneRow(ctx, tx, releaseDispatchLaneSQL,
			string(claim.Message.TenantID), string(claim.Message.ConnectionID), orderingKey, claim.LaneToken, string(claim.Message.ID),
		); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

const releaseBeforeDispatchSQL = `/* op:release_before_dispatch */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $10 AND lease.fencing_token = $7
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), locked_lane AS MATERIALIZED (
    SELECT lane.ordering_key
    FROM message_lanes AS lane
    JOIN locked_lease ON true
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2 AND lane.ordering_key = $3
      AND lane.owner_id = $10 AND lane.lane_token = $4 AND lane.claimed_message_id = $5
      AND lane.claim_expires_at > clock_timestamp()
    FOR UPDATE OF lane
), locked_attempt AS MATERIALIZED (
    SELECT attempt.attempt_id
    FROM message_attempts AS attempt
    JOIN locked_lane ON true
    WHERE attempt.tenant_id = $1 AND attempt.attempt_id = $6 AND attempt.message_id = $5
      AND attempt.owner_id = $10 AND attempt.lane_token = $4
      AND attempt.fencing_token = $7 AND attempt.phase = 'claimed'
    FOR UPDATE OF attempt
), locked_message AS MATERIALIZED (
    SELECT message.message_id
    FROM messages AS message
    JOIN locked_attempt ON true
    WHERE message.tenant_id = $1 AND message.message_id = $5
      AND message.current_state = 'dispatching'
    FOR UPDATE OF message
), completed AS (
    UPDATE message_attempts
    SET phase = 'complete', safe_result = $8, completed_at = clock_timestamp()
    WHERE tenant_id = $1 AND attempt_id IN (SELECT attempt_id FROM locked_attempt)
      AND EXISTS (SELECT 1 FROM locked_message)
    RETURNING attempt_id
), released AS (
    UPDATE message_lanes
    SET owner_id = NULL, claimed_message_id = NULL, claim_expires_at = NULL, updated_at = clock_timestamp()
    WHERE tenant_id = $1 AND connection_id = $2 AND ordering_key = $3
      AND lane_token = $4 AND claimed_message_id = $5
      AND EXISTS (SELECT 1 FROM completed)
    RETURNING ordering_key
), requeued AS (
    UPDATE messages
    SET current_state = 'queued', updated_at = clock_timestamp()
    WHERE tenant_id = $1 AND message_id = $5 AND EXISTS (SELECT 1 FROM released)
    RETURNING message_id
), status AS (
    INSERT INTO message_status_history (tenant_id, status_id, message_id, state, provider_status, safe_reason)
    SELECT $1, $9, message_id, 'queued', '', $8 FROM requeued
    RETURNING status_id
)
SELECT EXISTS (SELECT 1 FROM status)`

func (repository *Repository) ReleaseBeforeDispatch(ctx context.Context, claim messaging.DispatchClaim, safeReason string) error {
	orderingKey := dispatchOrderingKey(claim)
	if !validDispatchClaim(claim) || orderingKey == "" || claim.OwnerID == "" || len(claim.OwnerID) > 256 || len(safeReason) > 512 {
		return messaging.ErrInvalidCommand
	}
	_, err := inTenant(ctx, repository, claim.Message.TenantID, func(tx transaction) (struct{}, error) {
		var released bool
		err := tx.QueryRowContext(ctx, releaseBeforeDispatchSQL,
			string(claim.Message.TenantID), string(claim.Message.ConnectionID), orderingKey, claim.LaneToken,
			string(claim.Message.ID), claim.AttemptID, claim.FencingToken, safeReason, repository.newID(), claim.OwnerID,
		).Scan(&released)
		if err != nil {
			return struct{}{}, fmt.Errorf("release pre-dispatch claim: %w", err)
		}
		if !released {
			return struct{}{}, messaging.ErrDispatchFenceLost
		}
		return struct{}{}, nil
	})
	return err
}

func validDispatchClaim(claim messaging.DispatchClaim) bool {
	return claim.Message.TenantID != "" && claim.Message.ConnectionID != "" && claim.Message.ID != "" &&
		claim.AttemptID != "" && claim.LaneToken > 0 && claim.FencingToken > 0
}

func dispatchOrderingKey(claim messaging.DispatchClaim) string {
	if claim.OrderingKey != "" {
		return claim.OrderingKey
	}
	if claim.Message.ConversationID != "" {
		return claim.Message.ConversationID
	}
	if claim.Message.Recipient != "" {
		return "new:" + claim.Message.Recipient
	}
	return ""
}

func requireOneRow(ctx context.Context, tx transaction, query string, args ...any) error {
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return messaging.ErrDispatchFenceLost
	}
	return nil
}
