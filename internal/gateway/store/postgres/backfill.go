package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/lib/pq"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
)

const loadBackfillCheckpointSQL = `/* op:load_backfill_checkpoint */
SELECT checkpoint_id, next_cursor, terminal, scan_complete,
       conversation_ids, item_states, safe_errors
FROM provider_backfill_checkpoints
WHERE tenant_id = $1 AND connection_id = $2`

func (repository *Repository) LoadBackfillCheckpoint(
	ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID,
) (*messaging.BackfillCheckpoint, error) {
	if connectionID == "" {
		return nil, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (*messaging.BackfillCheckpoint, error) {
		var checkpoint messaging.BackfillCheckpoint
		var conversationIDs, states, safeErrors pq.StringArray
		err := tx.QueryRowContext(ctx, loadBackfillCheckpointSQL, string(tenantID), string(connectionID)).Scan(
			&checkpoint.ID, &checkpoint.NextCursor, &checkpoint.Terminal, &checkpoint.ScanComplete,
			&conversationIDs, &states, &safeErrors,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("load provider backfill checkpoint: %w", err)
		}
		if len(conversationIDs) != len(states) || len(states) != len(safeErrors) || len(states) > messaging.MaxBackfillConversationsPerPage {
			return nil, messaging.ErrBackfillCheckpointConflict
		}
		checkpoint.NextCursor = append([]byte(nil), checkpoint.NextCursor...)
		checkpoint.Items = make([]messaging.BackfillItem, len(states))
		for index := range states {
			state := messaging.BackfillItemState(states[index])
			if state != messaging.BackfillItemPending && state != messaging.BackfillItemComplete && state != messaging.BackfillItemPoisoned {
				return nil, messaging.ErrBackfillCheckpointConflict
			}
			checkpoint.Items[index] = messaging.BackfillItem{
				Ordinal: index, ConversationID: conversationIDs[index], State: state, SafeError: safeErrors[index],
			}
		}
		return &checkpoint, nil
	})
}

const stageBackfillPageSQL = `/* op:stage_backfill_page */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $3 AND lease.fencing_token = $4
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), matching_cursor AS MATERIALIZED (
    SELECT true AS matched
    FROM locked_lease
    WHERE COALESCE((
        SELECT committed_cursor
        FROM conversations
        WHERE tenant_id = $1 AND connection_id = $2 AND conversation_id = '_provider_page'
    ), ''::bytea) = COALESCE($6::bytea, ''::bytea)
), inserted AS (
    INSERT INTO provider_backfill_checkpoints (
        tenant_id, connection_id, checkpoint_id, next_cursor, terminal,
        conversation_ids, item_states, safe_errors
    )
    SELECT $1, $2, $5, NULLIF($7::bytea, ''::bytea), $8, $9, $10, $11
    FROM matching_cursor
    ON CONFLICT (tenant_id, connection_id) DO NOTHING
    RETURNING checkpoint_id
)
SELECT EXISTS (SELECT 1 FROM inserted)`

func (repository *Repository) StageBackfillPageFenced(
	ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID,
	ownerID string, fencingToken uint64, page messaging.BackfillPage,
) error {
	if connectionID == "" || ownerID == "" || fencingToken == 0 || len(page.BaseCursor) > ingress.MaxCursorBytes ||
		len(page.NextCursor) > ingress.MaxCursorBytes || (page.Terminal && len(page.NextCursor) != 0) || (!page.Terminal && len(page.NextCursor) == 0) ||
		len(page.Items) > messaging.MaxBackfillConversationsPerPage {
		return domain.ErrInvalidIdentifier
	}
	conversationIDs := make(pq.StringArray, len(page.Items))
	states := make(pq.StringArray, len(page.Items))
	safeErrors := make(pq.StringArray, len(page.Items))
	for index, item := range page.Items {
		if item.Ordinal != index || (item.State != messaging.BackfillItemPending && item.State != messaging.BackfillItemPoisoned) ||
			(item.ConversationID != "" && !domain.ValidProviderConversationID(item.ConversationID)) ||
			len(item.SafeError) > 128 || !utf8.ValidString(item.SafeError) || (item.State == messaging.BackfillItemPending && item.ConversationID == "") {
			return domain.ErrInvalidIdentifier
		}
		conversationIDs[index], states[index], safeErrors[index] = item.ConversationID, string(item.State), item.SafeError
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		var staged bool
		err := tx.QueryRowContext(ctx, stageBackfillPageSQL,
			string(tenantID), string(connectionID), ownerID, fencingToken, repository.newID(),
			append([]byte(nil), page.BaseCursor...), append([]byte(nil), page.NextCursor...), page.Terminal,
			conversationIDs, states, safeErrors,
		).Scan(&staged)
		if err != nil {
			return fmt.Errorf("stage provider backfill page: %w", err)
		}
		if !staged {
			return messaging.ErrBackfillCheckpointConflict
		}
		return nil
	})
}

const markBackfillItemSQL = `/* op:mark_backfill_item */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $3 AND lease.fencing_token = $4
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), updated AS (
    UPDATE provider_backfill_checkpoints AS checkpoint
    SET item_states[$6 + 1] = $7, safe_errors[$6 + 1] = $8,
        updated_at = clock_timestamp()
    FROM locked_lease
    WHERE checkpoint.tenant_id = $1 AND checkpoint.connection_id = $2
      AND checkpoint.checkpoint_id = $5 AND checkpoint.item_states[$6 + 1] = 'pending'
    RETURNING checkpoint.checkpoint_id
)
SELECT EXISTS (SELECT 1 FROM updated)`

func (repository *Repository) MarkBackfillItemFenced(
	ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID,
	ownerID string, fencingToken uint64, checkpointID string, ordinal int,
	state messaging.BackfillItemState, safeError string,
) error {
	if connectionID == "" || ownerID == "" || fencingToken == 0 || checkpointID == "" || ordinal < 0 || ordinal >= messaging.MaxBackfillConversationsPerPage ||
		(state != messaging.BackfillItemComplete && state != messaging.BackfillItemPoisoned) || len(safeError) > 128 || !utf8.ValidString(safeError) {
		return domain.ErrInvalidIdentifier
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		var marked bool
		err := tx.QueryRowContext(ctx, markBackfillItemSQL,
			string(tenantID), string(connectionID), ownerID, fencingToken, checkpointID, ordinal, string(state), safeError,
		).Scan(&marked)
		if err != nil {
			return fmt.Errorf("mark provider backfill item: %w", err)
		}
		if !marked {
			return messaging.ErrBackfillCheckpointConflict
		}
		return nil
	})
}

const completeBackfillPageSQL = `/* op:complete_backfill_page */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $3 AND lease.fencing_token = $4
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), locked_checkpoint AS MATERIALIZED (
    SELECT checkpoint.next_cursor, checkpoint.terminal
    FROM provider_backfill_checkpoints AS checkpoint
    JOIN locked_lease ON true
    WHERE checkpoint.tenant_id = $1 AND checkpoint.connection_id = $2
      AND checkpoint.checkpoint_id = $5
      AND checkpoint.item_states <@ ARRAY['complete']::text[]
    FOR UPDATE OF checkpoint
), advanced_cursor AS (
    INSERT INTO conversations (tenant_id, connection_id, conversation_id, committed_cursor)
    SELECT $1, $2, '_provider_page', checkpoint.next_cursor
    FROM locked_checkpoint AS checkpoint
    WHERE NOT checkpoint.terminal
    ON CONFLICT (tenant_id, connection_id, conversation_id) DO UPDATE
    SET committed_cursor = EXCLUDED.committed_cursor, updated_at = clock_timestamp()
    RETURNING conversation_id
), terminal AS (
    UPDATE provider_backfill_checkpoints AS checkpoint
    SET scan_complete = true, updated_at = clock_timestamp()
    FROM locked_checkpoint
    WHERE checkpoint.tenant_id = $1 AND checkpoint.connection_id = $2
      AND checkpoint.checkpoint_id = $5 AND locked_checkpoint.terminal
    RETURNING checkpoint.checkpoint_id
), cleared_cursor_history AS (
    DELETE FROM provider_cursor_history AS history
    USING locked_checkpoint
    WHERE history.tenant_id = $1 AND history.connection_id = $2
      AND locked_checkpoint.terminal
    RETURNING history.cursor_scope
), cleared_cursor_budgets AS (
    DELETE FROM provider_cursor_budgets AS budget
    USING locked_checkpoint
    WHERE budget.tenant_id = $1 AND budget.connection_id = $2
      AND locked_checkpoint.terminal
    RETURNING budget.cursor_scope
), removed AS (
    DELETE FROM provider_backfill_checkpoints AS checkpoint
    USING locked_checkpoint
    WHERE checkpoint.tenant_id = $1 AND checkpoint.connection_id = $2
      AND checkpoint.checkpoint_id = $5 AND NOT locked_checkpoint.terminal
      AND EXISTS (SELECT 1 FROM advanced_cursor)
    RETURNING checkpoint.checkpoint_id
)
SELECT EXISTS (SELECT 1 FROM terminal) OR EXISTS (SELECT 1 FROM removed)`

func (repository *Repository) CompleteBackfillPageFenced(
	ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID,
	ownerID string, fencingToken uint64, checkpointID string,
) error {
	if connectionID == "" || ownerID == "" || fencingToken == 0 || checkpointID == "" {
		return domain.ErrInvalidIdentifier
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		var completed bool
		err := tx.QueryRowContext(ctx, completeBackfillPageSQL,
			string(tenantID), string(connectionID), ownerID, fencingToken, checkpointID,
		).Scan(&completed)
		if err != nil {
			return fmt.Errorf("complete provider backfill page: %w", err)
		}
		if !completed {
			return messaging.ErrBackfillCheckpointConflict
		}
		return nil
	})
}
