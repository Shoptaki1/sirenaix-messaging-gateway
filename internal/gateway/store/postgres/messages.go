package postgres

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/lib/pq"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/eventcontract"
	"go.mau.fi/mautrix-gmessages/internal/gateway/ingress"
	"go.mau.fi/mautrix-gmessages/internal/gateway/messaging"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

// A missing row cannot be protected with SELECT FOR UPDATE. Serialize first
// use of a tenant-scoped key for the lifetime of this transaction, then load
// the committed winner and compare its canonical digest.
const lockMessageIdempotencySQL = `/* op:lock_message_idempotency */
SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array($1::text, $2::text)::text, 0))`

const (
	// ProviderACKCoordinationHardTimeout is the repository-owned upper bound for
	// the lock-holding transaction, including the single provider callback and
	// the post-wire ACK state commit.
	ProviderACKCoordinationHardTimeout = 5 * time.Second
	// providerACKPostWireBudget is reserved by an absolute callback deadline.
	// A provider success can therefore never consume the transaction's entire
	// deadline before ACK state is persisted and committed.
	providerACKPostWireBudget  = time.Second
	providerACKWireHardTimeout = ProviderACKCoordinationHardTimeout - providerACKPostWireBudget
)

const getMessageIdempotencySQL = `/* op:get_message_idempotency */
SELECT request_digest, message_id
FROM message_idempotency
WHERE tenant_id = $1 AND idempotency_key = $2
FOR UPDATE`

const insertOutboundMessageSQL = `/* op:insert_outbound_message */
INSERT INTO messages (
    tenant_id, message_id, connection_id, conversation_id, ordering_key,
    direction, recipient, line_id, route_mode, body_text,
    provider_tmp_id, current_state, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, 'outbound', $6, NULLIF($7, ''), $8, $9, $10, 'queued', $11, $11)`

const insertMessageIdempotencySQL = `/* op:insert_message_idempotency */
INSERT INTO message_idempotency (tenant_id, idempotency_key, request_digest, message_id)
VALUES ($1, $2, $3, $4)`

const recordKafkaCommandSQL = `/* op:record_kafka_command */
INSERT INTO kafka_commands (
    tenant_id, command_id, topic, partition_id, offset_id,
    producer_identity, idempotency_key, correlation_id, payload_digest, message_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (tenant_id, topic, partition_id, offset_id) DO UPDATE
SET command_id = kafka_commands.command_id
WHERE kafka_commands.producer_identity = EXCLUDED.producer_identity
  AND kafka_commands.idempotency_key = EXCLUDED.idempotency_key
  AND kafka_commands.correlation_id = EXCLUDED.correlation_id
  AND kafka_commands.payload_digest = EXCLUDED.payload_digest
  AND kafka_commands.message_id = EXCLUDED.message_id
RETURNING command_id, message_id`

const ensureMessageLaneSQL = `/* op:ensure_message_lane */
INSERT INTO message_lanes (tenant_id, connection_id, ordering_key)
VALUES ($1, $2, $3)
ON CONFLICT (tenant_id, connection_id, ordering_key) DO NOTHING`

// Route creation and outbound submission share this transaction-scoped row
// lock. RecordCreatedConversation uses a later statement snapshot after any
// concurrent submitter commits, so it cannot miss old-lane queued work.
const lockMessageRouteConnectionSQL = `/* op:lock_message_route_connection */
SELECT connection_id
FROM connections
WHERE tenant_id = $1 AND connection_id = $2
FOR UPDATE`

const validateMessageRouteSQL = `/* op:validate_message_route */
SELECT conversation.provider_default_outgoing_id,
       COALESCE((
           SELECT line.provider_outgoing_id
           FROM lines AS line
           WHERE line.tenant_id = conversation.tenant_id
             AND line.connection_id = conversation.connection_id
             AND line.line_id = NULLIF($4, '')
             AND line.active = true
       ), '') AS requested_provider_outgoing_id,
       COALESCE(NULLIF(conversation.ordering_key, ''), conversation.conversation_id)
FROM conversations AS conversation
WHERE conversation.tenant_id = $1 AND conversation.connection_id = $2
  AND conversation.conversation_id = $3
FOR SHARE OF conversation`

const insertOutboundMediaSQL = `/* op:insert_outbound_media */
WITH requested AS (
    SELECT media_id, position - 1 AS position
    FROM unnest($3::text[]) WITH ORDINALITY AS input(media_id, position)
), inserted AS (
    INSERT INTO message_media (tenant_id, message_id, media_id, position)
    SELECT $1, $2, media.media_id, requested.position
    FROM requested
    JOIN media_objects AS media
      ON media.tenant_id = $1 AND media.media_id = requested.media_id AND media.state = 'ready'
    RETURNING media_id
)
SELECT count(*) FROM inserted`

const insertMessageStatusSQL = `/* op:insert_message_status */
INSERT INTO message_status_history (tenant_id, status_id, message_id, state, provider_status, safe_reason, observed_at)
VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())`

const insertGatewayEventSQL = `/* op:insert_gateway_event */
INSERT INTO gateway_events (
    tenant_id, event_id, event_type, aggregate_type, aggregate_id,
    connection_id, conversation_id, canonical_body
) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)`

const insertGatewayEventIfAbsentSQL = `/* op:insert_gateway_event */
INSERT INTO gateway_events (
    tenant_id, event_id, event_type, aggregate_type, aggregate_id,
    connection_id, conversation_id, canonical_body
) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)
ON CONFLICT (tenant_id, event_id) DO NOTHING`

const insertEventOutboxSQL = `/* op:insert_event_outbox */
INSERT INTO event_outbox (tenant_id, outbox_id, event_id, destination)
SELECT $1, $2 || ':' || destination, $3, destination
FROM unnest(ARRAY['webhook'::text, 'kafka'::text]) AS destination`

const getOutboundMessageSQL = `/* op:get_outbound_message */
SELECT message_id, connection_id, conversation_id, direction, recipient, COALESCE(line_id, ''),
       route_mode, body_text, COALESCE(provider_message_id, ''), provider_tmp_id, transport, current_state, created_at,
       COALESCE(ARRAY(
           SELECT attachment.media_id
           FROM message_media AS attachment
           WHERE attachment.tenant_id = message.tenant_id AND attachment.message_id = message.message_id
           ORDER BY attachment.position
       ), ARRAY[]::text[]) AS media_ids
FROM messages AS message
WHERE message.tenant_id = $1 AND message.message_id = $2`

func (repository *Repository) CreateOutbound(ctx context.Context, command messaging.CreateOutbound) (messaging.CreateResult, error) {
	message := command.Message
	if message.TenantID == "" || message.ID == "" || message.ConnectionID == "" || command.IdempotencyKey == "" || message.State != domain.MessageStateQueued {
		return messaging.CreateResult{}, messaging.ErrInvalidCommand
	}
	if message.ConversationID != "" && !domain.ValidProviderConversationID(message.ConversationID) {
		return messaging.CreateResult{}, messaging.ErrInvalidRoute
	}
	return inTenant(ctx, repository, message.TenantID, func(tx transaction) (messaging.CreateResult, error) {
		if _, err := tx.ExecContext(ctx, lockMessageIdempotencySQL, string(message.TenantID), command.IdempotencyKey); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("serialize message idempotency: %w", err)
		}
		var storedDigest []byte
		var storedMessageID string
		err := tx.QueryRowContext(ctx, getMessageIdempotencySQL, string(message.TenantID), command.IdempotencyKey).Scan(&storedDigest, &storedMessageID)
		switch {
		case err == nil:
			if len(storedDigest) != sha256Size || subtle.ConstantTimeCompare(storedDigest, command.RequestDigest[:]) != 1 {
				return messaging.CreateResult{Outcome: messaging.CreateConflict}, nil
			}
			stored, loadErr := loadOutboundMessage(ctx, tx, message.TenantID, domain.MessageID(storedMessageID))
			if loadErr != nil {
				return messaging.CreateResult{}, loadErr
			}
			if command.CommandAudit != nil {
				if auditErr := repository.recordKafkaCommand(ctx, tx, command, stored.ID); auditErr != nil {
					return messaging.CreateResult{}, auditErr
				}
			}
			return messaging.CreateResult{Outcome: messaging.CreateDuplicate, Message: stored}, nil
		case !errors.Is(err, sql.ErrNoRows):
			return messaging.CreateResult{}, fmt.Errorf("read message idempotency: %w", err)
		}
		if _, err = tx.ExecContext(ctx, lockMessageRouteConnectionSQL, string(message.TenantID), string(message.ConnectionID)); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("lock message route: %w", err)
		}
		orderingKey := message.ConversationID
		if message.ConversationID != "" {
			var defaultOutgoingID, requestedOutgoingID string
			err = tx.QueryRowContext(ctx, validateMessageRouteSQL,
				string(message.TenantID), string(message.ConnectionID), message.ConversationID, string(message.LineID),
			).Scan(&defaultOutgoingID, &requestedOutgoingID, &orderingKey)
			if errors.Is(err, sql.ErrNoRows) {
				return messaging.CreateResult{}, messaging.ErrInvalidRoute
			}
			if err != nil {
				return messaging.CreateResult{}, fmt.Errorf("validate message route: %w", err)
			}
			if defaultOutgoingID == "" || (message.LineID != "" && requestedOutgoingID != defaultOutgoingID) {
				return messaging.CreateResult{}, messaging.ErrInvalidRoute
			}
		}

		if orderingKey == "" {
			orderingKey = "new:" + message.Recipient
		}
		if _, err = tx.ExecContext(ctx, insertOutboundMessageSQL,
			string(message.TenantID), string(message.ID), string(message.ConnectionID), message.ConversationID, orderingKey,
			message.Recipient, string(message.LineID), message.RouteMode, message.Text, message.ProviderTmpID, message.CreatedAt,
		); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("insert outbound message: %w", err)
		}
		if _, err = tx.ExecContext(ctx, ensureMessageLaneSQL, string(message.TenantID), string(message.ConnectionID), orderingKey); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("ensure message lane: %w", err)
		}
		if len(message.MediaIDs) > 0 {
			values := make([]string, len(message.MediaIDs))
			for index, mediaID := range message.MediaIDs {
				values[index] = string(mediaID)
			}
			var inserted int
			if err = tx.QueryRowContext(ctx, insertOutboundMediaSQL,
				string(message.TenantID), string(message.ID), pq.Array(values),
			).Scan(&inserted); err != nil || inserted != len(values) {
				if err == nil {
					err = messaging.ErrInvalidCommand
				}
				return messaging.CreateResult{}, fmt.Errorf("attach outbound media: %w", err)
			}
		}
		if _, err = tx.ExecContext(ctx, insertMessageIdempotencySQL, string(message.TenantID), command.IdempotencyKey, command.RequestDigest[:], string(message.ID)); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("insert message idempotency: %w", err)
		}
		if command.CommandAudit != nil {
			if err = repository.recordKafkaCommand(ctx, tx, command, message.ID); err != nil {
				return messaging.CreateResult{}, err
			}
		}
		if _, err = tx.ExecContext(ctx, insertMessageStatusSQL, string(message.TenantID), repository.newID(), string(message.ID), string(domain.MessageStateQueued), "", ""); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("insert queued status: %w", err)
		}
		eventID := repository.newID()
		body, marshalErr := outboundMessageEventBody(eventID, "message.queued", message, domain.MessageStateQueued, message.CreatedAt)
		if marshalErr != nil {
			return messaging.CreateResult{}, fmt.Errorf("encode queued event: %w", marshalErr)
		}
		if _, err = tx.ExecContext(ctx, insertGatewayEventSQL, string(message.TenantID), eventID, "message.queued", "message", string(message.ID), string(message.ConnectionID), message.ConversationID, body); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("insert queued event: %w", err)
		}
		if _, err = tx.ExecContext(ctx, insertEventOutboxSQL, string(message.TenantID), repository.newID(), eventID); err != nil {
			return messaging.CreateResult{}, fmt.Errorf("insert queued outbox: %w", err)
		}
		return messaging.CreateResult{Outcome: messaging.CreateInserted, Message: message}, nil
	})
}

func (repository *Repository) recordKafkaCommand(ctx context.Context, tx transaction, command messaging.CreateOutbound, messageID domain.MessageID) error {
	audit := command.CommandAudit
	if audit == nil {
		return nil
	}
	commandID := repository.newID()
	var storedCommandID, storedMessageID string
	err := tx.QueryRowContext(ctx, recordKafkaCommandSQL,
		string(command.Message.TenantID), commandID, audit.Topic, audit.Partition, audit.Offset,
		audit.ProducerIdentity, command.IdempotencyKey, audit.CorrelationID, audit.PayloadDigest[:], string(messageID),
	).Scan(&storedCommandID, &storedMessageID)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.ErrIdempotencyConflict
	}
	if err != nil {
		return fmt.Errorf("record Kafka command: %w", err)
	}
	if storedCommandID == "" || storedMessageID != string(messageID) {
		return messaging.ErrIdempotencyConflict
	}
	return nil
}

func (repository *Repository) GetMessage(ctx context.Context, tenantID domain.TenantID, messageID domain.MessageID) (messaging.OutboundMessage, error) {
	if tenantID == "" || messageID == "" {
		return messaging.OutboundMessage{}, messaging.ErrNotFound
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (messaging.OutboundMessage, error) {
		return loadOutboundMessage(ctx, tx, tenantID, messageID)
	})
}

func loadOutboundMessage(ctx context.Context, tx transaction, tenantID domain.TenantID, messageID domain.MessageID) (messaging.OutboundMessage, error) {
	var message messaging.OutboundMessage
	var id, connectionID, lineID, state string
	var mediaIDs pq.StringArray
	err := tx.QueryRowContext(ctx, getOutboundMessageSQL, string(tenantID), string(messageID)).Scan(
		&id, &connectionID, &message.ConversationID, &message.Direction, &message.Recipient, &lineID,
		&message.RouteMode, &message.Text, &message.ProviderMessageID, &message.ProviderTmpID, &message.Transport, &state, &message.CreatedAt, &mediaIDs,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return messaging.OutboundMessage{}, messaging.ErrNotFound
	}
	if err != nil {
		return messaging.OutboundMessage{}, fmt.Errorf("get outbound message: %w", err)
	}
	message.ID, message.TenantID, message.ConnectionID = domain.MessageID(id), tenantID, domain.ConnectionID(connectionID)
	message.LineID, message.State = domain.LineID(lineID), domain.MessageState(state)
	message.MediaIDs = toMediaIDs(mediaIDs)
	message.Attachments = toAttachments(message.MediaIDs)
	if message.State.Validate() != nil {
		return messaging.OutboundMessage{}, domain.ErrInvalidMessageState
	}
	return message, nil
}

const listMessagesSQL = `/* op:list_messages */
SELECT message_id, connection_id, conversation_id, direction, recipient, COALESCE(line_id, ''),
       route_mode, body_text, COALESCE(provider_message_id, ''), provider_tmp_id, transport, current_state, created_at,
       COALESCE(ARRAY(
           SELECT attachment.media_id
           FROM message_media AS attachment
           WHERE attachment.tenant_id = message.tenant_id AND attachment.message_id = message.message_id
           ORDER BY attachment.position
       ), ARRAY[]::text[]) AS media_ids
FROM messages AS message
WHERE message.tenant_id = $1 AND message.message_id > $2
ORDER BY message.message_id
LIMIT $3`

func (repository *Repository) ListMessages(ctx context.Context, tenantID domain.TenantID, options messaging.ListOptions) (messaging.MessagePage, error) {
	if tenantID == "" || options.Limit < 1 || options.Limit > 200 {
		return messaging.MessagePage{}, messaging.ErrInvalidCommand
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (messaging.MessagePage, error) {
		rows, err := tx.QueryContext(ctx, listMessagesSQL, string(tenantID), string(options.After), options.Limit+1)
		if err != nil {
			return messaging.MessagePage{}, err
		}
		defer rows.Close()
		messages := make([]messaging.OutboundMessage, 0, options.Limit+1)
		for rows.Next() {
			var message messaging.OutboundMessage
			var id, connectionID, lineID, state string
			var mediaIDs pq.StringArray
			if err = rows.Scan(
				&id, &connectionID, &message.ConversationID, &message.Direction, &message.Recipient, &lineID,
				&message.RouteMode, &message.Text, &message.ProviderMessageID, &message.ProviderTmpID, &message.Transport, &state, &message.CreatedAt, &mediaIDs,
			); err != nil {
				return messaging.MessagePage{}, err
			}
			message.ID, message.TenantID, message.ConnectionID = domain.MessageID(id), tenantID, domain.ConnectionID(connectionID)
			message.LineID, message.State = domain.LineID(lineID), domain.MessageState(state)
			message.MediaIDs = toMediaIDs(mediaIDs)
			message.Attachments = toAttachments(message.MediaIDs)
			messages = append(messages, message)
		}
		if err = rows.Err(); err != nil {
			return messaging.MessagePage{}, err
		}
		page := messaging.MessagePage{}
		if len(messages) > options.Limit {
			page.NextCursor = messages[options.Limit-1].ID
			messages = messages[:options.Limit]
		}
		page.Messages = messages
		return page, nil
	})
}

const getMessageLineSQL = `/* op:get_message_line */
SELECT line_id, connection_id, provider_participant_id, provider_outgoing_id, display_name
FROM lines
WHERE tenant_id = $1 AND connection_id = $2 AND line_id = $3 AND active = true`

func (repository *Repository) GetLine(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, lineID domain.LineID) (domain.Line, error) {
	if tenantID == "" || connectionID == "" || lineID == "" {
		return domain.Line{}, messaging.ErrInvalidRoute
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (domain.Line, error) {
		var line domain.Line
		var id, storedConnectionID string
		err := tx.QueryRowContext(ctx, getMessageLineSQL, string(tenantID), string(connectionID), string(lineID)).Scan(
			&id, &storedConnectionID, &line.ProviderParticipantID, &line.ProviderOutgoingID, &line.DisplayName,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Line{}, messaging.ErrInvalidRoute
		}
		if err != nil {
			return domain.Line{}, err
		}
		line.ID, line.TenantID, line.ConnectionID = domain.LineID(id), tenantID, domain.ConnectionID(storedConnectionID)
		return line, nil
	})
}

const recordCreatedConversationSQL = `/* op:record_created_conversation */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.fencing_token
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $7 AND lease.fencing_token = $8
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), routing_keys AS MATERIALIZED (
    SELECT message.ordering_key AS current_key,
           COALESCE(NULLIF(conversation.ordering_key, ''), conversation.conversation_id, message.ordering_key) AS existing_key
    FROM messages AS message
    JOIN locked_lease ON true
    LEFT JOIN conversations AS conversation
      ON conversation.tenant_id = message.tenant_id
     AND conversation.connection_id = message.connection_id
     AND conversation.conversation_id = $4
    WHERE message.tenant_id = $1 AND message.connection_id = $2
      AND message.message_id = $3 AND message.current_state = 'dispatching'
), locked_lanes AS MATERIALIZED (
    SELECT lane.ordering_key, lane.owner_id, lane.claimed_message_id
    FROM message_lanes AS lane
    JOIN routing_keys AS keys
      ON lane.ordering_key IN (keys.current_key, keys.existing_key)
    WHERE lane.tenant_id = $1 AND lane.connection_id = $2
    ORDER BY lane.ordering_key
    FOR UPDATE OF lane
), locked_attempts AS MATERIALIZED (
    SELECT attempt.attempt_id, attempt.message_id, attempt.ordering_key,
           attempt.owner_id, attempt.fencing_token, attempt.phase
    FROM message_attempts AS attempt
    JOIN routing_keys AS keys
      ON attempt.ordering_key IN (keys.current_key, keys.existing_key)
    WHERE attempt.tenant_id = $1 AND attempt.connection_id = $2
      AND attempt.phase IN ('claimed', 'provider_io_started')
    ORDER BY attempt.attempt_id
    FOR UPDATE OF attempt
), locked_messages AS MATERIALIZED (
    SELECT message.message_id, message.ordering_key, message.current_state
    FROM messages AS message
    JOIN routing_keys AS keys ON true
    WHERE message.tenant_id = $1 AND message.connection_id = $2
      AND (message.message_id = $3 OR (
          message.ordering_key = keys.existing_key
          AND message.current_state IN ('queued', 'dispatching')
      ))
    ORDER BY message.message_id
    FOR UPDATE OF message
), current_attempt AS MATERIALIZED (
    SELECT attempt.attempt_id
    FROM locked_attempts AS attempt
    JOIN routing_keys AS keys ON attempt.ordering_key = keys.current_key
    WHERE attempt.message_id = $3 AND attempt.owner_id = $7
      AND attempt.fencing_token = $8 AND attempt.phase = 'provider_io_started'
), current_lane AS MATERIALIZED (
    SELECT lane.ordering_key
    FROM locked_lanes AS lane
    JOIN routing_keys AS keys ON lane.ordering_key = keys.current_key
    WHERE lane.owner_id = $7 AND lane.claimed_message_id = $3
), other_lane_work AS MATERIALIZED (
    SELECT true AS busy
    FROM routing_keys AS keys
    WHERE keys.existing_key <> keys.current_key AND (
        EXISTS (
            SELECT 1 FROM locked_lanes AS lane
            WHERE lane.ordering_key = keys.existing_key AND lane.claimed_message_id IS NOT NULL
        ) OR EXISTS (
            SELECT 1 FROM locked_attempts AS attempt
            WHERE attempt.ordering_key = keys.existing_key
        ) OR EXISTS (
            SELECT 1 FROM locked_messages AS message
            WHERE message.message_id <> $3 AND message.ordering_key = keys.existing_key
              AND message.current_state IN ('queued', 'dispatching')
        )
    )
), route AS (
    INSERT INTO conversations (
        tenant_id, connection_id, conversation_id, provider_default_outgoing_id, is_group, ordering_key
    )
    SELECT $1, $2, $4, $5, $6, message.ordering_key
    FROM current_attempt
    JOIN current_lane ON true
    JOIN routing_keys AS keys ON true
    JOIN messages AS message ON message.tenant_id = $1 AND message.message_id = $3
    WHERE NOT EXISTS (SELECT 1 FROM other_lane_work)
    ON CONFLICT (tenant_id, connection_id, conversation_id) DO UPDATE
    SET provider_default_outgoing_id = EXCLUDED.provider_default_outgoing_id,
        is_group = EXCLUDED.is_group,
        ordering_key = EXCLUDED.ordering_key,
        updated_at = clock_timestamp()
    RETURNING conversation_id
), updated AS (
    UPDATE messages AS message
    SET conversation_id = route.conversation_id, updated_at = clock_timestamp()
    FROM route
    WHERE message.tenant_id = $1 AND message.message_id = $3
      AND message.connection_id = $2 AND message.current_state = 'dispatching'
    RETURNING message.message_id
)
SELECT EXISTS (SELECT 1 FROM current_attempt) AND EXISTS (SELECT 1 FROM current_lane),
       EXISTS (SELECT 1 FROM other_lane_work),
       EXISTS (SELECT 1 FROM updated)`

func (repository *Repository) RecordCreatedConversationFenced(
	ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, messageID domain.MessageID,
	conversationID, defaultOutgoingID string, isGroup bool, ownerID string, fencingToken uint64,
) error {
	if tenantID == "" || connectionID == "" || messageID == "" || !domain.ValidProviderConversationID(conversationID) ||
		!domain.ValidProviderIdentifier(defaultOutgoingID) || ownerID == "" || len(ownerID) > 256 || fencingToken == 0 || isGroup {
		return messaging.ErrInvalidRoute
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		if _, err := tx.ExecContext(ctx, lockMessageRouteConnectionSQL, string(tenantID), string(connectionID)); err != nil {
			return fmt.Errorf("lock created-conversation route: %w", err)
		}
		var fenced, busy, recorded bool
		if err := tx.QueryRowContext(ctx, recordCreatedConversationSQL,
			string(tenantID), string(connectionID), string(messageID), conversationID,
			defaultOutgoingID, isGroup, ownerID, fencingToken,
		).Scan(&fenced, &busy, &recorded); err != nil {
			return err
		}
		if !fenced {
			return ErrConnectionLeaseLost
		}
		if busy {
			return messaging.ErrCanonicalLaneBusy
		}
		if !recorded {
			return ErrConnectionLeaseLost
		}
		return nil
	})
}

func toMediaIDs(values []string) []domain.MediaID {
	result := make([]domain.MediaID, len(values))
	for index, value := range values {
		result[index] = domain.MediaID(value)
	}
	return result
}

func toAttachments(mediaIDs []domain.MediaID) []messaging.Attachment {
	attachments := make([]messaging.Attachment, len(mediaIDs))
	for index, mediaID := range mediaIDs {
		attachments[index] = messaging.Attachment{MediaID: mediaID, Position: index}
	}
	return attachments
}

const verifyInboxFenceSQL = `/* op:verify_inbox_fence */
SELECT true
FROM connections AS connection
JOIN connection_leases AS lease
  ON lease.tenant_id = connection.tenant_id AND lease.connection_id = connection.connection_id
WHERE connection.tenant_id = $1 AND connection.connection_id = $2
  AND lease.owner_id = $3 AND lease.fencing_token = $4
  AND lease.expires_at > clock_timestamp()
FOR UPDATE OF connection, lease`

const getProviderInboxSQL = `/* op:get_provider_inbox */
WITH locked_reservation AS MATERIALIZED (
    SELECT envelope_digest, disposition
    FROM provider_response_reservations
    WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3
      AND NOT conflicted
    FOR UPDATE
)
SELECT locked_reservation.envelope_digest, locked_reservation.disposition,
       CASE locked_reservation.disposition
           WHEN 'inbox' THEN (
               SELECT inbox.ack_pending
               FROM provider_inbox AS inbox
               WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2
                 AND inbox.provider_response_id = $3
               FOR UPDATE
           )
           ELSE (
               SELECT rejected.ack_pending
               FROM provider_rejected_responses AS rejected
               WHERE rejected.tenant_id = $1 AND rejected.connection_id = $2
                 AND rejected.provider_response_id = $3
               FOR UPDATE
           )
       END,
		CASE locked_reservation.disposition
		    WHEN 'inbox' THEN (
		        SELECT inbox.poisoned
               FROM provider_inbox AS inbox
               WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2
                 AND inbox.provider_response_id = $3
           )
		    ELSE true
		END,
		CASE locked_reservation.disposition
		    WHEN 'inbox' THEN (
		        SELECT inbox.poison_reason
		        FROM provider_inbox AS inbox
		        WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2
		          AND inbox.provider_response_id = $3
		    )
		    ELSE (
		        SELECT rejected.reason
		        FROM provider_rejected_responses AS rejected
		        WHERE rejected.tenant_id = $1 AND rejected.connection_id = $2
		          AND rejected.provider_response_id = $3
		    )
		END
FROM locked_reservation
WHERE (locked_reservation.disposition = 'inbox' AND EXISTS (
          SELECT 1 FROM provider_inbox AS inbox
          WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2
            AND inbox.provider_response_id = $3
      )) OR (locked_reservation.disposition = 'rejected' AND EXISTS (
          SELECT 1 FROM provider_rejected_responses AS rejected
          WHERE rejected.tenant_id = $1 AND rejected.connection_id = $2
            AND rejected.provider_response_id = $3
      ))`

const insertProviderInboxSQL = `/* op:insert_provider_inbox */
WITH reserved AS (
    INSERT INTO provider_response_reservations (
        tenant_id, connection_id, provider_response_id, envelope_digest,
        disposition, conflicted, occurrence_count, first_seen_at, last_seen_at
    )
    SELECT $1, $3, $4, $5, 'inbox', false, 1, $11, $11
    WHERE NOT $7 OR ((
        SELECT count(*)
        FROM provider_response_reservations AS poison_reservation
        JOIN provider_inbox AS poison_inbox
          ON poison_inbox.tenant_id = poison_reservation.tenant_id
         AND poison_inbox.connection_id = poison_reservation.connection_id
         AND poison_inbox.provider_response_id = poison_reservation.provider_response_id
        WHERE poison_reservation.tenant_id = $1
          AND poison_reservation.connection_id = $3
          AND poison_reservation.disposition = 'inbox'
          AND poison_inbox.poisoned
    ) < $12 AND COALESCE((
        SELECT sum(octet_length(poison_inbox.raw_envelope))
        FROM provider_inbox AS poison_inbox
        WHERE poison_inbox.tenant_id = $1
          AND poison_inbox.connection_id = $3
          AND poison_inbox.poisoned
    ), 0) + octet_length($6::bytea) <= $13)
    ON CONFLICT (tenant_id, connection_id, provider_response_id) DO NOTHING
    RETURNING tenant_id, connection_id, provider_response_id
)
INSERT INTO provider_inbox (
    tenant_id, inbox_id, connection_id, provider_response_id, envelope_digest,
    raw_envelope, poisoned, poison_reason, ack_pending, owner_id, fencing_token, received_at
)
SELECT $1, $2, $3, $4, $5, $6::bytea, $7, $8, NOT $14, $9, $10, $11
FROM reserved`

const insertProviderInboxConflictSQL = `/* op:insert_provider_inbox_conflict */
WITH conflicted_reservation AS (
    UPDATE provider_response_reservations
    SET conflicted = true,
        occurrence_count = LEAST(occurrence_count + 1, 2147483647),
        last_seen_at = clock_timestamp()
    WHERE tenant_id = $1 AND connection_id = $3 AND provider_response_id = $4
    RETURNING disposition
), fenced_inbox AS (
    UPDATE provider_inbox
    SET ack_pending = false
    WHERE tenant_id = $1 AND connection_id = $3 AND provider_response_id = $4
      AND EXISTS (SELECT 1 FROM conflicted_reservation WHERE disposition = 'inbox')
), fenced_rejected AS (
    UPDATE provider_rejected_responses
    SET conflicted = true, ack_pending = false
    WHERE tenant_id = $1 AND connection_id = $3 AND provider_response_id = $4
      AND EXISTS (SELECT 1 FROM conflicted_reservation WHERE disposition = 'rejected')
)
INSERT INTO provider_inbox_conflicts (
    tenant_id, conflict_id, connection_id, provider_response_id,
    conflicting_digest, conflicting_envelope_size, conflicting_raw_envelope
)
SELECT $1, $2, $3, $4, $5, octet_length($6::bytea), substring($6::bytea FROM 1 FOR 256)
WHERE EXISTS (SELECT 1 FROM conflicted_reservation WHERE disposition = 'inbox')
ON CONFLICT (tenant_id, connection_id, provider_response_id) DO UPDATE
SET occurrence_count = LEAST(provider_inbox_conflicts.occurrence_count + 1, 2147483647),
    observed_at = clock_timestamp()`

const reopenProviderACKSQL = `/* op:reopen_provider_ack */
WITH reservation AS MATERIALIZED (
    SELECT disposition
    FROM provider_response_reservations
    WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3
      AND NOT conflicted
    FOR UPDATE
), reopened_inbox AS (
    UPDATE provider_inbox
    SET ack_pending = true, acked_at = NULL
    WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3
      AND EXISTS (SELECT 1 FROM reservation WHERE disposition = 'inbox')
), reopened_rejected AS (
    UPDATE provider_rejected_responses
    SET ack_pending = true, acked_at = NULL
    WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3
      AND NOT conflicted
      AND EXISTS (SELECT 1 FROM reservation WHERE disposition = 'rejected')
)
SELECT 1`

const lockProjectedMessageSQL = `/* op:lock_projected_message */
SELECT message_id, current_state, direction
FROM messages
WHERE tenant_id = $1 AND connection_id = $2
  AND (($4 <> '' AND provider_tmp_id = $4) OR provider_message_id = $3)
ORDER BY CASE WHEN $4 <> '' AND provider_tmp_id = $4 THEN 0 ELSE 1 END
LIMIT 1
FOR UPDATE`

const listMessageStatesSQL = `/* op:list_message_states */
SELECT state
FROM message_status_history
WHERE tenant_id = $1 AND message_id = $2
ORDER BY observed_at, status_id
FOR UPDATE`

const insertProjectedMessageSQL = `/* op:insert_projected_message */
INSERT INTO messages (
    tenant_id, message_id, connection_id, conversation_id, ordering_key,
    direction, body_text, provider_message_id, provider_tmp_id, transport,
    current_state
) VALUES ($1, $2, $3, $4, $4, $5, $6, $7, NULLIF($8, ''), $9, $10)
ON CONFLICT DO NOTHING`

const updateProjectedMessageSQL = `/* op:update_projected_message */
UPDATE messages
SET conversation_id = CASE WHEN $4 = '' THEN conversation_id ELSE $4 END,
    provider_message_id = COALESCE(NULLIF($5, ''), provider_message_id),
    transport = CASE WHEN $6 = '' THEN transport ELSE $6 END,
    current_state = $7,
    body_text = CASE WHEN body_text = '' THEN $8 ELSE body_text END,
    direction = CASE
        WHEN direction = 'unknown' AND $9 IN ('inbound', 'outbound') THEN $9
        ELSE direction
    END,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND message_id = $3`

const insertInboundStatusSQL = `/* op:insert_inbound_status */
INSERT INTO message_status_history (tenant_id, status_id, message_id, state, provider_status, safe_reason)
VALUES ($1, $2, $3, $4, $5, '')`

const upsertProviderConversationSQL = `/* op:upsert_provider_conversation */
INSERT INTO conversations (
    tenant_id, connection_id, conversation_id, provider_default_outgoing_id, is_group, ordering_key
) VALUES ($1, $2, $3, $4, $5, $3)
ON CONFLICT (tenant_id, connection_id, conversation_id) DO UPDATE
SET provider_default_outgoing_id = EXCLUDED.provider_default_outgoing_id,
    is_group = EXCLUDED.is_group,
    ordering_key = COALESCE(NULLIF(conversations.ordering_key, ''), EXCLUDED.ordering_key),
    updated_at = clock_timestamp()`

const insertMediaObjectSQL = `/* op:insert_media_object */
INSERT INTO media_objects (
    tenant_id, media_id, message_id, state, mime_type, display_filename, byte_size
) VALUES ($1, $2, NULLIF($3, ''), 'pending', $4, $5, GREATEST($6, 0))`

const insertMediaFetchJobSQL = `/* op:insert_media_fetch_job */
INSERT INTO media_fetch_jobs (
    tenant_id, job_id, media_id, connection_id, provider_message_id,
    provider_locator, declared_mime_type, declared_size, display_filename,
    key_ciphertext, key_wrapped_dek, key_nonce, key_id, key_version,
    thumbnail_key_ciphertext, thumbnail_key_wrapped_dek, thumbnail_key_nonce,
    thumbnail_key_id, thumbnail_key_version, attachment_identity_digest
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, GREATEST($8, 0), $9,
    $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
)`

const insertInboundMediaLinkSQL = `/* op:insert_inbound_media_link */
INSERT INTO message_media (tenant_id, message_id, media_id, position, provider_identity_digest)
VALUES ($1, $2, $3, $4, $5)`

const lockExistingAttachmentSQL = `/* op:lock_existing_attachment */
SELECT association.media_id, media.state, message.direction,
       COALESCE(association.provider_identity_digest, (
           SELECT job.attachment_identity_digest
           FROM media_fetch_jobs AS job
           WHERE job.tenant_id = association.tenant_id AND job.media_id = association.media_id
           ORDER BY job.job_id
           LIMIT 1
       ), ''::bytea) AS attachment_identity_digest
FROM messages AS message
JOIN message_media AS association
  ON association.tenant_id = message.tenant_id AND association.message_id = message.message_id
JOIN media_objects AS media
  ON media.tenant_id = association.tenant_id AND media.media_id = association.media_id
WHERE message.tenant_id = $1 AND message.connection_id = $2
  AND (($4 <> '' AND message.provider_tmp_id = $4) OR message.provider_message_id = $3)
  AND association.position = $5
ORDER BY CASE WHEN $4 <> '' AND message.provider_tmp_id = $4 THEN 0 ELSE 1 END
LIMIT 1
FOR UPDATE OF message, association, media`

const bindOutboundAttachmentIdentitySQL = `/* op:bind_outbound_attachment_identity */
UPDATE message_media AS association
SET provider_identity_digest = $6
FROM messages AS message
WHERE association.tenant_id = $1
  AND message.tenant_id = association.tenant_id AND message.message_id = association.message_id
  AND message.connection_id = $2
  AND (($4 <> '' AND message.provider_tmp_id = $4) OR message.provider_message_id = $3)
  AND association.position = $5 AND association.provider_identity_digest IS NULL`

const finalizeProviderPoisonSQL = `/* op:finalize_provider_poison */
WITH candidate AS MATERIALIZED (
    SELECT raw_envelope
    FROM provider_inbox
    WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3
      AND NOT poisoned
    FOR UPDATE
), poison_usage AS MATERIALIZED (
    SELECT count(*) AS rows_used,
           COALESCE(sum(octet_length(raw_envelope)), 0) AS bytes_used
    FROM provider_inbox
    WHERE tenant_id = $1 AND connection_id = $2 AND poisoned
)
UPDATE provider_inbox AS inbox
SET poisoned = true, poison_reason = $4
FROM candidate, poison_usage
WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2 AND inbox.provider_response_id = $3
  AND poison_usage.rows_used < $5
  AND poison_usage.bytes_used + octet_length(candidate.raw_envelope) <= $6
RETURNING 'inbox'::text`

const compactProviderPoisonSQL = `/* op:compact_provider_poison */
WITH candidate AS MATERIALIZED (
    SELECT inbox.envelope_digest, inbox.received_at, reservation.conflicted
    FROM provider_inbox AS inbox
    JOIN provider_response_reservations AS reservation
      ON reservation.tenant_id = inbox.tenant_id
     AND reservation.connection_id = inbox.connection_id
     AND reservation.provider_response_id = inbox.provider_response_id
    WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2 AND inbox.provider_response_id = $3
      AND NOT inbox.poisoned AND reservation.disposition = 'inbox'
    FOR UPDATE OF inbox, reservation
), rejected_usage AS MATERIALIZED (
    SELECT count(*) AS rows_used
    FROM provider_response_reservations
    WHERE tenant_id = $1 AND connection_id = $2 AND disposition = 'rejected'
), removed_conflicts AS (
    DELETE FROM provider_inbox_conflicts AS conflict
    USING candidate, rejected_usage
    WHERE conflict.tenant_id = $1 AND conflict.connection_id = $2 AND conflict.provider_response_id = $3
      AND rejected_usage.rows_used < $5
    RETURNING conflict.provider_response_id
), removed AS (
    DELETE FROM provider_inbox AS inbox
    USING candidate, rejected_usage
    WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2 AND inbox.provider_response_id = $3
      AND rejected_usage.rows_used < $5
      AND (SELECT count(*) FROM removed_conflicts) >= 0
    RETURNING inbox.envelope_digest, inbox.received_at
), moved_reservation AS (
    UPDATE provider_response_reservations AS reservation
    SET disposition = 'rejected', last_seen_at = clock_timestamp()
    FROM removed, candidate
    WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
      AND reservation.provider_response_id = $3
      AND reservation.disposition = 'inbox'
      AND reservation.envelope_digest = removed.envelope_digest
    RETURNING reservation.envelope_digest, reservation.conflicted, reservation.occurrence_count
), inserted_rejected AS (
    INSERT INTO provider_rejected_responses (
        tenant_id, connection_id, provider_response_id, envelope_digest, reason,
        ack_pending, conflicted, occurrence_count, first_seen_at, last_seen_at
    )
    SELECT $1, $2, $3, moved_reservation.envelope_digest, $4,
           NOT moved_reservation.conflicted, moved_reservation.conflicted, moved_reservation.occurrence_count,
           removed.received_at, clock_timestamp()
    FROM moved_reservation
    JOIN removed ON removed.envelope_digest = moved_reservation.envelope_digest
    RETURNING provider_response_id
)
SELECT 'rejected'::text FROM inserted_rejected`

const advanceConversationCursorSQL = `/* op:advance_conversation_cursor */
INSERT INTO conversations (tenant_id, connection_id, conversation_id, committed_cursor)
VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, connection_id, conversation_id) DO UPDATE
SET committed_cursor = EXCLUDED.committed_cursor, updated_at = clock_timestamp()`

const seedProviderCursorHistorySQL = `/* op:seed_provider_cursor_history */
INSERT INTO provider_cursor_history (
    tenant_id, connection_id, cursor_scope, cursor_digest, provider_response_id
) VALUES ($1, $2, $3, $4, NULL)
ON CONFLICT (tenant_id, connection_id, cursor_scope, cursor_digest) DO NOTHING`

const insertProviderCursorHistorySQL = `/* op:insert_provider_cursor_history */
INSERT INTO provider_cursor_history (
    tenant_id, connection_id, cursor_scope, cursor_digest, base_cursor_digest, provider_response_id
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (tenant_id, connection_id, cursor_scope, cursor_digest) DO UPDATE
SET provider_response_id = provider_cursor_history.provider_response_id
WHERE provider_cursor_history.base_cursor_digest IS NOT DISTINCT FROM EXCLUDED.base_cursor_digest
RETURNING cursor_digest, base_cursor_digest, provider_response_id`

const ensureProviderCursorBudgetSQL = `/* op:ensure_provider_cursor_budget */
INSERT INTO provider_cursor_budgets (
    tenant_id, connection_id, cursor_scope, accepted_advances,
    last_provider_response_id, exhausted
) VALUES ($1, $2, $3, 0, $4, false)
ON CONFLICT (tenant_id, connection_id, cursor_scope) DO NOTHING`

const lockProviderCursorBudgetSQL = `/* op:lock_provider_cursor_budget */
SELECT accepted_advances, exhausted
FROM provider_cursor_budgets
WHERE tenant_id = $1 AND connection_id = $2 AND cursor_scope = $3
FOR UPDATE`

const upsertRejectedProviderResponseSQL = `/* op:upsert_rejected_provider_response */
WITH existing AS MATERIALIZED (
    SELECT disposition
    FROM provider_response_reservations
    WHERE tenant_id = $1 AND connection_id = $2 AND provider_response_id = $3
), capacity AS MATERIALIZED (
    SELECT count(*) AS used
    FROM provider_response_reservations
    WHERE tenant_id = $1 AND connection_id = $2 AND disposition = 'rejected'
), reservation AS (
    INSERT INTO provider_response_reservations (
        tenant_id, connection_id, provider_response_id, envelope_digest,
        disposition, conflicted, occurrence_count, first_seen_at, last_seen_at
    )
    SELECT $1, $2, $3, $4, 'rejected', false, 1, $5, $5
    FROM capacity
    WHERE capacity.used < $6 OR EXISTS (
        SELECT 1 FROM existing WHERE disposition = 'rejected'
    )
    ON CONFLICT (tenant_id, connection_id, provider_response_id) DO UPDATE
    SET last_seen_at = EXCLUDED.last_seen_at,
        occurrence_count = LEAST(provider_response_reservations.occurrence_count + 1, 2147483647),
        conflicted = provider_response_reservations.conflicted
            OR provider_response_reservations.envelope_digest <> EXCLUDED.envelope_digest
    RETURNING envelope_digest, disposition, conflicted, occurrence_count
), rejected AS (
    INSERT INTO provider_rejected_responses (
        tenant_id, connection_id, provider_response_id, envelope_digest, reason,
        ack_pending, conflicted, occurrence_count, first_seen_at, last_seen_at
    )
    SELECT $1, $2, $3, reservation.envelope_digest,
           'provider_cursor_budget_exhausted', NOT reservation.conflicted,
           reservation.conflicted, reservation.occurrence_count, $5, $5
    FROM reservation
    WHERE reservation.disposition = 'rejected'
    ON CONFLICT (tenant_id, connection_id, provider_response_id) DO UPDATE
    SET last_seen_at = EXCLUDED.last_seen_at,
        occurrence_count = EXCLUDED.occurrence_count,
        conflicted = EXCLUDED.conflicted,
        ack_pending = NOT EXCLUDED.conflicted,
        acked_at = CASE WHEN NOT EXCLUDED.conflicted THEN NULL ELSE provider_rejected_responses.acked_at END
    RETURNING envelope_digest, conflicted, occurrence_count
)
SELECT envelope_digest, conflicted, occurrence_count FROM rejected`

const findProviderCursorHistorySQL = `/* op:find_provider_cursor_history */
SELECT base_cursor_digest, provider_response_id
FROM provider_cursor_history
WHERE tenant_id = $1 AND connection_id = $2 AND cursor_scope = $3 AND cursor_digest = $4
FOR UPDATE`

const incrementProviderCursorBudgetSQL = `/* op:increment_provider_cursor_budget */
UPDATE provider_cursor_budgets
SET accepted_advances = accepted_advances + 1,
    last_provider_response_id = $4,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND cursor_scope = $3
  AND NOT exhausted AND accepted_advances < $5
RETURNING accepted_advances`

const exhaustProviderCursorBudgetSQL = `/* op:exhaust_provider_cursor_budget */
UPDATE provider_cursor_budgets
SET exhausted = true,
    last_provider_response_id = $4,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND connection_id = $2 AND cursor_scope = $3
  AND NOT exhausted AND accepted_advances >= $5
RETURNING exhausted`

const pruneProviderCursorHistorySQL = `/* op:prune_provider_cursor_history */
DELETE FROM provider_cursor_history
WHERE tenant_id = $1 AND connection_id = $2 AND cursor_scope = $3
  AND cursor_digest IN (
      SELECT cursor_digest
      FROM provider_cursor_history
      WHERE tenant_id = $1 AND connection_id = $2 AND cursor_scope = $3
      ORDER BY observed_at DESC, cursor_digest DESC
      OFFSET 64
  )`

const loadCommittedCursorSQL = `/* op:load_committed_cursor */
SELECT committed_cursor
FROM conversations
WHERE tenant_id = $1 AND connection_id = $2 AND conversation_id = $3
  AND committed_cursor IS NOT NULL
  AND octet_length(committed_cursor) <= 4096`

func (repository *Repository) LoadCommittedCursor(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, conversationID string) ([]byte, error) {
	if tenantID == "" || connectionID == "" ||
		(conversationID != domain.ProviderPageCursorID && !domain.ValidProviderConversationID(conversationID)) {
		return nil, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]byte, error) {
		var cursor []byte
		err := tx.QueryRowContext(ctx, loadCommittedCursorSQL, string(tenantID), string(connectionID), conversationID).Scan(&cursor)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("load committed provider cursor: %w", err)
		}
		return append([]byte(nil), cursor...), nil
	})
}

func (repository *Repository) CommitEnvelope(ctx context.Context, record ingress.EnvelopeRecord) (ingress.CommitResult, error) {
	if record.TenantID == "" || record.ConnectionID == "" || !domain.ValidProviderResponseID(record.ProviderResponseID) || record.OwnerID == "" || record.FencingToken == 0 ||
		len(record.Raw) == 0 || len(record.Raw) > 4<<20 || !record.ACKPending || (record.ACKWithheld && !record.Poisoned) {
		return 0, domain.ErrInvalidIdentifier
	}
	canonicalProjection, err := canonicalizeProjectionCursors(record.Projection)
	if err != nil {
		return 0, domain.ErrInvalidIdentifier
	}
	record.Projection = canonicalProjection
	if err := ingress.ValidateProjection(record.Projection, record.Media); err != nil {
		return 0, domain.ErrInvalidIdentifier
	}
	cursorCandidate := record.Projection.CursorCandidate
	if len(cursorCandidate) == 0 && len(record.Projection.Cursor) > 0 {
		cursorCandidate = record.Projection.Cursor
	}
	return inTenant(ctx, repository, record.TenantID, func(tx transaction) (ingress.CommitResult, error) {
		var fenced bool
		if err := tx.QueryRowContext(ctx, verifyInboxFenceSQL,
			string(record.TenantID), string(record.ConnectionID), record.OwnerID, record.FencingToken,
		).Scan(&fenced); err != nil || !fenced {
			if errors.Is(err, sql.ErrNoRows) || !fenced {
				return 0, ErrConnectionLeaseLost
			}
			return 0, fmt.Errorf("verify inbox fence: %w", err)
		}

		var acceptedAdvances int
		var exhausted bool
		if len(cursorCandidate) > 0 {
			scope := record.Projection.CursorConversationID
			if _, err = tx.ExecContext(ctx, ensureProviderCursorBudgetSQL,
				string(record.TenantID), string(record.ConnectionID), scope, record.ProviderResponseID,
			); err != nil {
				return 0, fmt.Errorf("ensure provider cursor budget: %w", err)
			}
			if err = tx.QueryRowContext(ctx, lockProviderCursorBudgetSQL,
				string(record.TenantID), string(record.ConnectionID), scope,
			).Scan(&acceptedAdvances, &exhausted); err != nil {
				return 0, fmt.Errorf("lock provider cursor budget: %w", err)
			}
			if acceptedAdvances < 0 || acceptedAdvances > ingress.MaxProviderCursorAdvances {
				return 0, errors.New("provider cursor budget is corrupt")
			}
		}

		// Resolve the response ID only after the cursor breaker is locked. The
		// authoritative reservation joins both raw inbox and rejected identities,
		// so a rejected-first ID can never later enter another action or scope.
		var existingDigest []byte
		var existingDisposition string
		var ackPending, existingPoisoned bool
		var existingPoisonReason string
		err := tx.QueryRowContext(ctx, getProviderInboxSQL, string(record.TenantID), string(record.ConnectionID), record.ProviderResponseID).Scan(
			&existingDigest, &existingDisposition, &ackPending, &existingPoisoned, &existingPoisonReason,
		)
		if err == nil {
			if len(existingDigest) == sha256Size && subtle.ConstantTimeCompare(existingDigest, record.Digest[:]) == 1 {
				if existingDisposition == "inbox" && existingPoisoned && existingPoisonReason == ingress.PoisonReasonInvalidSettingsSnapshot {
					return ingress.CommitDuplicateACKWithheld, nil
				}
				if !ackPending && !record.ACKWithheld {
					if _, reopenErr := tx.ExecContext(ctx, reopenProviderACKSQL,
						string(record.TenantID), string(record.ConnectionID), record.ProviderResponseID,
					); reopenErr != nil {
						return 0, fmt.Errorf("reopen provider ACK: %w", reopenErr)
					}
				}
				if existingPoisoned {
					return ingress.CommitDuplicatePoisoned, nil
				}
				return ingress.CommitDuplicate, nil
			}
			if _, err = tx.ExecContext(ctx, insertProviderInboxConflictSQL,
				string(record.TenantID), repository.newID(), string(record.ConnectionID), record.ProviderResponseID, record.Digest[:], record.Raw,
			); err != nil {
				return 0, fmt.Errorf("quarantine conflicting envelope: %w", err)
			}
			if existingDisposition == "inbox" && !existingPoisoned {
				if err = repository.finalizeProviderPoison(ctx, tx, record, "response_id_digest_conflict"); err != nil {
					// The reservation conflict and ACK fence above are the
					// authoritative safety state. Optional raw/rejected evidence
					// exhausting both bounded families must not roll that state
					// back: retain the existing non-poison raw row and commit the
					// conflict. Exact replay remains fenced by the reservation.
					if errors.Is(err, ingress.ErrProviderResponseCapacity) {
						return ingress.CommitConflict, nil
					}
					return 0, fmt.Errorf("bound conflicting envelope evidence: %w", err)
				}
			}
			return ingress.CommitConflict, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("read provider inbox: %w", err)
		}

		if len(cursorCandidate) > 0 {
			if exhausted {
				var storedDigest []byte
				var conflicted bool
				var occurrences int64
				if err = tx.QueryRowContext(ctx, upsertRejectedProviderResponseSQL,
					string(record.TenantID), string(record.ConnectionID), record.ProviderResponseID, record.Digest[:], record.ReceivedAt,
					ingress.MaxRejectedProviderResponses,
				).Scan(&storedDigest, &conflicted, &occurrences); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						return 0, errors.Join(ingress.ErrProviderResponseCapacity, ingress.ErrConflictingEnvelope)
					}
					return 0, fmt.Errorf("persist rejected provider response: %w", err)
				}
				digestMatches := len(storedDigest) == sha256Size && subtle.ConstantTimeCompare(storedDigest, record.Digest[:]) == 1
				if conflicted || !digestMatches {
					return ingress.CommitConflict, nil
				}
				if occurrences > 1 {
					return ingress.CommitDuplicatePoisoned, nil
				}
				return ingress.CommitPoisoned, nil
			}
		}
		inserted, insertErr := tx.ExecContext(ctx, insertProviderInboxSQL,
			string(record.TenantID), repository.newID(), string(record.ConnectionID), record.ProviderResponseID,
			record.Digest[:], record.Raw, record.Poisoned, record.PoisonReason, record.OwnerID, record.FencingToken, record.ReceivedAt,
			ingress.MaxPoisonedProviderInboxEntries,
			ingress.MaxPoisonedProviderInboxBytes,
			record.ACKWithheld,
		)
		if insertErr != nil {
			return 0, fmt.Errorf("insert provider inbox: %w", insertErr)
		}
		insertedCount, insertErr := inserted.RowsAffected()
		if insertErr != nil {
			return 0, fmt.Errorf("read inserted provider inbox count: %w", insertErr)
		}
		if insertedCount != 1 {
			if record.Poisoned {
				return 0, errors.Join(ingress.ErrProviderResponseCapacity, ingress.ErrConflictingEnvelope)
			}
			return 0, ingress.ErrConflictingEnvelope
		}

		if record.Projection.LineSnapshot {
			lineRecords := make([]LineRecord, 0, len(record.Projection.Lines))
			for _, projected := range record.Projection.Lines {
				if projected.TenantID != record.TenantID || projected.ConnectionID != record.ConnectionID ||
					projected.DiscoverySource != ingress.LineDiscoveryAuthenticatedGoogleSettings {
					return 0, errors.New("provider line snapshot crossed its durable scope")
				}
				phone, phoneErr := domain.ParseE164(projected.Phone)
				if phoneErr != nil {
					return 0, fmt.Errorf("parse projected line phone: %w", phoneErr)
				}
				lineRecords = append(lineRecords, LineRecord{
					Line: domain.Line{
						ID: projected.ID, TenantID: projected.TenantID, ConnectionID: projected.ConnectionID,
						ProviderParticipantID: projected.ProviderParticipantID, ProviderOutgoingID: projected.ProviderOutgoingID,
						DisplayName: projected.DisplayName,
					},
					Phone: phone, CarrierName: projected.CarrierName, ColorHex: projected.ColorHex, RCSEnabled: projected.RCSEnabled,
					ProviderSIMNumber: projected.ProviderSIMNumber, ProviderSIMPayloadType: projected.ProviderSIMPayloadType,
					DiscoverySource: LineDiscoveryAuthenticatedGoogleSettings,
				})
			}
			if lineErr := validateLineReplacement(record.TenantID, record.ConnectionID, lineRecords); lineErr != nil {
				return 0, fmt.Errorf("validate durable line snapshot: %w", lineErr)
			}
			if lineErr := replaceLineRows(ctx, tx, record.TenantID, record.ConnectionID, lineRecords); lineErr != nil {
				return 0, fmt.Errorf("replace durable line snapshot: %w", lineErr)
			}
		}

		if len(cursorCandidate) > 0 {
			scope := record.Projection.CursorConversationID
			var baseDigestBytes []byte
			if len(record.Projection.CursorBase) > 0 {
				baseDigest := sha256.Sum256(record.Projection.CursorBase)
				baseDigestBytes = baseDigest[:]
			}
			candidateDigest := sha256.Sum256(cursorCandidate)
			var storedBaseDigest []byte
			var storedProviderResponseID sql.NullString
			err = tx.QueryRowContext(ctx, findProviderCursorHistorySQL,
				string(record.TenantID), string(record.ConnectionID), scope, candidateDigest[:],
			).Scan(&storedBaseDigest, &storedProviderResponseID)
			if err == nil {
				baseMatches := len(baseDigestBytes) == len(storedBaseDigest) &&
					(len(baseDigestBytes) == 0 || subtle.ConstantTimeCompare(baseDigestBytes, storedBaseDigest) == 1)
				if !baseMatches {
					return repository.poisonCursorEnvelope(ctx, tx, record, "provider_cursor_cycle", true)
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				return 0, fmt.Errorf("find provider cursor history: %w", err)
			} else {
				if acceptedAdvances >= ingress.MaxProviderCursorAdvances {
					var newlyExhausted bool
					if err = tx.QueryRowContext(ctx, exhaustProviderCursorBudgetSQL,
						string(record.TenantID), string(record.ConnectionID), scope, record.ProviderResponseID, ingress.MaxProviderCursorAdvances,
					).Scan(&newlyExhausted); err != nil || !newlyExhausted {
						if err == nil {
							err = errors.New("provider cursor budget was not exhausted")
						}
						return 0, fmt.Errorf("exhaust provider cursor budget: %w", err)
					}
					return repository.poisonCursorEnvelope(ctx, tx, record, "provider_cursor_budget_exhausted", true)
				}
				if len(baseDigestBytes) > 0 {
					if _, err = tx.ExecContext(ctx, seedProviderCursorHistorySQL,
						string(record.TenantID), string(record.ConnectionID), scope, baseDigestBytes,
					); err != nil {
						return 0, fmt.Errorf("seed provider cursor history: %w", err)
					}
				}
				var insertedDigest, storedBaseDigest []byte
				var insertedResponseID sql.NullString
				err = tx.QueryRowContext(ctx, insertProviderCursorHistorySQL,
					string(record.TenantID), string(record.ConnectionID), scope, candidateDigest[:], baseDigestBytes, record.ProviderResponseID,
				).Scan(&insertedDigest, &storedBaseDigest, &insertedResponseID)
				baseMatches := len(baseDigestBytes) == len(storedBaseDigest) &&
					(len(baseDigestBytes) == 0 || subtle.ConstantTimeCompare(baseDigestBytes, storedBaseDigest) == 1)
				if err != nil || len(insertedDigest) != sha256Size || subtle.ConstantTimeCompare(insertedDigest, candidateDigest[:]) != 1 || !baseMatches ||
					!insertedResponseID.Valid || insertedResponseID.String != record.ProviderResponseID {
					if err == nil {
						err = errors.New("provider cursor history digest mismatch")
					}
					return 0, fmt.Errorf("insert provider cursor history: %w", err)
				}
				if err = tx.QueryRowContext(ctx, incrementProviderCursorBudgetSQL,
					string(record.TenantID), string(record.ConnectionID), scope, record.ProviderResponseID, ingress.MaxProviderCursorAdvances,
				).Scan(&acceptedAdvances); err != nil || acceptedAdvances < 1 || acceptedAdvances > ingress.MaxProviderCursorAdvances {
					if err == nil {
						err = errors.New("provider cursor budget increment is invalid")
					}
					return 0, fmt.Errorf("increment provider cursor budget: %w", err)
				}
			}
			if _, err = tx.ExecContext(ctx, pruneProviderCursorHistorySQL,
				string(record.TenantID), string(record.ConnectionID), scope,
			); err != nil {
				return 0, fmt.Errorf("prune provider cursor history: %w", err)
			}
		}

		projectedByProviderID := make(map[string]ingress.ProjectedMessage, len(record.Projection.Messages))
		for _, projected := range record.Projection.Messages {
			projectedByProviderID[projected.ProviderMessageID] = projected
		}
		locatorByAggregate := make(map[string]ingress.MediaLocator, len(record.Media))
		for _, locator := range record.Media {
			locatorByAggregate[ingress.AttachmentAggregateID(locator)] = locator
		}
		reusedAttachments := make(map[string]struct{}, len(record.Media))
		mediaByAggregate := make(map[string]eventcontract.Media, len(record.Media))
		for _, locator := range record.Media {
			projected := projectedByProviderID[locator.ProviderMessageID]
			var mediaID, state, direction string
			var storedIdentity []byte
			err = tx.QueryRowContext(ctx, lockExistingAttachmentSQL,
				string(record.TenantID), string(record.ConnectionID), locator.ProviderMessageID, projected.ProviderTmpID, locator.Position,
			).Scan(&mediaID, &state, &direction, &storedIdentity)
			identity := ingress.AttachmentIdentityDigest(locator)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				continue
			case err != nil:
				return 0, fmt.Errorf("lock existing attachment: %w", err)
			case len(storedIdentity) == len(identity) && subtle.ConstantTimeCompare(storedIdentity, identity[:]) == 1:
				aggregateID := ingress.AttachmentAggregateID(locator)
				reusedAttachments[aggregateID] = struct{}{}
				mediaByAggregate[aggregateID] = contractMedia(mediaID, state, locator)
				continue
			case len(storedIdentity) == 0 && direction == "outbound" && state == "ready":
				// An uploaded outbound attachment predates any provider locator.
				// The first remote echo binds that occupied position once; later
				// echoes must compare the same canonical provider identity.
				result, bindErr := tx.ExecContext(ctx, bindOutboundAttachmentIdentitySQL,
					string(record.TenantID), string(record.ConnectionID), locator.ProviderMessageID,
					projected.ProviderTmpID, locator.Position, identity[:],
				)
				if bindErr != nil {
					return 0, fmt.Errorf("bind outbound attachment identity: %w", bindErr)
				}
				if count, countErr := result.RowsAffected(); countErr != nil || count != 1 {
					return 0, errors.New("outbound attachment identity changed during binding")
				}
				aggregateID := ingress.AttachmentAggregateID(locator)
				reusedAttachments[aggregateID] = struct{}{}
				mediaByAggregate[aggregateID] = contractMedia(mediaID, state, locator)
				continue
			default:
				const reason = "media_identity_conflict"
				if err = repository.finalizeProviderPoison(ctx, tx, record, reason); err != nil {
					return 0, fmt.Errorf("poison conflicting attachment envelope: %w", err)
				}
				eventID := stableInboxEventID(record.TenantID, record.ConnectionID, record.ProviderResponseID, reason)
				body, marshalErr := providerPoisonEventBody(eventID, record, reason)
				if marshalErr != nil {
					return 0, fmt.Errorf("encode attachment conflict event: %w", marshalErr)
				}
				if _, err = tx.ExecContext(ctx, insertGatewayEventSQL,
					string(record.TenantID), string(eventID), "inbox.poisoned", "provider_envelope", record.ProviderResponseID,
					string(record.ConnectionID), "", body,
				); err != nil {
					return 0, fmt.Errorf("insert attachment conflict event: %w", err)
				}
				if _, err = tx.ExecContext(ctx, insertEventOutboxSQL, string(record.TenantID), repository.newID(), string(eventID)); err != nil {
					return 0, fmt.Errorf("insert attachment conflict outbox: %w", err)
				}
				return ingress.CommitPoisoned, nil
			}
		}
		for _, conversation := range record.Projection.Conversations {
			if !domain.ValidProviderConversationID(conversation.ConversationID) ||
				(conversation.DefaultOutgoingID != "" && !domain.ValidProviderIdentifier(conversation.DefaultOutgoingID)) {
				return 0, errors.New("invalid provider conversation projection")
			}
			if _, err = tx.ExecContext(ctx, upsertProviderConversationSQL,
				string(record.TenantID), string(record.ConnectionID), conversation.ConversationID,
				conversation.DefaultOutgoingID, conversation.IsGroup,
			); err != nil {
				return 0, fmt.Errorf("project provider conversation: %w", err)
			}
		}

		messageIDs := make(map[string]string, len(record.Projection.Messages))
		messageStates := make(map[string]domain.MessageState, len(record.Projection.Messages))
		messageDirections := make(map[string]string, len(record.Projection.Messages))
		messageInserted := make(map[string]bool, len(record.Projection.Messages))
		for _, projected := range record.Projection.Messages {
			var storedID, currentState, direction string
			var transition domain.MessageState
			err = tx.QueryRowContext(ctx, lockProjectedMessageSQL,
				string(record.TenantID), string(record.ConnectionID), projected.ProviderMessageID, projected.ProviderTmpID,
			).Scan(&storedID, &currentState, &direction)
			if errors.Is(err, sql.ErrNoRows) {
				candidateID := repository.newID()
				state := projected.State
				if state == "" {
					// An authenticated provider status that has no normalized final
					// mapping (for example, an inbound download in progress) is
					// uncertain. Never manufacture carrier-sent evidence.
					state = domain.MessageStateUncertain
				}
				if state.Validate() != nil {
					return 0, errors.New("invalid provider message state")
				}
				direction = projected.Direction
				if direction == "" {
					// Backward-compatible default for projections created before the
					// authenticated direction field existed. The production provider
					// adapter always supplies an exact direction.
					direction = "inbound"
				}
				insertResult, insertMessageErr := tx.ExecContext(ctx, insertProjectedMessageSQL,
					string(record.TenantID), candidateID, string(record.ConnectionID), projected.ConversationID,
					direction, projected.Text, projected.ProviderMessageID, projected.ProviderTmpID, projected.Transport, string(state),
				)
				if insertMessageErr != nil {
					return 0, fmt.Errorf("insert projected message: %w", insertMessageErr)
				}
				insertedRows, rowsErr := insertResult.RowsAffected()
				if rowsErr != nil {
					return 0, fmt.Errorf("read projected message insert count: %w", rowsErr)
				}
				switch insertedRows {
				case 1:
					storedID = candidateID
					currentState = string(state)
					messageInserted[projected.ProviderMessageID] = true
				case 0:
					// Another connection actor response may have committed the same
					// provider message between the first lookup and this unique insert.
					// Lock and use that semantic winner instead of retrying with a
					// response-scoped event identity.
					err = tx.QueryRowContext(ctx, lockProjectedMessageSQL,
						string(record.TenantID), string(record.ConnectionID), projected.ProviderMessageID, projected.ProviderTmpID,
					).Scan(&storedID, &currentState, &direction)
					if err != nil {
						return 0, fmt.Errorf("load concurrent projected message winner: %w", err)
					}
				default:
					return 0, errors.New("projected message insert count is invalid")
				}
			} else if err != nil {
				return 0, fmt.Errorf("lock projected message: %w", err)
			}

			if messageInserted[projected.ProviderMessageID] {
				messageStates[projected.ProviderMessageID] = domain.MessageState(currentState)
			} else {
				if projected.Direction != "" && projected.Direction != "unknown" {
					switch direction {
					case "unknown":
						direction = projected.Direction
					case projected.Direction:
					default:
						return 0, errors.New("provider message direction conflicts with durable identity")
					}
				}
				history, historyErr := loadMessageStateHistory(ctx, tx, record.TenantID, domain.MessageID(storedID))
				if historyErr != nil {
					return 0, historyErr
				}
				if len(history) == 0 {
					history = append(history, domain.MessageState(currentState))
				}
				if projected.State != "" {
					history = append(history, projected.State)
				}
				derived := domain.DeriveMessageState(history)
				if derived.Validate() != nil {
					return 0, errors.New("invalid reconciled provider message state")
				}
				if _, err = tx.ExecContext(ctx, updateProjectedMessageSQL,
					string(record.TenantID), string(record.ConnectionID), storedID, projected.ConversationID,
					projected.ProviderMessageID, projected.Transport, string(derived), projected.Text, direction,
				); err != nil {
					return 0, fmt.Errorf("reconcile provider message: %w", err)
				}
				if direction == "outbound" {
					if derived != domain.MessageState(currentState) {
						transition = derived
					}
				}
				messageStates[projected.ProviderMessageID] = derived
			}
			messageIDs[projected.ProviderMessageID] = storedID
			messageDirections[projected.ProviderMessageID] = direction
			if projected.State != "" {
				providerStatus := projected.ProviderStatus
				if providerStatus == "" {
					providerStatus = "gmessages"
				}
				if _, err = tx.ExecContext(ctx, insertInboundStatusSQL,
					string(record.TenantID), repository.newID(), storedID, string(projected.State), providerStatus,
				); err != nil {
					return 0, fmt.Errorf("insert inbound status: %w", err)
				}
			}
			if transition != "" {
				eventType := "message." + string(transition)
				eventID := receiptTransitionEventID(record.TenantID, domain.MessageID(storedID), transition, projected)
				occurredAt := record.ReceivedAt
				if !projected.ProviderOccurredAt.IsZero() {
					occurredAt = projected.ProviderOccurredAt
				}
				body, marshalErr := eventcontract.Marshal(eventcontract.Envelope{
					EventID: string(eventID), Type: eventType, OccurredAt: occurredAt, IngestedAt: record.ReceivedAt,
					TenantID: string(record.TenantID), ConnectionID: string(record.ConnectionID), ConversationID: projected.ConversationID,
					MessageID: storedID, ProviderMessageID: projected.ProviderMessageID, ProviderTmpID: projected.ProviderTmpID,
					Direction: "outbound", Provenance: string(projected.Provenance), ProviderStatus: projected.ProviderStatus,
					Sender: projected.Sender, Recipients: projected.Recipients, Text: projected.Text,
					Transport: projected.Transport, Status: string(transition), State: string(transition),
				})
				if marshalErr != nil {
					return 0, fmt.Errorf("encode receipt transition event: %w", marshalErr)
				}
				if _, err = tx.ExecContext(ctx, insertGatewayEventSQL,
					string(record.TenantID), string(eventID), eventType, "message", storedID,
					string(record.ConnectionID), projected.ConversationID, body,
				); err != nil {
					return 0, fmt.Errorf("insert receipt transition event: %w", err)
				}
				if _, err = tx.ExecContext(ctx, insertEventOutboxSQL, string(record.TenantID), repository.newID(), string(eventID)); err != nil {
					return 0, fmt.Errorf("insert receipt transition outbox: %w", err)
				}
			}
		}
		for _, locator := range record.Media {
			if _, reused := reusedAttachments[ingress.AttachmentAggregateID(locator)]; reused {
				continue
			}
			mediaID, jobID := repository.newID(), repository.newID()
			messageID := messageIDs[locator.ProviderMessageID]
			if messageID == "" {
				return 0, errors.New("provider media has no projected message")
			}
			if _, err = tx.ExecContext(ctx, insertMediaObjectSQL,
				string(record.TenantID), mediaID, messageID, locator.MIMEType,
				locator.DisplayFilename, locator.DeclaredSize,
			); err != nil {
				return 0, fmt.Errorf("insert pending media: %w", err)
			}
			identity := ingress.AttachmentIdentityDigest(locator)
			if _, err = tx.ExecContext(ctx, insertInboundMediaLinkSQL,
				string(record.TenantID), messageID, mediaID, locator.Position, identity[:],
			); err != nil {
				return 0, fmt.Errorf("associate inbound media: %w", err)
			}
			keyArgs := envelopeSQLArgs(locator.KeyEnvelope)
			thumbnailArgs := envelopeSQLArgs(locator.ThumbnailKeyEnvelope)
			args := []any{
				string(record.TenantID), jobID, mediaID, string(record.ConnectionID), locator.ProviderMessageID,
				locator.Locator, locator.MIMEType, locator.DeclaredSize, locator.DisplayFilename,
			}
			args = append(args, keyArgs...)
			args = append(args, thumbnailArgs...)
			args = append(args, identity[:])
			if _, err = tx.ExecContext(ctx, insertMediaFetchJobSQL, args...); err != nil {
				return 0, fmt.Errorf("insert media fetch job: %w", err)
			}
			mediaByAggregate[ingress.AttachmentAggregateID(locator)] = contractMedia(mediaID, "pending", locator)
		}
		for _, event := range record.Events {
			if event.Type == "media.pending" {
				if _, reused := reusedAttachments[event.AggregateID]; reused {
					continue
				}
			}
			if event.ID == "" || event.Type == "" || len(event.CanonicalBody) == 0 || len(event.CanonicalBody) > 1<<20 {
				return 0, errors.New("invalid durable event")
			}
			canonicalBody := event.CanonicalBody
			aggregateType := "provider_envelope"
			aggregateID := event.AggregateID
			eventID := event.ID
			eventType := event.Type
			partitionConversation := event.PartitionConversation
			semanticMessageEvent := event.Type == "message.received" || event.Type == "message.imported" || event.Type == "message.updated"
			if semanticMessageEvent {
				projected, exists := projectedByProviderID[event.AggregateID]
				messageID := messageIDs[event.AggregateID]
				if !exists || messageID == "" {
					return 0, errors.New("message event has no projected message")
				}
				media := make([]eventcontract.Media, 0, len(record.Media))
				for _, locator := range record.Media {
					if locator.ProviderMessageID != projected.ProviderMessageID {
						continue
					}
					if item, present := mediaByAggregate[ingress.AttachmentAggregateID(locator)]; present {
						media = append(media, item)
					}
				}
				legacy := map[string]any{
					"provider_message_id": projected.ProviderMessageID,
					"conversation_id":     projected.ConversationID,
				}
				switch {
				case actionableProviderMessage(projected):
					eventType = "message.received"
				case messageInserted[event.AggregateID] &&
					(projected.Provenance == ingress.MessageProvenanceHistory || projected.Provenance == ingress.MessageProvenanceReplay):
					eventType = "message.imported"
				default:
					eventType = "message.updated"
				}
				identityProjection := projected
				identityProjection.Direction = messageDirections[event.AggregateID]
				eventID = semanticMessageEventID(record.TenantID, domain.MessageID(messageID), eventType, identityProjection, messageStates[event.AggregateID], media)
				occurredAt := record.ReceivedAt
				if !projected.ProviderOccurredAt.IsZero() {
					occurredAt = projected.ProviderOccurredAt
				}
				canonicalBody, err = eventcontract.Marshal(eventcontract.Envelope{
					EventID: string(eventID), Type: eventType, OccurredAt: occurredAt, IngestedAt: record.ReceivedAt,
					TenantID: string(record.TenantID), ConnectionID: string(record.ConnectionID), ConversationID: projected.ConversationID,
					MessageID: messageID, ProviderMessageID: projected.ProviderMessageID, ProviderTmpID: projected.ProviderTmpID,
					Direction: messageDirections[event.AggregateID], Provenance: string(projected.Provenance), ProviderStatus: projected.ProviderStatus,
					Actionable: eventType == "message.received", Sender: projected.Sender, Recipients: projected.Recipients, Text: projected.Text,
					Transport: projected.Transport, Status: string(messageStates[projected.ProviderMessageID]), State: string(messageStates[projected.ProviderMessageID]),
					Media: media, Data: legacy,
				})
				if err != nil {
					return 0, fmt.Errorf("encode message event contract: %w", err)
				}
				aggregateType = "message"
				aggregateID = messageID
			} else if event.Type == "media.pending" {
				media, exists := mediaByAggregate[event.AggregateID]
				locator, locatorExists := locatorByAggregate[event.AggregateID]
				projected, messageExists := projectedByProviderID[locator.ProviderMessageID]
				messageID := messageIDs[locator.ProviderMessageID]
				if !exists || !locatorExists || !messageExists || media.ID == "" || messageID == "" {
					return 0, errors.New("pending media event has no allocated media object")
				}
				legacy := map[string]any{
					"provider_message_id": locator.ProviderMessageID,
					"message_id":          messageID,
					"conversation_id":     projected.ConversationID,
					"attachment_index":    locator.Position,
					"media_id":            media.ID,
					"status":              media.Status,
					"metadata_path":       media.MetadataPath,
				}
				canonicalBody, err = eventcontract.Marshal(eventcontract.Envelope{
					EventID: string(event.ID), Type: event.Type, OccurredAt: record.ReceivedAt,
					TenantID: string(record.TenantID), ConnectionID: string(record.ConnectionID), ConversationID: projected.ConversationID,
					MessageID: messageID, ProviderMessageID: locator.ProviderMessageID,
					MediaID: media.ID, MetadataPath: media.MetadataPath,
					Status: media.Status, State: media.Status, Media: []eventcontract.Media{media}, Data: legacy,
				})
				if err != nil {
					return 0, fmt.Errorf("encode pending media event contract: %w", err)
				}
				aggregateType = "media"
				aggregateID = media.ID
				partitionConversation = projected.ConversationID
			}
			insertEventSQL := insertGatewayEventSQL
			if semanticMessageEvent {
				insertEventSQL = insertGatewayEventIfAbsentSQL
			}
			insertedEvent, insertEventErr := tx.ExecContext(ctx, insertEventSQL,
				string(record.TenantID), string(eventID), eventType, aggregateType, aggregateID,
				string(record.ConnectionID), partitionConversation, canonicalBody,
			)
			if insertEventErr != nil {
				return 0, fmt.Errorf("insert ingress event: %w", insertEventErr)
			}
			insertedEventCount, rowsErr := insertedEvent.RowsAffected()
			if rowsErr != nil {
				return 0, fmt.Errorf("read ingress event insert count: %w", rowsErr)
			}
			if insertedEventCount == 0 && semanticMessageEvent {
				continue
			}
			if insertedEventCount != 1 {
				return 0, errors.New("ingress event insert count is invalid")
			}
			if _, err = tx.ExecContext(ctx, insertEventOutboxSQL, string(record.TenantID), repository.newID(), string(eventID)); err != nil {
				return 0, fmt.Errorf("insert ingress outbox: %w", err)
			}
		}
		if len(record.Projection.Cursor) > 0 {
			if record.Projection.CursorSource != ingress.CursorSourceListMessages ||
				!domain.ValidProviderConversationID(record.Projection.CursorConversationID) {
				return 0, errors.New("invalid committed cursor source or target")
			}
			if _, err = tx.ExecContext(ctx, advanceConversationCursorSQL,
				string(record.TenantID), string(record.ConnectionID), record.Projection.CursorConversationID, record.Projection.Cursor,
			); err != nil {
				return 0, fmt.Errorf("advance committed cursor: %w", err)
			}
		}
		if record.Poisoned {
			return ingress.CommitPoisoned, nil
		}
		return ingress.CommitInserted, nil
	})
}

func contractMedia(mediaID, status string, locator ingress.MediaLocator) eventcontract.Media {
	item := eventcontract.Media{
		ID: mediaID, Position: locator.Position, Status: status, MIMEType: locator.MIMEType,
		Size: locator.DeclaredSize, DisplayFilename: locator.DisplayFilename,
		MetadataPath: "/v1/media/" + mediaID,
	}
	if status == "ready" {
		item.ContentPath = "/v1/media/" + mediaID + "/content"
	}
	return item
}

func outboundMessageEventBody(eventID, eventType string, message messaging.OutboundMessage, state domain.MessageState, occurredAt time.Time) ([]byte, error) {
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	var recipients []string
	if phone, err := domain.ParseE164(message.Recipient); err == nil {
		recipients = []string{phone.String()}
	}
	media := make([]eventcontract.Media, 0, len(message.MediaIDs))
	for position, mediaID := range message.MediaIDs {
		media = append(media, eventcontract.Media{
			ID: string(mediaID), Position: position, Status: "ready", ContentPath: "/v1/media/" + string(mediaID) + "/content",
		})
	}
	return eventcontract.Marshal(eventcontract.Envelope{
		EventID: eventID, Type: eventType, OccurredAt: occurredAt,
		TenantID: string(message.TenantID), ConnectionID: string(message.ConnectionID), ConversationID: message.ConversationID,
		MessageID: string(message.ID), ProviderMessageID: message.ProviderMessageID, ProviderTmpID: message.ProviderTmpID,
		Direction: "outbound", Recipients: recipients, Text: message.Text, Transport: message.Transport,
		Status: string(state), State: string(state), Media: media,
	})
}

func canonicalizeProjectionCursors(projection ingress.Projection) (ingress.Projection, error) {
	var err error
	if len(projection.Cursor) > 0 {
		projection.Cursor, err = ingress.CanonicalProviderCursor(projection.Cursor)
		if err != nil {
			return ingress.Projection{}, err
		}
	}
	if len(projection.CursorBase) > 0 {
		projection.CursorBase, err = ingress.CanonicalProviderCursor(projection.CursorBase)
		if err != nil {
			return ingress.Projection{}, err
		}
	}
	if len(projection.CursorCandidate) > 0 {
		projection.CursorCandidate, err = ingress.CanonicalProviderCursor(projection.CursorCandidate)
		if err != nil {
			return ingress.Projection{}, err
		}
	}
	return projection, nil
}

func (repository *Repository) poisonCursorEnvelope(
	ctx context.Context, tx transaction, record ingress.EnvelopeRecord, reason string, emitEvent bool,
) (ingress.CommitResult, error) {
	if err := repository.finalizeProviderPoison(ctx, tx, record, reason); err != nil {
		return 0, fmt.Errorf("poison provider cursor envelope: %w", err)
	}
	if !emitEvent {
		return ingress.CommitPoisoned, nil
	}
	eventID := stableInboxEventID(record.TenantID, record.ConnectionID, record.ProviderResponseID, reason)
	body, marshalErr := providerPoisonEventBody(eventID, record, reason)
	if marshalErr != nil {
		return 0, fmt.Errorf("encode provider cursor poison event: %w", marshalErr)
	}
	if _, err := tx.ExecContext(ctx, insertGatewayEventSQL,
		string(record.TenantID), string(eventID), "inbox.poisoned", "provider_envelope", record.ProviderResponseID,
		string(record.ConnectionID), "", body,
	); err != nil {
		return 0, fmt.Errorf("insert provider cursor poison event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, insertEventOutboxSQL, string(record.TenantID), repository.newID(), string(eventID)); err != nil {
		return 0, fmt.Errorf("insert provider cursor poison outbox: %w", err)
	}
	return ingress.CommitPoisoned, nil
}

func (repository *Repository) finalizeProviderPoison(
	ctx context.Context, tx transaction, record ingress.EnvelopeRecord, reason string,
) error {
	var disposition string
	err := tx.QueryRowContext(ctx, finalizeProviderPoisonSQL,
		string(record.TenantID), string(record.ConnectionID), record.ProviderResponseID, reason,
		ingress.MaxPoisonedProviderInboxEntries, ingress.MaxPoisonedProviderInboxBytes,
	).Scan(&disposition)
	if err == nil {
		if disposition != "inbox" {
			return errors.New("provider poison disposition is invalid")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("admit raw provider poison evidence: %w", err)
	}
	err = tx.QueryRowContext(ctx, compactProviderPoisonSQL,
		string(record.TenantID), string(record.ConnectionID), record.ProviderResponseID, reason,
		ingress.MaxRejectedProviderResponses,
	).Scan(&disposition)
	if err == nil {
		if disposition != "rejected" {
			return errors.New("compacted provider poison disposition is invalid")
		}
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ingress.ErrProviderResponseCapacity
	}
	return fmt.Errorf("compact provider poison evidence: %w", err)
}

func receiptTransitionEventID(tenantID domain.TenantID, messageID domain.MessageID, state domain.MessageState, projected ingress.ProjectedMessage) domain.EventID {
	digest := sha256.Sum256([]byte(string(tenantID) + "\x00" + string(messageID) + "\x00" + string(state) + "\x00" + projected.ProviderMessageID + "\x00" + projected.ProviderTmpID))
	return domain.EventID(fmt.Sprintf("evt_%x", digest[:16]))
}

func semanticMessageEventID(
	tenantID domain.TenantID,
	messageID domain.MessageID,
	eventType string,
	projected ingress.ProjectedMessage,
	state domain.MessageState,
	media []eventcontract.Media,
) domain.EventID {
	if eventType != "message.updated" {
		digest := sha256.Sum256([]byte(string(tenantID) + "\x00" + string(messageID) + "\x00" + eventType))
		return domain.EventID(fmt.Sprintf("evt_%x", digest[:16]))
	}
	digest := sha256.New()
	writeMessageEventIdentityField(digest, string(tenantID))
	writeMessageEventIdentityField(digest, string(messageID))
	writeMessageEventIdentityField(digest, eventType)
	writeMessageEventIdentityField(digest, projected.ProviderMessageID)
	writeMessageEventIdentityField(digest, projected.ProviderTmpID)
	writeMessageEventIdentityField(digest, projected.ConversationID)
	writeMessageEventIdentityField(digest, projected.Direction)
	writeMessageEventIdentityField(digest, string(projected.Provenance))
	writeMessageEventIdentityField(digest, projected.ProviderStatus)
	writeMessageEventIdentityField(digest, projected.ProviderOccurredAt.UTC().Format(time.RFC3339Nano))
	writeMessageEventIdentityField(digest, projected.Sender)
	recipients := append([]string(nil), projected.Recipients...)
	sort.Strings(recipients)
	for _, recipient := range recipients {
		writeMessageEventIdentityField(digest, recipient)
	}
	writeMessageEventIdentityField(digest, projected.Text)
	writeMessageEventIdentityField(digest, projected.Transport)
	writeMessageEventIdentityField(digest, string(state))
	orderedMedia := append([]eventcontract.Media(nil), media...)
	sort.Slice(orderedMedia, func(i, j int) bool {
		if orderedMedia[i].Position == orderedMedia[j].Position {
			return orderedMedia[i].ID < orderedMedia[j].ID
		}
		return orderedMedia[i].Position < orderedMedia[j].Position
	})
	for _, item := range orderedMedia {
		writeMessageEventIdentityField(digest, item.ID)
		writeMessageEventIdentityField(digest, strconv.Itoa(item.Position))
		writeMessageEventIdentityField(digest, item.Status)
		writeMessageEventIdentityField(digest, item.MIMEType)
		writeMessageEventIdentityField(digest, strconv.FormatInt(item.Size, 10))
		writeMessageEventIdentityField(digest, item.DisplayFilename)
		writeMessageEventIdentityField(digest, item.MetadataPath)
		writeMessageEventIdentityField(digest, item.ContentPath)
	}
	return domain.EventID(fmt.Sprintf("evt_%x", digest.Sum(nil)[:16]))
}

func actionableProviderMessage(projected ingress.ProjectedMessage) bool {
	return projected.Actionable && projected.Direction == "inbound" &&
		projected.Provenance == ingress.MessageProvenanceLive && projected.ProviderStatus == "INCOMING_COMPLETE"
}

func writeMessageEventIdentityField(destination interface{ Write([]byte) (int, error) }, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = destination.Write(length[:])
	_, _ = destination.Write([]byte(value))
}

func stableInboxEventID(tenantID domain.TenantID, connectionID domain.ConnectionID, responseID, reason string) domain.EventID {
	digest := sha256.Sum256([]byte(string(tenantID) + "\x00" + string(connectionID) + "\x00" + responseID + "\x00" + reason))
	return domain.EventID(fmt.Sprintf("evt_%x", digest[:16]))
}

func providerPoisonEventBody(eventID domain.EventID, record ingress.EnvelopeRecord, reason string) ([]byte, error) {
	occurredAt := record.ReceivedAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	return eventcontract.Marshal(eventcontract.Envelope{
		EventID: string(eventID), Type: "inbox.poisoned", OccurredAt: occurredAt,
		TenantID: string(record.TenantID), ConnectionID: string(record.ConnectionID),
		ProviderResponseID: record.ProviderResponseID, Reason: reason,
	})
}

func loadMessageStateHistory(ctx context.Context, tx transaction, tenantID domain.TenantID, messageID domain.MessageID) ([]domain.MessageState, error) {
	rows, err := tx.QueryContext(ctx, listMessageStatesSQL, string(tenantID), string(messageID))
	if err != nil {
		return nil, fmt.Errorf("lock message status history: %w", err)
	}
	defer rows.Close()
	var history []domain.MessageState
	for rows.Next() {
		var state string
		if err = rows.Scan(&state); err != nil {
			return nil, fmt.Errorf("read message status history: %w", err)
		}
		history = append(history, domain.MessageState(state))
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("read message status history: %w", err)
	}
	return history, nil
}

func envelopeSQLArgs(envelope session.Envelope) []any {
	if len(envelope.Ciphertext) == 0 {
		return []any{nil, nil, nil, nil, nil}
	}
	return []any{envelope.Ciphertext, envelope.WrappedDEK, envelope.Nonce, envelope.KeyID, envelope.KeyVersion}
}

const markProviderACKedSQL = `/* op:mark_provider_acked */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    SELECT lease.connection_id
    FROM connection_leases AS lease
    JOIN locked_connection AS connection ON connection.connection_id = lease.connection_id
    WHERE lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $3 AND lease.fencing_token = $4
      AND lease.expires_at > clock_timestamp()
    FOR UPDATE OF lease
), updated_inbox AS (
    UPDATE provider_inbox
    SET ack_pending = false, acked_at = clock_timestamp()
    WHERE tenant_id = $1 AND connection_id = $2
      AND provider_response_id = ANY($5)
      AND ack_pending
      AND EXISTS (SELECT 1 FROM locked_lease)
      AND EXISTS (
          SELECT 1 FROM provider_response_reservations AS reservation
          WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
            AND reservation.provider_response_id = provider_inbox.provider_response_id
            AND reservation.disposition = 'inbox' AND NOT reservation.conflicted
      )
    RETURNING provider_response_id
), updated_rejected AS (
    UPDATE provider_rejected_responses
    SET ack_pending = false, acked_at = clock_timestamp()
    WHERE tenant_id = $1 AND connection_id = $2
      AND provider_response_id = ANY($5)
      AND ack_pending AND NOT conflicted
      AND EXISTS (SELECT 1 FROM locked_lease)
      AND EXISTS (
          SELECT 1 FROM provider_response_reservations AS reservation
          WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
            AND reservation.provider_response_id = provider_rejected_responses.provider_response_id
            AND reservation.disposition = 'rejected' AND NOT reservation.conflicted
      )
    RETURNING provider_response_id
), updated AS (
    SELECT provider_response_id FROM updated_inbox
    UNION
    SELECT provider_response_id FROM updated_rejected
)
SELECT EXISTS (SELECT 1 FROM locked_lease)
   AND (SELECT count(*) FROM updated) = cardinality($5)`

const coordinateProviderACKBatchSQL = `/* op:coordinate_provider_ack_batch */
WITH locked_connection AS MATERIALIZED (
    SELECT connection_id
    FROM connections
    WHERE tenant_id = $1 AND connection_id = $2
    FOR UPDATE
), locked_lease AS MATERIALIZED (
    UPDATE connection_leases AS lease
    SET expires_at = GREATEST(
            lease.expires_at,
            clock_timestamp() + ($6 * interval '1 microsecond')
        ),
        updated_at = clock_timestamp()
    FROM locked_connection AS connection
    WHERE connection.connection_id = lease.connection_id
      AND lease.tenant_id = $1 AND lease.connection_id = $2
      AND lease.owner_id = $3 AND lease.fencing_token = $4
      AND lease.expires_at > clock_timestamp()
    RETURNING lease.connection_id
), locked_reservations AS MATERIALIZED (
    SELECT reservation.provider_response_id, reservation.disposition
    FROM provider_response_reservations AS reservation
    JOIN locked_lease ON locked_lease.connection_id = reservation.connection_id
    WHERE reservation.tenant_id = $1 AND reservation.connection_id = $2
      AND reservation.provider_response_id = ANY($5)
      AND NOT reservation.conflicted
    ORDER BY reservation.provider_response_id
    FOR UPDATE OF reservation
), inbox_eligible AS MATERIALIZED (
    SELECT inbox.provider_response_id
    FROM provider_inbox AS inbox
    JOIN locked_reservations AS reservation
      ON reservation.provider_response_id = inbox.provider_response_id
     AND reservation.disposition = 'inbox'
    WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2 AND inbox.ack_pending
    ORDER BY inbox.provider_response_id
    FOR UPDATE OF inbox
), rejected_eligible AS MATERIALIZED (
    SELECT rejected.provider_response_id
    FROM provider_rejected_responses AS rejected
    JOIN locked_reservations AS reservation
      ON reservation.provider_response_id = rejected.provider_response_id
     AND reservation.disposition = 'rejected'
    WHERE rejected.tenant_id = $1 AND rejected.connection_id = $2
      AND rejected.ack_pending AND NOT rejected.conflicted
    ORDER BY rejected.provider_response_id
    FOR UPDATE OF rejected
), eligible AS MATERIALIZED (
    SELECT provider_response_id FROM inbox_eligible
    UNION ALL
    SELECT provider_response_id FROM rejected_eligible
)
SELECT COALESCE(ARRAY(
           SELECT provider_response_id FROM eligible
           ORDER BY array_position($5::text[], provider_response_id)
       ), ARRAY[]::text[]),
       EXISTS (SELECT 1 FROM locked_lease)`

const completeCoordinatedProviderACKSQL = `/* op:complete_coordinated_provider_ack */
WITH updated_inbox AS (
    UPDATE provider_inbox AS inbox
    SET ack_pending = false, acked_at = clock_timestamp()
    WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2
      AND inbox.provider_response_id = ANY($3) AND inbox.ack_pending
      AND EXISTS (
          SELECT 1 FROM provider_response_reservations AS reservation
          WHERE reservation.tenant_id = inbox.tenant_id
            AND reservation.connection_id = inbox.connection_id
            AND reservation.provider_response_id = inbox.provider_response_id
            AND reservation.disposition = 'inbox' AND NOT reservation.conflicted
      )
    RETURNING inbox.provider_response_id
), updated_rejected AS (
    UPDATE provider_rejected_responses AS rejected
    SET ack_pending = false, acked_at = clock_timestamp()
    WHERE rejected.tenant_id = $1 AND rejected.connection_id = $2
      AND rejected.provider_response_id = ANY($3)
      AND rejected.ack_pending AND NOT rejected.conflicted
      AND EXISTS (
          SELECT 1 FROM provider_response_reservations AS reservation
          WHERE reservation.tenant_id = rejected.tenant_id
            AND reservation.connection_id = rejected.connection_id
            AND reservation.provider_response_id = rejected.provider_response_id
            AND reservation.disposition = 'rejected' AND NOT reservation.conflicted
      )
    RETURNING rejected.provider_response_id
), updated AS (
    SELECT provider_response_id FROM updated_inbox
    UNION ALL
    SELECT provider_response_id FROM updated_rejected
)
SELECT count(*) FROM updated`

const listPendingProviderACKsSQL = `/* op:list_pending_provider_acks */
WITH locked_lease AS MATERIALIZED (
    SELECT connection_id
    FROM connection_leases
    WHERE tenant_id = $1 AND connection_id = $2
      AND owner_id = $3 AND fencing_token = $4
      AND expires_at > clock_timestamp()
    FOR UPDATE
), inbox_pending AS MATERIALIZED (
    SELECT inbox.provider_response_id, inbox.received_at, inbox.inbox_id
    FROM provider_inbox AS inbox
    JOIN locked_lease ON locked_lease.connection_id = inbox.connection_id
    JOIN provider_response_reservations AS reservation
      ON reservation.tenant_id = inbox.tenant_id
     AND reservation.connection_id = inbox.connection_id
     AND reservation.provider_response_id = inbox.provider_response_id
     AND reservation.disposition = 'inbox' AND NOT reservation.conflicted
    WHERE inbox.tenant_id = $1 AND inbox.connection_id = $2 AND inbox.ack_pending
	ORDER BY inbox.received_at, inbox.inbox_id
	LIMIT $5
), rejected_pending AS MATERIALIZED (
    SELECT rejected.provider_response_id, rejected.first_seen_at AS received_at,
           'rejected:' || rejected.provider_response_id AS inbox_id
    FROM provider_rejected_responses AS rejected
    JOIN locked_lease ON locked_lease.connection_id = rejected.connection_id
    JOIN provider_response_reservations AS reservation
      ON reservation.tenant_id = rejected.tenant_id
     AND reservation.connection_id = rejected.connection_id
     AND reservation.provider_response_id = rejected.provider_response_id
     AND reservation.disposition = 'rejected' AND NOT reservation.conflicted
    WHERE rejected.tenant_id = $1 AND rejected.connection_id = $2
      AND rejected.ack_pending AND NOT rejected.conflicted
    ORDER BY rejected.first_seen_at, rejected.provider_response_id
    LIMIT $5
), pending AS MATERIALIZED (
    SELECT candidates.provider_response_id, candidates.received_at, candidates.inbox_id
    FROM (
        SELECT provider_response_id, received_at, inbox_id FROM inbox_pending
        UNION ALL
        SELECT provider_response_id, received_at, inbox_id FROM rejected_pending
    ) AS candidates
    ORDER BY candidates.received_at, candidates.inbox_id
    LIMIT $5
)
SELECT COALESCE(ARRAY(
           SELECT provider_response_id FROM pending
           ORDER BY received_at, inbox_id LIMIT $5
       ), ARRAY[]::text[]),
       EXISTS (SELECT 1 FROM locked_lease)`

func (repository *Repository) ListPendingProviderACKsFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken uint64, limit int) ([]string, error) {
	if tenantID == "" || connectionID == "" || ownerID == "" || fencingToken == 0 || limit < 1 || limit > 256 {
		return nil, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]string, error) {
		var ids pq.StringArray
		var owned bool
		err := tx.QueryRowContext(ctx, listPendingProviderACKsSQL, string(tenantID), string(connectionID), ownerID, fencingToken, limit).Scan(&ids, &owned)
		if err != nil {
			return nil, fmt.Errorf("list pending provider ACKs: %w", err)
		}
		if !owned {
			return nil, ErrConnectionLeaseLost
		}
		for _, id := range ids {
			if !domain.ValidProviderResponseID(id) {
				// A row that predates the canonical response-ID constraint is
				// provider-local durable corruption. Keep it distinct from a query
				// or database failure so one legacy tenant cannot stop the gateway.
				return nil, errors.Join(ingress.ErrInvalidProviderResponseID, domain.ErrInvalidIdentifier)
			}
		}
		return append([]string(nil), ids...), nil
	})
}

func (repository *Repository) MarkProviderACKedFenced(ctx context.Context, tenantID domain.TenantID, connectionID domain.ConnectionID, ownerID string, fencingToken uint64, responseIDs []string) (bool, error) {
	if tenantID == "" || connectionID == "" || ownerID == "" || fencingToken == 0 || len(responseIDs) == 0 || len(responseIDs) > 256 {
		return false, domain.ErrInvalidIdentifier
	}
	seen := make(map[string]struct{}, len(responseIDs))
	for _, id := range responseIDs {
		if !domain.ValidProviderResponseID(id) {
			return false, domain.ErrInvalidIdentifier
		}
		if _, exists := seen[id]; exists {
			return false, domain.ErrInvalidIdentifier
		}
		seen[id] = struct{}{}
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		var owned bool
		err := tx.QueryRowContext(ctx, markProviderACKedSQL,
			string(tenantID), string(connectionID), ownerID, fencingToken, pq.Array(responseIDs),
		).Scan(&owned)
		if err != nil {
			return false, fmt.Errorf("mark provider ACKed: %w", err)
		}
		return owned, nil
	})
}

// CoordinateProviderACKsFenced holds the connection, live lease, exact
// reservation, and ACK-row locks across one bounded provider callback. This is
// intentionally stricter than a preflight check: a conflicting envelope on
// any replica either commits first and is filtered, or waits until the
// provider result and local ACK state commit atomically.
func (repository *Repository) CoordinateProviderACKsFenced(
	ctx context.Context,
	tenantID domain.TenantID,
	connectionID domain.ConnectionID,
	ownerID string,
	fencingToken uint64,
	leaseTTL time.Duration,
	responseIDs []string,
	send func(context.Context, []string) error,
) (ingress.ACKCoordinationResult, error) {
	var result ingress.ACKCoordinationResult
	if tenantID == "" || connectionID == "" || ownerID == "" || fencingToken == 0 ||
		leaseTTL <= ProviderACKCoordinationHardTimeout || leaseTTL > maximumLeaseTTL ||
		len(responseIDs) == 0 || len(responseIDs) > 256 || send == nil {
		return result, domain.ErrInvalidIdentifier
	}
	seen := make(map[string]struct{}, len(responseIDs))
	for _, id := range responseIDs {
		if !domain.ValidProviderResponseID(id) {
			return result, domain.ErrInvalidIdentifier
		}
		if _, duplicate := seen[id]; duplicate {
			return result, domain.ErrInvalidIdentifier
		}
		seen[id] = struct{}{}
	}
	// This method owns the distributed lock-holding invariant. Even a direct
	// internal caller with an unbounded parent context cannot hold the
	// connection and lease rows beyond the hard provider ACK budget; a shorter
	// caller deadline still wins.
	coordinationStarted := time.Now()
	coordinationCtx, cancel := context.WithTimeout(ctx, ProviderACKCoordinationHardTimeout)
	defer cancel()
	tx, err := repository.db.BeginTx(coordinationCtx, nil)
	if err != nil {
		return result, fmt.Errorf("begin provider ACK coordination: %w", err)
	}
	// Commit renders this harmless. On a callback panic or any future early
	// return it is the last-resort release for the connection/lease locks held
	// across the bounded provider request.
	defer func() { _ = tx.Rollback() }()
	rollback := func() { _ = tx.Rollback() }
	if _, err = tx.ExecContext(coordinationCtx, tenantContextSQL, string(tenantID)); err != nil {
		rollback()
		return result, fmt.Errorf("set provider ACK tenant context: %w", err)
	}
	var admitted pq.StringArray
	var owned bool
	if err = tx.QueryRowContext(coordinationCtx, coordinateProviderACKBatchSQL,
		string(tenantID), string(connectionID), ownerID, fencingToken, pq.Array(responseIDs), leaseTTL.Microseconds(),
	).Scan(&admitted, &owned); err != nil {
		rollback()
		return result, fmt.Errorf("lock provider ACK batch: %w", err)
	}
	if !owned {
		rollback()
		return result, ErrConnectionLeaseLost
	}
	result.AdmittedIDs = append([]string(nil), admitted...)
	if len(result.AdmittedIDs) == 0 {
		if err = tx.Commit(); err != nil {
			rollback()
			return result, fmt.Errorf("commit empty provider ACK admission: %w", err)
		}
		return result, nil
	}
	wireCtx, cancelWire := context.WithDeadline(coordinationCtx, coordinationStarted.Add(providerACKWireHardTimeout))
	func() {
		defer cancelWire()
		err = send(wireCtx, append([]string(nil), result.AdmittedIDs...))
	}()
	if err != nil {
		rollback()
		result.ProviderError = err
		return result, nil
	}
	var updated int
	if err = tx.QueryRowContext(coordinationCtx, completeCoordinatedProviderACKSQL,
		string(tenantID), string(connectionID), pq.Array(result.AdmittedIDs),
	).Scan(&updated); err != nil {
		rollback()
		return result, fmt.Errorf("persist coordinated provider ACK: %w", err)
	}
	if updated != len(result.AdmittedIDs) {
		rollback()
		return result, errors.New("coordinated provider ACK rows changed while locked")
	}
	if err = tx.Commit(); err != nil {
		rollback()
		return result, fmt.Errorf("commit coordinated provider ACK: %w", err)
	}
	return result, nil
}

const sha256Size = 32

var _ messaging.Store = (*Repository)(nil)
var _ messaging.DispatchStore = (*Repository)(nil)
