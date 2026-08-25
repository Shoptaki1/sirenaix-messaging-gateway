package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	gatewaykafka "go.mau.fi/mautrix-gmessages/internal/gateway/kafka"
	"go.mau.fi/mautrix-gmessages/internal/gateway/webhook"
)

const createWebhookEndpointSQL = `/* op:create_webhook_endpoint */
INSERT INTO webhook_endpoints (
    tenant_id, endpoint_id, destination_url, key_id,
    secret_ciphertext, secret_wrapped_dek, secret_nonce,
    secret_key_id, secret_key_version, active, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, true, $10)`

const lockWebhookEndpointQuotaSQL = `/* op:lock_webhook_endpoint_quota */
SELECT tenant_id FROM tenants WHERE tenant_id = $1 FOR UPDATE`

const checkWebhookEndpointQuotaSQL = `/* op:check_webhook_endpoint_quota */
SELECT count(*) FROM webhook_endpoints WHERE tenant_id = $1 AND active`

func (repository *Repository) CreateEndpoint(ctx context.Context, record webhook.EndpointRecord, maxEndpoints int) error {
	endpoint := record.Endpoint
	if endpoint.TenantID == "" || endpoint.ID == "" || endpoint.Destination == "" || endpoint.KeyID == "" ||
		record.Secret.Validate() != nil || record.Secret.Provider != "webhook" || maxEndpoints < 1 || maxEndpoints > webhook.MaxEndpointsPerTenant {
		return webhook.ErrInvalidEndpoint
	}
	return inTenantExec(ctx, repository, endpoint.TenantID, func(tx transaction) error {
		if _, err := tx.ExecContext(ctx, lockWebhookEndpointQuotaSQL, string(endpoint.TenantID)); err != nil {
			return fmt.Errorf("lock webhook endpoint quota: %w", err)
		}
		var count int
		if err := tx.QueryRowContext(ctx, checkWebhookEndpointQuotaSQL, string(endpoint.TenantID)).Scan(&count); err != nil {
			return fmt.Errorf("check webhook endpoint quota: %w", err)
		}
		if count >= maxEndpoints {
			return webhook.ErrEndpointQuotaExceeded
		}
		_, err := tx.ExecContext(ctx, createWebhookEndpointSQL,
			string(endpoint.TenantID), endpoint.ID, endpoint.Destination, endpoint.KeyID,
			record.Secret.Ciphertext, record.Secret.WrappedDEK, record.Secret.Nonce,
			record.Secret.KeyID, record.Secret.KeyVersion, endpoint.CreatedAt,
		)
		return err
	})
}

const rotateWebhookEndpointSQL = `/* op:rotate_webhook_endpoint */
UPDATE webhook_endpoints
SET previous_key_id = key_id,
    previous_secret_ciphertext = secret_ciphertext,
    previous_secret_wrapped_dek = secret_wrapped_dek,
    previous_secret_nonce = secret_nonce,
    previous_secret_key_id = secret_key_id,
    previous_secret_key_version = secret_key_version,
    previous_valid_until = $10,
    key_id = $3, secret_ciphertext = $4, secret_wrapped_dek = $5,
    secret_nonce = $6, secret_key_id = $7, secret_key_version = $8
WHERE tenant_id = $1 AND endpoint_id = $2 AND active
  AND $9 = 'webhook'`

func (repository *Repository) RotateEndpoint(ctx context.Context, rotation webhook.EndpointRotation) error {
	if rotation.TenantID == "" || rotation.EndpointID == "" || rotation.KeyID == "" ||
		rotation.Secret.Validate() != nil || rotation.Secret.Provider != "webhook" || rotation.PreviousValidUntil.IsZero() {
		return webhook.ErrInvalidEndpoint
	}
	return inTenantExec(ctx, repository, rotation.TenantID, func(tx transaction) error {
		result, err := tx.ExecContext(ctx, rotateWebhookEndpointSQL,
			string(rotation.TenantID), rotation.EndpointID, rotation.KeyID,
			rotation.Secret.Ciphertext, rotation.Secret.WrappedDEK, rotation.Secret.Nonce,
			rotation.Secret.KeyID, rotation.Secret.KeyVersion, rotation.Secret.Provider, rotation.PreviousValidUntil,
		)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return webhook.ErrInvalidEndpoint
		}
		return nil
	})
}

const listWebhookEndpointsSQL = `/* op:list_webhook_endpoints */
SELECT endpoint_id, destination_url, key_id, active, created_at
FROM webhook_endpoints
WHERE tenant_id = $1 AND endpoint_id > $2
ORDER BY endpoint_id
LIMIT $3`

func (repository *Repository) ListEndpoints(ctx context.Context, tenantID domain.TenantID, options webhook.EndpointListOptions) (webhook.EndpointPage, error) {
	if tenantID == "" || len(options.After) > 256 || options.Limit < 1 || options.Limit > 200 {
		return webhook.EndpointPage{}, webhook.ErrInvalidEndpoint
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (webhook.EndpointPage, error) {
		rows, err := tx.QueryContext(ctx, listWebhookEndpointsSQL, string(tenantID), options.After, options.Limit+1)
		if err != nil {
			return webhook.EndpointPage{}, err
		}
		defer rows.Close()
		endpoints := make([]webhook.Endpoint, 0, options.Limit+1)
		for rows.Next() {
			var endpoint webhook.Endpoint
			if err = rows.Scan(&endpoint.ID, &endpoint.Destination, &endpoint.KeyID, &endpoint.Active, &endpoint.CreatedAt); err != nil {
				return webhook.EndpointPage{}, err
			}
			endpoint.TenantID = tenantID
			endpoints = append(endpoints, endpoint)
		}
		if err = rows.Err(); err != nil {
			return webhook.EndpointPage{}, err
		}
		page := webhook.EndpointPage{Endpoints: endpoints}
		if len(endpoints) > options.Limit {
			page.Endpoints = endpoints[:options.Limit]
			page.NextCursor = page.Endpoints[len(page.Endpoints)-1].ID
		}
		return page, nil
	})
}

const deleteWebhookEndpointSQL = `/* op:delete_webhook_endpoint */
WITH locked_endpoint AS MATERIALIZED (
    SELECT endpoint_id, active, generation FROM webhook_endpoints
    WHERE tenant_id = $1 AND endpoint_id = $2
    FOR UPDATE
), locked_deliveries AS MATERIALIZED (
    SELECT delivery.delivery_id, delivery.event_id, delivery.canonical_body, delivery.attempt_count
    FROM webhook_deliveries AS delivery
    JOIN locked_endpoint AS endpoint ON endpoint.endpoint_id = delivery.endpoint_id
    WHERE delivery.tenant_id = $1 AND delivery.state IN ('pending', 'delivering')
      AND (delivery.http_started_at IS NULL OR delivery.claim_expires_at <= clock_timestamp())
    FOR UPDATE OF delivery
), finished_attempts AS (
    UPDATE webhook_attempts AS attempt
    SET finished_at = clock_timestamp(), safe_error = 'endpoint revoked'
    FROM locked_deliveries AS delivery
    WHERE attempt.tenant_id = $1 AND attempt.delivery_id = delivery.delivery_id
      AND attempt.attempt_number = delivery.attempt_count AND attempt.finished_at IS NULL
    RETURNING attempt.delivery_id
), revoked_deliveries AS (
    UPDATE webhook_deliveries AS delivery
    SET state = 'dead', claimed_by = NULL, claim_expires_at = NULL,
        claim_token = delivery.claim_token + 1, completed_at = clock_timestamp()
    FROM locked_deliveries AS locked
    WHERE delivery.tenant_id = $1 AND delivery.delivery_id = locked.delivery_id
    RETURNING delivery.delivery_id, delivery.event_id, delivery.canonical_body
), dead_letters AS (
    INSERT INTO webhook_dlq (tenant_id, dlq_id, delivery_id, event_id, canonical_body, safe_error)
    SELECT $1, delivery_id || ':revoked', delivery_id, event_id, canonical_body, 'endpoint revoked'
    FROM revoked_deliveries
    ON CONFLICT (tenant_id, delivery_id) DO NOTHING
    RETURNING delivery_id
), revoked_endpoint AS (
    UPDATE webhook_endpoints AS endpoint
    SET active = false,
        generation = CASE WHEN endpoint.active THEN endpoint.generation + 1 ELSE endpoint.generation END
    FROM locked_endpoint
    WHERE endpoint.tenant_id = $1 AND endpoint.endpoint_id = locked_endpoint.endpoint_id
    RETURNING endpoint.endpoint_id
), on_wire AS (
    SELECT delivery.delivery_id
    FROM webhook_deliveries AS delivery
    JOIN revoked_endpoint AS endpoint ON endpoint.endpoint_id = delivery.endpoint_id
    WHERE delivery.tenant_id = $1 AND delivery.state = 'delivering'
      AND delivery.http_started_at IS NOT NULL
      AND delivery.claim_expires_at > clock_timestamp()
)
SELECT EXISTS (SELECT 1 FROM revoked_endpoint), EXISTS (SELECT 1 FROM on_wire)`

func (repository *Repository) DeleteEndpoint(ctx context.Context, tenantID domain.TenantID, endpointID string) (webhook.DeleteResult, error) {
	if tenantID == "" || endpointID == "" {
		return webhook.DeleteResult{}, webhook.ErrInvalidEndpoint
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (webhook.DeleteResult, error) {
		var deleted, deleting bool
		if err := tx.QueryRowContext(ctx, deleteWebhookEndpointSQL, string(tenantID), endpointID).Scan(&deleted, &deleting); err != nil {
			return webhook.DeleteResult{}, err
		}
		if !deleted {
			return webhook.DeleteResult{}, webhook.ErrInvalidEndpoint
		}
		return webhook.DeleteResult{Deleting: deleting}, nil
	})
}

const replayWebhookDLQSQL = `/* op:replay_webhook_dlq */
WITH locked_endpoint AS MATERIALIZED (
    SELECT endpoint.endpoint_id, endpoint.active
    FROM webhook_dlq AS dlq
    JOIN webhook_deliveries AS delivery
      ON delivery.tenant_id = dlq.tenant_id AND delivery.delivery_id = dlq.delivery_id
    JOIN webhook_endpoints AS endpoint
      ON endpoint.tenant_id = delivery.tenant_id AND endpoint.endpoint_id = delivery.endpoint_id
    WHERE dlq.tenant_id = $1 AND dlq.dlq_id = $2 AND dlq.replayed_at IS NULL AND endpoint.active
    FOR UPDATE OF endpoint
), locked_dlq AS MATERIALIZED (
    SELECT delivery_id
    FROM webhook_dlq AS dlq
    JOIN locked_endpoint ON true
    WHERE dlq.tenant_id = $1 AND dlq.dlq_id = $2 AND dlq.replayed_at IS NULL
    FOR UPDATE OF dlq
), replayed_delivery AS (
    UPDATE webhook_deliveries AS delivery
    SET state = 'pending', cycle_attempt_count = 0,
        available_at = clock_timestamp(), claimed_by = NULL,
        claim_expires_at = NULL, claim_token = delivery.claim_token + 1, completed_at = NULL
    FROM locked_dlq
    WHERE delivery.tenant_id = $1 AND delivery.delivery_id = locked_dlq.delivery_id
    RETURNING delivery.delivery_id
), marked_dlq AS (
    UPDATE webhook_dlq AS dlq
    SET replayed_at = clock_timestamp()
    FROM replayed_delivery
    WHERE dlq.tenant_id = $1 AND dlq.dlq_id = $2
    RETURNING dlq.dlq_id
)
SELECT EXISTS (SELECT 1 FROM marked_dlq)`

func (repository *Repository) ReplayDLQ(ctx context.Context, tenantID domain.TenantID, dlqID string) error {
	if tenantID == "" || dlqID == "" {
		return webhook.ErrInvalidEndpoint
	}
	_, err := inTenant(ctx, repository, tenantID, func(tx transaction) (struct{}, error) {
		var replayed bool
		if err := tx.QueryRowContext(ctx, replayWebhookDLQSQL, string(tenantID), dlqID).Scan(&replayed); err != nil {
			return struct{}{}, err
		}
		if !replayed {
			return struct{}{}, webhook.ErrInvalidEndpoint
		}
		return struct{}{}, nil
	})
	return err
}

const purgeExpiredWebhookSecretsSQL = `/* op:purge_expired_webhook_secrets */
UPDATE webhook_endpoints
SET previous_key_id = NULL, previous_secret_ciphertext = NULL,
    previous_secret_wrapped_dek = NULL, previous_secret_nonce = NULL,
    previous_secret_key_id = NULL, previous_secret_key_version = NULL,
    previous_valid_until = NULL
WHERE tenant_id = $1 AND previous_valid_until <= clock_timestamp()`

func (repository *Repository) PurgeExpiredSecrets(ctx context.Context, tenantID domain.TenantID) error {
	if tenantID == "" {
		return webhook.ErrInvalidWorker
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		_, err := tx.ExecContext(ctx, purgeExpiredWebhookSecretsSQL, string(tenantID))
		return err
	})
}

const claimWebhookDeliveriesSQL = `/* op:claim_webhook_deliveries */
WITH purged_previous AS MATERIALIZED (
    UPDATE webhook_endpoints
    SET previous_key_id = NULL, previous_secret_ciphertext = NULL,
        previous_secret_wrapped_dek = NULL, previous_secret_nonce = NULL,
        previous_secret_key_id = NULL, previous_secret_key_version = NULL,
        previous_valid_until = NULL
WHERE tenant_id = $1 AND previous_valid_until <= clock_timestamp()
    RETURNING endpoint_id
), locked_outbox AS MATERIALIZED (
    SELECT outbox.outbox_id, outbox.event_id
    FROM event_outbox AS outbox
    WHERE outbox.tenant_id = $1 AND outbox.destination = 'webhook'
      AND outbox.published_at IS NULL AND outbox.available_at <= clock_timestamp()
      AND (outbox.claimed_by IS NULL OR outbox.claim_expires_at <= clock_timestamp())
    ORDER BY outbox.available_at, outbox.outbox_id
    FOR UPDATE OF outbox SKIP LOCKED
    LIMIT $3
), candidate_pairs AS MATERIALIZED (
    SELECT event.tenant_id, event.event_id, event.canonical_body,
           endpoint.endpoint_id, endpoint.generation
    FROM locked_outbox
    JOIN gateway_events AS event
      ON event.tenant_id = $1 AND event.event_id = locked_outbox.event_id
    JOIN webhook_endpoints AS endpoint
      ON endpoint.tenant_id = $1 AND endpoint.active
    WHERE NOT EXISTS (
        SELECT 1 FROM webhook_deliveries AS existing
        WHERE existing.tenant_id = $1 AND existing.endpoint_id = endpoint.endpoint_id
          AND existing.event_id = event.event_id
    )
    ORDER BY locked_outbox.outbox_id, endpoint.endpoint_id
    LIMIT $3
    FOR SHARE OF endpoint
), materialized AS MATERIALIZED (
    INSERT INTO webhook_deliveries (
        tenant_id, delivery_id, endpoint_id, event_id, canonical_body, endpoint_generation
    )
    SELECT pair.tenant_id, pair.endpoint_id || ':' || pair.event_id,
           pair.endpoint_id, pair.event_id, pair.canonical_body, pair.generation
    FROM candidate_pairs AS pair
    ON CONFLICT (tenant_id, endpoint_id, event_id) DO NOTHING
    RETURNING endpoint_id, event_id, delivery_id
), completed_events AS MATERIALIZED (
    SELECT locked_outbox.event_id
    FROM locked_outbox
    WHERE NOT EXISTS (
        SELECT 1
        FROM webhook_endpoints AS endpoint
        WHERE endpoint.tenant_id = $1 AND endpoint.active
          AND NOT EXISTS (
              SELECT 1 FROM webhook_deliveries AS existing
              WHERE existing.tenant_id = $1 AND existing.endpoint_id = endpoint.endpoint_id
                AND existing.event_id = locked_outbox.event_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM materialized
              WHERE materialized.endpoint_id = endpoint.endpoint_id
                AND materialized.event_id = locked_outbox.event_id
          )
    )
), closed_outbox AS MATERIALIZED (
    UPDATE event_outbox AS outbox
    SET published_at = clock_timestamp(), claimed_by = NULL, claim_expires_at = NULL
    WHERE outbox.tenant_id = $1 AND outbox.destination = 'webhook'
      AND outbox.event_id IN (SELECT event_id FROM completed_events)
    RETURNING outbox_id
), candidates AS MATERIALIZED (
    SELECT delivery.delivery_id, endpoint.generation
    FROM webhook_deliveries AS delivery
    JOIN webhook_endpoints AS endpoint
      ON endpoint.tenant_id = delivery.tenant_id AND endpoint.endpoint_id = delivery.endpoint_id
     AND endpoint.active
    CROSS JOIN (SELECT count(*) FROM purged_previous) AS purge_barrier
    CROSS JOIN (SELECT count(*) FROM materialized) AS materialization_barrier
    CROSS JOIN (SELECT count(*) FROM closed_outbox) AS close_barrier
    WHERE delivery.tenant_id = $1
      AND delivery.state IN ('pending', 'delivering')
      AND delivery.available_at <= clock_timestamp()
      AND (delivery.claimed_by IS NULL OR delivery.claim_expires_at <= clock_timestamp())
    ORDER BY delivery.available_at, delivery.delivery_id
    FOR UPDATE OF delivery SKIP LOCKED
    LIMIT $3
), claimed AS (
    UPDATE webhook_deliveries AS delivery
    SET state = 'delivering', claimed_by = $2,
        claim_expires_at = clock_timestamp() + interval '30 seconds',
        claim_token = delivery.claim_token + 1,
        endpoint_generation = candidates.generation,
        http_started_at = NULL,
        attempt_count = delivery.attempt_count + 1,
        cycle_attempt_count = delivery.cycle_attempt_count + 1
    FROM candidates
    WHERE delivery.tenant_id = $1 AND delivery.delivery_id = candidates.delivery_id
    RETURNING delivery.tenant_id, delivery.delivery_id, delivery.endpoint_id,
              delivery.event_id, delivery.canonical_body,
              delivery.attempt_count, delivery.cycle_attempt_count,
              delivery.claimed_by, delivery.claim_token, delivery.endpoint_generation
), attempts AS (
    INSERT INTO webhook_attempts (tenant_id, attempt_id, delivery_id, attempt_number)
    SELECT claimed.tenant_id, claimed.delivery_id || ':' || claimed.attempt_count,
           claimed.delivery_id, claimed.attempt_count
    FROM claimed
    RETURNING delivery_id
)
SELECT claimed.tenant_id, claimed.delivery_id, claimed.endpoint_id, claimed.event_id,
       endpoint.destination_url, endpoint.key_id, claimed.canonical_body,
       claimed.cycle_attempt_count, claimed.claimed_by, claimed.claim_token,
       claimed.endpoint_generation,
       0::bigint, 1, 'webhook', endpoint.secret_ciphertext,
       endpoint.secret_wrapped_dek, endpoint.secret_nonce,
       endpoint.secret_key_id, endpoint.secret_key_version
FROM claimed
JOIN attempts ON attempts.delivery_id = claimed.delivery_id
JOIN webhook_endpoints AS endpoint
  ON endpoint.tenant_id = claimed.tenant_id AND endpoint.endpoint_id = claimed.endpoint_id
 AND endpoint.active`

func (repository *Repository) Claim(ctx context.Context, tenantID domain.TenantID, ownerID string, limit int) ([]webhook.Delivery, error) {
	if tenantID == "" || ownerID == "" || len(ownerID) > 256 || limit < 1 || limit > 256 {
		return nil, webhook.ErrInvalidWorker
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]webhook.Delivery, error) {
		rows, err := tx.QueryContext(ctx, claimWebhookDeliveriesSQL, string(tenantID), ownerID, limit)
		if err != nil {
			return nil, fmt.Errorf("claim webhook deliveries: %w", err)
		}
		defer rows.Close()
		deliveries := make([]webhook.Delivery, 0, limit)
		for rows.Next() {
			var delivery webhook.Delivery
			var tenant string
			if err = rows.Scan(
				&tenant, &delivery.DeliveryID, &delivery.EndpointID, &delivery.EventID,
				&delivery.Destination, &delivery.KeyID, &delivery.CanonicalBody, &delivery.Attempt,
				&delivery.OwnerID, &delivery.ClaimToken, &delivery.EndpointGeneration,
				&delivery.Secret.Revision, &delivery.Secret.Version, &delivery.Secret.Provider,
				&delivery.Secret.Ciphertext, &delivery.Secret.WrappedDEK, &delivery.Secret.Nonce,
				&delivery.Secret.KeyID, &delivery.Secret.KeyVersion,
			); err != nil {
				return nil, fmt.Errorf("scan webhook delivery: %w", err)
			}
			delivery.TenantID = domain.TenantID(tenant)
			deliveries = append(deliveries, delivery)
		}
		if err = rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate webhook deliveries: %w", err)
		}
		return deliveries, nil
	})
}

const admitWebhookClaimSQL = `/* op:admit_webhook_claim */
WITH locked_endpoint AS MATERIALIZED (
    SELECT endpoint_id
    FROM webhook_endpoints AS endpoint
    WHERE endpoint.tenant_id = $1 AND endpoint.endpoint_id = $3
      AND endpoint.active AND endpoint.generation = $6
    FOR SHARE OF endpoint
)
UPDATE webhook_deliveries AS delivery
SET http_started_at = clock_timestamp()
FROM locked_endpoint
WHERE delivery.tenant_id = $1 AND delivery.delivery_id = $2
  AND delivery.endpoint_id = locked_endpoint.endpoint_id
  AND delivery.claimed_by = $4 AND delivery.claim_token = $5
  AND delivery.endpoint_generation = $6
  AND delivery.state = 'delivering' AND delivery.http_started_at IS NULL
  AND delivery.claim_expires_at > clock_timestamp()`

func (repository *Repository) AdmitClaim(ctx context.Context, delivery webhook.Delivery) (bool, error) {
	if delivery.TenantID == "" || delivery.DeliveryID == "" || delivery.EndpointID == "" || delivery.OwnerID == "" ||
		len(delivery.OwnerID) > 256 || delivery.ClaimToken == 0 || delivery.EndpointGeneration == 0 {
		return false, webhook.ErrInvalidWorker
	}
	return inTenant(ctx, repository, delivery.TenantID, func(tx transaction) (bool, error) {
		result, err := tx.ExecContext(ctx, admitWebhookClaimSQL, string(delivery.TenantID), delivery.DeliveryID,
			delivery.EndpointID, delivery.OwnerID, delivery.ClaimToken, delivery.EndpointGeneration)
		if err != nil {
			return false, err
		}
		count, err := result.RowsAffected()
		return err == nil && count == 1, err
	})
}

const renewWebhookClaimSQL = `/* op:renew_webhook_claim */
UPDATE webhook_deliveries AS delivery
SET claim_expires_at = clock_timestamp() + interval '30 seconds'
WHERE delivery.tenant_id = $1 AND delivery.delivery_id = $2
  AND delivery.claimed_by = $3 AND delivery.claim_token = $4
  AND delivery.state = 'delivering' AND delivery.claim_expires_at > clock_timestamp()
  AND delivery.http_started_at IS NOT NULL`

func (repository *Repository) RenewClaim(ctx context.Context, tenantID domain.TenantID, deliveryID, ownerID string, claimToken uint64) (bool, error) {
	if tenantID == "" || deliveryID == "" || ownerID == "" || len(ownerID) > 256 || claimToken == 0 {
		return false, webhook.ErrInvalidWorker
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (bool, error) {
		result, err := tx.ExecContext(ctx, renewWebhookClaimSQL, string(tenantID), deliveryID, ownerID, claimToken)
		if err != nil {
			return false, err
		}
		count, err := result.RowsAffected()
		return err == nil && count == 1, err
	})
}

const completeWebhookAttemptSQL = `/* op:complete_webhook_attempt */
WITH locked_endpoint AS MATERIALIZED (
    SELECT endpoint.endpoint_id
    FROM webhook_endpoints AS endpoint
    JOIN webhook_deliveries AS delivery
      ON delivery.tenant_id = endpoint.tenant_id AND delivery.endpoint_id = endpoint.endpoint_id
    WHERE delivery.tenant_id = $1 AND delivery.delivery_id = $2
    FOR UPDATE OF endpoint
), locked_delivery AS MATERIALIZED (
    SELECT delivery_id, event_id, canonical_body, attempt_count, locked_endpoint.active AS endpoint_active
    FROM webhook_deliveries AS delivery
    JOIN locked_endpoint ON true
    WHERE delivery.tenant_id = $1 AND delivery.delivery_id = $2
      AND delivery.state = 'delivering' AND delivery.cycle_attempt_count = $3
      AND delivery.claimed_by = $4 AND delivery.claim_token = $5
    FOR UPDATE OF delivery
), finished_attempt AS (
    UPDATE webhook_attempts AS attempt
    SET finished_at = clock_timestamp(), status_code = NULLIF($6, 0),
        safe_error = CASE WHEN NOT locked_delivery.endpoint_active THEN 'endpoint revoked' ELSE $7 END
    FROM locked_delivery
    WHERE attempt.tenant_id = $1 AND attempt.delivery_id = locked_delivery.delivery_id
      AND attempt.attempt_number = locked_delivery.attempt_count
    RETURNING attempt.delivery_id
), updated_delivery AS (
    UPDATE webhook_deliveries AS delivery
    SET state = CASE WHEN NOT locked_delivery.endpoint_active THEN 'dead' WHEN $8 THEN 'succeeded' WHEN $9 THEN 'dead' ELSE 'pending' END,
        available_at = CASE WHEN NOT locked_delivery.endpoint_active OR $8 OR $9 THEN delivery.available_at ELSE $10 END,
        claimed_by = NULL, claim_expires_at = NULL,
        completed_at = CASE WHEN NOT locked_delivery.endpoint_active OR $8 OR $9 THEN clock_timestamp() ELSE NULL END
    FROM finished_attempt
    JOIN locked_delivery ON locked_delivery.delivery_id = finished_attempt.delivery_id
    WHERE delivery.tenant_id = $1 AND delivery.delivery_id = finished_attempt.delivery_id
    RETURNING delivery.delivery_id, delivery.event_id, delivery.canonical_body, locked_delivery.endpoint_active
), dead_letter AS (
    INSERT INTO webhook_dlq (tenant_id, dlq_id, delivery_id, event_id, canonical_body, safe_error)
    SELECT $1, $11, delivery_id, event_id, canonical_body,
           CASE WHEN NOT endpoint_active THEN 'endpoint revoked' ELSE $7 END
    FROM updated_delivery WHERE $9 OR NOT endpoint_active
    ON CONFLICT (tenant_id, delivery_id) DO NOTHING
    RETURNING dlq_id
)
SELECT EXISTS (SELECT 1 FROM updated_delivery)`

func (repository *Repository) CompleteAttempt(ctx context.Context, result webhook.AttemptResult) error {
	if result.TenantID == "" || result.DeliveryID == "" || result.Attempt < 1 || result.OwnerID == "" || len(result.OwnerID) > 256 || result.ClaimToken == 0 || len(result.SafeError) > 1024 {
		return webhook.ErrInvalidWorker
	}
	_, err := inTenant(ctx, repository, result.TenantID, func(tx transaction) (struct{}, error) {
		var completed bool
		next := result.NextAvailableAt.UTC()
		if next.IsZero() {
			next = time.Now().UTC()
		}
		err := tx.QueryRowContext(ctx, completeWebhookAttemptSQL,
			string(result.TenantID), result.DeliveryID, result.Attempt, result.OwnerID, result.ClaimToken, result.StatusCode,
			result.SafeError, result.Succeeded, result.Dead, next, repository.newID(),
		).Scan(&completed)
		if err != nil {
			return struct{}{}, fmt.Errorf("complete webhook attempt: %w", err)
		}
		if !completed {
			return struct{}{}, errors.New("webhook delivery claim lost")
		}
		return struct{}{}, nil
	})
	return err
}

const claimKafkaEventsSQL = `/* op:claim_kafka_events */
WITH locked_outbox AS MATERIALIZED (
    SELECT outbox.outbox_id, outbox.event_id
    FROM event_outbox AS outbox
    WHERE outbox.tenant_id = $1 AND outbox.destination = 'kafka'
      AND outbox.published_at IS NULL AND outbox.available_at <= clock_timestamp()
      AND (outbox.claimed_by IS NULL OR outbox.claim_expires_at <= clock_timestamp())
    ORDER BY outbox.available_at, outbox.outbox_id
    FOR UPDATE OF outbox SKIP LOCKED
    LIMIT $3
), materialized AS MATERIALIZED (
    INSERT INTO kafka_event_deliveries (tenant_id, delivery_id, event_id, topic, partition_key)
    SELECT event.tenant_id, event.event_id || ':kafka', event.event_id, $4,
           event.tenant_id || ':' || COALESCE(event.connection_id, '') || ':' || event.conversation_id
    FROM locked_outbox
    JOIN gateway_events AS event ON event.tenant_id = $1 AND event.event_id = locked_outbox.event_id
    ON CONFLICT (tenant_id, event_id, topic) DO NOTHING
    RETURNING event_id
), claimed_outbox AS (
    UPDATE event_outbox AS outbox
    SET claimed_by = $2, claim_expires_at = clock_timestamp() + interval '30 seconds',
        attempt_count = outbox.attempt_count + 1
    FROM locked_outbox
    WHERE outbox.tenant_id = $1 AND outbox.outbox_id = locked_outbox.outbox_id
    RETURNING outbox.event_id
), claimed_delivery AS (
    UPDATE kafka_event_deliveries AS delivery
    SET state = 'publishing', attempt_count = delivery.attempt_count + 1
    FROM claimed_outbox
    WHERE delivery.tenant_id = $1 AND delivery.event_id = claimed_outbox.event_id AND delivery.topic = $4
    RETURNING delivery.event_id
)
SELECT event.event_id, event.tenant_id, COALESCE(event.connection_id, ''),
       event.conversation_id, event.canonical_body
FROM claimed_delivery
CROSS JOIN (SELECT count(*) FROM materialized) AS materialization_barrier
JOIN gateway_events AS event
  ON event.tenant_id = $1 AND event.event_id = claimed_delivery.event_id
ORDER BY event.event_id`

func (repository *Repository) ClaimEvents(ctx context.Context, tenantID domain.TenantID, ownerID string, limit int) ([]gatewaykafka.OutboxEvent, error) {
	if tenantID == "" || ownerID == "" || len(ownerID) > 256 || limit < 1 || limit > 256 {
		return nil, gatewaykafka.ErrInvalidOutboxWorker
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]gatewaykafka.OutboxEvent, error) {
		rows, err := tx.QueryContext(ctx, claimKafkaEventsSQL, string(tenantID), ownerID, limit, gatewaykafka.DefaultEventsTopic)
		if err != nil {
			return nil, fmt.Errorf("claim Kafka events: %w", err)
		}
		defer rows.Close()
		events := make([]gatewaykafka.OutboxEvent, 0, limit)
		for rows.Next() {
			var event gatewaykafka.OutboxEvent
			var tenant, connection string
			if err = rows.Scan(&event.EventID, &tenant, &connection, &event.ConversationID, &event.CanonicalBody); err != nil {
				return nil, fmt.Errorf("scan Kafka event: %w", err)
			}
			event.TenantID, event.ConnectionID = domain.TenantID(tenant), domain.ConnectionID(connection)
			events = append(events, event)
		}
		if err = rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate Kafka events: %w", err)
		}
		return events, nil
	})
}

const markKafkaEventPublishedSQL = `/* op:mark_kafka_event_published */
WITH marked_outbox AS (
    UPDATE event_outbox
    SET published_at = clock_timestamp(), claimed_by = NULL, claim_expires_at = NULL
    WHERE tenant_id = $1 AND event_id = $2 AND destination = 'kafka' AND published_at IS NULL
    RETURNING event_id
), marked_delivery AS (
    UPDATE kafka_event_deliveries AS delivery
    SET state = 'published', published_at = clock_timestamp()
    FROM marked_outbox
    WHERE delivery.tenant_id = $1 AND delivery.event_id = marked_outbox.event_id
    RETURNING delivery.event_id
)
SELECT count(*) FROM marked_delivery`

func (repository *Repository) MarkPublished(ctx context.Context, tenantID domain.TenantID, eventID string) error {
	if tenantID == "" || eventID == "" {
		return gatewaykafka.ErrInvalidOutboxWorker
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		_, err := tx.ExecContext(ctx, markKafkaEventPublishedSQL, string(tenantID), eventID)
		if err != nil {
			return fmt.Errorf("mark Kafka event published: %w", err)
		}
		return nil
	})
}

const insertKafkaCommandDLQSQL = `/* op:insert_kafka_command_dlq */
INSERT INTO kafka_command_dlq (
    tenant_id, dlq_id, command_id, topic, partition_id, offset_id,
    bounded_payload, safe_error, attempt_count, correlation_id
) VALUES ($1, $2, NULL, $3, $4, $5, $6, $7, 1, $8)
ON CONFLICT (tenant_id, topic, partition_id, offset_id) DO NOTHING`

func (repository *Repository) Store(ctx context.Context, record gatewaykafka.CommandDLQRecord) error {
	if record.AuthorizedTenant == "" || record.Topic == "" ||
		len(record.OriginalPayload) > 1<<20 || record.SafeError == "" || len(record.SafeError) > 1024 || len(record.CorrelationID) > 256 {
		return gatewaykafka.ErrInvalidConsumer
	}
	return inTenantExec(ctx, repository, record.AuthorizedTenant, func(tx transaction) error {
		_, err := tx.ExecContext(ctx, insertKafkaCommandDLQSQL,
			string(record.AuthorizedTenant), repository.newID(), record.Topic, record.Partition,
			record.Offset, record.OriginalPayload, record.SafeError, record.CorrelationID,
		)
		return err
	})
}

var _ webhook.Store = (*Repository)(nil)
var _ gatewaykafka.EventStore = (*Repository)(nil)
var _ gatewaykafka.CommandDLQStore = (*Repository)(nil)
