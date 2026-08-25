package postgres

import (
	"context"
	"fmt"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

type OperationalQueueDepth struct {
	Messages int64
	Media    int64
	Webhooks int64
	Kafka    int64
}

const operationalQueueDepthsSQL = `/* op:operational_queue_depths */
SELECT
    (SELECT count(*) FROM messages WHERE tenant_id = $1 AND current_state = 'queued'),
    (SELECT count(*) FROM media_fetch_jobs WHERE tenant_id = $1 AND state = 'pending'),
    (SELECT count(*) FROM webhook_deliveries WHERE tenant_id = $1 AND state = 'pending'),
    (SELECT count(*) FROM event_outbox WHERE tenant_id = $1 AND destination = 'kafka' AND published_at IS NULL)`

func (repository *Repository) OperationalQueueDepths(ctx context.Context, tenantID domain.TenantID) (OperationalQueueDepth, error) {
	return inTenant(ctx, repository, tenantID, func(tx transaction) (OperationalQueueDepth, error) {
		var depths OperationalQueueDepth
		if err := tx.QueryRowContext(ctx, operationalQueueDepthsSQL, string(tenantID)).Scan(
			&depths.Messages, &depths.Media, &depths.Webhooks, &depths.Kafka,
		); err != nil {
			return OperationalQueueDepth{}, fmt.Errorf("read operational queue depths: %w", err)
		}
		return depths, nil
	})
}
