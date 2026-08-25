package postgres

import (
	"context"
	"fmt"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

const createLabelSQL = `/* op:create_label */
INSERT INTO labels (tenant_id, label_id, name, normalized_slug)
VALUES ($1, $2, $3, $4)`

func (repository *Repository) CreateLabel(ctx context.Context, tenantID domain.TenantID, label domain.Label) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if label.TenantID != tenantID {
		return domain.ErrTenantBoundary
	}
	canonical, err := domain.NewLabel(label.ID, tenantID, label.Name)
	if err != nil {
		return err
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		if _, err := tx.ExecContext(ctx, createLabelSQL, string(tenantID), string(canonical.ID), canonical.Name, canonical.Slug); err != nil {
			return fmt.Errorf("create label: %w", err)
		}
		return nil
	})
}

const listLabelsSQL = `/* op:list_labels */
SELECT label_id, name, normalized_slug
FROM labels
WHERE tenant_id = $1
ORDER BY normalized_slug, label_id`

func (repository *Repository) ListLabels(ctx context.Context, tenantID domain.TenantID) ([]domain.Label, error) {
	return inTenant(ctx, repository, tenantID, func(tx transaction) ([]domain.Label, error) {
		rows, err := tx.QueryContext(ctx, listLabelsSQL, string(tenantID))
		if err != nil {
			return nil, fmt.Errorf("list labels: %w", err)
		}
		defer rows.Close()
		var labels []domain.Label
		for rows.Next() {
			var id, name, slug string
			if err := rows.Scan(&id, &name, &slug); err != nil {
				return nil, fmt.Errorf("scan label: %w", err)
			}
			labels = append(labels, domain.Label{ID: domain.LabelID(id), TenantID: tenantID, Name: name, Slug: slug})
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate labels: %w", err)
		}
		return labels, nil
	})
}

const attachLabelSQL = `/* op:attach_label */
INSERT INTO contact_labels (tenant_id, contact_id, label_id)
SELECT $1, contacts.contact_id, labels.label_id
FROM contacts
JOIN labels ON labels.tenant_id = contacts.tenant_id
WHERE contacts.tenant_id = $1 AND contacts.contact_id = $2 AND labels.label_id = $3
ON CONFLICT (tenant_id, contact_id, label_id) DO UPDATE
SET tenant_id = EXCLUDED.tenant_id`

func (repository *Repository) AttachLabel(ctx context.Context, tenantID domain.TenantID, contactID domain.ContactID, labelID domain.LabelID) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if contactID == "" || labelID == "" {
		return domain.ErrInvalidIdentifier
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		result, err := tx.ExecContext(ctx, attachLabelSQL, string(tenantID), string(contactID), string(labelID))
		if err != nil {
			return fmt.Errorf("attach label: %w", err)
		}
		return requireAffected(result, ErrContactLabelLinkNotFound)
	})
}

const detachLabelSQL = `/* op:detach_label */
WITH object_state AS (
    SELECT
        EXISTS (
            SELECT 1 FROM contacts
            WHERE tenant_id = $1 AND contact_id = $2
        ) AS contact_exists,
        EXISTS (
            SELECT 1 FROM labels
            WHERE tenant_id = $1 AND label_id = $3
        ) AS label_exists
), deleted AS (
    DELETE FROM contact_labels
    WHERE tenant_id = $1 AND contact_id = $2 AND label_id = $3
      AND (SELECT contact_exists AND label_exists FROM object_state)
    RETURNING 1
)
SELECT object_state.contact_exists, object_state.label_exists,
       EXISTS (SELECT 1 FROM deleted) AS deleted
FROM object_state`

func (repository *Repository) DetachLabel(ctx context.Context, tenantID domain.TenantID, contactID domain.ContactID, labelID domain.LabelID) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if contactID == "" || labelID == "" {
		return domain.ErrInvalidIdentifier
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		var contactExists, labelExists, deleted bool
		err := tx.QueryRowContext(ctx, detachLabelSQL, string(tenantID), string(contactID), string(labelID)).Scan(
			&contactExists, &labelExists, &deleted,
		)
		if err != nil {
			return fmt.Errorf("detach label: %w", err)
		}
		if !contactExists {
			return ErrContactNotFound
		}
		if !labelExists {
			return ErrLabelNotFound
		}
		return nil
	})
}

const beginContactSyncSQL = `/* op:begin_contact_sync */
INSERT INTO contact_sync_runs (
    tenant_id, sync_run_id, connection_id, status, imported_count,
    rejected_count, error_summary, started_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

func (repository *Repository) BeginContactSyncRun(ctx context.Context, tenantID domain.TenantID, run ContactSyncRun) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if run.TenantID != tenantID {
		return domain.ErrTenantBoundary
	}
	if run.ID == "" || run.ConnectionID == "" {
		return domain.ErrInvalidIdentifier
	}
	if run.Status != ContactSyncRunning || run.StartedAt.IsZero() || run.ImportedCount < 0 || run.RejectedCount < 0 {
		return fmt.Errorf("invalid contact sync run")
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		_, err := tx.ExecContext(ctx, beginContactSyncSQL, string(tenantID), run.ID, string(run.ConnectionID),
			string(run.Status), run.ImportedCount, run.RejectedCount, run.ErrorSummary, run.StartedAt)
		if err != nil {
			return fmt.Errorf("begin contact sync run: %w", err)
		}
		return nil
	})
}

const finishContactSyncSQL = `/* op:finish_contact_sync */
UPDATE contact_sync_runs
SET status = $3, imported_count = $4, rejected_count = $5,
    error_summary = $6, finished_at = $7
WHERE tenant_id = $1 AND sync_run_id = $2 AND status = 'running'`

func (repository *Repository) FinishContactSyncRun(
	ctx context.Context,
	tenantID domain.TenantID,
	runID string,
	status ContactSyncStatus,
	importedCount, rejectedCount int,
	errorSummary string,
	finishedAt time.Time,
) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if runID == "" {
		return domain.ErrInvalidIdentifier
	}
	if (status != ContactSyncSucceeded && status != ContactSyncFailed) || importedCount < 0 || rejectedCount < 0 || finishedAt.IsZero() {
		return fmt.Errorf("invalid contact sync completion")
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		result, err := tx.ExecContext(ctx, finishContactSyncSQL, string(tenantID), runID, string(status),
			importedCount, rejectedCount, errorSummary, finishedAt)
		if err != nil {
			return fmt.Errorf("finish contact sync run: %w", err)
		}
		return requireAffected(result, ErrContactSyncRunNotFound)
	})
}
