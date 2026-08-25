package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
)

const createMediaMetadataSQL = `/* op:create_media_metadata */
INSERT INTO media_objects (
    tenant_id, media_id, object_key, state, mime_type, display_filename,
    byte_size, sha256_digest, width, height, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, clock_timestamp())
ON CONFLICT (tenant_id, media_id) DO UPDATE
SET object_key = EXCLUDED.object_key, state = EXCLUDED.state,
    mime_type = EXCLUDED.mime_type, display_filename = EXCLUDED.display_filename,
    byte_size = EXCLUDED.byte_size, sha256_digest = EXCLUDED.sha256_digest,
    width = EXCLUDED.width, height = EXCLUDED.height, updated_at = clock_timestamp()
WHERE media_objects.state IN ('pending', 'failed')
  AND (media_objects.object_key IS NULL OR media_objects.object_key = EXCLUDED.object_key)`

func (repository *Repository) Create(ctx context.Context, record media.Record) error {
	if !validMediaRecord(record) {
		return domain.ErrInvalidIdentifier
	}
	return inTenantExec(ctx, repository, record.TenantID, func(tx transaction) error {
		result, err := tx.ExecContext(ctx, createMediaMetadataSQL,
			string(record.TenantID), string(record.ID), record.ObjectKey, record.State,
			record.MIMEType, record.DisplayFilename, record.Size, record.SHA256,
			record.Width, record.Height, record.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("create media metadata: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil || count != 1 {
			return errors.New("media metadata identity conflict")
		}
		return nil
	})
}

const getMediaMetadataSQL = `/* op:get_media_metadata */
SELECT media_id, COALESCE(object_key, ''), mime_type, byte_size,
       COALESCE(sha256_digest, ''::bytea), COALESCE(width, 0), COALESCE(height, 0),
       display_filename, state, created_at
FROM media_objects
WHERE tenant_id = $1 AND media_id = $2
  AND NOT EXISTS (
      SELECT 1 FROM media_fetch_jobs AS job
      WHERE job.tenant_id = media_objects.tenant_id AND job.media_id = media_objects.media_id
        AND job.state = 'failed'
  )`

func (repository *Repository) Get(ctx context.Context, tenantID domain.TenantID, mediaID domain.MediaID) (media.Record, error) {
	if tenantID == "" || mediaID == "" {
		return media.Record{}, media.ErrNotFound
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (media.Record, error) {
		var record media.Record
		var id string
		err := tx.QueryRowContext(ctx, getMediaMetadataSQL, string(tenantID), string(mediaID)).Scan(
			&id, &record.ObjectKey, &record.MIMEType, &record.Size, &record.SHA256,
			&record.Width, &record.Height, &record.DisplayFilename, &record.State, &record.CreatedAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return media.Record{}, media.ErrNotFound
		}
		if err != nil {
			return media.Record{}, fmt.Errorf("get media metadata: %w", err)
		}
		record.ID, record.TenantID = domain.MediaID(id), tenantID
		return record, nil
	})
}

func validMediaRecord(record media.Record) bool {
	if record.TenantID == "" || record.ID == "" || !strings.HasPrefix(record.ObjectKey, "objects/") || strings.Contains(record.ObjectKey, "..") ||
		record.Size < 0 || record.Size > media.HardMaxBytes || len(record.SHA256) != 32 || record.Width < 1 || record.Height < 1 ||
		record.DisplayFilename == "" || len(record.DisplayFilename) > 255 || record.State != "ready" {
		return false
	}
	switch record.MIMEType {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}

var _ media.Metadata = (*Repository)(nil)
