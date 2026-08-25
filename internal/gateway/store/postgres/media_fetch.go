package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
	"go.mau.fi/mautrix-gmessages/internal/gateway/session"
)

const claimMediaFetchJobSQL = `/* op:claim_media_fetch_job */
WITH candidate AS MATERIALIZED (
    SELECT job_id
    FROM media_fetch_jobs
    WHERE tenant_id = $1 AND state IN ('pending', 'fetching')
      AND available_at <= clock_timestamp()
      AND (owner_id IS NULL OR claim_expires_at <= clock_timestamp())
    ORDER BY available_at, job_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
), claimed AS (
    UPDATE media_fetch_jobs AS job
    SET state = 'fetching', attempt_count = attempt_count + 1,
        owner_id = $2, claim_token = claim_token + 1,
        claim_expires_at = clock_timestamp() + interval '60 seconds'
    FROM candidate
    WHERE job.tenant_id = $1 AND job.job_id = candidate.job_id
    RETURNING job.job_id, job.media_id, job.connection_id, job.provider_message_id,
              job.provider_locator, job.declared_mime_type, job.declared_size,
              job.display_filename, job.key_ciphertext, job.key_wrapped_dek,
              job.key_nonce, job.key_id, job.key_version,
              job.thumbnail_key_ciphertext, job.thumbnail_key_wrapped_dek,
              job.thumbnail_key_nonce, job.thumbnail_key_id, job.thumbnail_key_version,
              job.attempt_count, job.owner_id, job.claim_token
)
SELECT claimed.job_id, claimed.media_id, claimed.connection_id, claimed.provider_message_id, claimed.provider_locator,
       claimed.declared_mime_type, claimed.declared_size, claimed.display_filename,
       COALESCE(claimed.key_ciphertext, ''::bytea), COALESCE(claimed.key_wrapped_dek, ''::bytea),
       COALESCE(claimed.key_nonce, ''::bytea), COALESCE(claimed.key_id, ''), COALESCE(claimed.key_version, 0),
       COALESCE(claimed.thumbnail_key_ciphertext, ''::bytea), COALESCE(claimed.thumbnail_key_wrapped_dek, ''::bytea),
       COALESCE(claimed.thumbnail_key_nonce, ''::bytea), COALESCE(claimed.thumbnail_key_id, ''), COALESCE(claimed.thumbnail_key_version, 0),
       claimed.attempt_count, claimed.owner_id, claimed.claim_token,
       object.state, COALESCE(object.object_key, ''), object.mime_type, object.byte_size,
       COALESCE(object.sha256_digest, ''::bytea), COALESCE(object.width, 0), COALESCE(object.height, 0),
       object.display_filename, object.created_at
FROM claimed
JOIN media_objects AS object ON object.tenant_id = $1 AND object.media_id = claimed.media_id`

func (repository *Repository) ClaimFetch(ctx context.Context, tenantID domain.TenantID, ownerID string) (media.FetchJob, bool, error) {
	if tenantID == "" || ownerID == "" || len(ownerID) > 256 {
		return media.FetchJob{}, false, domain.ErrInvalidIdentifier
	}
	type claimResult struct {
		job media.FetchJob
		ok  bool
	}
	result, err := inTenant(ctx, repository, tenantID, func(tx transaction) (claimResult, error) {
		var job media.FetchJob
		var jobID, mediaID, connectionID string
		var keyCiphertext, keyWrapped, keyNonce []byte
		var keyID string
		var keyVersion int
		var thumbnailCiphertext, thumbnailWrapped, thumbnailNonce []byte
		var thumbnailKeyID string
		var thumbnailVersion int
		var objectState string
		var imported media.Record
		err := tx.QueryRowContext(ctx, claimMediaFetchJobSQL, string(tenantID), ownerID).Scan(
			&jobID, &mediaID, &connectionID, &job.ProviderMessageID, &job.Locator,
			&job.DeclaredMIME, &job.DeclaredSize, &job.DisplayFilename,
			&keyCiphertext, &keyWrapped, &keyNonce, &keyID, &keyVersion,
			&thumbnailCiphertext, &thumbnailWrapped, &thumbnailNonce, &thumbnailKeyID, &thumbnailVersion,
			&job.AttemptCount, &job.OwnerID, &job.ClaimToken,
			&objectState, &imported.ObjectKey, &imported.MIMEType, &imported.Size, &imported.SHA256,
			&imported.Width, &imported.Height, &imported.DisplayFilename, &imported.CreatedAt,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return claimResult{}, nil
		}
		if err != nil {
			return claimResult{}, fmt.Errorf("claim media fetch: %w", err)
		}
		job.TenantID, job.JobID, job.MediaID, job.ConnectionID = tenantID, jobID, domain.MediaID(mediaID), domain.ConnectionID(connectionID)
		job.KeyEnvelope = mediaKeyEnvelope(keyCiphertext, keyWrapped, keyNonce, keyID, keyVersion)
		job.ThumbnailKeyEnvelope = mediaKeyEnvelope(thumbnailCiphertext, thumbnailWrapped, thumbnailNonce, thumbnailKeyID, thumbnailVersion)
		if objectState == "ready" {
			imported.ID, imported.TenantID, imported.State = job.MediaID, tenantID, "ready"
			job.Imported = &imported
		}
		if job.JobID == "" || job.MediaID == "" || job.ConnectionID == "" || job.OwnerID != ownerID || job.ClaimToken == 0 || job.AttemptCount < 1 {
			return claimResult{}, errors.New("invalid media fetch claim returned by database")
		}
		return claimResult{job: job, ok: true}, nil
	})
	return result.job, result.ok, err
}

const renewMediaFetchSQL = `/* op:renew_media_fetch */
UPDATE media_fetch_jobs
SET claim_expires_at = clock_timestamp() + interval '60 seconds'
WHERE tenant_id = $1 AND job_id = $2 AND media_id = $3
  AND state = 'fetching' AND owner_id = $4 AND claim_token = $5
  AND claim_expires_at > clock_timestamp()
RETURNING true`

func (repository *Repository) RenewFetch(ctx context.Context, job media.FetchJob) (bool, error) {
	if !validFetchCompletion(job) {
		return false, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, job.TenantID, func(tx transaction) (bool, error) {
		var owned bool
		err := tx.QueryRowContext(ctx, renewMediaFetchSQL,
			string(job.TenantID), job.JobID, string(job.MediaID), job.OwnerID, job.ClaimToken,
		).Scan(&owned)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return owned, err
	})
}

func mediaKeyEnvelope(ciphertext, wrapped, nonce []byte, keyID string, keyVersion int) session.Envelope {
	if len(ciphertext) == 0 {
		return session.Envelope{}
	}
	return session.Envelope{
		Version: session.EnvelopeVersion, Provider: "gmessages-media", Ciphertext: append([]byte(nil), ciphertext...),
		WrappedDEK: append([]byte(nil), wrapped...), Nonce: append([]byte(nil), nonce...), KeyID: keyID, KeyVersion: keyVersion,
	}
}

const completeMediaFetchReadySQL = `/* op:complete_media_fetch_ready */
WITH locked AS MATERIALIZED (
    SELECT job.job_id, job.media_id, job.connection_id, job.provider_message_id
    FROM media_fetch_jobs AS job
    JOIN media_objects AS object
      ON object.tenant_id = job.tenant_id AND object.media_id = job.media_id
    WHERE job.tenant_id = $1 AND job.job_id = $2 AND job.media_id = $3
      AND job.state = 'fetching' AND job.owner_id = $4 AND job.claim_token = $5
      AND object.state = 'ready' AND object.object_key = $6
      AND object.byte_size = $10 AND object.sha256_digest = $11 AND object.mime_type = $12
    FOR UPDATE OF job, object
), completed AS (
    UPDATE media_fetch_jobs AS job
    SET state = 'ready', owner_id = NULL, claim_expires_at = NULL, last_safe_error = ''
    FROM locked
    WHERE job.tenant_id = $1 AND job.job_id = locked.job_id
    RETURNING locked.media_id, locked.connection_id, locked.provider_message_id
), event AS (
    INSERT INTO gateway_events (
        tenant_id, event_id, event_type, aggregate_type, aggregate_id,
        connection_id, conversation_id, canonical_body
    )
    SELECT $1, $7, 'media.ready', 'media', completed.media_id,
           completed.connection_id, '', $8
    FROM completed
    RETURNING event_id
), outbox AS (
    INSERT INTO event_outbox (tenant_id, outbox_id, event_id, destination)
    SELECT $1, $9 || ':' || destination, event.event_id, destination
    FROM event CROSS JOIN unnest(ARRAY['webhook'::text, 'kafka'::text]) AS destination
    RETURNING outbox_id
)
SELECT EXISTS (SELECT 1 FROM event) AND (SELECT count(*) FROM outbox) = 2`

func (repository *Repository) CompleteReady(ctx context.Context, job media.FetchJob, record media.Record, eventID string, body []byte) error {
	if !validFetchCompletion(job) || record.TenantID != job.TenantID || record.ID != job.MediaID || record.State != "ready" || record.ObjectKey == "" ||
		record.Size < 1 || record.Size > media.HardMaxBytes || len(record.SHA256) != 32 || record.MIMEType == "" || eventID == "" || len(body) == 0 || len(body) > 1<<20 {
		return domain.ErrInvalidIdentifier
	}
	return repository.completeMediaQuery(ctx, job, completeMediaFetchReadySQL,
		string(job.TenantID), job.JobID, string(job.MediaID), job.OwnerID, job.ClaimToken,
		record.ObjectKey, eventID, body, repository.newID(), record.Size, record.SHA256, record.MIMEType,
	)
}

const retryMediaFetchSQL = `/* op:retry_media_fetch */
WITH updated AS (
    UPDATE media_fetch_jobs
    SET state = 'pending', owner_id = NULL, claim_expires_at = NULL,
        available_at = $6, last_safe_error = $7
    WHERE tenant_id = $1 AND job_id = $2 AND media_id = $3
      AND state = 'fetching' AND owner_id = $4 AND claim_token = $5
    RETURNING job_id
)
SELECT EXISTS (SELECT 1 FROM updated)`

const failMediaFetchSQL = `/* op:fail_media_fetch */
WITH locked AS MATERIALIZED (
    SELECT job_id, media_id, connection_id, provider_message_id
    FROM media_fetch_jobs
    WHERE tenant_id = $1 AND job_id = $2 AND media_id = $3
      AND state = 'fetching' AND owner_id = $4 AND claim_token = $5
    FOR UPDATE
), failed_job AS (
    UPDATE media_fetch_jobs AS job
    SET state = 'failed', owner_id = NULL, claim_expires_at = NULL, last_safe_error = $6
    FROM locked
    WHERE job.tenant_id = $1 AND job.job_id = locked.job_id
    RETURNING locked.media_id, locked.connection_id, locked.provider_message_id
), failed_object AS (
    UPDATE media_objects AS object
    SET state = 'failed', updated_at = clock_timestamp()
    FROM failed_job
    WHERE object.tenant_id = $1 AND object.media_id = failed_job.media_id
	  AND object.state <> 'ready'
    RETURNING object.media_id
), event AS (
    INSERT INTO gateway_events (
        tenant_id, event_id, event_type, aggregate_type, aggregate_id,
        connection_id, conversation_id, canonical_body
    )
    SELECT $1, $7, 'media.failed', 'media', failed_job.media_id,
           failed_job.connection_id, '', $8
    FROM failed_job
    RETURNING event_id
), outbox AS (
    INSERT INTO event_outbox (tenant_id, outbox_id, event_id, destination)
    SELECT $1, $9 || ':' || destination, event.event_id, destination
    FROM event CROSS JOIN unnest(ARRAY['webhook'::text, 'kafka'::text]) AS destination
    RETURNING outbox_id
)
SELECT EXISTS (SELECT 1 FROM event) AND (SELECT count(*) FROM outbox) = 2`

func (repository *Repository) CompleteFailed(ctx context.Context, job media.FetchJob, safeReason, eventID string, body []byte, retryAt time.Time) error {
	if !validFetchCompletion(job) || safeReason == "" || len(safeReason) > 128 {
		return domain.ErrInvalidIdentifier
	}
	if !retryAt.IsZero() {
		return repository.completeMediaQuery(ctx, job, retryMediaFetchSQL,
			string(job.TenantID), job.JobID, string(job.MediaID), job.OwnerID, job.ClaimToken, retryAt.UTC(), safeReason,
		)
	}
	if eventID == "" || len(body) == 0 || len(body) > 1<<20 {
		return domain.ErrInvalidIdentifier
	}
	return repository.completeMediaQuery(ctx, job, failMediaFetchSQL,
		string(job.TenantID), job.JobID, string(job.MediaID), job.OwnerID, job.ClaimToken,
		safeReason, eventID, body, repository.newID(),
	)
}

func (repository *Repository) completeMediaQuery(ctx context.Context, job media.FetchJob, query string, arguments ...any) error {
	return inTenantExec(ctx, repository, job.TenantID, func(tx transaction) error {
		var completed bool
		if err := tx.QueryRowContext(ctx, query, arguments...).Scan(&completed); err != nil {
			return err
		}
		if !completed {
			return media.ErrFetchFenceLost
		}
		return nil
	})
}

func validFetchCompletion(job media.FetchJob) bool {
	return job.TenantID != "" && job.JobID != "" && job.MediaID != "" && job.ConnectionID != "" &&
		job.OwnerID != "" && job.ClaimToken > 0 && job.AttemptCount > 0
}

var _ media.FetchStore = (*Repository)(nil)
