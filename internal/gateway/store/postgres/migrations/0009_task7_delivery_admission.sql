-- Webhook endpoint revocation uses a generation fence. A claimed delivery is
-- admitted to HTTP atomically against that generation immediately before the
-- request starts, so revocation can distinguish queued work from a request
-- that may already be on the wire.
ALTER TABLE webhook_endpoints
    ADD COLUMN generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0);

ALTER TABLE webhook_deliveries
    ADD COLUMN endpoint_generation bigint NOT NULL DEFAULT 1 CHECK (endpoint_generation > 0),
    ADD COLUMN http_started_at timestamptz;

-- Canonical attachment identity covers locator, exact declared metadata, and
-- both provider key identities. NULL legacy rows fail closed on redelivery.
ALTER TABLE media_fetch_jobs
    ADD COLUMN attachment_identity_digest bytea
        CHECK (attachment_identity_digest IS NULL OR octet_length(attachment_identity_digest) = 32);

-- Identity belongs to the stable message/position association as well as the
-- fetch job. This lets a locally uploaded outbound attachment bind its first
-- provider echo once and fail closed on every later changed echo.
ALTER TABLE message_media
    ADD COLUMN provider_identity_digest bytea
        CHECK (provider_identity_digest IS NULL OR octet_length(provider_identity_digest) = 32);
