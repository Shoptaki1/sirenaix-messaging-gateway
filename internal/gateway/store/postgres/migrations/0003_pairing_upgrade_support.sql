-- Phase 1 adds nullable support columns, then deterministically reconciles
-- every row admitted by the preceding schema before constraints are staged.
BEGIN;

ALTER TABLE connections
    ADD COLUMN pairing_prior_state text,
    ADD COLUMN pairing_started_at timestamptz,
    ADD COLUMN pairing_attempt_id text;

ALTER TABLE connection_sessions
    ADD COLUMN revision bigint;

-- These tables are FORCE RLS under the application/table-owner role. Keep RLS
-- enabled, but temporarily remove FORCE inside this transaction so the owner
-- can reconcile every tenant. A failure rolls back both DML and this catalog
-- change; other sessions cannot observe the uncommitted NO FORCE state.
ALTER TABLE connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE connection_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE connections NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_sessions NO FORCE ROW LEVEL SECURITY;

-- Legacy unpaired/pairing rows and paired rows without an exact SHA-256
-- fingerprint cannot safely use their old encrypted session. Pairing rows have
-- no durable owner in the old schema, so they are never fabricated as active.
DELETE FROM connection_sessions AS sessions
USING connections
WHERE sessions.tenant_id = connections.tenant_id
  AND sessions.connection_id = connections.connection_id
  AND (
    connections.state IN ('unpaired', 'pairing')
    OR (connections.state IN ('connected', 'degraded', 'reauthorization-required', 'suspended', 'disconnected')
        AND (connections.provider_device_fingerprint IS NULL
             OR octet_length(connections.provider_device_fingerprint) <> 32))
  );

UPDATE connections
SET state = 'unpaired',
    provider_device_fingerprint = NULL,
    reauthorization_event_id = NULL,
    pairing_prior_state = NULL,
    pairing_started_at = NULL,
    pairing_attempt_id = NULL,
    updated_at = now()
WHERE state = 'pairing'
   OR (state IN ('connected', 'degraded', 'reauthorization-required', 'suspended', 'disconnected')
       AND (provider_device_fingerprint IS NULL
            OR octet_length(provider_device_fingerprint) <> 32));

-- Preserve valid legacy reauthorization rows and give each a deterministic,
-- tenant-scoped event ID without inventing provider credentials.
UPDATE connections
SET reauthorization_event_id = 'legacy-reauth-' || md5(tenant_id || chr(31) || connection_id),
    updated_at = now()
WHERE state = 'reauthorization-required'
  AND (reauthorization_event_id IS NULL OR length(btrim(reauthorization_event_id)) = 0);

UPDATE connections
SET reauthorization_event_id = NULL
WHERE state <> 'reauthorization-required';

UPDATE connection_sessions
SET revision = 1
WHERE revision IS NULL OR revision <= 0;

ALTER TABLE connection_sessions
    ALTER COLUMN revision SET DEFAULT 1;

ALTER TABLE connections FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_sessions FORCE ROW LEVEL SECURITY;

COMMIT;
