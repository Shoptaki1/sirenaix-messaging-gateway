-- A response ID has one digest and one storage disposition for its entire
-- operator-controlled connection epoch. This file owns its transaction:
-- deployment runners must execute it intact, without statement splitting or
-- an outer transaction, so every temporary FORCE RLS change rolls back on any
-- failure.
BEGIN;

ALTER TABLE provider_inbox NO FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox_conflicts NO FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_rejected_responses NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connections NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_leases NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_actor_health NO FORCE ROW LEVEL SECURITY;

ALTER TABLE provider_rejected_responses
    DROP CONSTRAINT provider_rejected_responses_reason_check;
ALTER TABLE provider_rejected_responses
    ADD CONSTRAINT provider_rejected_responses_reason_check
    CHECK (reason IN (
        'provider_cursor_budget_exhausted', 'provider_cursor_cycle',
        'media_identity_conflict', 'response_id_digest_conflict'
    ));

CREATE TABLE provider_response_reservations (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    provider_response_id text NOT NULL,
    envelope_digest bytea NOT NULL CHECK (octet_length(envelope_digest) = 32),
    disposition text NOT NULL CHECK (disposition IN ('inbox', 'rejected')),
    conflicted boolean NOT NULL DEFAULT false,
    occurrence_count bigint NOT NULL DEFAULT 1 CHECK (occurrence_count BETWEEN 1 AND 2147483647),
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id, provider_response_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CHECK (sirenaix_valid_provider_response_id(provider_response_id)),
    CHECK (NOT conflicted OR disposition IN ('inbox', 'rejected'))
);

ALTER TABLE provider_response_reservations ENABLE ROW LEVEL SECURITY;
CREATE POLICY provider_response_reservations_tenant_isolation ON provider_response_reservations
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

CREATE TABLE provider_response_overflow_audits (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    overflow_poison_rows bigint NOT NULL DEFAULT 0 CHECK (overflow_poison_rows >= 0),
    overflow_poison_bytes bigint NOT NULL DEFAULT 0 CHECK (overflow_poison_bytes >= 0),
    overflow_rejected_rows bigint NOT NULL DEFAULT 0 CHECK (overflow_rejected_rows >= 0),
    first_envelope_digest bytea CHECK (first_envelope_digest IS NULL OR octet_length(first_envelope_digest) = 32),
    last_envelope_digest bytea CHECK (last_envelope_digest IS NULL OR octet_length(last_envelope_digest) = 32),
    reason text NOT NULL DEFAULT 'legacy_provider_response_quota_exceeded',
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CHECK (overflow_poison_rows + overflow_rejected_rows > 0)
);

ALTER TABLE provider_response_overflow_audits ENABLE ROW LEVEL SECURITY;
CREATE POLICY provider_response_overflow_audits_tenant_isolation ON provider_response_overflow_audits
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

INSERT INTO provider_response_reservations (
    tenant_id, connection_id, provider_response_id, envelope_digest,
    disposition, conflicted, occurrence_count, first_seen_at, last_seen_at
)
SELECT tenant_id, connection_id, provider_response_id, envelope_digest,
       'inbox', false, 1, received_at, received_at
FROM provider_inbox;

INSERT INTO provider_response_reservations (
    tenant_id, connection_id, provider_response_id, envelope_digest,
    disposition, conflicted, occurrence_count, first_seen_at, last_seen_at
)
SELECT tenant_id, connection_id, provider_response_id, envelope_digest,
       'rejected', conflicted, occurrence_count, first_seen_at, last_seen_at
FROM provider_rejected_responses
ON CONFLICT (tenant_id, connection_id, provider_response_id) DO UPDATE
SET conflicted = provider_response_reservations.conflicted
        OR EXCLUDED.conflicted
        OR provider_response_reservations.envelope_digest <> EXCLUDED.envelope_digest,
    occurrence_count = LEAST(
        provider_response_reservations.occurrence_count + EXCLUDED.occurrence_count,
        2147483647
    ),
    last_seen_at = GREATEST(provider_response_reservations.last_seen_at, EXCLUDED.last_seen_at);

-- When both legacy families contain the exact same digest, the inbox row is
-- the canonical physical disposition. The reservation retains the combined
-- occurrence count, but the duplicate rejected row must not remain claimable.
DELETE FROM provider_rejected_responses AS rejected
USING provider_response_reservations AS reservation
WHERE reservation.tenant_id = rejected.tenant_id
  AND reservation.connection_id = rejected.connection_id
  AND reservation.provider_response_id = rejected.provider_response_id
  AND reservation.disposition = 'inbox'
  AND NOT reservation.conflicted
  AND reservation.envelope_digest = rejected.envelope_digest;

-- Legacy same-family conflicts already prove that the response ID is
-- ambiguous. Import that state into the authoritative reservation before any
-- ACK is allowed; later deduplication only bounds its raw audit sample.
WITH conflict_counts AS (
    SELECT tenant_id, connection_id, provider_response_id, count(*) AS occurrences
    FROM provider_inbox_conflicts
    GROUP BY tenant_id, connection_id, provider_response_id
)
UPDATE provider_response_reservations AS reservation
SET conflicted = true,
    occurrence_count = LEAST(
        reservation.occurrence_count + LEAST(conflict_counts.occurrences, 2147483646),
        2147483647
    ),
    last_seen_at = clock_timestamp()
FROM conflict_counts
WHERE reservation.tenant_id = conflict_counts.tenant_id
  AND reservation.connection_id = conflict_counts.connection_id
  AND reservation.provider_response_id = conflict_counts.provider_response_id;

-- A legacy cross-family digest collision is retained as bounded audit state,
-- but it is removed from ACK eligibility and cannot be projected again.
UPDATE provider_inbox AS inbox
SET poisoned = true, poison_reason = 'cross_family_response_id_conflict', ack_pending = false
FROM provider_response_reservations AS reservation
WHERE reservation.tenant_id = inbox.tenant_id
  AND reservation.connection_id = inbox.connection_id
  AND reservation.provider_response_id = inbox.provider_response_id
  AND reservation.conflicted;

UPDATE provider_rejected_responses AS rejected
SET conflicted = true, ack_pending = false
FROM provider_response_reservations AS reservation
WHERE reservation.tenant_id = rejected.tenant_id
  AND reservation.connection_id = rejected.connection_id
  AND reservation.provider_response_id = rejected.provider_response_id
  AND reservation.conflicted;

-- Preserve only the oldest conflicting raw envelope for a response ID. Future
-- observations increment a bounded counter instead of appending attacker-sized
-- copies for every chosen digest.
ALTER TABLE provider_inbox_conflicts
    ADD COLUMN conflicting_envelope_size bigint;
UPDATE provider_inbox_conflicts
SET conflicting_envelope_size = octet_length(conflicting_raw_envelope),
    conflicting_raw_envelope = substring(conflicting_raw_envelope FROM 1 FOR 256);
ALTER TABLE provider_inbox_conflicts
    ALTER COLUMN conflicting_envelope_size SET NOT NULL;
ALTER TABLE provider_inbox_conflicts
    ADD CONSTRAINT provider_inbox_conflicts_size_boundary
    CHECK (conflicting_envelope_size BETWEEN 1 AND 4194304);
ALTER TABLE provider_inbox_conflicts
    ADD CONSTRAINT provider_inbox_conflicts_sample_boundary
    CHECK (octet_length(conflicting_raw_envelope) BETWEEN 1 AND 256);

ALTER TABLE provider_inbox_conflicts
    ADD COLUMN occurrence_count bigint NOT NULL DEFAULT 1
    CHECK (occurrence_count BETWEEN 1 AND 2147483647);
WITH conflict_occurrences AS (
    SELECT tenant_id, connection_id, provider_response_id, count(*) AS occurrences
    FROM provider_inbox_conflicts
    GROUP BY tenant_id, connection_id, provider_response_id
)
UPDATE provider_inbox_conflicts AS conflict
SET occurrence_count = LEAST(conflict_occurrences.occurrences, 2147483647)
FROM conflict_occurrences
WHERE conflict.tenant_id = conflict_occurrences.tenant_id
  AND conflict.connection_id = conflict_occurrences.connection_id
  AND conflict.provider_response_id = conflict_occurrences.provider_response_id;

DELETE FROM provider_inbox_conflicts AS newer
USING provider_inbox_conflicts AS older
WHERE newer.tenant_id = older.tenant_id
  AND newer.connection_id = older.connection_id
  AND newer.provider_response_id = older.provider_response_id
  AND (newer.observed_at, newer.conflict_id) > (older.observed_at, older.conflict_id);

ALTER TABLE provider_inbox_conflicts
    ADD CONSTRAINT provider_inbox_conflicts_one_per_response
    UNIQUE (tenant_id, connection_id, provider_response_id);

-- Determine overflow before deleting anything. The ordering is stable across
-- retries: oldest poison/rejected evidence wins, and poisoned raw evidence is
-- retained only while both the 256-row and 32 MiB cumulative limits hold.
CREATE TEMP TABLE task7_provider_response_overflow ON COMMIT DROP AS
WITH poison_ranked AS (
    SELECT tenant_id, connection_id, provider_response_id, envelope_digest,
           octet_length(raw_envelope)::bigint AS raw_bytes, received_at AS observed_at,
           row_number() OVER (
               PARTITION BY tenant_id, connection_id
               ORDER BY received_at, inbox_id
           ) AS row_ordinal,
           sum(octet_length(raw_envelope)) OVER (
               PARTITION BY tenant_id, connection_id
               ORDER BY received_at, inbox_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS cumulative_bytes
    FROM provider_inbox
    WHERE poisoned
), rejected_ranked AS (
    SELECT tenant_id, connection_id, provider_response_id, envelope_digest,
           0::bigint AS raw_bytes, first_seen_at AS observed_at,
           row_number() OVER (
               PARTITION BY tenant_id, connection_id
               ORDER BY first_seen_at, provider_response_id
           ) AS row_ordinal
    FROM provider_rejected_responses
)
SELECT tenant_id, connection_id, provider_response_id, envelope_digest,
       'poison'::text AS source, raw_bytes, observed_at
FROM poison_ranked
WHERE row_ordinal > 256 OR cumulative_bytes > 33554432
UNION ALL
SELECT tenant_id, connection_id, provider_response_id, envelope_digest,
       'rejected'::text AS source, raw_bytes, observed_at
FROM rejected_ranked
WHERE row_ordinal > 256;

INSERT INTO provider_response_overflow_audits (
    tenant_id, connection_id, overflow_poison_rows, overflow_poison_bytes,
    overflow_rejected_rows, first_envelope_digest, last_envelope_digest
)
SELECT tenant_id, connection_id,
       count(*) FILTER (WHERE source = 'poison'),
       COALESCE(sum(raw_bytes) FILTER (WHERE source = 'poison'), 0),
       count(*) FILTER (WHERE source = 'rejected'),
       decode(min(encode(envelope_digest, 'hex')), 'hex'),
       decode(max(encode(envelope_digest, 'hex')), 'hex')
FROM task7_provider_response_overflow
GROUP BY tenant_id, connection_id;

CREATE TEMP TABLE task7_provider_response_affected ON COMMIT DROP AS
SELECT DISTINCT tenant_id, connection_id
FROM provider_response_reservations
WHERE conflicted
UNION
SELECT tenant_id, connection_id
FROM provider_response_overflow_audits;

-- Conflicted or over-quota legacy state requires operator resolution. Fence
-- the active generation before pruning excess ACK identities/raw evidence.
UPDATE connection_leases AS lease
SET owner_id = NULL, fencing_token = fencing_token + 1,
    expires_at = clock_timestamp(), updated_at = clock_timestamp()
FROM task7_provider_response_affected AS affected
WHERE lease.tenant_id = affected.tenant_id AND lease.connection_id = affected.connection_id;

UPDATE connections AS connection
SET state = 'suspended', updated_at = clock_timestamp()
FROM task7_provider_response_affected AS affected
WHERE connection.tenant_id = affected.tenant_id AND connection.connection_id = affected.connection_id;

UPDATE connection_actor_health AS health
SET actor_state = 'stopped', connection_state = 'suspended',
    last_safe_reason = 'provider-protocol', current_backoff_microseconds = 0,
    requires_reauthorization = false, updated_at = clock_timestamp()
FROM task7_provider_response_affected AS affected
WHERE health.tenant_id = affected.tenant_id AND health.connection_id = affected.connection_id;

DELETE FROM provider_inbox_conflicts AS conflict
USING task7_provider_response_overflow AS overflow
WHERE conflict.tenant_id = overflow.tenant_id
  AND conflict.connection_id = overflow.connection_id
  AND conflict.provider_response_id = overflow.provider_response_id;

DELETE FROM provider_rejected_responses AS rejected
USING task7_provider_response_overflow AS overflow
WHERE rejected.tenant_id = overflow.tenant_id
  AND rejected.connection_id = overflow.connection_id
  AND rejected.provider_response_id = overflow.provider_response_id;

DELETE FROM provider_inbox AS inbox
USING task7_provider_response_overflow AS overflow
WHERE inbox.tenant_id = overflow.tenant_id
  AND inbox.connection_id = overflow.connection_id
  AND inbox.provider_response_id = overflow.provider_response_id;

DELETE FROM provider_response_reservations AS reservation
USING task7_provider_response_overflow AS overflow
WHERE reservation.tenant_id = overflow.tenant_id
  AND reservation.connection_id = overflow.connection_id
  AND reservation.provider_response_id = overflow.provider_response_id;

ALTER TABLE provider_inbox FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox_conflicts FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_rejected_responses FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_response_reservations FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_response_overflow_audits FORCE ROW LEVEL SECURITY;
ALTER TABLE connections FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_leases FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_actor_health FORCE ROW LEVEL SECURITY;

COMMIT;
