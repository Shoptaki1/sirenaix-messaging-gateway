-- Provider response IDs are durable inbox/ACK protocol keys. Reconcile rows
-- admitted by the legacy schema before validating the canonical 256-byte
-- boundary. The documented migration role is the table owner; FORCE RLS is
-- removed only inside this transaction and is restored by commit or rollback.
BEGIN;

CREATE FUNCTION sirenaix_valid_provider_response_id(candidate text)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE STRICT PARALLEL SAFE
AS $provider_response_id_validator$
DECLARE
    position integer;
    codepoint integer;
BEGIN
    IF octet_length(candidate) NOT BETWEEN 1 AND 256 OR candidate <> btrim(candidate) THEN
        RETURN false;
    END IF;
    FOR position IN 1..char_length(candidate) LOOP
        codepoint := ascii(substr(candidate, position, 1));
        IF codepoint BETWEEN 0 AND 31 OR codepoint BETWEEN 127 AND 159
           OR codepoint = ascii(U&'\00A0') OR codepoint = ascii(U&'\1680')
           OR codepoint BETWEEN ascii(U&'\2000') AND ascii(U&'\200A')
           OR codepoint BETWEEN ascii(U&'\2028') AND ascii(U&'\2029')
           OR codepoint = ascii(U&'\202F') OR codepoint = ascii(U&'\205F') OR codepoint = ascii(U&'\3000')
           OR codepoint = ascii(U&'\00AD') OR codepoint BETWEEN ascii(U&'\0600') AND ascii(U&'\0605')
           OR codepoint = ascii(U&'\061C') OR codepoint = ascii(U&'\06DD') OR codepoint = ascii(U&'\070F')
           OR codepoint BETWEEN ascii(U&'\0890') AND ascii(U&'\0891') OR codepoint = ascii(U&'\08E2')
           OR codepoint = ascii(U&'\180E') OR codepoint BETWEEN ascii(U&'\200B') AND ascii(U&'\200F')
           OR codepoint BETWEEN ascii(U&'\202A') AND ascii(U&'\202E')
           OR codepoint BETWEEN ascii(U&'\2060') AND ascii(U&'\2064')
           OR codepoint BETWEEN ascii(U&'\2066') AND ascii(U&'\206F')
           OR codepoint = ascii(U&'\FEFF') OR codepoint BETWEEN ascii(U&'\FFF9') AND ascii(U&'\FFFB')
           OR codepoint = ascii(U&'\+0110BD') OR codepoint = ascii(U&'\+0110CD')
           OR codepoint BETWEEN ascii(U&'\+013430') AND ascii(U&'\+01343F')
           OR codepoint BETWEEN ascii(U&'\+01BCA0') AND ascii(U&'\+01BCA3')
           OR codepoint BETWEEN ascii(U&'\+01D173') AND ascii(U&'\+01D17A')
           OR codepoint = ascii(U&'\+0E0001') OR codepoint BETWEEN ascii(U&'\+0E0020') AND ascii(U&'\+0E007F')
           OR codepoint BETWEEN ascii(U&'\E000') AND ascii(U&'\F8FF')
           OR codepoint BETWEEN ascii(U&'\+0F0000') AND ascii(U&'\+0FFFFD')
           OR codepoint BETWEEN ascii(U&'\+100000') AND ascii(U&'\+10FFFD')
           OR codepoint BETWEEN ascii(U&'\FDD0') AND ascii(U&'\FDEF')
           OR (codepoint & 65534) = 65534 THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END
$provider_response_id_validator$;

CREATE TABLE provider_response_id_quarantine (
    tenant_id text NOT NULL,
    quarantine_id text NOT NULL,
    connection_id text NOT NULL,
    data_family text NOT NULL CHECK (data_family IN ('inbox', 'conflict', 'cursor_history', 'cursor_budget')),
    response_id_fingerprint text NOT NULL CHECK (octet_length(response_id_fingerprint) = 32),
    response_id_octets bigint NOT NULL CHECK (response_id_octets >= 0),
    envelope_digest bytea CHECK (envelope_digest IS NULL OR octet_length(envelope_digest) = 32),
    reason text NOT NULL CHECK (reason = 'invalid_provider_response_id'),
    quarantined_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, quarantine_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

ALTER TABLE provider_response_id_quarantine ENABLE ROW LEVEL SECURITY;
CREATE POLICY provider_response_id_quarantine_tenant_isolation ON provider_response_id_quarantine
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE connections NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_leases NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_actor_health NO FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox NO FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox_conflicts NO FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_cursor_history NO FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_cursor_budgets NO FORCE ROW LEVEL SECURITY;

-- Store only bounded audit metadata, never the potentially 4 MiB raw envelope
-- or unsafe identifier itself. Existing envelope SHA-256 digests are retained;
-- md5 is used solely as a non-secret lookup fingerprint for the legacy text.
INSERT INTO provider_response_id_quarantine (
    tenant_id, quarantine_id, connection_id, data_family,
    response_id_fingerprint, response_id_octets, envelope_digest, reason
)
SELECT tenant_id, 'inbox:' || inbox_id, connection_id, 'inbox',
       md5(provider_response_id), octet_length(provider_response_id), envelope_digest,
       'invalid_provider_response_id'
FROM provider_inbox
WHERE NOT sirenaix_valid_provider_response_id(provider_response_id);

INSERT INTO provider_response_id_quarantine (
    tenant_id, quarantine_id, connection_id, data_family,
    response_id_fingerprint, response_id_octets, envelope_digest, reason
)
SELECT tenant_id, 'conflict:' || conflict_id, connection_id, 'conflict',
       md5(provider_response_id), octet_length(provider_response_id), conflicting_digest,
       'invalid_provider_response_id'
FROM provider_inbox_conflicts
WHERE NOT sirenaix_valid_provider_response_id(provider_response_id);

INSERT INTO provider_response_id_quarantine (
    tenant_id, quarantine_id, connection_id, data_family,
    response_id_fingerprint, response_id_octets, envelope_digest, reason
)
SELECT tenant_id,
       'cursor-history:' || md5(connection_id || chr(31) || cursor_scope) || ':' || encode(cursor_digest, 'hex'),
       connection_id, 'cursor_history', md5(provider_response_id), octet_length(provider_response_id), NULL,
       'invalid_provider_response_id'
FROM provider_cursor_history
WHERE provider_response_id IS NOT NULL
  AND NOT sirenaix_valid_provider_response_id(provider_response_id);

INSERT INTO provider_response_id_quarantine (
    tenant_id, quarantine_id, connection_id, data_family,
    response_id_fingerprint, response_id_octets, envelope_digest, reason
)
SELECT tenant_id,
       'cursor-budget:' || md5(connection_id || chr(31) || cursor_scope),
       connection_id, 'cursor_budget', md5(last_provider_response_id), octet_length(last_provider_response_id), NULL,
       'invalid_provider_response_id'
FROM provider_cursor_budgets
WHERE NOT sirenaix_valid_provider_response_id(last_provider_response_id);

-- Fence and suspend every affected connection before removing the unsafe keys
-- from active inbox/ACK/checkpoint state. Healthy tenants remain runnable.
WITH affected AS (
    SELECT DISTINCT tenant_id, connection_id
    FROM provider_response_id_quarantine
)
UPDATE connection_leases AS lease
SET owner_id = NULL, fencing_token = fencing_token + 1,
    expires_at = clock_timestamp(), updated_at = clock_timestamp()
FROM affected
WHERE lease.tenant_id = affected.tenant_id AND lease.connection_id = affected.connection_id;

WITH affected AS (
    SELECT DISTINCT tenant_id, connection_id
    FROM provider_response_id_quarantine
)
UPDATE connections AS connection
SET state = 'suspended', updated_at = clock_timestamp()
FROM affected
WHERE connection.tenant_id = affected.tenant_id AND connection.connection_id = affected.connection_id;

WITH affected AS (
    SELECT DISTINCT tenant_id, connection_id
    FROM provider_response_id_quarantine
)
UPDATE connection_actor_health AS health
SET actor_state = 'stopped', connection_state = 'suspended',
    last_safe_reason = 'provider-protocol', current_backoff_microseconds = 0,
    requires_reauthorization = false, updated_at = clock_timestamp()
FROM affected
WHERE health.tenant_id = affected.tenant_id AND health.connection_id = affected.connection_id;

DELETE FROM provider_inbox_conflicts
WHERE NOT sirenaix_valid_provider_response_id(provider_response_id);

DELETE FROM provider_cursor_history
WHERE provider_response_id IS NOT NULL
  AND NOT sirenaix_valid_provider_response_id(provider_response_id);

DELETE FROM provider_cursor_budgets
WHERE NOT sirenaix_valid_provider_response_id(last_provider_response_id);

DELETE FROM provider_inbox
WHERE NOT sirenaix_valid_provider_response_id(provider_response_id);

ALTER TABLE provider_inbox
    ADD CONSTRAINT provider_inbox_response_id_boundary
    CHECK (sirenaix_valid_provider_response_id(provider_response_id)) NOT VALID;

ALTER TABLE provider_inbox_conflicts
    ADD CONSTRAINT provider_inbox_conflicts_response_id_boundary
    CHECK (sirenaix_valid_provider_response_id(provider_response_id)) NOT VALID;

ALTER TABLE provider_cursor_history
    ADD CONSTRAINT provider_cursor_history_response_id_boundary
    CHECK (
        provider_response_id IS NULL OR sirenaix_valid_provider_response_id(provider_response_id)
    ) NOT VALID;

ALTER TABLE provider_cursor_budgets
    ADD CONSTRAINT provider_cursor_budgets_response_id_boundary
    CHECK (sirenaix_valid_provider_response_id(last_provider_response_id)) NOT VALID;

ALTER TABLE provider_inbox VALIDATE CONSTRAINT provider_inbox_response_id_boundary;
ALTER TABLE provider_inbox_conflicts VALIDATE CONSTRAINT provider_inbox_conflicts_response_id_boundary;
ALTER TABLE provider_cursor_history VALIDATE CONSTRAINT provider_cursor_history_response_id_boundary;
ALTER TABLE provider_cursor_budgets VALIDATE CONSTRAINT provider_cursor_budgets_response_id_boundary;

ALTER TABLE connections FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_leases FORCE ROW LEVEL SECURITY;
ALTER TABLE connection_actor_health FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox_conflicts FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_cursor_history FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_cursor_budgets FORCE ROW LEVEL SECURITY;
ALTER TABLE provider_response_id_quarantine FORCE ROW LEVEL SECURITY;

COMMIT;
