-- Persist bounded canonical provider cursor fingerprints so pagination cycles
-- remain detectable across actor/gateway restarts. Rows are written only from
-- the same fenced inbox transaction that accepts a cursor-bearing envelope.
ALTER TABLE connection_actor_health
    DROP CONSTRAINT connection_actor_health_last_safe_reason_check;
ALTER TABLE connection_actor_health
    ADD CONSTRAINT connection_actor_health_last_safe_reason_check
    CHECK (last_safe_reason IN ('none', 'transient-network', 'provider-auth', 'provider-config', 'provider-protocol', 'shared-infrastructure', 'lease-lost', 'session-conflict', 'shutdown'));

CREATE TABLE provider_cursor_history (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    cursor_scope text NOT NULL CHECK (octet_length(cursor_scope) BETWEEN 1 AND 512),
    cursor_digest bytea NOT NULL CHECK (octet_length(cursor_digest) = 32),
    base_cursor_digest bytea CHECK (base_cursor_digest IS NULL OR octet_length(base_cursor_digest) = 32),
    provider_response_id text,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id, cursor_scope, cursor_digest),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id)
);

CREATE INDEX provider_cursor_history_eviction
    ON provider_cursor_history (tenant_id, connection_id, cursor_scope, observed_at DESC, cursor_digest DESC);

ALTER TABLE provider_cursor_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_cursor_history FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_cursor_history_tenant_isolation ON provider_cursor_history
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
