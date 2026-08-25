CREATE TABLE connection_leases (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    owner_id text,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id),
    FOREIGN KEY (tenant_id, connection_id)
        REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CHECK (owner_id IS NULL OR length(owner_id) BETWEEN 1 AND 256)
);

CREATE TABLE connection_actor_health (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    actor_state text NOT NULL CHECK (actor_state IN ('acquiring', 'connecting', 'ready', 'backoff', 'stopped', 'lease-lost')),
    connection_state text NOT NULL CHECK (connection_state IN ('connected', 'degraded', 'reauthorization-required', 'disconnected', 'suspended')),
    connected_at timestamptz,
    last_frame_at timestamptz,
    last_phone_response_at timestamptz,
    reconnect_count bigint NOT NULL DEFAULT 0 CHECK (reconnect_count >= 0),
    current_backoff_microseconds bigint NOT NULL DEFAULT 0 CHECK (current_backoff_microseconds >= 0),
    last_safe_reason text NOT NULL CHECK (last_safe_reason IN ('none', 'transient-network', 'provider-auth', 'provider-config', 'provider-protocol', 'lease-lost', 'session-conflict', 'shutdown')),
    requires_reauthorization boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id),
    FOREIGN KEY (tenant_id, connection_id)
        REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

ALTER TABLE connection_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE connection_leases FORCE ROW LEVEL SECURITY;
CREATE POLICY connection_leases_tenant_isolation ON connection_leases
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE connection_actor_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE connection_actor_health FORCE ROW LEVEL SECURITY;
CREATE POLICY connection_actor_health_tenant_isolation ON connection_actor_health
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
