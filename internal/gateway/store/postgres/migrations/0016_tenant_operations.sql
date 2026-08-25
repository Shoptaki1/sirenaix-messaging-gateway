-- Supported tenant lifecycle and quota controls. Administrative commands set
-- the same fail-closed tenant context as the application before every change.
ALTER TABLE tenants
    ADD COLUMN status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'suspended')),
    ADD COLUMN max_connections integer NOT NULL DEFAULT 128
        CHECK (max_connections BETWEEN 1 AND 128),
    ADD COLUMN suspended_at timestamptz;

ALTER TABLE tenants
    ADD CONSTRAINT tenants_suspension_timestamp CHECK (
        (status = 'active' AND suspended_at IS NULL)
        OR (status = 'suspended' AND suspended_at IS NOT NULL)
    );

ALTER TABLE connections
    ADD COLUMN tenant_suspend_prior_state text
        CHECK (tenant_suspend_prior_state IN (
            'unpaired', 'connected', 'degraded',
            'reauthorization-required', 'suspended', 'disconnected'
        )),
    ADD CONSTRAINT connections_tenant_suspension_state CHECK (
        tenant_suspend_prior_state IS NULL OR state = 'suspended'
    );

-- Administrative suspension must also close the pairing path for an unpaired
-- phone. Preserve the original unpaired state while continuing to reject any
-- other suspended row without an exact fingerprint.
ALTER TABLE connections
    DROP CONSTRAINT connections_fingerprint_matches_state,
    ADD CONSTRAINT connections_fingerprint_matches_state CHECK (
        (state = 'unpaired' AND provider_device_fingerprint IS NULL)
        OR (state = 'pairing' AND (provider_device_fingerprint IS NULL OR octet_length(provider_device_fingerprint) = 32))
        OR (state = 'suspended' AND (
            octet_length(provider_device_fingerprint) = 32
            OR (tenant_suspend_prior_state = 'unpaired' AND provider_device_fingerprint IS NULL)
        ))
        OR (state IN ('connected', 'degraded', 'reauthorization-required', 'disconnected')
            AND octet_length(provider_device_fingerprint) = 32)
    );

CREATE TABLE tenant_admin_events (
    tenant_id text NOT NULL,
    event_id text NOT NULL,
    action text NOT NULL CHECK (action IN ('provision', 'update', 'suspend', 'resume')),
    actor text NOT NULL CHECK (octet_length(actor) BETWEEN 1 AND 128),
    tenant_name text NOT NULL CHECK (octet_length(tenant_name) BETWEEN 1 AND 256),
    max_connections integer NOT NULL CHECK (max_connections BETWEEN 1 AND 128),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id)
);

CREATE INDEX tenant_admin_events_time
    ON tenant_admin_events (tenant_id, occurred_at DESC, event_id DESC);

ALTER TABLE tenant_admin_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_admin_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_admin_events_tenant_isolation ON tenant_admin_events
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
