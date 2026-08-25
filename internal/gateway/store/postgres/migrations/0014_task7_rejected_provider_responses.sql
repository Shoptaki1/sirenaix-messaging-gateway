-- Once a pagination scope is durably exhausted, retain only the bounded ACK
-- identity/digest needed for poison convergence. Full provider envelopes are
-- never inserted into provider_inbox after the sticky circuit breaker trips.
CREATE TABLE provider_rejected_responses (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    provider_response_id text NOT NULL,
    envelope_digest bytea NOT NULL CHECK (octet_length(envelope_digest) = 32),
    reason text NOT NULL CHECK (reason = 'provider_cursor_budget_exhausted'),
    ack_pending boolean NOT NULL DEFAULT true,
    acked_at timestamptz,
    conflicted boolean NOT NULL DEFAULT false,
    occurrence_count bigint NOT NULL DEFAULT 1 CHECK (occurrence_count BETWEEN 1 AND 2147483647),
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id, provider_response_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CHECK (sirenaix_valid_provider_response_id(provider_response_id)),
    CHECK (acked_at IS NULL OR NOT ack_pending),
    CHECK (NOT conflicted OR NOT ack_pending)
);

CREATE INDEX provider_rejected_responses_ack_pending
    ON provider_rejected_responses (tenant_id, connection_id, first_seen_at, provider_response_id)
    WHERE ack_pending AND NOT conflicted;

ALTER TABLE provider_rejected_responses ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_rejected_responses FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_rejected_responses_tenant_isolation ON provider_rejected_responses
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
