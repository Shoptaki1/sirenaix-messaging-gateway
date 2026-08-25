-- SirenaIX durable messaging, media, webhook, and Kafka state.
-- All claims use database time and repository queries use FOR UPDATE SKIP LOCKED.

CREATE TABLE conversations (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    conversation_id text NOT NULL,
    provider_default_outgoing_id text NOT NULL DEFAULT '',
    is_group boolean NOT NULL DEFAULT false,
    committed_cursor bytea,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id, conversation_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id)
);

CREATE TABLE messages (
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    connection_id text NOT NULL,
    conversation_id text NOT NULL DEFAULT '',
    ordering_key text NOT NULL,
    direction text NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    recipient text NOT NULL DEFAULT '',
    line_id text,
    route_mode text NOT NULL DEFAULT '',
    body_text text NOT NULL DEFAULT '' CHECK (octet_length(body_text) <= 65536),
    provider_message_id text,
    provider_tmp_id text,
    transport text NOT NULL DEFAULT '' CHECK (transport IN ('', 'sms', 'mms', 'rcs')),
    current_state text NOT NULL CHECK (current_state IN (
        'queued', 'dispatching', 'provider_accepted', 'awaiting_phone',
        'sent', 'delivered', 'read', 'uncertain', 'failed'
    )),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, message_id),
    UNIQUE (tenant_id, connection_id, provider_message_id),
    UNIQUE (tenant_id, connection_id, provider_tmp_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id),
    FOREIGN KEY (tenant_id, line_id) REFERENCES lines (tenant_id, line_id)
);

CREATE TABLE message_status_history (
    tenant_id text NOT NULL,
    status_id text NOT NULL,
    message_id text NOT NULL,
    state text NOT NULL CHECK (state IN (
        'queued', 'dispatching', 'provider_accepted', 'awaiting_phone',
        'sent', 'delivered', 'read', 'uncertain', 'failed'
    )),
    provider_status text NOT NULL DEFAULT '',
    safe_reason text NOT NULL DEFAULT '',
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, status_id),
    UNIQUE (tenant_id, message_id, status_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, message_id)
);

CREATE INDEX message_status_history_order
    ON message_status_history (tenant_id, message_id, observed_at, status_id);

CREATE TABLE message_idempotency (
    tenant_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    message_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, idempotency_key),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, message_id)
);

CREATE TABLE message_lanes (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    ordering_key text NOT NULL,
    lane_token bigint NOT NULL DEFAULT 0 CHECK (lane_token >= 0),
    owner_id text,
    claimed_message_id text,
    claim_expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id, ordering_key),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id),
    FOREIGN KEY (tenant_id, claimed_message_id) REFERENCES messages (tenant_id, message_id)
);

CREATE TABLE message_attempts (
    tenant_id text NOT NULL,
    attempt_id text NOT NULL,
    message_id text NOT NULL,
    connection_id text NOT NULL,
    ordering_key text NOT NULL,
    owner_id text NOT NULL,
    lane_token bigint NOT NULL CHECK (lane_token > 0),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    phase text NOT NULL CHECK (phase IN ('claimed', 'provider_io_started', 'complete')),
    safe_result text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    provider_io_started_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, attempt_id),
    UNIQUE (tenant_id, message_id, attempt_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, message_id),
    FOREIGN KEY (tenant_id, connection_id, ordering_key) REFERENCES message_lanes (tenant_id, connection_id, ordering_key)
);

CREATE TABLE gateway_events (
    tenant_id text NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    connection_id text,
    conversation_id text NOT NULL DEFAULT '',
    canonical_body bytea NOT NULL CHECK (octet_length(canonical_body) <= 1048576),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id)
);

CREATE TABLE event_outbox (
    tenant_id text NOT NULL,
    outbox_id text NOT NULL,
    event_id text NOT NULL,
    destination text NOT NULL CHECK (destination IN ('webhook', 'kafka')),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_by text,
    claim_expires_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    published_at timestamptz,
    PRIMARY KEY (tenant_id, outbox_id),
    UNIQUE (tenant_id, event_id, destination),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES gateway_events (tenant_id, event_id)
);

CREATE INDEX event_outbox_claim_order
    ON event_outbox (destination, available_at, tenant_id, outbox_id)
    WHERE published_at IS NULL;

CREATE TABLE provider_inbox (
    tenant_id text NOT NULL,
    inbox_id text NOT NULL,
    connection_id text NOT NULL,
    provider_response_id text NOT NULL,
    envelope_digest bytea NOT NULL CHECK (octet_length(envelope_digest) = 32),
    raw_envelope bytea NOT NULL CHECK (octet_length(raw_envelope) BETWEEN 1 AND 4194304),
    poisoned boolean NOT NULL DEFAULT false,
    poison_reason text NOT NULL DEFAULT '',
    ack_pending boolean NOT NULL DEFAULT true,
    acked_at timestamptz,
    owner_id text NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, inbox_id),
    UNIQUE (tenant_id, connection_id, provider_response_id),
    UNIQUE (tenant_id, connection_id, provider_response_id, envelope_digest),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id)
);

CREATE INDEX provider_inbox_ack_pending
    ON provider_inbox (tenant_id, connection_id, received_at, inbox_id)
    WHERE ack_pending;

CREATE TABLE provider_inbox_conflicts (
    tenant_id text NOT NULL,
    conflict_id text NOT NULL,
    connection_id text NOT NULL,
    provider_response_id text NOT NULL,
    conflicting_digest bytea NOT NULL CHECK (octet_length(conflicting_digest) = 32),
    conflicting_raw_envelope bytea NOT NULL CHECK (octet_length(conflicting_raw_envelope) BETWEEN 1 AND 4194304),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, conflict_id),
    UNIQUE (tenant_id, connection_id, provider_response_id, conflicting_digest),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id, provider_response_id) REFERENCES provider_inbox (tenant_id, connection_id, provider_response_id)
);

CREATE TABLE media_objects (
    tenant_id text NOT NULL,
    media_id text NOT NULL,
    message_id text,
    object_key text,
    state text NOT NULL CHECK (state IN ('pending', 'ready', 'failed')),
    mime_type text NOT NULL DEFAULT '',
    display_filename text NOT NULL DEFAULT '',
    byte_size bigint NOT NULL DEFAULT 0 CHECK (byte_size >= 0 AND byte_size <= 26214400),
    sha256_digest bytea CHECK (sha256_digest IS NULL OR octet_length(sha256_digest) = 32),
    width integer CHECK (width IS NULL OR width > 0),
    height integer CHECK (height IS NULL OR height > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, media_id),
    UNIQUE (tenant_id, object_key),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, message_id)
);

CREATE TABLE message_media (
    tenant_id text NOT NULL,
    message_id text NOT NULL,
    media_id text NOT NULL,
    position integer NOT NULL CHECK (position >= 0),
    PRIMARY KEY (tenant_id, message_id, media_id),
    UNIQUE (tenant_id, message_id, position),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, message_id),
    FOREIGN KEY (tenant_id, media_id) REFERENCES media_objects (tenant_id, media_id)
);

CREATE TABLE media_fetch_jobs (
    tenant_id text NOT NULL,
    job_id text NOT NULL,
    media_id text NOT NULL,
    connection_id text NOT NULL,
    provider_message_id text NOT NULL,
    provider_locator text NOT NULL,
    declared_mime_type text NOT NULL DEFAULT '',
    declared_size bigint NOT NULL DEFAULT 0,
    display_filename text NOT NULL DEFAULT '',
    key_ciphertext bytea,
    key_wrapped_dek bytea,
    key_nonce bytea,
    key_id text,
    key_version integer,
    thumbnail_key_ciphertext bytea,
    thumbnail_key_wrapped_dek bytea,
    thumbnail_key_nonce bytea,
    thumbnail_key_id text,
    thumbnail_key_version integer,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'fetching', 'ready', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    owner_id text,
    claim_token bigint NOT NULL DEFAULT 0 CHECK (claim_token >= 0),
    claim_expires_at timestamptz,
    last_safe_error text NOT NULL DEFAULT '',
    PRIMARY KEY (tenant_id, job_id),
    UNIQUE (tenant_id, media_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, media_id) REFERENCES media_objects (tenant_id, media_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id)
);

CREATE INDEX media_fetch_jobs_claim_order
    ON media_fetch_jobs (available_at, tenant_id, job_id)
    WHERE state IN ('pending', 'fetching');

CREATE TABLE webhook_endpoints (
    tenant_id text NOT NULL,
    endpoint_id text NOT NULL,
    destination_url text NOT NULL,
    key_id text NOT NULL,
    secret_ciphertext bytea NOT NULL,
    secret_wrapped_dek bytea NOT NULL,
    secret_nonce bytea NOT NULL,
    secret_key_id text NOT NULL,
    secret_key_version integer NOT NULL,
    previous_key_id text,
    previous_secret_ciphertext bytea,
    previous_secret_wrapped_dek bytea,
    previous_secret_nonce bytea,
    previous_secret_key_id text,
    previous_secret_key_version integer,
    previous_valid_until timestamptz,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, endpoint_id),
    UNIQUE (tenant_id, destination_url),
    UNIQUE (tenant_id, key_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id)
);

CREATE TABLE webhook_deliveries (
    tenant_id text NOT NULL,
    delivery_id text NOT NULL,
    endpoint_id text NOT NULL,
    event_id text NOT NULL,
    canonical_body bytea NOT NULL CHECK (octet_length(canonical_body) <= 1048576),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'delivering', 'succeeded', 'dead')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    cycle_attempt_count integer NOT NULL DEFAULT 0 CHECK (cycle_attempt_count >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_by text,
    claim_expires_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, delivery_id),
    UNIQUE (tenant_id, endpoint_id, event_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, endpoint_id) REFERENCES webhook_endpoints (tenant_id, endpoint_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES gateway_events (tenant_id, event_id)
);

CREATE INDEX webhook_deliveries_claim_order
    ON webhook_deliveries (available_at, tenant_id, delivery_id)
    WHERE state IN ('pending', 'delivering');

CREATE TABLE webhook_attempts (
    tenant_id text NOT NULL,
    attempt_id text NOT NULL,
    delivery_id text NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    status_code integer,
    safe_error text NOT NULL DEFAULT '',
    PRIMARY KEY (tenant_id, attempt_id),
    UNIQUE (tenant_id, delivery_id, attempt_number),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, delivery_id) REFERENCES webhook_deliveries (tenant_id, delivery_id)
);

CREATE TABLE webhook_dlq (
    tenant_id text NOT NULL,
    dlq_id text NOT NULL,
    delivery_id text NOT NULL,
    event_id text NOT NULL,
    canonical_body bytea NOT NULL CHECK (octet_length(canonical_body) <= 1048576),
    safe_error text NOT NULL,
    dead_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    replayed_at timestamptz,
    PRIMARY KEY (tenant_id, dlq_id),
    UNIQUE (tenant_id, delivery_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, delivery_id) REFERENCES webhook_deliveries (tenant_id, delivery_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES gateway_events (tenant_id, event_id)
);

CREATE TABLE kafka_commands (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    topic text NOT NULL,
    partition_id integer NOT NULL,
    offset_id bigint NOT NULL,
    producer_identity text NOT NULL,
    idempotency_key text NOT NULL,
    correlation_id text NOT NULL DEFAULT '',
    payload_digest bytea NOT NULL CHECK (octet_length(payload_digest) = 32),
    message_id text,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, command_id),
    UNIQUE (tenant_id, topic, partition_id, offset_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, message_id) REFERENCES messages (tenant_id, message_id)
);

CREATE TABLE kafka_event_deliveries (
    tenant_id text NOT NULL,
    delivery_id text NOT NULL,
    event_id text NOT NULL,
    topic text NOT NULL,
    partition_key text NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'publishing', 'published')),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    PRIMARY KEY (tenant_id, delivery_id),
    UNIQUE (tenant_id, event_id, topic),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES gateway_events (tenant_id, event_id)
);

CREATE TABLE kafka_command_dlq (
    tenant_id text NOT NULL,
    dlq_id text NOT NULL,
    command_id text,
    topic text NOT NULL,
    partition_id integer NOT NULL,
    offset_id bigint NOT NULL,
    bounded_payload bytea NOT NULL CHECK (octet_length(bounded_payload) <= 1048576),
    safe_error text NOT NULL,
    attempt_count integer NOT NULL CHECK (attempt_count > 0),
    correlation_id text NOT NULL DEFAULT '',
    dead_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, dlq_id),
    UNIQUE (tenant_id, topic, partition_id, offset_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, command_id) REFERENCES kafka_commands (tenant_id, command_id)
);

CREATE OR REPLACE FUNCTION reject_gateway_event_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'immutable gateway event';
END;
$$;

CREATE TRIGGER gateway_events_immutable_update
BEFORE UPDATE OR DELETE ON gateway_events
FOR EACH ROW EXECUTE FUNCTION reject_gateway_event_mutation();

-- Fail closed tenant isolation for every new data family.
ALTER TABLE conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversations FORCE ROW LEVEL SECURITY;
CREATE POLICY conversations_tenant_isolation ON conversations
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages FORCE ROW LEVEL SECURITY;
CREATE POLICY messages_tenant_isolation ON messages
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE message_status_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE message_status_history FORCE ROW LEVEL SECURITY;
CREATE POLICY message_status_history_tenant_isolation ON message_status_history
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE message_idempotency ENABLE ROW LEVEL SECURITY;
ALTER TABLE message_idempotency FORCE ROW LEVEL SECURITY;
CREATE POLICY message_idempotency_tenant_isolation ON message_idempotency
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE message_lanes ENABLE ROW LEVEL SECURITY;
ALTER TABLE message_lanes FORCE ROW LEVEL SECURITY;
CREATE POLICY message_lanes_tenant_isolation ON message_lanes
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE message_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE message_attempts FORCE ROW LEVEL SECURITY;
CREATE POLICY message_attempts_tenant_isolation ON message_attempts
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE gateway_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE gateway_events FORCE ROW LEVEL SECURITY;
CREATE POLICY gateway_events_tenant_isolation ON gateway_events
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE event_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_outbox FORCE ROW LEVEL SECURITY;
CREATE POLICY event_outbox_tenant_isolation ON event_outbox
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE provider_inbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_inbox_tenant_isolation ON provider_inbox
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE provider_inbox_conflicts ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_inbox_conflicts FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_inbox_conflicts_tenant_isolation ON provider_inbox_conflicts
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE media_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE media_objects FORCE ROW LEVEL SECURITY;
CREATE POLICY media_objects_tenant_isolation ON media_objects
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE message_media ENABLE ROW LEVEL SECURITY;
ALTER TABLE message_media FORCE ROW LEVEL SECURITY;
CREATE POLICY message_media_tenant_isolation ON message_media
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE media_fetch_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE media_fetch_jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY media_fetch_jobs_tenant_isolation ON media_fetch_jobs
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE webhook_endpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_endpoints FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_endpoints_tenant_isolation ON webhook_endpoints
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE webhook_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_deliveries_tenant_isolation ON webhook_deliveries
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE webhook_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_attempts FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_attempts_tenant_isolation ON webhook_attempts
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE webhook_dlq ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_dlq FORCE ROW LEVEL SECURITY;
CREATE POLICY webhook_dlq_tenant_isolation ON webhook_dlq
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE kafka_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE kafka_commands FORCE ROW LEVEL SECURITY;
CREATE POLICY kafka_commands_tenant_isolation ON kafka_commands
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE kafka_event_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE kafka_event_deliveries FORCE ROW LEVEL SECURITY;
CREATE POLICY kafka_event_deliveries_tenant_isolation ON kafka_event_deliveries
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE kafka_command_dlq ENABLE ROW LEVEL SECURITY;
ALTER TABLE kafka_command_dlq FORCE ROW LEVEL SECURITY;
CREATE POLICY kafka_command_dlq_tenant_isolation ON kafka_command_dlq
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
