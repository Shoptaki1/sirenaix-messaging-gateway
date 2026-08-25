-- A conversation-list cursor covers every child conversation/message page.
-- Keep one fenced, resumable parent checkpoint per connection and advance the
-- committed provider cursor only after all children are complete.
CREATE TABLE provider_backfill_checkpoints (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    checkpoint_id text NOT NULL CHECK (octet_length(checkpoint_id) BETWEEN 1 AND 128),
    next_cursor bytea CHECK (next_cursor IS NULL OR octet_length(next_cursor) BETWEEN 1 AND 4096),
    terminal boolean NOT NULL,
    scan_complete boolean NOT NULL DEFAULT false,
    conversation_ids text[] NOT NULL DEFAULT '{}',
    item_states text[] NOT NULL DEFAULT '{}',
    safe_errors text[] NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id),
    UNIQUE (tenant_id, connection_id, checkpoint_id),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id),
    CHECK (cardinality(conversation_ids) <= 100),
    CHECK (cardinality(conversation_ids) = cardinality(item_states)),
    CHECK (cardinality(conversation_ids) = cardinality(safe_errors)),
    CHECK (item_states <@ ARRAY['pending', 'complete', 'poisoned']::text[]),
    CHECK (octet_length(array_to_string(safe_errors, '')) <= 12800),
    CHECK (NOT scan_complete OR terminal)
);

ALTER TABLE provider_backfill_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_backfill_checkpoints FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_backfill_checkpoints_tenant_isolation ON provider_backfill_checkpoints
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
