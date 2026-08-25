-- A durable per-connection/scope advance budget bounds pagination even when a
-- malicious provider cycle is larger than the short fingerprint history and
-- the process restarts between every page. Exceeding the budget is provider-
-- local poison; database errors remain shared-infrastructure failures.
CREATE TABLE provider_cursor_budgets (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    cursor_scope text NOT NULL,
    accepted_advances integer NOT NULL DEFAULT 0,
    exhausted boolean NOT NULL DEFAULT false,
    last_provider_response_id text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, connection_id, cursor_scope),
    FOREIGN KEY (tenant_id, connection_id) REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CHECK (accepted_advances >= 0 AND accepted_advances <= 256),
    CHECK (octet_length(cursor_scope) BETWEEN 1 AND 512),
    CHECK (octet_length(last_provider_response_id) BETWEEN 1 AND 256)
);

ALTER TABLE provider_cursor_budgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_cursor_budgets FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_cursor_budgets_tenant_isolation ON provider_cursor_budgets
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
