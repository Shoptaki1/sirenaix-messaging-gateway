CREATE TABLE tenants (
    tenant_id text NOT NULL,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id)
);

CREATE TABLE connections (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    name text NOT NULL,
    state text NOT NULL CHECK (state IN (
        'unpaired', 'pairing', 'connected', 'degraded',
        'reauthorization-required', 'suspended', 'disconnected'
    )),
    provider_device_fingerprint bytea NOT NULL CHECK (octet_length(provider_device_fingerprint) >= 16),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connection_id),
    UNIQUE (tenant_id, provider_device_fingerprint),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE TABLE lines (
    tenant_id text NOT NULL,
    line_id text NOT NULL,
    connection_id text NOT NULL,
    provider_participant_id text NOT NULL,
    provider_outgoing_id text NOT NULL,
    normalized_phone text NOT NULL,
    display_name text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, line_id),
    UNIQUE (tenant_id, connection_id, line_id),
    UNIQUE (tenant_id, connection_id, provider_participant_id, provider_outgoing_id),
    FOREIGN KEY (tenant_id, connection_id)
        REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE TABLE connection_sessions (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    ciphertext bytea NOT NULL CHECK (octet_length(ciphertext) > 0),
    wrapped_dek bytea NOT NULL CHECK (octet_length(wrapped_dek) > 0),
    nonce bytea NOT NULL CHECK (octet_length(nonce) > 0),
    key_id text NOT NULL CHECK (length(key_id) > 0),
    key_version integer NOT NULL CHECK (key_version > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connection_id),
    FOREIGN KEY (tenant_id, connection_id)
        REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE TABLE contacts (
    tenant_id text NOT NULL,
    contact_id text NOT NULL,
    normalized_phone text NOT NULL,
    server_alias text NOT NULL DEFAULT '',
    provider_display_name text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, contact_id),
    UNIQUE (tenant_id, normalized_phone),
    UNIQUE (tenant_id, contact_id, normalized_phone),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE TABLE provider_contact_sources (
    tenant_id text NOT NULL,
    connection_id text NOT NULL,
    provider_contact_id text NOT NULL,
    contact_id text NOT NULL,
    normalized_phone text NOT NULL,
    provider_display_name text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connection_id, provider_contact_id),
    FOREIGN KEY (tenant_id, connection_id)
        REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, contact_id, normalized_phone)
        REFERENCES contacts (tenant_id, contact_id, normalized_phone) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE INDEX provider_contact_sources_contact_fk_idx
    ON provider_contact_sources (tenant_id, contact_id, normalized_phone);

CREATE TABLE labels (
    tenant_id text NOT NULL,
    label_id text NOT NULL,
    name text NOT NULL,
    normalized_slug text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, label_id),
    UNIQUE (tenant_id, normalized_slug),
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE TABLE contact_labels (
    tenant_id text NOT NULL,
    contact_id text NOT NULL,
    label_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, contact_id, label_id),
    FOREIGN KEY (tenant_id, contact_id)
        REFERENCES contacts (tenant_id, contact_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, label_id)
        REFERENCES labels (tenant_id, label_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE INDEX contact_labels_label_fk_idx
    ON contact_labels (tenant_id, label_id);

CREATE TABLE contact_sync_runs (
    tenant_id text NOT NULL,
    sync_run_id text NOT NULL,
    connection_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    imported_count integer NOT NULL DEFAULT 0 CHECK (imported_count >= 0),
    rejected_count integer NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    error_summary text NOT NULL DEFAULT '',
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    PRIMARY KEY (tenant_id, sync_run_id),
    UNIQUE (tenant_id, connection_id, sync_run_id),
    FOREIGN KEY (tenant_id, connection_id)
        REFERENCES connections (tenant_id, connection_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenants_tenant_isolation ON tenants
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE connections FORCE ROW LEVEL SECURITY;
CREATE POLICY connections_tenant_isolation ON connections
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE lines ENABLE ROW LEVEL SECURITY;
ALTER TABLE lines FORCE ROW LEVEL SECURITY;
CREATE POLICY lines_tenant_isolation ON lines
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE connection_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE connection_sessions FORCE ROW LEVEL SECURITY;
CREATE POLICY connection_sessions_tenant_isolation ON connection_sessions
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts FORCE ROW LEVEL SECURITY;
CREATE POLICY contacts_tenant_isolation ON contacts
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE provider_contact_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_contact_sources FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_contact_sources_tenant_isolation ON provider_contact_sources
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE labels ENABLE ROW LEVEL SECURITY;
ALTER TABLE labels FORCE ROW LEVEL SECURITY;
CREATE POLICY labels_tenant_isolation ON labels
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE contact_labels ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_labels FORCE ROW LEVEL SECURITY;
CREATE POLICY contact_labels_tenant_isolation ON contact_labels
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));

ALTER TABLE contact_sync_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_sync_runs FORCE ROW LEVEL SECURITY;
CREATE POLICY contact_sync_runs_tenant_isolation ON contact_sync_runs
    USING (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('sirenaix.tenant_id', true), ''));
