ALTER TABLE connections
    ALTER COLUMN provider_device_fingerprint DROP NOT NULL,
    ADD COLUMN reauthorization_event_id text;

ALTER TABLE connection_sessions
    ADD COLUMN envelope_version integer NOT NULL DEFAULT 1 CHECK (envelope_version = 1),
    ADD COLUMN provider text NOT NULL DEFAULT 'gmessages' CHECK (length(provider) > 0);

ALTER TABLE connection_sessions ALTER COLUMN envelope_version DROP DEFAULT;
ALTER TABLE connection_sessions ALTER COLUMN provider DROP DEFAULT;
