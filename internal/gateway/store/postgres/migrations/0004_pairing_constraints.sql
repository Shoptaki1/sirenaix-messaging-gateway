-- Phase 2 installs the fail-closed contracts without scanning/validating the
-- whole table in the same migration as reconciliation.
ALTER TABLE connections
    ADD CONSTRAINT connections_fingerprint_matches_state CHECK (
        (state = 'unpaired' AND provider_device_fingerprint IS NULL)
        OR (state = 'pairing' AND (provider_device_fingerprint IS NULL OR octet_length(provider_device_fingerprint) = 32))
        OR (state IN ('connected', 'degraded', 'reauthorization-required', 'suspended', 'disconnected')
            AND octet_length(provider_device_fingerprint) = 32)
    ) NOT VALID,
    ADD CONSTRAINT connections_reauthorization_event_required CHECK (
        state <> 'reauthorization-required'
        OR (reauthorization_event_id IS NOT NULL AND length(btrim(reauthorization_event_id)) > 0)
    ) NOT VALID,
    ADD CONSTRAINT connections_pairing_metadata_required CHECK (
        (state = 'pairing'
            AND pairing_attempt_id IS NOT NULL
            AND length(pairing_attempt_id) BETWEEN 8 AND 128
            AND pairing_attempt_id ~ '^[A-Za-z0-9_-]+$'
            AND pairing_prior_state IN ('unpaired', 'reauthorization-required')
            AND pairing_started_at IS NOT NULL)
        OR (state <> 'pairing'
            AND pairing_attempt_id IS NULL
            AND pairing_prior_state IS NULL
            AND pairing_started_at IS NULL)
    ) NOT VALID;

ALTER TABLE connection_sessions
    ADD CONSTRAINT connection_sessions_revision_positive CHECK (
        revision IS NOT NULL AND revision > 0
    ) NOT VALID;
