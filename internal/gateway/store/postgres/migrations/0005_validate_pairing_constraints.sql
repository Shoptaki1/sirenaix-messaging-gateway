-- Phase 3 is deliberately separate so deployment can observe reconciliation
-- before paying the validation scan/lock cost.
ALTER TABLE connections
    VALIDATE CONSTRAINT connections_fingerprint_matches_state,
    VALIDATE CONSTRAINT connections_reauthorization_event_required,
    VALIDATE CONSTRAINT connections_pairing_metadata_required;

ALTER TABLE connection_sessions
    VALIDATE CONSTRAINT connection_sessions_revision_positive;

ALTER TABLE connection_sessions
    ALTER COLUMN revision SET NOT NULL;
