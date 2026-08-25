ALTER TABLE lines
    ADD COLUMN carrier_name text NOT NULL DEFAULT '',
    ADD COLUMN color_hex text NOT NULL DEFAULT '',
    ADD COLUMN rcs_enabled boolean NOT NULL DEFAULT false,
    ADD COLUMN provider_sim_number integer NOT NULL DEFAULT 0,
    ADD COLUMN provider_sim_payload_type integer NOT NULL DEFAULT 0,
    ADD COLUMN discovery_source text NOT NULL DEFAULT 'legacy_unknown',
    ADD COLUMN active boolean NOT NULL DEFAULT true;

ALTER TABLE lines
    ADD CONSTRAINT lines_carrier_name_boundary CHECK (octet_length(carrier_name) <= 255),
    ADD CONSTRAINT lines_color_hex_boundary CHECK (octet_length(color_hex) <= 64),
    ADD CONSTRAINT lines_discovery_source_check
        CHECK (discovery_source IN ('legacy_unknown', 'authenticated_google_settings'));

-- Provider status is authenticated, but future/unknown status values must not
-- be rewritten as inbound or outbound merely to satisfy storage. The event is
-- non-actionable until its direction is known.
ALTER TABLE messages
    DROP CONSTRAINT messages_direction_check,
    ADD CONSTRAINT messages_direction_check
        CHECK (direction IN ('inbound', 'outbound', 'unknown'));

-- Rows that entered reauthorization-required before the durable event contract
-- existed already have a stable, tenant-scoped reauthorization_event_id. Repair
-- their event and both destination outboxes in-place. The application owner is
-- normally subject to FORCE RLS, so temporarily remove FORCE (while retaining
-- RLS) inside this transaction exactly as the earlier legacy reconciliations do.
ALTER TABLE connections ENABLE ROW LEVEL SECURITY;
ALTER TABLE gateway_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE connections NO FORCE ROW LEVEL SECURITY;
ALTER TABLE gateway_events NO FORCE ROW LEVEL SECURITY;
ALTER TABLE event_outbox NO FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM connections
        WHERE state = 'reauthorization-required'
          AND (
              reauthorization_event_id IS NULL
              OR reauthorization_event_id <> btrim(reauthorization_event_id)
              OR octet_length(reauthorization_event_id) = 0
              OR octet_length(reauthorization_event_id) > 256
              OR position(chr(10) IN reauthorization_event_id) > 0
              OR position(chr(13) IN reauthorization_event_id) > 0
          )
    ) THEN
        RAISE EXCEPTION 'unsafe legacy reauthorization event identity';
    END IF;
END
$$;

INSERT INTO gateway_events (
    tenant_id, event_id, event_type, aggregate_type, aggregate_id,
    connection_id, conversation_id, canonical_body
)
SELECT connection.tenant_id,
       connection.reauthorization_event_id,
       'connection.reauthorization_required',
       'connection',
       connection.connection_id,
       connection.connection_id,
       '',
       convert_to(jsonb_build_object(
           'event_id', connection.reauthorization_event_id,
           'type', 'connection.reauthorization_required',
           'version', 1,
           'occurred_at', to_char(connection.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
           'tenant_id', connection.tenant_id,
           'connection_id', connection.connection_id,
           'status', 'reauthorization-required',
           'state', 'reauthorization-required'
       )::text, 'UTF8')
FROM connections AS connection
WHERE connection.state = 'reauthorization-required'
ON CONFLICT (tenant_id, event_id) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM connections AS connection
        LEFT JOIN gateway_events AS event
          ON event.tenant_id = connection.tenant_id
         AND event.event_id = connection.reauthorization_event_id
        WHERE connection.state = 'reauthorization-required'
          AND (
              event.event_id IS NULL
              OR event.event_type <> 'connection.reauthorization_required'
              OR event.aggregate_type <> 'connection'
              OR event.aggregate_id <> connection.connection_id
              OR event.connection_id IS DISTINCT FROM connection.connection_id
          )
    ) THEN
        RAISE EXCEPTION 'legacy reauthorization event identity conflict';
    END IF;
END
$$;

INSERT INTO event_outbox (tenant_id, outbox_id, event_id, destination)
SELECT connection.tenant_id,
       connection.reauthorization_event_id || ':' || destination.name,
       connection.reauthorization_event_id,
       destination.name
FROM connections AS connection
CROSS JOIN unnest(ARRAY['webhook'::text, 'kafka'::text]) AS destination(name)
WHERE connection.state = 'reauthorization-required'
ON CONFLICT (tenant_id, event_id, destination) DO NOTHING;

ALTER TABLE connections FORCE ROW LEVEL SECURITY;
ALTER TABLE gateway_events FORCE ROW LEVEL SECURITY;
ALTER TABLE event_outbox FORCE ROW LEVEL SECURITY;
