-- Task 7 independent-review hardening. Keep canonical new-chat ordering lanes
-- after the provider assigns a conversation identity.
ALTER TABLE conversations
    ADD COLUMN ordering_key text NOT NULL DEFAULT '';

-- 0007 FORCEs RLS. The documented migration role is the table owner (not a
-- BYPASSRLS application role). Keep the temporary NO FORCE boundary inside
-- one atomic DO statement, so even a runner that executes one SQL statement
-- at a time cannot strand FORCE disabled after a failed backfill.
DO $task7_ordering_backfill$
BEGIN
    EXECUTE 'ALTER TABLE conversations NO FORCE ROW LEVEL SECURITY';
    BEGIN
        UPDATE conversations
        SET ordering_key = conversation_id
        WHERE ordering_key = '';
    EXCEPTION WHEN OTHERS THEN
        EXECUTE 'ALTER TABLE conversations FORCE ROW LEVEL SECURITY';
        RAISE;
    END;
    EXECUTE 'ALTER TABLE conversations FORCE ROW LEVEL SECURITY';
END
$task7_ordering_backfill$;

-- Every webhook lease cycle receives a monotonic token. Owner plus token is
-- the completion fence; replay and endpoint revocation both increment it.
ALTER TABLE webhook_deliveries
    ADD COLUMN claim_token bigint NOT NULL DEFAULT 0 CHECK (claim_token >= 0);
