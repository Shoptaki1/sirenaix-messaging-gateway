# Task 8 product verification

Date: 2026-08-25
Product commits:

- Task 8 implementation: `8ec9cff50858905283c797bebfe943d74e8191c4`
- Task 8 review hardening: `1bb717be1d1dc00f30422443493d629a98f36170`
- Task 8 semantic actionability and line-lifecycle hardening: `74ac8e64898d42acbcda38b07842d4834a5a7dba`

## Delivered contract

- Durable gateway events now use a deterministic version 1 body shared unchanged by the webhook and Kafka outboxes. `message.received` includes the tenant, connection, conversation, internal and provider message IDs, direction, exact canonical sender/recipients when supplied, text, transport, persisted status, and safe attachment identifiers/metadata without provider fetch locators or keys.
- Only a genuinely live inbound provider update with exact status `INCOMING_COMPLETE` can establish actionable `message.received`. `INCOMING_DELIVERED` and `INCOMING_DISPLAYED` only enrich an existing message non-actionably; when the message is absent they import/update it without ever establishing actionability. `LIST_MESSAGES` history and restart backlog become non-actionable `message.imported`; phone/manual outgoing updates become `message.updated`. libgm assigns restart provenance before the durable callback and retains the backlog counter when persistence fails. Correlated RPC responses do not consume that counter.
- Actionability depends on the absence of the stable semantic `message.received` event and outboxes, not on whether the message row was first inserted in that envelope. A history or pending-MMS row may therefore be promoted exactly once by a later genuinely live `INCOMING_COMPLETE`. The actionable event ID is derived from the tenant and durable internal message identity, not the provider response ID. Different response IDs, concurrent delivery, and restart converge to one `message.received` event and exactly two destination outboxes; delivered/displayed enrichment uses deterministic non-actionable update identities.
- Authenticated provider direction may refine a durable message only from `unknown` to `inbound` or `outbound`, atomically with the rest of the envelope. A known direction may repeat unchanged; a conflicting known direction fails the transaction and withholds ACK.
- `media.pending` is rebuilt only after `media_id` allocation and includes the internal/provider message IDs, conversation, pending status, safe declared metadata, and tenant-authenticated metadata path. Ready/failed events include fetch paths/status; terminal failure events contain only a bounded safe reason token.
- Connection reauthorization transitions emit one durable event and both outbox destinations in the same tenant transaction. Existing legacy `reauthorization-required` rows are repaired by the migration, and both fenced and control-plane retries idempotently repair a missing event/outbox without changing the stored event ID.
- `PUT /v1/contacts` idempotently converges a tenant contact by an exact canonical E.164 phone number. An omitted alias preserves the current value; `null` or an empty value clears it. Provider names and server label links survive later provider contact sync, and this API never writes to the phone.
- Primary encrypted Google `GET_UPDATES` Settings events carry explicit authenticated provenance into the gateway runtime. A complete valid SIM snapshot is written in the same fenced inbox transaction that determines ACK eligibility; there is no post-ACK event-handler write. Empty or unauthenticated snapshots cannot erase the last known-good lines, stale generations cannot mutate inbox or line state, and an impossible/partial snapshot is durably quarantined with ACK recovery disabled across restart.
- Authenticated Settings snapshots are capped at 16 before line allocation or SQL. Exactly 16 valid lines are preserved without truncation. Provider timestamps are interpreted using libgm's actual microsecond protocol unit and are used only inside a conservative 2000–2200 trust window; ingestion time remains separate.
- Settings replacement upserts the current lines before retiring every absent row as inactive rather than deleting it, preserving any durable message foreign key. Inactive rows are excluded from line listing, lookup, routing assertions, and new sends. Identical refreshes preserve stable line IDs, and stale fencing still prevents all line mutation.

## Dual-SIM capability truth

- Supported: discover, persist, and list multiple SIM lines/numbers from an authenticated Google Settings snapshot; keep line IDs distinct by tenant, connection, and provider participant identity.
- Supported: for an existing conversation, an explicitly requested line is accepted only when its provider outgoing identity exactly matches Google's `defaultOutgoingID` for that conversation.
- Not supported by the observed Google create-conversation request: selecting the originating SIM for a new conversation. An explicit line is rejected before provider I/O; omitting it delegates routing to the phone default.
- The REST response reports this as `existing_conversation_match_only`, `new_conversation_line_selection: false`, and `new_conversation_route: phone_default`. No secondary-SIM routing field is invented.

## Migration adoption contract

The integrated migration is `0017_task8_line_metadata.sql`. Its former outer `BEGIN`/`COMMIT` wrapper was removed because the production migration runner supplies the atomic transaction. The immutable legacy-digest allowlist was not extended.

An adoption probe must require all of the following structures; a partial match must fail closed:

- `carrier_name text NOT NULL DEFAULT ''`
- `color_hex text NOT NULL DEFAULT ''`
- `rcs_enabled boolean NOT NULL DEFAULT false`
- `provider_sim_number integer NOT NULL DEFAULT 0`
- `provider_sim_payload_type integer NOT NULL DEFAULT 0`
- `discovery_source text NOT NULL DEFAULT 'legacy_unknown'`
- `active boolean NOT NULL DEFAULT true`
- validated constraint `lines_carrier_name_boundary`: `octet_length(carrier_name) <= 255`
- validated constraint `lines_color_hex_boundary`: `octet_length(color_hex) <= 64`
- validated constraint `lines_discovery_source_check`: exactly `legacy_unknown` or `authenticated_google_settings`
- validated `messages_direction_check`: exactly `inbound`, `outbound`, or `unknown`
- RLS remains enabled and forced on `connections`, `gateway_events`, and `event_outbox` after the legacy reauthorization repair

The migration intentionally uses ordinary `ADD COLUMN` and `ADD CONSTRAINT` statements rather than `IF NOT EXISTS`. The migration runner's structural adoption probe is the sole safe way to recognize an already-applied migration; unexpected partial or incompatible structures must not be silently accepted.

## Verification evidence

Passed:

```text
go test -count=1 ./internal/gateway/... ./pkg/libgm/... ./cmd/sirenaix-gateway
go vet ./internal/gateway/... ./pkg/libgm/... ./cmd/sirenaix-gateway
go test -tags=postgres_integration -run '^$' ./internal/gateway/...
go test -tags=postgres_integration -count=1 ./internal/gateway/...
go test ./pkg/libgm ./internal/gateway/provider/gmessages ./internal/gateway/store/postgres ./internal/gateway/ingress -run 'RestartBacklog|SemanticMessageDedupe|LiveCompletePromotes|DeliveredOrDisplayed|RefinesOnlyUnknown|RejectsKnownProviderDirection|AuthenticatedLineSnapshot|AtomicallyReplacesAuthenticatedLineSnapshot|ReplaceLines|GetLineExcludes|ActionableEventOnly' -count=20
git diff --check
```

The PostgreSQL-tagged suite compiled and its unit portions passed. The live Postgres cases explicitly skipped because `SIRENAIX_POSTGRES_TEST_DSN` was not set. Their Task 8 scenarios cover server-first contact convergence and isolation, atomic/fenced Settings line persistence, FK-safe replacement with a message referencing a retired line, terminal Settings recovery, history/pending-to-live promotion, restart/concurrent semantic message deduplication, identical actionable webhook/Kafka bodies, and legacy reauthorization event/outbox repair.

The repository-wide `go test ./cmd/...` check reaches the SirenaIX gateway successfully but cannot build the unrelated legacy `cmd/mautrix-gmessages` target in this Windows environment: `CGO_ENABLED=0`, no `gcc` is installed, and the upstream Matrix bridge's SQLite dependency consequently has no `sqlite3.Error` definitions. The checked-in CI target intentionally uses `./cmd/sirenaix-gateway`; no Task 8 file changes the legacy command or its dependency graph. A focused race run was also unavailable because Go's race detector requires CGO in this environment; the concurrency-sensitive tests were instead repeated 20 times.

Strict test-driven development was used for each contract slice. Round 2 observed red tests for restart provenance arriving after the durable boundary, consumption of the restart counter on a failed persistence attempt, ambiguous pending-media linkage, live/history/manual direction semantics, response-scoped message deduplication, non-atomic Settings writes, terminal Settings ACK recovery, and legacy reauthorization delivery before the corresponding fixes. Round 3 observed red tests for delivered/displayed actionability, history/pending rows blocking later live promotion, response/restart/concurrent duplicate outboxes, unknown-direction refinement, conflicting known directions, inactive-line routing/listing, and destructive line replacement violating a referenced message foreign key before the corresponding fixes.
