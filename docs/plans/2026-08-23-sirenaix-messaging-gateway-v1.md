# SirenaIX Messaging Gateway v1 implementation plan

## Outcome

Create a public AGPL SirenaIX gateway around the Matrix-independent `pkg/libgm`
protocol engine. The gateway must support many tenants, multiple paired Android
phones per tenant, discovered dual-SIM lines, bidirectional text and media, and
one-way contact-name synchronization into tenant-scoped SirenaIX contacts. AI
aliases and labels are server-only metadata and must survive provider resyncs.

The primary path uses the official Google Messages app. It does not require a
SirenaIX Android APK, but it does require an online Android phone with Google
Messages as the default SMS app. Google sessions are durable but not permanent;
reauthorization is an explicit supported state.

## Non-negotiable safety rules

- Every persisted and API-addressable object is scoped by `tenant_id`.
- A physical paired phone is a `connection`; a discovered SIM is a `line`.
- Never silently select a different SIM. If a requested route cannot be honored,
  reject it before dispatch.
- Provider contact sync may update provider names, but never overwrites a
  SirenaIX alias or AI/user labels.
- Session credentials are envelope-encrypted and never logged or returned.
- Persist inbound data before acknowledging it to Google.
- Timed-out sends become `uncertain`; never blindly retry them.
- Exactly one fenced actor owns a provider connection at a time.

## Contact model and synchronization contract

- Canonical contact identity is `(tenant_id, normalized_phone_number)` for v1.
- Provider sources are keyed by `(tenant_id, connection_id, provider_contact_id)`.
- Display-name precedence is SirenaIX alias, then provider name, then normalized
  phone number.
- Labels are tenant-owned objects with stable IDs and normalized unique slugs.
- Sync is one way: Google Messages/phone to SirenaIX. Creating aliases or labels
  does not edit the phone address book.
- A later Google People integration may provide opt-in two-way address-book
  writes; it is not part of this protocol gateway milestone.

## Work items

### Task 1: Tenant-aware domain and contact-sync service

Build the dependency-free domain and application contracts for tenants,
connections, lines, contacts, labels, provider contact sources, and connection
auth/health states. Add a contact-sync service that is idempotent, validates the
tenant/connection boundary, imports provider names, and preserves aliases and
labels.

Acceptance:

- Tests are written and observed failing before implementation.
- One tenant may own multiple connections and each connection multiple lines.
- Duplicate provider imports converge on the canonical normalized number.
- Resync updates a provider name without changing alias or labels.
- Cross-tenant connection/contact operations fail closed.
- Invalid or empty phone numbers are quarantined, not imported.

Verification: `go test ./internal/gateway/...`

### Task 2: PostgreSQL schema and repository

Add forward-only migrations and repository implementations for tenants,
connections, lines, encrypted sessions, contacts, provider contact sources,
labels, contact-label links, and contact sync runs. Every unique key and foreign
key must include or enforce tenant ownership.

Acceptance:

- Schema contract tests cover tenant isolation and required uniqueness.
- Repository conformance tests prove idempotent upsert and metadata preservation.
- No plaintext provider credentials appear in schema fixtures or logs.

Verification: `go test ./internal/gateway/store/...`

### Task 3: Google Messages contact and line adapter

Wrap `pkg/libgm` behind a provider interface. Map `ListContacts` results into the
sync contract and map Google settings/SIM records into SirenaIX lines. Add the
conversation cursor enhancement from the MaxGhenis fork on top of current
upstream.

Acceptance:

- Adapter tests use sanitized fixtures and never contact Google.
- Contact names and normalized numbers import correctly.
- Multiple physical connections and dual-SIM settings remain distinct.
- Existing-conversation sends preserve the provider outgoing line.
- Unsupported new-chat secondary-line selection returns a typed error.

Verification: `go test ./internal/gateway/provider/... ./pkg/libgm/...`

### Task 4: Authenticated REST contact and connection API

Expose tenant-scoped connection health, lines, contacts, aliases, labels, manual
sync, and cursor pagination. Authentication is OIDC/JWT through a fail-closed
verifier; tenant identity comes from the verified principal, never request JSON.

Initial routes:

- `GET /v1/connections`
- `GET /v1/connections/{connection_id}/health`
- `GET /v1/connections/{connection_id}/lines`
- `POST /v1/connections/{connection_id}/contacts:sync`
- `GET /v1/contacts`
- `PATCH /v1/contacts/{contact_id}`
- `GET /v1/labels`
- `POST /v1/labels`
- `PUT /v1/contacts/{contact_id}/labels/{label_id}`
- `DELETE /v1/contacts/{contact_id}/labels/{label_id}`

Acceptance:

- API contract and handler tests cover 401/403, tenant isolation, validation,
  pagination, idempotency, and stable error shapes.
- AI callers can create and apply `potential-client` without provider writeback.

Verification: `go test ./internal/gateway/httpapi/...`

### Task 5: Pairing, encrypted sessions, and reauthorization

Implement the active Google-account-cookie plus emoji approval flow, encrypted
session storage, explicit device selection, safe cookie rotation, and an operator
friendly reauthorization flow. Never expose raw cookies to logs or ordinary API
responses.

Acceptance:

- State machine covers unpaired, pairing, connected, degraded,
  reauthorization-required, suspended, and disconnected.
- Restart restores encrypted sessions without laptop/browser presence.
- Provider 401 marks reauthorization-required and emits one stable event.

### Task 6: Single-owner connection actor and recovery

Replace ad hoc polling lifecycle with one cancellable actor per connection,
distributed lease/fencing, bounded exponential backoff with jitter, liveness
metrics, and clean shutdown of poll/ACK workers.

Acceptance:

- Race tests cover repeat connect/disconnect, shutdown during backoff, lease
  loss, duplicate starts, and ticker cleanup.
- Multiple service replicas cannot actively own the same phone session.

### Task 7: Durable messages, media, webhooks, and Kafka

Add durable inbox-before-ACK, deduplication, outbound idempotency and ordering,
message status reconciliation, bounded media/object storage, signed webhooks,
and Kafka command/event/dead-letter topics. SirenaIX AI and scheduling remain
outside this repository and call these interfaces.

Acceptance:

- Bidirectional SMS/MMS/RCS text and images have contract/integration tests.
- Outbound state includes `queued`, `dispatching`, `provider_accepted`,
  `awaiting_phone`, `sent`, `delivered`, `read`, `uncertain`, and `failed`.
- Crash-window tests prove inbound events are not lost before ACK.
- Media tests enforce MIME, size, timeout, host, and private-IP restrictions.

### Task 8: Deployment, observability, and pilot gates

Add public-source notices, container build, OpenAPI/AsyncAPI, health/readiness,
metrics and redaction, operator and tenant guides, plus a physical-device test
matrix. Pilot with at most 50 real connections after a multi-day auth soak;
validate at least 1,000 simulated connection actors before wider rollout.

Acceptance:

- No secrets or message contents in default logs/metric labels.
- Runbooks cover logout/reauth, phone offline, wrong default SMS app, wrong SIM,
  Google throttling, webhook backlog, and uncertain delivery.
- Android fallback provider remains a pre-GA contingency, not a pilot
  dependency.

## Baseline note

On Windows, `go test ./pkg/...` passes at upstream commit
`9743919f4884327db998fe0f227c073f3f3aceb3`. The legacy Matrix executable has a
pre-existing Windows SQLite compilation failure in the upstream `mxmain`
dependency. The new SirenaIX gateway packages must remain independently testable
and must not depend on that executable.
