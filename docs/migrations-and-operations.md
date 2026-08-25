# Migrations and operations

The service never migrates its database during `serve`. Run migrations with a
separate DDL credential:

```text
SIRENAIX_MIGRATION_DATABASE_URL=... sirenaix-gateway migrate status --check
SIRENAIX_MIGRATION_DATABASE_URL=... sirenaix-gateway migrate up
```

The runner holds a PostgreSQL advisory lock on one dedicated connection,
verifies the ordered filename and SHA-256 ledger, and commits each migration
and its ledger row together. It rejects gaps, duplicate versions, checksum
drift, and a database newer than the binary. Lock and statement timeouts are
bounded by `SIRENAIX_MIGRATION_LOCK_TIMEOUT` and
`SIRENAIX_MIGRATION_STATEMENT_TIMEOUT`.

Before database access, a lexical preflight rejects top-level `BEGIN`, `START
TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, and `PREPARE TRANSACTION`
forms, including prepared-transaction completion. Words inside SQL strings,
comments, identifiers, and dollar-quoted function bodies are ignored. Only the
byte-hash-locked outer envelopes in three already-shipped migrations receive
the documented compatibility treatment; new migration transaction control is
always rejected.

The migration role must own or be allowed to alter the gateway schema. Grant
the runtime role only its required table operations plus read access to
`sirenaix_schema_migrations`; the runner does not guess a deployment-specific
role name or grant privileges to `PUBLIC`.

An installation that previously ran the shipped SQL files without the ledger
must first be backed up and inspected. `migrate up --adopt-existing` is an
explicit one-time operation: it records checksums only after every known
schema marker is present. It refuses partial schemas. Do not use adoption to
hide a failed or hand-edited migration. Adoption also verifies the exact
canonical `USING` and `WITH CHECK` tenant expression for every RLS table and
rejects missing, additional, permissive cross-tenant, or wrong-setting
policies.

Tenant administration uses a separate `SIRENAIX_ADMIN_DATABASE_URL` and a
bounded, non-secret `SIRENAIX_ADMIN_ACTOR` audit identity:

```text
sirenaix-gateway tenant provision --id tenant-a --name "Tenant A" --max-connections 16
sirenaix-gateway tenant status --id tenant-a
sirenaix-gateway tenant suspend --id tenant-a
sirenaix-gateway tenant resume --id tenant-a
```

Provisioning is idempotent. Suspension refuses an active pairing, fences live
connection leases, and preserves restorable connection state. Destructive
tenant deletion is deliberately unsupported.

`SIRENAIX_OPS_ADDRESS` hosts only `/livez`, `/readyz`, and `/metrics` on a
listener separate from the customer API. Readiness requires the supervisor,
database, current schema, and configured Kafka/S3 dependencies. A disconnected
phone affects actor metrics and connection health, not whole-service readiness.
Metrics use fixed label enumerations and never tenant IDs, phone numbers,
connection IDs, topic names, database URLs, or request paths. Readiness logs
and counters expose only a fixed dependency and failure class; migration and
tenant CLI stderr likewise maps internal failures to a fixed redacted class.

Database pool defaults are 32 open and 8 idle connections. Safe overrides are
`SIRENAIX_DB_MAX_OPEN_CONNS`, `SIRENAIX_DB_MAX_IDLE_CONNS`,
`SIRENAIX_DB_CONN_MAX_LIFETIME`, and `SIRENAIX_DB_CONN_MAX_IDLE_TIME`.
