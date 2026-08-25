# SirenaIX gateway runtime configuration

`cmd/sirenaix-gateway` is the fail-closed SirenaIX process. It does not run
database migrations at startup: deployment automation must apply the checked-in
migrations with a DDL role before starting the gateway, while the gateway uses a
separate application role subject to the tenant RLS policies.

The migration runner hashes each file's original bytes and applies files in
lexical order on one dedicated database session. Every migration and its ledger
row commit in one runner-owned transaction. A lexical scanner rejects
top-level transaction commands hidden by spacing, comments, or same-line SQL;
only the exact immutable hashes of the three previously shipped files with a
plain outer `BEGIN`/`COMMIT` envelope are recognized, and only that envelope is
removed before the runner supplies the atomic transaction. Transaction words
inside literals, comments, and dollar-quoted function bodies are ignored.

Production requires all of the following:

- `SIRENAIX_ENVIRONMENT=production`, an explicit bounded `SIRENAIX_OWNER_ID`,
  `SIRENAIX_HTTP_ADDRESS`, and a non-empty comma-separated `SIRENAIX_TENANTS`.
- `SIRENAIX_DATABASE_URL` for the application role.
- `SIRENAIX_OIDC_ISSUER`, `SIRENAIX_OIDC_AUDIENCE`, and
  `SIRENAIX_OIDC_TENANT_CLAIM`. Plain HTTP issuers are accepted only for an
  explicitly enabled loopback issuer in development.
- `SIRENAIX_KEY_BACKEND=aws-kms`, `SIRENAIX_KMS_KEYS` in
  `version=AWS-KMS-key-ID` form,
  `SIRENAIX_KMS_CURRENT_VERSION`, and `SIRENAIX_AWS_REGION`. There is no
  implicit plaintext or local-key production fallback.
- `SIRENAIX_OBJECT_BACKEND=s3`, `SIRENAIX_S3_BUCKET`, and AWS credentials from
  the normal SDK chain. AWS S3 endpoints require
  `SIRENAIX_S3_EXPECTED_BUCKET_OWNER`; the explicit missing-owner exception is
  limited to a configured custom S3-compatible endpoint. The `local` backend is
  development-only and requires `SIRENAIX_DEV_OBJECT_ROOT` to name a
  pre-existing, owner-private (`0700` on POSIX) directory whose ancestors are
  not links or reparse points. The gateway never creates this trusted root.
- A non-empty allowlist in `SIRENAIX_PROVIDER_MEDIA_HOSTS`.
- A server certificate/key pair, or the explicit
  `SIRENAIX_ALLOW_PLAIN_HTTP_BEHIND_TLS_PROXY=true` declaration when a trusted
  local reverse proxy terminates TLS.

Media byte and pixel limits may be lowered with `SIRENAIX_MAX_MEDIA_BYTES` and
`SIRENAIX_MAX_MEDIA_PIXELS`. The byte limit can never exceed the hard 25 MiB
cap. `SIRENAIX_MAX_WEBHOOK_ENDPOINTS` configures the bounded per-tenant
endpoint quota (default 32, hard maximum 128). `SIRENAIX_MEDIA_TEMP_DIRECTORY`
selects a private temporary staging root.

Provider ACK HTTP is a single-attempt, transactionally fenced operation.
`SIRENAIX_ACK_TIMEOUT` defaults to 4 seconds and may be set from 100 ms through
4 seconds; the repository independently caps the entire lock-holding operation
at 5 seconds and reserves its final second for the local ACK commit.
`SIRENAIX_ACK_CONCURRENCY` defaults to 8 and cannot exceed 16. The executable's
32-connection database pool therefore retains capacity for lease renewal,
ingress, and API work while provider ACKs are in flight. The admission ceiling
is process-local. The v1 pilot must run exactly one gateway process: pairing
attempts and contact synchronization are owned in process, and database lease
fencing alone cannot route those operations between replicas. Scale-out needs
owner-aware RPC and is not currently supported.

Development may explicitly select `SIRENAIX_KEY_BACKEND=local` and provide an
exact 32-byte, standard-base64 master key in
`SIRENAIX_DEV_MASTER_KEY_B64`. The setting is rejected outside development,
is never generated or defaulted by the process, and should be supplied through
the local secret manager rather than checked into a file.

## Kafka trust boundary

Kafka is disabled when `SIRENAIX_KAFKA_BROKERS` is empty. Enabling tenant
command consumption requires TLS plus a client certificate and key, a consumer
group, and explicit topic bindings:

```text
SIRENAIX_KAFKA_COMMAND_TOPICS=tenant-a.commands.v1=tenant-a=producer-a,tenant-b.commands.v1=tenant-b=producer-b
```

Each command topic is a server-configured trust boundary. Broker ACLs must grant
only the named authenticated producer access to its bound topic and must prevent
other tenants from producing to it. The default event topic cannot also be a
mapped or shared command topic; that collision is rejected before a Kafka
client is created. The adapter derives tenant/principal from this mapping and
rejects a payload whose tenant differs. It never infers the producer identity
from ordinary Kafka record headers. A shared command topic is not exposed by
the executable; library callers may enable one only with the concrete
cryptographic record authenticator.

Set `SIRENAIX_KAFKA_CLIENT_ID`, `SIRENAIX_KAFKA_GROUP_ID`,
`SIRENAIX_KAFKA_CLIENT_CERT_FILE`, and `SIRENAIX_KAFKA_CLIENT_KEY_FILE` for
command mode. `SIRENAIX_KAFKA_CA_FILE` optionally adds a bounded PEM CA bundle.
All broker addresses must include an explicit port.

When Kafka is enabled, readiness uses one bounded total deadline for topic
metadata, `DescribeCluster`, and (in command mode) `DescribeGroups`; it never
publishes a probe record. The broker must support those APIs and advertise
authorized operations. The gateway identity needs `DESCRIBE` and `WRITE` for
the event topic, `DESCRIBE` and `READ` for every command topic, `READ` on the
configured consumer group, and cluster `DESCRIBE` and `IDEMPOTENT_WRITE` for
franz-go's default idempotent producer. The `DescribeCluster`
authorized-operations field itself requires cluster `DESCRIBE`, so both
cluster operations must be granted before readiness can prove producer access.
Missing topics and dependency failures use fixed redacted classes. An absent
or unsupported authorized-operations field fails closed as
`authorization_unverifiable`; an advertised ACL denial is `authorization`.
There is no v1 compatibility bypass—leave Kafka disabled until the broker can
prove all required operations.

Google Messages remains a reverse-engineered, permanent-session provider. The
gateway implements durable at-least-once ingress, idempotent commands, audit
history, and visible `uncertain` reconciliation; it does not promise provider
exactly-once sends, complete receipt availability, forced SMS/RCS selection,
explicit secondary-SIM new-chat routing, or provider media retention.
