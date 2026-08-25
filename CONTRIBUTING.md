# Contributing

Thank you for improving SirenaIX Messaging Gateway. Changes should preserve
tenant isolation, durable delivery, privacy boundaries, and the ability to
review the fork against its mautrix/gmessages baseline.

## Development workflow

1. Add or update a focused test that fails for the missing behavior.
2. Make the smallest production change that passes the test.
3. Run formatting, focused tests, the gateway suite, and static checks.
4. Explain security, migration, compatibility, and operational effects in the
   pull request.

Use synthetic identifiers and message content in tests and examples. Never
commit real phone numbers, contacts, conversations, pairing/session data,
tokens, credentials, production logs, or database exports. Logs and test
failures must not reveal provider payloads or tenant data.

## Gateway checks

The SirenaIX boundary is intentionally narrower than the legacy Matrix bridge,
which has separate SQLite/libolm CGO and packaging requirements. Run:

```sh
go test -count=1 ./internal/gateway/... ./pkg/libgm/... ./cmd/sirenaix-gateway
go vet ./internal/gateway/... ./pkg/libgm/... ./cmd/sirenaix-gateway
```

On Linux with CGO available, run the messaging lifecycle race suite used by
`.github/workflows/go.yml`. PostgreSQL changes must also pass the tagged
integration suite with an isolated test database:

```sh
SIRENAIX_POSTGRES_TEST_DSN='postgres://...' \
  go test -count=1 -tags=postgres_integration \
  ./internal/gateway/store/postgres \
  ./internal/gateway/app \
  ./internal/gateway/provider/gmessages \
  -run TestPostgresIntegration
```

Run `./scripts/check-licenses` when dependencies change. It regenerates and
byte-compares the committed `third_party/licenses` distribution bundle; review
any differences before replacing that directory. Release archives and images
copy the reviewed bundle and do not download or generate notices at runtime.
Container changes must
pass `./scripts/verify-container IMAGE` and the container workflow's
vulnerability and SBOM gates.

## Database migrations

Applied migrations are immutable. Never edit, renumber, reuse, or delete an
existing migration. Add the next numbered migration, make it safe for rolling
deployments, and add both migration-order tests and PostgreSQL integration
coverage. Destructive or long-locking changes require an explicit rollout and
rollback explanation.

## Provider and API compatibility

Treat Google/provider payloads as untrusted and size-bound them before parsing
or persistence. Preserve durable ACK fencing, tenant ownership checks, retry
idempotency, and bounded network calls. Update the versioned API contract and
contract tests whenever public request or response behavior changes.

## Upstream attribution and license

This repository is based on mautrix/gmessages `v0.2608.0`. Keep upstream
copyright headers, the AGPL license, the baseline attribution in `NOTICE.md`,
and relevant ancestry when preparing changes for publication. Do not imply
that upstream's named license exceptions apply to SirenaIX. Contributions to
this repository are provided under the repository's AGPLv3-or-later terms.
