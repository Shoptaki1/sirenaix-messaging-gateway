# Test result evidence

Physical pilot status: **not executed**. The 5-, 20-, and 50-phone waves and
the 72-hour/seven-day soaks have no results yet.

## Automated 1,000-actor simulation

The build-tagged `TestSimulated1000Actors` uses the real connection actor
`Pool` and `Actor` boundaries. It exercises 1,000 distinct tenant/connection
owners, duplicate start admission, fenced lease acquisition and renewal,
provider readiness, actor operation queues, deterministic full-jitter
backoff, 100 reconnects released in waves of 10, cancellation, lease release,
and bounded shutdown.

The deterministic substitutes are the Google/provider connection, PostgreSQL
persistence, encrypted session storage, and wall-clock timers. The test makes
no Google or carrier network call. It does **not** prove Google authorization
longevity, real SMS/MMS/RCS behavior, carrier or dual-SIM routing, PostgreSQL
contention, KMS behavior, or multi-replica failover. Those claims require the
physical matrix and, for failover, a later multi-replica test.

The test records duration, operation and shutdown duration, goroutines, heap
allocation before/after two garbage collections, maximum active generations,
and remaining leases. It fails if retained goroutines grow by more than 64 or
retained Go heap grows by more than 192 MiB. These deliberately conservative
limits detect stuck workers or gross retention without turning normal runner
variation into a flaky performance benchmark.

Run the deterministic local gate:

```sh
go test -tags=loadtest -count=1 -v -timeout=10m \
  -run '^TestSimulated1000Actors$' ./internal/gateway/connectionactor
```

The required Linux CI gate adds the race detector:

```sh
go test -race -tags=loadtest -count=1 -v -timeout=9m \
  -run '^TestSimulated1000Actors$' ./internal/gateway/connectionactor
```

The workflow has a ten-minute job limit and uploads a redacted JSON summary.
The JSON contains aggregate counts, timings, runtime platform, source revision,
goroutine/heap samples, and pass status only. It contains no tenant/connection
identifiers, phone numbers, message/contact data, media, or credentials.

## Final release integration contract

The `release_contract` build tag adds a machine check for the separately
developed `.github/workflows/release.yml`. On this isolated branch the
integration test reports a visible skip because that file is absent; mutation
fixtures still prove that missing commands and publication dependencies fail.

Final integration must add a release job with the exact ID `load-race`, a
ten-minute job limit, and this command:

```sh
go test -race -tags=loadtest -count=1 -v -timeout=9m \
  -run '^TestSimulated1000Actors$' ./internal/gateway/connectionactor
```

That job must run independently on `ubuntu-24.04` with exactly three steps:
the reviewed pinned checkout action, the reviewed pinned Go 1.26.6 setup
action, and the command above. The contract rejects job/step conditions,
`continue-on-error`, custom environments/defaults/shells/working directories,
extra setup commands, shell operators or substitutions, redirects, additional
physical command lines, duplicate/overriding flags, and alternate test or
package selections.

Both `provenance.needs` and `publish.needs` must directly include `load-race`.
The release workflow's policy job must run the contract so a future edit cannot
silently remove the gate:

```sh
go test -tags=release_contract -count=1 \
  ./internal/gateway/connectionactor -run '^TestRelease'
```

The policy invocation must be the first run step immediately after the same
reviewed checkout and setup-go steps, with no condition, error masking, shell,
working-directory, or environment override.

Until that patch is present and the contract passes against the merged
`release.yml`, provenance and publication are not approved integration gates.

## Recorded results

| Date (UTC) | Source commit | Platform | Race | Status | Evidence |
|---|---|---|---|---|---|
| 2026-08-25 | `5095585818fa34dd51032e7ee6f8898318afb3a9` | Windows/amd64 local | No | Passed | [Redacted JSON](2026-08-25-windows-amd64.json) |
| _pending_ | _pending_ | Linux/amd64 CI | Yes | Not executed | GitHub Actions artifact pending |

The local Windows host has no C compiler, so Go could not build a race-enabled
binary (`-race` requires CGO and `gcc` was unavailable). The non-race local
result is evidence for deterministic behavior only and does not replace the
required Linux race job.

Add evidence only for an actually executed command. A result file must be the
unmodified JSON emitted by the test and named `YYYY-MM-DD-<platform>.json`.
Failed or interrupted runs remain failures; do not manufacture a passing JSON
file. Link physical results through [device-matrix.md](../device-matrix.md)
without copying sensitive evidence into this repository.
