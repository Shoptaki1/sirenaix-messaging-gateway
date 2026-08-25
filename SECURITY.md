# Security policy

## Reporting a vulnerability

Use GitHub's private vulnerability reporting flow for this repository:

1. Open the repository's **Security** tab.
2. Select **Advisories**, then **Report a vulnerability**.
3. Describe the affected version, impact, and a minimal reproduction.

Do not open a public issue for a suspected vulnerability. If private
vulnerability reporting is not available, do not publish sensitive details;
wait until the maintainers provide a private GitHub reporting channel. This
project does not publish a security-reporting email address.

Never include live credentials, Google pairing/session material, access or
refresh tokens, database connection strings, webhook secrets, private keys, or
cloud credentials in a report. Do not attach real phone numbers, contact lists,
SMS/MMS/RCS content, media, tenant data, production logs, or database snapshots.
Use synthetic, redacted data. If a credential may have been exposed, revoke or
rotate it through its issuer before continuing the report.

Maintainers will acknowledge reports and coordinate next steps through the
private advisory. Response and remediation time depend on severity,
reproducibility, provider behavior, and release risk; this policy makes no fixed
response-time promise.

## Supported versions

Before the first tagged release, the default branch is development software and
is not a supported production release. After releases begin, only the latest
tagged release receives security fixes. Older releases and untagged builds are
not supported unless a release notice explicitly says otherwise.

Provider outages, Google protocol changes, revoked pairings, carrier delivery
failures, and feature requests are normally operational or compatibility issues,
not security vulnerabilities. Report them through the regular issue tracker
without including private message or account data.

## Release owner prerequisites

The release workflow re-runs every required test and supply-chain gate on the
tagged commit. Before setting the repository variable
`SIRENAIX_RELEASE_CONTROLS_ACKNOWLEDGED` to `true`, an owner must configure the
GitHub `release` environment with required reviewers, prevent self-review and
administrative bypass where the plan supports those controls, and restrict the
environment to protected `v*` tags. Tag protection or a ruleset must prevent
release tags from being moved or deleted. GitHub immutable releases must also be
enabled for the repository.

The workflow checks the acknowledgment variable and uses the protected
environment, but GitHub Actions cannot verify those repository settings before
publication. The CLI's `--verify-tag` option confirms only that the remote tag
already exists. GitHub's immutable-release protections apply after publication,
so the release job does not describe the tag or release itself as immutable.
