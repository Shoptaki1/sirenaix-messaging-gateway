# SirenaIX Messaging Gateway

This is a Matrix-independent gateway fork from `mautrix-gmessages` that exposes
tenant-aware REST/Kafka/webhook APIs for receiving and sending SMS/MMS through
Google Messages on Android devices.

It is intended for server-side AI assistants to:

- receive inbound texts/media fast in a durable store
- emit outbound replies quickly and safely
- maintain tenant, connection, labels, and contact metadata server-side
- operate with multiple Android phones and multiple SIM lines per phone where
  available

## What it is not

- It is not an official Google API product; it is reverse-engineered behavior
  over the official Google Messages app.
- It is not designed to be a high-availability multi-replica cluster yet.
- It is not a cross-platform phone bridge (iPhone is not supported here).

## Android phone requirement and APK

- This version does **not** require a custom Android APK.
- The official Google Messages app must be installed, active, and connected
  to the phone’s default SMS role.
- Pairing uses QR / cookie flow and encrypted in-gateway session storage.

If you use the same phone on iPhone in the same SIM/eSIM, this code path
does not apply. iPhone support is a separate product path.

## Multi-tenant / multi-phone model

- Tenant isolation is enforced everywhere.
- One tenant can hold multiple connections (`connection_id`).
- One connection can hold multiple discovered lines (SIM/eSIM metadata).
- Line routing is tenant-scoped and lease-fenced by connection actor ownership.

## Dual-SIM behavior

From current behavior:

- Existing conversations can explicitly pin route when the conversation
  metadata includes a matching outgoing line.
- New chats cannot yet reliably force a secondary SIM at the API level; they
  route by phone default.
- Retired/disconnected lines are preserved for historical integrity and not
  eligible for new sends.

The API returns explicit routing capability flags so your AI service can avoid
guessing line behavior when the phone/carrier can’t guarantee it.

## Contact sync and labels

- Contact import is one-way, server-owned and safe:
  - Provider contact names are synced into tenant contacts.
  - Server alias / labels are preserved and not overwritten by provider sync.
- `PUT /v1/contacts` is idempotent by normalized E.164.
- Alias and label assignment is done in the server API and does not write to the
  phone address book.

## Message/media support

- Text + image/MMS are supported through the message send/message/ingress APIs
  defined in the OpenAPI contract.
- Deliverability speed varies by phone/provider/network state; the gateway is
  optimized for stable acceptance, durable durability, and duplicate-safe
  behavior.
- Inbound/outbound pipelines now include explicit uncertain/terminal states to
  avoid unsafe retry behavior.

## Quick-start

1. Build and run migrations (DDL role first).
2. Start gateway with production profile and required secret sources.
3. Create a connection.
4. Start pairing on the phone, complete pairing in the API.
5. Subscribe to webhooks or Kafka to feed your AI.
6. Send messages via `/v1/connections/{connection_id}/messages`.

For concrete settings and security boundaries, start with:

- [gateway runtime configuration](docs/gateway-runtime-configuration.md)
- [Migrations and operations](docs/migrations-and-operations.md)
- [API contract (OpenAPI)](internal/gateway/httpapi/openapi.yaml)
- [Deployment playbook](deploy/README.md)

## Docs and references

- [Plan](docs/plans/2026-08-23-sirenaix-messaging-gateway-v1.md)
- [Pilot plan](docs/testing/pilot-plan.md)
- [Device matrix](docs/testing/device-matrix.md)
- [Product verification notes](docs/task8-product-verification.md)
- [Contributing / security / notices](CONTRIBUTING.md), [SECURITY.md](SECURITY.md),
  [NOTICE.md](NOTICE.md), [CHANGELOG.md](CHANGELOG.md)

## Development status

Current focus is v1 pilot hardening:

- tenant isolation and line metadata contracts
- durable idempotent messaging lifecycle
- safe session pairing and explicit reauthorization flow
- contact upsert + labels on server side
- load/race and release-contract gating

## License

AGPL-3.0-or-later (same upstream license family).
