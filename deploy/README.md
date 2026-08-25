# Deployment assets

The Compose stack is a local development baseline. Copy
`deploy/compose/.env.example` to a private `.env`, replace every placeholder,
then run `docker compose -f deploy/compose/compose.yaml up --build`. It uses an
external HTTPS OIDC issuer; it does not ship a development identity provider.
Run exactly one `gateway` service; `docker compose --scale gateway=...` is not
supported by the v1 pilot.

## Kubernetes pilot topology

The v1 gateway deliberately runs one pod with a `Recreate` rollout. Pairing
attempt state and actor-owned contact synchronization live in the owning
process: an in-progress pairing attempt is lost if that process restarts, so
the operator or user must start a new QR pairing attempt. Scale-out is not
supported until owner-aware RPC can route pairing and contact operations to
the process that owns each connection. The base therefore has no PDB,
multi-replica, topology-spread, or high-availability claim. A disconnected
phone affects its connection health and does not make the entire service
unready.

The Kubernetes assets contain no Secret object. Before running them, replace
every ConfigMap placeholder and create:

- `sirenaix-gateway-runtime` with `database-url`, `kms-keys`,
  `aws-access-key-id`, and `aws-secret-access-key`;
- `sirenaix-gateway-migration` with the separate DDL `database-url`; and
- `sirenaix-gateway-tls` with `tls.crt` and `tls.key`.

Label only intended callers with `sirenaix.ai/gateway-client=true` or
`sirenaix.ai/ops-client=true`; adapt the NetworkPolicy for an ingress
controller in another namespace.

Every Kubernetes image is the deliberately unusable
`registry.invalid/sirenaix-gateway:REQUIRED_IMMUTABLE_RELEASE_IMAGE`. In a
disposable copy of `deploy/kubernetes`, set one immutable image value such as
`ghcr.io/example/sirenaix-gateway@sha256:<64 lowercase hex>` in all three
kustomizations:

```sh
IMAGE='ghcr.io/example/sirenaix-gateway@sha256:<64-lowercase-hex>'
RELEASE_ID='v1-2-3-abcdef'

(cd migration && kustomize edit set image registry.invalid/sirenaix-gateway="$IMAGE" && kustomize edit set namesuffix -- "-$RELEASE_ID")
(cd migration-status && kustomize edit set image registry.invalid/sirenaix-gateway="$IMAGE" && kustomize edit set namesuffix -- "-$RELEASE_ID-status")
(cd base && kustomize edit set image registry.invalid/sirenaix-gateway="$IMAGE")
```

Use a new lowercase DNS-safe `RELEASE_ID` for every attempt; never reapply a
different pod template to an existing immutable Job. The operator flow is
strictly serialized:

```sh
kustomize build bootstrap | kubectl apply -f -

kustomize build migration | kubectl apply -f -
kubectl wait --for=condition=complete --timeout=30m "job/sirenaix-gateway-migrate-$RELEASE_ID"
kubectl logs "job/sirenaix-gateway-migrate-$RELEASE_ID"

kustomize build migration-status | kubectl apply -f -
kubectl wait --for=condition=complete --timeout=5m "job/sirenaix-gateway-migrate-$RELEASE_ID-status"
kubectl logs "job/sirenaix-gateway-migrate-$RELEASE_ID-status"

kustomize build base | kubectl apply -f -
kubectl rollout status deployment/sirenaix-gateway
```

Do not apply the Deployment unless both Jobs completed successfully. The
migration and status Jobs and the Deployment must render the exact same image
digest. Apply the separately owned `bootstrap` NetworkPolicy before creating
either Job in a fresh namespace. It denies all ingress to migration/status
pods and permits only cluster DNS plus TCP port 5432 egress; keep it in place
through both Jobs. Its distinct resource name avoids ownership conflicts with
the long-running gateway policy in `base`. Kubernetes NetworkPolicy allows are
additive, so verify that no separately managed broad policy selects these Job
pods.

The public service terminates TLS in the gateway on port 8443. The operations
listener is plaintext inside the cluster on port 9090 and must remain internal.
No Ingress, certificate issuer, database, OIDC provider, or AWS credential is
fabricated by this base.

## Release and rollback checklist

Before rollout, verify the immutable image digest and build metadata, take a
restorable database backup, inspect `migrate status` with the migration
credential, and confirm the runtime role cannot perform DDL. Apply the
versioned migration Job first, wait for completion, and require the separate
versioned `migrate status --check` Job to succeed before changing the
Deployment. Then confirm `/livez`,
`/readyz`, and `/metrics` through an authorized operations client and send one
bounded API smoke request.

Stop the rollout if readiness fails, database or dependency errors increase,
or durable queue depth grows without recovery. Application images may be
rolled back only to a version whose embedded migration catalog recognizes the
current database version. Migrations are forward-only: never edit or reverse a
shipped migration file, and never use adoption as rollback. If schema recovery
is required, stop writers and follow the database restore procedure validated
for that deployment.
