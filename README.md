# service-config

gRPC service for reading and mutating configuration documents stored in Postgres.

## Change Delivery

`service-config` stores the authoritative config state in Postgres. NATS change notifications are a
best-effort latency optimization, not a durable source of truth.

- Writes commit the database transaction before attempting to publish `config.<app>.<env>.changed`.
- If the pod crashes after the commit or NATS is unavailable, the database state is still correct
  and the change event can be missed.
- Consumers must treat the NATS payload as a refresh trigger only and fetch state through
  `GetSnapshotIfNewer` before applying changes locally.
- Config consumers must treat `GetSnapshotIfNewer` snapshot polling as the reconciliation path and
  source of truth for cache correctness.

## Security

The admin gRPC API does not enforce application-level authentication or authorization in this
service today. That is currently expected to be handled by deployment controls such as private
networking or mTLS.

This is a deliberate gap to track, not an implicit guarantee. Admin API auth is still required and
must be treated as a follow-up item rather than part of the current PR scope.

## Operations

`quarkus.flyway.migrate-at-start=true` is enabled today for convenience, but it is not the preferred
deployment model for multi-replica rollouts. Flyway locking reduces migration races, yet the
cleaner setup is a dedicated migration job or release step that runs before application pods start.
