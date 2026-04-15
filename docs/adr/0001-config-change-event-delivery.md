# ADR 0001: Config Change Event Delivery

- Status: Accepted
- Date: 2026-04-14

## Context

`service-config` persists config documents and app versions in Postgres, then publishes
`config.<app>.<env>.changed` notifications over NATS so consumers can refresh quickly.

Consumers do not derive correctness from the NATS payload itself. The payload is only a signal that
the consumer should call `GetSnapshotIfNewer` and reconcile against the latest snapshot.

The write flow commits the database transaction before attempting the NATS publish. That creates an
intentional failure window:

- If the process terminates after the database commit and before `nats.publish`, the change is
  durable in Postgres but no event is emitted.
- If NATS is disconnected or a publish attempt fails, the service does not persist the event for
  retry.

This means NATS notifications are not an exactly-once or at-least-once delivery mechanism.

## Decision

We treat NATS change notifications as best-effort invalidation hints. Postgres snapshots remain the
source of truth.

Consumers are required to reconcile through `GetSnapshotIfNewer` on a periodic basis and must not
assume that every database change produces a corresponding NATS event.

Consumers must also treat the NATS payload as a refresh trigger only. Applying config changes based
solely on event delivery or payload contents is outside the contract and would introduce a fragile
dependency on best-effort transport.

We are documenting this explicitly instead of adding a transactional outbox in the current change.

## Consequences

- Consumers can observe temporary cache drift after a missed event until the next snapshot
  reconciliation completes.
- Operators can still detect the degraded state through readiness and publish failure logs.
- The current design stays simple and avoids introducing outbox tables, drainers, and retry
  orchestration in this PR.
- The admin API still lacks application-level authz/authn in this service. Deployments may mitigate
  exposure through mTLS or network isolation, but proper admin API authentication remains a TODO
  and is out of scope for this PR.
- Flyway startup migration is enabled in the application today, which is acceptable for now but not
  the preferred multi-replica deployment model. A dedicated migration job remains the cleaner
  operational setup.

## Follow-up

If the best-effort model becomes insufficient, the next step is a transactional outbox:

1. Write the config mutation and an outbox record in the same database transaction.
2. Drain the outbox asynchronously to NATS with retries and observability.
3. Mark or delete outbox records only after publish acknowledgement.
