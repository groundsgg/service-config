# service-config

Service for reading and mutating configuration documents stored in Postgres.

## API

REST under `/v1/config`, documented by an OpenAPI snapshot published to
[groundsgg/api-reference](https://github.com/groundsgg/api-reference) on every
release. Rendering stays central — this service serves no Swagger UI of its own.

```bash
./gradlew generateOpenApiSnapshot   # -> build/api-reference/openapi.json
```

A snapshot carries its version as an `ETag`. Send it back as `If-None-Match` and
an unchanged snapshot answers `304` with no body — that is the HTTP spelling of
`GetSnapshotIfNewer`, and the two RPCs are one endpoint because of it.

HTTP is the only transport. The `ConfigService` and `ConfigAdminService` gRPC
surfaces were removed once plugin-config 1.0.0 and plugin-proxy 2.0.0 had moved
to REST and the proxies had been rolled onto those jars.

One thing gRPC left behind: `quarkus-grpc` is still on the classpath, because it
owns the protoc codegen and the document services still pass the generated
messages around as internal request/response types. Giving them domain types is
what removes the extension, the `library-grpc-contracts-config` dependency and
the `scan-for-proto` line — a refactor of the write paths, worth doing on its
own rather than alongside a transport removal.

## Change Delivery

`service-config` stores the authoritative config state in Postgres. NATS change notifications are a
best-effort latency optimization, not a durable source of truth.

- Writes commit the database transaction before attempting to publish `config.<app>.<env>.changed`.
- If the pod crashes after the commit or NATS is unavailable, the database state is still correct
  and the change event can be missed.
- Consumers must treat the NATS payload as a refresh trigger only and fetch state through
  the snapshot endpoint with `If-None-Match` before applying changes locally.
- Config consumers must treat conditional snapshot polling as the reconciliation path and source
  of truth for cache correctness.

## Security

Every call is authenticated on both transports against the same
`WorkloadAuthenticator`: `GroundsAuthInterceptor` and `WorkloadAuthFilter` verify the caller's projected
ServiceAccount JWT against the cluster's JWKS and requires audience `grounds-services`. Set
`GROUNDS_AUTH_ENABLED=false` for local dev, where no token is projected.

Authorization on top of that:

| API | who |
|---|---|
| `ConfigService` (reads, `SyncDefaults`) | any authenticated caller |
| `ConfigAdminService.ListDocuments` / `GetDocument` / `CreateDocument` | admins only |
| `ConfigAdminService.PutDocument` / `DeleteDocument` | admins, **plus** a writer named for that app |

An admin is a caller whose JWT `sub` ends in `:platform-admin` or `:config-admin` — ops creates
those ServiceAccounts by hand, and their existence is the grant (`AuthGuard`).

`GROUNDS_CONFIG_WRITERS` names callers that may replace or delete documents of **one** app without
being admins, as `<subject-suffix>=<app>`:

```
GROUNDS_CONFIG_WRITERS=":velocity=velocity,:velocity-2=velocity"
```

This is how a service owns its own configuration — the Velocity proxies write the network MOTD —
without being handed every other app's along with it. A pod cannot opt into being an admin for one
call: a projected token always carries the pod's own ServiceAccount, so making the proxies
`config-admin` would be the only alternative, and that grants far more than the one document.

Suffix matching is namespace-agnostic, deliberately and exactly like the admin rule: the same
deployment exists in every region, and pinning the namespace would mean an entry per region that
nobody would keep in step. Two entries above rather than one because the two proxy releases run
under two ServiceAccounts and share a single document.

Unset (the default) means admin-only. Creating stays admin-only in every case: which documents exist
in an app is a shape decision, and `PutDocument` is already the create-or-replace path a
self-configuring service needs.

## Operations

`quarkus.flyway.migrate-at-start=true` is enabled today for convenience, but it is not the preferred
deployment model for multi-replica rollouts. Flyway locking reduces migration races, yet the
cleaner setup is a dedicated migration job or release step that runs before application pods start.
