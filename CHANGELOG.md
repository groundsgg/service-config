# Changelog

## [0.8.0](https://github.com/groundsgg/service-config/compare/v0.7.0...v0.8.0) (2026-08-10)


### Features

* **api:** serve config over REST alongside gRPC ([#48](https://github.com/groundsgg/service-config/issues/48)) ([58333c3](https://github.com/groundsgg/service-config/commit/58333c37376b1bdcfbb66f18a115dcc25bb27234))

## [0.7.0](https://github.com/groundsgg/service-config/compare/v0.6.0...v0.7.0) (2026-08-04)


### Features

* add grounds push integration ([#33](https://github.com/groundsgg/service-config/issues/33)) ([e108b8e](https://github.com/groundsgg/service-config/commit/e108b8e6635207f48f7b5db61ffca59bdabe44e9))
* **metrics:** expose JVM, HTTP and connection-pool metrics ([#42](https://github.com/groundsgg/service-config/issues/42)) ([e482cd4](https://github.com/groundsgg/service-config/commit/e482cd415a9b6c417aef56342062f160113bcf8b))


### Bug Fixes

* default scoped writers to empty policy ([#37](https://github.com/groundsgg/service-config/issues/37)) ([f412a9f](https://github.com/groundsgg/service-config/commit/f412a9f6eca308f4bf1b3ec7eae7876ba732a9d5))
* package executable jar for grounds push ([#35](https://github.com/groundsgg/service-config/issues/35)) ([3bfbf8a](https://github.com/groundsgg/service-config/commit/3bfbf8a36b0cef210e36dcda5bd93e2df8c9e9dd))
* run executable Quarkus jar ([#36](https://github.com/groundsgg/service-config/issues/36)) ([77b824c](https://github.com/groundsgg/service-config/commit/77b824c192b4ce735f9924f19c4095f63fd28c52))

## [0.6.0](https://github.com/groundsgg/service-config/compare/v0.5.0...v0.6.0) (2026-08-02)


### Features

* **auth:** let a named writer change one app's documents ([#31](https://github.com/groundsgg/service-config/issues/31)) ([958605d](https://github.com/groundsgg/service-config/commit/958605d5bc94f0452070ac9f8cbedeb4a6f80180))

## [0.5.0](https://github.com/groundsgg/service-config/compare/v0.4.0...v0.5.0) (2026-06-02)


### Features

* **nats:** present projected SA-token as NATS bearer (B4) ([#30](https://github.com/groundsgg/service-config/issues/30)) ([5582191](https://github.com/groundsgg/service-config/commit/5582191096c441c2fbd380c8e625d4c740f6c068))


### Bug Fixes

* **auth:** JWKS fetch trusts cluster CA + sends SA-bearer (OVH-MKS) ([#28](https://github.com/groundsgg/service-config/issues/28)) ([8f40179](https://github.com/groundsgg/service-config/commit/8f401795d87651e05d921a43b5e6dd0d6f12058d))

## [0.4.0](https://github.com/groundsgg/service-config/compare/v0.3.0...v0.4.0) (2026-05-28)


### Features

* **otel:** server-side traces to Alloy ([#26](https://github.com/groundsgg/service-config/issues/26)) ([24dc5df](https://github.com/groundsgg/service-config/commit/24dc5df2ea37cc26b5a627d778807f6b4a3ef1fc))

## [0.3.0](https://github.com/groundsgg/service-config/compare/v0.2.0...v0.3.0) (2026-05-27)


### Features

* **auth:** admin-only Method-ACL for ConfigAdminGrpcService ([#22](https://github.com/groundsgg/service-config/issues/22)) ([b62e92c](https://github.com/groundsgg/service-config/commit/b62e92c9dd589b5b2f04a1a75a8af54e9bd3e568))

## [0.2.0](https://github.com/groundsgg/service-config/compare/v0.1.0...v0.2.0) (2026-05-27)


### Features

* **auth:** JWT validation + AuthGuard for service-config ([#20](https://github.com/groundsgg/service-config/issues/20)) ([3865dc5](https://github.com/groundsgg/service-config/commit/3865dc52e81545712f663b7a3b2ae3d53254cddf))

## [0.1.0](https://github.com/groundsgg/service-config/compare/v0.0.1...v0.1.0) (2026-04-19)


### Features

* add config service ([#1](https://github.com/groundsgg/service-config/issues/1)) ([0e33a24](https://github.com/groundsgg/service-config/commit/0e33a2461d19764c1e5202b4efa5a1011dc2d16f))
* add github actions and release workflow ([#2](https://github.com/groundsgg/service-config/issues/2)) ([db2e34b](https://github.com/groundsgg/service-config/commit/db2e34bea645724d05fb893c0132e34940e8efba))
