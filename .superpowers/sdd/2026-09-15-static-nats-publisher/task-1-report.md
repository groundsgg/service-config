# Task 1 report: static NATS publisher authentication

Implementation commit: `2e77506f1eb551d2d7a66d1c6ce3e26838fc2ff6`

## Evidence

| Stage | Exact command | Result |
| --- | --- | --- |
| Baseline | `./gradlew -Pgithub.user=build -Pgithub.token=build test --tests gg.grounds.events.ConfigChangePublisherTest` | Passed: 2 focused tests, 16 actionable tasks. |
| RED | `./gradlew -Pgithub.user=build -Pgithub.token=build test --tests gg.grounds.events.ConfigChangePublisherTest` | Failed as intended during `compileTestKotlin`: `buildConnectionOptions` and `natsAuthMode` did not exist. |
| GREEN | `./gradlew -Pgithub.user=build -Pgithub.token=build test --tests gg.grounds.events.ConfigChangePublisherTest` | Passed: 6 focused tests, 16 actionable tasks. |
| Formatter | `./gradlew -Pgithub.user=build -Pgithub.token=build spotlessApply` | Passed: 5 actionable tasks. |
| Full verification | `./gradlew -Pgithub.user=build -Pgithub.token=build check` | Passed: 67 test cases, 21 actionable tasks; Spotless check included. |

Focused coverage uses real jnats `Options`: projected-token supplier rotation, static bearer absence with URI userinfo retained, invalid-mode rejection, and malformed authenticated URL diagnostics without a network connection, credential sentinel, or attached throwable.

## Deviations

- The repository requires `github.user` and `github.token` Gradle properties even when resolving the already-cached private build plugin. Test commands supplied non-secret placeholder values (`build`) for those properties.
- Gradle's Kotlin daemon has a host-level `NoSuchMethodError` and falls back to in-process compilation. Each listed command completed with its stated Gradle result.
- No production dependency, HTTP workload authentication, deployment configuration, live broker test, push, merge, rebase, PR, or release action was performed.
