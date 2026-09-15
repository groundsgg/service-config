# Explicit Static NATS Publisher Authentication

> REQUIRED: superpowers:subagent-driven-development. One bounded task; primary owns cross-branch integration and final security review.

**Goal:** Support the approved private Core Stage Config instance on a static-user NATS hub without sending its projected HTTP ServiceAccount token as a NATS bearer.

**Approved spec:** `/home/lukas/grounds/.worktrees/docs-packset-portal-observability/docs/superpowers/specs/2026-09-14-packset-selection-and-rollback-design.md`, Core Stage control-plane subsection.

**Architecture:** Add an explicit publisher-only authentication mode to the existing connection-options builder. Default projected-token behavior retains per-reconnect token rotation and local missing-file compatibility. Static mode relies on the existing authenticated NATS_URL and never configures a projected token supplier. Do not change workload HTTP authentication.

## Global Constraints

- Worktree `/home/lukas/grounds/.worktrees/service-config-static-nats-publisher`, branch `feat/static-nats-publisher`, original baseline `7974f87f7ce5beb3102cab832e579e1f3b25e420`. Original checkout and all other worktrees are untouched.
- Independent publisher-only slice runs alongside Config-Service API PR66 CI. Primary integrates the updated origin/main after PR66 merges, then verifies/reviews the final combined runtime. Worker must not merge/rebase/push branches.
- New `nats.auth-mode=${NATS_AUTH_MODE:projected-token}` supports exactly `projected-token` and `static`. Reject unsupported values before connecting; no silent fallback or automatic credential detection.
- Append a defaulted primary constructor argument, preserving the existing two-argument constructor and direct call sites. Keep DI configuration explicit.
- Extract only a small internal `buildConnectionOptions(): Options` within ConfigChangePublisher for real-options tests; no connector interface, executor, retry/cache/lifecycle abstraction or new dependencies.
- Keep URL, max reconnects, reconnect wait and connection listener behavior intact. Default mode reads the same projected token file on each supplier invocation; absent-file local/dev behavior stays compatible. Static mode never accesses/configures that token path, even if a projected token file exists.
- HTTP WorkloadAuthenticator and `grounds.token-file` are unchanged: static NATS authentication must not disable HTTP projected-JWT authentication.
- Authenticated URL credentials and exceptions that may embed those URLs must not leak through publisher-owned logs. Simplest bounded approach: omit URL fields from connection logs and log exception class/reason code only, not raw Throwable/message/options/token. Apply to stale-close/connect/publish failures in this publisher; do not build a general redaction framework.
- Preserve best-effort post-commit change hints, app-version counters, NATS subjects/payload, event diagnostics, synchronization and shutdown lifecycle.
- No static credentials/real tokens in source/config/test output, no live broker or cluster tests, no deployment/Pulumi/bundle/document changes. Use synthetic test sentinels and short test-owned temporary files only.
- Terra medium implements; no worker subagents/reviewers, push, merge, release/tag. Primary dispatches independent reviews; Release Please owns publishing.

## Task 1: Publisher mode, safe diagnostics and focused options tests

**Files:** `src/main/kotlin/gg/grounds/events/ConfigChangePublisher.kt`, `src/main/resources/application.properties`, `src/test/kotlin/gg/grounds/events/ConfigChangePublisherTest.kt`, `README.md`.

**Interfaces:** Existing public connect/publish/close and two-argument constructor remain. Published jnats2.26.2 `Options.getTokenChars()` invokes its token supplier each time (primary verified resolved bytecode); `Options.servers` retains configured server URI. Tests use the real built Options, not reflected internal supplier fields or a fake connector.

- [ ] Run the existing focused publisher tests as a fresh baseline before implementation; record actual output.
- [ ] Add RED tests for default projected-token rotation (temp file first/second values via the same Options), static mode with an existing projected token file returning no bearer and retaining synthetic authenticated server URI, and invalid mode rejection. Keep reconnect/listener options assertions in the same cases.
- [ ] Add one focused publisher-owned diagnostic capture case using syntactically malformed synthetic authenticated URL so options construction fails before any network call. Assert no credential sentinel/raw URL or attached Throwable is logged; use existing JUL/JBoss logging facilities, no test logger framework. Verify actual jnats construction failure occurs synchronously rather than connecting to a broker.
- [ ] Implement the minimal mode/options builder and safe publisher-owned error logging after actual RED. Keep projected HTTP auth untouched and avoid token-path access entirely in static branch.
- [ ] README documents default mode, explicit static opt-in via a runtime secret-backed NATS_URL, unchanged HTTP token authentication and least-privilege `config.network.stage.changed` publisher prerequisite. Do not claim deployed or enable any capability.
- [ ] Formatter, focused GREEN then full `./gradlew check` once. Inspect changed files for unrelated churn, signed commit only mapped files. Write `.superpowers/sdd/2026-09-15-static-nats-publisher/task-1-report.md` with exact RED/GREEN commands/results, counts, commit and deviations. No push/merge/review dispatch.

## Integration and Review

- [ ] Independent Terra task spec/quality review; fix verified findings through original worker and scoped re-review.
- [ ] Primary merges current origin/main containing accepted PR66 into this isolated branch without rewriting worker commits; rerun full check and OpenAPI export to ensure HTTP contracts remain intact.
- [ ] Astra final security/correctness review at the final integrated head, including both authentication domains and publisher-owned diagnostics.
- [ ] Normal PR, required CI green and exact reviewed-head merge. Release Please owns tags/releases. Private Core Stage deployment and live pin activation remain separate verified rollout gates.

## Plan self-review

Publisher connection options do not depend on the REST/concurrency files changed by PR66, so implementation can proceed independently in an isolated worktree. Integration precedes final review/CI, preventing a stale-main test claim. One explicit mode controls only the NATS bearer; HTTP workload auth remains independent. Real options exercise token rotation without networking or reflection; no server test infrastructure is introduced. URL/error omission is simpler and safer than a generic credential redactor.
