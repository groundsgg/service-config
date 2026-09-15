# PackSet Document Authorization and Concurrency Plan

> REQUIRED: superpowers:subagent-driven-development. Execute tasks sequentially; primary owns integration and final security review.

**Goal:** Extend the existing Config Service REST API for the approved fixed Stage PackSet document without granting Forge app-wide writes or losing Int64 concurrency versions.

**Approved spec:** `/home/lukas/grounds/.worktrees/docs-packset-portal-observability/docs/superpowers/specs/2026-09-14-packset-selection-and-rollback-design.md`.

**Architecture:** Keep the existing policy, normalized document contexts, repository compare-and-swap and HTTP error mapper. Add an exact-document writer grant alongside legacy app grants, and transport document versions through strong ETags/If-Match. No schema, generated contract or deployment changes.

## Global Constraints

- Isolated worktree `/home/lukas/grounds/.worktrees/service-config-packset-concurrency`, branch `feat/packset-document-concurrency`, origin/main baseline `7974f87`. Original checkout and its untracked configuration remain untouched.
- Baseline `./gradlew test` succeeded before implementation. Existing Kotlin daemon linkage warning falls back successfully; do not upgrade unrelated dependencies to silence it.
- Preserve `<subject-suffix>=<app>` legacy grants and admin privileges. New exact syntax is `<subject-suffix>=<app>/<env>/<namespace>/<configKey>`; malformed entries fail closed, never degrade to app-wide grants.
- Reuse `ConfigRequestContexts` normalization/validation. Authorize the same normalized four-part identity passed to persistence. Exact writers gain PUT/DELETE only for that identity, not admin browsing, reads or creation.
- Keep existing JSON response bodies and optional body `expectedVersion` compatibility. No generic proxy, retry-on-conflict or unchecked overwrite behavior.
- New `If-Match` accepts exactly one strong quoted canonical positive signed-Int64 decimal version, allowing surrounding HTTP whitespace only. Reject weak, bare, wildcard, list, overflow, noncanonical and nonpositive values with 400. Header plus body version is 400, even if equal.
- Single-document public/admin GET and successful PUT carry strong version ETags. Existing snapshot If-None-Match semantics stay unchanged. Stale compare-and-swap remains 409 through the existing error mapper.
- Tests are focused plain JUnit/Mockito at changed boundaries, not new cluster or database integration infrastructure. Use lossless examples above JavaScript's safe-integer limit and Long.MAX_VALUE.
- No live pin mutation, capability activation, bundle pin, deployment, kubecontext/Pulumi changes, manual tag/release or unrelated NATS changes. Static Core NATS publisher mode is a subsequent approved infrastructure prerequisite, not this API slice.
- Implementers use Terra medium, do not spawn agents/reviewers. Primary dispatches independent task and final reviews. Signed commits and normal PR/CI integration only.

## Task 1: Exact-document writer grants

**Files:** `src/main/kotlin/gg/grounds/auth/ConfigWritePolicy.kt`, `src/main/kotlin/gg/grounds/rest/ConfigAdminResource.kt`, focused policy/resource tests, `README.md`.

**Interfaces:** Preserve existing two-argument `mayWriteAs(subject, app)` for legacy grants only. Add a four-part document-aware overload (prefer existing `DocumentContext` argument). Store a small typed app/exact-document grant, not duplicated regex or a new authorization subsystem. PUT and DELETE normalize all segments before policy checks and build requests from that same context.

- [ ] Add policy RED tests for exact allowed identity, each mismatching segment, legacy compatibility, admin access and old two-argument denial for document-only grants. Malformed two/three/five-part, empty/invalid segments must not create any grant; preserve existing duplicate-suffix last-valid-entry behavior.
- [ ] Add direct resource tests with existing real service + mocked repository/publisher or a mocked service: allowed PUT/DELETE pass normalized identity; denied neighboring document/environment yields 403 and zero service side effects. No new HTTP test harness.
- [ ] Run focused tests and capture actual RED before production changes. Implement minimal typed parsing and context authorization. Update REST operation descriptions and README syntax/least-privilege example `:forge=network/stage/resourcepacks/global`.
- [ ] Run focused GREEN and `./gradlew test`; inspect diff and commit only Task 1 files. Write task report with actual commands/results, commit hash and any deviations. Do not touch Task 2 or spawn reviewers.

## Task 2: Lossless document ETags and conditional PUT

**Files:** `src/main/kotlin/gg/grounds/rest/ConfigResource.kt`, `ConfigAdminResource.kt`, a small shared REST helper if needed, focused REST tests, `README.md`.

**Interfaces:** Public/admin document GET return `Response.ok(existingDto).tag(etag(document.version))`. PUT appends optional `@HeaderParam("If-Match") ifMatch: String? = null` to preserve direct call sites, returns existing `WriteResultResponse` entity with the successful version tag. Strict parser stays separate from existing lenient snapshot parser. Body version remains supported unchanged; map parsed header to existing `PutDocumentRequest.expectedVersion` and let repository/service enforce concurrency.

- [ ] Add RED tests for both document GET ETags and successful PUT ETag/body preservation, unconditional/body/header paths, mutually exclusive header/body, strong canonical parser rejects, and exact `9007199254740993`/Long.MAX_VALUE transport. Verify stale header CAS maps to existing 409 without retry or alternate write.
- [ ] Implement minimal Response wrapping and parser. Add accurate OpenAPI operation/header response descriptions using existing annotation conventions; do not alter wire body schemas or snapshot behavior.
- [ ] Run focused GREEN then `./gradlew test`; record actual evidence, inspect diff and signed-commit only task files. Write Task 2 report, do not spawn reviewers or push/merge yourself.

## Integration

- [ ] Independent Terra task review after each task; fixes through original implementer and scoped re-review.
- [ ] Primary security/cross-boundary review and Astra final broad review; resolve verified findings, rerun full check at final head.
- [ ] Normal feature PR, required CI green and exact reviewed-head merge. Release Please owns publishing; keep live capability disabled until runtime convergence is verified.

## Plan self-review

Task 1 independently establishes least privilege; Task 2 depends on its normalized request path but adds a separately rejectable HTTP concurrency contract. Both reuse existing service/repository behavior. Header versions stay strings at the browser-facing boundary while Config Service retains native Long values. Deployment and static publisher authentication remain separate, avoiding accidental infrastructure changes while cluster access is unresolved.
