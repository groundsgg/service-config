# GitHub Workflows Design

## Goal

Add the standard GitHub automation baseline to `service-config` by mirroring the reusable workflow
setup already used in `service-player`.

## Scope

This design adds:

- reusable workflow entrypoints for CI, Docker build/push, label sync, and release-please
- Dependabot configuration for Gradle and GitHub Actions updates
- release-please bootstrap metadata with an initial version of `0.0.1`

This design does not add repository-specific workflow logic. `service-config` should stay aligned
with the organization-level reusable workflows in `groundsgg/.github`.

## Files

Create these files:

- `.github/workflows/ci.yml`
- `.github/workflows/docker-gradle-build-push.yml`
- `.github/workflows/labels.yml`
- `.github/workflows/release-please.yml`
- `.github/dependabot.yml`
- `release-please-config.json`
- `.release-please-manifest.json`

## Decisions

- Use the same reusable workflow references as `service-player`.
- Keep the same workflow permissions as `service-player` to avoid drift.
- Bootstrap release-please with `.release-please-manifest.json` set to `0.0.1`.

## Verification

After adding the files, run the repository-required Gradle commands:

- `./gradlew test`
- `./gradlew spotlessApply`
- `./gradlew build`
