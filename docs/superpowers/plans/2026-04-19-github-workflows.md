# GitHub Workflows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the standard reusable GitHub workflow and release-please baseline to `service-config`.

**Architecture:** Mirror `service-player` workflow entrypoints so this repository delegates build,
release, and maintenance behavior to the shared reusable workflows in `groundsgg/.github`. Keep
repo-specific state limited to Dependabot config and release-please bootstrap version metadata.

**Tech Stack:** GitHub Actions, Dependabot, release-please, Gradle

---

### Task 1: Add the reusable workflow entrypoints

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/docker-gradle-build-push.yml`
- Create: `.github/workflows/labels.yml`
- Create: `.github/workflows/release-please.yml`

- [ ] **Step 1: Create the CI workflow entrypoint**

```yaml
name: Java Build

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  reusable:
    uses: groundsgg/.github/.github/workflows/gradle-ci.yml@main
```

- [ ] **Step 2: Create the Docker workflow entrypoint**

```yaml
name: Docker Build

on:
  push:
    branches:
      - main
    tags:
      - "v*"
  pull_request:

permissions:
  packages: write
  contents: write
  actions: write

jobs:
  reusable:
    uses: groundsgg/.github/.github/workflows/docker-gradle-build-push.yml@main
```

- [ ] **Step 3: Create the label sync workflow entrypoint**

```yaml
name: Label Sync

on:
  push:
    branches:
      - "main"
    paths:
      - ".github/workflows/labels.yml"
  pull_request:
    paths:
      - ".github/workflows/labels.yml"
  workflow_dispatch:

permissions:
  contents: read
  issues: write

jobs:
  reusable:
    uses: groundsgg/.github/.github/workflows/label-sync.yml@main
```

- [ ] **Step 4: Create the release-please workflow entrypoint**

```yaml
name: Release Please

on:
  push:
    branches: [main]

permissions:
  contents: write
  issues: write
  pull-requests: write

jobs:
  reusable:
    uses: groundsgg/.github/.github/workflows/release-please.yml@main
    secrets:
      RELEASE_PLEASE_TOKEN: ${{ secrets.RELEASE_PLEASE_TOKEN }}
```

### Task 2: Add repository maintenance and release metadata

**Files:**
- Create: `.github/dependabot.yml`
- Create: `release-please-config.json`
- Create: `.release-please-manifest.json`

- [ ] **Step 1: Create Dependabot configuration**

```yaml
version: 2
updates:
  - package-ecosystem: "gradle"
    directory: "/"
    schedule:
      interval: "daily"
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "daily"
    open-pull-requests-limit: 5
    groups:
      github-actions-minor-patch:
        patterns:
          - "*"
        update-types:
          - "minor"
          - "patch"
```

- [ ] **Step 2: Create release-please package configuration**

```json
{
  "packages": {
    ".": {
      "release-type": "simple"
    }
  }
}
```

- [ ] **Step 3: Create release-please manifest bootstrap**

```json
{
  ".": "0.0.1"
}
```

### Task 3: Verify repository health

**Files:**
- Modify: none

- [ ] **Step 1: Run the test suite**

Run: `./gradlew test`
Expected: Gradle exits with code 0.

- [ ] **Step 2: Apply formatting**

Run: `./gradlew spotlessApply`
Expected: Gradle exits with code 0.

- [ ] **Step 3: Run the build**

Run: `./gradlew build`
Expected: Gradle exits with code 0.
