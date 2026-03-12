# Greengage CI Workflow

This directory contains the CI pipelines for the Greengage project,
orchestrating the build, test, and upload stages for containerized
environments. The pipeline is designed to be flexible, with parameterized
inputs for version and target operating systems, allowing it to adapt to
different branches and configurations.

## ⚠️ Important Notice

**Branch Policy Change:** The `main` branch is deprecated as the default
development branch and will be transitioned to protected read-only status.
The `6.x` branch now serves as the primary versioned branch for all
development activities. The `main` branch is retained solely for backward
compatibility during the transition period and to prevent accidental feature
branch creation from unintended `git push origin main` commands. **Do not use
`main` for new development.**

Whenever the list of **NAMES of required jobs** in the workflow (including any
**reusable workflows**) is **added, removed, or renamed**, you must contact a
repository administrator to update the **Branch Protection Rules** accordingly.
Without this, new, deleted, or renamed jobs will not be recognized as required
when checking Pull Requests.

## Overview

The `Greengage CI` workflow triggers on:

- **Push events** to `6.x` or `main` branch (after merged PR) or versioned
  release tags (`6.*`).
- **Pull requests** to any branch.

It executes the following jobs in a matrix strategy for multiple target
operating systems:

- **Build**: Constructs and pushes Docker images to the GitHub Container
  Registry (GHCR) with development commit SHA tag and branchname tag. Runs for
  pull requests and all push events (main and tags).
- **Tests**: Runs multiple test suites only for pull requests, including:
  - Behave tests
  - Regression tests
  - Orca tests
  - Resource group tests
- **Upload**: Retags and pushes final Docker images to GHCR and optionally
  DockerHub. Runs for push to `6.x`/`main` (retags to `latest`) and tags (uses
  tag like `6.28.2`) after build.

## Release Workflow

A separate workflow `Greengage release` handles the uploading of Debian package
to GitHub releases. It is triggered when a release is published and uses a
composite action to manage package deployment.

### Key Features

- **Triggers:** `release: [published]` - Runs when a release is published,
including re-publishing.
- **Concurrency:** Uses the same concurrency group as the CI workflow
(`Greengage CI-${{ github.ref }}`) to ensure proper sequencing and prevent race
conditions.
- **Cache-based Artifacts:** Restores built packages from cache using the
commit SHA as the key, rather than downloading artifacts from previous jobs.
- **Manual Recovery:** If the cache is missing, the workflow checks the status
of the last build for the tag and provides clear instructions for manual
intervention. It does not automatically trigger builds to avoid infinite loops.
- **Safe Uploads:** Uploads packages with fixed naming patterns and optional
overwrite (`clobber` flag).

### Behavior

1. **Normal Flow (Cache Available):** Restores packages from cache, renames
them to the pattern `${PACKAGE_NAME}${VERSION}.${EXT}`, and uploads to the
release.
2. **Cache Miss Scenarios:**
   - **No previous build or previous build successful:** Provides instructions
   to manually trigger the CI build, then restart the release workflow.
   - **Previous build failed:** Reports the failure with a link to the failed
   run and requires manual fixing before retrying.

The release workflow is designed to be robust and provide clear feedback when
issues occur, ensuring that releases are always consistent and reliable.

## SQL Dump Workflow

A separate workflow `Greengage SQL Dump` is responsible for generating SQL dump
artifacts after the main CI process completes successfully. It is triggered
automatically upon the completion of the `Greengage CI` workflow.

### Key Features

- **Triggers:** `workflow_run: workflows: ["Greengage CI"], types: [completed]`
- **Branch Targeting:** Runs only for the `main`, `6.x`, and `7.x` branches.
- **Version Detection:** Automatically determines the database version based on
  the triggering branch: `main` → version 6, `<digit>.x` → `<digit>` (e.g.,
  `6.x` → 6, `7.x` → 7).
- **Artifact Creation:** Executes regression tests with the `dump_db: "true"`
  parameter to generate a SQL dump archive, which is then uploaded as a workflow
  artifact.
- **Controlled Execution:** Since the main CI workflow runs on `main`, `6.x`
  and `7.x` branches only for push events (which occur after final merge of a
  PR), SQL dump are generated exclusively for verified, approved patches after
  they are merged into the main branches.
- **Artifact Retention:** The generated SQL dump artifact is retained 90 days
  after the last download. Each new run of the `behave tests gpexpand` workflow
  (which consumes this artifact as a consumer) resets this retention period to
  90 days when it downloads the artifact.

### Behavior

1. **Triggering:** Automatically starts after the `Greengage CI` workflow
   finishes on the `main`, `6.x`, or `7.x` branch.
2. **Preparation:** Configures Docker storage on the runner to utilize
   `/mnt/docker` for increased disk space.
3. **Version Mapping:** Maps the branch name (`main` → version 6, `6.x` → 6,
   `7.x` → 7, `<digit>.x` → `<digit>`) to select the correct Docker image for
   testing.
4. **Dump Generation:** Runs the regression test suite using the reusable
   action with the `dump_db` option enabled, which creates a
   `*_postgres_sqldump.tar` file.
5. **Artifact Upload:** Uploads the generated SQL dump archive as a named
   artifact (e.g., `sqldump_ggdb7_ubuntu`) to the workflow run.

This workflow ensures that a current database schema dump is available as an
artifact following successful CI runs on the primary branches `main`, `6.x` and
`7.x`.

## Configuration

The workflow is parameterized to support flexibility:

- **Version**: Specifies the Greengage version (e.g., `6`), configurable per
  branch.
- **Target OS**: Supports multiple operating systems, defined in the matrix
  strategy.

All jobs use reusable workflows stored in the `greengagedb/greengage-ci`
repository, accessible publicly for detailed inspection.

## Usage

To use this pipeline:

1. Ensure the repository has a valid `GITHUB_TOKEN` with `packages: write`
   permissions for GHCR access.
2. Optionally configure `DOCKERHUB_TOKEN` and `DOCKERHUB_USERNAME` for
   DockerHub uploads.
3. Configure the version and target OS parameters in the branch-specific
   workflow configuration.
4. Create a pull request or push a tag (`6.*`) to trigger the pipeline.

## Additional Documentation

Detailed README files for each process are available in the `README` directory
of the `greengagedb/greengage-ci` repository. For example:

- Build process:
  [README/REUSABLE-BUILD.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-BUILD.md)
- Behave tests:
  [README/REUSABLE-TESTS-BEHAVE.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-BEHAVE.md)
- Regression tests:
  [README/REUSABLE-TESTS-REGRESSION.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-REGRESSION.md)
- Orca tests:
  [README/REUSABLE-TESTS-ORCA.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-ORCA.md)
- Resource group tests:
  [README/REUSABLE-TESTS-RESGROUP.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-RESGROUP.md)
- Upload process:
  [README/REUSABLE-UPLOAD.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-UPLOAD.md)

## Notes

- The pipeline uses a `fail-fast: true` strategy to stop on any matrix job
  failure, ensuring quick feedback.
- The full process, including build, tests, and upload, runs only before pull
  request approval. For push events (main or tags), a build occurs to ensure
  correct commit references and product version, using the closest tag to HEAD,
  followed by upload. If DockerHub credentials (`DOCKERHUB_TOKEN`,
  `DOCKERHUB_USERNAME`) are missing or invalid, DockerHub upload is skipped,
  but other processes (GHCR upload, etc.) are unaffected.
- For specific details on each stage, refer to the respective reusable workflow
  files and their READMEs in the `greengagedb/greengage-ci` repository.
