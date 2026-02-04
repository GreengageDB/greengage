# CI Workflows

This directory contains the CI workflows for the Greengage project:

- **Greengage ABI Tests**: Checks for ABI (Application Binary Interface)
  changes in the codebase
- **Greengage CI**: Main CI pipeline orchestrating build, test, upload
- **Greengage release**: Handles the uploading of Debian packages to Releases
- **Greengage SQL Dump**: Generates SQL dump artifacts after `Greengage CI`

## Greengage CI Workflow

Orchestrating the build, test, and upload stages for containerized
environments. The pipeline is designed to be flexible, with parameterized
inputs for version and target operating systems, allowing it to adapt to
different branches and configurations.

## ⚠️ Important Notice

Whenever the list of **NAMES of required jobs** in the workflow (including any
**reusable workflows**) is **added, removed, or renamed**, you must contact a
repository administrator to update the **Branch Protection Rules** accordingly.
Without this, new, deleted, or renamed jobs will not be recognized as required
when checking Pull Requests.

## Overview

The `Greengage CI` workflow triggers on:

- **Push events** to `main` branch (after merged PR) or versioned release tags
  (`6.*`).
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
  DockerHub. Runs for push to `main` (retags to `latest`) and tags (uses tag
  like `6.28.2`) after build.

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

## Greengage ABI Tests Workflow

A separate workflow, `Greengage ABI Tests`, is responsible for checking
Application Binary Interface (ABI) compatibility between the current codebase
and the latest stable release. It helps ensure that no breaking changes are
introduced to the binary interface.

### Key Features

- **Triggers:** `workflow_dispatch`, `pull_request` (for specific paths), and
`push` to the `6X_STABLE` branch (for specific paths).
- **Concurrency Control:** Uses concurrency groups to cancel previous runs on
new push to same PR/branch.
- **Baseline Detection:** Automatically determines the baseline version (latest
tag in the 6.* series) for ABI comparison.
- **Exception Lists:** Supports exception lists for symbols and types to ignore
during ABI comparison.
- **Artifact Creation:** Generates ABI dumps for both baseline and current
code, then compares them and produces a compatibility report.

### Behavior

1. **Setup Phase:** Determines the baseline version (latest 6.* tag) and checks
for exception lists.
2. **ABI Dump Phase:** Builds Greengage for both the baseline version and the
current commit (or PR) and creates ABI dumps for the `postgres` library.
3. **Comparison Phase:** Compares the two ABI dumps using
`abi-compliance-checker`, taking into account any exception lists. The report
is then printed and uploaded as an artifact.

The workflow helps ensure that no unintended ABI changes are introduced. If ABI
changes are detected, the workflow will fail, and the report will show the
differences.

## Greengage SQL Dump Workflow

A separate workflow `Greengage SQL Dump` is responsible for generating SQL dump
artifacts after the main CI process completes successfully. It is triggered
automatically upon the completion of the `Greengage CI` workflow.

### Key Features

- **Triggers:** `workflow_run: workflows: ["Greengage CI"], types: [completed]`
- **Branch Targeting:** Runs only for the `main` and `7.x` branches.
- **Version Detection:** Automatically determines the database version (6 or 7)
based on the triggering branch.
- **Artifact Creation:** Executes regression tests with the `dump_db: "true"`
parameter to generate a SQL dump archive, which is then uploaded as a workflow
artifact.
- **Controlled Execution:** Since the main CI workflow runs on `main` and `7.x`
branches only for push events (which occur after tagging or final merge of a
pull request), SQL dumps are generated exclusively for verified, approved
patches after they are merged into the main branches.
- **Artifact Retention:** The generated SQL dump artifact is retained 90 days
after the last download. Each new run of the `behave tests gpexpand` workflow
(which consumes this artifact as a consumer) resets this retention period to
90 days when it downloads the artifact.

### Behavior

1. **Triggering:** Automatically starts after the `Greengage CI` workflow
finishes on the `main` or `7.x` branch.
2. **Preparation:** Configures Docker storage on the runner to utilize
`/mnt/docker` for increased disk space.
3. **Version Mapping:** Maps the branch name (`main` -> version 6, `7.x` ->
version 7) to select the correct Docker image for testing.
4. **Dump Generation:** Runs the regression test suite using the reusable
action with the `dump_db` option enabled, which creates a
`*_postgres_sqldump.tar` file.
5. **Artifact Upload:** Uploads the generated SQL dump archive as a named
artifact (e.g., `sqldump_ggdb7_ubuntu`) to the workflow run.

This workflow ensures that a current database schema dump is available as an
artifact following successful CI runs on the primary branches `main` and `7.x`.
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
