# Greengage CI Workflow

This directory contains the CI pipelines for the Greengage project,
orchestrating the build, test, and upload stages for containerized
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

- **Push events** to versioned release branches (`6.x`, `7.x`) after
  merged PR, or versioned release tags (`6.*`, `7.*`).
- **Pull requests** to any branch.

It executes the following jobs in a matrix strategy for multiple target
operating systems:

- **Build**: Constructs and pushes Docker images to the GitHub Container
  Registry (GHCR) with development commit SHA tag and branchname tag. Runs for
  pull requests and all push events (default branch and tags).
- **Tests**: Runs multiple test suites only for pull requests. The available
  suites depend on the branch version:
  - Behave tests
  - Regression tests
  - Orca tests
  - Resource group tests
  - JIT tests (version 7.x only)
- **Upload**: Retags and pushes final Docker images to GHCR and optionally
  DockerHub. Runs for push to the default branch (retags to `latest`) and tags
  after build.
- **Package**: Builds Debian packages and optionally tests deployment.
  Currently supported for version 6.x only.

## Release Workflow

A separate workflow `Greengage release` handles the uploading of Debian package
to GitHub releases. It is triggered when a release is published and uses a
composite action to manage package deployment. Currently supported for version
6.x only; package build and release upload are not yet available for version
7.x.

### Key Features

- **Triggers:** `release: [published]` — runs when a release is published,
  including re-publishing.
- **Concurrency:** Uses the same concurrency group as the CI workflow
  (`Greengage CI-${{ github.ref }}`) to ensure proper sequencing and prevent
  race conditions.
- **Cache-based Artifacts:** Restores built packages from cache using the
  commit SHA as the key, rather than downloading artifacts from previous jobs.
- **Manual Recovery:** If the cache is missing, the workflow checks the status
  of the last build for the tag and provides clear instructions for manual
  intervention. It does not automatically trigger builds to avoid infinite
  loops.
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

- **Triggers:** `workflow_run: workflows: ["Greengage CI"],
  types: [completed]`
- **Branch Targeting:** Runs only for the `6.x` and `7.x` branches.
- **Version Detection:** Automatically determines the database version (6 or 7)
  based on the triggering branch (`6.x` → version 6, `7.x` → version 7).
  Since `Greengage SQL Dump` is not branch-specific itself, the version is
  derived at runtime from the branch of the triggering `Greengage CI` run.
- **Matrix Strategy:** Runs across multiple OS configurations to generate dumps
  for all available build targets. The set of configurations mirrors what was
  built in `Greengage CI`:
  - `ubuntu` (default, no version suffix — compatible with Ubuntu 22.04
    artifact naming)
  - `ubuntu24.04` (version 6.x only)
- **Image Pull Check:** Before creating a SQL dump, the workflow attempts to
  pull the Docker image from GHCR. This handles cases where the matrix includes
  OS versions for which no image was built in the triggering CI run.
- **Conditional Dump Generation:** If the image is pulled successfully, the
  workflow runs the regression test suite with the `dump_db: "true"` parameter
  to generate a SQL dump archive. If the pull fails, the dump creation and
  upload steps are skipped for that matrix entry.
- **Artifact Upload:** Uploads the generated SQL dump archive as a named
  artifact (e.g., `sqldump_ggdb6_ubuntu`, `sqldump_ggdb7_ubuntu24.04`).
- **Verification Job:** A final job checks if at least one SQL dump was created
  across all matrix configurations by querying the GitHub Actions jobs API. If
  no dumps were uploaded, the workflow fails with an error.
- **Controlled Execution:** Since the main CI workflow runs on `6.x` and `7.x`
  branches only for push events (which occur after final merge of a PR), SQL
  dumps are generated exclusively for verified, approved patches after they are
  merged into the main branches.
- **Artifact Retention:** The generated SQL dump artifact is retained 90 days
  after the last download. Each new run of the `behave tests gpexpand` workflow
  (which consumes this artifact as a consumer) resets this retention period to
  90 days when it downloads the artifact.

### Behavior

1. **Triggering:** Automatically starts after the `Greengage CI` workflow
   finishes on the `6.x` or `7.x` branch for push events where the conclusion
   is `success`.
2. **Version Mapping:** Maps the branch name (`6.x` → version 6, `7.x` →
   version 7) and constructs the expected Docker image name using the commit
   SHA.
3. **Image Pull:** For each matrix entry, attempts to pull the Docker image
   from GHCR. If the pull succeeds, subsequent steps proceed; if it fails, all
   remaining steps for that matrix entry are skipped without failing the job.
4. **Disk Space:** Maximizes available disk space on the runner before running
   regression tests.
5. **Conditional Dump Generation:** If the image was pulled successfully, runs
   the regression test suite with the `dump_db` option enabled, which creates a
   `*_postgres_sqldump.tar` file.
6. **Artifact Upload:** Uploads the generated SQL dump archive as a named
   artifact (e.g., `sqldump_ggdb6_ubuntu`, `sqldump_ggdb7_ubuntu24.04`).
7. **Artifact Reporting:** Logs the artifact name and direct URL for each
   successfully uploaded dump.
8. **Verification:** A final job queries the GitHub Actions jobs API to count
   how many matrix jobs completed the `Upload SQL Dump` step successfully.
   Fails the workflow if none succeeded; passes if at least one dump was
   created.

This workflow ensures that a current database schema dump is available as an
artifact following successful CI runs on the primary branches `6.x` and `7.x`.

## Configuration

The workflow is parameterized to support flexibility:

- **Version**: Specifies the Greengage version (e.g., `6` or `7`), derived
  automatically from the triggering branch in the SQL Dump workflow, and
  hardcoded per branch in the CI workflow.
- **Target OS**: Supports multiple operating systems, defined in the matrix
  strategy. Ubuntu 22.04 uses no version suffix for backward compatibility with
  existing artifact naming; Ubuntu 24.04 support is currently available for
  version 6.x only.
- **Package Build and Release**: Debian package build and upload to GitHub
  releases are currently supported for version 6.x only. Version 7.x does not
  yet include package build steps in the CI workflow.

## Usage

To use this pipeline:

1. Ensure the repository has a valid `GITHUB_TOKEN` with `packages: read`
   permissions for GHCR access and `actions: write` for artifact upload.
2. Configure DockerHub credentials (`DOCKERHUB_TOKEN`, `DOCKERHUB_USERNAME`)
   for DockerHub uploads:
   - For `greengagedb/greengage`: mandatory — login failure will stop the
     workflow.
   - For other repositories: optional — login failure is allowed and other
     processes (GHCR upload, etc.) are unaffected.
3. Configure the version and target OS parameters in the branch-specific
   workflow configuration.
4. Create a pull request or push to the default branch (`7.x`) to trigger the
   pipeline.

## Important Notes on `target_os_version`

> **⚠️ BACKWARD COMPATIBILITY WARNING**
>
> For `ubuntu`, specifying `target_os_version: "22.04"` explicitly is **not
> recommended** and may break backward compatibility with previous CI versions.
>
> **Reason**: In earlier CI versions, Ubuntu version was not versioned — it was
> hardcoded as the only possible option. The version did not appear anywhere in
> the configuration.
>
> **Recommended approach**:
> - For `ubuntu`, **omit** `target_os_version` (leave it empty) to use the
>   default behavior.
> - Specify `target_os_version: "24.04"` only when you explicitly need Ubuntu
>   24.04.
>
> **Example**:
> ```yaml
> # Correct for default Ubuntu (recommended)
> - target_os: ubuntu
>
> # Correct for explicit Ubuntu 24.04
> - target_os: ubuntu
>   target_os_version: "24.04"
>
> # NOT recommended (breaks backward compatibility)
> - target_os: ubuntu
>   target_os_version: "22.04"
> ```

## Additional Documentation

Detailed README files for each process are available in the directory [README](https://github.com/greengagedb/greengage-ci/blob/main/README/)
of the `greengagedb/greengage-ci` repository:

- Build process:
  [README/REUSABLE-BUILD.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-BUILD.md)
- Package process:
  [README/REUSABLE-PACKAGE.md](https://github.com/greengagedb/greengage-ci/blob/main/README/README/REUSABLE-PACKAGE.md)
- Behave tests:
  [README/REUSABLE-TESTS-BEHAVE.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-BEHAVE.md)
- Orca tests:
  [README/REUSABLE-TESTS-ORCA.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-ORCA.md)
- Regression tests:
  [README/REUSABLE-TESTS-REGRESSION.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-REGRESSION.md)
- Resource group tests:
  [README/REUSABLE-TESTS-RESGROUP.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-RESGROUP.md)
- Upload process:
  [README/REUSABLE-UPLOAD.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-UPLOAD.md)

## Notes

- The pipeline uses a `fail-fast: false` strategy to ensure all matrix entries
  are executed, even if one fails. This allows the SQL Dump workflow to check
  all OS configurations and skip missing images gracefully.
- The full process, including build, tests, and upload, runs only before pull
  request approval. For push events (branches `6.x`, `7.x` or tags `6.*`,
  `7.*`), a build occurs to ensure correct commit references and product
  version, using the closest tag to HEAD, followed by upload.
- For `greengagedb/greengage`, DockerHub credentials (`DOCKERHUB_TOKEN`,
  `DOCKERHUB_USERNAME`) are mandatory — login failure will stop the workflow.
  For other repositories they are optional — if missing or invalid, DockerHub
  upload is skipped but other processes (GHCR upload, etc.) are unaffected.
- For specific details on each stage, refer to the respective reusable workflow
  files and their READMEs in the `greengagedb/greengage-ci` repository.
