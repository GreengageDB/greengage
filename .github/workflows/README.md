# CI Workflows

This directory contains the CI workflows for the Greengage project,
orchestrating the build, test, and upload stages for containerized
environments. The workflows are designed to be flexible, with parameterized
inputs for version and target operating systems, allowing them to adapt to
different configurations within the `6.x` branch.

## ⚠️ Important Notice

Whenever the list of **NAMES of required jobs** in the workflow (including any
**reusable workflows**) is **added, removed, or renamed**, you must contact a
repository administrator to update the **Branch Protection Rules** accordingly.
Without this, new, deleted, or renamed jobs will not be recognized as required
when checking Pull Requests.

## Greengage CI

The `Greengage CI` workflow triggers on:

- **Push events** to versioned release branch (`6.x`) after merged PR, or
  versioned release tags (`6.*`).
- **Pull requests** to any branch.

It executes the following jobs in a matrix strategy for Ubuntu target operating
systems:

- **Build**: Constructs and pushes Docker images to the GitHub Container
  Registry (GHCR) with development commit SHA tag and branchname tag. Runs for
  pull requests and all push events (`6.x` and tags).
- **Tests**: Runs multiple test suites only for pull requests:
  - Behave tests
  - Regression tests
  - Orca tests
  - Resource group tests
  - Coverage
- **Upload**: Retags and pushes final Docker images to GHCR and optionally
  DockerHub. Runs for push to `6.x` (retags to `latest`) and tags (uses
  tag like `6.28.2`) after build.
- **Package**: Builds Debian packages and tests deployment.

## Greengage CI (No Tests)

The `Greengage CI (No Tests)` workflow follows the same build process as
`Greengage CI`, but does not run test jobs.

It is used for target operating systems where build and package validation is
required, but the test pipeline is not required.

### Key Features

- **Same Build Process:** Uses the same build workflow as `Greengage CI`,
  with Rocky Linux as the fixed target operating system.

- **No Tests:** Does not run any test suites.

- **Non-Required Workflow:** Is not configured as a required check for pull
  requests. Failures do not block pull request merges, but remain visible in
  GitHub Actions for investigation.

- **Additional OS Targets:** Used for Rocky Linux 8 and 9, which are not
  included in the main CI test matrix.

### Behavior

1. **Build:** Builds Docker images for Rocky Linux 8 and 9.

2. **Package:** Builds the RPM package for Rocky Linux 8 and 9 in the images
   from the Build job, checks it with `rpmlint`, and installs it in a clean
   container.

3. **Tests:** No test jobs are executed.

## Release Workflow

A separate workflow `Greengage release` handles the uploading of Debian
packages to GitHub releases. It is triggered when a release is published and
uses a composite action to manage package deployment.

### Key Features

- **Triggers:** `release: [released]` - Runs when a release is published
  and is not a draft or a pre-release.
- **Multi-OS Matrix:** Builds and uploads packages for ubuntu 22.04 and
  ubuntu 24.04 in parallel.
- **Cache-based Artifacts:** Restores built packages from cache using OS and
  commit SHA as the key, rather than downloading artifacts from previous jobs.
- **OS-specific Filenames:** Inserts `~{target_os}{target_os_version}` into
  each package filename before the architecture suffix prior to upload
  (e.g. `greengage6_6.30.1_amd64.deb` → `greengage6_6.30.1~ubuntu24.04_amd64.deb`).
- **Manual Recovery:** If the cache is missing, the workflow checks the status
  of the last build for the tag and provides clear instructions for manual
  intervention. It does not automatically trigger builds to avoid infinite
  loops.
- **Safe Uploads:** Uploads packages with fixed naming patterns and optional
  overwrite (`clobber` flag).

### Behavior

1. **Normal Flow (Cache Available):** Restores the Debian packages
   (`.deb`, `.ddeb`) from cache, renames them to the pattern
   `${PACKAGE_NAME}${VERSION}.${EXT}`, and uploads to the release.
2. **Cache Miss Scenarios:**
   - **No previous build or previous build successful:** Provides instructions
     to manually trigger the CI build, then restart the release workflow.
   - **Previous build failed:** Reports the failure with a link to the failed
     run and requires manual fixing before retrying.

The release workflow is designed to be robust and provide clear feedback when
issues occur, ensuring that releases are always consistent and reliable.

## Configuration

The workflow is parameterized to support flexibility:

- **Version**: Specifies the Greengage version (e.g., `6`), hardcoded in the
  CI workflow for branch `6.x`.
- **Target OS**: Supports multiple operating systems, defined in the matrix
  strategy. Ubuntu 22.04 uses no version suffix for backward compatibility
  with existing artifact naming; Ubuntu 24.04 support is available.
  Rocky Linux 8 and 9 are handled by the `Greengage CI (No Tests)` workflow.

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
4. Create a pull request or push to `6.x` or tag (`6.*`) to trigger the
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
>
> **Note**: The release workflow (`greengage-release.yml`) is exempt from this
> rule — it explicitly sets `target_os_version` for all matrix entries to
> ensure unambiguous cache key matching with the build workflow.

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

- The pipeline uses `fail-fast: false` for build and package jobs, and
  `fail-fast: true` for test jobs.
- Tests run only for pull requests.
- Upload runs only for push events to `6.x` or tags (`6.*`).
- Package job in `Greengage CI` rebuilds production-ready version without
  debug extensions and creates Debian packages. The RPM package is built by
  the `Greengage CI (No Tests)` workflow. The release workflow uploads only
  Debian packages; the RPM package is not uploaded to GitHub releases.
- The `Greengage CI (No Tests)` workflow is not a required check, so its
  failures do not block pull request merges.
- For `greengagedb/greengage`, DockerHub credentials (`DOCKERHUB_TOKEN`,
  `DOCKERHUB_USERNAME`) are mandatory — login failure will stop the workflow.
  For other repositories they are optional — if missing or invalid, DockerHub
  upload is skipped but other processes (GHCR upload, etc.) are unaffected.
- For specific details on each stage, refer to the respective reusable workflow
  files and their READMEs in the `greengagedb/greengage-ci` repository.
