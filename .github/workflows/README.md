# Greengage CI Workflow

- `greengage-ci.yml` is GitHub Actions workflow manages the continuous integration pipeline for the Greengage project, orchestrating build, test, and upload stages across multiple operating systems. It leverages reusable workflows from a separate repository to maintain modularity and flexibility.

## Purpose

The workflow coordinates a sequence of isolated stages—build, test, and upload—ensuring each stage completes successfully before the next begins. It uses a matrix strategy to process multiple operating systems in parallel, halting the pipeline if any stage fails, to ensure reliability.

## Workflow Stages

1. **Build Stage**: Invokes a reusable workflow to build Docker images for each target OS, pushing them to a container registry with a temporary commit short SHA tag for subsequent testing.
2. **Test Stage**: Executes multiple test suites (behavior, regression, orca, and resource group tests) for each target OS, dependent on the build stage, using the SHA-tagged images. All tests must pass for the upload stage to proceed.
3. **Upload Stage**: Calls the "Reusable Docker Retag and Upload Workflow" to retag the SHA-tagged images as developer (branch name) or production (tag name) images and push them to the container registry, executed only if all tests succeed.

## Triggers

- **Push**: Runs on tags matching `6.*` for versioned releases.
- **Pull Request**: Runs on pull requests for any branch.

## Notes

- The workflow relies on reusable workflows from the `greengagedb/greengage-ci` repository, which may change independently. It passes minimal inputs (`version`, `target_os`) and a custom token to ensure compatibility.
- A matrix strategy processes `ubuntu` and `centos` in parallel, with `fail-fast` enabled to halt on any failure.
- The build stage produces a temporary SHA-tagged image, which is retagged as a branch or tag-based image during the upload stage if tests pass.
- A custom secret (`GHCR_TOKEN`) is required for container registry access.

## Limitations

- The workflow depends on the correct functioning of external reusable workflows.
- Any failure in a stage (e.g., build or test for any OS) halts subsequent stages.
