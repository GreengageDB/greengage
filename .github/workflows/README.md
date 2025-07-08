# Greengage CI Workflow

This repository contains the main CI pipeline for the Greengage project, orchestrating the build, test, and upload stages for containerized environments. The pipeline is designed to be flexible, with parameterized inputs for version and target operating systems, allowing it to adapt to different branches and configurations.

## Overview

The `Greengage CI` workflow triggers on:

- **Push events** to `main` branch (after merged PR) or versioned release tags (`6.*`).
- **Pull requests** to any branch.

It executes the following jobs in a matrix strategy for multiple target operating systems:

- **Build**: Constructs and pushes Docker images to the GitHub Container Registry (GHCR) with development commit SHA tag. Runs for pull requests and push to tags.
- **Tests**: Runs multiple test suites only for pull requests, including:
  - Behave tests
  - Regression tests
  - Orca tests
  - Resource group tests
- **Upload**: Retags and pushes final Docker images to GHCR and optionally DockerHub. Runs for push to `main` (unconditionally, retags to `latest`) and tags (after build, uses tag like `6.28.2`).

## Configuration

The workflow is parameterized to support flexibility:

- **Version**: Specifies the Greengage version (e.g., `6`), configurable per branch.
- **Target OS**: Supports multiple operating systems, defined in the matrix strategy.

All jobs use reusable workflows stored in the `greengagedb/greengage-ci` repository, accessible publicly for detailed inspection.

## Usage

To use this pipeline:

1. Ensure the repository has a valid `GITHUB_TOKEN` with `packages: write` permissions for GHCR access.
2. Optionally configure `DOCKERHUB_TOKEN` and `DOCKERHUB_USERNAME` for DockerHub uploads.
3. Configure the version and target OS parameters in the branch-specific workflow configuration.
4. Create a pull request or push a tag (`6.*`) to trigger the pipeline.

## Additional Documentation

Detailed README files for each process are available in the `README` directory of the `greengagedb/greengage-ci` repository. For example:

- Build process: [README/REUSABLE-BUILD.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-BUILD.md)
- Behave tests: [README/REUSABLE-TESTS-BEHAVE.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-BEHAVE.md)
- Regression tests: [README/REUSABLE-TESTS-REGRESSION.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-REGRESSION.md)
- Orca tests: [README/REUSABLE-TESTS-ORCA.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-ORCA.md)
- Resource group tests: [README/REUSABLE-TESTS-RESGROUP.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-TESTS-RESGROUP.md)
- Upload process: [README/REUSABLE-UPLOAD.md](https://github.com/greengagedb/greengage-ci/blob/main/README/REUSABLE-UPLOAD.md)

## Notes

- The pipeline uses a `fail-fast: true` strategy to stop on any matrix job failure, ensuring quick feedback.
- For push to `main`, only the upload stage runs, using images from the pull request.
- The full process, including build, tests, and upload, runs only before pull request approval. For tags, a rebuild without tests occurs to update the product version, using the closest tag to HEAD. If DockerHub credentials (`DOCKERHUB_TOKEN`, `DOCKERHUB_USERNAME`) are missing or invalid, DockerHub upload is skipped, but other processes (GHCR upload, etc.) are unaffected.
- For specific details on each stage, refer to the respective reusable workflow files and their READMEs in the `greengagedb/greengage-ci` repository.
