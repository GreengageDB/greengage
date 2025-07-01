# Greengage CI Workflow

This repository contains the main CI pipeline for the Greengage project, orchestrating the build, test, and upload stages for containerized environments. The pipeline is designed to be flexible, with parameterized inputs for version and target operating systems, allowing it to adapt to different branches and configurations.

## Overview

The `Greengage CI` workflow triggers on:

- **Push events** for versioned release tags.
- **Pull requests** to any branch.

It executes the following jobs in a matrix strategy for multiple target operating systems:

- **Build**: Constructs and pushes Docker images to the GitHub Container Registry (GHCR) with development commit SHA tag.
- **Tests**: Runs multiple test suites, including:
  - Behave tests
  - Regression tests
  - Orca tests
  - Resource group tests
- **Upload**: Retags and pushes final Docker images to GHCR after successful tests.

## Configuration

The workflow is parameterized to support flexibility:

- **Version**: Specifies the Greengage version, configurable per branch.
- **Target OS**: Supports multiple operating systems, defined in the matrix strategy.

All jobs use reusable workflows stored in the `greengagedb/greengage-ci` repository, accessible publicly for detailed inspection.

## Usage

To use this pipeline:

1. Ensure the repository has a valid `GITHUB_TOKEN` with `packages: write` permissions for GHCR access.
2. Configure the version and target OS parameters in the branch-specific workflow configuration.
3. Push a tag or create a pull request to trigger the pipeline.

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
- For specific details on each stage, refer to the respective reusable workflow files and their READMEs in the `greengagedb/greengage-ci` repository.
