
# Running Tests

## Building the Image

> **Important: Before building the image, make sure to initialize and update**
> **Git submodules from the repository root:**
>
> ```bash
> git submodule update --init --recursive
> ```
>
> This ensures that all dependencies are properly fetched before `docker build`
> Missing submodules will cause build failures.

Set the target environment:

```bash
# Ubuntu 22.04 (default)
export TARGET_OS=ubuntu
export OS_VERSION=22.04

# Ubuntu 24.04
export TARGET_OS=ubuntu
export OS_VERSION=24.04

# Rocky Linux 8
export TARGET_OS=rockylinux
export OS_VERSION=8
```

Build the image from the repository root:

```bash
docker build -t ggdb6_${TARGET_OS}${OS_VERSION} \
  --build-arg OS_VERSION \
  -f ci/Dockerfile.${TARGET_OS} .
```

All test commands below use `ggdb6_${TARGET_OS}${OS_VERSION}` as the
image name. Make sure the variables are exported before running them.

## Regression Tests

Runs the full suite via a demo cluster inside the container:

```bash
docker run --name gpdb6_regress --rm -it \
  -e TEST_OS=${TARGET_OS} \
  -e MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=on' installcheck-world" \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  ggdb6_${TARGET_OS}${OS_VERSION} \
  bash -c "ssh-keygen -A && /usr/sbin/sshd && \
    bash /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash"
```

To switch from ORCA to the PostgreSQL planner, set:

```bash
PGOPTIONS='-c optimizer=off'
```

Add `--privileged` to enable debugger support inside the container.

## ORCA Unit Tests

```bash
docker run --rm -it ggdb6_${TARGET_OS}${OS_VERSION} \
  bash -c "gpdb_src/concourse/scripts/unit_tests_gporca.bash"
```

## ORCA Linter

The working tree must be clean — stage or commit changes first:

```bash
docker build -t ggdb6_linter -f ci/Dockerfile.linter .
docker run --rm -it ggdb6_linter
```

## Behave Tests

Feature files are in `gpMgmt/test/behave/mgmt_utils`.

```bash
export IMAGE=ggdb6_${TARGET_OS}${OS_VERSION}

# Run all features
bash ci/scripts/run_behave_tests.bash

# Run specific features
bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

Allure output is written to `allure-results`. Reports include GPDB logs
for failed tests. `gpMgmt/tests` must be on `PYTHONPATH`.

> **Note:** `allure-behave` is pinned to an older version for Python 2
> compatibility.

## Resource Group Tests

```bash
bash ci/scripts/run_resgroup_test.bash
```

## Running a Demo Cluster Manually

1. Start a container:

   ```bash
   docker run --name gpdb6_demo --rm -it \
     --sysctl 'kernel.sem=500 1024000 200 4096' \
     ggdb6_${TARGET_OS}${OS_VERSION} \
     bash -c "ssh-keygen -A && /usr/sbin/sshd && bash"
   ```

2. Set up and start the cluster inside the container:

   ```bash
   source gpdb_src/concourse/scripts/common.bash
   install_and_configure_gpdb
   gpdb_src/concourse/scripts/setup_gpadmin_user.bash
   make_cluster
   ```

3. Connect to the database:

   ```bash
   su - gpadmin -c '
     source /usr/local/greengage-db-devel/greengage_path.sh
     source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
     psql postgres'
   ```
