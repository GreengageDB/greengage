# How to run tests

## Build docker gpdb image with developer options

Change directory to gpdb sources destination. Make sure that directry doesn't contain binary objects from previous builds. Then run:
for Ubuntu:
```bash
docker build -t gpdb7_u22:latest -f ci/Dockerfile.ubuntu .
```
for Rocky Linux:
```bash
docker build -t gpdb7_regress:latest -f ci/Dockerfile .
```

## Full regression tests suite run

We need to execute [../concourse/scripts/ic_gpdb.bash](../concourse/scripts/ic_gpdb.bash) in container to create demo cluster and run different test suites against it:
for Ubuntu:
```bash
 docker run --name gpdb7_opt_on --rm -it -e TEST_OS=ubuntu \
  -e MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=on' installcheck-world" \
  --sysctl "kernel.sem=500 1024000 200 4096" gpdb7_u22:latest \
  /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash
```
for Rocky Linux:
```bash
 docker run --name gpdb7_opt_on --rm -it -e TEST_OS=centos \
  -e MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=on' installcheck-world" \
  --sysctl "kernel.sem=500 1024000 200 4096" gpdb7_regress:latest \
  /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash
```

## Jit regression tests suite

* jit tests are basically no different from regular regression tests except they are executed with jit enabled
* jit tests need to be executed with optimizer both on and off. Notice that make flags differ a bit for each scenario

* optimizer=on
for Ubuntu:
```bash
 docker run --name gpdb7_opt_on --rm -it -e TEST_OS=ubuntu \
  -e MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=on -c jit=on -c jit_above_cost=0 -c optimizer_jit_above_cost=0 -c gp_explain_jit=off' installcheck" \
  --sysctl "kernel.sem=500 1024000 200 4096" gpdb7_u22:latest \
  /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash
```
for Rocky Linux:
```bash
 docker run --name gpdb7_opt_on --rm -it -e TEST_OS=centos \
  -e MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=on -c jit=on -c jit_above_cost=0 -c optimizer_jit_above_cost=0 -c gp_explain_jit=off' installcheck" \
  --sysctl "kernel.sem=500 1024000 200 4096" gpdb7_regress:latest \
  /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash
```

* optimizer=off
for Ubuntu:
```bash
 docker run --name gpdb7_opt_on --rm -it -e TEST_OS=ubuntu \
  -e MAKE_TEST_COMMAND="make -k PGOPTIONS='-c optimizer=off -c jit=on -c jit_above_cost=0 -c gp_explain_jit=off' installcheck" \
  --sysctl "kernel.sem=500 1024000 200 4096" gpdb7_u22:latest \
  /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash
```
for Rocky Linux:
```bash
 docker run --name gpdb7_opt_on --rm -it -e TEST_OS=centos \
  -e MAKE_TEST_COMMAND="make -k PGOPTIONS='-c optimizer=off -c jit=on -c jit_above_cost=0 -c gp_explain_jit=off' installcheck" \
  --sysctl "kernel.sem=500 1024000 200 4096" gpdb7_regress:latest \
  /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash
```

* we need to modify `MAKE_TEST_COMMAND` environment variable to run different suite. e.g. we may run test againt Postgres optimizer or ORCA with altering `PGOPTIONS` environment variable;
* we need to increase semaphore amount to be able to run demo cluster

To use gdb inside the container, add the `--privileged` flag to the run command.

## ORCA linter

```bash
docker build -t orca-linter:test -f ci/Dockerfile.linter .
docker run --rm -it orca-linter:test
```

The work directory must be clean to pass this test. Please, stage or even commit your changes.

## ORCA unit test run

for Ubuntu:
```bash
docker run --rm -it gpdb7_u22:latest bash -c "gpdb_src/concourse/scripts/unit_tests_gporca.bash"
```
for Rocky Linux:

```bash
docker run --rm -it gpdb7_regress:latest bash -c "gpdb_src/concourse/scripts/unit_tests_gporca.bash"
```

## Clang check

To just compile the code and check it for warnings with Clang on Ubuntu:
```bash
docker build -t gpdb7_u22_clang_check:latest -f ci/Dockerfile.ubuntu.clang-check .
```

Options for C and C++ compilers can be overridden via
`--build-arg CFLAGS=<...>`. The image can be safely removed afterwards, as the
check itself is performed in the build stage.

## How to run demo cluster inside docker container manually

1. Build or pull from internal registry (see above) needed image
1. Start container with
  for Ubuntu:
   ```bash
   docker run --name gpdb7_demo --rm -it --sysctl 'kernel.sem=500 1024000 200 4096' gpdb7_u22:latest \
     bash
   ```
  for Rocky Linux:
   ```bash
   docker run --name gpdb7_demo --rm -it --sysctl 'kernel.sem=500 1024000 200 4096' gpdb7_regress:latest \
     bash
   ```
1. Run the next commands in container
   ```bash
   source gpdb_src/concourse/scripts/common.bash
   # this command unpack binaries to `/usr/local/greengage-db-devel/`
   install_and_configure_gpdb
   gpdb_src/concourse/scripts/setup_gpadmin_user.bash
   make_cluster
   su - gpadmin -c '
   source /usr/local/greengage-db-devel/greengage_path.sh;
   source gpdb_src/gpAux/gpdemo/gpdemo-env.sh;
   psql postgres'
   ```

## Behave test run

Behave tests now can run locally with docker-compose.

Feature files are located in `gpMgmt/test/behave/mgmt_utils`
Before run tests you need to build a docker-image
for Ubuntu:
```bash
docker build -t "greengage7_u22:${BRANCH_NAME}" -f ci/Dockerfile.ubuntu .
```
for Rocky Linux:
```bash
docker build -t "greengage7_regress:${BRANCH_NAME}" -f ci/Dockerfile .
```

Command to run features:

for Ubuntu:
```bash
# Run all tests
IMAGE=greengage7_u22:${BRANCH_NAME} bash ci/scripts/run_behave_tests.bash

# Run specific features
IMAGE=greengage7_u22:${BRANCH_NAME} bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

for Rocky Linux:
```bash
# Run all tests
IMAGE=greengage7_regress:${BRANCH_NAME} bash ci/scripts/run_behave_tests.bash

# Run specific features
IMAGE=greengage7_regress:${BRANCH_NAME} bash ci/scripts/run_behave_tests.bash gpstart gpstop
```


Tests use `allure-behave` package and store allure output files in `allure-results` folder.
Also, the allure report for each failed test has gpdb logs attached files. See `gpMgmt/test/behave_utils/ci/formatter.py`
It required to add `gpMgmt/tests` directory to `PYTHONPATH`. 

Greengage cluster in Docker containers has its own peculiarities in preparing a cluster for tests.
All tests are run in one way or another on the demo cluster, wherever possible.
For example, cross_subnet tests or tests with tag `concourse_cluster` currently not worked because of too complex cluster preconditions.

Tests in a docker-compose cluster use the same ssh keys for `gpadmin` user and pre-add the cluster hosts to `.ssh/know_hosts` and `/etc/hosts`.

## Running pg_upgrade test locally

To run `pg_upgrade_run_6X_to_7X_migration.sh` locally, it is necessary to specify the following environment variables:
- GREENGAGE6_SRC - directory of the Greengage6 source code.
- GREENGAGE7_SRC - directory of the Greengage7 source code.
- GREENGAGE6_INSTALLATION - directory where the Greengage6 installation is located (i.e., after make install).
- GREENGAGE7_INSTALLATION - directory where the Greengage7 installation is located (i.e., after make install).
- SQL_SCHEMA - optional path to an sql file that will be loaded before performing the upgrade. If it is not specified,
  the upgrade will be performed on an empty cluster.
- CLEANUP_SCRIPT - optional path to an sql file that will be executed after loading SQL_SCHEMA, before performing pg_upgrade. It can be used to remove deprecated or otherwise failing objects from SQL_SCHEMA. If it is not specified, no cleanup will occur.
- DUMP_OPTIONS - optional parameters that will be passed to pg_dumpall to collect pre- and post-upgrade dumps. These dumps are compared at the end of the test, to determine whether it was successful. If this option is not specified, pg_dumpall will be executed with no parameters.

For example:
```bash
export GREENGAGE6_SRC=/home/gpadmin/ggdb6_src
export GREENGAGE7_SRC=/home/gpadmin/gpdb7_src
export GREENGAGE6_INSTALLATION=/usr/local/greengage-db-6X
export GREENGAGE7_INSTALLATION=/usr/local/greengage-db-7X
export SQL_SCHEMA=/home/gpadmin/dump.sql
export CLEANUP_SCRIPT=/home/gpadmin/cleanup.sql
export DUMP_OPTIONS="--data-only"
bash   ${GREENGAGE7_SRC}/src/bin/pg_upgrade/pg_upgrade_run_6X_to_7X_migration.sh
```

## Running pg_upgrade test from the image

Docker image already has all the necessary environment variables set to run the test WITHOUT a schema. The only thing left is to setup gpadmin user:
```bash
docker build -f ci/Dockerfile.pg_upgrade -t gpdb7_pgupgrade:latest .
docker run --rm \
    gpdb7_pgupgrade bash -c \
    "gpdb_src/concourse/scripts/setup_gpadmin_user.bash; \
     su gpadmin /home/gpadmin/gpdb_src/ci/scripts/pg_upgrade_run_6X_to_7X_migration.bash"
```

Note, that the command above will pull the latest Greengage images from the github repository, meaning that they wouldn't have your local changes.
Therefore, when building the pg_upgrade image, it is possible to provide locally built Greengage images via `GGDB6_IMAGE` and `GGDB7_IMAGE` build arguments. Any combination of images (e.g., none, only Greengage 6, only Greengage 7, or both Greengage 6 and Greengage 7) will work. Here is an example how to specify a local Greengage 7 image:
```bash
# Assuming that the current directory is the root of the Greengage 7 source code
docker build -t gpdb7_u22:latest -f ci/Dockerfile.ubuntu .
docker build -f ci/Dockerfile.pg_upgrade -t gpdb7_pgupgrade:latest --build-arg GGDB7_IMAGE=gpdb7_u22:latest .
docker run --rm \
    gpdb7_pgupgrade bash -c \
    "gpdb_src/concourse/scripts/setup_gpadmin_user.bash; \
     su gpadmin /home/gpadmin/gpdb_src/ci/scripts/pg_upgrade_run_6X_to_7X_migration.bash"
```

To specify a schema, mount a directory with it into the docker image:
```bash
mkdir dump_dir
echo "create database test2;" > dump_dir/dump.sql
docker run --rm \
	-v $(pwd)/dump_dir:/dump_dir \
	gpdb7_pgupgrade bash -c \
	"export SQL_SCHEMA=/dump_dir/dump.sql; \
	 gpdb_src/concourse/scripts/setup_gpadmin_user.bash; \
	 su gpadmin /home/gpadmin/gpdb_src/ci/scripts/pg_upgrade_run_6X_to_7X_migration.bash;"
```

To collect execution logs, mount an additional volume:
```bash
mkdir logs
docker run --rm \
	-v $(pwd)/dump_dir:/dump_dir \
	-v $(pwd)/logs:/logs \
	gpdb7_pgupgrade bash -c \
	"export SQL_SCHEMA=/dump_dir/dump.sql; \
	 gpdb_src/concourse/scripts/setup_gpadmin_user.bash; \
	 su gpadmin /home/gpadmin/gpdb_src/ci/scripts/pg_upgrade_run_6X_to_7X_migration.bash; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/tmp_check/dump1.sql /logs/dump1.sql; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/tmp_check/dump2.sql /logs/dump2.sql; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/regression.diffs /logs/regression.diffs;"
```

To run pg_upgrade tests with the regression dump, it is necessary to specify `cleanup_regression_dump_from_6X.sql` cleanup script. Also, only --data-only dumps can be compared without any difference. The --extra-float-digits option is specified to synchronize the formatting of floats in the dumps.
```bash
mkdir logs
docker run --rm \
	-v $(pwd)/dump_dir:/dump_dir \
	-v $(pwd)/logs:/logs \
	gpdb7_pgupgrade bash -c \
	"export SQL_SCHEMA=/dump_dir/dump.sql; \
	 export CLEANUP_SCRIPT=/home/gpadmin/gpdb_src/src/bin/pg_upgrade/cleanup_regression_dump_from_6X.sql; \
	 export DUMP_OPTIONS='--data-only --extra-float-digits=-3'; \
	 gpdb_src/concourse/scripts/setup_gpadmin_user.bash; \
	 su gpadmin /home/gpadmin/gpdb_src/ci/scripts/pg_upgrade_run_6X_to_7X_migration.bash; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/tmp_check/dump1.sql /logs/dump1.sql; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/tmp_check/dump2.sql /logs/dump2.sql; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/regression.diffs /logs/regression.diffs;"
```
