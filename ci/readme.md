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

## Resource group v2 isolation tests

Resource group v2 tests require a Linux host with cgroup v2 enabled.
`gpdemo-datadirs` and `testtablespace` must be directories on a regular host
filesystem, not Docker overlay or tmpfs. Note that the script changes host
cgroup settings.

For Ubuntu:
```bash
mkdir -p gpdemo-datadirs testtablespace logs
chmod -R 777 gpdemo-datadirs testtablespace logs

docker run --name gpdb7_resgroup_v2 --rm -it -e TEST_OS=ubuntu \
  --sysctl "kernel.sem=500 1024000 200 4096" \
  --privileged \
  --cgroupns=host \
  -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
  -v "$PWD/gpdemo-datadirs":/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs:rw \
  -v "$PWD/testtablespace":/home/gpadmin/gpdb_src/src/test/isolation2/testtablespace:rw \
  -v "$PWD/logs":/logs:rw \
  gpdb7_u22:latest \
  /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb_resgroup_v2.bash
```

Required Docker options:

* `--privileged` allows cgroup controller and process-management operations that
  are otherwise blocked by Docker isolation.
* `--cgroupns=host` makes `/sys/fs/cgroup` paths refer to the host cgroup
  namespace instead of a private container namespace.
* `-v /sys/fs/cgroup:/sys/fs/cgroup:rw` gives the container writable access to
  the host cgroup v2 filesystem.
* `gpdemo-datadirs` and `testtablespace` mounts keep database files used by
  IO_LIMIT tests on the host filesystem, where cgroup v2 can resolve real block
  devices.
* `logs` is optional and lets the script write `/logs/.exitcode` for CI wrappers
  or local debugging.

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
