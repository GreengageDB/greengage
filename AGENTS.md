# Greengage contributor guide

This file records Greengage 6.x behavior that may differ from PostgreSQL.
Prefer the Docker workflow. The project images contain the supported
dependencies, Python versions, services, and kernel settings. Record recurring
workflow mistakes in the configured `MISTAKES.md` file.

## Branch context

Greengage 6.x is based on PostgreSQL 9.4.26. Do not assume that APIs from
later PostgreSQL releases exist. This branch uses `master`,
`MASTER_DATA_DIRECTORY`, and `qddir` in code and scripts.

A Greengage cluster has one master and multiple segment instances. The master
holds the catalog, plans queries, dispatches plan slices, and gathers results.
User data is stored on segments. Motion nodes move tuples between segment
processes or back to the master. Changes to planning, execution, snapshots,
transactions, catalogs, or error handling must be checked in distributed
execution as well as utility mode and master-only execution.

The main Greengage additions are located here.

- `src/backend/cdb` contains dispatch, Motion, distributed snapshots and
  transactions, distribution hashing, plan parallelization, and endpoints.
- `src/backend/gpopt` translates PostgreSQL trees and metadata to and from the
  DXL representation used by GPORCA.
- `src/backend/gporca` contains GPORCA. Its libraries include `libgpos`,
  `libgpopt`, `libgpdbcost`, and `libnaucrates`.
- `src/backend/access/appendonly` contains append optimized row and column
  storage.
- `src/backend/fts` contains segment fault detection.
- `src/backend/utils/gdd` contains the global deadlock detector.
- `src/backend/utils/mmgr` contains memory accounting, virtual memory limits,
  runaway query cleanup, and red-zone handling.
- `gpMgmt` contains the Python cluster administration commands and Behave
  tests.
- `gpAux` contains the release build, packaging, dependency, and demo cluster
  makefiles.
- `gpcontrib` contains Greengage extensions. PostgreSQL extensions remain in
  `contrib`.

Read the subsystem documentation before changing a subsystem. Useful starting
points include `src/backend/cdb/dispatcher/README.md`,
`src/backend/cdb/motion/README.ic-proxy.md`,
`src/backend/cdb/endpoint/README`, `src/backend/fts/README`,
`src/backend/utils/gdd/README.md`,
`src/backend/access/appendonly/README.md`, and
`src/backend/utils/resscheduler/README`.

## Docker development

Choose a workflow based on the work. The repository `ci` files reproduce
project builds and tests.

### Local development tools

The `envman`, `gpdb.docker`, and `arenadata.sh` tools have different goals.

- The `envman` tool creates and configures a complete local Docker environment
  with one command. Use it when its managed image, mounts, ports, and helper
  scripts fit the task.
- The `gpdb.docker` repository provides a standalone Docker image and container
  workflow. Use it when the environment must be assembled or controlled with
  individual Docker commands.
- The `arenadata.sh` repository provides granular commands for configuring,
  building, testing, and operating Greengage inside a prepared development
  environment. Use it when the environment already has the required
  dependencies and mounts.

Do not assume that any tool is installed or that a repository has a particular
host path. Ask the user for the Greengage checkout path and the selected tool
path before using host files. Inspect the available tools, then use the tool
whose goal fits the task. The tools can be used independently or together.

The Ubuntu development image used by GitHub Actions is
`ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest`. The Ubuntu 24.04 image is
`ghcr.io/greengagedb/greengage/ggdb6_ubuntu24.04:latest`. The upload workflow
assigns `latest` after a push to `6.x`. Internal pull requests and pushes can
publish commit and branch tags. Authenticate before pulling a private tag. The
token must have the `read:packages` scope.

```bash
gh auth status
gh auth token \
  | docker login ghcr.io -u "$(gh api user --jq .login)" --password-stdin
docker pull ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest
```

The available images use amd64. An arm64 host must pass
`--platform linux/amd64` when it pulls, builds, or runs an image. Demo clusters
need the semaphore setting and a running sshd. Debuggers and cgroup setup need
`--privileged`.

Build an image only from a clean source tree without artifacts from an earlier
configure or build. The Dockerfile copies the whole checkout because this
repository has no `.dockerignore`. The image contains source at
`/home/gpadmin/gpdb_src` and a compiled archive at
`/home/gpadmin/bin_gpdb/bin_gpdb.tar.gz`. A bind mount over `gpdb_src` hides the
image source. It does not hide the compiled archive. The mounted source and
compiled archive must come from the same commit unless the mismatch is
intentional.

The repository and `gpdb.docker` workflows install under
`/usr/local/greengage-db-devel`. The Env helpers install under
`/usr/local/greenplum-db-devel`. Source the environment for the workflow that
built the binaries.

### Repository Docker files

Use the repository files to reproduce project builds and CI jobs. Build an
Ubuntu or Rocky Linux image from the repository root.

```bash
docker build --platform linux/amd64 -t gpdb6_regress:latest \
  -f ci/Dockerfile.ubuntu .
docker build --platform linux/amd64 -t gpdb6_ubuntu24.04:latest \
  --build-arg OS_VERSION=24.04 -f ci/Dockerfile.ubuntu .
docker build --platform linux/amd64 -t gpdb6_rockylinux9:latest \
  --build-arg OS_VERSION=9 -f ci/Dockerfile.rockylinux .
```

Start a shell and create a demo cluster inside the container.

```bash
docker run --platform linux/amd64 --name gpdb6_demo --rm -it \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest \
  bash -c 'ssh-keygen -A && /usr/sbin/sshd && bash'

source gpdb_src/concourse/scripts/common.bash
install_and_configure_gpdb
gpdb_src/concourse/scripts/setup_gpadmin_user.bash
make_cluster
su - gpadmin
source /usr/local/greengage-db-devel/greengage_path.sh
source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
psql postgres
```

### Env helper workflow

The optional Env driver creates the Ubuntu 22.04 environment with one command.
Ask the user for the Env checkout path, then assign it to `ENV_ROOT`.

```bash
ENV_ROOT=/path/to/Env
"$ENV_ROOT/envman" gpdb gg6u22
```

The settings are in `$ENV_ROOT/env/gpdb/gg6u22`, and the Docker operations are
in `$ENV_ROOT/env/gpdb/aux/image-base.sh`. The driver supports `PULL_IMG`,
`OVERRIDE_IMAGE_NAME`, `OVERRIDE_CONTAINER_NAME`, `OVERRIDE_PORT_VARIATION`,
and `BEAST_MODE`.

The following commands use the same image and mounts without `envman`. They
mount the checkout and helper directory. Container ports 6000 through 6100 are
mapped to host ports 26000 through 26100 on `127.0.0.1`.
Ask the user for the host paths before assigning these variables.

```bash
ENV_ROOT=/path/to/Env
WORK_ROOT=/path/to/workspaces
GPDB_SRC=/path/to/greengage

docker pull --platform linux/amd64 \
  ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest
docker tag ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest \
  greengage-6-ubuntu-22

docker run --detach --interactive --tty --privileged \
  --platform linux/amd64 --cgroupns=host \
  --name ggdb_6_u22_default --hostname ggdb_6_u22_default \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  --sysctl 'net.unix.max_dgram_qlen=4096' \
  --volume "$ENV_ROOT/env/gpdb/aux:/home/gpadmin/container-scripts:rw" \
  --volume "$WORK_ROOT:/home/gpadmin/host-sources:rw" \
  --volume "$GPDB_SRC:/home/gpadmin/gpdb_src:rw" \
  --publish 127.0.0.1:26000-26100:6000-6100 \
  greengage-6-ubuntu-22

docker exec ggdb_6_u22_default bash -c \
  "$(cat "$ENV_ROOT/env/gpdb/aux/image-setup.sh")"
docker exec -it ggdb_6_u22_default bash -c \
  "$(cat "$ENV_ROOT/env/gpdb/aux/container-entrypoint.sh")"
```

The demo master is available only at `127.0.0.1:26000`. The entry point
recreates `/home/gpadmin/.ssh` on each start, so manual keys and `known_hosts`
entries do not persist.

The setup helper creates `gpdb_src/run-configure`, `~/setup-cgroups`, and
`~/env`. Use the cgroup v2 mode when Docker uses a unified hierarchy. The
configure helper requires `V=6` and creates a debug development build. Its
configure flags are maintained in
`$ENV_ROOT/env/gpdb/aux/container-run-configure.sh`.

```bash
V=2 ~/setup-cgroups
V=6 ~/gpdb_src/run-configure
make -j"$(nproc)"
make install
source ~/env
make create-demo-cluster
source ~/env
```

Do not use `DOCKER_BUILD` from `image-base.sh` in this checkout. It names the
missing `arenadata/Dockerfile.ubuntu` and reads an unset `GPDB_SRC`. Pull an
image, build `ci/Dockerfile.ubuntu`, or set `OVERRIDE_IMAGE_NAME`.

### The gpdb.docker repository

Ask the user for the `gpdb.docker` checkout path, then assign it to
`GPDB_DOCKER`. Ask for the Greengage checkout path before assigning `GPDB_SRC`.

```bash
GPDB_DOCKER=/path/to/gpdb.docker
GPDB_SRC=/path/to/greengage
git clone https://github.com/RekGRpth/gpdb.docker.git "$GPDB_DOCKER"
cd "$GPDB_DOCKER"
docker build --platform linux/amd64 --file gpdb6.Dockerfile \
  --pull --network=host --tag gpdb6 .

docker volume create gpdb6-local
docker run --detach --platform linux/amd64 \
  --name gpdb6.cdw --hostname cdw --init --privileged \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  --sysctl 'net.unix.max_dgram_qlen=4096' \
  --ulimit nofile=65535 \
  --env USER_ID="$(id -u)" --env GROUP_ID="$(id -g)" \
  --mount type=volume,source=gpdb6-local,destination=/usr/local \
  --mount type=bind,source="$GPDB_SRC",destination=/home/gpadmin/gpdb_src \
  gpdb6 sudo /usr/sbin/sshd -De
docker exec -it gpdb6.cdw bash
```

The image extends the Ubuntu 24.04 GHCR image. Its entry point maps the
`gpadmin` user to the supplied host IDs, initializes SSH, and restores the
image files into the `/usr/local` volume. Build and test commands can then use
the mounted checkout.

Inspect `run6.sh` before using it. It assumes a Linux Docker host and a
`/tmpfs/data/6` directory. Its first run can stop because it changes the mode of
`.local/6` before creating that directory. It creates a `gpdb6` network and
attaches containers to a separate `docker` network. An `exit` inside the host
loop starts only `gpdb6.cdw`. The supporting `ubuntu.sh` pulls an image from the
internal `hub.adsw.io` registry.

### The arenadata.sh repository

The script repository can be cloned into any development container that has
the build dependencies and exposes the checkout as `~/gpdb_src`. Ask the user
for the intended clone path, then assign it to `ARENADATA_SH`.

```bash
ARENADATA_SH=/path/to/arenadata.sh
git clone https://github.com/RekGRpth/arenadata.sh.git "$ARENADATA_SH"
```

The scripts provide granular configure, clean, build, demo cluster, regression,
isolation2, resource group, unit, upgrade, restart, and log entry points. Read a
script before running it. The common development sequence is shown here.

```bash
"$ARENADATA_SH/config.sh"
"$ARENADATA_SH/clean.sh"
"$ARENADATA_SH/build.sh"
"$ARENADATA_SH/demo.sh"
```

The test runners accept test names, find their prerequisites, and order the
result from the suite schedules.

```bash
"$ARENADATA_SH/regress-deps.sh" test_name
"$ARENADATA_SH/isolation2-deps.sh" test_name
```

Inspect every other script before running it. Many files are personal command
templates with one hard-coded active test, commands after an early `exit`, or
references to Arenadata services and sibling repositories. Some scripts kill
all matching server processes or change cgroup permissions. Run those scripts
only inside a disposable privileged container. `dist.sh` exports debug
configure flags before it calls `gpAux dist`. It creates a debug package and
must not be used for a release build.

## Builds

Initialize submodules before building.

```bash
git submodule update --init --recursive --force
```

Release builds must use the `dist` target in `gpAux`.

```bash
make -C gpAux GPROOT=/usr/local PARALLEL_MAKE_OPTS=-j"$(nproc)" dist
```

Use the `devel` target for assertions, debug symbols, dependency tracking, and
debug extensions. Set `DEVPATH` to the installation directory because the
target uses it for its copy steps.

```bash
make -C gpAux GPROOT=/usr/local \
  DEVPATH=/usr/local/greengage-db-devel \
  PARALLEL_MAKE_OPTS=-j"$(nproc)" devel
```

For a custom development build with `envman`, use the configure options
from `$ENV_ROOT/env/gpdb/aux/container-run-configure.sh`. Platform release
flags are maintained in `gpAux/Makefile`. `CONFIGURE_FLAGS` appends options to
the `gpAux` configuration. `ENABLE_VPATH_BUILD` moves `devel` builds into
`Debug` and release builds into `Release`.

Edit `configure.in`, then regenerate `configure` and `pg_config.h.in` with the
make target.

```bash
make -C gpAux autoconf
```

Do not add targets to the top-level `Makefile`. It only locates GNU make.
Project targets belong in `GNUmakefile.in`, `gpAux/Makefile`, or a subsystem
makefile.

## Regression tests

A running cluster is required, and `make check` does not provide one. The
`installcheck` target runs `installcheck-good`. Use `installcheck-world` for
the full installed tree.

```bash
make installcheck-world
PGOPTIONS='-c optimizer=off' make installcheck-world
make installcheck-resgroup
make installcheck-mirrorless
```

`installcheck-world` also runs `gpcheckcat -A` and the `pg_upgrade` checks.
Errors can appear late if an earlier test leaves inconsistent catalog data.

Add Greengage regression tests to `src/test/regress/greengage_schedule`.
Do not add them to an inherited PostgreSQL schedule. Keep groups near 20 tests
or smaller. A test that injects a fault must have its own schedule group.
`installcheck-icudp` is skipped when `BUILD_TYPE=prod`.

Regression output passes through `gpdiff.pl`, `gpstringsubs.pl`, `explain.pl`,
and `atmsort.pl`. Check these normalizers and `src/test/regress/init_file`
before changing an expected file.

Leave representative objects for new object types in the regression database.
The gpbackup and gpupgrade integration tests use that database. Follow
`src/test/regress/README` when deciding which objects must remain.

Run one regression or isolation2 test with the helper mounted by the Env
workflow.

```bash
/home/gpadmin/container-scripts/container-run-tests.sh regress test_name
/home/gpadmin/container-scripts/container-run-tests.sh isolation2 test_name
/home/gpadmin/container-scripts/container-run-tests.sh resgroup test_name
/home/gpadmin/container-scripts/container-run-tests.sh prc test_name
```

The isolation2 suite uses multiple sessions and its own syntax. Read
`src/test/isolation2/sql_isolation_testcase.py` before editing an isolation2
test.

### Regression output names and resource group modes

`pg_regress` checks `<test>_optimizer_resgroup.out` when ORCA and resource
groups are enabled, then `<test>_optimizer.out` with ORCA, then
`<test>_resgroup.out` with resource groups, and finally `<test>.out`. The same
order applies to other expected file extensions.

The `_optimizer` and `_resgroup` parts are configuration suffixes. A name such
as `xml_1.out`, `xml_2.out`, or `create_function_3.out` is an ordinary expected
file for a test whose schedule name contains that numeric suffix. Numeric
suffixes do not select optimizer or resource group behavior. Platform-specific
expected files are selected through `src/test/regress/resultmap`.

Append optimized source tests can generate `_row`, `_column`,
`_row_optimizer`, and `_column_optimizer` files. Update the generated variant
that corresponds to the changed source test.

Greengage 6.x supports resource groups with cgroups v1 only. The resource group
target is `installcheck-resgroup`, and its isolation2 schedule is
`isolation2_resgroup_v1_schedule`.

The resource group test setup creates writable `cpuset`, `cpu`, `cpuacct`, and
`memory` controller directories under `/sys/fs/cgroup`, then creates the `gpdb`
directory for `gpadmin`. The setup is implemented by
`concourse/scripts/ic_gpdb_resgroup.bash` and `concourse/scripts/ic_resgroup.bash`.

Isolation2 lines use `<session><flags>: <sql>`. A numeric session runs
synchronously. `&` starts a command that must block, `>` starts a background
command, `<` joins a background command, and `q` quits a session. Add `U` for a
utility connection and `R` for a retrieve connection. `*U` and `*R` target the
coordinator and all primary segments. Use `@db_name` at the start of a session
to select its database. Use `@pre_run` and `@post_run` for shell substitutions.

Write Behave tests for management command behavior in `gpMgmt/test/behave`.
Add scenarios to an existing feature file when it covers the command area.
Create a feature file when no suitable file exists. Use the existing step
libraries. Tag scenarios that require the remote Compose cluster with
`@concourse_cluster`. Run selected tests with
`make -f Makefile.behave behave tags=smoke` or pass multiple options through
`flags`. The target requires `tags` or `flags`.

## Unit and management tests

Backend unit tests use cmockery and generated mocks. Run a subsystem target or
run `check` from its `test` directory. This command runs the mmgr tests.

```bash
make -C src/backend/utils/mmgr/test check
```

Do not run `make -C src/backend/utils/mmgr check`. That target has an empty
recipe and exits successfully without running the tests.

Run GPORCA unit tests through the container entry point.

```bash
docker run --rm -it gpdb6_regress:latest \
  bash -c 'gpdb_src/concourse/scripts/unit_tests_gporca.bash'
```

Behave 1.2.4 uses its own tag expression syntax. Read `gpMgmt/test/README`
before writing tag expressions. The make target requires either `tags` or
`flags`.

```bash
cd gpMgmt
make -f Makefile.behave behave tags=smoke
make -f Makefile.behave behave flags='--tags ~concourse_cluster'
```

The management code and tests support Python 2.7 and Python 3.9 through 3.12.
Ubuntu 22.04 development images use Python 2 for management and Behave tests.
Python 3 builds and installs PyGreSQL wheels for isolation2. Use syntax and
dependencies supported by every runtime required by the affected component.

The compose Behave cluster has one `cdw` and six segment hosts. Set `IMAGE`
before starting it. The script creates `allure-results`, `coverage`, and
`ssh_keys`. Create `sqldump/dump.sql` as a file before Compose processes its
bind mount. Otherwise Docker may create a directory at that path.

```bash
mkdir -p sqldump
touch sqldump/dump.sql
export IMAGE='gpdb6_regress:latest'
bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

`CI=1` prevents the local Behave wrapper from running `docker compose down -v`
afterward. Use it when containers and volumes must remain for inspection.

## Catalog changes

Greengage functions are declared in `src/include/catalog/pg_proc.sql` and
generated into `pg_proc_gp.h`. PostgreSQL functions remain in `pg_proc.h`.
Follow `src/include/catalog/README.add_catalog_function`.

```bash
cd src/include/catalog
./unused_oids
perl catullus.pl -procdef pg_proc.sql -prochdr pg_proc_gp.h
cd ../../..
make -C src/backend/catalog
```

Claim the selected OID, update `pg_proc.sql`, regenerate `pg_proc_gp.h`, and
bump `CATALOG_VERSION_NO` in `src/include/catalog/catversion.h`. The catalog
make builds `gpMgmt/bin/gppylib/data/6.json`. The filename uses the Greengage
major version, and the JSON embeds `CATALOG_VERSION_NO`. Commit the regenerated
file whenever the catalog or its `FOREIGN_KEY` declarations change, or
`gpcheckcat` will use old metadata.

Use `GPDB_COLUMN_DEFAULT` and `GPDB_EXTRA_COL` for Greengage additions to
PostgreSQL catalog rows. They avoid rewriting inherited `DATA` declarations.
`gpcheckcat` reads catalog `FOREIGN_KEY` declarations. The server ignores them.

## ABI compatibility

Every pull request runs `.github/workflows/greengage-abi-tests.yml`. It compares
the pull request build against the newest `6.*` tag in the upstream
`GreengageDB/greengage` repository. Removing an exported symbol or changing a
public type can fail even when source tests pass.

If a change is compatible for known consumers but the checker reports a break,
add the reviewed exception to
`.abi-check/<baseline>/postgres.symbols.ignore` or
`.abi-check/<baseline>/postgres.types.ignore`. Follow `.abi-check/README.md`.
Do not use an exception to bypass an unexplained ABI change.

## GitHub Actions and images

`.github/workflows/greengage-ci.yml` delegates CI work to versioned reusable
workflows in `greengagedb/greengage-ci`. Local and container runs use
`concourse/scripts`. Concourse does not run CI for this repository.

Check the workflow behavior before changing or copying it. The regression,
Behave, ORCA, resource group, and coverage jobs run only for pull requests. A
push to `6.x` or a `6.*` tag builds images and runs the upload job without those
suites. The package job has no event condition, so it also runs for pull
requests. Rocky package installation tests are disabled by `test_install:
false`.

The reusable build workflow pinned in `.github/workflows/greengage-ci.yml`
names images with this pattern.

```text
ghcr.io/greengagedb/greengage/ggdb<major>_<os><optional-version>:<tag>
```

Internal pull requests publish a sanitized branch tag and a commit SHA tag.
Fork pull requests save the image as an Actions cache when registry upload is
not allowed. A push uploads the SHA image and later updates release-facing
image tags through the upload workflow.

Package builds must call the release make targets. The reusable package
workflow unsets `CONFIGURE_FLAGS` before invoking the selected `gpAux` package
target. Keep that property when changing packaging or release automation.

Changing a required job name does not update branch protection. Coordinate
that change with a repository administrator as described in
`.github/workflows/README.md`.

## Coding and review rules

When a concrete problem is specified, search a local PostgreSQL Git checkout
for relevant upstream commits and code before designing the fix. Ask the user
for the checkout path when it is unknown. Inspect each candidate commit and
verify that it is compatible with this PostgreSQL 9.4-based branch. Cherry-pick
a commit only when its complete change is compatible and limited to the
specified problem. Otherwise, adapt only the required parts.

Keep every fix surgical. Change only the behavior and tests required by the
specified problem. Do not refactor code unless the user explicitly requests a
refactor. Treat unrequested refactoring as a defect that must be removed before
completion.

Match the style of the code being changed. This repository contains code from
several PostgreSQL releases and Greengage-only code from different periods.
Preserve formatting in upstream-derived regions to reduce future merge
conflicts.

Use the repository indentation rules.

- PostgreSQL-derived files must use the pgindent version appropriate for their
  newest upstream code. Do not reindent an inherited file with a newer tool.
- Greengage-only directories such as `src/backend/cdb` and
  `src/backend/access/appendonly` use current pgindent rules.
- Use this repository's `src/tools/pgindent/typedefs.list`, and regenerate it
  when accurate typedef coverage is required.
- GPORCA formatting is checked with clang-format 11 through `src/tools/fmt`.
  The linter runs `git diff --exit-code`, so its worktree must be clean.
- Python uses four spaces and Pylint. Go uses gofmt.

Apply these rules from the team development guidance when they fit the
surrounding code.

- Prefer `enum`, `static const`, or `static inline` over a macro when this keeps
  the code simple.
- Keep variable scope narrow. Mark unchanged values `const`, especially
  function parameters.
- Mark module-private functions and variables `static`.
- Prefer typed interfaces over `void *` and casts.
- Add a format attribute to printf-like functions.
- Put declarations used outside a source file in a header included by both the
  definition and every caller.
- Use `snprintf` and `strlcpy` when overflow is possible.
- Declare a C function with no arguments as `function(void)`.
- Keep function comments accurate and within 80 columns.
- End every file with one newline and remove trailing whitespace.

Keep patches focused. Tests must reproduce the bug without the fix and cover
the required cases. Add them to an existing suitable test file and match that
file's style. Explain a missing test in the pull request. A minor release patch
must preserve ABI unless a reviewed exception is required. A build must add no
compiler warnings.

Write the pull request title in the imperative mood. Describe the wrong
behavior, the solution, and why it is correct. Keep the description current
with the patch and wrap commit-ready prose at 80 columns. Preserve external
authorship and licensing headers. Substantial contributions require one
`Co-authored-by` trailer per contributor.

Review the whole behavior described by the pull request. Reproduce the failure,
confirm the fix, check the task and description against the code, inspect side
effects, and check that variables are initialized before use. Two architectural
committee approvals are required before merge unless the project sets another
rule.

## Common traps

- Generated headers such as `pg_config.h`, `gram.h`, `errcodes.h`, and
  `fmgroids.h` do not exist until configure and build targets generate them.
- `configure` is generated. Edit `configure.in` and run `make -C gpAux
  autoconf`.
- A fault injection test requires a `devel` or `dist_faultinj` build and an
  isolated schedule group.
- `gp_use_legacy_hashops` changes distribution hash operator classes and
  matters for upgraded databases and distribution tests.
- ORCA-only plan changes normally update an `_optimizer.out` file, not the
  planner output.
- A mounted source tree can disagree with the compiled archive in the image.
- The old `README.docker.md` and `src/tools/docker/README.md` name obsolete
  registries. Use `ci/readme.md` and the GHCR workflow.
- The Env container entry point recreates `/home/gpadmin/.ssh` on each start.
  Hand-written keys and `known_hosts` entries do not persist.
