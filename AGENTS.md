# Greengage contributor guide

This guide describes how to build, test, and change Greengage 6.x. It also
records the places where Greengage behaves differently from PostgreSQL.

Greengage can be built natively on a Linux host or inside a Docker container.
A native build uses the current checkout and the dependencies described in
`README.linux.md`. The container workflows and their images are described in
[Development environments](#development-environments).

## Branch context

Greengage 6.x is based on PostgreSQL 9.4.26. Do not assume that APIs from
later PostgreSQL releases exist. This branch uses `master`,
`MASTER_DATA_DIRECTORY`, and `qddir` in code and scripts.

A Greengage cluster has one master and multiple segment instances. The master
holds the catalog, plans queries, dispatches plans, and gathers results. User
data is stored on segments. A plan is divided into slices. A slice is a part of
the plan that one group of processes executes, either on the master or on
every segment. Motion nodes connect slices and move tuples between segments or
back to the master.

A change to planning, execution, snapshots, transactions, catalogs, or error
handling can behave differently in distributed execution, in utility mode, and
in master-only execution. Test each of these modes that the changed code can
reach.

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

## Development environments

Five workflows are described below.

- A native build runs on the host and uses the current checkout.
- The repository Docker files reproduce the CI builds and tests.
- The `envman` helper creates one development container with the checkout
  mounted from the host and reuses it on later runs.
- The `gpdb.docker` repository builds a development image and runs a cluster
  of containers that share one volume.
- The `arenadata.sh` repository provides build and test scripts that run
  inside a `gpdb.docker` container.

Use a native build when the host is a Linux system with the dependencies
installed. Use the repository Docker files to reproduce a CI job. Use
`envman` or `gpdb.docker` when the host lacks the dependencies and a long-lived
development container is wanted. Reproduce a reported bug in the environment
described in [Reproduction environment](#reproduction-environment).

The `envman`, `gpdb.docker`, and `arenadata.sh` helpers are personal projects
outside this repository. Check whether a helper is installed before using it.
A container helper mounts host directories, so ask the user for the checkout
path and the helper path before running it.

### Reproduction environment

The preferred environment for reproducing a reported bug is a distributed
cluster on cloud machines. Install the database there from the community
packages together with their debug symbol packages. The packages are not
stored in this repository. They are published as assets of the releases at
<https://github.com/GreengageDB/greengage/releases>. Each recent release
provides `greengage6_<version>.<os>_amd64.deb` and the matching
`greengage6-dbgsym_<version>.<os>_amd64.ddeb`. Install both files of the same
version. Develop and test the fix with a native build or a development
container.

### Native build

`README.linux.md` lists the steps for a Linux host. The dependency scripts are
`README.ubuntu.bash`, `README.Rhel-Rocky.bash`, and `README.CentOS.bash`. macOS
on Intel and Apple processors is described in `README.macOS.md`. CI does not
test macOS builds.

### Container images

The CI images are needed only for the container workflows and for reproducing
CI. A native build does not use them.

The Ubuntu 22.04 development image is
`ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest`. The Ubuntu 24.04 image is
`ghcr.io/greengagedb/greengage/ggdb6_ubuntu24.04:latest`. The upload workflow
assigns `latest` after a push to `6.x`. Internal pull requests and pushes also
publish commit and branch tags. Authenticate before pulling a private tag. The
token must have the `read:packages` scope.

```bash
gh auth status
gh auth token \
  | docker login ghcr.io -u "$(gh api user --jq .login)" --password-stdin
docker pull ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest
```

The images are built for amd64 only. The release platforms in `gpAux/Makefile`
are x86_64, and no CI job builds for arm64. An arm64 host can run the images
only through amd64 emulation, and every Docker command then needs
`--platform linux/amd64`. Docker Desktop on macOS provides this emulation. A
Linux arm64 host needs QEMU user emulation registered through `binfmt_misc`.
Without emulation, an arm64 host must use a native build.

Demo clusters in a container need the semaphore setting and a running sshd.
Debuggers and cgroup setup need `--privileged`.

Build an image only from a clean source tree without files left by an earlier
configure or build. The Dockerfile copies the whole checkout because this
repository has no `.dockerignore`. The image contains source at
`/home/gpadmin/gpdb_src` and a compiled archive at
`/home/gpadmin/bin_gpdb/bin_gpdb.tar.gz`. A bind mount over `gpdb_src` hides the
image source. It does not hide the compiled archive. The mounted source and
compiled archive must come from the same commit unless the mismatch is
intentional.

The repository and `gpdb.docker` workflows install under
`/usr/local/greengage-db-devel`. The `envman` helper installs under
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

### The envman helper

`envman` is a Bash environment manager published at
<https://github.com/toiletbril/envman>. Its `gpdb` environment creates one
development container with the Greengage checkout mounted from the host and
reuses that container on later runs. The command is interactive. It opens a
shell in the container and stops the container when that shell exits.

Clone the repository and put `envman` on `PATH`.

```bash
ENV_ROOT=/path/to/envman
git clone https://github.com/toiletbril/envman.git "$ENV_ROOT"
ln -s "$ENV_ROOT/envman" /usr/local/bin/envman
```

The 6.x profile is `$ENV_ROOT/env/gpdb/gg6u22`. It uses the image
`greengage-6-ubuntu-22` and the container `ggdb_6_u22_default`. The profile
knows the source paths of a few named hosts. On any other host, set `SRC_DIR`
to the directory that contains the checkout and `GPDB_SUBDIR` to the name of
the checkout directory. The image must exist locally, or `PULL_IMG` must name
an image to pull and tag.

```bash
SRC_DIR=/path/to/workspace GPDB_SUBDIR=greengage \
  PULL_IMG=ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest \
  envman gpdb gg6u22
```

The first run creates the container with these mounts and ports.

- The checkout is mounted at `/home/gpadmin/gpdb_src`, and its `src/test`
  directory is mounted again at the same place inside it.
- `SRC_DIR` is mounted at `/home/gpadmin/host-sources`.
- The helper scripts in `$ENV_ROOT/env/gpdb/aux` are mounted at
  `/home/gpadmin/container-scripts`.
- `$HOME/Public` is mounted at `/public`. The ccache directory is
  `/public/ccache`.
- `$HOME/.tmux.conf` is mounted at `/etc/tmux.conf`.
- Container ports 6000 through 6100 are published on host ports 26000 through
  26100 of `127.0.0.1`. The demo master is available at `127.0.0.1:26000`.

The first run then executes `image-setup.sh` as root. The script creates the
`gpadmin` user, replaces `/etc/apt/sources.list` with the
`mirror.truenetwork.ru` Ubuntu mirror, installs extra packages and Go, and
creates `gpdb_src/run-configure`, `~/env`, and `~/setup-cgroups`. Every start
runs `container-entrypoint.sh`. It recreates `/home/gpadmin/.ssh`, so
hand-written keys and `known_hosts` entries do not persist.

Build and start a demo cluster inside the container as `gpadmin`. The
configure helper requires `V=6` and creates a debug build. Its configure flags
are maintained in `$ENV_ROOT/env/gpdb/aux/container-run-configure.sh`.
Greengage 6.x resource groups support only cgroup v1. Run the cgroup helper in
its default v1 mode. That mode needs a Docker host with cgroup v1.

```bash
~/setup-cgroups
V=6 ~/gpdb_src/run-configure
make -j"$(nproc)"
make install
source ~/env
make create-demo-cluster
source ~/env
```

Run one regression, isolation2, resource group, or parallel retrieve cursor
test with the mounted test helper.

```bash
/home/gpadmin/container-scripts/container-run-tests.sh regress test_name
/home/gpadmin/container-scripts/container-run-tests.sh isolation2 test_name
/home/gpadmin/container-scripts/container-run-tests.sh resgroup test_name
/home/gpadmin/container-scripts/container-run-tests.sh prc test_name
```

The driver also reads `OVERRIDE_IMAGE_NAME`, `OVERRIDE_CONTAINER_NAME`, and
`OVERRIDE_PORT_VARIATION`. `BEAST_MODE` names a committed image, and the
driver replaces the container with one created from that image. Do not use
`DOCKER_BUILD`. It names the missing `arenadata/Dockerfile.ubuntu` and reads
an unset `GPDB_SRC`.

### The gpdb.docker repository

`gpdb.docker` is published at <https://github.com/RekGRpth/gpdb.docker>. Its
`gpdb6.Dockerfile` adds debugging and build tools to
`ghcr.io/greengagedb/greengage/ggdb6_ubuntu24.04:latest` and creates the image
`gpdb6`. Its `run6.sh` starts the containers. All containers share the `gpdb`
volume as the home directory of `gpadmin`. The repository README describes the
complete setup for 7.x, and the 6.x steps differ only in the version number.
The scripts assume a Linux host with access to the volume directory under
`/var/lib/docker/volumes`.

```bash
git clone https://github.com/RekGRpth/gpdb.docker.git
cd gpdb.docker
./build6.sh
docker volume create gpdb
GPDB="$(docker volume inspect --format '{{ .Mountpoint }}' gpdb)"
mkdir -p "$GPDB/src" "$GPDB/.local/6"
git clone https://github.com/RekGRpth/arenadata.sh.git "$GPDB/src"
git clone --branch 6.x --recurse-submodules \
  https://github.com/GreengageDB/greengage.git "$GPDB/src/gpdb6"
./run6.sh
docker exec -it gpdb6.cdw bash
```

Inspect `run6.sh` before running it. It removes and recreates the containers
and clears `$GPDB/.local/6`. That directory is mounted as `/usr/local` in the
containers. The script fails on its first run unless `$GPDB/.local/6` exists.
It mounts `/tmpfs/data/6` as the data directory. Create that directory or
remove the mount. It creates a `gpdb6` network and attaches the containers to
a separate network named `docker`. The supporting `ubuntu.sh` creates that
network and pulls an image from the internal `hub.adsw.io` registry. An `exit`
inside the host loop starts only `gpdb6.cdw`, although the loop names `sdw1`
through `sdw6` as well. The README also changes the permissions of
`/var/lib/docker` and of the volume directory so that the user can write
there.

The image sets `GP_MAJOR=6`, `GPHOME=/usr/local/greengage-db-devel`, and
`PGPORT=6000`. Its entry point maps the `gpadmin` user to the supplied host
IDs, creates SSH keys, and copies the image files into the `/usr/local` mount.

### The arenadata.sh repository

`arenadata.sh` is published at <https://github.com/RekGRpth/arenadata.sh>. It
contains build and test scripts for a `gpdb.docker` container. The scripts
expect `GP_MAJOR` and `GPHOME`, the checkout at `~/gpdb_src`, and the scripts
at `~/src`. The `gpdb.docker` image and the clone layout above provide all of
them. Read a script before running it. The common development sequence is
shown here.

```bash
~/src/config.sh
~/src/clean.sh
~/src/build.sh
~/src/demo.sh
```

The test runners accept test names, add the tests they depend on, and order
the result from the suite schedules.

```bash
~/src/regress-deps.sh test_name
~/src/isolation2-deps.sh test_name
```

Inspect every other script before running it. Many files are personal command
templates with one hard-coded active test, commands after an early `exit`, or
references to Arenadata services and sibling repositories. `build.sh` and
`demo.sh` kill all matching server processes, and some scripts change cgroup
permissions. Run those scripts only inside a disposable privileged container.
`dist.sh` exports debug configure flags before it calls `gpAux dist`. It
creates a debug package and must not be used for a release build.

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

Platform release flags are maintained in `gpAux/Makefile`. `CONFIGURE_FLAGS`
appends options to the `gpAux` configuration. `ENABLE_VPATH_BUILD` moves
`devel` builds into `Debug` and release builds into `Release`.

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
before changing an expected file. `atmsort` removes costs and row estimates
from text EXPLAIN output. A test of a row estimate must return the value as a
query result.

Leave representative objects for new object types in the regression database.
The gpbackup and gpupgrade integration tests use that database. Follow
`src/test/regress/README` when deciding which objects must remain.

The isolation2 suite uses multiple sessions and its own syntax. Read
`src/test/isolation2/sql_isolation_testcase.py` before editing an isolation2
test.

### Regression output names and resource group modes

`pg_regress` first checks `src/test/regress/resultmap` for a platform-specific
expected file. It then checks `<test>_optimizer_resgroup.out` when ORCA and
resource groups are enabled, `<test>_optimizer.out` with ORCA,
`<test>_resgroup.out` with resource groups, and finally `<test>.out`. The same
order applies to other expected file extensions.

Files such as `xml_1.out` and `xml_2.out` are alternative expected outputs for
the test `xml`. `pg_regress` compares the result with every `_0` through `_9`
alternative and reports the closest one. A test whose own name ends in a
number, such as `create_function_3`, has an ordinary expected file.

Append optimized source tests can generate `_row`, `_column`,
`_row_optimizer`, and `_column_optimizer` files. Update the generated variant
that corresponds to the changed source test.

Greengage 6.x supports resource groups with cgroups v1 only. The resource group
target is `installcheck-resgroup`, and its isolation2 schedule is
`isolation2_resgroup_schedule`.

The resource group test setup creates writable `cpuset`, `cpu`, `cpuacct`, and
`memory` controller directories under `/sys/fs/cgroup`, then creates the `gpdb`
directory for `gpadmin`. The setup is implemented by
`concourse/scripts/ic_gpdb_resgroup.bash`. The Compose run in
`ci/scripts/run_resgroup_test.bash` prepares only the `cpuset`, `cpu`, and
`memory` controllers.

Isolation2 lines use `<session><flags>: <sql>`. A numeric session runs
synchronously. `&` starts a command that must block, `>` starts a background
command, `<` joins a background command, and `q` quits a session. Add `U` for a
utility connection and `R` for a retrieve connection. A `U` or `R` line takes a
content ID in place of the session number. `*U` and `*R` target the master and
all primary segments. Use `@db_name` at the start of a session to select its
database. Use `@pre_run` and `@post_run` for shell substitutions.

Write Behave tests for management command behavior in `gpMgmt/test/behave`.
Add scenarios to an existing feature file when it covers the command area.
Create a feature file when no suitable file exists. Use the existing step
libraries. Tag scenarios that need the multi-host cluster with
`@concourse_cluster`. The Compose runner runs tagged and untagged scenarios
separately.

## Unit and management tests

Backend unit tests use cmockery and generated mocks. Run `check` from the
`test` directory of a subsystem. This command runs the mmgr tests.

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

The bundled Behave is version 1.2.6. `gpMgmt/test/README` describes the tag
expression syntax under a note about Behave 1.2.4, and that syntax still
applies. The make target requires either `tags` or `flags`.

```bash
cd gpMgmt
make -f Makefile.behave behave tags=smoke
make -f Makefile.behave behave flags='--tags ~concourse_cluster'
```

The management code and tests support Python 2.7 and Python 3.9 through 3.12.
Ubuntu 22.04 development images use Python 2 for management and Behave tests.
Python 3 builds and installs PyGreSQL wheels for isolation2. Use syntax and
dependencies supported by every runtime required by the affected component.

The Compose Behave cluster has one `cdw` and six segment hosts. Set `IMAGE`
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
bump `CATALOG_VERSION_NO` in `src/include/catalog/catversion.h`. Commit
`pg_proc_gp.h` together with `pg_proc.sql` and `catversion.h`.

The catalog make generates `gpMgmt/bin/gppylib/data/6.json`, and `gpcheckcat`
reads catalog metadata from that file. The JSON embeds `CATALOG_VERSION_NO`.
The file is ignored by Git and must not be committed, although step 4 of
`README.add_catalog_function` still says otherwise.

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
- A fault injection test needs a build configured with
  `--enable-debug-extensions`, such as a `devel` build, and its own schedule
  group.
- `gp_use_legacy_hashops` changes distribution hash operator classes and
  matters for upgraded databases and distribution tests.
- ORCA-only plan changes normally update an `_optimizer.out` file, not the
  planner output.
- A mounted source tree can disagree with the compiled archive in the image.
- The `envman` container entry point recreates `/home/gpadmin/.ssh` on each
  start. Hand-written keys and `known_hosts` entries do not persist.
