# Greengage contributor guide

## Quick reference

Use these commands in the selected build environment, from the checkout root,
after dependencies and the configured installation prefix are available.

```bash
./configure --with-perl --with-python --with-libxml --with-gssapi \
  --prefix=/usr/local/gpdb
make -j8
make install
source /usr/local/gpdb/greengage_path.sh
make create-demo-cluster
source gpAux/gpdemo/gpdemo-env.sh
make -C src/test/regress installcheck-tests TESTS="test_name"
make -C src/backend/utils/mmgr/test check
```

Build the isolation2 runner and run one test from `src/test/isolation2`.

```bash
make -C src/test/isolation2 install
cd src/test/isolation2
./pg_isolation2_regress --init-file=../regress/init_file \
  --init-file=./init_file_isolation2 --load-extension=gp_inject_fault \
  --dbname=isolation2test test_name
```

Keep an existing build's prefix and configure flags.

For optional in-depth guidance, install the gg-agent plugin.

```text
/plugin marketplace add GreengageDB/gg-agent
/plugin install greengage@greengage-agent-skills
```

## Branch context

Greengage 7.x is based on PostgreSQL 12.22. Do not assume that APIs from later
PostgreSQL releases exist. Code and scripts use the coordinator terminology.
The demo cluster exports `COORDINATOR_DATA_DIRECTORY` and also the older
`MASTER_DATA_DIRECTORY`. The coordinator data directory is still named `qddir`.

A Greengage cluster has one coordinator and multiple segment instances. The
coordinator holds the catalog, plans queries, dispatches plans, and gathers
results. User data is stored on segments. A plan is divided into slices. A
slice is a part of the plan that one group of processes executes, on the
coordinator, on one segment, or on every segment. Motion nodes connect slices
and move tuples between segments or back to the coordinator.

A change to planning, execution, snapshots, transactions, catalogs, or error
handling can behave differently in distributed execution, in utility mode, and
in coordinator-only execution. Test each of these modes that the changed code
can reach.

- `src/backend/cdb` contains dispatch, Motion, distributed snapshots and
  transactions, distribution hashing, plan parallelization, and endpoints.
- `src/backend/gpopt` translates PostgreSQL trees and metadata to and from the
  DXL representation used by GPORCA.
- `src/backend/gporca` contains GPORCA. Its libraries include `libgpos`,
  `libgpopt`, `libgpdbcost`, and `libnaucrates`.
- `src/backend/access/appendonly` contains append optimized row storage.
  Column storage is in `src/backend/access/aocs`.
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

Read the subsystem documentation before changing a subsystem. Start with
`src/backend/cdb/dispatcher/README.md`,
`src/backend/cdb/motion/README.ic-proxy.md`,
`src/backend/cdb/endpoint/README`, `src/backend/fts/README`,
`src/backend/utils/gdd/README.md`,
`src/backend/access/appendonly/README.md`, and
`src/backend/utils/resscheduler/README`.

## Development environments

Prefer a development container. Use an existing one, or build one from the
repository Docker files. Use a native build only when the checkout is already
configured. A configured checkout has `config.status` in its build directory.
Do not audit the host to decide whether it can build Greengage. If
neither a container nor a configured build exists, report the missing
environment. Do not install tools, provision cloud machines, or pull private
images automatically.

Use the current checkout. Confirm whether commands run on the host or
inside a container. If that remains unknown, stop before process termination,
cgroup changes, or privileged operations. Scripts that kill server processes
must run inside a disposable container. Privileged containers with writable
host mounts can affect the host.

### Reproduction environment

Reproduce bugs on the available development cluster. Use cloud machines only
when access and the reproduction environment have been provided. Community
packages and matching debug symbols are published at
<https://github.com/GreengageDB/greengage/releases>. Install both packages from
the same version when reproducing a packaged release.

### Native build

`README.md` describes configure, build, install, and demo cluster commands.
`README.Linux.md` links Linux dependency setup. `README.macOS.md` covers
Intel and Apple processors. CI does not test macOS. Do not install host
dependencies unless environment setup is part of the task.

### Container images

The Ubuntu 22.04 development image is
`ghcr.io/greengagedb/greengage/ggdb7_ubuntu:latest`. Prefer an existing local
image. Private pulls require authenticated GHCR access with `read:packages`.

Images are amd64. ARM hosts require amd64 emulation and
`--platform linux/amd64`. Docker Desktop provides emulation on macOS.
Linux ARM hosts require QEMU registered through `binfmt_misc`.
Without emulation, use an existing native build or report the limitation.

Repository containers install under `/usr/local/greengage-db-devel`. Source
`greengage_path.sh` from the installation whose binaries are in use. Demo
clusters need the semaphore setting and sshd.

### Repository Docker files

Use `ci/Dockerfile.ubuntu` or `ci/Dockerfile.rockylinux` and the local test
procedures in `ci/readme.md`. Build from a separate clean checkout if the
current tree contains configured or compiled files. Never clean user work
automatically. The Dockerfile copies the checkout, including build leftovers.

Image source is at `/home/gpadmin/gpdb_src`, and its compiled archive is at
`/home/gpadmin/bin_gpdb/bin_gpdb.tar.gz`. A source mount hides the image
source but leaves the archive unchanged. Use matching source and binaries.

### Unofficial helper tools

Some developers manage development containers with
[envman](https://github.com/toiletbril/envman),
[gpdb.docker](https://github.com/RekGRpth/gpdb.docker), and
[arenadata.sh](https://github.com/RekGRpth/arenadata.sh). GreengageDB does not
ship or maintain these tools. Use one only when the user asks for it, and
follow its own documentation.

## Builds

Initialize submodules before building.

```bash
git submodule update --init --recursive --force
```

Add `--enable-debug --enable-cassert` to the configure command in the quick
reference for a debugging build. `--enable-debug-extensions` builds the fault
injector and is enabled by default. Fault injection tests need a build that
has not disabled it. Release builds use the `dist` target in `gpAux/Makefile`.
Do not create a release build unless the task requires one.

Do not edit generated `configure` or `src/include/pg_config.h.in` directly.
Edit `configure.in`, regenerate both, and commit tracked generated changes
with their source. Do not commit ignored build products or generated catalog
JSON.

```bash
make -C gpAux autoconf
```

Do not add targets to the top-level `Makefile`. It only locates GNU make.
Project targets belong in `GNUmakefile.in`, `gpAux/Makefile`, or a subsystem
makefile.

## Regression tests

Regression tests require a running cluster. Create the demo cluster explicitly.
The quick reference shows how to run one regression test and one isolation2
test.
`src/test/isolation2/Makefile`
provides schedule targets such as `installcheck-resource-queue`,
`installcheck-parallel-retrieve-cursor`, and `installcheck-mirrorless`.
It has no `installcheck-tests` target. Use `installcheck-world` for the full
installed tree. Start with the narrowest relevant test. Full regression and
Behave suites can take hours, so run them only when requested or required by
the change.

```bash
make installcheck-world
PGOPTIONS='-c optimizer=off' make installcheck-world
make installcheck-resgroup
make installcheck-mirrorless
```

`installcheck-world` also runs `vacuumdb --all --analyze-only`,
`gp_replica_check`, `gpcheckcat -A`, and the `pg_upgrade` checks. Errors can
appear late if an earlier test leaves inconsistent catalog data.

Add Greengage regression tests to `src/test/regress/greengage_schedule`.
Do not add them to an inherited PostgreSQL schedule. Keep groups near 20 tests
or smaller. A test that injects a fault must have its own schedule group.
`installcheck-icudp` is skipped when `BUILD_TYPE=prod`.

Regression output passes through `gpdiff.pl`, `gpstringsubs.pl`, `explain.pl`,
and `atmsort.pl`. When output differs, inspect `regression.diffs`, the raw
result, and matching rules in these normalizers and `src/test/regress/init_file`
before editing an expected file. `atmsort` removes costs and row estimates
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

Greengage 7.x supports resource groups with cgroups v1 and cgroups v2. The v1
target is `installcheck-resgroup`, and its isolation2 schedule is
`isolation2_resgroup_v1_schedule`. The v2 target is `installcheck-resgroup-v2`,
and its schedule is `isolation2_resgroup_v2_schedule`.

The v1 test setup creates writable `cpuset`, `cpu`, `cpuacct`, and `memory`
controller directories under `/sys/fs/cgroup`, then creates the `gpdb`
directory for `gpadmin`. The v1 setup is implemented by
`concourse/scripts/ic_gpdb_resgroup.bash`.

The v2 test setup requires a Linux host with a cgroup v2 mount. Run the test as
root with `--privileged`, `--cgroupns=host`, and a read and write bind mount for
`/sys/fs/cgroup`. The setup creates `/sys/fs/cgroup/gpdb`, enables the `cpuset`,
`io`, `cpu`, and `memory` controllers, and makes the cgroup processes file
writable. The IO limit tests also require the demo data directory and test
tablespace to be on regular host filesystems. The v2 setup is implemented by
`concourse/scripts/ic_gpdb_resgroup_v2.bash`.

Isolation2 lines use `<session><flags>: <sql>`. A numeric session runs
synchronously. `&` starts a command that must block, `>` starts a background
command, `<` joins a background command, and `q` quits a session. Add `U` for a
utility connection and `R` for a retrieve connection. `*U` and `*R` target the
coordinator and all primary segments. Use `@db_name` at the start of a session
to select its database. Use `@pre_run` and `@post_run` for shell substitutions.

## Unit and management tests

Run backend cmockery tests from the subsystem's `test` directory.

```bash
make -C src/backend/utils/mmgr/test check
```

`make -C src/backend/utils/mmgr check` exits without running tests.
GPORCA unit-test guidance is in `src/backend/gporca/AGENTS.md`.
Management and Behave guidance is in `gpMgmt/AGENTS.md`.

## Catalog changes

Catalog rows, including Greengage functions, are declared in the `.dat` files
under `src/include/catalog`. Greengage functions are rows in `pg_proc.dat`.
Follow `src/include/catalog/README.modifying_catalogs`. `genbki.pl` generates
the catalog headers and `postgres.bki` during the build.

```bash
cd src/include/catalog
./unused_oids
./duplicate_oids
cd ../../..
make -C src/backend/catalog
```

Claim an unused OID, add the row, and bump `CATALOG_VERSION_NO` in
`src/include/catalog/catversion.h`. `renumber_oids.pl` and
`reformat_dat_file.pl` are in the same directory.

`gpcheckcat` reads catalog metadata from `gpMgmt/bin/gppylib/data/7.json`.
`make -C src/backend/catalog catalog_json` generates that file from the catalog
headers and `catversion.h`. The file is ignored by Git and must not be
committed. `gpcheckcat` reads the catalog `FOREIGN_KEY` declarations from it.
The server ignores them.

## ABI compatibility

Preserve exported symbols and public types in minor releases. When a change
affects them, use the dump and comparison commands in `.abi-check/README.md`
or inspect the existing ABI job report. The baseline is the newest upstream
`7.*` release tag without a `-` suffix. Do not infer that an ABI break is safe
from tests or an unknown set of consumers. Exceptions in
`.abi-check/<baseline>/postgres.symbols.ignore` or `postgres.types.ignore`
require review and an explanation of compatibility.

## Coding and review rules

When a concrete problem is specified, search a local PostgreSQL Git checkout
for relevant upstream commits and code before designing the fix. Ask the user
for the checkout path when it is unknown. Inspect each candidate commit and
verify that it is compatible with this PostgreSQL 12-based branch. Cherry-pick
a commit only when its complete change is compatible and limited to the
specified problem. Otherwise, adapt only the required parts.

Follow `CONTRIBUTING.md`. Limit fixes to the requested behavior and tests.
Remove unrequested refactoring. Match surrounding style and preserve formatting
in upstream-derived code to reduce merge conflicts.

- PostgreSQL-derived files must use the pgindent version appropriate for their
  newest upstream code. Do not reindent an inherited file with a newer tool.
- Greengage-only directories such as `src/backend/cdb` and
  `src/backend/access/appendonly` use current pgindent rules.
- Use this repository's `src/tools/pgindent/typedefs.list`, and regenerate it
  when accurate typedef coverage is required.
- GPORCA formatting rules are in `src/backend/gporca/AGENTS.md`.
- Python uses four spaces and Pylint. Go uses gofmt.

Where surrounding code permits, prefer `enum`, `static const`, or
`static inline` over macros. Keep scope narrow, unchanged values `const`, and
private symbols `static`. Prefer typed interfaces to `void *` and casts.
Add format attributes to printf-like functions. Put shared declarations in a header
included by the definition and callers. Use `snprintf` and `strlcpy` when
overflow is possible. Declare no-argument C functions as `function(void)`.
Keep comments accurate and within 80 columns. End files with one newline and
remove trailing whitespace.

Keep patches focused. Tests must reproduce the bug without the fix and cover
the required cases. Add them to an existing suitable test file and match that
file's style. Explain a missing test in the pull request. A minor release patch
must preserve ABI unless a reviewed exception is required. A build must add no
compiler warnings.

Write the pull request title in the imperative mood. Describe the wrong
behavior, the solution, and why it is correct. Keep the description current
with the patch and wrap commit-ready prose at 80 columns. Preserve external
authorship and licensing headers. Credit human co-authors with
`Co-authored-by` trailers. Follow `CONTRIBUTING.md` for licensing and review.

Review the whole behavior described by the pull request. Reproduce the failure,
confirm the fix, check the task and description against the code, inspect side
effects, and check that variables are initialized before use. Two architectural
committee approvals are required before merge unless the project sets another
rule.

Record workflow mistakes in `MISTAKES.md` at the repository root, and read it
before starting a task. Git ignores the file, and it must not be committed.

## Common traps

- Generated headers such as `pg_config.h`, `gram.h`, `errcodes.h`, and
  `fmgroids.h` do not exist until configure and build targets generate them.
- `configure` is generated. Edit `configure.in` and run `make -C gpAux
  autoconf`.
- Put each fault-injection test on a separate `test:` line in its schedule.
  Tests on one line run concurrently. Verify debug extensions are enabled.
- `gp_use_legacy_hashops` changes distribution hash operator classes and
  matters for upgraded databases and distribution tests.
- ORCA-only plan changes normally update `<test>_optimizer.out`.
  PostgreSQL planner runs use `optimizer=off` and normally `<test>.out`.
  Apply the expected-file selection rules above for resource groups and
  platform overrides.
