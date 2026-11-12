# Greengage contributor guide

## Quick reference

From the checkout root in the selected build environment, after the
dependencies and the configured installation prefix are available:

```bash
./configure --with-perl --with-python --with-libxml --with-gssapi \
  --enable-debug-extensions --prefix=/usr/local/gpdb
make -j8
make install
source /usr/local/gpdb/greengage_path.sh
make create-demo-cluster
source gpAux/gpdemo/gpdemo-env.sh
make -C src/test/regress installcheck-tests TESTS="test_name"
make -C src/backend/utils/mmgr/test check
```

Build the `isolation2` runner and run one test from `src/test/isolation2`:

```bash
make -C src/test/isolation2 install
cd src/test/isolation2
./pg_isolation2_regress --init-file=../regress/init_file \
  --init-file=./init_file_isolation2 test_name
```

Keep an existing build's prefix and `configure` flags. For optional in-depth
guidance, install the `gg-agent` plugin with
`/plugin marketplace add GreengageDB/gg-agent` and
`/plugin install greengage@greengage-agent-skills`.

## Branch context

Greengage 6.x is based on PostgreSQL 9.4.26; do not assume that APIs from
later PostgreSQL releases exist. Code and scripts use `master`,
`MASTER_DATA_DIRECTORY`, and `qddir`.

A Greengage cluster has one master and multiple segments. The master holds
the catalog, plans queries, dispatches plans, and gathers results; user data
lives on the segments. A plan splits into slices, and one group of processes
executes each slice on the master, on one segment, or on every segment.
Motion nodes connect slices and move tuples between segments or back to the
master.

Changes to planning, execution, snapshots, transactions, catalogs, or error
handling behave differently in distributed execution, utility mode, and
master-only execution; test every mode the changed code can reach.

- `src/backend/cdb`: dispatch, Motion, distributed snapshots and
  transactions, distribution hashing, plan parallelization, endpoints.
- `src/backend/gpopt`: translation of PostgreSQL trees and metadata to and
  from the DXL representation used by GPORCA.
- `src/backend/gporca`: GPORCA and the `libgpos`, `libgpopt`, `libgpdbcost`,
  and `libnaucrates` libraries.
- `src/backend/access/appendonly`: append optimized row storage; column
  storage is in `src/backend/access/aocs`.
- `src/backend/fts`: segment fault detection.
- `src/backend/utils/gdd`: global deadlock detector.
- `src/backend/utils/mmgr`: memory accounting, virtual memory limits,
  runaway query cleanup, red-zone handling.
- `gpMgmt`: Python cluster administration commands and Behave tests.
- `gpAux`: release build, packaging, dependency, and demo cluster makefiles.
- `gpcontrib`: Greengage extensions; PostgreSQL extensions remain in
  `contrib`.

Read the subsystem documentation before changing a subsystem; start with:

- `src/backend/cdb/dispatcher/README.md`
- `src/backend/cdb/motion/README.ic-proxy.md`
- `src/backend/cdb/endpoint/README`
- `src/backend/fts/README`
- `src/backend/utils/gdd/README.md`
- `src/backend/access/appendonly/README.md`
- `src/backend/utils/resscheduler/README`

## Development environments

Prefer a Docker development container when developing and testing changes;
use an existing one or build one from the repository Dockerfiles. Use a
native build only in a configured checkout, which contains `config.status`
in its build directory. Do not audit the host to decide whether it can build
Greengage, and do not install tools, provision cloud machines, or pull
private images automatically. If neither a container nor a configured build
exists, report the missing environment.

Use the current checkout. Confirm whether commands run on the host or inside
a container; if that is unknown, stop before process termination, cgroup
changes, or privileged operations. Scripts that terminate server processes
must run in a disposable container, and privileged containers with writable
host mounts can affect the host.

### Unofficial helper tools

GreengageDB does not ship or maintain these unofficial tools for managing
development containers. Prefer them for Docker work and follow each tool's
documentation.

- [envman](https://github.com/toiletbril/envman)
- [gpdb.docker](https://github.com/RekGRpth/gpdb.docker)
- [arenadata.sh](https://github.com/RekGRpth/arenadata.sh)

### Reproduction environment

Reproduce bugs on the available development cluster; use cloud machines only
with provided access and a prepared reproduction environment. Community
packages and matching debug symbols are published at
<https://github.com/GreengageDB/greengage/releases>. When reproducing a
packaged release, install the server package and the debug symbols from the
same version.

### Native build

- `README.md`: configure, build, install, and demo cluster commands.
- `README.linux.md`: Linux dependencies.
- `README.macOS.md`: Intel and Apple silicon.

CI does not test macOS. Do not install host dependencies unless the task
explicitly includes environment setup.

### Container images

- `ghcr.io/greengagedb/greengage/ggdb6_ubuntu:latest`: Ubuntu 22.04
  development image.
- `ghcr.io/greengagedb/greengage/ggdb6_ubuntu24.04:latest`: Ubuntu 24.04
  development image.
- `gpdb6_regress:latest`: local image for the Compose Behave runner and
  GPORCA unit tests.

Prefer an existing local image; private pulls require GitHub Container
Registry authentication with the `read:packages` scope.

Images are `amd64` only; ARM hosts need `amd64` emulation and the
`--platform linux/amd64` flag. Docker Desktop provides the emulation on
macOS, and Linux ARM hosts need QEMU registered through `binfmt_misc`.
Without emulation, use an existing native build or report the limitation.

Repository containers install under `/usr/local/greengage-db-devel`. Source
the environment in every `docker exec`: `greengage_path.sh` from the
installation whose binaries are in use, then the demo env; without it
`$GPHOME` is empty. Demo clusters need adequate kernel semaphores and a
running `sshd`, and `gpstart -a` refuses when `sshd` is down.

### Repository Dockerfiles

Use `ci/Dockerfile.ubuntu` or `ci/Dockerfile.rockylinux`; see `ci/readme.md`
for local test procedures. Build images from a separate clean checkout when
the working tree contains configured or compiled files, because the
Dockerfile copies the whole checkout including build leftovers. Never clean
user work automatically.

The image keeps source at `/home/gpadmin/gpdb_src` and the compiled archive
at `/home/gpadmin/bin_gpdb/bin_gpdb.tar.gz`. For development, mount the host
checkout at `/home/gpadmin/gpdb_src`; the mount hides the baked-in source
but not the archive, so use source and binaries from the same commit. The
image also treats `src/test` as a volume, which shadows the mounted
checkout at that path, so mount `src/test` explicitly as well. The mounted
tree allows rebuilding on demand: run `make` and `make install` inside the
container. The helper tools above mount the checkout; their documentation
has the commands.

## Builds

Initialize submodules with `git submodule update --init --recursive --force`.

`gpAux/Makefile` provides `dist` for release builds and `devel` for
development builds; `devel` enables assertions, debug symbols, debug
extensions, and dependency tracking. `--enable-debug-extensions` compiles
the fault injector, is disabled by default, and is required by fault
injection tests, so the quick reference passes it. Do not create a release
build unless the task requires one.

Do not edit the generated `configure` or `src/include/pg_config.h.in`; edit
`configure.in`, regenerate both, and commit tracked generated files with
their source. Do not commit ignored build products or generated catalog
JSON; regenerate with `make -C gpAux autoconf`.

The top-level `Makefile` only locates GNU make; put project targets in
`GNUmakefile.in`, `gpAux/Makefile`, or a subsystem makefile.

## Regression tests

Regression tests need a running cluster; `make check` does not provide one,
so create the demo cluster explicitly. The quick reference shows single-test
commands for regress and `isolation2`. `src/test/isolation2/Makefile`
defines schedule targets such as `installcheck-parallel-retrieve-cursor`,
`installcheck-resgroup`, and `installcheck-mirrorless`, but no
`installcheck-tests`; `installcheck-world` covers the full installed tree.
Start with the narrowest relevant test. Full regression and Behave suites
take hours and run in CI, not on the host. When the specific tests for a
change pass, ask the user to push and let CI run the full suite.

```bash
make installcheck-world
PGOPTIONS='-c optimizer=off' make installcheck-world
make installcheck-resgroup
make installcheck-mirrorless
```

`installcheck-world` also runs `gpcheckcat -A`, `gp_replica_check`, and the
`pg_upgrade` checks; errors can appear late when an earlier test leaves
inconsistent catalog data.

Add new tests to `src/test/regress/greengage_schedule`, never to an
inherited PostgreSQL schedule. Keep groups near 20 tests, give each
fault-injection test its own group; `BUILD_TYPE=prod` skips
`installcheck-icudp`.

Output passes through `gpdiff.pl`, `gpstringsubs.pl`, `explain.pl`, and
`atmsort.pl`. When output differs, inspect `regression.diffs`, the raw
result, and the matching rules in those scripts and
`src/test/regress/init_file` before editing an expected file. `atmsort`
strips costs and row estimates from text `EXPLAIN` output, so return a row
estimate as a query result when testing one.

Leave representative objects for new object types in the regression
database; the `gpbackup` and `gpupgrade` integration suites test against
them. Follow the retention rules in `src/test/regress/README`.

The `isolation2` suite uses multiple sessions and its own syntax; read
`src/test/isolation2/sql_isolation_testcase.py` before editing its tests.

### Regression output names and resource group modes

For expected files, `pg_regress` checks `src/test/regress/resultmap` for a
platform-specific match first, then `<test>_optimizer_resgroup.out` with
ORCA and resource groups, `<test>_optimizer.out` with ORCA,
`<test>_resgroup.out` with resource groups, and `<test>.out`. The same
order applies to other expected extensions.

`xml_1.out` and `xml_2.out` are alternative expected outputs for the test
`xml`; `pg_regress` compares every `_0` through `_9` alternative and reports
the closest. A test name ending in a digit, such as `create_function_3`, has
an ordinary expected file.

Append optimized source tests can generate `_row`, `_column`,
`_row_optimizer`, and `_column_optimizer` files; update the variant matching
the changed source test.

Greengage 6.x supports resource groups on cgroups v1 only; the target is
`installcheck-resgroup` and its schedule is `isolation2_resgroup_schedule`.
The setup creates writable `cpuset`, `cpu`, `cpuacct`, and `memory`
controller directories under `/sys/fs/cgroup` plus the `gpdb` directory for
`gpadmin`; it is implemented by `concourse/scripts/ic_gpdb_resgroup.bash`.
The Compose run in `ci/scripts/run_resgroup_test.bash` prepares only
`cpuset`, `cpu`, and `memory`.

### Isolation2 syntax

Isolation2 lines use `<session><flags>: <sql>`. Numeric sessions run
synchronously; `&` marks a command that must block, `>` a background
command, `<` joins it, and `q` quits the session. `U` and `R` open utility
and retrieve connections, taking a content ID in place of the session
number; `*U` and `*R` target the master and all primary segments.
`@db_name` at the start of a session selects its database; `@pre_run` and
`@post_run` run shell substitutions. Each run recreates the test database,
so a test that needs `setup.sql` helpers must name `setup` first in the
same invocation.

## Unit and management tests

Run backend `cmockery` tests from the subsystem's `test` directory, for
example `make -C src/backend/utils/mmgr/test check`.
`make -C src/backend/utils/mmgr check` exits without running tests. GPORCA
unit-test guidance is in `src/backend/gporca/AGENTS.md`.

When a change modifies a `gpMgmt` management utility, run the Behave
features covering the command and add or update a scenario for the new
behavior. Run selected features or tags from `gpMgmt` with the database
environment sourced (`make -f Makefile.behave behave tags=smoke`); the make
target requires `tags` or `flags`.

Prefer a Docker container when testing management utilities: from the
repository root, the Compose runner provisions a multi-host cluster with an
existing local image and runs the named feature files
(`export IMAGE='gpdb6_regress:latest'`;
`bash ci/scripts/run_behave_tests.bash gpstart gpstop`). Management and
Behave guidance is in `gpMgmt/AGENTS.md`.

## Catalog changes

Greengage SQL functions are declared in `src/include/catalog/pg_proc.sql`
and generated into `pg_proc_gp.h`; PostgreSQL functions remain in
`pg_proc.h`. Follow `src/include/catalog/README.add_catalog_function`.

```bash
cd src/include/catalog
./unused_oids
perl catullus.pl -procdef pg_proc.sql -prochdr pg_proc_gp.h
cd ../../..
make -C src/backend/catalog
```

Claim an unused OID, update `pg_proc.sql`, regenerate `pg_proc_gp.h`, bump
`CATALOG_VERSION_NO` in `src/include/catalog/catversion.h`, and commit
`pg_proc_gp.h` with `pg_proc.sql` and `catversion.h`.

`gpMgmt/bin/gppylib/data/6.json` is not in the repository; the catalog make
generates it from the catalog headers and `catversion.h`. The JSON embeds
`CATALOG_VERSION_NO` and feeds `gpcheckcat` its catalog metadata. Git
ignores the file; do not commit it, although step 4 of
`README.add_catalog_function` still says otherwise.

Use `GPDB_COLUMN_DEFAULT` and `GPDB_EXTRA_COL` to add Greengage columns to
PostgreSQL catalog rows without rewriting inherited `DATA` declarations.
`gpcheckcat` reads catalog `FOREIGN_KEY` declarations; the server ignores
them.

## ABI compatibility

Preserve exported symbols and public types across minor releases. When a
patch touches them, run the dump and comparison commands in
`.abi-check/README.md` or inspect the CI ABI report; the baseline is the
newest upstream `6.*` tag. Do not infer that an ABI break is safe from
passing tests or unknown consumers. Exceptions in
`.abi-check/<baseline>/postgres.symbols.ignore` or `postgres.types.ignore`
need reviewer approval and a written compatibility analysis.

## Coding and review rules

For a specific bug, search a local upstream PostgreSQL checkout for related
commits before designing the fix; ask the user for its path if unknown.
Verify each candidate against this PostgreSQL 9.4-based branch, and
cherry-pick only when the whole change is compatible and limited to the
bug; otherwise adapt the needed parts.

Follow `CONTRIBUTING.md`. Restrict patches to the requested behavior and
its tests, and remove unrequested refactoring. In files inherited from
upstream PostgreSQL, match surrounding style and preserve formatting to
avoid merge conflicts.

- PostgreSQL-derived files use the `pgindent` version matching their newest
  upstream code; never reindent an inherited file with a newer tool.
- Greengage-only directories such as `src/backend/cdb` and
  `src/backend/access/appendonly` use current `pgindent` rules.
- Regenerate `src/tools/pgindent/typedefs.list` when accurate typedef
  coverage is required.
- GPORCA rules are in `src/backend/gporca/AGENTS.md`; Python uses four
  spaces and Pylint, and Go uses gofmt.

Where surrounding code permits, prefer `enum`, `static const`, and `static
inline` over macros. Keep scope narrow, mark unchanged values `const` and
file-local symbols `static`, and prefer typed interfaces over `void *` and
casts. Give printf-like functions a format attribute, put shared
declarations in a header included by the definition and all callers, use
`snprintf` and `strlcpy` where overflow is possible, and declare
no-argument C functions as `function(void)`. Keep comments accurate within
80 columns, end files with one newline, and remove trailing whitespace.

Keep patches focused. Tests must reproduce the failure without the fix and
pass with it. Add new cases to an existing suitable test file in its style,
explain any missing test in the pull request, preserve ABI in minor-release
patches unless a reviewed exception is approved, and add no compiler
warnings.

Write pull request titles in the imperative mood; describe the broken
behavior, the fix, and why the fix is correct. Keep the description current
with the patch, wrap commit-ready prose at 80 columns, preserve external
authorship and license headers, credit human co-authors with
`Co-authored-by` trailers, and follow `CONTRIBUTING.md` for licensing and
review.

Record workflow mistakes in the repository-root `MISTAKES.md` and read it
before starting a task; Git ignores it, so never commit it.

## Common traps

- Generated headers such as `pg_config.h`, `gram.h`, `errcodes.h`, and
  `fmgroids.h` do not exist until configure and build targets generate them.
- `configure` is generated: edit `configure.in` and run `make -C gpAux
  autoconf`.
- Fault-injection tests each need their own `test:` schedule line because
  tests on one line run concurrently; the build must be configured with
  `--enable-debug-extensions`.
- `gp_use_legacy_hashops` changes distribution hash operator classes and
  matters for upgraded databases and distribution tests.
- ORCA-only changes normally update `<test>_optimizer.out`; planner runs
  use `optimizer=off` and normally `<test>.out`. Apply the expected-file
  rules above for resource groups and platforms.
