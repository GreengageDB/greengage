# Greengage contributor guide

## Environment and workflow

Unless stated otherwise, run all configure, build, cluster, and test commands
inside a Docker development container with the checkout mounted at
`/home/gpadmin/gpdb_src`. Never build Greengage, start server processes, modify
cgroups, or install dependencies outside the container.

1. Reuse an existing running container that mounts this checkout at
   `/home/gpadmin/gpdb_src` (`docker ps`).
2. Otherwise start one from a local image (`gpdb7_regress:latest`,
   `gpdb7_u22:latest`, `ghcr.io/greengagedb/greengage/ggdb7_ubuntu:latest`, or
   `ghcr.io/greengagedb/greengage/ggdb7_ubuntu24.04:latest`). Do not pull
   private images or provision cloud machines automatically; if no suitable
   container or local image exists, report the missing environment unless asked
   to build `ci/Dockerfile.ubuntu`.

```bash
docker run -d --name gpdb7_dev --privileged \
  --cgroupns=host \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  --sysctl 'net.unix.max_dgram_qlen=4096' \
  -v "$PWD:/home/gpadmin/gpdb_src" \
  -v "$PWD/src/test:/home/gpadmin/gpdb_src/src/test" \
  ghcr.io/greengagedb/greengage/ggdb7_ubuntu:latest \
  bash -c 'ssh-keygen -A && /usr/sbin/sshd && sleep infinity'
```

From the host, set up `gpadmin` once in the fresh container. For a new
account, use the checkout user's numeric UID and GID so builds can write to
the bind mount without changing its permissions.

```bash
docker exec -i -u root -e DEV_UID="$(id -u)" -e DEV_GID="$(id -g)" \
  gpdb7_dev bash -se <<'BOOTSTRAP'
if ! id gpadmin >/dev/null 2>&1; then
  getent group "$DEV_GID" >/dev/null || groupadd -g "$DEV_GID" gpadmin
  useradd -M -u "$DEV_UID" -g "$DEV_GID" -d /home/gpadmin -s /bin/bash gpadmin
fi
# Change only the container's home directory, not its mounted contents.
chown gpadmin:"$(id -g gpadmin)" /home/gpadmin
sudo -i -u gpadmin bash -se <<'GPADMIN'
if ! test -w "$HOME/gpdb_src" || ! test -w "$HOME/gpdb_src/src/test"; then
  echo 'gpadmin cannot write to the checkout or src/test mount' >&2
  exit 1
fi
umask 077
mkdir -p "$HOME/.ssh"
if [ ! -f "$HOME/.ssh/id_rsa" ]; then
  ssh-keygen -q -t rsa -N '' -f "$HOME/.ssh/id_rsa"
fi
cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
printf 'localhost,0.0.0.0,%s %s\n' "$(hostname)" \
  "$(cat /etc/ssh/ssh_host_ed25519_key.pub)" >> "$HOME/.ssh/known_hosts"
ssh -o BatchMode=yes "$(hostname)" true
GPADMIN
BOOTSTRAP
```

Do not run `concourse/scripts/setup_gpadmin_user.bash` against a bind-mounted
checkout, it's too broad.

Container traps:

- Image binaries in `/home/gpadmin/bin_gpdb/bin_gpdb.tar.gz` come from the
  image build commit, not your mounted branch. Rebuild and `make install` from
  `/home/gpadmin/gpdb_src` before trusting test results.
- Images with a `src/test` volume shadow `/home/gpadmin/gpdb_src/src/test` if
  only `gpdb_src` is mounted; always mount `src/test` explicitly too.
- `ci/Dockerfile.ubuntu` and `ci/Dockerfile.rockylinux` copy the entire build
  context without a `.dockerignore`; build images only from a clean checkout,
  and never clean user work automatically.
- `gpstart -a` requires a running `sshd` and the `kernel.sem` sysctl above.
- Record recurring workflow mistakes in `/MISTAKES.md` and read it at the start
  of a task; Git ignores it, so never commit it.

## Build and verify (inside container as `gpadmin`)

`gpAux/Makefile` owns platform `configure` and compiler flags (`devel` for
debug/assertions/dependencies, `dist` for release builds; `CONFIGURE_FLAGS`
appends extra flags, `ENABLE_VPATH_BUILD=1` builds in `Debug/` or `Release/`,
and `--enable-debug-extensions` is enabled by default). If `config.status`
already exists, keep its prefix and flags and run incremental
`make -j"$(nproc)" && make install`.

After bootstrap, enter a login shell as `gpadmin`:

```bash
docker exec -it -u root gpdb7_dev sudo -i -u gpadmin
```

Use a login shell so `USER=gpadmin` is set. `docker exec -u gpadmin` alone
does not guarantee this variable, which `gpMgmt` and some regression tests
rely on. Substitute the running container's name when reusing one.
Source `greengage_path.sh` after login and `gpdemo-env.sh` before running
cluster tests.

```bash
cd /home/gpadmin/gpdb_src

# Initial configure + debug build + install via gpAux:
make -C gpAux GPROOT=/usr/local \
  DEVPATH="$HOME/greengage-db-devel" \
  PARALLEL_MAKE_OPTS=-j"$(nproc)" devel

# Incremental rebuilds after initial configure:
source "$HOME/greengage-db-devel/greengage_path.sh"
export PGDATABASE=postgres
make -j"$(nproc)" && make install

make create-demo-cluster
source gpAux/gpdemo/gpdemo-env.sh
```

Demo cluster logs live in `/home/gpadmin/gpAdminLogs` and segment directories
under `gpAux/gpdemo/datadirs/` (`qddir/demoDataDir-1`, `standby`,
`dbfast<N>/demoDataDir<N-1>`, `dbfast_mirror<N>/demoDataDir<N-1>`).

Initialize submodules first when needed:
`git submodule update --init --recursive --force`.

Generated build files:

- Never edit `configure` or `src/include/pg_config.h.in` directly; edit
  `configure.in`, run `make -C gpAux autoconf`, and commit tracked generated
  files with `configure.in`.
- Generated headers (`pg_config.h`, `gram.h`, `errcodes.h`, `fmgroids.h`) do
  not exist until `configure` and `make` generate them.
- Top-level `Makefile` only locates GNU make; add project targets to
  `GNUmakefile.in`, `gpAux/Makefile`, or a subsystem makefile.
- Do not run `gpAux` `dist` release builds unless requested.

### Verification: narrow tests locally, full suites in CI

Run only the narrow tests covering your change against the running demo cluster
(`make check` does not start a cluster). Do not run full suites locally
(`installcheck-world`, full `isolation2`, or full Behave).

When full-suite verification is needed, push the branch and open or update a
pull request: GitHub Actions (`.github/workflows/greengage-ci.yml` and
`greengage-abi-tests.yml`) runs `regression-tests`, `behave-tests`,
`orca-tests`, `resgroup-tests`, `resgroup-v2-tests`, `jit-tests`,
`pg_upgrade-test`, `package`, and ABI checks in parallel. Test jobs trigger on
`pull_request` pushes (a branch push without an open PR does not run them).

```bash
# SQL regression test (pass PGOPTIONS='-c optimizer=off' for planner):
make -C src/test/regress installcheck-tests TESTS="test_name"

# Fault-injection regression test (loads gp_inject_fault):
make -C src/test/regress installcheck-tests TESTS="test_name" \
  EXTRA_REGRESS_OPTS="--load-extension=gp_inject_fault"

# Single isolation2 / resgroup / parallel-retrieve-cursor test
# (prepend 'setup' before test_name if it uses setup.sql helpers):
make -C src/test/isolation2 install
cd src/test/isolation2 && ./pg_isolation2_regress \
  --init-file=../regress/init_file --load-extension=gp_inject_fault \
  --init-file=./init_file_isolation2 --dbname=isolation2test test_name
# For resgroup: --init-file=./init_file_resgroup --dbname=isolation2resgrouptest
# For PRC: --dbname=isolation2parallelretrcursor

# Backend cmockery unit tests ('<subsystem>/test'; parent 'check' is a no-op):
make -C src/backend/utils/mmgr/test check

# GPORCA unit tests (builds and runs RelWithDebInfo and Debug):
/home/gpadmin/gpdb_src/concourse/scripts/unit_tests_gporca.bash

# Behave scenarios on demo cluster (requires 'tags' or 'flags'):
make -C gpMgmt -f Makefile.behave behave tags=smoke
make -C gpMgmt -f Makefile.behave behave flags='--tags ~concourse_cluster'
```

Targeted multi-host Behave via Docker Compose from repo root (`touch
sqldump/dump.sql` first; `CI=1` skips `docker compose down -v`; the Compose
runner excludes `@concourse_cluster` scenarios):

```bash
mkdir -p sqldump && touch sqldump/dump.sql
IMAGE=gpdb7_regress:latest bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

## Architecture and branch invariants

- **PostgreSQL 12.22 base:** Do not use APIs, catalog columns, or SQL syntax
  from PostgreSQL 13+. Code and scripts use coordinator terminology: the demo
  cluster exports `COORDINATOR_DATA_DIRECTORY` (and legacy
  `MASTER_DATA_DIRECTORY`), while the coordinator data directory is still named
  `qddir`.
- **MPP execution:** The coordinator holds catalogs, plans queries, dispatches
  plan slices, and gathers results; user data lives on segments. Slices run on
  the coordinator, one segment, or all segments, connected by Motion nodes.
  Test changes to planning, execution, snapshots, transactions, catalogs, or
  error handling across distributed execution, utility mode, and
  coordinator-only execution.
- **Python runtime:** `gpMgmt`, Behave, and `isolation2` use Python 3 (`gpMgmt`
  connects via `psycopg2`).

Non-obvious directory roles and entry docs:

- `src/backend/cdb`: dispatch (`dispatcher/README.md`), Motion
  (`motion/README.ic-proxy.md`), endpoints (`endpoint/README`), distributed
  snapshots/transactions, distribution hashing (`gp_use_legacy_hashops` affects
  hash opclasses on upgraded clusters).
- `src/backend/gpopt` & `src/backend/gporca`: DXL translator (`gpopt`) and
  Cascades cost-based optimizer (`libgpos`, `libgpopt`, `libgpdbcost`,
  `libnaucrates`); unsupported queries fall back to the planner
  (`optimizer_log_failure`).
- `src/backend/access/appendonly` (`README.md`) & `src/backend/access/aocs`:
  append-optimized row and column storage.
- `src/backend/fts` (`README`): fault tolerance service.
- `src/backend/utils/gdd` (`README.md`): global deadlock detector.
- `src/backend/utils/mmgr`: memory accounting, vmem limits, runaway cleanup.
- `src/backend/utils/resscheduler` (`README`): resource queues and cgroups v1
  (`installcheck-resgroup`, `concourse/scripts/ic_gpdb_resgroup.bash`) and v2
  (`installcheck-resgroup-v2`, `concourse/scripts/ic_gpdb_resgroup_v2.bash`).
- `gpMgmt`: Python cluster utilities and `gpMgmt/test/behave` suites
  (`gpMgmt/test/README` for Behave 1.2.4/1.2.6 tag syntax).
- `gpcontrib` vs `contrib`: Greengage vs inherited PostgreSQL extensions.

## Test authoring traps

- **Schedules:** Add Greengage regression tests to
  `src/test/regress/greengage_schedule` (~20 tests per group), never to
  inherited PostgreSQL schedules. Fault-injection tests require debug
  extensions enabled and an isolated `test:` line because tests on the same
  line run concurrently.
- **Output normalizers:** `pg_regress` pipes output through `gpdiff.pl`,
  `gpstringsubs.pl`, `explain.pl`, `atmsort.pl`, and
  `src/test/regress/init_file`. `atmsort.pl` strips `EXPLAIN` costs and row
  estimates—query row estimates as SQL results when testing them.
- **Expected file precedence:** `src/test/regress/resultmap`, then
  `<test>_optimizer_resgroup.out`, `<test>_optimizer.out`,
  `<test>_resgroup.out`, `<test>.out`. `_0`..`_9` suffixes (e.g. `xml_1.out`)
  are alternative outputs unless the test name itself ends in a digit. Update
  only the variants affected (`_row`, `_column`, `_optimizer`).
- **Retained objects:** Keep representative objects for new types in the
  regression database for `gpbackup`/`gpupgrade` (`src/test/regress/README`),
  and ensure `gpcheckcat -A` stays clean.
- **Isolation2 syntax** (`src/test/isolation2/sql_isolation_testcase.py`):
  `<session><flags>: <sql>` where `&` blocks, `>` backgrounds, `<` joins, `q`
  quits, `U`/`R` open utility/retrieve sessions by content ID (`*U`/`*R` for
  coordinator + primaries), `@db_name` switches DB, and `@pre_run`/`@post_run`
  transform commands. Each test recreates its DB.

## Catalog and ABI rules

- **Catalog changes** (`src/include/catalog/README.modifying_catalogs`):
  Catalog rows (including Greengage SQL functions in `pg_proc.dat`) live in
  `.dat` files under `src/include/catalog`; `genbki.pl` generates catalog
  headers and `postgres.bki` during build. Claim an OID with `./unused_oids`
  (verify with `./duplicate_oids`; `renumber_oids.pl` and
  `reformat_dat_file.pl` are alongside), bump `CATALOG_VERSION_NO` in
  `src/include/catalog/catversion.h`, and run `make -C src/backend/catalog`.
  Never commit the gitignored `gpMgmt/bin/gppylib/data/7.json` generated by
  `make -C src/backend/catalog catalog_json` for `gpcheckcat` (which also reads
  catalog `FOREIGN_KEY` declarations).
- **ABI stability** (`.abi-check/README.md`): Minor releases compare exported
  symbols and public types against the newest upstream `7.*` tag without a `-`
  suffix. Never assume an ABI break is safe because tests pass; ignore-list
  entries in `.abi-check/<baseline>/postgres.{symbols,types}.ignore` require
  compatibility analysis and reviewer approval.

## Code style and patches

- **Upstream bugs:** Before fixing a PostgreSQL-inherited bug, ask for a local
  upstream PostgreSQL checkout path if needed, inspect upstream commits against
  12.22, and cherry-pick only when the full commit is compatible and scoped to
  the bug; otherwise adapt minimal hunks.
- **Surgical diffs:** Change only code and tests required by the task; no
  unrequested refactoring, and tests must fail without the fix and pass with
  it. Add zero compiler warnings.
- **Formatting:**
  - Inherited PostgreSQL C/Perl files: match surrounding style; never reindent
    with a newer `pgindent` (`src/tools/pgindent/README.gpdb`).
  - Greengage-only C (`src/backend/cdb`, `src/backend/access/appendonly`):
    current `pgindent` with `src/tools/pgindent/typedefs.list`.
  - GPORCA and `gpopt` (`src/backend/gporca`, `src/backend/gpopt`,
    `src/include/gpopt`): `CLANG_FORMAT=clang-format-11 src/tools/fmt chk`
    (CI linter checks a clean worktree via `ci/Dockerfile.linter`).
  - Python: 4 spaces + `pylint`; Go: `gofmt`.
- **Commits & PRs** (`CONTRIBUTING.md`): Imperative title; 80-column body
  stating broken behavior, fix, and why it holds; preserve third-party headers
  and `Co-authored-by` trailers.
