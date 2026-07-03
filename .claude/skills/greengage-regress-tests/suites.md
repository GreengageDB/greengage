# GGDB test-suite inventory

Every suite a PG-bump campaign must eventually bring to green, with the command
to run it and its main gotcha. Ground truth for the CI-run flavors is
`arenadata/readme.md`; make targets live in `GNUmakefile.in`,
`src/test/regress/GNUmakefile`, and `src/test/isolation2/Makefile`.
`<container>` = the campaign's docker container (each campaign pairs one with
its git worktree). All in-container commands need
`sudo docker exec -u gpadmin -e USER=gpadmin <container> ...` plus
`source /usr/local/greenplum-db-devel/greenplum_path.sh` and
`source gpAux/gpdemo/gpdemo-env.sh`.

## Summary table

| Suite | Command (from repo root unless noted) | Covers | Top gotcha |
|---|---|---|---|
| Core regress, upstream schedule | `make -C src/test/regress installcheck-small` (or `installcheck-parallel` for MAX_CONNECTIONS control) | parallel_schedule only (~200 upstream tests) | fresh `regression` db each run; pollution-free baseline |
| Core regress, full | `make -C src/test/regress installcheck-good` (= `make installcheck`) | parallel_schedule + greenplum_schedule in ONE db | duplicated setup across schedules double-loads shared tables |
| GGDB-only tests | part of `installcheck-good` (greenplum_schedule) | MPP DDL/DML, AO, partitioning, gp_* | many tests dispatch to all segments — grouped small to fit max_connections |
| Interconnect UDP | `make -C src/test/regress installcheck-icudp` | icudp_schedule, faults + gp_udpic_* GUCs | `icudp/icudp_full` alone takes ~15 min; skipped in prod builds |
| JIT matrix | installcheck with jit PGOPTIONS, × optimizer on/off (below) | same tests, JIT codepaths | shares the same expected/*.out as non-JIT jobs |
| Isolation (upstream-style) | `make -C src/test/isolation installcheck` | concurrent-transaction specs | drops its db only on success — an aborted run leaves an orphan db that fails gpcheckcat |
| isolation2 (main) | `make -C src/test/isolation2 installcheck` | GGDB concurrency, faults, crash-recovery, HA | wedge risk; also chains ic-tcp/ic-proxy/parallel-retrieve sub-targets |
| isolation2 resgroup | `make -C src/test/isolation2 installcheck-resgroup` | resource-group enforcement | needs writable cgroup **v1**; don't enable group mode on cgroup-v2 hosts |
| isolation2 sub-schedules | `installcheck-parallel-retrieve-cursor`, `installcheck-ic-tcp`, `installcheck-ic-proxy`, `installcheck-mirrorless` | retrieve cursors, TCP/proxy interconnect, mirrorless | ic-* targets are no-ops unless PGOPTIONS sets the matching `gp_interconnect_type`; ic-proxy needs `--enable-ic-proxy` (libuv); mirrorless needs a WITH_MIRRORS=false cluster |
| Mock unit tests | `make -s unittest-check` (top level) | cmockery mocks of backend/bin C units | serial, not `-j`; CI stops at FIRST failure — always run the full suite locally |
| gpMgmt unit | `make -C gpMgmt/bin unitdevel` | gppylib python unit tests | needs gpdemo-env.sh sourced (COORDINATOR_DATA_DIRECTORY) or several fail at import |
| contrib / gpcontrib | `make -C contrib/<c> installcheck` (same for `gpcontrib/<c>`) | curated extension list (GNUmakefile.in ICW_TARGETS) | all share db `contrib_regression`; one hung/killed run cascades DROP DATABASE failures into every later one |
| installcheck-world | `make -k installcheck-world` | everything above + src/pl, gppc, src/bin, gpcheckcat, gp_replica_check, pg_upgrade | multi-hour; use `-k`; pg_upgrade leg STOPS the main cluster (gpstart after) |
| behave (mgmt utils) | `bash arenadata/scripts/run_behave_tests.bash [features...]` (docker-compose) | gpstart/gpstop/gprecoverseg/gpexpand... | NOT runnable in a single container — scenarios build a 4-host cluster and kill your local demo cluster |
| resgroup CI flavor | `make PGOPTIONS='-c optimizer=off -c statement_mem=125MB' installcheck-resgroup -C gpdb_src/` (in the CI image, cgroups pre-chmod'd) | resource-group runtime | see arenadata/readme.md "Resource group tests" for the cgroup setup block |
| ORCA unit tests | `docker run --rm -it <image> bash -c "gpdb_src/concourse/scripts/unit_tests_gporca.bash"` | gporca C++ unit tests | CPU-heavy (cmake builds); don't run beside a measured suite |
| ORCA linter | `docker build -f arenadata/Dockerfile.linter .` + run | clang-format check of ORCA sources | requires a CLEAN worktree — stage/commit first |
| pg_upgrade | `make -C src/bin/pg_upgrade check` | in-place upgrade round-trip | builds its OWN mirrorless cluster, copies the main gpdemo (leftover dbs included), and stops the main cluster |

## Core regress details

`installcheck-good` = `parallel_schedule` then `greenplum_schedule` into one
`regression` db (`src/test/regress/GNUmakefile`). All targets pass
`--init-file=init_file` via `REGRESS_OPTS`. Cap parallelism with
`MAX_CONNECTIONS=N` — also the tool to separate real failures from
memory-pressure cascades. `EXTRA_TESTS=<name>` appends single tests to a
schedule run.

## JIT matrix

JIT tests are the SAME regression tests run with JIT forced on — there is no
separate JIT schedule. Two flavors (from `arenadata/readme.md`), run via the CI
entry `concourse/scripts/ic_gpdb.bash` with `MAKE_TEST_COMMAND`:

```bash
# optimizer=on (ORCA)
MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=on -c jit=on -c jit_above_cost=0 -c optimizer_jit_above_cost=0 -c gp_explain_jit=off' installcheck"
# optimizer=off (planner)
MAKE_TEST_COMMAND="make -k PGOPTIONS='-c optimizer=off -c jit=on -c jit_above_cost=0 -c gp_explain_jit=off' installcheck"
```

- Needs a `--with-llvm` build (check `llvmjit.so` exists) — see
  [greengage-build](../greengage-build/SKILL.md).
- Historically (PG14 and PG15 campaigns) there were NO JIT-only failures:
  every "JIT failure" also failed without jit (general merge drift). Before
  chasing a JIT bug, re-run the test without the jit GUCs.
- `init_file`'s `m/^ Settings:.*/` matchignore already hides the
  `Settings: jit='on'...` EXPLAIN line, so jit-on output regens cleanly against
  jit-off expected files (see [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md)).

## Mock unit tests (unittest-check)

```bash
make -s unittest-check          # recurses src/backend + src/bin with CFLAGS=-DUNITTEST
make -C src/backend/<dir>/test <name>-check   # one mock binary
```

cmockery-based mocks (`src/test/unit/cmockery/` + generated mocks from
`src/test/unit/mock/`). CI runs this inside
`concourse/scripts/compile_gpdb.bash` (`make GPROOT=/usr/local -s unittest-check`)
and **stops at the first failing binary** — a red CI unit job may hide more
failures; always run the full serial suite locally. Recurring post-merge
breakage classes: stale mock `expect_*` for functions upstream moved (e.g. into
shmem_request_hook), uninitialized stack structs exposed by struct-layout
changes (fix with `= {0}`), and mocks poking fields a refactor moved (e.g.
xlogreader's `main_data` moving into DecodedXLogRecord).

## isolation2 details

Build first (`make -C src/test/isolation2 -j8 install` — the harness is
not built by the backend make). The make targets add
`--load-extension=gp_inject_fault` via `FAULTINJECTOR_OPTS` only when configured
`--enable-debug-extensions`; direct `pg_isolation2_regress` invocations must
pass it explicitly. Direct full-schedule run (avoids chaining the ic-* targets):

```bash
cd src/test/isolation2
./pg_isolation2_regress --inputdir=. --bindir=$GPHOME/bin \
  --init-file=../regress/init_file --init-file=./init_file_isolation2 \
  --load-extension=gp_inject_fault --schedule=./isolation2_schedule
```

- Split the schedule into non-fault vs fault specs for a safe baseline: non-fault
  specs can fail but not wedge; fault specs (grep the schedule for gp_inject_fault
  users) carry hang/wedge risk and are best run in small batches.
- Some specs deliberately reconfigure the cluster (e.g. `prepare_limit` lowers
  `max_prepared_transactions` to 3 and restarts) and can abort a full run when
  scheduled after a heavy WAL backlog; expect a small demote/skip list to get a
  completing baseline run, then triage the demoted specs individually.
- Back-to-back failover/recovery specs (segwalrep/*, fts_*) race a just-promoted
  segment; stabilization pattern (gang-retry GUCs, pg_isready waits) is in
  [greengage-cluster-ops](../greengage-cluster-ops/SKILL.md).
- `!\retcode CMD;` lines auto-wrap their output in start_ignore in the `.out`.
- Resgroup sub-schedule uses `init_file_resgroup` + dbname
  `isolation2resgrouptest` and requires `gp_resource_manager=group` on cgroup v1.

## contrib / gpcontrib

Curated list = `ICW_TARGETS` in `GNUmakefile.in` (auto_explain, citext,
btree_gin, file_fdw, formatter_fixedwidth, extprotocol, dblink, pg_trgm,
indexscan, hstore, ltree, pgcrypto, sslinfo, uuid-ossp + gpcontrib + src/bin +
gpMgmt/bin). Run per-extension: `make -C contrib/<c> install && make -C
contrib/<c> installcheck` (default optimizer=on). Gotchas: the shared
`contrib_regression` db cascade (kill leftover sessions between extensions);
formatter_fixedwidth is legitimately slow (~200 s per heavy test), not hung;
PGXS installcheck applies the regress init_file only when the contrib's Makefile
adds it via `REGRESS_OPTS` (most ICW contribs do; formatter_fixedwidth,
extprotocol, indexscan, hstore and sslinfo don't) — without it, notices that
core regress masks (e.g. DISTRIBUTED BY) show up in diffs.

## behave (management utilities)

CI/docker-compose (the only reliable way — scenarios recreate a 4-host cluster
cdw+sdw1-3 and will stop a local demo cluster; recover with `gpstart -a`):

```bash
IMAGE=<registry>/gpdb8_u22:<branch> bash arenadata/scripts/run_behave_tests.bash [gpstart gpstop ...]
```

Whole-feature local repro on a healthy gpdemo with mirrors — use the SAME tag
selection the CI behave jobs use (`BEHAVE_FLAGS` in the workflow), or you get
~10 bogus failures from scenarios that genuinely need a multi-host cluster:

```bash
sudo docker exec -u gpadmin -e USER=gpadmin <container> bash -lc '
  source /usr/local/greenplum-db-devel/greenplum_path.sh
  cd /home/gpadmin/gpdb_src; source gpAux/gpdemo/gpdemo-env.sh; cd gpMgmt
  export PYTHONPATH=$PYTHONPATH:$GPHOME/bin/lib:$PWD/test
  behave test/behave/mgmt_utils/<feature>.feature -s -k \
    --tags <feature> --tags=~concourse_cluster'   # add --name "<substring>" for one scenario
```

(Equivalent make entry: `make -C gpMgmt behave tags=<tag>`.) Only
`@concourse_cluster` scenarios require the multi-host cluster; the rest run
against a local mirrored gpdemo exactly like CI's docker-compose job does.
Gotchas:
- `-e USER=gpadmin` is mandatory (gp utilities abort "USER environment variable
  must be set"); a failed gpconfig substep makes behave's `check_return_code`
  raise `AttributeError: 'Context' object has no attribute 'error_message'`,
  MASKING the real error — read the substep output above it, not the traceback.
- A FAILED/timed-out scenario SKIPS its remaining steps — including fault
  resets (`walsender` suspend etc.) and async-process waits — so the next
  scenarios inherit an active fault and a still-running gpmovemirrors/
  pg_basebackup and hang. Kill strays by exact name and recreate before rerun.
- Mirror-move scenarios (gpmovemirrors/gprecoverseg -i) permanently relocate
  mirror datadirs INTO the scenarios' temp trees. Never bulk-delete those trees
  while the cluster lives: a running mirror survives datadir deletion for ~1min
  on open fds and FTS only notices on its next probe, so checks keep passing on
  stale catalog state and the cluster is silently degraded for everything after
  (the cleanup-kills-live-mirror class fixed in behave's environment.py).

## Suite-green order that worked for PG15

core regress opt=off → opt=on (ORCA) → greenplum_schedule both → isolation2
main → contrib/gpcontrib → interconnect (icudp, ic-tcp) → src/bin +
pg_upgrade/pg_dump round-trip → gpMgmt unit → behave/resgroup in CI. Each stage
assumes the previous is green; crash-class bugs found late contaminate earlier
suites' reruns.
