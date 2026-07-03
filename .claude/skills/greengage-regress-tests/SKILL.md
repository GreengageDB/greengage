---
name: greengage-regress-tests
description: Run GGDB regression tests (src/test/regress and isolation2) against the running gpdemo cluster, select the optimizer=on/off path and the right expected file, satisfy setup dependencies, and avoid false passes. Use when reproducing or validating a single test or a schedule under optimizer=off or ORCA, when picking the right make target (installcheck/installcheck-good/installcheck-world, JIT matrix, unittest-check, isolation2, behave, resgroup), or when deciding whether a failure is flaky.
---

# Running GGDB regression tests

Tests run against the already-running gpdemo (`PGPORT=7000`) via `pg_regress`
with `--use-existing`. `gpdiff.pl`/`atmsort.pl` normalize output at diff time
(trailing whitespace, separator widths, unordered result-set ordering, and
`init_file` matchignore/matchsubs patterns); the expected files store raw output.
Each merge campaign pairs a docker container with a git worktree — commands below
use `<container>` for it. Full suite inventory + commands: [suites.md](suites.md);
the canonical CI suite list is `arenadata/readme.md`.

## Single regress test

```bash
sudo docker exec -e USER=gpadmin -u gpadmin <container> bash -lc '
  source /usr/local/greenplum-db-devel/greenplum_path.sh; export PGPORT=7000 GPHOME=/usr/local/greenplum-db-devel
  cd /home/gpadmin/gpdb_src/src/test/regress; mkdir -p testtablespace
  PGOPTIONS="-c optimizer=off" ./pg_regress --inputdir=. --bindir=$GPHOME/bin \
     --use-existing --dbname=regression --init-file=init_file <test>'
```

## optimizer on/off and which expected file is used

- `PGOPTIONS="-c optimizer=off"` → Postgres planner → compares against the base
  `<test>.out`. `optimizer=on` → ORCA → uses `<test>_optimizer.out` if it exists,
  else falls back to `<test>.out`.
- **Consequence:** regenerating a base `<test>.out` is safe only if a separate
  `<test>_optimizer.out` exists (ORCA uses that); if not, the base file is shared
  and must work under both optimizers. Same rule in reverse for `_optimizer.out`.
- ADB primarily uses ORCA (optimizer=on); optimizer=off is the secondary path.
  Both must be green — CI runs both (see [greengage-ci-triage](../greengage-ci-triage/SKILL.md)).
- pg_regress picks the best-matching expected VARIANT; when regenerating, target
  the file named in the regression.diffs HEADER, not the one you assume.

## The `--use-existing` pollution trap (authoritative runs are fresh + sequential)

Tests are NOT idempotent: they create scratch tables and never drop them, and many
depend on shared tables loaded exactly once (tenk1, onek, point_tbl...). Re-running
into a used db yields false "already exists", doubled rows, and wrong plans.
- Quick iteration: `createdb <fresh>` + `--dbname=<fresh>` (roles are
  cluster-global — `DROP ROLE IF EXISTS` first; `DROP OWNED` for leftovers).
- **The AUTHORITATIVE result is a full schedule run that drops/recreates the db:**
  `make -C src/test/regress installcheck-small` (parallel_schedule) or
  `installcheck-good` (both schedules). A single-test pass on a polluted db proves
  nothing; a single-test failure there may be pollution, not a bug.

## Two schedules, ONE database

`make installcheck` (= `installcheck-good`) runs `parallel_schedule` **then**
`greenplum_schedule` into the same `regression` db. A test present in both
schedules (or a GGDB copy of setup that upstream moved into `test_setup`) runs
twice: its CREATEs fail "already exists" and its INSERTs **double-load shared
read-only tables**, silently corrupting later tests (see the NB comment at the
top of `greenplum_schedule`). After an upstream merge, diff both schedules for
duplicated tests and for setup-SQL the upstream reorg moved into
`test_setup.sql`/`conversion.sql`.

## Setup dependencies (upstream/core tests)

Upstream tests (brin, gin, domain, join, create_index, ...) live in
`parallel_schedule` and need a long setup chain (`test_setup`, `create_type`/
`create_function_1` → city_budget; `create_table` → point_tbl/fast_emp4000;
`copy` → tenk1; `create_misc` → onek2). To run one standalone, run the schedule
prefix up to the target on a fresh db:
`awk '/^test:/{sub(/^test: */,"");printf "%s ",$0} /<test>/{exit}' parallel_schedule`.

## `.source` tests are generated

`input/*.source` → `sql/*.sql` and `output/*.source` → `expected/*.out` are
generated at run start by pg_regress `convert_sourcefiles()` (substituting
`@abs_builddir@` etc.). The generated files are **not git-tracked** — edit the
`.source`, never a generated `.out` (see
[greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md)).
Templates MUST live in `input/`+`output/`; a `.source` misplaced into
`sql/`/`expected/` is silently never generated (audit:
`git ls-files '*.source' | grep -vE '/(input|output|yml_in)/'`). A stale
container tree can keep deleted `.source` files that regenerate stale sql each run.

## isolation2 tests (concurrent / fault-injection)

```bash
cd src/test/isolation2 && make -j8 && make install   # pg_isolation2_regress is NOT built by the backend make
source ../../../gpAux/gpdemo/gpdemo-env.sh           # COORDINATOR_DATA_DIRECTORY — several specs shell out to it
./pg_isolation2_regress --init-file=../regress/init_file --init-file=./init_file_isolation2 \
   --load-extension=gp_inject_fault \
   --bindir=$GPHOME/bin --inputdir=. --use-existing --dbname=isolation2test <spec>
```

- `--load-extension=gp_inject_fault` is REQUIRED for any fault-using spec (the
  make targets add it via `FAULTINJECTOR_OPTS` only when configured with
  `--enable-debug-extensions`); without it, fault functions "do not exist".
- Specs assume the schedule's `setup` test ran first (isolation2 runs it as a
  prereq); standalone runs may need `CREATE EXTENSION gp_inject_fault` manually.
- **Killing a fault-injection spec mid-flight wedges the cluster** (fault left
  armed + suspended workers + in-doubt 2PC). Recreate the cluster; `gpstop -afr`
  only if mirrors are synced and no fault remains — see
  [greengage-cluster-ops](../greengage-cluster-ops/SKILL.md). Kill the harness by
  PID of `sql_isolation_testcase.py`; `pkill -f pg_isolation2_regress` from a
  docker-exec shell SELF-KILLS (its own cmdline matches).
- gdd/* deadlock specs require the `gdd/prepare` … `gdd/end` bracket
  (`gp_enable_global_deadlock_detector=on` + restart); without it they hang.
- Sub-schedules (resgroup, ic-tcp/proxy, parallel-retrieve, mirrorless): [suites.md](suites.md).

## Known-flaky classes — NEVER regen a flaky

Confirm a suspected flaky by 2-3 reruns: the failing-set INTERSECTION is
deterministic (triage/regen candidates); the rest is flaky (track separately).
- EXPLAIN ANALYZE actual-rows / `Executor Memory` lines / `(never executed)`:
  per-segment row counts and memory flutter run-to-run (create_index, matview,
  select_parallel, explain header width).
- Index/KNN scan row order and cost-ties: point index-scan order, distance ties;
  shared tables re-COPY'd each run make correlation stats (and thus plan choice)
  vary — fix with a scoped `set enable_<x>=off` or an ORDER BY tiebreak in the .sql.
- Tables created WITHOUT `DISTRIBUTED BY` under ORCA get random distribution →
  per-segment counts/row-order/LIMIT-ties flutter; fix the .sql (add DISTRIBUTED
  BY / ORDER BY), then regen both expected files.
- truncate_gp AO segfile stats, AO index reltuples 0↔N: vacuum/analyze timing.
- Segment memory pressure in full parallel runs fakes failures ("could not fork",
  OOM cascades): re-run with `MAX_CONNECTIONS=4` to get the true failing set, and
  never regen from a pressure-failed result.

## False-pass traps

- gpdiff exit status: a pass = empty `regression.diffs`; check it, not just the
  test line. And grep the run log for `Use of uninitialized value` — a gpdiff perl
  warning (e.g. from a bad init_file mask) aborts comparison and fakes a pass.
- `docker exec` without `-e USER=gpadmin` breaks gp utilities and
  ``\set cur_user `echo $USER` ``-style tests silently — always pass it.
- Completion checks: match the tally line ("All N tests passed" / "N of M failed");
  `pgrep -x pg_regress` matches zombies from killed runs.
- Zombie/leftover psql sessions hold locks ("hangs"); a leftover injected fault
  aborts the next statement. A timed-out contrib run leaves a session holding
  `contrib_regression` → every later contrib's DROP DATABASE fails (cascade).
- Segment crashes leave 2PC/pgstat debris that fails the NEXT run at DROP
  DATABASE — `gpstop -ar` and re-probe before believing such errors.

See also: [suites.md](suites.md), [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md),
[greengage-cluster-ops](../greengage-cluster-ops/SKILL.md), [greengage-debug](../greengage-debug/SKILL.md),
[greengage-ci-triage](../greengage-ci-triage/SKILL.md).
