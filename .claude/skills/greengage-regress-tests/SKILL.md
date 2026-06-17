---
name: greengage-regress-tests
description: Run GGDB regression tests (src/test/regress and isolation2) against the running gpdemo cluster, select the optimizer=on/off path and the right expected file, satisfy setup dependencies, and avoid false passes. Use to reproduce or validate a single test or a schedule under optimizer=off or ORCA.
---

# Running GGDB regression tests

Tests run against the already-running gpdemo (`PGPORT=7000`) via `pg_regress`
with `--use-existing`. `gpdiff.pl`/`atmsort.pl` normalize output at diff time
(trailing whitespace, separator widths, unordered result-set ordering, and
`init_file` ignore-patterns); the expected files store raw output.

## Single regress test

```bash
sudo docker exec -e USER=gpadmin -u gpadmin gpdb_clean bash -lc '
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
  and must work under both optimizers.
- ADB primarily uses ORCA (optimizer=on); optimizer=off is the secondary path and
  its base `.out` files were under-validated by the merge campaign.

## isolation2 tests (concurrent / fault-injection)

```bash
cd src/test/isolation2 && make -j8 && make install   # pg_isolation2_regress is NOT built by the backend make
./pg_isolation2_regress --init-file=../regress/init_file --init-file=./init_file_isolation2 \
   --bindir=$GPHOME/bin --inputdir=. --use-existing --dbname=isolation2test <spec>
```

Gotcha: most isolation2 specs do **not** `CREATE EXTENSION gp_inject_fault`
themselves — an early schedule test (gpdispatch) does it and it persists. Running
one spec standalone fails with `gp_inject_fault_infinite(...) does not exist`; fix
with `psql -d isolation2test -c "CREATE EXTENSION gp_inject_fault"` first.

## Setup dependencies (upstream/core tests)

Upstream tests (brin, gin, domain, join, subselect, create_index, ...) are NOT
in `greenplum_schedule`; they live in `parallel_schedule`/`serial_schedule` and
need a long setup chain. To run one standalone you need its prerequisites:
`create_type`/`create_function_1` create the `city_budget` type; `create_table`
creates point_tbl/fast_emp4000/int8_tbl; `copy` populates tenk1; `create_misc`
makes onek2. The reliable way is to run the schedule prefix up to the target:
`awk '/^test:/{sub(/^test: */,"");printf "%s ",$0} /<test>/{exit}' parallel_schedule`.

## Fresh db vs the polluted `regression` db

Re-running a test with `--use-existing --dbname=regression` after prior runs hits
`relation "..." already exists` because each test's CREATEs persist (tests aren't
self-cleaning). For a clean run, `createdb <fresh>` and use `--dbname=<fresh>`
(roles are cluster-global — `drop role if exists` first). A passing test writes
no `regression.diffs`; a failing one writes it (empty content after gpdiff = pass).

## False-pass traps

- gpdiff exit status: `pg_regress` reports ok only if gpdiff returns clean; check
  `regression.diffs` is empty, not just the test line.
- `docker exec` without `-e USER=gpadmin` breaks gp utilities silently.
- zombie/leftover psql sessions can hold locks and make later queries "hang"; a
  prior fault-injection left active aborts the next statement.

See also: [greengage-answer-file-regen], [greengage-cluster-ops], [greengage-debug].
