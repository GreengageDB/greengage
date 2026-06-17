---
name: greengage-answer-file-regen
description: Regenerate optimizer=off (and other) regression answer files for cosmetic PG14/GGDB drift WITHOUT masking a real bug. Use when a failing test's diff is deparse/plan-shape/psql-format/error-message drift rather than a behavior change. Includes the success->error safety gate, noise stripping, and verification.
---

# Regenerating answer files safely

After a PG major-version merge, many optimizer=off base `.out` files drift
cosmetically: PG14 psql adds a `\d+` Compression column; `AT TIME ZONE`
deparses as `(x AT TIME ZONE z)` not `timezone(z,x)`; grammar/error text changes
(CREATE STATISTICS on expressions, `EXTRACT`); MPP plan shapes and slice numbers
shift; upstream adds new test queries. These are answer-file regens, not bugs —
but a real bug can hide in the same pile (e.g. a query that returned data now
ERRORs). Never bulk-copy blindly.

## The SAFETY GATE: success->error scan (the most important step)

The signature of a real bug hiding in a "cosmetic" pass is a removed result row /
`(N rows)` replaced by an added `+ERROR`. Scan for it **on the gpdiff
`regression.diffs`, NOT the raw `git diff`**:

- `regression.diffs` already honors `--start_ignore/--end_ignore` blocks and
  atmsort row-sorting, so its success->error hits are real.
- A raw `git diff` scan FALSE-POSITIVES: ignore-block content (the .out lines
  aren't always individually `GP_IGNORE:`-prefixed) and row-reordering context
  shifts a `(N rows)` next to an unrelated `+ERROR`. This burned an entire pass —
  join/portals/subselect were flagged but were actually cosmetic (errors inside
  `--start_ignore` blocks documenting accepted GGDB limits + `error->error`
  stale-line drift like `pathnode.c:485 -> :275`).
- **A clean re-run that reports `ok` is the definitive proof** there is no
  non-ignored success->error (it would otherwise FAIL the test).

Real bugs this gate caught (do NOT regen — fix them): gp_dqa multi-DQA+FILTER
`variable not found in subplan target list`; matview `DISTRIBUTED BY` an
aggregate `could not find hash distribution key expressions`; matview REFRESH
CONCURRENTLY `text *= text`.

## Source of truth: the CI result tarball, not a local run

The CI matrix runs the SAME `expected/*.out` across FOUR jobs — {JIT, non-JIT} ×
{ORCA `optimizer=on`, Postgres `optimizer=off`} — so a regen that greens one can
break another. Three rules learned by breaking them:

- **Regenerate from the failing job's CI result tarball** (`*_results.tar.gz` →
  `gpdb_src/src/test/regress/results/<t>.out`), NOT a local gpdemo run — *unless*
  the test's output is fully normalized by `explain_filter`/atmsort. A local run
  bakes environment-specific text that CI then rejects: a fresh db name folded into
  `current_database()` literals (e.g. `'jps2'` vs `'regression'`), `1 segment`
  instead of the normalized `n segments`, local row-ordering. (Fully-normalized
  tests like `explain`, where numbers map to `N`, ARE safe to regen locally.)
  Correctness check for a big reorder diff: the sorted DATA-ROW SET must equal the
  known-good `_optimizer.out` set (e.g. qp_misc_jiras opt=off == ORCA, 0 set diff).
- **A shared base `<t>.out` (no `<t>_optimizer.out`) is used by ORCA too** (ORCA
  falls back to base). Regenerating it to the Postgres-planner output BREAKS the
  ORCA jobs. Only regen a base file if a separate `_optimizer.out` exists.
- **`_optimizer.out` coverage is self-balancing — do NOT "backfill" it.** A test
  without one passes under ORCA only because its *non-ignored* output already
  matches the base (data-only, ORCA-falls-back, or the plans are in `--start_ignore`
  blocks). Any test whose asserted output genuinely diverges under ORCA already has
  an `_optimizer.out` or it would be red. Adding one to a passing shared-base test
  asserts nothing new (verified for delete/insert_conflict/with). Real ORCA gaps are
  *untested features* (find them by exercising the feature under both optimizers, cf.
  the GROUP BY DISTINCT bug 4dd440a4e69), not missing `_optimizer.out` files.
- **Never bake JIT-only output into a file a non-JIT job compares.** Under jit,
  EXPLAIN adds a ` Settings: jit = ...` line (init_file-ignored) and inflated
  `Executor memory:` (atmsort-normalized) — so they don't diff — but the wide
  Settings row still perturbs `explain_filter`'s column width. See "What NOT" below.

## Procedure

1. **Prefer the CI result tarball** (above). Only when regenerating locally is
   appropriate, run the test(s) under `optimizer=off` in a FRESH db with full setup
   (see [greengage-regress-tests]); the polluted `regression` db gives
   `already exists` cascades.
2. Scan `regression.diffs` for success->error. Investigate any hit (is the new
   ERROR inside a `--start_ignore` block? is it `error->error` with only a line
   number change? does a `--FAIL with ERROR` comment in the `.sql` document it?).
3. For confirmed-cosmetic tests, `cp results/<t>.out expected/<t>.out`.
4. **Strip gpdiff-ignored noise** so the diff is real content only — `init_file`
   drops these but the raw copy keeps them:
   ```
   drop lines matching  ^(HINT|NOTICE):\s+.+'DISTRIBUTED BY' clause
                  and    Distributed by: \(
   ```
   (omitting this adds dozens of NOTICE lines of git noise per file).
5. **Verify**: re-run on a fresh db → `ok` / empty `regression.diffs`. If the
   same-db re-run fails only with `<test>_tbl already exists`, that's a re-run
   artifact (drop the table and re-run).
6. `docker cp` lands files root-owned on the host — `chown` before editing.

## What NOT to regen

- **Flaky** tests: `truncate_gp` (AO segfile-stats vary 3 rows vs 0). Regenerating
  captures one run and flakes on the next. (`explain` LOOKS jit-flaky but is NOT —
  the jit `Settings:` line widens the `explain_filter` output column, which atmsort
  doesn't normalize; the real fix is to pin the jit GUCs to their boot defaults at
  the top of explain.sql — `set jit=off; set jit_above_cost=100000; set
  optimizer_jit_above_cost=7500;` — so EXPLAIN(SETTINGS), which reports only
  modified-from-boot-default GUCs, drops them. Then regen both expected files.)
- **OOM victims**: the gpdemo is too small for the full concurrent schedule;
  qp_olap_window/qp_with_clause/etc. hit `Out of memory`/`failed to acquire
  resources` and their results are corrupt — they pass on CI's bigger runner.
- **Environment-specific**: gp_toolkit (live replication conninfo), gp_connections
  (`$COORDINATOR_DATA_DIRECTORY` unset), createdb (fault injection didn't fire).

## Inherent reorder noise

For data-returning tests the git diff is large (thousands of lines) because
unordered MPP results come back in segment order; gpdiff sorts them, so the test
is clean, but git shows every moved row. Commit it as an isolated, clearly-labeled
regen commit; the success->error gate (not the visual diff) is the correctness check.

See also: [greengage-regress-tests], [greengage-internals].
