---
name: greengage-answer-file-regen
description: Regenerate optimizer=off (and other) regression answer files for cosmetic PG14/GPDB drift WITHOUT masking a real bug. Use when a failing test's diff is deparse/plan-shape/psql-format/error-message drift rather than a behavior change. Includes the success->error safety gate, noise stripping, and verification.
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
  `--start_ignore` blocks documenting accepted GPDB limits + `error->error`
  stale-line drift like `pathnode.c:485 -> :275`).
- **A clean re-run that reports `ok` is the definitive proof** there is no
  non-ignored success->error (it would otherwise FAIL the test).

Real bugs this gate caught (do NOT regen — fix them): gp_dqa multi-DQA+FILTER
`variable not found in subplan target list`; matview `DISTRIBUTED BY` an
aggregate `could not find hash distribution key expressions`; matview REFRESH
CONCURRENTLY `text *= text`.

## Procedure

1. Run the test(s) under `optimizer=off` in a FRESH db with full setup (see
   [greengage-regress-tests]). The polluted `regression` db gives
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

- **Flaky** tests: `explain` (memory width the explain_filter regex misses),
  `truncate_gp` (AO segfile-stats vary 3 rows vs 0). Regenerating captures one
  run and flakes on the next.
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
