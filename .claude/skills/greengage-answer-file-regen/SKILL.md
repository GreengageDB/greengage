---
name: greengage-answer-file-regen
description: Regenerate regression answer files (expected/*.out, output/*.source) for cosmetic post-merge drift WITHOUT masking a real bug, or better, avoid the regen with an init_file/atmsort mask. Use when a failing test's diff is deparse/plan-shape/psql-format/error-message drift rather than a behavior change, when output differs between CI and local runs (baked addresses, db names), or when deciding between base .out and _optimizer.out. Includes the success->error safety gate, the CI-tarball source-of-truth rule, matchignore/matchsubs mechanics, .source-file handling, and verification.
---

# Regenerating answer files safely

After a PG major-version merge, many regression `.out` files drift cosmetically
(psql columns, deparse `AT TIME ZONE` → `timezone()`, error text, MPP plan
shapes/slice numbers, new upstream queries). These are regens, not bugs — but a
real bug can hide in the same pile (a query that returned data now ERRORs).
Never bulk-copy blindly.

**Decision order: (1) mask environment-varying text in `init_file`,
(2) fix the test (missing ORDER BY, leaked GUC), (3) regen the expected file.**

## The SAFETY GATE: success->error scan (the most important step)

The signature of a real bug hiding in a "cosmetic" pass is a removed result row /
`(N rows)` replaced by an added `+ERROR`. Scan for it **on the gpdiff
`regression.diffs`, NOT a raw `diff`/`git diff`**:

- `regression.diffs` already honors `--start_ignore/--end_ignore`, atmsort
  row-sorting, and init_file masks, so its success->error hits are real. Raw
  diffs over-report wildly (observed: 5326 raw lines → 16 real) and
  FALSE-POSITIVE on ignore-block content and reorder context.
- **A clean re-run that reports `ok` is the definitive proof** there is no
  non-ignored success->error.
- A `-ERROR` line in the diff may be a MOVE, not a removal — `grep -c` it on
  both sides before treating it as a regression.
- **"No +ERROR" is NOT sufficient**: a changed DATA-ROW set with no error is
  also a real bug (a doubled shared table produced exactly this). Per-test
  inspection, never blind bulk `cp`.
- Bulk triage of many `_optimizer.out` drifts: per test, compare the ERROR-line
  COUNT in `results/<t>.out` vs the known-green expected, after stripping
  `(segN sliceN ip:port pid=N)` suffixes and digits (pid-differing duplicate
  errors otherwise look "new"). delta=0 → plan-shape drift, safe; delta>0 →
  investigate.
- **Never regen from a run that failed under memory pressure or a crashed
  cluster** — it bakes `could not fork` / cascade errors into the expected.

Real bugs this gate caught (fixed, not regenned): multi-DQA+FILTER
`variable not found in subplan target list`; matview `DISTRIBUTED BY` aggregate;
mixed DISTINCT+regular aggregate wrong results (per-aggref aggsplit); doubled
tenk2/point tables from test-reorg leftovers.

## Prefer an init_file mask over baking environment text

`src/test/regress/init_file` is passed to gpdiff (`--gpd_init`, see
pg_regress.c) and applies to EVERY test; individual tests can embed their own
`-- start_matchsubs` blocks in the `.sql`. Two block types:

- **matchignore** (`-- start_matchignore`): bare `m/.../` patterns; a matching
  line is dropped from BOTH result and expected. E.g. `m/^ Settings:.*/`
  already ignores the EXPLAIN `Settings:` line — including jit GUCs, so do NOT
  chase jit-on vs jit-off `Settings:` diffs.
- **matchsubs** (`-- start_matchsubs`): PAIRED `m/.../` + `s/.../.../` rules;
  where the match hits, the substitution is applied to both sides.

**Worked example** — libpq (PG14+) prepends the server address to connection
errors (`connection to server at "<host>" (<addr>), port N failed: ...`);
regenerating would bake environment-specific addresses. Instead init_file
strips the prefix once (paired m// + s/// for the host and socket variants) —
tests then pass against the ORIGINAL expected files in every environment.

Per-test matchsub example: a plan that folds `now()::date` into
`Hash Key: '<date>'::date` rolls over daily — `sql/direct_dispatch.sql` masks
it with an embedded matchsub; the echoed block must appear in BOTH `<t>.out`
and `<t>_optimizer.out`.

atmsort built-ins you get for free: unordered-SELECT row sorting; stripping of
`(segN ... pid=N)` message suffixes (a default matchsub in atmsort.pm);
normalization of the EXPLAIN ANALYZE `Slice statistics:`/`Executor memory:`
trailer. Costed text EXPLAIN is canonicalized into a heavily-pruned plan tree
(explain.pm), so cost/row drift is a non-issue and plan-SHAPE drift means
editing the node-type WORDS in the expected file. `EXPLAIN (COSTS OFF)` output
is NOT processed — it compares verbatim and matchsubs apply to it normally
(that is how the direct_dispatch mask above works). atmsort does NOT normalize
psql column-header width/trailing spaces (beware editors that strip trailing
whitespace).

## Source of truth: the CI result tarball, not a local run

The CI matrix runs the SAME `expected/*.out` across four jobs — {JIT, non-JIT}
× {ORCA `optimizer=on`, planner `optimizer=off`} — so a regen that greens one
job can break another (fetching artifacts:
[greengage-ci-triage](../greengage-ci-triage/SKILL.md)). Hard rules:

- **Regenerate from the failing job's CI result tarball** (`*_results.tar` →
  `.../results/<t>.out`), NOT a local gpdemo run — *unless* the output is fully
  normalized (e.g. `explain`'s `explain_filter`). A local run bakes a fresh db
  name into `current_database()` literals, `1 segment` vs `n segments`, local
  addresses, local row order. Correctness check for a big reorder diff: the
  sorted DATA-ROW SET must equal a known-good file's set (0 set diff).
- **A shared base `<t>.out` (no `<t>_optimizer.out`) is compared by ORCA jobs
  too** (ORCA falls back to base). Regenerating it to planner output BREAKS the
  ORCA jobs. If both optimizers need different output, SPLIT: `cp` the current
  ORCA-passing base to `<t>_optimizer.out`, then regen the base from the
  optimizer=off result. (Exception: a `--disable-orca` bring-up phase has only
  one path, so base regens are safe until ORCA is built.)
- **`_optimizer.out` coverage is self-balancing — do NOT "backfill" it.** A
  passing shared-base test needs none; a genuinely-diverging test is already
  red. Real ORCA gaps are untested features, not missing files.
- **Never bake JIT-only output into a file a non-JIT job compares.** (In
  practice the jit `Settings:` line is matchignored and the Executor-memory
  trailer is atmsort-normalized; the one real jit artifact was a widened
  `explain_filter` column, fixed by pinning `jit`, `jit_above_cost` and
  `optimizer_jit_above_cost` to boot defaults in `sql/explain.sql`.)

## `.source`-based tests

Tests listed under `input/`/`output/` as `<t>.source` have their `sql/<t>.sql`
and `expected/<t>.out` GENERATED by pg_regress `convert_sourcefiles()`
(substituting `@abs_builddir@` etc.); the generated files are git-ignored (see
`expected/.gitignore`). To regen one, edit/copy into `output/<t>.source`
(gpsourcify.pl reverse-substitutes a results file back to tokens); never commit
a generated `.out`. These tests carry absolute paths/addresses — the least
locally-reproducible class; prefer init_file masks or CI results.

## Procedure

1. Prefer the CI result tarball (above). For a local regen run the test under
   the right optimizer in a FRESH db with full setup (see
   [greengage-regress-tests](../greengage-regress-tests/SKILL.md)); a polluted
   `regression` db gives `already exists` cascades.
2. Scan `regression.diffs` for success->error; investigate every hit (inside a
   `--start_ignore` block? error->error with only a line number change?
   documented by a `-- FAIL` comment in the `.sql`?).
3. For confirmed-cosmetic tests, `cp results/<t>.out expected/<t>.out`.
4. Strip gpdiff-ignored noise the raw copy keeps:
   `sed -i -E "/(NOTICE|HINT):.*DISTRIBUTED BY.*clause/d; /^Distributed by: \(/d"`.
5. **Verify**: re-run on a fresh db → `ok` / empty `regression.diffs`, under
   BOTH optimizers if the file is shared.
6. `docker cp` lands files root-owned on the host — `chown` before editing.

## What NOT to regen

- **Flaky output.** Confirm determinism by running twice and diffing the two
  `results/<t>.out`: the intersection of failures is regen-candidate, the
  rotating remainder is flaky. Known flaky classes: EXPLAIN-ANALYZE per-segment
  `actual rows` and `(never executed)` flutter (harden with an ignore/mask, do
  not re-pin), index-scan row order without ORDER BY, AO segfile stats
  (`truncate_gp`), CTAS-without-DISTRIBUTED-BY under ORCA (random policy →
  per-segment counts flutter; fix the `.sql` with explicit DISTRIBUTED BY /
  ORDER BY, then regen both files).
- **Pressure victims**: gpdemo is small; tests hitting `Out of memory` /
  `failed to acquire resources` / fork failures have corrupt results.
- **Environment-specific text** (replication conninfo, data-dir paths,
  addresses): mask it instead.
- **Coverage-defeating regens**: if the new plan LOSES the feature the test
  exists to prove (e.g. a partition-elimination test whose new plan scans all
  partitions), regenning silently deletes the coverage — hold and investigate.

## Inherent reorder noise

For data-returning tests the git diff is huge because unordered MPP results
come back in segment order; gpdiff sorts them, so the test is clean, but git
shows every moved row. Commit regens as isolated, clearly-labeled commits; the
success->error gate (not the visual diff) is the correctness check.

See also: [greengage-regress-tests](../greengage-regress-tests/SKILL.md),
[greengage-ci-triage](../greengage-ci-triage/SKILL.md),
[greengage-internals](../greengage-internals/SKILL.md).
