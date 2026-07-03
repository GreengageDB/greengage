---
name: greengage-ci-triage
description: Triage red CI jobs on a GGDB branch end-to-end - understand the shared-answer-file job matrix (JIT/non-JIT × ORCA/planner, assert build, resgroup, behave, unittest-check), fetch regression.diffs/results artifacts from GitHub Actions, classify each failing test as cosmetic drift vs real bug vs flaky, and reproduce locally without being fooled by environment differences. Use when a CI run reports failing tests, when deciding whether a failure needs a code fix or an answer-file regen, or when a "fix" for one job broke another.
---

# Triaging CI failures

## The job matrix — and what shares what

| Job | Build | Runs | Compares against |
|---|---|---|---|
| Regression tests with ORCA | cassert + ORCA | `installcheck`, `PGOPTIONS='-c optimizer=on'` | `<t>_optimizer.out`, else base `<t>.out` |
| Regression tests with Postgres | cassert | same, `optimizer=off` | base `<t>.out` |
| JIT tests with ORCA / with Postgres | + `--with-llvm` | same + `-c jit=on -c jit_above_cost=0 -c gp_explain_jit=off` (ORCA twin also `-c optimizer_jit_above_cost=0`) | same files as the non-JIT twins |
| unittest-check | (inside the compile job, `concourse/scripts/compile_gpdb.bash`) | cmockery mock tests | — |
| resgroup isolation | privileged container, cgroups | `installcheck-resgroup` | resgroup expected files |
| behave | docker-compose cluster | `gpMgmt/test/behave/mgmt_utils` features | step assertions |
| ORCA unit / linter | — | `unit_tests_gporca.bash` / lint image | — |

Suite commands and container invocations are inventoried in `arenadata/readme.md` and
[greengage-regress-tests](../greengage-regress-tests/SKILL.md); the entry point for the
regress jobs is `concourse/scripts/ic_gpdb.bash` (`MAKE_TEST_COMMAND` carries `PGOPTIONS`).

Hard-won matrix facts:
- **All four regress jobs compare the SAME `expected/*.out` tree.** A regen that greens one
  job can break another. Before touching an expected file, ask "which jobs read this file"
  — see the shared-base and JIT rules in
  [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md).
- **JIT-specific failures essentially don't exist** (verified across two majors by running
  the matrix with and without jit): every JIT-job failure also reproduces without jit.
  Triage a red JIT job as an ordinary regression failure first. The one real JIT-only class
  is column-width perturbation of `explain_filter` output by the wide EXPLAIN `Settings:`
  row (fix in the regen skill), not a jit codegen bug.
- **The assert∩ORCA intersection is the rarest coverage.** Historically assert builds ran
  `--disable-orca` and ORCA jobs ran without asserts, so "JIT tests with ORCA"
  (cassert + ORCA) finds real crashes invisible everywhere else — e.g. an over-strict
  `Assert(node)` in `extract_nodes_expression()` crashing every bare `count(*)` under ORCA
  (fixed as `fa6503474c3` on claude-merge-2). Take its failures seriously.
- One early crash **aborts the schedule and masks everything behind it**; after fixing a
  crash expect a new, longer failure list on the next run.

## Fetch the evidence

Job list is readable unauthenticated; logs and artifacts need a token in the `GH_TOKEN`
env var. Use `gh run download`/`gh api`, or raw curl:

```bash
curl -L -H "Authorization: Bearer $GH_TOKEN" \
  https://api.github.com/repos/<org>/<repo>/actions/artifacts/<id>/zip -o art.zip
unzip art.zip   # contains nested *_regression.diffs.tar and *_results.tar
```

- `*_regression.diffs.tar` → the gpdiff-filtered `regression.diffs` = what actually failed.
- `*_results.tar` → `.../results/<t>.out` = the **authoritative regen source** (never regen
  from a local run for environment-sensitive tests).
- For crashes/non-regress failures read the job log plus the segment/QD csv logs inside the
  artifact: `server process was terminated by signal 6 ... Failed process was running: <SQL>`
  → grep the QD/QE log for that PID → `TRAP: FailedAssertion(...)`. See
  [greengage-debug](../greengage-debug/SKILL.md) for log-reading.
- Always work from the **real regression.diffs**, never from a guess about the failure. A
  "flaky-looking" resgroup failure turned out to be a deterministic `CREATE VIEW` syntax
  error (PG14 removed the postfix `!` operator) whose cascade mimicked flakiness.

## Classify: split, categorize, adversarially verify

Split `regression.diffs` into per-test chunks (`csplit` on the `^diff ` headers) and give
each a verdict: **COSMETIC / REAL / UNSURE**. A diff is cosmetic only if EVERY +/- hunk is:

| Cosmetic class | Example |
|---|---|
| Plan-shape drift (EXPLAIN text only) | IndexScan↔IndexOnlyScan, Agg↔TupleSplit, Motion/slice renumber |
| Error-message/prefix drift | libpq `could not connect to host "...": ` prefix; error→error with only a `(file.c:NNN)` line-number change |
| Catalog/OID drift | opclass OIDs shifted by new upstream entries |
| Operator/function renames upstream | point `<^`→`<<|`; `(x)!`→`factorial(x)` |
| Documented GGDB limitations | "CREATE INDEX CONCURRENTLY is not supported" + cascade; errors inside `--start_ignore` blocks |
| Row order without ORDER BY | MPP segment-order nondeterminism |
| Sampling/stats drift | ANALYZE MCV vs histogram |
| New upstream test content | added queries/rows in the upstream .sql |

REAL-bug signatures — never regen these away:
- a query **RESULT value** is wrong (sum=0, count off), or result rows missing/extra;
- an unexpected `+ERROR`/`PANIC`/`FailedAssertion`/"server closed the connection"/"cache
  lookup failed"/"variable not found in subplan";
- success→error: a committed `(N rows)` replaced by `+ERROR` (the safety gate in
  [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md)).

Then **adversarially re-verify every REAL/UNSURE verdict** (try to refute it against the
cosmetic table) before writing code, and spot-check a sample of COSMETICs. This two-pass
method triaged 48 failing JIT tests into 1 real executor bug (mixed DISTINCT+regular
aggregate, per-aggref `aggsplit`; `5bdd9c0a3b4` on ai-bump-1) + 44 cosmetic.

Keep a fourth bucket: **HELD**. A diff can be "correct output, lost coverage" — e.g. an
ORCA plan that stopped using Dynamic Partition Elimination still returns right rows, but
regenerating the expected would silently delete the DPE test. Hold those for a real
investigation instead of regen.

## Local repro — and why local ≠ CI

Match the job exactly: same `PGOPTIONS` (optimizer + jit GUCs) against the gpdemo cluster
in the campaign's docker container (each merge campaign pairs one `<container>` with one
git worktree — see [greengage-build](../greengage-build/SKILL.md)). Single-test recipes,
`--use-existing` pollution traps, and isolation2 flags:
[greengage-regress-tests](../greengage-regress-tests/SKILL.md).

Environment differences that make output differ even when behavior is identical:
container IP/hostname and socket paths baked into connection errors; the database name
folded into `current_database()` literals; `1 segment` vs the normalized `n segments`;
segment ports; `.source`-generated tests embedding `@abs_builddir@`. Consequences:
- Some CI failures **cannot be verified locally** (e.g. tests asserting CI's segment
  address) — regen those only from the CI results tarball.
- **Prefer an `init_file` matchsubs mask over baking any environment value** into an
  expected file (worked example: masking the libpq connect prefix fixed three tests at
  once, permanently, instead of regenerating them with CI addresses).

## Flaky discipline

- A failure that **rotates between runs** (EXPLAIN ANALYZE actual-row counts, point
  index-scan row order, `explain` header width, AO segfile-stat counts, HA
  promotion-timing isolation2 tests) is flaky: confirm by rerun/local repro, do NOT regen
  (you'd bake one run's output), track separately. A steady residue of 1-2 such tests in a
  ~200-test schedule is a known-accepted state — verify it's the same accepted set.
- **unittest-check stops at the FIRST failing binary**, so a green-after-one-fix CI can
  still hide more: always run the full `make -s unittest-check` locally.
- A CI failure can be a **stale-binary artifact** of the CI build cache or of your local
  container lagging the branch — a diff matching an already-reverted commit's signature is
  the tell. Cross-check commit history before triaging it as new.
- Wedged-cluster cascades ("all segments are not currently running", segment down from a
  prior scenario) fail whole behave/isolation2 batches from one root cause — recover the
  cluster first ([greengage-cluster-ops](../greengage-cluster-ops/SKILL.md)), then rerun.

## Behave / mgmt-utility job repro

Single-feature local recipe, the full-suite docker-compose runner, and the two
gotchas that mask real errors (`-e USER=gpadmin` mandatory; `check_return_code`'s
`AttributeError` hiding the failed substep) are in the behave section of
[suites.md](../greengage-regress-tests/suites.md). Triage note: read the substep
output ABOVE a behave traceback, not the traceback itself, and remember behave
CI scenarios rebuild a 4-host cluster — most cannot run against a local gpdemo.
