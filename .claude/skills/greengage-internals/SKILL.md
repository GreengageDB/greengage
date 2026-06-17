---
name: greengage-internals
description: Architectural knowledge for making CORRECT changes in the GGDB (MPP) fork of PostgreSQL - the merge re-graft methodology, the MPP planner/executor (motion, locus, distribution keys, multi-stage and multi-DQA aggregation), matview refresh, and recurring PG14-merge bug classes. Use when writing or reviewing a backend fix, especially planner/executor/catalog changes after an upstream merge.
---

# GGDB internals for correct fixes

GGDB/ADB is an MPP fork: data is hash/random-distributed across segments; the
planner inserts **Motion** nodes to move tuples; **ORCA** (optimizer=on) and the
**Postgres planner** (optimizer=off) are two separate planning paths. ADB
primarily uses ORCA, so optimizer=off bugs are real but secondary.

## Merge re-graft methodology (the #1 source of merge bugs)

When resolving a merge conflict in a function upstream rewrote: **adopt the
upstream API shape first, then re-graft the GGDB-specific logic into the new
shape** — never blindly take `ours`/`theirs`. The classic regression is taking
upstream's line verbatim and dropping the GGDB graft. Tell-tales of an incomplete
graft: a **declared-but-unused variable** (the GGDB graft computed it, but the
re-grafted line still uses the upstream variable), e.g. matview's `newattr` was
computed but the leftop reverted to upstream's `attr`. Always `git blame` a
suspicious line — if it blames to an upstream commit inside GGDB-specific code,
the graft is probably wrong. Reference resolutions:
1e11aaff762, f2b03841, 1fa092913d2, 3e9744465db, ed7a5095716ee, 4dbcb3f844ec,
a91e2fa94180, 55a1954da16, 80831bcdbe, eb57bd9c1.

## PG14 Aggref aggno/aggtransno — a whole bug class

PG14 (dfd85ea03f6) added `aggno`/`aggtransno` to `Aggref`, stamped early in
`preprocess_aggrefs()` (prepagg.c) and compared by `_equalAggref()`. Any code
that compares or copies Aggrefs across planning stages can now mismatch on these
physical-slot numbers. Manifestations seen: ORCA translator omitted them (all
aggregates returned the first agg's value); `extract_nodes_expression`'s assert;
`cdbpullup_findEclassInTargetList` failed to match an aggregate distribution key
(`could not find hash distribution key expressions in target list`) because the
distkey copy was un-numbered. Fix pattern: when matching for *semantic* identity,
normalize/ignore aggno/aggtransno (and remember `aggsplit` differs across stages).

## MPP planner (optimizer=off path)

- **Locus & distribution keys** (`cdbpathlocus.c`, `cdbpullup.c`): a path's locus
  (Hashed/SingleQE/Replicated/Strewn/Entry...) drives motions.
  `cdbpathlocus_get_distkey_exprs` -> `cdbpullup_findEclassInTargetList` matches
  each distribution-key equivalence member against the subplan targetlist.
- **Motions** (`createplan.c create_motion_plan`, `make_motion`): `make_motion`
  asserts `!IsA(lefttree, Motion)` — a Motion may not sit directly on a Motion.
  When a path that already plans to a Motion is redistributed again (e.g. CTAS
  `DISTRIBUTED BY (<aggregate>)` redistributes a single-row gathered aggregate),
  interpose a pass-through `Result` so the two motions occupy adjacent slices.
- **MIN/MAX optimization** (`planagg.c`): clones the PlannerInfo and asserts
  `eq_classes == NIL`; a `DISTRIBUTED BY` requirement populates eq_classes early,
  so guard the optimization off in that case (it's only an optimization).
- **Multi-DQA / TupleSplit** (`cdbgroupingpaths.c`): multiple DISTINCT aggregates
  are planned via a `TupleSplit` node that emits one tuple per DQA tagged with an
  `AggExprId`; a DQA's FILTER is enforced in TupleSplit (`DQAExpr.agg_filter`),
  not in the Agg stages. PG14's `make_partial_grouping_target()` makes SEPARATE
  flat-copies of the Aggrefs, so adjustments made to the cost-list Aggrefs
  (agg_expr_id stamping, aggfilter stripping) must be re-applied to the partial
  AND final plan targets (use private copies — `copy_pathtarget` is shallow).

## Executor

- `nodeMotion.c` — interconnect; an executor bug that never emits EOS or loops
  forever shows up as an uninterruptible motion-IPC hang.
- `nodeTupleSplit.c` — DQA splitting; applies `agg_filter_array` per DQA. Watch
  for state not reset on every branch (a NULL-filter DQA must reset `filter_out`,
  else it spins).

## Materialized view refresh (`matview.c refresh_by_match_merge`)

Builds a "diff" via SQL over a transient "newdata" heap. GGDB created that heap
with a `_$` column prefix (c654c503faf) to avoid colliding with the matview's own
column names, and writes whole-row references as `alias.*` (not bare `alias`) so an
alias can't be mistaken for a same-named column. The PG14 merge reintroduced both
hazards (prefixed aliases, dropped `.*`) — restore them. Note the heap-creation
mutates the matview relcache descriptor in place (a relcache-timing heisenbug).

## Test-level expected-behavior markers

`--start_ignore`/`--end_ignore` in `.sql` (and `GP_IGNORE:` in `.out`) mark output
gpdiff must ignore — GGDB uses them to **document accepted limitations** (e.g. a
LATERAL join that yields "could not devise a query plan", "backward scan is not
supported", forward-only cursors). A `-- FAIL with ERROR: ...` comment documents an
expected failure. Don't mistake these for regressions.

See also: [greengage-debug], [greengage-answer-file-regen], [greengage-build].
