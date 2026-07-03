---
name: greengage-internals
description: Architectural knowledge for making CORRECT changes in the GGDB (MPP) fork of PostgreSQL - what differs from vanilla PostgreSQL (QD/QE, dispatch, Motion/slices, DTX, FTS, AO tables), the MPP planner/executor (motion, locus, distribution keys, multi-stage and multi-DQA aggregation), matview refresh, and the recurring GGDB bug classes that upstream merges keep reintroducing (per-aggref aggsplit, Aggref aggno, AO-aux relkinds, QD/QE dispatch desyncs, FTS-outside-transaction, process-local assumptions). Use when writing or reviewing a backend fix, especially planner/executor/catalog/HA changes after an upstream merge, or when a symptom only makes sense on a distributed cluster.
---

# GGDB internals for correct fixes

GGDB/ADB is an MPP fork: data is hash/random-distributed across segments, the
planner inserts **Motion** nodes to move tuples, and **ORCA** (optimizer=on,
primary for ADB) and the **Postgres planner** (optimizer=off) are two separate
planning paths. Full map of GGDB-vs-vanilla differences (components, where they
live, what merges break): [greengage-vs-postgres.md](greengage-vs-postgres.md).

## Merge re-graft tell-tales (#1 source of merge bugs)

Full merge methodology: [greengage-pg-merge](../greengage-pg-merge/SKILL.md).
When reviewing merged code: **adopt the upstream API shape first, then re-graft
the GGDB-specific logic into the new shape** — never blindly `ours`/`theirs`.
The classic regression is taking upstream's line verbatim and dropping the GGDB
graft. Tell-tale of an incomplete graft: a **declared-but-unused variable** —
the graft computed it but the merged line uses the upstream variable (matview:
`newattr` computed, leftop reverted to `attr`). Always `git blame` a suspicious
line — if it blames to an upstream commit inside GGDB code, suspect the graft.
Reference resolutions: 1e11aaff762, f2b03841, 1fa092913d2, 3e9744465db,
ed7a5095716ee, 4dbcb3f844ec, a91e2fa94180, 55a1954da16, 80831bcdbe, eb57bd9c1.

## Recurring GGDB bug classes — audit after every merge

### Per-aggref aggsplit (executor rule for DQA)
GGDB multi-stage/multi-DQA plans put aggregates with DIFFERENT `aggsplit` in ONE
Agg node (regular agg = COMBINE at final stage; DISTINCT agg = SIMPLE from
dedup'd values). `nodeAgg.c` must therefore test `peragg->aggref->aggsplit`,
never node-level `aggstate->aggsplit` (most sites already do — a merge that
reverts even one site breaks mixed `count(x), sum(DISTINCT y) GROUP BY z`).
Symptoms: `ERROR: aggregate N needs to have compatible input type and
transition type`, or silent wrong results (sum=0, 0 rows) under ORCA. Fix
reference: `5bdd9c0a3b4` on ai-bump-1 (greengage_sync remote).

### Aggref aggno/aggtransno
PG14 (dfd85ea03f6) added `aggno`/`aggtransno` to `Aggref`, stamped early in
`preprocess_aggrefs()` and compared by `_equalAggref()`; code comparing/copying
Aggrefs across planning stages can mismatch on these physical-slot numbers.
Seen: ORCA translator omitted them (every aggregate returned the first
agg's value); `cdbpullup_findEclassInTargetList` failed to match an aggregate
distribution key (`could not find hash distribution key expressions in target
list`). Fix pattern: for *semantic* identity, normalize/ignore aggno/
aggtransno; `aggsplit` also legitimately differs across stages.

### AO-aux relkinds vs RELKIND_HAS_* macros
GGDB append-only tables have auxiliary heap relations with relkinds
`RELKIND_AOSEGMENTS 'o'` / `RELKIND_AOBLOCKDIR 'b'` / `RELKIND_AOVISIMAP 'M'`
(src/include/catalog/pg_class.h). They ARE heaps and get vacuumed like heaps,
but every new upstream path keyed on `RELKIND_HAS_TABLE_AM()`/relkind asserts
misses them. PG15 examples: `cluster_rel()`'s relkind assert broke VACUUM FULL;
`RelationSetNewRelfilenode` left aux relfrozenxid=0 after TRUNCATE → later
VACUUM assert. GGDB pattern: keep the upstream macro unchanged, add a separate
AO-aux `if` beside it. Audit every new `RELKIND_HAS_*` use in a merge.

### QD→QE dispatch desyncs
Anything serialized QD→QE must stay in sync end to end:
- **Binary plan serialization** = `src/backend/nodes/outfast.c` (writer) +
  `readfast.c` (reader) — NOT the text outfuncs/readfuncs; updating the text
  pair does not update the wire. Since PG16's generated *funcs.c they are
  STANDALONE, synced by hand (gen_node_support.pl does not emit them). Audit:
  per `_outX` in outfast.c, diff its `WRITE_*FIELD` sequence against the
  reader's `READ_*FIELD`; check `case T_x:` exists in both switches. A GARBAGE
  tag in "could not deserialize unrecognized node type N" = stream slip from a
  missing field; a REAL small tag = missing reader case. Example: PG14 JoinExpr
  `join_using_alias` was added to text out/read but not outfast.c.
- **GUC dispatch lists**: `sync_guc_names_array`/`unsync_guc_names_array` in
  src/backend/utils/misc/guc_gp.c — every new upstream GUC must be classified.
- **libpq-riding grafts**: GGDB multiplexes COPY data and nextval responses
  over dispatch connections; `pq_getmessage(buf, 0)` means "no length limit"
  (a GGDB convention upstream checks break); `PQsendGpQuery_shared`
  (src/backend/cdb/dispatcher/cdbpq.c) must append a libpq command-queue entry
  (PG14+) or every dispatched row-returning query drops its rows. When an MPP
  feature riding libpq breaks post-merge, diff the touched libpq/pqcomm
  function against the pre-merge branch for a lost GGDB-commented guard.

### FTS handler runs OUTSIDE a transaction
The FTS message handler (src/backend/fts/ftsmessagehandler.c) executes on
segments with no transaction: any code it reaches must not do
syscache/catalog/ACL lookups (`Assert(IsTransactionState())` — trips only on
cassert builds). PG15 example: `AlterSystemSetConfigFile` grew a second
per-parameter-ACL `superuser()` check; it needed the same `am_ftshandler`
bypass as the first. Any NEW permission/catalog check upstream adds to a path
FTS uses (e.g. clearing `synchronous_standby_names` during promotion)
re-breaks mirror promotion. Related: mirrors sit in PM_RECOVERY forever
(`hot_standby=off`); postmaster connection acceptance must return
CAC_MIRROR_READY (`GetMirrorReadyFlag()`) BEFORE any newer "not consistent
yet" reject, or FTS can never probe mirrors.

### Upstream "process-local / single-node" assumptions
- **ORCA is not re-entrant**: planning/executing SQL during ORCA metadata
  retrieval (e.g. a partition descriptor built lazily inside ORCA calling an
  SQL-language opclass support function) corrupts ORCA's memory pool → later
  SIGSEGV. Guard: fall back to the Postgres planner up front
  (`query_has_nondefault_partition_opclass()` in optimizer/plan/planner.c).
- **Subplan init across Motions**: `getLocallyExecutableSubplans()`
  (execUtils.c) picks which SubPlans a slice initializes by walking down to
  Motions, but each chosen subplan is ExecInitNode'd IN FULL through its own
  internal Motions — InitPlans referenced below such Motions need the fixpoint
  closure (`SubPlanNestedFinderWalker`), else "subplan was not initialized".
- **AlternativeSubPlan is not dispatched**: `make_subplan` (subselect.c) skips
  building the hashed alternative when `Gp_role == GP_ROLE_DISPATCH`.
- **Correlated targetlist subquery filters** ride
  `create_projection_path_with_quals` / `cdb_restrict_clauses` above a Broadcast
  Motion; path-collapsing shortcuts dropping those quals silently lose the
  correlation (every row gets the uncorrelated result).
- **pg_basebackup/pg_rewind grafts**: segments have identity (`gp_dbid` in
  `internal.auto.conf`) and GGDB options (`--target-gp-dbid`, `--slot`,
  `--force-overwrite`, `-E`); every upstream rewrite of these tools drops some
  graft. Details in [greengage-vs-postgres.md](greengage-vs-postgres.md).

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

Builds a "diff" via SQL over a transient "newdata" heap. GGDB creates that heap
with a `_$` column-name prefix (c654c503faf) to avoid colliding with the
matview's own columns, and writes whole-row references as `alias.*` (not bare
`alias`) so an alias can't be mistaken for a same-named column. The PG14 merge
reintroduced both hazards — restore them. Note the heap-creation mutates the
matview relcache descriptor in place (a relcache-timing heisenbug).

## Test-level expected-behavior markers

`--start_ignore`/`--end_ignore` in `.sql` (and `GP_IGNORE:` in `.out`) mark
output gpdiff must ignore — GGDB uses them to **document accepted limitations**
(e.g. "could not devise a query plan", "backward scan is not supported"); a
`-- FAIL with ERROR: ...` comment documents an expected failure. Don't mistake
these for regressions.

See also: [greengage-pg-merge](../greengage-pg-merge/SKILL.md), [greengage-debug](../greengage-debug/SKILL.md), [greengage-regress-tests](../greengage-regress-tests/SKILL.md), [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md), [greengage-build](../greengage-build/SKILL.md).
