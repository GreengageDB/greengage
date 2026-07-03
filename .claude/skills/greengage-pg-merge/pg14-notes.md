# PG14 notes — what PG14 changed that bites GGDB

Class reference distilled from two PG14 campaigns: the full PG13→PG14 merge (branch
`claude-merge-2`) and the incremental PG14-dev bump (branch `ai-bump-1` on the
`greengage_sync` remote — ~370 upstream commits to `f315205f3fa`, 528 conflicts).
Settled resolutions to consult with `git show <branch>:<file>`: `claude-merge-2`,
`adb-8.x`, `adb-7.2.0`. Use this as a checklist when resolving or auditing any
PG14-era area. Methodology lives in [SKILL.md](SKILL.md).

## Catalog / genbki tightening

- `genbki.pl` REJECTS `oid_symbol` for pg_proc/pg_type (auto-generates `F_<NAME>` /
  `<TYPE>OID`). Drop the field; add explicit `#define`s only for symbols C code
  references by name.
- `DECLARE_TOAST`/`DECLARE_*INDEX` moved into the per-catalog `pg_*.h` headers.
  The merge tends to take upstream's gutted central `indexing.h` and DROP every
  GGDB catalog index: re-declare each one in its own catalog header as
  `DECLARE_[UNIQUE_]INDEX(name, oid, <Sym>IndexId, on ...)` (~30 declarations across
  the GGDB catalog headers: `pg_extprotocol.h`, `gp_distribution_policy.h`,
  `pg_appendonly.h`, `pg_resqueue.h`, ...). Symptom: `<X>IndexId undeclared`
  build errors in aclchk.c/auth.c. `perl src/include/catalog/duplicate_oids` must be clean.
- Every `#define <Catalog>IndexId` needs a matching `DECLARE_UNIQUE_INDEX` (PG14
  added the pg_range multirange index).
- OID collisions: PG14 grabbed 3000s/4000s/6150–6171 (multirange, GiST sortsupport,
  new functions). Example: multirange took 4198/4199, which GGDB used for the
  `ao_row`/`ao_column` tableam handlers → renumbered to 9449/9450 (pg_am.dat refs by
  name, C uses generated `F_` macros). Pick gaps from `src/include/catalog/unused_oids`.
- BKI is now **single-quoted**: `genbki.pl`, `bootscanner.l`, initdb's
  `escape_quotes_bki()` and `guc-file.l` must ALL agree (a mixed resolution →
  `syntax error ... unexpected character "` at initdb).
- GGDB-only genbki substitutions get dropped (`PGUID` → `$BOOTSTRAP_SUPERUSERID`).
- Catalog header order in `src/backend/catalog/Makefile` matters (a catalog's array
  type exists only after the catalog): `pg_statistic.h` before `pg_statistic_ext*.h`.
- `pg_proc.dat` duplicate keys are Perl **last-wins**: if both the GGDB and PG14
  `proargnames`/`proallargtypes` survive in one entry, one silently vanishes —
  collapse to the single set matching the C function. Edit `.dat` with a script
  (editors mis-detect it as binary).
- PG14 moved ~46 function bodies from `pg_proc.dat` into
  `src/backend/catalog/system_functions.sql` (placeholder prosrc
  `'see system_functions.sql'`). initdb must run that file AND the catalog Makefile
  must install it — the merge dropped both once, yielding 439×
  `syntax error at or near "see"` (every `\d+` calls col_description). Conversely,
  a relocated function that ends up defined in TWO places (pg_proc.dat /
  system_functions.sql / system_views.sql — a relocation resolved on both sides)
  fails initdb with "already exists": give each function exactly one home.

## Big structural re-grafts

- **PGXACT eliminated** → dense arrays in `PROC_HDR`: `ProcGlobal->xids[proc->pgxactoff]`,
  `->subxidStates[]`, `->statusFlags[]`; `MyPgXact` → `MyProc->pgxactoff`. Re-graft the
  GGDB distributed-snapshot code (procarray.c) and reader-writer XID sharing
  (`IsCurrentTransactionIdForReader`, slot `writer_xact` → `writer_proc`) onto them.
- **`relkind` → `objtype`** (`ObjectType` enum) in CreateTableAsStmt /
  RefreshMatViewStmt / IntoClause — mechanical, but char → enum.
- **copy.c split** (copyfrom/copyto, `CopyState` → `Copy{From,To}State`): GGDB KEEPS
  the monolithic `copy.c` + unified `CopyStateData` (heavily extended for external
  tables) — this decision was made independently on both claude-merge-2 and
  ai-bump-1. `copy.c` holds the LIVE `CopyFrom`; where upstream's split files exist
  in-tree their duplicates are `#if 0`'d. Callers (contrib/file_fdw, tablesync.c)
  must use the monolith's 8-arg `BeginCopyFrom` and `CopyState`. Re-graft only the
  protocol change (drop pre-3.0 branches). TRAP: a single leftover
  `CopyFromState cstate;` declaration in DoCopy read fields past the end of the
  smaller `CopyStateData` → nondeterministic segment-initdb crashes at high
  max_connections, and the demo cluster silently came up in utility mode — verify
  `SHOW gp_role` = `dispatch` before trusting any regress run.
- **Grammar**: PG14's `bare_label_keyword` + `BareColLabel` supersede GGDB's
  `ColLabelNoAs` — REMOVE the old `a_expr ColLabelNoAs` target_el rule (keeping both
  = hundreds of reduce/reduce conflicts); add GGDB keywords to `bare_label_keyword`
  (re-sorted), but clause-introducers (PARTITION/DISTRIBUTED/SCATTER) must stay
  `AS_LABEL` or you get ~7000 conflicts. Always verify with bison, not marker absence.
- Others: long-lived `WaitEventSet` for `WaitLatch` (thread `InitializeLatchWaitSet`
  into GGDB startup); `InsertPgAttributeTuple` → plural bulk insert + new
  `attcompression`; toast tables get no pg_type (`InvalidOid`);
  `RecentGlobalXmin`/`GetFullRecentGlobalXmin` removed → `GlobalVis*` horizon API.
- **ReindexStmt `options` (int) → `params` (List)**: either adopt full
  `ReindexParams` or thread an `int options` through ReindexIndex/ReindexTable —
  but audit every caller: GGDB's bitmap AM `bmbulkdelete` passed literal `0` where
  `reindex_index` now derefs a `ReindexParams *` → NULL-deref; pass
  `&(ReindexParams){0}`.
- Parenthesized `REINDEX (CONCURRENTLY) TABLE` carries CONCURRENTLY in the options
  list, BYPASSING gram.y's token-form rejection → proceeds on the QD →
  "could not serialize current snapshot" + QD/QE index-OID divergence. Downgrade
  with a NOTICE in ReindexTable under `GP_ROLE_DISPATCH` (`b5adfd3e9d4` on
  ai-bump-1). REINDEX CONCURRENTLY on distributed non-partitioned tables is a
  multi-layer gap (upstream's swap intentionally changes the index OID; QEs reindex
  non-concurrently and keep the old OID → corrupt catalog) — do NOT partially fix.

## Lazy row identity + the UPDATE/DELETE rework (upstream 86dc90056df)

The single biggest PG14 cluster: PG14 added `add_row_identity_columns`/ROWID_VAR,
removed `inheritance_planner`, and switched UPDATE to a partial SET-only tlist +
`updateColnos` + `ExecBuildUpdateProjection`. GGDB logic was dropped at 5 layers:

| Layer | Symptom | Fix |
|---|---|---|
| `add_row_identity_columns` (optimizer/util/appendinfo.c) emits only `ctid` | `could not find gp_segment_id in subplan's targetlist` | also emit the `gp_segment_id` junk Var (INT4OID, attno -7); rides ROWID_VAR sharing so partition leaves get it |
| grouping_planner builds per-leaf resultRelations but a 1-element is_split_updates | `ModifyTable node is missing is-split-update information` | build `is_split_updates` in lockstep with resultRelations in ALL branches (the length Assert is compiled out in non-cassert builds) |
| Split update (distribution-key UPDATE) vs the partial tlist | `targetColnos does not match subplan target list`; partition key silently NULL | `expand_targetlist` FIRST, then extract update_colnos from the expanded tlist; CMD_UPDATE fills missing cols with old-value Vars not NULL Consts; look up `DMLAction`/`gp_segment_id` junk attnos in ExecInitModifyTable; dispatch DML_INSERT/DML_DELETE to the split handlers |
| AO/AOCS UPDATE (can't fetch-by-TID) | `feature not supported on appendoptimized relations`; later an assert on an empty oldSlot | expand the tlist fully for AO rels too; skip the old-tuple fetch, using `ExecStoreAllNullTuple(oldSlot)` (a cleared slot trips `ExecGetUpdateNewTuple`'s assert) |
| SplitUpdate vs scrambled partition-leaf column order | cdbhash `pg_detoast_datum` SIGSEGV; `table row type and query-specified row type do not match` | `create_splitupdate_plan` must use the NOMINAL target relation (parse->resultRelation), not `path->resultRelation` (a leaf); build the insert projection against `rootResultRelInfo` + set up tuple routing for split updates |

Related: add `ROWID_VAR` to GGDB's `set_plan_references_input_asserts` legal-varno
list; ORCA must populate `ModifyTable.updateColnosLists` (next section).

## Aggregation: aggno/aggtransno, per-aggref aggsplit, AggClauseCosts

- PG14 `preprocess_aggrefs` assigns `Aggref.aggno`/`aggtransno`; the executor reads
  results from `aggvalues[aggref->aggno]` / `pertrans[aggref->aggtransno]`.
  **ORCA plans never pass through preprocess_aggrefs**, so the DXL→Plan translator
  (`src/backend/gpopt/translate/CTranslatorDXLToPlStmt.cpp`) must densely renumber
  each Agg node's Aggrefs 0..N-1 over tlist+qual (dense — `finalize_aggregates`
  walks 0..max, gaps crash); nodeAgg's GGDB `agg_renumber_walker` compacts sparse
  subsets in multi-stage/DQA/grouping-set chains; and GGDB's SCATTER BY needs its
  own `preprocess_aggrefs(root, scatterClause)` call in planner.c. Symptoms: every
  multi-aggregate ORCA query returns the FIRST aggregate's value for all (silent
  wrong results), or a `count(*)` assert in walkers.c `extract_nodes_expression`
  (drop the over-strict `Assert(node)` — `plan->qual` is legitimately NIL).
  Reference fixes: `dfd85ea03f6` (claude-merge-2), `6a7b52b15c7` + `ca1b5c21608`
  (ai-bump-1).
- **Per-aggref aggsplit rule**: GGDB DQA/multi-stage plans put aggregates with
  DIFFERENT aggsplit modes in ONE Agg node (regular agg = COMBINE at final stage,
  DISTINCT agg = SIMPLE over deduplicated values). Every `DO_AGGSPLIT_*` test in
  nodeAgg.c must use `peragg->aggref->aggsplit` / `aggref->aggsplit`, NEVER the
  node-level `aggstate->aggsplit`. Symptom: mixed DISTINCT+regular aggregate under
  ORCA → `aggregate N needs to have compatible input type and transition type` or a
  silent 0/empty result. Fixed as `5bdd9c0a3b4` on ai-bump-1 (2 sites); audit with
  `rg 'aggstate->aggsplit' src/backend/executor/nodeAgg.c` after any nodeAgg merge.
- `get_agg_clause_costs` moved to prepagg.c with signature
  `(root, aggsplit, costs)` — no Node arg; it walks `root->agginfos` (ALL aggs).
  Do NOT keep two calls filling one costs struct (double-counts every aggregate).
  Re-populate the GGDB fields upstream never had, all read by the MPP planner
  (planner.c / cdbgroupingpaths.c):
  `hasNonCombine`/`hasNonSerial` (else 2-stage plans for `string_agg`/`json_agg` →
  `cache lookup failed for function 0`), `distinctAggrefs` (else DQA falls through
  to generic 2-stage partial agg → segment SIGSEGV), and
  `numPureOrderedAggs`/`numOrderedAggs` (else ordered aggs are wrongly multi-staged).
- prepagg's COMBINE branch calls `add_function_cost(combinefn_oid)` unguarded;
  GGDB's multi-stage planner speculatively costs 2-stage for combinefn-less aggs
  (upstream never hits this) → guard with `OidIsValid`.

## ORCA translator gaps — new upstream fields ORCA silently omits

Reusable diagnostic: a plan-node field that is NIL/0 on the QE but populated by the
Postgres planner → suspect the ORCA translator, which builds plan nodes
independently. Compare `SET debug_print_plan=on; SET client_min_messages=log;`
output under `optimizer=off` vs `on` and grep the field. Audit new **Query-level
flags** too, not just plan-node fields.

| Field / feature | Symptom (optimizer=on only) | Fix |
|---|---|---|
| `ModifyTable.updateColnosLists` | every distributed UPDATE SIGSEGVs the QE (`list_nth(NIL)`) | build update_colnos in `TranslateDXLDml` (`4dba7042fea` on claude-merge-2) |
| `Aggref.aggno`/`aggtransno` | all aggregates return the first one's value | dense renumbering (above) |
| `SubscriptingRef.refrestype` (ArrayRef→SubscriptingRef refactor; `exprType()` reads it) | `cache lookup failed for type 0` on ANY subscript expression | set from the DXL op's `ReturnTypeMDid()` in `CTranslatorDXLToScalar` |
| `Query.groupDistinct` (GROUP BY DISTINCT) | duplicated grouping sets → wrong row counts | ORCA can't dedup → raise unsupported in `CTranslatorQueryToDXL::CheckUnsupportedNodeTypes` (planner fallback) |
| `anymultirange`-returning aggs (`range_agg`) | `type N is not a multirange type` at execution | fallback via `gpdb::IsMultirangeType(aggref->aggtype)` in the same gate |
| `forceTupleRouting` executor consumer removed when adopting the PG14 nodeModifyTable rework (`b04e5596c24` on claude-merge-2; PG14 uses per-leaf resultRelations + tableoid junk, which ORCA plans lack) | partitioned ORCA UPDATE/DELETE → `could not open file "pg_tblspc/0/..."` (all-zero relfilenode = storage-less root) | bounded: raise unsupported in TranslateUpdate/DeleteQueryToDXL → planner fallback; proper per-leaf routing is a separate project |

## QD→QE dispatch desyncs

- **The binary dispatch path is `outfast.c`/`readfast.c`, NOT
  outfuncs.c/readfuncs.c** (`#define COMPILING_BINARY_FUNCS` + `#include` trick;
  nodes wrapped `#ifndef COMPILING_BINARY_FUNCS` have a separate binary twin inside
  out/readfast.c). A fix applied only to readfuncs.c silently does nothing for
  dispatch — always locate and fix the twin. Scan: diff each `_outX`'s
  `WRITE_*FIELD` sequence against the reader's `READ_*FIELD` (strip `_AS` aliases),
  and check `case T_x:` presence in both switches. A GARBAGE node tag in the error
  = stream slip (missing field somewhere earlier); a real small tag = missing
  reader case.
- PG14 added per-message-type length limits in `SocketBackend` (postgres.c):
  the GGDB dispatch message types `'M'` (serialized plan) and `'T'` (DTX protocol)
  need `maxmsglen = PQ_LARGE_MESSAGE_LIMIT` or EVERY dispatch fails with
  `invalid message length` and the cluster never leaves utility mode.
- Field desyncs actually hit: `ColumnDef.compression`, `FuncCall`
  (funcformat + `over` position), `A_Expr` AEXPR_NOT_DISTINCT (THREE copies:
  readfuncs + outfast + readfast), `RestrictInfo` (kept-both write, missing reads),
  `JoinExpr.join_using_alias` (updated in text writer but not outfast.c),
  `SelectStmt.groupDistinct`, `CreateFunctionStmt.sql_body` (BEGIN ATOMIC),
  `LockingClause.waitPolicy`.
- `unrecognized node type: N` thrown by a nodeFuncs.c walker (not readfast) =
  the planner generated an MPP-unsupported node. Bounded fix = disable the
  generating GUC, not add serialization (PG14: `enable_resultcache` → off; GGDB
  doesn't integrate Result Cache/Memoize).

## Runtime bring-up checklist (crashes the cluster or hangs connections)

| What | Symptom | Fix |
|---|---|---|
| `hash_create` needs a key-type flag (upstream b3817f5f) | cassert: `FailedAssertion("flags & HASH_STRINGS", dynahash.c)` on every connect | add `\| HASH_STRINGS` to every string-keyed GGDB call (cdbutil.c segment_dns_cache/HostSegs, cdbdtxrecovery.c, reloptions_gp.c); scan literal-flag calls AND variable-flag ones |
| `socket_putmessage` resolution concatenated both versions | client gets 0 bytes; first SELECT hangs forever (initdb fine — never uses pqcomm) | take clean upstream PG14 body |
| postmaster/postgres getopt strings lost GGDB `-M`/`-m` (+ doubled `while` line) | `invalid option -- 'm'`, cluster won't start | restore `Mm`; collapse the twinned `while` |
| `raw_parser` init dropped `yyextra.tail_partition_magic = false` | `syntax error at or near "PARTITION"` on `OVER (PARTITION BY ...)` | restore the init (GGDB lexer tie-in) |
| `foreach` + `list_delete_first` on the same list (array-based Lists) | cassert: `list != NIL` assert on the first post-bootstrap CREATE TABLE | upstream's `while (stmts != NIL)` idiom |
| GGDB `init_var` macro overwritten by upstream's same-named memset macro | `pfree(NULL)` abort in every numeric op | restore GGDB's do-while (`quick_init_var` sets `buf = ndb`) — the "GGDB-tuned macro silently replaced" trap |
| `GetCachedPlan` lost GGDB's extra IntoClause param (stubbed to a `= NULL` local) | CTAS `AS EXECUTE` → `oids were assigned, but not dispatched to QEs` | restore the param; grep for `= NULL;` locals shadowing documented params |
| `ExplainExecuteQuery` passes the cached plan with intoClause unset | `EXPLAIN ANALYZE CREATE TABLE AS EXECUTE` → QD SIGSEGV in intorel_startup_dummy | copyObject the pstmt + set `intoClause` (GGDB creates the rel in `intorel_initplan`) |
| GGDB temp tables live in SHARED buffers; upstream short-circuits temp rels to local-buffer drop | PANIC storm `could not open file "base/<db>/t_<relfilenode>"` after checkpoint | keep the `#if 0` around the RelFileNodeBackendIsTemp short-circuit in `DropRelFileNodesAllBuffers` (match its sibling) |
| `VARSIZE_TO_SHORT` hardcoded big-endian short-varlena form | AO/AOCS varlena corruption after UPDATE → scan SIGSEGV | make it endianness-aware (match `SET_VARSIZE_1B`) |
| PG14 bottom-up btree index deletion calls `rd_tableam->index_delete_tuples` unguarded | SIGSEGV via `_bt_bottomupdel_pass` on btree-over-AO/AOCS (their AMs leave it NULL by design) | gate BOTH passes in `_bt_delete_or_dedup_one_page` on the callback being non-NULL (a NULL no-op in tableam.h would corrupt the index) |
| `GetSnapshotDataReuse` fast path (upstream 623a9ba79b) | cassert: `TransactionXmin <= RecentXmin` assert via catcache | `XidCacheRemoveRunningXids` must bump `ShmemVariableCache->xactCompletionCount` — an upstream fix POSTDATING the merge target that a clean merge silently dropped |
| VACUUM option dispatch | `unrecognized vacuum option 40` on QEs | clear/emit new `VACOPT_PROCESS_TOAST`; `index_cleanup`/`truncate` are now tri-state (UNSPECIFIED/AUTO/…) |
| `regression_main` grew a 5th arg | isolation2 fails to BUILD (breaks resgroup CI) | pass NULL `postprocess_result_function` in isolation2_main.c — build `src/test/isolation2` locally, plain regress doesn't cover it |
| gpstop flag validation | `Can not mix --mode options with older deprecated -f,-i,-s` | use `gpstop -ar -M fast` (drop `-f`); a silently failed restart = stale-binary trap |

## Assert-build crashers (--enable-cassert)

The assert∩ORCA matrix gap: CI's assert job builds `--disable-orca` (optimizer=off)
while ORCA jobs build without asserts — the intersection is never tested; run both.
11 distinct crashers were fixed on claude-merge-2 (find them with `git log` there):
two `InterruptHoldoffCount>0` syncrep asserts (DTX `FinishPreparedTransaction`
early-return and the replication-lag throttle — wrap in HOLD_INTERRUPTS);
split-update tlist labeling; ROWID_VAR missing from setrefs input asserts;
recursive-UNION SingleQE built with General's numsegments=-1; COPY
`pq_endmsgread` double-end (the `COPY_OLD_FE`→`COPY_FRONTEND` alias made a dead
old-protocol branch fire on every stdin COPY); `tableamapi.c` completeness assert
`index_delete_tuples != NULL` (drop — AO AMs are NULL by design); `clausesel.c`
`s1==1.0` damping assert (PG14 extended stats legitimately pre-seed s1); logtape
`LogicalTapeSetBlocks` assert (GGDB reports sort stats mid-spill on squelch — drop);
combocid cmin/cmax reader assert (restore the GGDB cursor-reader exemption —
cursor QEs legitimately lag the writer gang); nodeModifyTable AO-update empty
oldSlot. Two recurring patterns: "drop an upstream completeness/precondition assert
GGDB intentionally exceeds" vs "re-graft the GGDB exemption upstream re-tightened".

## Mock unit tests / GUC lists

- PG14 split `errstart` into `errstart`/`errstart_cold` (cold = compile-time-constant
  elevel >= ERROR), so every ERROR-path mock breaks ("Could not get value for
  errstart_cold"). `elog_mock.c` is GENERATED by `src/test/unit/mock/mocker.py`
  (gitignored): add a `make_body_errstart_cold` delegating to `errstart` in
  `src/test/unit/mock/special.py`, then delete `elog_mock.*` to force regen (make
  has no dep on special.py). Where a test asserts on level, branch its
  `EXPECT_EREPORT` (sub-ERROR paths keep plain `errstart`).
- Every GUC must appear in exactly one of `src/include/utils/sync_guc_name.h` /
  `unsync_guc_name.h` (guc_test coverage tests enforce it); new upstream GUCs go to
  **unsync**. The lists are runtime-sorted, but keep them alphabetical anyway.

## Test-side / answer files

- **libpq connect-error prefix**: PG14 libpq prepends
  `could not connect to host "..." (...), port N: ` (or the socket variant) to
  connection FATALs; the host/socket text is environment-specific (CI vs local).
  Do NOT bake it into expected files — mask once with an init_file matchsubs rule
  (done on ai-bump-1; mechanics in
  [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md)).
- `m/^ Settings:.*/` in `src/test/regress/init_file` already ignores the EXPLAIN
  `Settings:` line, so jit-on vs jit-off `Settings: jit='on'` diffs are a non-issue.
- In both PG14 campaigns every "JIT failure" reproduced with `jit=off` — the real
  bugs were ORCA/general (aggno, aggsplit, refrestype). Treat JIT as a red herring
  until a diff survives `jit=off`.
- `--use-existing` pollution, schedule choice, and flaky classes: see
  [greengage-regress-tests](../greengage-regress-tests/SKILL.md); MPP internals
  behind these bugs: [greengage-internals](../greengage-internals/SKILL.md).
