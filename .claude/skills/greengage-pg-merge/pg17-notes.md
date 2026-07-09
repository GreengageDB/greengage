# PG17 merge notes (branch claude-merge-5)

> Status: **PLANNED — merge not started.** Branch `claude-merge-5` was cut from the
> PG16-green tip `claude-merge-4` (`e49f77e924d`); `merge-base == 97d89101045` (the PG16
> target) so the delta is a clean PG16→PG17. This file is **seeded from a grounded
> per-subsystem audit of the upstream `97d89101045..7dcc6f8e6d7` delta** (SHAs below are
> real upstream commits you can `git show`). General methodology is in [SKILL.md](SKILL.md);
> the PG16 **post-merge** classes in [pg16-notes.md](pg16-notes.md) mostly recur — read its
> "Post-merge bring-up" section first. PG15 catalog/WAL traps: [pg15-notes.md](pg15-notes.md).

## Target and calibration

Merge `7dcc6f8e6d7` ("Run pgperltidy", REL_17-era pinned target): `git merge --no-commit
--no-ff 7dcc6f8e6d7`. Calibration (all measured):

- **2423 commits / 3880 files** upstream — a normal single-major volume (≈ PG16's 2538/3934).
- Conflict surface (upstream∆ ∩ GGDB∆) = **1281 files** (503 .c, 301 .h, 102 .out, 93 .sgml,
  83 .sql, 60 .po, 36 .pl, 11 .dat). **Expect ~600–820 UU** — at or slightly above PG16's
  high end, because upstream churn lands squarely on GGDB's most-forked files.
- **Biggest single-file collisions** (upstream-lines/GGDB-lines): `tablecmds.c` 3749/4952
  (the worst), `gram.y` 928/4106, `pg_dump.c` 1264/3144, `planner.c` 1110/2863,
  `copy.c` 378/7278, `vacuumlazy.c` 1552/34 (tiny GGDB delta onto a near-total rewrite),
  `namespace.c` 1136/439, `xlog.c` 1130/663, `nodeModifyTable.c` 761/771.
- Bulk take-theirs/DU: 60 `.po` + 99 doc `.sgml` + 102 `expected/*.out` in the overlap, plus
  16 tree-wide pgindent/pgperltidy/reindent commits (incl. the TGT tip itself) and copyright
  (`29275b1d177` "Update copyright for 2024").

## The recurring-class scorecard (what recurs from PG16, what doesn't)

**⭐ GOOD NEWS — the GUC boot_val relocation does NOT recur.** PG17 keeps the GUC table in
`guc_tables.c` (no `guc.c→guc_tables.c` move like PG16). Verified: upstream touches **0 lines**
of every GGDB-overridden boot_val entry (enable_nestloop/enable_mergejoin/work_mem/
max_connections/from_collapse_limit/join_collapse_limit/constraint_exclusion/
superuser_reserved_connections/max_prepared_transactions/wal_keep_size/wal_sender_timeout/
log_rotation_size/log_filename/logging_collector/hot_standby/jit); only
`max_locks_per_transaction` shows 1 cosmetic description re-quote on a different line. So the
PG16 recursive-CTE-hang mechanism will NOT repeat — smoke-test `SHOW enable_nestloop` etc.
after the merge, but do not budget a mass boot_val audit. `guc_tables.h` also added **no new
required struct field** (no IndexVacuumInfo.heaprel analog there).

**⚠️ BUT the same "authority got relocated, GGDB's entries silently vanish" pattern recurs in
FOUR new places this cycle** — treat each exactly like the PG16 GUC audit:

| Relocated authority | Commit | GGDB entries that vanish | Re-graft |
|---|---|---|---|
| **Wait events** → `utils/activity/wait_event_names.txt` (generated) | `fa88928470b` | GGDB's custom `WAIT_EVENT_*` members in `wait_event.h` (160 members in base → 3 in TGT) | Re-add every GGDB Activity/IPC event to the `.txt` in **strict alphabetical order** (`generate-wait_event_types.pl` hard-errors on mis-order). Oid-carrying custom CLASSES (ResourceGroup/ResourceQueue/Replication — 16-bit event id can't hold an Oid) stay as hand cases in `pgstat_get_wait_event_type`/`pgstat_get_wait_event`. `#ifdef USE_INTERNAL_FTS` around `WAIT_EVENT_FTS_PROBE_MAIN` **cannot** survive codegen — always-define it in the `.txt` or use the runtime custom-wait-event API. |
| **LWLocks** → `storage/lwlocklist.h` (`lwlocknames.txt` DELETED) | `da952b415f4`, `5b1b9bce844` | GGDB's 14 custom locks (IDs 48–61: SharedSnapshot … ParallelCursorEndpoint) | Move each to `lwlocklist.h` as `PG_LWLOCK(48, SharedSnapshot)` (drop the `Lock` suffix; macro adds it) **AND** register it in `wait_event_names.txt` in the same order — `generate-lwlocknames.pl` now `die`s on a lwlocklist↔wait_event mismatch. **This build-breaks until done.** |
| **Syscache** → `MAKE_SYSCACHE()` macros in catalog headers (`syscache.c cacheinfo[]` + `syscache.h` enum DELETED) | `9b1a6f50b91` | GGDB's 5 hand-written syscaches | Add `MAKE_SYSCACHE(EXTPROTOCOLOID, pg_extprotocol_oid_index, 128)` + `EXTPROTOCOLNAME` (pg_extprotocol.h), `GPPOLICYID` (gp_distribution_policy.h), `RESGROUPOID`+`RESGROUPNAME` (pg_resgroup.h). **Note: MAKE_SYSCACHE takes the INDEX NAME, not the `*IndexId` macro.** |
| **gram.y precedence block** relocated | `a916b47e232` | GGDB's kept `%nonassoc SET` + the `/* SQL/JSON */` `%nonassoc UNIQUE JSON`/`KEYS OBJECT_P SCALAR VALUE_P`/`WITH WITHOUT` block | Upstream deletes those and folds the keywords into the `IDENT` `%nonassoc` line. Reconcile against GGDB's `GENERATED NULL_P` IDENT edit + the 294-line per-keyword MPP hack (gram.y L999-1297). **Only `bison -Wcounterexamples` proves the parse tables are still correct** — marker-clean ≠ correct. |

**Dead-macro sweep RECURS** (the PG16 `HAVE_UNIX_SOCKETS` class). PG17 removes several
`pg_config.h.in` macros; every GGDB `#ifdef` on them becomes silent dead code:
`ENABLE_THREAD_SAFETY` (`68a4b58eca0`, thread-safety now unconditional), `HAVE_LOCALE_T`
(`8d9a9f034e9`), `HAVE_BIO_GET_DATA`/`HAVE_X509_GET_SIGNATURE_NID` (`8e278b65766`/
`c82207a548d`, OpenSSL≥1.0.2), `HAVE_DECL_LLVMGETHOSTCPU*` (`820b5af73dc`, LLVM≥10), plus
**AIX support removed** (`0b16bb8776b`: `src/template/aix` deleted + PORTNAME=aix branches).
After the merge, grep the whole tree (incl. `gpcontrib/`, cdb, ic) for each removed macro.

**Missing/changed-struct-field RECURS** (the PG16 `IndexVacuumInfo.heaprel` class, evolved):
`pg_attribute.attstattarget` REMOVED from fixed `FormData_pg_attribute` → nullable
`FormExtraData_pg_attribute` (`4f622503d6d`); `VacAttrStats.attr` deleted → `int attstattarget`
(`c69bdf837f1`). `VacDeadItems` (array) REPLACED by `TidStore*` + `VacDeadItemsInfo`
(`667e65aac35`). `scan_analyze_next_block` callback `BlockNumber`→`ReadStream*` (`041b96802ef`).
These are compile-breaks (good) not `{0}`-init NULL-derefs — but AO/AOCS handler files are
GGDB-only (no conflict marker) so the compiler catches them only after you build.

**ORCA translator re-graft is LIGHTER than PG16** (no `RTEPermissionInfo` bijection this cycle;
node copy/equal/out/read are auto-generated for new fields). Real ORCA surface, see the ORCA
section at the end.

## Node layer (the centerpiece — same generator machinery as PG16)

The `gen_node_support.pl` node layer works as in PG16 ([pg16-notes.md](pg16-notes.md) has the
generator mechanics). PG17-specific node deltas:

- **⭐ `ParseLoc` retype** (`605721f819f`, `2ea5d8bece8`): `nodes.h` adds `typedef int ParseLoc`
  and gen_node_support.pl now keys COPY/COMPARE/WRITE/READ/JUMBLE-of-location on
  **`$t eq 'ParseLoc'`** instead of `int && name =~ /location$/`. Every `int location` field was
  mechanically retyped. **Trap:** any GGDB-added `int ...location` field left as `int` compiles
  fine (ParseLoc==int) but **silently loses location handling** → falls to plain `WRITE_INT_FIELD`;
  combined with `d20d8fbd3e4` (location masked to -1 in serialization by default) this desyncs
  GGDB's hand-written `outfast.c`/`readfast.c`. Grep GGDB parsenodes/primnodes for
  `int .*location` and retype to `ParseLoc`; confirm GGDB's gen_node_support.pl adopts the
  `$t eq 'ParseLoc'` branch (base still has the old regex).
- **`MemoryContextMethods` +`int flags` on alloc/realloc, new `BumpContext`** (`743112a2e99`,
  `29f6a959cfd`). This extends the PG16 mcxt cluster ([pg16_mcxt_cluster] / pg16-notes.md): every
  method table (`aset.c`/`slab.c`/`generation.c` **and the NEW `bump.c`**) must take the `flags`
  param AND keep GGDB's 2-arg `delete_context(context,parent)` + 4 accounting methods
  (declare_accounting_root/get_current_usage/get/set_peak_usage). BumpContext also goes in
  gen_node_support.pl `@extra_tags`. Give `bump.c`'s method table the same `gp_malloc`/peak
  hooks or bump-allocated memory bypasses GGDB accounting.
- **MERGE node expansion** (`0294df2f1f8` WHEN NOT MATCHED BY SOURCE, `c649fa24a42` RETURNING,
  `5f2e179bd31` into-views): `MergeAction` MOVED parsenodes.h→primnodes.h with
  `matchKind`(`MergeMatchKind`: MATCHED/NOT_MATCHED_BY_SOURCE/NOT_MATCHED_BY_TARGET,
  `NUM_MERGE_MATCH_KINDS`) **replacing the old `bool matched`**; new `MergeSupportFunc` primnode;
  `Query` drops `mergeUseOuterJoin`, adds `mergeTargetRelation`+`mergeJoinCondition`;
  `ModifyTable` adds `mergeJoinConditions`; `ResultRelInfo` replaces
  ri_matched/notMatchedMergeAction with `ri_MergeActions[NUM_MERGE_MATCH_KINDS]`+`ri_MergeJoinCondition`.
- **`plannodes.h` no longer includes `parsenodes.h`** (`615f5f6faa0`) — that's WHY MergeAction/
  OverridingKind moved to primnodes.h. GGDB `.c/.h` that relied on the transitive include will
  fail to compile — add explicit `#include` after merge.
- **SQL/JSON + JSON_TABLE node explosion** (`de3600452b6`, `bb766cde63b`, `6185c9737cf`):
  primnodes.h gains JsonExpr/JsonBehavior/JsonTablePath/JsonTablePlan(abstract)/JsonTablePathScan/
  JsonTableSiblingJoin + enums; parsenodes.h gains raw JsonFuncExpr/JsonTable/JsonTableColumn/
  JsonArgument/JsonParseExpr/…; `TableFunc` gains `functype`(TableFuncType)/colvalexprs/
  passingvalexprs/plan. `makeJsonValueExpr` sig changed; `makeJsonEncoding` removed.
- Smaller: `WindowFunc.runCondition` + new `WindowFuncRunCondition` node (`c65102006b6`);
  `PathKeyInfo`→`GroupByOrdering` **rename** (`0c1af2c35c7`, GGDB still declares PathKeyInfo);
  RangeTblEntry field reorder (`e03349144b0`/`b4080fa3dcf`) — re-verify GGDB's hand-written
  `_out/_readRangeTblEntry` ordering; Bitmapset "no trailing zero words" invariant + asserts
  (`a8c09daa8bb`/`71a3e8c43ba`) — GGDB code that memcpies/hand-builds Bitmapsets can trip it;
  `newNode()` macro→static-inline rewrite (`3c080fb4fad`, `newNodeMacroHolder`/`palloc0fast`
  deleted); `LIMIT_OPTION_DEFAULT` removed (`a6be0600ac3`); trailing commas added to ~30 enums
  (`611806cd726`) → noisy trivial conflicts wherever GGDB appended enum members.
- **`outfast.c`/`readfast.c` are HAND-MAINTAINED** (banner at outfast.c:335: "one-to-one
  correspondence…or you will likely crash the system"). Every new PG17 serialized node/field
  (MergeSupportFunc, WindowFuncRunCondition, all Json* nodes, ModifyTable.mergeJoinConditions,
  TableFunc.functype/colvalexprs/passingvalexprs/plan, WindowFunc.runCondition,
  Query.mergeTargetRelation/mergeJoinCondition) must be added to BOTH in identical order or
  QD→QE dispatch desyncs (the classic GGDB binary-dispatch-desync bug).

## Catalog / genbki

- **⭐ DECLARE_*INDEX arity change** (`226d0a6b989`): new 5-arg form
  `DECLARE_INDEX(name, oid, IndexId, tblname, decl)` — the `on <tbl> using` prefix is GONE and
  a `tblname` arg is added. Catalog.pm's ParseHeader regex now REQUIRES the 5th capture; GGDB's
  **28 old-format lines across 16 headers** (gp_distribution_policy.h, gp_segment_configuration.h×2,
  pg_appendonly.h, pg_extprotocol.h×2, pg_resgroup.h×2, pg_resqueue*.h, …) will **silently
  mis-parse** ('on' captured as table_name) → corrupt BKI, not a clean error. Convert each: drop
  `on X using`, insert `X` as the 4th arg.
- **attstattarget nullable** (`4f622503d6d`, `c69bdf837f1`, `d939cb2fd61`, `3e2e0d5ad7f`): removed
  from fixed `FormData_pg_attribute` (now `BKI_FORCE_NULL` in CATALOG_VARLEN) + new
  `FormExtraData_pg_attribute{attstattarget, attoptions}`. GGDB reads `attr->attstattarget` in
  heap.c (×3), analyze.c (×13), tablecmds.c (×2) and `VacAttrStats.attr` — all break (compile).
  Migrate to `FormExtraData`/`stats->attstattarget`. Same for `pg_statistic_ext.stxstattarget`
  (`012460ee93c`). ORCA's relcache translator (`CTranslatorRelcacheToDXL`) reads Form_pg_attribute
  — verify gpopt compiles after the field move.
- **⭐ ROLE_PG_MAINTAIN OID COLLISION** (`ecb0fd33720`): GGDB pg_authid.dat already defines
  `ROLE_PG_MAINTAIN` at OID **4549**; upstream PG17 uses **6337**. Merging upstream's entry yields
  a duplicate `#define` + duplicate initdb row → initdb failure. **Keep exactly ONE.** The whole
  MAINTAIN feature (ACL_MAINTAIN 1<<14, N_ACL_RIGHTS→15, aclchk map, pg_class_aclmask branch,
  per-command `pg_class_aclcheck(ACL_MAINTAIN)` in vacuum/cluster/indexcmds) is **already
  backported in GGDB** — do NOT re-apply upstream's aclcheck insertions; adopt upstream shape only
  where GGDB lacks it. (`pg_use_reserved_connections` 4550 and `pg_create_subscription` 6304
  already match upstream.)
- **Column renames** (compile-break sweeps across GGDB dbcommands/collationcmds/postinit/pg_dump/
  pg_upgrade/describe): `pg_database.daticulocale`→`datlocale` + new `dathasloginevt`
  (`2d819a08a1c`, builtin collation provider); `pg_collation.colliculocale`→`colllocale`.
- **Structural genbki changes:** genbki.pl output MOVED into `src/include/catalog/` and the
  catalog-file list moved from `backend/catalog/Makefile` to `include/catalog/{Makefile,meson.build}`
  (`6ab2e8385d5`) — GGDB's extra catalog headers must be registered in the NEW location; Perl
  warnings now FATAL (`c5385929593`) so GGDB's gp_segment_id / PGUID-subst / bitmap-opfamily /
  pg_magic_oid grafts in genbki.pl + Catalog.pm GenerateBitmap* hooks must be warning-clean.
- **`ObjectClass`/OCLASS_* removed** (`89e5ef7e218`): dependency.c (×12) / objectaddress.c (×4) now
  switch on `classid`; GGDB's OCLASS entries for its custom catalogs (pg_compression, pg_extprotocol,
  …) must convert to the classid scheme.
- **namespace.c 1136-line rewrite**: search_path cache (`f26c2368dca`), `OverrideSearchPath`→
  `SearchPathMatcher` (`d3a38318ac6`) + `PushOverrideSearchPath`/`PopOverrideSearchPath` REMOVED
  (`7c5c4e1c039`), BackendId→ProcNumber temp-namespace naming. GGDB MPP temp-namespace / gp_dist_random
  edits collide. New **`RestrictSearchPath()`** (`2af07e2f749`) fires in VACUUM/REINDEX/MATVIEW/
  CREATE INDEX — ensure it runs on the **dispatched QE** side too or QD/QE search_path diverge
  mid-dispatch (an MPP correctness re-graft, not just a compile fix).
- **catversion** 202306141→202406281. **No new system catalogs/indexes** and **no relation/index
  OID collisions** (verified) — but new pg_proc/pg_type OIDs were grabbed (3813, 811, 6099, 6105,
  6312–6346 incl. pg_maintain=6337; plus `random(min,max)` grabs 12, interval/to_timestamp, JSON
  fns) — run `duplicate_oids` and check against GGDB's reserved range (7014-7040 in PG15).

## Build system (`configure.ac` / Makefiles / `pg_config.h.in`)

- **Regenerate `configure` with autoconf 2.69** from the resolved `configure.ac` — never hand-merge
  it (same as PG16). Keep GGDB's CFLAGS block (`-O3`, `-Wno-unused-but-set-variable`,
  `-Werror=implicit-fallthrough=3`, `-Wdeprecated-register`) and all `--enable-orca/gpcloud/…`
  while taking upstream's AIX/thread-safety/OpenSSL block deletions in the same regions.
- **`src/backend/Makefile` `postgres:` link rule**: keep GGDB's `$(CXX)` link + `SUBDIRS +=
  gporca gpopt` while accepting upstream's AIX-block deletion (`0b16bb8776b`) and the distprep/
  lwlocknames/wait_event/generated-headers rule rewrites.
- **distprep removed** (`721856ff24b`): `make distprep`/`maintainer-clean` in src/backend are gone;
  genbki output now lands in `src/include/catalog/` directly (`6ab2e8385d5`) — GGDB gp_* catalog
  headers register in the NEW `src/include/catalog/Makefile` (+153 lines). CI/build scripts calling
  `make distprep` break.
- New `pg_config.h.in` macros to adopt: `USE_INJECTION_POINTS` (`d86d20f0ba7`,
  `--enable-injection-points`), `USE_AVX512_POPCNT_WITH_RUNTIME_CHECK`+`HAVE_XSAVE_INTRINSICS`
  (`792752af4eb`, adds `src/port/pg_popcount_avx512.o` with `CFLAGS_XSAVE`/`CFLAGS_POPCNT` —
  GGDB's `override CPPFLAGS` in Makefile.global.in must not clobber per-object flags),
  `HAVE_COPY_FILE_RANGE` (`d93627bcbe5`). Drop stale `enable_thread_safety = @enable_thread_safety@`
  at Makefile.global.in:222.
- **NO new `-Wshadow` this cycle** — verified no `-Wshadow`/`-std=` churn in configure.ac. The
  PG16 gporca `-Wshadow=compatible-local` breakage does NOT recur; **do not re-add the gporca
  filter chasing a phantom.**
- `meson.build` (~494 lines churn): GGDB is autoconf-only → adopt wholesale (0 GGDB delta).

## Grammar (`gram.y`, `kwlist.h`, parser)

- **⭐ Precedence-block relocation** (`a916b47e232`) — see the scorecard table. `bison
  -Wcounterexamples` is the gate.
- **⭐ DUPLICATE-KEYWORD collision** (this campaign's signature risk): GGDB **pre-defines `SPLIT`
  and `PARTITIONS`** keywords (kwlist.h + gram.y `%token` unreserved list + per-keyword precedence)
  for legacy GPDB partition DDL; PG17 `87c21bb9412`/`1adf16b8fba` add the SAME spellings/tokens for
  `ALTER TABLE ... SPLIT/MERGE PARTITIONS`. A naive "take both" → bison "symbol PARTITIONS/SPLIT
  redefined" + shift/reduce ambiguity. **Dedup the kwlist/token/precedence lines AND consciously
  choose which SPLIT PARTITION grammar wins** (GGDB's legacy `SPLIT [DEFAULT] PARTITION … AT (…)
  INTO (…)` / GpSplitPartitionCmd vs PG17's `SPLIT PARTITION name INTO (partitions_list)` /
  AT_SplitPartition). `check_keywords.pl` now uses `warnings FATAL=>'all'` → any mis-order/category
  mismatch is a hard build failure.
- **Keyword-category divergence:** GGDB reclassified partition/preceding/following/exclude→RESERVED;
  PG17 reclassified `json`→COL_NAME. These touch the same kwlist lines — a line-merge can silently
  pick the wrong category.
- **~20 new PG17 keywords** (json_exists/json_query/json_scalar/json_serialize/json_table/json_value/
  conditional/empty/error/keep/nested/omit/path/plan/quotes/string/unconditional/source/target/
  merge_action) must interleave alphabetically into GGDB's 71-keyword-modified list.
- **MERGE grammar/transform:** `MergeWhenClause.matched`(bool)→`matchKind`(MergeMatchKind);
  `transformReturningList` gained an `exprKind` arg (all INSERT/UPDATE/DELETE callers + GGDB's MPP
  `transformMergeStmt` must pass it); `returning_clause` on MergeStmt; new `merge_action()` SRF.
  GGDB's re-grafted MPP MERGE (a PG15 "real-bug" area) must migrate every `.matched` reader to
  `.matchKind`.
- **JSON_TABLE:** new file `parse_jsontable.c` (add to Makefile/meson OBJS), huge
  `transformExprRecurse` additions in parse_expr.c (collides with GGDB's `case T_GroupId`), new
  grammar nonterminals. `parse_merge.c` is un-customized by GGDB (near-clean upstream adoption).
- scan.l: adopt `\v`-whitespace / `non_newline_space` rename / positional-param-no-underscore, but
  **preserve GGDB's `scanner_errposition` int→void** (an auto-merge that takes upstream wholesale
  restores `int`). parser.c: upstream C99-designated-initializer `mode[]` array vs GGDB's
  `Gp_role==GP_ROLE_EXECUTE` guards.

## Planner / optimizer

- **`PathKeyInfo`→`GroupByOrdering` rename** (`0c1af2c35c7`) + group-by pathkey reordering
  (`0452b461bc4`: new `get_useful_group_keys_orderings`, `enable_group_by_reordering` GUC — already
  pre-seeded in GGDB's unsync_guc_name.h). GGDB pathnodes.h:1711 still declares PathKeyInfo — rename.
- **`subquery_planner()`/`grouping_planner()` gained a `SetOperationStmt *setops` param**
  (`12933dc6048`, UNION-via-Merge-Append) — thread NULL at all GGDB call sites (planner.c
  standard_planner + allpaths.c ×5 at 2934/3178/3432/3438/3467). New `PlannerInfo.setop_pathkeys`;
  co-resolve with GGDB's MPP setop/Motion insertion.
- **⭐ setrefs.c silent-wrong-result risk:** `find_minmax_agg_replacement_param` (`fd0398fcb09`
  region) rewrites the exact Aggref→Param area where GGDB re-grafted the **PG16 multi-DQA
  `Aggref.agg_expr_id` equal_ignore fix (`987e469fedc`)**. A lost equal_ignore compiles clean and
  shows only as wrong HAVING results — **re-run gp_dqa / multi-DQA after this merge.**
- **WindowAgg run-condition** (`c65102006b6`): `cost_windowagg` sig `(int,int)`→`(WindowClause*)`
  (costsize.c/cost.h), `create_windowagg_plan` uses `best_path->runCondition`,
  `set_windowagg_runcondition_references` (setrefs.c) — update GGDB MPP window/costing callers.
- **MERGE machinery** threads `ModifyTable(Path).mergeJoinConditions` through
  create_modifytable_path/make_modifytable/create_modifytable_plan; `MergeSupportFunc` +
  `replace_outer_merge_support`.
- **FDW/Custom gating** (`fdw_restrictinfo`/`custom_restrictinfo` new params on
  create_foreign*_path + create_scan_plan). **IS-NULL qual opt** (`RelOptInfo.notnullattnums` +
  restriction_is_always_true/false). CTE pathkey propagation (`create_ctescan_path` +pathkeys arg).
- **geqo**: GGDB deleted most geqo files → upstream's geqo changes don't overlap (low risk).

## Executor / access / storage (the heaviest-rewritten subsystem this cycle)

- **⭐ VACUUM dead_items → TidStore** (`30e144287a7` new `access/common/tidstore.c`, `667e65aac35`):
  `VacDeadItems`(flex-array) REPLACED by `TidStore *dead_items` + `VacDeadItemsInfo{max_bytes,
  num_items}`; sigs of `vac_bulkdel_one_index`/`parallel_vacuum_init`/`parallel_vacuum_get_dead_items`
  changed; `MAXDEADITEMS`/`vac_max_items_to_alloc_size` DELETED. GGDB still carries the old names
  (vacuum.c:3574-3658, vacuumlazy.c ×7) + AO vacuum dead-item accounting — **rewrite, don't
  zero-default.** vacuumlazy.c is a near-total upstream rewrite (+1552) onto a tiny GGDB delta
  (rename heap_vacuum_rel→lazy_vacuum_rel_heap, Gp_role elevel, HeapTupleSatisfiesVacuum rel-arg,
  vac_update_relstats isvacuum) — re-graft those onto the new file.
- **⭐ Combined prune+freeze + WAL-format merge** (`6dbb490261a`, `f83d709760d`): `heap_page_prune`
  → `heap_page_prune_and_freeze(...PruneFreezeResult*, PruneReason...)`; `heap_freeze_execute_prepared`
  split into `heap_pre_freeze_checks`+`heap_freeze_prepared_tuples`. **WAL:** `XLOG_HEAP2_PRUNE`/
  `VACUUM`/`FREEZE_PAGE` renamed to `PRUNE_ON_ACCESS`/`PRUNE_VACUUM_SCAN`/`PRUNE_VACUUM_CLEANUP`;
  `xl_heap_vacuum`+`xl_heap_freeze_page` folded into a single `xl_heap_prune` with `XLHP_HAS_*`
  flags. **GGDB emits `xl_heap_vacuum` (vacuumlazy.c:2578) and decodes it (decode.c:456-458) —
  mirror/standby replay authority MUST use the merged record** (a stale writer vs new reader
  silently corrupts replay). Also: GGDB adds `Relation relation` as the FIRST arg to
  `HeapTupleSatisfiesVacuum`/`...Horizon` — every NEW callsite in the rewritten pruneheap.c/heapam.c
  must get the GGDB rel arg re-added or lose MPP-AO visibility semantics.
- **⭐ Streaming read (ReadStream)** (`b5a9b18cd0b` new `storage/aio/read_stream.c`): adopted by
  seqscans (`HeapScanDescData.rs_read_stream`), **ANALYZE** (`041b96802ef`: tableam callback
  `scan_analyze_next_block(TableScanDesc, BlockNumber)`→`(TableScanDesc, ReadStream*)`), and bitmap
  heap scan. **Hardest semantic port:** GGDB's AO/AOCS `scan_analyze_next_block`
  (appendonlyam_handler.c:1287, aocsam_handler.c:1478 — GGDB-only, NO conflict marker) break against
  the new typedef; AO tables aren't block/buffer-based, so this needs a semantic adapter (or the
  `dd1f6b0c172` "block-level AMs re-use acquire_sample_rows" generalization), not a signature swap.
  Also `nodeBitmapHeapscan.c` MPP prefetch reconciles with skip_fetch-pushed-into-table-AM
  (`04e72ed617b`) + begin-scan-after-bitmap (`1577081e961`).
- **MERGE in `nodeModifyTable.c`** (+761 upstream, +713 GGDB) — the worst executor collision:
  WHEN NOT MATCHED BY SOURCE + RETURNING + cross-partition MERGE vs GGDB's MPP split-update/
  distributed-DML overrides (a PG15 re-graft that WILL re-conflict).
- **New AM/AM-interface fields:** `TupleTableSlotOps.is_current_xact_tuple` (NULL-deref if a GGDB
  custom slot op omits it), amapi `amcanbuildparallel`+`aminsertcleanup` (GGDB bitmap index AM
  amroutine must add them), `smgr` owner→pinned model (smgrclose→smgrdestroy/smgrrelease/smgrpin),
  new `bulk_write.c` (`8af25652489`, nbtsort adoption). New files `tidstore.c`/`read_stream.c`/
  `bulk_write.c` must join the autoconf Makefiles.
- **REVERTED in-window (net-zero, don't chase intermediate sigs):** `tuple_update`/`tuple_delete`
  lock-updated-tuples param (`87985cc9252` reverted by `193e6d18e55`); "Generalize relation analyze
  in table AM" (`27bc1772fc8` reverted `6377e12a5a5`).

## WAL / xact / replication

- **⭐ GOOD: the `xl_xact` WAL record layout is STABLE** — `xact.h`/xact.c ParseCommit/AbortRecord/
  XactLogCommit/AbortRecord are UNTOUCHED upstream. GGDB's serialization re-graft (deldbs/distrib
  sections, `XACT_XINFO_HAS_DROPPED_STATS`=1<<10, `XLOG_XACT_DISTRIBUTED_COMMIT`=0x70) faces **no
  upstream collision — keep GGDB's versions verbatim, do NOT regen from upstream.**
- **⭐ BackendId → 0-based ProcNumber** (`024c5211175`, `ab355e3a88d`): `storage/backendid.h`
  DELETED → `storage/procnumber.h`; `MyBackendId`→`MyProcNumber`, `InvalidBackendId`→
  `INVALID_PROC_NUMBER`, `proc->backendId`→`proc->vxid.backendId`, `BackendIdGet*`→`ProcNumberGet*`.
  **63 GGDB files / 292 refs**, plus **1-based→0-based indexing** (off-by-one trap in
  BackendStatusArray / beentry access). GGDB dispatch/temp-namespace/sinval/DTX use BackendId
  heavily — resolve tree-wide. `pgstat_fetch_stat_beentry(BackendId)` REMOVED →
  `pgstat_get_beentry_by_proc_number(ProcNumber)`.
- **⭐ SLRU 64-bit + per-bank locks** (`4ed8f0913bf`, `53c2a97a926`): page numbers `int`→`int64`
  across the whole SLRU API; `SimpleLruInit` drops `ctllock`, adds buffer/bank tranche ids +
  SyncRequestHandler; single ctllock → `SimpleLruGetBankLock(ctl, pageno)`. **GGDB's
  `distributedlog.c` (+1087, GGDB-only, NO conflict marker) must be rebuilt on the PG17 clog.c/
  subtrans.c pattern** or it silently miscompiles (`int page` truncation) — model on PG17 clog.c.
  Per-SLRU buffer GUCs added (transaction_buffers/subtransaction_buffers/…).
- **Renames:** `ShmemVariableCache`→`TransamVariables` (`b31ba5310b5`, varsup.c ×64);
  `sync_method`→`wal_sync_method` + `SYNC_METHOD_*`→enum `WalSyncMethod`/`WAL_SYNC_METHOD_*`
  (`8d140c58229`, 10 GGDB files; the GUC NAME string is unchanged; delete GGDB's copy of the
  relocated `sync_method_options` array at xlog.c ~189-201). xlog.c `LogwrtResult`→atomics
  `logWriteResult`/`logFlushResult` via `RefreshXLogWriteResult()` (`c9920a9068e`, ~15 GGDB sites).
- **New WAL record `XLOG_CHECKPOINT_REDO`** (`afd12774ae8`) inserted at redo point in online
  checkpoints — add to GGDB's `xlog_redo` switch alongside GGDB's custom rmgr records.
- **New subsystems (clean AU adds, but MPP-interacting):** WAL summarizer + incremental backup
  (`174c480508a` walsummarizer.c, `dc212340058` pg_combinebackup) — `BlockRefTable` only tracks
  standard registered block refs, so **GGDB AO/AOCS custom WAL won't be summarized (incremental
  backup correctness gap — likely defer the feature)**; slot sync / failover logical slots
  (`93db6cbda03` slotsync.c, standby_slot_names). Both register as aux children via the refactored
  `postmaster_child_launch()`/launch_backend.c (`aafc05de1bf`, rewrites 517 postmaster.c lines GGDB
  re-grafts for FTS — add the two new child types without clobbering GGDB FTS/aux additions).
- **`transaction_timeout` GUC** (`51efe38cb92`): per-backend timer armed in StartTransaction — on
  GGDB it can kill long dispatched QE/QD queries mid-flight; handle like `statement_timeout` across
  dispatch (managed/disabled on QEs), don't adopt blindly.
- **2PC FullTransactionId filenames** (`5a1dfde8334`): path `/%08X`→`/%08X%08X` + 8-byte fxid
  header. GGDB DTX writes/reads prepared-xact files — adopt `AdjustToFullTransactionId`/
  `TwoPhaseFilePath` on both write and recovery paths.

## pgstat

- Wait-event generation migration (see scorecard). BackendId→ProcNumber (see WAL section).
- **`PgStat_CheckpointerStats` field rename** (`96f052613f3`): requested_checkpoints/
  timed_checkpoints→num_requested/num_timed + restartpoints_*; breaks GGDB postmaster/checkpointer.c
  (lines 371/385/582) — compile-break, grep every consumer.
- `pgstat_prepare_io_time(void)`→`(bool track_io_guc)` (`3c9d9acae0b`) — any GGDB AO/mirror IO
  caller passes the arg. New `local_blk`/`shared_blk` IO timing columns.
- **RESQUEUE PgStat kind re-graft is CLEAN** — `PgStat_Kind` enum is byte-identical base↔TGT (no new
  stats kind in PG17; per-backend IO/PGSTAT_KIND_BACKEND is PG18), and `PGSTAT_FILE_FORMAT_ID` stayed
  `0x01A5BCAC` so GGDB's `0x01A5BCAD` (+1) survives — but a careless take-theirs would revert it.
- `pgstat_hash_hash_key` now `fasthash32` over raw bytes (`e97b672c88f`) — GGDB RESQUEUE must key on
  a standard `PgStat_HashKey{kind,dboid,objoid}` with no padding.
- Stale `src/backend/postmaster/pgstat.c` (old collector, 6469 lines) is tracked on cm5 but absent
  from both base and TGT upstream → NO conflict, but dead code; confirm the Makefile doesn't compile
  it and consider removing.

## commands / adt / ruleutils

- **ALTER TABLE SPLIT/MERGE PARTITION** (`87c21bb9412`/`1adf16b8fba`) + SET ACCESS METHOD rework
  (`e2395cdbe83`): new `AT_SplitPartition`/`AT_MergePartitions`/`AT_SetExpression`/`AT_ReAddStatistics`
  enum members + switch cases collide with GGDB's `AT_Part*`/`AT_SetDistributedBy`/`AT_ExpandTable` in
  the same ATPrepCmd/ATExecCmd/ATRewriteCatalogs regions of tablecmds.c (the worst file, 3749/4952).
  Verify NO case falls through after merge.
- **`MergeAttributes()` split into helpers** (`64444ce071f` et al) — GGDB's distribution/
  partition-inheritance patches live in the old monolithic function; re-graft into the new helper
  boundaries (MergeChildAttribute/MergeInheritedAttribute), don't paste GGDB logic back into a
  now-shrunken function.
- **COPY new options** on_error/log_verbosity/force_null-* (`9e2d8701194`/`f5a227895e1`/`f6d4c9cf162`)
  land in `ProcessCopyOptions` — GGDB relocated that into copy.c and hand-codes force_null, so merge
  into GGDB's copy.c copy (not copyfrom.c). GGDB has NOT backported these.
- **ruleutils deparse drift (PG16 pattern repeats → answer-file regens on BOTH matrices):** MERGE
  (mergeJoinCondition/WHEN NOT MATCHED BY SOURCE/RETURNING), JSON_TABLE (get_json_table_columns…),
  AT LOCAL, SubPlan/output-param EXPLAIN redesign (`fd0398fcb09` find_param_generator), RECORD-Var
  nesting fix (`e0e492e5a92`), not-null/conperiod. `pg_get_expr_worker` dropped its `relname` arg
  (`ce571434ae7`). Regen from CI tarball; don't mask a real MERGE/MPP behavior change.
- **`estimate_array_length` sig** `int(Node*)`→`double(PlannerInfo*, Node*)` (`9391f71523b`) —
  compile-break at costsize.c/arrayfuncs.c/selfuncs.c callers. `examine_simple_variable` handles
  INSERT-RETURNING/CTE-ref stats.
- numeric `random(min,max)` (`e6341323a8d`, 12 new pg_proc OIDs), interval ±inf (`519fc1bd9e9`),
  to_timestamp TZ/OF — check pg_proc OID collisions vs GGDB's grabbed range.

## Reverted-in-window churn (NET-ZERO at TGT — resolve to upstream/deleted, do NOT re-graft)

The log shows large feature series that were fully reverted before the TGT; `git grep` on
`7dcc6f8e6d7` confirms they're absent. Do not chase intermediate commits or re-graft GGDB hooks for:
remove-useless-self-joins / SJE (`d3d55ce5713` reverted `d1d286d83c0`/`07746a8ef2a` —
`remove_useless_self_joins` absent), OR-clauses-to-ANY (`72bd38cc99a` reverted `ff9f72c68f6`),
temporal PK/FK conperiod (`8aee330af55`), structural not-null (`6f8bb7c1e96`, though the catalog
CONSTRAINT_NOTNULL survives), backtrace_on_internal_error GUC (`a740b213d4b` reverted `592a2283721`),
or_to_any_transform_limit GUC, the tuple_update/delete lock-updated param, and "Generalize relation
analyze in table AM". Resolve every one to the FINAL TGT state.

## ORCA re-graft surface (LIGHTER than PG16 — no bijection this cycle)

Node copy/equal/out/read for new fields are auto-generated (pg_node_attr live in GGDB primnodes.h),
so the classic "hand-written reader missing a new field" trap does NOT apply. Real ORCA surface:

1. **Fallback-gating, not translation:** ORCA doesn't do MERGE or JSON_TABLE — verify
   `CTranslatorScalarToDXL`/`CTranslatorQueryToDXL` cleanly **fall back** (unsupported-feature) for
   `JsonExpr`/`MergeSupportFunc`/JSON_TABLE rather than asserting. JSON_TABLE produces a `TableFunc`
   with a coldeflist → it hits the exact `TranslateDXLTvf` coldeflist path that was **already a PG16
   ORCA bug** (funccolcount=0 → EXPLAIN-VERBOSE invalid-attnum, fixed in `85737e48a5f`); new
   JSON_TABLE columns widen it — re-verify.
2. **Field population on ORCA-built nodes:** `ModifyTable.mergeJoinConditions`=NIL,
   `TableFunc.functype`=TFT_XMLTABLE(=0, but set explicitly on the existing XMLTABLE translation),
   `WindowFunc.runCondition`=NIL — a `{0}`-init is mostly safe but confirm the XMLTABLE path sets
   functype so JSON_TABLE-vs-XMLTABLE dispatch doesn't misfire.
3. **⭐ setrefs.c multi-DQA:** `find_minmax_agg_replacement_param` sits on the shared Aggref path —
   re-verify ORCA's per-aggref aggsplit / `agg_expr_id` handling after the merge (see planner
   section; silent-wrong-result).
4. **Compile-break:** `estimate_array_length` sig change hits any GGDB/ORCA cost caller.
5. **`attstattarget`/`Form_pg_attribute`** move — verify `CTranslatorRelcacheToDXL` still compiles.
6. **Build:** re-`touch` gporca/gpopt sources before relinking (no dep tracking); no `-Wshadow`
   filter needed this cycle.

## Big-file conflict budget (do these individually, NOT in a sweep)

`tablecmds.c` (worst), `gram.y` (bison-validate), `pg_dump.c`, `planner.c`, `copy.c`,
`nodeModifyTable.c`, `namespace.c`, `xlog.c`, `distributedlog.c` (SLRU re-graft, no marker),
`vacuum.c`+`vacuumlazy.c` (TidStore), `heapam.c`+`pruneheap.c` (prune/freeze/WAL), `guc.c`
(AlterSystemSetConfigFile is the single highest-conflict function: GGDB FTS-handler/superuser
bypass + gp_replication.conf vs upstream AllowAlterSystem gate `d3ae2a24f26` + custom-GUC refactor
`2d870b4aeff`), `ruleutils.c`, `aclchk.c`, `xact.c`, `analyze.c`, `Catalog.pm`/`genbki.pl`.

## Phased bring-up (per SKILL.md — each phase catches a class the previous cannot)

Node layer (gen_node_support.pl exit 0) → memory-context cluster (incl. new BumpContext) →
catalog/genbki (DECLARE arity, syscache MAKE_SYSCACHE, ROLE_PG_MAINTAIN dedup, `duplicate_oids`) →
lwlocklist.h + wait_event_names.txt (build-breaks until the 14 GGDB locks migrate) → long-tail
sweep → big clusters → gram.y (`bison -Wcounterexamples`) → configure regen (autoconf 2.69) →
build → `make -s unittest-check` serial (**add the 19 new PG17 GUCs to `unsync_guc_name.h`** or the
guc-coverage mock test fails) → initdb/gpdemo → regress matrix {opt=off, opt=on} → greenplum_schedule
→ isolation2 → CI. Then re-run the PG16 deferred checks: gp_dqa (setrefs multi-DQA), AO vacuum
(TidStore re-graft), mirror/standby WAL replay (heap prune/freeze record merge).
