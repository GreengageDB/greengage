# PG15 merge notes — what PG15 changed that bites GGDB

Upstream merge target: `adadae45816` (~15beta2). Campaign branch: **claude-merge-3** —
all commit hashes below live there unless marked upstream; use `git show <hash>` as a
resolution reference. Method (phases, clusters, verifiers) is in [SKILL.md](SKILL.md);
this file is the PG15-specific class reference. Sibling versions:
[pg14-notes.md](pg14-notes.md), [pg16-notes.md](pg16-notes.md).

## Big structural splits and rewrites (adopt upstream shape, re-graft GGDB)

### xlog.c → xlogrecovery.c split
- StartupXLOG clean-merges into a Frankenstein (GGDB body + PG15 patches referencing
  now-file-local vars). Reconstruct from upstream's version + re-graft the GGDB deltas
  (mirror/standby state, DTX/gxid counters, fault injectors, ResetMirrorReadyFlag,
  UpdateCatalogForStandbyPromotion). Remove functions now duplicated in both files
  (link errors); find GGDB functions the merge silently dropped by listing xlog.h
  externs with no definition left in the xlog*.c family.
- **CRITICAL: the split dropped `XLogProcessCheckpointRecord()` and its call from
  `ReadCheckpointRecord`** (only an orphan forward-decl survived). GGDB writes an
  EXTENDED checkpoint record (DTX payload appended after `CheckPoint`); recovery must
  unpack and redo the in-doubt DTX from the checkpoint it starts from, else distributed
  commits abort on segments after crash recovery (`could not open relation with OID N`).
  Re-graft into xlogrecovery.c next to ReadCheckpointRecord (`543096f91c2`).
- ReadCheckpointRecord's strict `xl_tot_len` check rejects the DTX-extended record →
  PANIC "could not locate a valid checkpoint record"; re-graft the GGDB length check.
- Re-graft `XLOG_XACT_DISTRIBUTED_COMMIT` handling into getRecordTimestamp,
  recoveryStopsBefore/After and recoveryApplyDelay (PITR-by-time/xid on the QD).

### PostgresMain → PostgresSingleUserMain split
GGDB globals set at the old shared function top (`main_tid = pthread_self()`,
idle-gang state) must move into PostgresMain proper, not only the single-user path —
otherwise forked backends never initialize them (broken signal forwarding).

### system_views.sql → system_functions.sql split
PG15 moved standard function redeclarations + REVOKEs into the new
`src/backend/catalog/system_functions.sql`. Grep each GGDB-specific name there FIRST —
if absent, it must stay in system_views.sql. initdb must run the new
system_functions.sql step: ~46 pg_proc.dat entries carry placeholder prosrc
`see system_functions.sql`; dropping the step makes col_description() etc. fail with
`syntax error at or near "see"` (see the comment at src/bin/initdb/initdb.c
`setup_run_file`).

### basebackup → bbsink/bbstreamer rewrite (recurring HA hazard)
Server side is basebackup*.c (moved again to `src/backend/backup/` in PG16); client
side is pg_basebackup.c + `src/bin/pg_basebackup/bbstreamer_file.c`. The rewrite
repeatedly dropped GGDB grafts — **audit every GGDB pg_basebackup option/path**:

| Dropped graft | Symptom | Fix |
|---|---|---|
| `--target-gp-dbid` internal.auto.conf write (`WriteInternalConfFile` call) | mirror/standby keeps SOURCE's gp_dbid; dormant until promotion, then FTS rejects probes ("PROBE received dbid:N doesn't match") → recovery never completes | `f6e3c69cfb8` |
| `--force-overwrite` dir EEXIST tolerance (extract_directory) | `gprecoverseg -aF`: "could not create directory .../pg_serial: File exists" | `3345bd16806` |
| `--force-overwrite` symlink (extract_link unlink-first) | stale pg_tblspc symlink "File exists" during mirror-move recovery | `069c9fe3dae` |
| `--force-overwrite` regular file (create_file_for_extract) | fopen "wb" over a read-only AO segfile → EACCES; mirror stays down | `36c5675722d` |
| `-E/--exclude` under the new BASE_BACKUP option syntax | EXCLUDE silently never sent; coordinator's `promote` dir copied to new standby → standby self-promotes | `83538a7e42e` |
| tablespace source-dbid lop-off (basebackup_copy.c SendTablespaceList) + client extraction dir | data dir and symlink target diverge; EEXIST on the GP tablespace version dir | `db73cce9493` |

### pgstat → shared-memory stats
Adopt the shmem model wholesale (pgstat.h = upstream + one marked GGDB block). GGDB
port pattern: new `src/backend/utils/activity/pgstat_gp.c` (resqueue/resgroup/
sessionid/portal stats) + QD tabstat-combine re-grafted into
`src/backend/utils/activity/pgstat_relation.c`; rename callers
pgstat_initstats → pgstat_init_relation. Runtime fallout class:
- Pre-existing double `pgstat_drop_database` in dropdb — harmless under the UDP
  collector, fatal under shmem ("can only drop stats once" → QD PANIC).
- dshash `find_locked` assert on error paths — backport upstream `eed959a457e`
  (adds `LWLockAnyHeldByMe`).
- pgstat_report_tempfile during proc_exit (FileSet temp-file cleanup runs after
  pgstat shutdown) → assert + segment crash under load; guard with
  `proc_exit_inprogress` (`565f490fbcb`).
- Flush throttling (PGSTAT_MIN_INTERVAL=1000ms) breaks pg_sleep-tuned tests → use
  `pg_stat_force_next_flush()`.
- 2PC/pgstat errors right after segment crashes are usually debris — restart and
  re-probe before believing them.

### Other adopt-shape clusters

| Cluster | PG15 shape | GGDB re-graft |
|---|---|---|
| buffile/fileset | SharedFileSet→FileSet, BufFile*Shared→*FileSet, +missing_ok | keep GGDB `work_set` as 3rd BufFileCreateFileSet param |
| logtape ∩ nodeAgg spill | LogicalTape-as-object; get_hash_mem→get_hash_memory_limit | keep statement_mem model: cap limit with PlanStateOperatorMemKB; move the spill fault-injector to the tapeset-creation site |
| nodeModifyTable | ModifyTableContext + Prologue/Act/Epilogue helpers + MERGE | thread segid, splitUpdate, AO trigger guards through the new signatures; fix clean-merged old-sig calls in GGDB-only helpers (ExecSplitUpdate_Insert, ExecOnConflictUpdate) |
| partition pruning | ExecFindMatchingSubPlans(prunestate, initial_prune) | re-graft GGDB `join_prune_paramids` (5-arg) for the Dynamic*scan nodes |
| planner distinct/grouping | create_final/partial_distinct_paths split; group-key-orderings loop | move GGDB dNumGroups + cdb_prepare_path_for_sorted_agg into the per-ordering loop; partial (INITIAL_SERIAL) stage gets NO Motion |
| datetime | pg_tm→pg_itm interval rework | pure take-theirs (GGDB has no custom interval logic) |
| Memoize | ResultCache→Memoize rename | mechanical, incl. GUC lists |
| plan structs | NestPath={JoinPath jpath}, SeqScan={Scan scan} | `->jpath.path` / `->scan.plan` sweeps, including the ORCA translator |

### gram.y
Bison-validate after resolving (see SKILL.md). PG15-specific finds that markers hid:
FORMAT token declared twice; FORMAT/OBJECT_P/VALUE_P/WITHOUT given precedence twice
(GGDB per-keyword block + PG15 JSON lines); duplicate `SET ACCESS METHOD` production
(reduce/reduce); GGDB's ColLabelNoAs / no-AS-alias machinery is dead — superseded by
PG15's `target_el: a_expr BareColLabel`.

### tablecmds.c clean-merge trap
`ATSimplePermissions` gained a leading `AlterTableType` argument; five old-signature
calls in GGDB-only ATPrepCmd cases merged clean with no markers. After ANY signature
change, sweep all call sites for the old arity (multi-line-aware — args continue on
the next line).

### Value-node split (cassert crash class)
PG15 split `Value` into String/Integer/Float/Boolean (T_Null gone) and made
`strVal()`/`intVal()` castNode() asserts. Every GGDB site that raw-cast a non-String
node crashes: transformDistributedBy (strVal on IndexElem), `SET <float_guc>` (Float
A_Const), external-table "no URL" sentinel (use a NULL list element), the ORCA
translator's makeString/makeInteger sites. Binary serializers: _outValue/_readValue
reworked around ValUnion; _outNull removed.

### MERGE (new command) on MPP
Scope decision: **co-located joins only** — dispatch to the writer gang iff the target
is not redistributed, else a clean ERROR (MERGE cannot use a final Explicit Motion:
not-matched rows have no gp_segment_id). ~10 GGDB layers need CMD_MERGE /
mergeActionLists cases: plan walkers (walkers.c T_ModifyTable must walk
mergeActionLists — missing it = QE SIGSEGV on subplans inside WHEN actions), BOTH
completion switches in pquery.c (missing the second → "MERGE 0"), autostats, the QE
DML-plan allowlist in postgres.c, binary out/readfast T_MergeAction. Audit every
GGDB `T_ModifyTable` switch when upstream adds a ModifyTable list field.

## Catalog/genbki (PG15 round)
- `DECLARE_*INDEX` became 4-arg with per-catalog IndexId macro emission; GGDB's
  central indexing.h is unparseable by the new Catalog.pm → distribute the ~30
  GGDB-only index DECLAREs into their per-catalog headers.
- OID watermark: set BOTH FirstBootstrapObjectId and FirstUnpinnedObjectId to 12500
  (GGDB ships more catalog objects; otherwise GGDB objects 12000–12499 become
  unpinned/droppable).
- **New-catalog OID dispatch (recurring):** every new upstream catalog with its own
  OID (PG15: pg_publication_namespace) needs a `GetNewOidForXXX` wrapper in
  `src/backend/catalog/oid_dispatch.c` and its insert path switched from generic
  GetNewOidWithIndex — else QE PANIC "allocated OID N ... in segment (catalog.c)"
  (`324004d5fc3`).
- Every new GUC must appear in sync_guc_names_array or
  `src/include/utils/unsync_guc_name.h`, else startup ERROR "Neither
  sync_guc_names_array nor unsync_guc_names_array contains predefined guc name"
  (11 new PG15 GUCs). Audit: diff guc.c/guc_gp.c GUC definitions vs the two lists.

## Runtime behavior churn that breaks GGDB
- **public schema CREATE revoked from PUBLIC.** Non-superuser CREATE in `public`
  fails. Grants landed at src/test/regress/sql/test_setup.sql:67 and
  src/test/isolation2/sql/setup.sql (`GRANT ALL ON SCHEMA public TO public;`);
  tests that create their OWN database need their own grant. The symptom is often
  indirect: a fault point "never fires" / a test hangs because a CREATE upstream of
  it failed (`85822f6e66b`).
- **FTS handler runs outside a transaction.** PG15 added a second per-parameter-ACL
  superuser() check in AlterSystemSetConfigFile → syscache lookup →
  `Assert(IsTransactionState())` → a promoted segment's FTS handler crash-loops and
  failover never completes. Guard the new check with `if (!am_ftshandler &&
  !superuser())` like the first (`1a5d8d3983f`; both guards visible in guc.c).
  Recurring rule: any new superuser()/syscache/transaction-requiring call on an
  FTS-message path re-breaks promotion (assert builds).
- **WaitForProcSignalBarrier self-deadlock** (PG15 added emit sites to dropdb/movedb).
  The emitter must absorb its own barrier via CHECK_FOR_INTERRUPTS; a leaked
  InterruptHoldoffCount makes that a no-op → hangs forever on its own slot. Leak
  source = GGDB catch-and-continue PG_CATCH blocks (cdbtm.c doNotifying*/
  retryAbortPrepared, resgroup OnCommit/OnAbort) restoring a saved holdoff that
  belongs to a now-unwound frame (note: errfinish zeroes the counts, so
  "skipped RESUME" theories are wrong). Fix = reset InterruptHoldoffCount +
  QueryCancelHoldoffCount at PostgresMain's outer error-recovery handler, before its
  HOLD_INTERRUPTS (`eb937c899ee`). Later versions add more barrier emitters; this
  invariant reset defends all of them.
- **WAL prefetcher assert crashes mirrors** (xlogprefetcher.c record-tracking,
  15beta2, cassert-only but 100% reproducible on replay): mirrors crash-loop →
  cluster degrades mid-run. Ship boot default `recovery_prefetch=off`
  (`05595eafa5c`) until the upstream REL_15_STABLE fix is backported.
- **GGDB firstchild assert vs upstream context resets:** GGDB's AllocSetReset forbids
  resetting a context that still has children; upstream code that
  MemoryContextResetOnly's a parent (PG15 JSON_TABLE nested scans) crashes — flatten
  to sibling contexts (`4d539977f14`).
- **Extensions and shmem_request_hook:** RequestAddinShmemSpace /
  RequestNamedLWLockTranche outside the new hook = FATAL. Port `_PG_init` callers
  (orafce: `fa462283d92`); GGDB's own SharedSnapshotLocks request moved into ipci.c.

## AO-aux relkind class (recurring in PG15 and later)
GGDB append-only aux relations (relkind `'o'` aoseg / `'b'` blkdir / `'M'` visimap)
are heap-AM tables that get vacuumed, but new upstream code keyed on
`RELKIND_HAS_TABLE_AM` (or relkind asserts listing only RELATION/MATVIEW/TOASTVALUE)
misses them:
- cluster_rel() relkind Assert → VACUUM FULL of an AO table crashes (`e5fa2979696`);
- RelationSetNewRelfilenode's RELKIND_HAS_TABLE_AM gate → TRUNCATE leaves aux
  relfrozenxid=0 → later VACUUM hits the wraparound-failsafe assert (`70339adc6f4`);
- GlobalVisHorizonKindForRel assert; plancat estimate_rel_size.

Pattern: keep the upstream macro UNCHANGED, add an explicit AO-aux `if` beside it
(matches existing sites in relcache.c/plancat.c). Audit EVERY new
`RELKIND_HAS_*` / relkind-switch use introduced by the merge.

## Serializer/dispatch gaps (QD→QE)
- out/read field lists must be PAIRED: _outIndexStmt wrote PG15-new
  `nulls_not_distinct` but _readIndexStmt didn't read it → binary readfast "lost
  sync" → every CREATE TABLE with a PK broken. When readfast desyncs, diff the
  _outX vs _readX field lists.
- A node with binary _out/_read but no TEXT ones → "could not dump unrecognized node
  type" (GpPolicy, node 1001; `053f95257b0`). Text-only bodies must be
  `#ifndef COMPILING_BINARY_FUNCS`-guarded (outfast.c #includes outfuncs.c).
- New upstream parse nodes that GGDB dispatches raw (SQL/JSON nodes,
  PublicationObjSpec/PublicationTable) need _out/_read bodies + cases in all FOUR
  switches, even though upstream never serializes them (`11ec71bb10c`).

## Test-infra churn
- Upstream `cc50080a828` moved shared-object creation into test_setup.sql (+
  conversion.sql). Remove every GGDB duplicate creator or objects double up:
  create_table.sql's "CLASS DEFINITIONS" block, create_function_0's C-func dups,
  create_misc's leftover `INSERT INTO tenk2` (tenk2 = 20000 rows; select_parallel
  was CORRECTLY failing — `eedce9f6d23`), point.sql dup inserts. `test: test_setup`
  belongs ONLY in parallel_schedule (installcheck runs both schedules into one DB).
- **pg_regress convert_sourcefiles(): the merge dropped the call** → every .source
  test dies "No such file" (generated sql/expected are NOT git-tracked; local trees
  mask it with stale generated copies). Restored in `fd3a89ae61e`. Related: git
  directory-rename detection auto-moved GGDB .source files `input/`→`sql/` and
  `output/`→`expected/` (move them back), and the GENERATE_ROW_AND_COLUMN_FILES
  markers must stay in the input//output/ dirs. Repo check:
  `git ls-files '*.source' | grep -vE '/(input|output|yml_in)/'` should return only
  src/tutorial.
- recordDependencyOnCurrentExtension: PG15 callers pass isReplace=true and rely on
  the new free-standing-object absorb; keeping the GGDB/PG14 reject breaks
  CREATE EXTENSION for anything with shell operators/types — citext, pg_trgm,
  hstore, ltree (`677b716b159`). Adopt upstream pg_depend.c.
- psql: describe.c callers moved to validateSQLNamePattern; listTables() was left on
  raw processSQLNamePattern — found via `grep -c validateSQLNamePattern` 57 vs
  upstream 58 (`d15af16bb38`).
- Cosmetic .out churn to expect (regen, don't chase): "trailing junk after numeric
  literal" for `123abc` negative tests; psql -c echoes command tags (BEGIN); deparse
  emits `AS "?column?"`; stricter float8 underflow/overflow errors; CLUSTER now
  allowed on partitioned tables (needs per-child indexname when dispatched —
  `6c9aabe29ee`).

## Unit-test (cmockery mock) churn
- shmem_request_hook relocation invalidates RequestNamedLWLockTranche mock
  expectations ("Remaining item(s)") — delete the stale expect blocks.
- New PGPROC/proc-array layout exposes uninitialized stack structs in tests (garbage
  `pgxactoff` indexes ProcGlobal arrays out of bounds) — initialize `= {0}`.
- xlogreader: main_data moved into the decoded record — mocks must set
  `record->record = palloc0(sizeof(DecodedXLogRecord))` (+ main_data/main_data_len)
  instead of poking XLogReaderState->main_data.
- RmgrTable is now non-const and mocks must stub RmgrStartup/RmgrCleanup/RmgrNotFound.

## Misc one-liners
- GGDB's elog.h declares errmsg/errdetail as void: new upstream functions doing
  `return errdetail(...)` need statement-form rewrites.
- Many-versions-behind GGDB files (pgbench): hunk-merging makes a Frankenstein —
  take theirs wholesale, then re-graft the small GGDB feature diff
  (`git diff <merge-base>..HEAD -- <file>` to catalog it).
- Old List-API residue still bites: ic_tcp.c used the pre-PG13 lnext-then-delete
  idiom → assert on any `gp_interconnect_type=tcp` query; use
  foreach_delete_current (`b75f5346ca2`).
- ORCA translator port = Value split + SeqScan={Scan scan} + the objfiles.txt orca.o
  relink trap (`93a1ffbbc9d`; relink details in
  [greengage-build](../greengage-build/SKILL.md)).
- `free()` on palloc'd memory now aborts (new MemoryChunk headers) — latent GGDB
  bugs like bootstrap.c `free(userDoption)` surface at initdb.
- nodeAgg mixed-DQA rule: key transfn choice AND combine-build on `aggref->aggsplit`,
  not `aggstate->aggsplit` (`abc8963abdd`) — same class as the PG14 fix; see
  [pg14-notes.md](pg14-notes.md) and
  [greengage-internals](../greengage-internals/SKILL.md).

Testing the result: run order is compile → `make -s unittest-check` → initdb/gpdemo →
regress matrix → isolation2 → contrib/bin → CI matrix (see
[greengage-regress-tests](../greengage-regress-tests/SKILL.md),
[greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md),
[greengage-ci-triage](../greengage-ci-triage/SKILL.md),
[greengage-cluster-ops](../greengage-cluster-ops/SKILL.md)).
