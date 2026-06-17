---
name: greengage-pg-merge
description: Resolve conflicts and bring up a PostgreSQL major-version merge into the GGDB (MPP) fork - the phased bring-up (compile -> mock unit-tests -> initdb/bootstrap -> regress, each catching a different breakage class), the recurring merge-artifact garbage, the catalog/genbki tightening, and the big PG14 structural re-grafts. Use when merging an upstream PG major version or resolving its conflicts.
---

# Bringing up a PG major-version merge in GGDB

A catch-up merge pulls a large upstream commit range, so you hit several PG
releases' changes at once. Resolve semantically - adopt the upstream API shape,
then re-graft GGDB logic (see [greengage-internals]); never blind `ours`/`theirs`.
`git merge --no-commit --no-ff <tag>`, then `git diff --name-only --diff-filter=U`.

## The decisive diagnostic - diff BOTH merge parents
For any conflicted/suspect file the authoritative answers come from the two merge
parents, NOT from `gg_upgrade` (it tracks the OLD pre-merge PG base):
```
git show <merge-commit>^2:src/path   # what UPSTREAM PG does (second parent)
git show <merge-commit>^1:src/path   # what GGDB had before (first parent)
```
**Apache Cloudberry already did PG14 + Greenplum** - `git show cloudberry/main:path`
is the closest public reference for the *shape* of a PG14 decl, a renamed signature,
a header split, or which GGDB additions upstream superseded. Reference, not a patch
(Cloudberry diverges on optimizer / resource-group). Verify clean:
`git diff --name-only --diff-filter=U` and `rg "^<<<<<<<" src/ doc/` both empty.

## Phased bring-up - each phase catches a class the previous cannot
1. **Compile + link** - mechanical API-shape fixes.
2. **Mock unit tests** - `make -s unittest-check`, run SERIAL (`-j` races on the
   shared `cmockery.o`/`*_mock.o` and yields spurious `undefined reference` / "cannot
   find .o"; re-run the dir serially before believing a break). PG14 split `errstart`
   into `errstart`/`errstart_cold` (cold = compile-time-constant elevel >= ERROR) so
   every ERROR-path mock breaks - branch the test's `EXPECT_EREPORT` on level (tests
   that only hit sub-ERROR keep plain `errstart`). Every new GUC must be in exactly
   one of `sync_guc_name.h`/`unsync_guc_name.h` (new upstream GUCs -> unsync,
   alphabetical). New params on a mocked fn need matching `expect_*`. `mock.mk` link
   order: list the server `libpgcommon_srv.a`/`libpgport_srv.a` AFTER the mock objects.
3. **initdb / bootstrap** - catalog/BKI/planner regressions that NEITHER the compiler
   NOR the mocks catch; they fire only when initdb builds template1. Diagnose by
   diffing the suspect file against the PG14 parent (^2). Recurring:
   - BKI is now **single-quoted**; `genbki.pl`, `bootscanner.l`, initdb's
     `escape_quotes_bki()`, and `guc-file.l` must ALL agree (the merge takes some from
     each side -> `syntax error ... unexpected character "`).
   - GGDB-only genbki substitutions get dropped (e.g. `PGUID` -> `$BOOTSTRAP_SUPERUSERID`).
   - Catalog header order in `catalog/Makefile` (a catalog's array type exists only
     after the catalog) - e.g. `pg_statistic.h` before `pg_statistic_ext*.h`.
   - Every `#define <Catalog>IndexId` needs a matching `DECLARE_UNIQUE_INDEX` (PG14
     added the pg_range multirange index).
   - `pg_proc.dat` duplicate keys are Perl **last-wins**: if both the GGDB and PG14
     `proargnames`/`proallargtypes` survive in one entry, one set silently vanishes -
     collapse to the single set matching the C function's column macro. Edit `.dat`
     with a script (editors mis-detect it as binary).
   - A function PG14 relocated between `pg_proc.dat` and `system_views.sql` ->
     "already exists" (GGDB installs system_views.sql but NOT system_functions.sql,
     so functions PG14 put in the latter must live in pg_proc.dat / system_views.sql).
   - PG14 lazy row-identity: `preprocess_targetlist` must call
     `add_row_identity_columns` for UPDATE/DELETE or you get "could not find junk ctid
     column"; the UPDATE `update_colnos` / `ModifyTable.updateColnosLists` thread (and
     its ORCA translator counterpart) were the campaign's hardest item and ARE now
     adopted - audit other PG14 plan-node fields ORCA may still omit.
4. **Regress** - see [greengage-regress-tests] and [greengage-answer-file-regen].

## Recurring merge-artifact garbage (mechanical - recognize on sight)
- **Duplicate-and-truncate signatures**: git keeps both a PG13 prototype and a
  truncated PG14 one -> "storage class specified for parameter" cascade down the
  header. Delete the old, keep the new (cross-check Cloudberry).
- **Lost opening `/*` or `#ifdef`**: an orphan `* foo` / `#else /* WIN32 */` ->
  "missing terminating", "#endif without #if", or (on include-guarded headers)
  double-include redeclaration spam. Restore the opener (hit in postgres.h,
  miscadmin.h, pathnodes.h).
- **Header splits**: PG14 broke `pgstat.h` into `backend_status.h` / `wait_event.h` /
  `backend_progress.h` (old pgstat.h just `#include`s them). MOVE GGDB additions (the
  `PG_WAIT_RESOURCE_GROUP/QUEUE/REPLICATION` macros) into the new home and delete the
  duplicated blocks from pgstat.h, or they get dropped next time someone tidies it.

## Catalog / genbki tightening (PG14)
- `genbki.pl` REJECTS `oid_symbol` for pg_proc/pg_type (it auto-generates `F_<NAME>`
  and `<TYPE>OID`). Drop the field; add explicit `#define`s only for the few symbols
  C code still references by name.
- `DECLARE_TOAST`/`DECLARE_*INDEX` moved into the per-catalog `pg_*.h` headers - strip
  any duplicates left in the central indexing.h/toasting.h vs the per-catalog copies.
- OID collisions: PG14 grabbed 3000s/4000s/6150-6171 for multirange, GiST sort-support
  and new functions. Renumber colliding GGDB OIDs (legacy `cdbhash_*`, AO table/AM)
  to a confirmed gap from `src/include/catalog/unused_oids`.

## Biggest PG14 structural re-grafts
- **PGXACT eliminated** -> dense arrays in `PROC_HDR`: `ProcGlobal->xids[proc->pgxactoff]`,
  `->subxidStates[]`, `->statusFlags[]`; `MyPgXact` -> `MyProc->pgxactoff`. Re-graft GGDB
  distributed-snapshot (procarray.c) and reader-writer XID sharing
  (`IsCurrentTransactionIdForReader`, the slot's `writer_xact` -> `writer_proc`) onto them.
- **`relkind` -> `objtype`** (now an `ObjectType` enum) in CreateTableAsStmt /
  RefreshMatViewStmt / IntoClause - mechanical, but the type changed from char to enum.
- **`copy.c` split** into copyfrom/copyto (`CopyState` -> `Copy{From,To}State`). GGDB
  KEEPS the monolithic `copy.c` + unified `CopyStateData` (heavily extended for external
  tables) - map upstream back and re-graft only the protocol change (drop v2 branches).
- **Grammar**: PG14's `bare_label_keyword` + `BareColLabel` supersede GGDB's
  `ColLabelNoAs` - REMOVE the old `a_expr ColLabelNoAs` target_el rule (keeping both =
  hundreds of reduce/reduce conflicts); add GGDB `BARE_LABEL` keywords to the rule
  (re-sorted), but clause-introducers (PARTITION/DISTRIBUTED/SCATTER) must be
  `AS_LABEL`, never bare, or you get ~7000 conflicts.
- Others: long-lived `WaitEventSet` for `WaitLatch` (thread `InitializeLatchWaitSet`
  into GGDB startup); `InsertPgAttributeTuple` -> plural bulk insert + new
  `attcompression` column; toast tables get no pg_type (pass `InvalidOid`);
  `RecentGlobalXmin`/`GetFullRecentGlobalXmin` removed -> `GlobalVis*` horizon API.

See also: [greengage-internals] (re-graft methodology + MPP internals),
[greengage-build], [greengage-regress-tests], [greengage-answer-file-regen].
