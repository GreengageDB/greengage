# GreengageDB — PostgreSQL 13→14 Major-Version Merge Rules

This document supplements `GG_PG_MERGE_RULES.md` (PG12→13 rules) with patterns
specific to the PG13→14 merge. Ground truth sources:

1. **PR #2545** ("Sync 14x b12 merge") — the production batch-12 merge into
   `arenadata/gpdb/adb-8.x`, approved by four reviewers, merged 2026-05-20.
2. **PR #2439 / #2490** — exploratory and staging variants of the same batch.
3. **`claude-merge-2` exercise** — merge of PG commits
   `d259afa736..e1c1c30f635` (2022 commits, 701 conflict files) onto
   `adb-8.x` tip `0f7c5267a2c`.

---

## 1. Key structural changes in PG14 and their resolution strategy

### 1.1 `configure.in` → `configure.ac` rename (commit `25244b8`)

PG14 renamed `configure.in` to `configure.ac` to conform with Autoconf
conventions. GreengageDB already tracks `configure.ac` on `adb-8.x`; this
conflict is a pure **take ours** for the file name. The content conflict inside
`configure.ac` follows the same rules as §3.1 of `GG_PG_MERGE_RULES.md`:

```
Resolution:
  Keep GPDB's AC_INIT name + contact, update PG_PACKAGE_VERSION to PG14
  version string from upstream AC_INIT (e.g. "14beta2").
  Keep all GPDB --with-* options.
  Take upstream copyright year update (2020 → 2021).
```

### 1.2 PGXACT elimination — snapshot scalability (commits `dc7420c`–`623a9ba`)

This is the dominant structural change in PG14. The `PGXACT` struct and the
`allPgXact[]` array in `PROC_HDR` were abolished. Transaction XID state is now
stored in dense per-field arrays directly in `PROC_HDR`:

| Old (`PGXACT` field) | New (`PROC_HDR` / `PGPROC` field) |
|---|---|
| `allPgXact[i].xid` | `ProcGlobal->xids[proc->pgxactoff]` |
| `allPgXact[i].nxids` | `ProcGlobal->subxidStates[proc->pgxactoff].count` |
| `allPgXact[i].overflowed` | `ProcGlobal->subxidStates[proc->pgxactoff].overflowed` |
| `allPgXact[i].vacuumFlags` | `ProcGlobal->statusFlags[proc->pgxactoff]` |
| `MyPgXact` pointer | `proc->pgxactoff` index into the dense arrays |

**Resolution strategy** (adopted in PR #2545):

1. **Take upstream shape entirely.** Remove `PGXACT` struct references, remove
   `allPgXact` array, replace with `pgxactoff` index-based access.
2. **Re-graft GPDB-specific `IsCurrentTransactionIdForReader()`** in `xact.c`
   onto the new `PGPROC` fields:
   ```c
   /* OLD (PG13 / GPDB) */
   writer_xact->xid
   writer_xact->overflowed
   writer_xact->nxids
   /* NEW (PG14) */
   writer_proc->xid
   writer_proc->subxidStatus.overflowed
   writer_proc->subxidStatus.count
   ```
3. Remove `SharedLocalSnapshotSlot->writer_xact` pointer — it pointed into
   `allPgXact[]` which no longer exists. The slot now holds the writer
   `PGPROC *` directly.
4. In `procarray.c`, update all GPDB-specific distributed-snapshot logic
   (`GetDistributedSnapshotMaxInProgressXids()`, `GetLocalOldestXmin()`)
   to use `ProcGlobal->xids[pgxactoff]` instead of `MyPgXact->xid`.

**Files most affected**: `src/backend/storage/ipc/procarray.c`,
`src/backend/access/transam/xact.c`, `src/include/storage/proc.h`,
`src/backend/postmaster/autovacuum.c`.

### 1.3 `RecentGlobalXmin` / `GetFullRecentGlobalXmin()` removal

PG14 removed `RecentGlobalXmin` and `RecentGlobalDataXmin` globals from
`snapmgr.c`, replacing them with the `GlobalVis*` horizon mechanism.

The GPDB-specific `GetFullRecentGlobalXmin()` function (which wrapped
`RecentGlobalXmin`) must also be **deleted**. Update its callers to use
`GetOldestNonRemovableTransactionId()` or the `GlobalVis*` API.

### 1.4 `relkind` → `objtype` field rename in parse nodes (commit `cc35d89`)

Upstream renamed the `relkind` field to `objtype` in:
- `CreateTableAsStmt`
- `RefreshMatViewStmt`
- `IntoClause`
- Various `AlterTableCmd` subtypes

**Resolution**: Mechanical rename — take upstream form. The field type changes
from `char` to an enum (`ObjectType`), so comparisons like
`stmt->relkind == RELKIND_RELATION` become `stmt->objtype == OBJECT_TABLE`.

Grep to find all sites in GPDB code:
```bash
grep -rn "->relkind\b\|\.relkind\b" src/backend/ src/include/ \
  --include="*.c" --include="*.h" \
  | grep -v "rd_rel->relkind\|Form_pg_class\|RELKIND_"
```

### 1.5 `InsertPgAttributeTuple` → `InsertPgAttributeTuples` (bulk insert)

PG14 refactored `heap.c` to bulk-insert `pg_attribute` rows using
`TupleTableSlot[]` instead of one row at a time.

- Function renamed: `InsertPgAttributeTuple` → `InsertPgAttributeTuples` (plural).
- New `pg_attribute` column: `attcompression` (column-level compression method).
- Constant renamed: `MAX_PGATTRIBUTE_INSERT_BYTES` → `MAX_CATALOG_MULTI_INSERT_BYTES`.

**Resolution**: Take upstream bulk-insert implementation. Add the
`attcompression` slot value in GPDB-specific catalog-insert paths. GPDB's
`MetaTrackAddUpdInternal` call in the same file is preserved in place.

### 1.6 No `pg_type` entries for sequences and toast tables (commit `f3faf35`)

Upstream stopped pre-allocating a `toast_typid` via `GetPreassignedOidForType()`
in `toasting.c`. The GPDB OID-preassignment block for `toast_typid` must be
**removed** — `toast_typid` is now passed as `InvalidOid` to `heap_create()`.

```c
/* REMOVE this GPDB block: */
if (IsBinaryUpgrade)
    toast_typid = GetPreassignedOidForType(...);
/* Change the call to: */
heap_create(..., InvalidOid, ...);
```

PG14 also adds `attcompression = InvalidCompressionMethod` for all three toast
attribute descriptors (chunk_id, chunk_seq, chunk_data) — keep these additions.

### 1.7 MinimalTuple for tuple queues (`tqueue.c`, commit `cdc7169`)

`TupleQueueReaderNext()` return type changed from `HeapTuple` to `MinimalTuple`.
**Resolution**: Take upstream shape entirely — no GPDB-specific logic in `tqueue.c`.

### 1.8 Long-lived `WaitEventSet` for `WaitLatch()` (commit `3347c98`)

`WaitLatch()` no longer delegates to `WaitLatchOrSocket()`. A module-static
`LatchWaitSet` is used, initialized by new `InitializeLatchWaitSet()`.

**Resolution**: Take upstream. Thread `InitializeLatchWaitSet()` into GPDB's
process startup paths (postmaster, bgworker initialization).

### 1.9 GUC renames and removals

| Old GUC | New GUC | Change type |
|---|---|---|
| `wal_keep_segments` (count) | `wal_keep_size` (MB) | **Semantic change** — values are numerically different |
| `hashagg_avoid_disk_plan` | *(removed)* | Explicitly delete; GPDB-added GUC with a `GPDB_13_MERGE_FIXME` tag |
| `enable_incrementalsort` | `enable_incremental_sort` | Rename (upstream naming convention) |
| `REPLICATION_MASTER` group | `REPLICATION_PRIMARY` | Rename |

For `hashagg_avoid_disk_plan`: **delete the GUC entry** from `guc_gp.c`
and its backing variable. Any GPDB code that tested this flag should have
its condition removed or hardcoded.

### 1.10 `StrNCpy` → `strlcpy` global rename (commit `1784f27`)

PG14 replaced all `StrNCpy()` calls with `strlcpy()` across the tree.
`StrNCpy` is still present in `c.h` but deprecated. Take upstream `strlcpy`
form everywhere. Not a compile error but clean up GPDB-specific files too.

### 1.11 Logical decoding — in-memory streaming for large transactions

`reorderbuffer.c` gained in-memory streaming support. `worker.c` gained binary
replication column support. `xact.c` gained `ResetLogicalStreamingState()` in
both `AbortTransaction` and `AbortSubTransaction`.

**Resolution**: All upstream shape. Update `src/test/isolation2` expected
outputs where replication output format changes.

### 1.12 `pg_type` catalog — new `typsubscript` column

PG14 added `typsubscript` to `pg_type` for custom subscript handlers.
After resolving `pg_type.dat`, run the duplicate-OID scan (§3.6 of
`GG_PG_MERGE_RULES.md`) to ensure no collisions.

---

## 2. Conflict classification matrix (PG13→14 specific)

### 2.1 Build / config identity

Same rules as §3.1 of `GG_PG_MERGE_RULES.md`. `configure.ac` replaces
`configure.in` — conflicts on this file follow the same strategy.

### 2.2 Additive conflicts in `xact.c`

PG14 adds `ResetLogicalStreamingState()` in `AbortTransaction`. GPDB updates
`IsCurrentTransactionIdForReader()` with new writer-proc field names. Keep
**both**.

### 2.3 GPDB FIXME tag cleanup

Search for `GPDB_13_MERGE_FIXME` after resolution:
```bash
grep -rn "GPDB_13_MERGE_FIXME" src/ --include="*.c" --include="*.h"
```
Each hit must be evaluated: if PG14 subsumes the workaround, remove the GPDB
code and the tag. Otherwise, re-tag as `GPDB_14_MERGE_FIXME`.

### 2.4 Test and translation files

- `src/test/regress/expected/*.out` — always take GPDB version.
- `src/test/isolation2/expected/*.out` — always take GPDB version.
- `*.po` translation files — always take GPDB version (GPDB does not maintain
  localization; take ours to avoid regressing GPDB-added strings).

---

## 3. Verification checklist (PG13→14 additions)

Run the full checklist from §5 of `GG_PG_MERGE_RULES.md`, plus:

```bash
# 1. No PGXACT references (struct removed in PG14)
grep -rn "PGXACT\|allPgXact\|MyPgXact" src/ --include="*.c" --include="*.h" \
  | grep -v "^Binary\|/\*"
# Every non-comment hit is a missed migration

# 2. No old relkind field accesses in parse nodes
grep -rn "->relkind\b\|\.relkind\b" src/backend/ src/include/ \
  --include="*.c" --include="*.h" \
  | grep -v "rd_rel->relkind\|Form_pg_class\|RELKIND_"

# 3. wal_keep_segments must be gone
grep -rn "wal_keep_segments" src/ --include="*.c" --include="*.h"
# Expected: zero hits

# 4. hashagg_avoid_disk_plan must be gone
grep -rn "hashagg_avoid_disk_plan" src/
# Expected: zero hits

# 5. InsertPgAttributeTuple (singular) must be gone
grep -rn "InsertPgAttributeTuple[^s]" src/ --include="*.c" --include="*.h"
# Expected: zero hits

# 6. Evaluate all GPDB_13_MERGE_FIXME tags
grep -rn "GPDB_13_MERGE_FIXME" src/ --include="*.c" --include="*.h"

# 7. Standard checks (from GG_PG_MERGE_RULES.md §5)
git diff --name-only --diff-filter=U   # must be empty
rg "^<<<<<<<" src/ doc/               # must be empty
```

---

## 4. File-by-file quick reference (PG13→14 specific)

| File | Resolution strategy |
|---|---|
| `configure.ac` | GPDB AC_INIT + update PG_PACKAGE_VERSION to PG14 version |
| `src/include/catalog/catversion.h` | Take higher value |
| `src/backend/storage/ipc/procarray.c` | Take upstream PGXACT-elimination; re-graft GPDB distributed-snapshot logic onto `ProcGlobal->xids[pgxactoff]` |
| `src/backend/access/transam/xact.c` | Keep both: upstream `ResetLogicalStreamingState()` + GPDB `IsCurrentTransactionIdForReader()` migration to writer_proc |
| `src/include/storage/proc.h` | Take upstream (adds `pgxactoff`, `statusFlags`; removes `PGXACT`) |
| `src/backend/catalog/heap.c` | Take upstream bulk-insert + `attcompression`; keep `MetaTrackAddUpdInternal` |
| `src/backend/catalog/toasting.c` | Remove GPDB's `GetPreassignedOidForType` for toast type; pass `InvalidOid`; add `attcompression = InvalidCompressionMethod` for toast attrs |
| `src/backend/catalog/pg_type.c` | Take upstream (adds `typsubscript`); update GPDB OID-dispatch paths |
| `src/backend/utils/misc/guc.c` | Delete `wal_keep_segments`, add `wal_keep_size`; delete `hashagg_avoid_disk_plan` |
| `src/backend/utils/misc/guc_gp.c` | Remove `hashagg_avoid_disk_plan`; update `enable_incremental_sort` name |
| `src/backend/parser/gram.y` | Take upstream (`.relkind` → `.objtype`); keep GPDB dispatch grammar |
| `src/backend/commands/copy.c` | Take upstream binary COPY optimization; preserve GPDB external-table dispatch |
| `src/backend/replication/logical/reorderbuffer.c` | Take upstream streaming additions; no GPDB-specific logic |
| `src/backend/storage/ipc/latch.c` | Take upstream long-lived `WaitEventSet`; thread `InitializeLatchWaitSet()` into GPDB startup |
| `src/test/regress/expected/*.out` | Always take GPDB version |
| `src/backend/po/*.po` / `src/bin/*/po/*.po` | Always take GPDB version |

---

## 5. Common compile errors after PG13→14 merge

| Error | Cause | Fix |
|---|---|---|
| `'PGXACT' undeclared` | PGXACT struct removed | Replace `allPgXact[i].field` with `ProcGlobal->field[proc->pgxactoff]` |
| `'PROC_HDR' has no member 'allPgXact'` | Dense array removed | Use `ProcGlobal->xids`, `->subxidStates`, `->statusFlags` |
| `'MyPgXact' undeclared` | Pointer removed | Use `MyProc->pgxactoff` to index into `ProcGlobal` arrays |
| `'CreateTableAsStmt' has no member 'relkind'` | Field renamed to `objtype` | Replace `.relkind` with `.objtype`; type is now `ObjectType` enum |
| `implicit declaration of 'InsertPgAttributeTuple'` | Renamed to plural form | Update call sites to `InsertPgAttributeTuples` |
| `'MAX_PGATTRIBUTE_INSERT_BYTES' undeclared` | Renamed | Replace with `MAX_CATALOG_MULTI_INSERT_BYTES` |
| `implicit declaration of 'GetFullRecentGlobalXmin'` | Function deleted | Use `GetOldestNonRemovableTransactionId()` |
| `'wal_keep_segments' undeclared` | GUC renamed to `wal_keep_size` | Update variable name and unit (`GUC_UNIT_MB`) |
| `implicit declaration of 'InitializeLatchWaitSet'` | New function not added to startup | Add call in postmaster/bgworker init |

---

## 6. Use `cloudberrydb/cloudberrydb` as a reference branch

Apache Cloudberry already completed a PG14 merge of the Greenplum codebase
and is the closest public reference for "how should a working GPDB-on-PG14
look." When resolving a difficult merge conflict, fetch the cloudberry
remote and diff the file against `cloudberry/main`:

```bash
git remote add cloudberry https://github.com/cloudberrydb/cloudberrydb.git
git fetch cloudberry --depth=1
git show cloudberry/main:src/path/to/file.c | diff -u - src/path/to/file.c
```

Cloudberry diverges from Greengage in some areas (different optimizer
hooks, different resource-group implementation, etc.), so the diff is a
**reference, not a patch** — use it to confirm the *shape* of a PG14
declaration, the *parameter signature* of a renamed function, the
*split* of a header that PG14 broke up, and which GPDB-specific
additions can be deleted because they were superseded upstream.

Cases where cloudberry was decisive in `claude-merge-2`:

| Question | Cloudberry-resolved finding |
|---|---|
| Does PG14 still need GPDB's `a_expr ColLabelNoAs` target_el rule? | No — `BareColLabel` covers all cases once GPDB keywords are added to `bare_label_keyword` |
| Did `create_append_path` keep `List *partitioned_rels`? | No — removed; the new signature has nine args, not ten |
| Where did `cost_material`, `exprType`, `is_opclause` etc. live? | `optimizer/cost.h` and `nodes/nodeFuncs.h` — no change from PG13 |
| Should `pgstat.h` still declare `BackendState`/`WaitEvent*`? | No — moved to `utils/backend_status.h` and `utils/wait_event.h`; pgstat.h just `#include`s them |

## 7. Merge-artifact patterns we kept hitting (`claude-merge-2`)

The recursive merge driver leaves three kinds of garbage that the build
later trips over. Recognize them on sight; the fix is mechanical.

### 7.1 Duplicate-and-truncate in function signatures

When upstream changes a signature and GPDB had local edits in the same
area, git often keeps **both** signatures and **truncates** one of them.
Result: a valid PG13 prototype, then an orphan tail of the PG14 one (or
vice versa). Cascades as "storage class specified for parameter X" on
every following extern in the same header.

```c
extern void ExecSimpleRelationInsert(EState *estate, TupleTableSlot *slot);   // ← PG13
extern void ExecSimpleRelationUpdate(EState *estate, EPQState *epqstate,      // ← truncated
extern void ExecSimpleRelationInsert(ResultRelInfo *resultRelInfo,            // ← PG14 (correct)
                                     EState *estate, TupleTableSlot *slot);
```

Fix: delete the older signature (and the truncated tail) — keep PG14.
Cross-check against `cloudberry/main` if unsure.

### 7.2 Lost opening `/*` or `#ifdef`

The merge sometimes eats the leading line of a comment block or `#ifdef`
arm, leaving an orphan `* foo` or `#else /* WIN32 */` with no opener.
Compile error is "missing terminating ' character", "expected
specifier-qualifier-list before ..." or "#endif without #if". Symptom on
include-guarded headers: every consumer sees the file double-included,
producing redeclaration spam for every top-level decl.

Hit in `claude-merge-2`:
  - `src/include/postgres.h` — lost `#ifdef WORDS_BIGENDIAN`
  - `src/include/miscadmin.h` — lost `#ifndef WIN32`
  - `src/include/nodes/pathnodes.h` — lost `/*` before `VolatileFunctionStatus` comment and before `TidRangePath` comment

### 7.3 PG14 split of `pgstat.h`

PG14 broke `pgstat.h` into three headers and made the old `pgstat.h`
`#include` them for backward compatibility:

| Type | New home |
|---|---|
| `BackendState`, `PgBackendStatus`, `PgBackendSSLStatus`, `PgBackendGSSStatus`, `LocalPgBackendStatus` | `utils/backend_status.h` |
| `WaitEventActivity`/`Client`/`IPC`/`Timeout`/`IO`, `PG_WAIT_*` macros, `pgstat_report_wait_start`/`end` | `utils/wait_event.h` |
| `ProgressCommandType`, `PGSTAT_NUM_PROGRESS_PARAM` | `utils/backend_progress.h` |

GPDB extensions to those groups (the `PG_WAIT_RESOURCE_GROUP`,
`PG_WAIT_RESOURCE_QUEUE`, `PG_WAIT_REPLICATION`,
`PG_WAIT_PARALLEL_RETRIEVE_CURSOR` macros) must be **moved** into the
new home, not left in `pgstat.h` — otherwise they'll get dropped the
next time someone tidies `pgstat.h`.

Resolution: in `pgstat.h`, delete the duplicated blocks (`BackendState`,
the `PG_WAIT_*` and `WaitEvent*` block, `ProgressCommandType` /
`PGSTAT_NUM_PROGRESS_PARAM`, the `PgBackend*Status` structs, and the
inline `pgstat_report_wait_start`/`end` helpers). Add the GPDB
`PG_WAIT_*` macros to `wait_event.h`.

## 8. Catalog/`genbki.pl` rules tightened in PG14

### 8.1 `oid_symbol` is rejected for `pg_proc` and `pg_type`

`genbki.pl` (line ~651) now *errors out* with "custom OID symbols are
not allowed for pg_proc entries" / "for pg_type entries". The fmgr
table (`Gen_fmgrtab.pl`) auto-generates `F_<PRONAME>` for every pg_proc
row; `form_pg_type_symbol()` auto-generates `<TYPNAME>OID` /
`<TYPNAME>ARRAYOID` for every pg_type row.

Resolution:
- Drop the `oid_symbol => '...'` field from every `pg_proc.dat` and
  `pg_type.dat` entry.
- For pg_proc symbols that C/C++ code still references by name
  (`COUNT_ANY_OID`, `MEDIAN_*_OID`, etc.), add explicit `#define`s in
  `pg_proc.h` near the related `IS_MEDIAN_OID` macro.
- For pg_type symbols already matching the auto-generated form
  (`COMPLEXOID` from `typname 'complex'`, `ANYTABLEOID` from
  `typname 'anytable'`), no `#define` is needed — `form_pg_type_symbol`
  produces the same name.

### 8.2 `assign_next_oid()` replaced `$GenbkiNextOid`

PG14 replaced the file-global `$GenbkiNextOid` scalar with a per-catalog
`assign_next_oid($catname)` call. Any GPDB-local `genbki.pl` patch that
uses `$GenbkiNextOid++` must be updated to call `assign_next_oid()` on
the appropriate catalog (e.g. `assign_next_oid('pg_opfamily')`).

### 8.3 `DECLARE_TOAST` / `DECLARE_UNIQUE_INDEX` moved into per-catalog headers

PG14 moved index and toast declarations from `catalog/indexing.h` and
`catalog/toasting.h` into the individual `pg_*.h` headers. The merge
must **not** keep both copies — any duplicated `DECLARE_*` lines in the
per-catalog headers (when the central headers still have them too) will
fail with "found N duplicate OID(s) in catalog data".

Resolution: strip the duplicated `DECLARE_TOAST`, `DECLARE_INDEX`,
`DECLARE_UNIQUE_INDEX`, `DECLARE_UNIQUE_INDEX_PKEY` lines from the
**per-catalog headers** (taking PG14's central-header form). Leave the
central `indexing.h` / `toasting.h` declarations in place.

### 8.4 GPDB-OID conflicts with PG14 multirange/sort-support OIDs

PG14 grabbed OIDs in the 3000s, 4000s and 6150-6171 range for new
multirange types/operators, GiST sort_support, and `pg_stat_get_-
replication_slot` / `bit_count` functions. GPDB-specific entries that
landed there (notably the legacy `cdbhash_*` family and the AO_*
table/handler OIDs) collide.

Resolution: renumber the GPDB entries to a confirmed-unused range. Run
`src/include/catalog/unused_oids` for an authoritative gap list — at
the time of `claude-merge-2` the script's first suggestion was 9446.
The renumbering used **9446–9469** (24 OIDs):

  - 3435 (AO_COLUMN_TABLE_AM_OID) → 9446
  - 4161/4162 (pg_collation toast) → 9447/9448
  - 4198 (AO_ROW_TABLE_AM_HANDLER_OID) → 9449
  - 4199 (AO_COLUMN_TABLE_AM_HANDLER_OID) → 9450
  - 6150–6158 (cdbhash) → 9451–9459
  - 6162–6171 (cdbhash) → 9460–9469

cdbhash 6140–6149 do **not** clash with PG14 and were left alone.

## 9. Parser/grammar rules added in PG14

### 9.1 New `bare_label_keyword` rule + `check_keywords.pl` enforcement

PG14 added a separate `bare_label_keyword` rule in `gram.y` that
enumerates the keywords usable as a column label *without* `AS`. The
`check_keywords.pl` script run by `Makefile` now enforces three
invariants:

  1. Every keyword tagged `BARE_LABEL` in `kwlist.h` must appear in
     `bare_label_keyword`.
  2. Conversely, every keyword in `bare_label_keyword` must be tagged
     `BARE_LABEL` (or `AS_LABEL`) in `kwlist.h`.
  3. The `bare_label_keyword` rule must be alphabetically sorted
     (with the `_P` suffix stripped for comparison, matching
     `check_alphabetical_order` in `check_keywords.pl`).

Resolution: when merging, append every GPDB-specific keyword that
`kwlist.h` marks as `BARE_LABEL` to `bare_label_keyword` and re-sort
the whole rule. In `claude-merge-2` this was 62 GPDB keywords.

Exception: clause-introducing keywords (`PARTITION`, `DISTRIBUTED`,
`SCATTER`) **cannot** be `BARE_LABEL` because they create
shift/reduce conflicts with their clause syntax (e.g. `SELECT x
SCATTER` is ambiguous with `SELECT x [AS] alias FROM ... SCATTER`).
Mark these `AS_LABEL` in `kwlist.h` and omit from
`bare_label_keyword`. Forgetting this produces ~7000 reduce/reduce
conflicts and the build fails at the bison stage.

### 9.2 New `BareColLabel` non-terminal supersedes GPDB's `ColLabelNoAs`

PG14's `BareColLabel: IDENT | bare_label_keyword` covers the GPDB
extension that previously needed a separate `ColLabelNoAs` /
`keywords_ok_in_alias_no_as` rule. Once GPDB keywords are added to
`bare_label_keyword`, the `target_el: a_expr ColLabelNoAs { ... }`
alternative becomes redundant — and in fact **must** be removed
because keeping both produces hundreds of reduce/reduce conflicts.

The `PartitionIdentKeyword` rule itself stays — it's still referenced
by `PartitionColId` in the ALTER TABLE partition syntax.

### 9.3 New `opt_routine_body` / `opt_createfunc_opt_list` (commit `e717a9a18b2`)

PG14 collapsed the four `CreateFunctionStmt` alternatives to a single
shape using `opt_createfunc_opt_list opt_routine_body` for SQL-standard
function bodies. The merge frequently mis-resolves these, leaving
truncated action blocks. The correct PG14 form is:

```c
CreateFunctionStmt:
        CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
        RETURNS func_return opt_createfunc_opt_list opt_routine_body { ... }
      | CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
        RETURNS TABLE '(' table_func_column_list ')' opt_createfunc_opt_list opt_routine_body { ... }
      | CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
        opt_createfunc_opt_list opt_routine_body { ... }
      | CREATE opt_or_replace PROCEDURE func_name func_args_with_defaults
        opt_createfunc_opt_list opt_routine_body { ... }
;
```

When the merge leaves multiple half-merged variants, just delete
everything and paste this block back.

---

## 10. Batch-by-batch process for PG14 merge

The adb-8.x history shows PG14 was merged in named batches (b1–b12). For
each batch:

```bash
BATCH_END=<pg-commit-sha>
PREV_END=<pg-commit-sha>   # last commit already merged

git merge --no-commit --no-ff $BATCH_END
git diff --name-only --diff-filter=U | tee /tmp/conflicts_batch.txt
wc -l /tmp/conflicts_batch.txt

# Resolve per this document and GG_PG_MERGE_RULES.md
# ...

# Verify
git diff --name-only --diff-filter=U
rg "^<<<<<<<" src/ doc/
grep -rn "PGXACT\|allPgXact" src/ --include="*.c" --include="*.h"

# Build test
sudo docker build -t gpdb8_u22:test -f arenadata/Dockerfile.ubuntu . 2>&1 \
  | grep -E "\.c:[0-9]+: error|\.cpp:[0-9]+: error" | head -20

git commit -m "Merge PG14 commits $PREV_END..$BATCH_END

Batch: <name>
Conflicts resolved: <N>"
```

---

## 11. Unit-test (mock/cmockery) phase — PG13→14

Once the tree compiles **and links**, a separate class of PG14 breakage
shows up only when running the backend mock tests:

```bash
# CI form (serial — see §11.6); recurses src/backend then src/bin and
# runs every cmockery-based *_test.c program.
make -s unittest-check
```

Ground truth: the `claude-merge-2` unit-test fix commits (`185a312bb2a`,
`55eb9df9af6`, `a38fa247d9e`, `3237b4bff21`, `e1216380a35` …
`f3d3b9e9f5c`, plus the `mock.mk` and `ftsmessagehandler_test.c` fixes
made while resolving the test run). At the end of `claude-merge-2` all
53 mock test suites (backend + `src/bin`) pass.

### 11.1 `errstart` split into `errstart` / `errstart_cold` (the dominant test break)

PG14 split `errstart()` into a warm path and a cold path. The
`ereport`/`elog` macros now call **`errstart_cold()`** when the elevel is
a compile-time constant `>= ERROR`, and `errstart()` otherwise. Mock
tests that drive an ERROR/FATAL path set their `expect_*`/`will_return`
on the wrong symbol, so cmockery aborts with a "no expectations" error
on `errstart`.

Fix each affected test's local `EXPECT_EREPORT()` helper to branch on the
level (note: the exact `will_return*` form differs per file — preserve
the file's own side-effect callback):

```c
#define EXPECT_EREPORT(LOG_LEVEL)                              \
    if (LOG_LEVEL < ERROR) {                                   \
        expect_value(errstart, elevel, (LOG_LEVEL));           \
        expect_any(errstart, domain);                          \
        will_return(errstart, false);                          \
    } else {                                                   \
        expect_value(errstart_cold, elevel, (LOG_LEVEL));      \
        expect_any(errstart_cold, domain);                     \
        will_return_with_sideeffect(errstart_cold, false, &_errfinish_impl, NULL); \
    }
```

Files fixed: `tcop/test/postgres_test.c`, `utils/fmgr/test/dfmgr_test.c`,
`utils/init/test/postinit_test.c`, `utils/test/session_state_test.c`,
`utils/mmgr/test/runaway_cleaner_test.c`,
`replication/test/gp_replication_test.c`.
**Do not** blindly edit every test that names `errstart`:
`utils/mmgr/test/redzone_handler_test.c` and `libpq/test/pqcomm_test.c`
only exercise sub-ERROR levels and must keep plain `errstart`.

### 11.2 GUC coverage test — every new PG14 GUC must be listed

`utils/misc/test/guc_test.c` (and `guc_gp_test.c`) assert that **every**
GUC in `ConfigureNamesBool/Int/Real/String/Enum` appears in exactly one
of `src/include/utils/sync_guc_name.h` or `unsync_guc_name.h`. A new PG14
GUC trips `test_*_guc_coverage`:

```
GUC: '<name>' does not exist in both list.
```

Resolution: add each new upstream PG14 GUC to **`unsync_guc_name.h`**,
**alphabetically** (it is not distributed/synced to segments), and keep
the two lists mutually exclusive (`test_guc_name_list_mutual_exclusion`).
The 14 GUCs added in `claude-merge-2`:

```
compute_query_id, debug_invalidate_system_caches_always,
default_toast_compression, enable_async_append, enable_resultcache,
idle_session_timeout, in_hot_standby, log_recovery_conflict_waits,
recovery_init_sync_method, remove_temp_files_after_crash, ssl_crl_dir,
track_wal_io_timing, vacuum_failsafe_age, vacuum_multixact_failsafe_age
```

### 11.3 mock-link breakage: PG14 widened what the test programs pull in

Two `src/backend/mock.mk` changes were needed:

1. **`uuid_le` / `brin_minmax_multi`.** PG14's new `brin_minmax_multi.c`
   calls `uuid_le()`. Remove `src/backend/utils/adt/uuid.o` from
   `EXCL_OBJS` and add `$(UUID_LIBS)` to `MOCK_LIBS`, or every test
   program fails to link with `undefined reference to 'uuid_le'`.

2. **`get_dirent_type` pulls in the FRONTEND `libpgcommon`.** PG14's
   `fd.c` (`walkdir`) calls the new `get_dirent_type()`, which lives in
   `src/common/file_utils.c` (outside its `#ifdef FRONTEND`, so it is in
   *both* `libpgcommon_srv.a` and the frontend `libpgcommon.a`). The one
   test that mocks `fd` — `cdb/test/cdbappendonlyxlog` — then has
   `fd_mock.o` referencing `get_dirent_type` **after** the linker has
   already scanned the server `libpgcommon_srv.a` (it sits in
   `objfiles.txt`, ahead of the mock objects). The reference is resolved
   from the FRONTEND `libpgcommon.a` carried in `$(LIBS)`, dragging in
   frontend `file_utils.o` + `fe_memutils.o` and detonating with:

   ```
   multiple definition of `fsync_fname'  / `durable_rename'  (vs fd_mock.o)
   multiple definition of `palloc' / `pfree' / `pstrdup' ...  (vs mcxt.o)
   ```

   Fix: re-list the **server** archives *after* the mock objects in the
   `%.t` link rule, so late `src/common` references resolve against the
   server variant (which omits the FRONTEND-only `fsync_fname` /
   `durable_rename` / `palloc`):

   ```make
   MOCK_SRV_LIBS := $(top_builddir)/src/common/libpgcommon_srv.a \
                    $(top_builddir)/src/port/libpgport_srv.a
   # ... in the %.t recipe, between the mock objects and $(MOCK_LIBS):
   ... $(filter-out %/objfiles.txt, $^) $(MOCK_SRV_LIBS) $(MOCK_LIBS) -o $@
   ```

   This is the correct fix (server `file_utils_srv.o` defines only
   `get_dirent_type`, references `palloc` which `mcxt.o` already
   satisfies) — prefer it over `-Wl,--allow-multiple-definition`.

### 11.4 Mock expectations must cover new PG14 function parameters

When PG14 adds a parameter to a function a test mocks, the
auto-generated mock checks the new parameter and the test fails with:

```
Could not get value to check parameter <param> of function <fn>
```

Add the matching `expect_value()`/`expect_any()` with the value from the
real call site. Concrete case: PG14 added `bool two_phase` to
`ReplicationSlotCreate()`; `fts/test/ftsmessagehandler_test.c` needed
`expect_value(ReplicationSlotCreate, two_phase, false);` to match the
`ReplicationSlotCreate(name, false, RS_PERSISTENT, false)` call in
`ftsmessagehandler.c`. Post-merge grep: `Could not get value to check parameter`.

### 11.5 Verification

```bash
# Run the whole suite the way CI does (serial):
make -s unittest-check 2>&1 | tee /tmp/ut.log
grep -E "\[ FAILED" /tmp/ut.log        # must be empty
echo "exit=$?"                          # make must exit 0

# Targeted re-run of one directory while iterating:
make -C src/backend/<subsys>/test check
```

### 11.6 Caveat: `make -j unittest-check` races (do not mistake for regressions)

The mock build generates shared objects (`cmockery.o`, the per-file
`*_mock.o`) on demand; under `-j`, multiple test directories build the
same shared object concurrently and intermittently fail with
`cannot find .../cmockery.o`, spurious `undefined reference`, or
truncated-object link errors. CI runs the target **serially**. When
triaging a `-j` failure, **re-run the offending directory serially**
before assuming a real break — in `claude-merge-2`, 3 of the 5 initial
`-j` failures (`catalog/storage_tablespace`, `utils/datumstream`,
`utils/hash/dynahash`) were only this race; the two genuine failures were
§11.3.2 (`cdbappendonlyxlog`) and §11.4 (`ftsmessagehandler`).

---

## 12. Other PG14 API-shape changes hit during `claude-merge-2`

These are mechanical "adopt the new signature, re-graft GPDB args" fixes
not covered above. Each row is a real commit from the fix history.

| Symbol / area | PG14 change | Resolution |
|---|---|---|
| `commands/copy.c` | `copy.c` split into `copyfrom.c`/`copyto.c`; `CopyState`→`CopyFromState`/`CopyToState`; protocol-v2 removed | **GPDB keeps the monolithic `copy.c` + unified `CopyStateData`** (it heavily extends it for external tables / distribution). Map upstream `Copy{From,To}State` back to `CopyState`; re-graft only the protocol change: drop v2 branches, collapse `COPY_OLD_FE`/`COPY_NEW_FE`→`COPY_FRONTEND` (keep old names as compat macros), use direct `pq_beginmessage`/`pq_endmessage`. |
| `BeginCopyFrom()` | upstream gained a `whereClause` arg | GPDB's signature differs — when fixing callers (`file_fdw`) match **GPDB's** arg list; the merge tends to insert a spurious extra `NULL`. |
| `src/bin/scripts` connect API | `connectDatabase`/`connectMaintenanceDatabase` now take a `ConnParams *` (`fe_utils/connect_utils.h`) | Take **cloudberry's `scripts/common.c`/`.h` wholesale**; revert any transient `*_cparams` wrapper shims. |
| `simple_prompt()` | returns the string (no caller buffer); moved `src/port`→`src/common` | Update `initdb.c`, `pgbench.c`, `scripts/common.c`; add `sprompt.o` to link where needed. |
| `output_completion_banner()` | now 1 arg (was 2) | `pg_upgrade` caller. |
| `fmtQualifiedId()` | GPDB form takes no encoding arg | Drop cloudberry's `fmtQualifiedIdEnc`; use `fmtQualifiedId`. |
| `ReindexIndex`/`ReindexTable` | unified into `ExecReindex()` | Replace the GPDB Reindex dispatch in `utility.c` with `ExecReindex`. |
| `ProcedureCreate()` | upstream arg list changed | Re-add the two **GPDB-specific trailing args** `prodataaccess`, `proexeclocation` (e.g. the four multirange-constructor calls in `typecmds.c`). |
| `cluster_rel()` | return type `bool`→`void` | Drop the return-value use. |
| `pg_hex_encode()`/`pg_hex_decode()` | gained a `dstlen` arg | Thread the destination length through callers. |
| `errcontext_msg()` / `set_errcontext_domain()` | return `int` (was `void`), for the new `ereport` | Fix the return types in GPDB copies. |
| `pqPutMsgStart()` | dropped the `force` parameter | Update `cdbdisp_async.c`. |
| `nodeModifyTable` | single subplan: `mt_whichplan`/`mt_nplans`/`mt_plans` gone; `TransitionCaptureState.tcs_map` gone; `jf_junkAttNo`→`ri_RowIdAttNo`; `ri_PartitionCheck`→`rd_rel->relispartition` | Remove the dead multi-subplan logic; fix `ExecInsert`/`ExecUpdate`/`ExecDelete` and `ExecCrossPartitionUpdate` (new `segid`) call sites. |
| `ReadNewTransactionId()` | renamed `ReadNextTransactionId()` | Mechanical rename (also in `test/regress`). |
| `doputenv()` | use `setenv()` | `pg_regress.c` / regress driver. |
| backend `libpq` protocol v2 | `fe-protocol2.c` removed | Drop `fe-protocol2` from the backend libpq `Makefile`. |

---

## 13. `initdb` / cluster-bootstrap phase — PG13→14

After the tree compiles **and** the mock unit tests pass, the next gate is
`initdb` (creating the demo cluster). The merge left a whole class of
catalog / BKI / planner regressions that **neither the compiler nor the
cmockery tests can catch** — they only fire when `initdb` actually builds
and populates `template1`. In `claude-merge-2`, `initdb` was completely
broken and took **7 distinct fixes** (commit `770bc1f1fb2`) to get through
bootstrap and most of post-bootstrap; one larger item (the PG14 UPDATE
rework, §13.5) remained.

### 13.1 How to run and diagnose

Fast dev loop (don't rebuild the Docker image per fix — see
[[build-test-docker-workflow]]): run a container with the **mounted** source
plus `--sysctl kernel.sem=...`, build once with
`--prefix=/usr/local/greenplum-db-devel` (ORCA on), install, create the
gpadmin user + demo cluster, then iterate:

```bash
# after editing a source file, reinstall only what changed and re-run initdb
make -j20 -C src/backend install            # backend (.c) or BKI/genbki/catalog headers
make -C src/backend/catalog install         # *.sql data files (system_views.sql etc.)
su gpadmin -c '.../bin/initdb -E UNICODE -D /tmp/idbtest'   # or make_cluster
```

`initdb` errors arrive as a backend `FATAL`/`PANIC` (the `PANIC: cannot
abort transaction 1, it was already committed` is just fallout — look at
the `FATAL` line just above it) followed by
`initdb: error: ... cdb_init.d directory: Broken pipe`.

**The decisive diagnostic for every one of these:** diff the suspect file
against the merge's **PG14 parent**, not `gg_upgrade` (which is PG13):

```bash
git show <merge-commit>^2:src/path/to/file   # authoritative "what PG14 does"
git show <pre-merge-tip>:src/path/to/file    # what GPDB had before (first parent)
```

Bootstrap and post-bootstrap are two distinct sub-phases:

### 13.2 Bootstrap (`postgres.bki`) failures

These happen during `running bootstrap script ...` — the backend reads the
generated `postgres.bki`. All are catalog/genbki resolution errors.

- **Catalog header ordering.** PG14's `pg_statistic_ext_data.stxdexpr` is
  `pg_statistic[]` (`_pg_statistic`), so `pg_statistic.h` must precede
  `pg_statistic_ext*.h` in `CATALOG_HEADERS` (`src/backend/catalog/Makefile`)
  — the BKI creates a catalog's array type only when the catalog itself is
  created. Symptom: `FATAL: unrecognized type "_pg_statistic"` in
  `bootstrap.c gettype`. (See also §8; the comment in that Makefile warns of
  "undocumented ordering dependencies".)

- **BKI string-literal quoting must be self-consistent.** PG14 switched the
  BKI to **single-quoted** strings, inverted by `DeescapeQuotedString()`.
  Four files must agree, and the merge is prone to taking some from PG14 and
  some from GPDB-PG13:
  - `genbki.pl` — emit `sprintf("'%s'", …)`, escape `''`;
  - `bootscanner.l` — `sid \'([^']|\'\')*\'`, action `DeescapeQuotedString(yytext)`
    (PG14 dropped `scanstr()` and `parser/scansup.h`'s use here);
  - `initdb.c` `escape_quotes_bki()` — wrap in **single** quotes (not the old
    double-quote / `\042`-octal form);
  - `guc-file.l` `DeescapeQuotedString()` — single-quote (shared with
    `postgresql.conf`, so leave it single-quote).
  Symptom: `FATAL: syntax error at line N: unexpected character """` (a `"`
  from the old `initdb.c`) or `... "'"` (a `'` reaching a double-quote
  scanner). Fix all four to the PG14 single-quote convention.

- **GPDB-only genbki substitutions get dropped.** PG14's `genbki.pl` has no
  `PGUID` handling (it's GPDB-specific, for `pg_compression.compowner`'s
  `BKI_DEFAULT(PGUID)`). When the merge adopts upstream `genbki.pl` shape it
  loses GPDB's `s/\bPGUID\b/$BOOTSTRAP_SUPERUSERID/g` (and the
  `$BOOTSTRAP_SUPERUSERID` definition). Symptom: `FATAL: invalid input
  syntax for type oid: "PGUID"` in `InsertOneValue`. Re-add the substitution.

- **Missing per-catalog index `DECLARE`s.** PG14 added the `pg_range`
  multirange index `pg_range_rngmultitypid_index` (oid 2228). GPDB keeps
  index declarations in central `indexing.h` (§8.3); the merge moved
  `pg_range`'s `rngtypid` index but dropped the new `rngmultitypid` one.
  Symptom: `FATAL: could not open relation with OID 2228` in post-bootstrap.
  Cross-check every `#define <Catalog>IndexId` against a matching
  `DECLARE_UNIQUE_INDEX(...)`.

### 13.3 Post-bootstrap (SQL) failures

These happen during `performing post-bootstrap initialization ...`, while
`initdb` feeds `system_views.sql`, `information_schema.sql`, snowball, and
GPDB's `cdb_init.d/*.sql`. **Note GPDB installs only `system_views.sql`
(plus information_schema/snowball) — there is no `system_functions.sql` in
the install**, so functions PG14 placed in `system_functions.sql` must
instead live in `pg_proc.dat` or `system_views.sql`.

- **Duplicate keys in a `pg_proc.dat` entry (Perl last-wins).** When both the
  GPDB and the upstream PG14 versions of `proallargtypes` / `proargmodes` /
  `proargnames` survive in one entry, Perl keeps the **last**, silently
  dropping the other. Hit on `pg_stat_get_activity`: the GPDB set
  (`…leader_pid,sess_id,rsgid,rsgname`, with `sslcompression`) and the PG14
  set (`…leader_pid,query_id`, no `sslcompression`) were both present, so the
  GPDB columns vanished and `CREATE VIEW pg_stat_activity` failed with
  `column s.sess_id does not exist`. Fix: collapse to **one** set that
  matches the C function (`pgstatfuncs.c`, `PG_STAT_GET_ACTIVITY_COLS`)
  exactly — here PG14's columns *plus* GPDB's `sess_id`/`rsgid`/`rsgname`.
  `.dat` files can't be opened by some editors as text (binary heuristic on
  the extension) — edit with a verified `python3`/`perl` script and assert
  the three token-counts are equal.

- **A function PG14 relocated between `pg_proc.dat` and `system_views.sql`.**
  PG14 moved `ts_debug` *into* `pg_proc.dat` (as `prolang => 'sql'` entries)
  and removed it from `system_views.sql`. The merge took the new `pg_proc.dat`
  entries but kept GPDB's old `system_views.sql` copy → `FATAL: function
  "ts_debug" already exists`. Remove the stale `system_views.sql` definition.

- **PG14 row-identity wiring missing for UPDATE/DELETE.** The whole PG14
  lazy row-identity machinery (`add_row_identity_columns`,
  `add_row_identity_var`, `distribute_row_identity_vars`, `row_identity_vars`)
  was merged into `appendinfo.c`/`inherit.c`/`planmain.c`, but
  `preprocess_targetlist()` (`preptlist.c`) was left as GPDB's old version
  that never calls `add_row_identity_columns()` for the base result relation.
  So a simple `DELETE` gets no `ctid` junk column → `FATAL: could not find
  junk ctid column` in `ExecInitModifyTable`. Fix: add the upstream block to
  `preprocess_targetlist`:
  ```c
  if ((command_type == CMD_UPDATE || command_type == CMD_DELETE) &&
      !target_rte->inh)
  {
      root->processed_tlist = tlist;
      add_row_identity_columns(root, result_relation, target_rte, target_relation);
      tlist = root->processed_tlist;
  }
  ```

### 13.4 Error → cause → fix quick table

| `initdb` FATAL | Phase | Cause | Fix |
|---|---|---|---|
| `unrecognized type "_pg_statistic"` | bootstrap | catalog header order | `pg_statistic.h` before `pg_statistic_ext*.h` |
| `syntax error … unexpected character "…"` | bootstrap | BKI quote convention split across files | single-quote everywhere (§13.2) |
| `invalid input syntax for type oid: "PGUID"` | bootstrap | genbki lost GPDB `PGUID` sub | restore `s/\bPGUID\b/.../` + `$BOOTSTRAP_SUPERUSERID` |
| `could not open relation with OID 2228` | post | missing index `DECLARE` | add `pg_range_rngmultitypid_index` to `indexing.h` |
| `could not find junk ctid column` | post | `preprocess_targetlist` missing row-identity call | add `add_row_identity_columns` block |
| `column s.sess_id does not exist` | post | duplicate `.dat` keys, last-wins | one merged `pg_stat_get_activity` col set |
| `function "ts_debug" already exists` | post | function in both `pg_proc.dat` and `system_views.sql` | drop the `system_views.sql` copy |
| `targetColnos does not match subplan target list` | post | PG14 UPDATE rework not adopted | §13.5 (open) |

### 13.5 Open: PG14 UPDATE planning rework (`update_colnos`)

PG14 commit `86dc90056d` reworked UPDATE/DELETE planning. The **DELETE** half
is handled by §13.3's row-identity wiring. The **UPDATE** half is larger and
was *not* adopted: PG14 `preprocess_targetlist` does
`root->update_colnos = extract_update_targetlist_colnos(tlist)` (it does
**not** expand the UPDATE tlist), threads `updateColnos` through
`ModifyTable`, and `ExecBuildUpdateProjection` uses it. GPDB's merged
`preprocess_targetlist` still calls the old `expand_targetlist()` for
`CMD_UPDATE`, so the executor trips on `targetColnos does not match subplan
target list`. Re-grafting this is entangled with GPDB's MPP **split-update**
(distribution-key updates), so it's a substantial, higher-risk change —
use Apache Cloudberry (which already did PG14 + GPDB) as the reference (§6).
