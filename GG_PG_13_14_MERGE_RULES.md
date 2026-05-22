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

## 6. Batch-by-batch process for PG14 merge

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
