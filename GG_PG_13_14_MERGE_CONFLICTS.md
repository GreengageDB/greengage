# GreengageDB — PG13→14 Merge: Conflicts Requiring Deep Analysis

Conflict sites in the PG13→14 merge (`d259afa736..e1c1c30f635`, 2022 commits,
701 conflict files on `claude-merge-2` based on `adb-8.x` tip `0f7c5267a2c`)
that cannot be resolved mechanically and require analysis of PG or Greengage
git history.

For resolution rules, see `GG_PG_13_14_MERGE_RULES.md`.

---

## CONFLICT-01: `procarray.c` — PGXACT elimination + GPDB distributed snapshot

**File**: `src/backend/storage/ipc/procarray.c`
**Conflict count**: 4 markers

### What happened

PG14 eliminated `PGXACT` (commits `dc7420c`–`623a9ba`). XID state previously
accessed via `allPgXact[i]` is now in dense arrays in `PROC_HDR`:

```c
/* PG13 */
allPgXact[i].xid
allPgXact[i].overflowed

/* PG14 */
ProcGlobal->xids[proc->pgxactoff]
ProcGlobal->subxidStates[proc->pgxactoff].overflowed
```

GPDB's `GetDistributedSnapshotMaxInProgressXids()` and `GetLocalOldestXmin()`
access XID state via the old `PGXACT` form.

### Analysis required

```bash
# Find how adb-8.x resolved these functions
git show origin/adb-8.x:src/backend/storage/ipc/procarray.c \
  | grep -A30 "GetDistributedSnapshotMaxInProgressXids"

git show origin/adb-8.x:src/backend/storage/ipc/procarray.c \
  | grep -A20 "GetLocalOldestXmin"
```

Expected: replace `allPgXact[i].xid` with `ProcGlobal->xids[proc->pgxactoff]`
inside all GPDB-specific loops over `arrayP->pgprocnos[]`.

### Reference

PG commits: `dc7420c`, `1f51c17`, `941697c`, `5788e25`, `73487a6`, `623a9ba`.

---

## CONFLICT-02: `xact.c` — GPDB reader-writer XID sharing + streaming abort

**File**: `src/backend/access/transam/xact.c`

### What happened

Two changes landed in the same region of `AbortTransaction()`:

- **PG14**: Added `ResetLogicalStreamingState()` in `AbortTransaction()` and
  `AbortSubTransaction()`; new global `CheckXidAlive`/`bsysscan`.
- **GPDB**: `SharedLocalSnapshotSlot->writer_xact` (pointing into removed
  `allPgXact[]`) must be replaced. `IsCurrentTransactionIdForReader()`
  accesses writer XID state through the slot.

### Analysis required

```bash
# Check what replaced writer_xact in the slot struct
git show origin/adb-8.x:src/include/storage/lock.h \
  | grep -A5 "writer_xact\|writer_proc"

# Check resolved IsCurrentTransactionIdForReader
git show origin/adb-8.x:src/backend/access/transam/xact.c \
  | grep -A20 "IsCurrentTransactionIdForReader"
```

Expected field mapping:
```c
writer_xact->xid           → writer_proc->xid
writer_xact->overflowed    → writer_proc->subxidStatus.overflowed
writer_xact->nxids         → writer_proc->subxidStatus.count
```

---

## CONFLICT-03: `gram.y` — `relkind`→`objtype` rename + GPDB grammar

**File**: `src/backend/parser/gram.y`
**Conflict count**: 21 markers

### What happened

PG14 renamed `.relkind` to `.objtype` in `CreateTableAsStmt` and related
parse nodes. GPDB has substantial grammar additions (SCATTER BY, external
tables, resource queues, ENCODING, etc.) that conflict with the same regions.

21 markers in a grammar file is high risk — a single malformed rule causes
parse errors on all SQL.

### Analysis required

```bash
# List all conflict positions
git diff HEAD -- src/backend/parser/gram.y | grep -n "<<<<<<" | head -25

# After resolution, verify grammar parses
make -C src/backend/parser gram.tab.c 2>&1 | grep "error\|conflict"

# Check GPDB grammar additions in adb-8.x
git show origin/adb-8.x:src/backend/parser/gram.y \
  | grep -n "SCATTER\|ENCODING\|DISTRIBUTED\|EXTERNAL" | head -20
```

For each conflict: if it is a `.relkind`/`.objtype` rename, update GPDB code
in the same rule to use `.objtype`. If it is a GPDB-only grammar rule that PG
also touched, merge both sets of changes.

---

## CONFLICT-04: `guc.c` / `guc_gp.c` — `wal_keep_size` + `hashagg` removal

**Files**: `src/backend/utils/misc/guc.c`, `src/include/utils/guc.h`,
`src/include/utils/guc_tables.h`

### What happened

- `wal_keep_segments` (integer, segment count) → `wal_keep_size` (integer, MB):
  semantic change, not just rename. Existing configuration values differ.
- `hashagg_avoid_disk_plan` (GPDB-added, `GPDB_13_MERGE_FIXME`) must be deleted.
- PG14 reorganized `guc.c` struct types (`GucContext`, `GucFlags`).

### Analysis required

```bash
# Verify hashagg_avoid_disk_plan is gone in adb-8.x
git show origin/adb-8.x:src/backend/utils/misc/guc_gp.c \
  | grep "hashagg_avoid_disk"
# Expected: no output

# Check wal_keep_size variable name and unit
git show origin/adb-8.x:src/backend/utils/misc/guc.c \
  | grep -A15 "wal_keep"

# Verify GucFlags type usage in GPDB GUC table entries
git show origin/adb-8.x:src/include/utils/guc_tables.h | head -50
```

---

## CONFLICT-05: `toasting.c` — OID preassignment removal + `attcompression`

**File**: `src/backend/catalog/toasting.c`

### What happened

PG14 (commit `f3faf35`) stopped creating `pg_type` entries for toast tables,
removing the need for `toast_typid`. PG14 also requires that toast attribute
descriptors set `attcompression = InvalidCompressionMethod`.

### Analysis required

```bash
# Check how adb-8.x resolved this
git show origin/adb-8.x:src/backend/catalog/toasting.c \
  | grep -n "GetPreassigned\|toast_typid\|attcompression"

# Check pg_type.c for related changes
git show origin/adb-8.x:src/backend/catalog/pg_type.c \
  | grep -n "GetPreassigned\|sequence\|toast" | head -15
```

Expected: remove the `if (IsBinaryUpgrade) toast_typid = GetPreassigned...`
block; pass `InvalidOid` to `heap_create()`; add `attcompression =
InvalidCompressionMethod` for all three toast attrs.

---

## CONFLICT-06: `heapam.c` — HOT updates + AO table dispatch

**File**: `src/backend/access/heap/heapam.c`

### What happened

PG14 made significant changes to HOT update logic and added `attcompression`
handling. GPDB has `CdbDispatch*` hooks and AO-table bypass paths in the same
file.

### Analysis required

```bash
# Find all GPDB-specific additions in heapam.c on adb-8.x
git show origin/adb-8.x:src/backend/access/heap/heapam.c \
  | grep -n "CdbDispatch\|AO_\|AppendOnly\|gp_" | head -20

# For each conflict region, determine: dispatch hook, AO bypass, or comment
git diff HEAD -- src/backend/access/heap/heapam.c \
  | grep -n "<<<<<<" | head -20
```

---

## CONFLICT-07: `copy.c` — binary COPY optimization + external table dispatch

**File**: `src/backend/commands/copy.c`

### What happened

PG14 (commit `cd22d3c`) avoided redundant buffer allocations in binary COPY
FROM. PG14 also later splits `copy.c` into `copyfrom.c` / `copyto.c`, but
verify whether that split falls within `e1c1c30f635`:

```bash
git log --oneline e1c1c30f635 -- src/backend/commands/copyfrom.c 2>/dev/null \
  | head -3
# If no output, the split has not happened at our target
```

GPDB has extensive external-table dispatch in `copy.c`.

### Analysis required

```bash
git show origin/adb-8.x:src/backend/commands/copy.c \
  | grep -n "CdbDispatch\|external\|ExtTable\|url_" | head -20
```

Position the binary-COPY optimization (avoiding allocations) correctly relative
to GPDB's external-table dispatch path.

---

## CONFLICT-08: `vacuumlazy.c` — GlobalVis horizon + GPDB AO vacuum

**File**: `src/backend/access/heap/vacuumlazy.c`

### What happened

PG14's snapshot-scalability series replaced `GetOldestXmin()` with the
`GlobalVis*` horizon API in vacuum:

```c
/* PG13 */
OldestXmin = GetOldestXmin(rel, PROCARRAY_FLAGS_VACUUM);
/* PG14 */
vacrel->vistest = GlobalVisTestFor(rel);
/* then: */
GlobalVisTestIsRemovable(vistest, xid)
```

GPDB has AO-table vacuum dispatch in the same file.

### Analysis required

```bash
git show origin/adb-8.x:src/backend/access/heap/vacuumlazy.c \
  | grep -n "AO\|AppendOnly\|OldestXmin\|GlobalVis" | head -20
```

Verify `GetLocalOldestXmin()` in `procarray.c` is updated or removed — if it
returns `RecentGlobalXmin` (deleted in PG14), it must be rewritten.

---

## CONFLICT-09: `pg_aggregate.c` / `aggregatecmds.c` — new aggregate options

**Files**: `src/backend/catalog/pg_aggregate.c`,
`src/backend/commands/aggregatecmds.c`

### What happened

PG14 added `MFINALFUNC_EXTRA` and other aggregate definition options, changing
the `AggregateCreate()` signature. GPDB has custom ordered-aggregate handling
and distributed aggregate dispatch.

### Analysis required

```bash
git diff d259afa736..e1c1c30f635 -- src/backend/catalog/pg_aggregate.c \
  | grep "^[+-].*AggregateCreate\|mfinalfunc_extra" | head -10

# Find all GPDB callers of AggregateCreate
grep -rn "AggregateCreate(" src/ --include="*.c" | head -10
```

---

## CONFLICT-10: `src/tools/pgindent/typedefs.list` — additive merge

**File**: `src/tools/pgindent/typedefs.list`

### What happened

Both PG14 and GPDB added new typedef names in the same alphabetically-sorted
regions. This is a pure additive conflict.

### Resolution

Keep **all** entries from both sides, maintaining alphabetical order. Do not
remove GPDB-specific typedef names (e.g., `MotionNode`, `CdbVisitOpt`, etc.).

---

## Template for new entries

When a conflict requires reading commit history to resolve, add an entry:

```markdown
## CONFLICT-NN: `file` — brief description

**File**: `path/to/file`
**Conflict count**: N markers

### What happened
[PG14 change + GPDB content in same area]

### Analysis required
[Specific git commands to run]

### Reference
[PG commit hashes or GPDB PR numbers]
```
