# PG16 merge notes (branch claude-merge-4)

> Status: **COMPLETE.** Merge `f0a59594975` (target `97d89101045`) + ~40 follow-up
> commits. Build/install/unittest/initdb/cluster green; opt=off AND opt=on
> `parallel_schedule` = 203/203; ORCA builds and runs (`--enable-orca`);
> greenplum_schedule triaged (a few deep MPP/flake items deferred, listed at the end).
> General methodology lives in [SKILL.md](SKILL.md); PG15 traps that still apply
> (dup OIDs after genbki, WAL-section order, clean-merge signature traps, init_file
> regen) are in [pg15-notes.md](pg15-notes.md). The first half of this file is
> conflict-resolution classes; the **"Post-merge bring-up"** section at the end holds
> the highest-value recurring lessons (GUC-table relocation audit, ORCA translator
> port, missing-struct-field, dead-macro) — read those first when starting PG17.

## Target and inventory

Merge `97d89101045` (REL_16_BETA2-era pinned target) into the PG15-green tip:
`git merge --no-commit --no-ff 97d89101045` produced **528 conflicts**. Shape: .c 216,
.h 100 (the real work), .out 55 + .sql 18 (defer to regress phase), .po 47 (GGDB strips
translations — keep deleted), .sgml 34 (docs — defer/theirs), .y 3, .l 3.

Mechanical first pass pays off before any semantic work:
- ~155 files carry a pure-copyright conflict hunk — resolve by script (keep the GGDB
  Greenplum/VMware lines + upstream's new year); ~70 of them become fully clean.
- ~84 deleted-by-us conflicts (translations, docs GGDB dropped) — `git rm`.
- The merge index does NOT survive `git reset --hard`; keep hand-crafted resolutions of
  the hardest files (nodes.h, gen_node_support.pl, outfast.c/readfast.c) backed up in a
  plain directory so the state is re-creatable by re-merging + copying + re-running the
  mechanical script.

Resolution order (proven in PG15, refined here): node layer → memory-context cluster →
catalog/genbki headers → long-tail sweep → big clusters → gram.y → build files → build
(fresh container) → mock unittests → initdb → regress.

## Node layer: copy/equal/out/read are now GENERATED (the centerpiece)

PG16 (upstream `964d01ae90c`+) generates node copy/equal/out/read/queryjumble functions
from `pg_node_attr(...)` annotations in the node headers, via
`src/backend/nodes/gen_node_support.pl`. `copyfuncs.c`/`equalfuncs.c`/`outfuncs.c`/
`readfuncs.c` are now thin stubs: macros → `#include "X.funcs.c"` (generated) → manual
functions for `custom_*` nodes → `#include "X.switch.c"`. **Resolution = adopt the
generated approach and re-express every GGDB node and GGDB-added field as header
annotations**, keeping hand-written functions only for nodes marked `custom_*`.
There is NO middle ground: a leftover hand-written `_copyX` for a non-custom node is a
duplicate symbol; a missing manual function for a `custom_*` node is a link error.

### Generator mechanics (the rules that bite)

- **Three lists must match exactly** (count AND order; the script asserts):
  `@all_input_files` in `gen_node_support.pl`, `node_headers` in
  `src/backend/nodes/Makefile`, `node_support_input_i` in
  `src/include/nodes/meson.build`. Append GGDB headers AFTER the upstream ones
  (NodeTag ordering). Currently 31 entries each.
- Node-level attrs (line after `{`): `abstract`, `custom_copy_equal`,
  `custom_read_write`, `no_copy`, `no_equal`, `no_read`, `no_query_jumble`,
  `nodetag_only`, `special_read_write`. A supertype first field (`Plan plan;`)
  inherits the parent's fields and its `no_*` attrs.
- Field-level attrs: `array_size(F)`, `copy_as(V)`, `copy_as_scalar`,
  `equal_as_scalar`, `equal_ignore`, `read_as(V)`, `read_write_ignore` (needs
  `no_read` or `read_as`), `query_jumble_ignore`. The `array_size` size field must
  LEXICALLY PRECEDE the array — check when re-grafting GGDB fields into PG16 structs.
- Type dispatch: a field type must be in `@scalar_types`, an enum in a scanned header,
  a known node pointer, or one of char*/Bitmapset*/array/location — else the script
  DIES. `Datum` and varlena (`bytea*`) are generator-fatal → make the holder node
  `custom_*` or annotate the field away (`read_write_ignore, read_as(0)`).
- The generator requires `typedef struct X {...} X;` with tag == typedef name.
  Anonymous GGDB typedefs had to be named: `DQAExpr` (primnodes.h),
  `ExtTableTypeDesc` (parsenodes.h); `tupleDescNode` was renamed `TupleDescNode`.

### GGDB scaffolding added (all present on claude-merge-4; use as the reference)

- Headers added to all 3 lists: `catalog/gp_distribution_policy.h` (GpPolicy — becoming
  a real node makes `Query.intoPolicy`/`RelOptInfo.cdbpolicy` auto-copy/equal),
  `executor/execdesc.h`, `cdb/cdbgang.h`, `access/tupdesc.h`, `catalog/heap.h`;
  nodetag-only: `nodes/altertablenodes.h`, `access/formatter.h`,
  `access/extprotocol.h`, `nodes/tidbitmap.h`.
- `@scalar_types` += `CdbLocusType EstimatedBytes ParentStmtType ItemPointerData`
  (gen_node_support.pl ~line 203).
- Grafts INSIDE the generator itself (grep `GPDB` in gen_node_support.pl):
  widened array-dimension regex (computed dims like `words[Max(A,B)]`); out/read
  branches for the int family and the GGDB scalars (ParentStmtType→UINT,
  EstimatedBytes→FLOAT, CdbLocusType→ENUM); `CdbPathLocus` special case (by-value
  struct holding a List*: copy = assignment, out = hand-written `_outCdbPathLocus`,
  no read since Path/PlannerInfo are no_read); a `PlanSlice*` branch emitting
  `COPY_POINTER_FIELD(f, sizeof(PlanSlice))` so `Motion.senderSliceInfo` copies
  shallowly without making Motion custom.
- `PlanSlice`/`ExecSlice` stay NON-nodes (the generator only handles `Foo**` node
  arrays, not `Foo*` value arrays) — their holders `PlannedStmt`/`SliceTable` are
  `custom_copy_equal, custom_read_write` instead.

### Manual functions in the stubs

Custom GGDB-relevant nodes as annotated in the headers: `PlannedStmt`, `SliceTable`,
`SerializedParams`, `CursorPosInfo`, `TupleDescNode`, `ColumnDef`
(Datum `missingVal`), `RangeTblFunction` (bytea `funcuserdata`, field annotated
`read_write_ignore, read_as(0), query_jumble_ignore`). `QueryDispatchDesc` needs NO
custom funcs (`no_equal, no_query_jumble, no_read` — copy/out fully generated).
Rules learned:

- **Derive manual funcs from the FINAL merged struct, never verbatim from the PG15
  hand-written ones** — those are stale (`_copyPlannedStmt` lacked PG16's `permInfos`,
  `_copyColumnDef` lacked `storage_name`). Then run a field-coverage check (every
  struct field appears in its manual func) and an out/read pairing check (the
  WRITE_*/READ_* field sequences must match exactly).
- **Binary-only GGDB nodes get `no_read`**: `SerializedParams`, `TupleDescNode`,
  `QueryDispatchDesc` are serialized QD→QE in binary only (readfast.c); text read is
  never called. This matches PG16's own out-only pattern (e.g. EquivalenceClass).
- Keep the GGDB varlena macros in the stubs: `COPY_VARLENA_FIELD` /
  `COMPARE_VARLENA_FIELD` (funcuserdata), WRITE/READ `_DUMMY_FIELD`.
- `_outCdbPathLocus` needs a FORWARD DECLARATION before `#include
  "outfuncs.funcs.c"` — the generated code calls it for every `Path.locus`.
- Historical copy-paste bug fixed on port: the old `_copySerializedParams` execParams
  loop read `externParams[i].value` — re-check if porting from an old branch.
- DELETE absorbed grafts or they become duplicate symbols: GGDB `_out/_read` for
  upstream parse/utility statements (PG16 auto-serializes ALL of them),
  RangeVar.catalogname serialization, PG15-era JSON serializers (PG16 reworked the
  node shapes). Keep only the GGDB-ADDED FIELDS in those structs.
- Upstream `Group` plan node returns (GGDB had `#ifdef NOT_USED`'d it) — let it
  auto-generate; drop GGDB's `_copyGroup`/`_readGroup`.

### outfast.c / readfast.c (GGDB binary QD→QE serializers) must be decoupled

GGDB used to compile outfuncs.c/readfuncs.c a SECOND time under
`#define COMPILING_BINARY_FUNCS` (via `#include` from outfast.c/readfast.c). PG16 stubs
`#include` generated files and cannot be re-included under a macro. Fix: make
outfast.c/readfast.c STANDALONE — snapshot the binary variant of every shared function
into them (`git show <pg15-branch>:src/backend/nodes/outfuncs.c | unifdef
-DCOMPILING_BINARY_FUNCS` gives faithful binary-resolved bodies to splice in), and drop
all `COMPILING_BINARY_FUNCS` gates from the stub files. Keep `nodeReadSkip()` and
`pg_strtok_peek_fldname()` in `src/include/nodes/readfuncs.h` (readfast.c uses them).
**New recurring maintenance trap: a node/field change is now edited in TWO places** —
the annotated header (generator) AND *fast.c by hand. The spliced bodies carry PG15
field lists; reconcile them against PG16 structs during the build phase.

### Other node-layer files

- `nodes.h`: take theirs — the NodeTag enum is now `#include "nodes/nodetags.h"`
  (generated; GGDB tags come automatically from the annotated headers). Keep BOTH
  `JOIN_RIGHT_ANTI` (new in PG16) and `JOIN_LASJ_NOTIN` (GGDB) in JoinType;
  `IS_OUTER_JOIN` must include both.
- `nodeFuncs.c`: semantic merge. Keep GGDB exprType/exprCollation cases
  (DMLActionExpr/AggExprId→INT4OID, RowIdExpr→INT8OID); DROP the PG15 JSON cases
  (PG16 has its own reworked ones — keeping both = duplicate case labels referencing
  dead fields); re-express GGDB walker cases with PG16's `WALK()`/`LIST_WALK()` macros.
- Adversarially verify each resolved header for DROPPED GGDB tokens (scan the pre-merge
  side): one sweep dropped the `RELOPT_DEADREL` RelOptKind enum value (pathnodes.h),
  still used by `equivclass.c`.

### The gate

`gen_node_support.pl` exiting 0 is the node layer's build-verify gate (run it via
`make -C src/backend/nodes node-support-stamp`, or perl directly with the header list).
Also check the generated `nodetags.h` contains the GGDB tags (T_Motion, T_GpPolicy,
T_StreamBitmap, T_DQAExpr, ...). A full build is not possible until the rest of the
tree resolves — this gate is what makes the node layer independently verifiable.

## Memory-context cluster: the MemoryChunk rework

PG16 (upstream `c6e0fe1f2a`) deleted `AllocChunkData` (per-chunk `size`/`aset` header)
for an encoded 8-byte `MemoryChunk`, added `src/include/utils/memutils_internal.h`, and
**centralized the method tables**: no per-allocator `AllocSetMethods` — mcxt.c holds one
`mcxt_methods[]` indexed by `MemoryContextMethodID`; pointer ops dispatch via
`GetMemoryChunkMethodID(ptr)`. Adopt PG16's allocators WHOLESALE, then re-graft. Resolve
as ONE all-or-nothing cluster (API and every impl must agree): `memnodes.h`,
`memutils.h`, `memutils_internal.h`, `aset.c`, `slab.c`, `generation.c`, `mcxt.c`.
(`portalmem.c` is a separate, catalog-coupled conflict — not part of this cluster.)

GGDB re-grafts, each into FOUR places (memnodes.h method struct, memutils_internal.h
decls, allocator impl, `mcxt_methods[]` entry):

| Graft | Detail |
|---|---|
| `gp_malloc`/`gp_free` | vmem tracker (runaway detector / resgroups). Every BLOCK alloc/free in **aset.c only** (slab/generation historically use raw malloc — don't "fix" them). Needs `utils/gp_alloc.h`. |
| 4 accounting methods | `declare_accounting_root` / `get_current_usage` / `get_peak_usage` / `set_peak_usage` after `stats` in MemoryContextMethods. Subtree+peak system (`accountingParent`/`localAllocated`/`currentAllocated`/`peakAllocated` + `MEMORY_ACCOUNT_INC/DEC` hooks on alloc/free/realloc/reset). NOT redundant with upstream `mem_allocated` (per-context block bytes, no subtree, no peak). Consumers: tuplesort, explain_gp peak-memory. Get chunk sizes from PG16's alloc/free machinery, not `chunk->size`. |
| 2-arg `delete_context(context, parent)` | AllocSetDelete rolls the dying context's peak up into `parent`'s accounting root; mcxt.c `MemoryContextDeleteOnly` must capture `parent` BEFORE the unlink (PG16 sets parent=NULL first) and pass it through. All 7 layers must agree on the 2-arg shape. |
| `MemoryContextContainsGenericAllocation` + `AllocSetContains` | For MemTuples (pointer may be mid-chunk). AllocSetContains walks BLOCKS, not chunk headers — chunk-format independent, survives as-is. Keep the neutered `MemoryContextContains` too (unit-tested). |

Dropped: per-chunk `CDB_PALLOC_TAGS` (no room in MemoryChunk). PG16 also removed
`AssertArg` — convert stragglers to `Assert`.

No-build verify checklist: gp_malloc on every aset block alloc/free; the 4 accounting
methods in `mcxt_methods[MCTX_ASET_ID]`; delete 2-arg in all layers; no leftover
`AllocChunkData` refs; markers 0; braces balanced. Real verification = build +
memory-quota/tuplesort-peak/resgroup tests.

## Broad-sweep method for the long tail (~340 files after node+mcxt)

Validated approach for the 1–5-hunk files (the bulk):

- Batch subagents, ONE file per agent, each given the fixed rule set: HEAD = GGDB,
  `97d89101045` = PG16. (1) both-add-different → keep both; (2) same-code-changed →
  PG16 shape + GGDB intent; (3) deliberate GGDB behavior (MPP/AO/distribution/ORCA/
  "GPDB:" comments) → keep and re-graft; (4) pure PG16 cosmetic → theirs. Each agent
  reports `{markers_remaining, resolution, ggdb_preserved, pg16_adopted, uncertainty}`.
- Verify every batch by script: conflict markers == 0 AND brace-delta vs theirs == 0
  per file (a nonzero delta is usually noise from `'{'` char literals — check before
  panicking); then READ the uncertainty fields and spot-check a few files against both
  parents before `git add`.
- Order by hunk count: 1-hunk → 2-hunk → 3–5-hunk batches.
- **Big clusters (≥6 hunks, ~27 files) are EXCLUDED from sweeps — do individually**:
  `pg_upgrade/check.c`, `optimizer/plan/planner.c`, `catalog/storage.c`,
  `storage/buffer/bufmgr.c`, `utils/misc/guc.c`, `commands/vacuum.c`,
  `access/transam/xact.c`, `commands/tablecmds.c`, `bin/pg_dump/pg_dump.c`,
  `optimizer/plan/initsplan.c`, `commands/user.c`, `access/transam/twophase.c`,
  `access/heap/heapam.c`, `test/regress/pg_regress.c`, `executor/nodeHash.c`, plus
  `gram.y`, `portalmem.c`, catalog `.dat`, and the configure/Makefile build files.

Recurring PG16 rename churn the sweep applies everywhere: `RelFileNode` →
`RelFileLocator` (`rnode` → `rlocator`), `varatt.h` split out of postgres.h,
`AssertArg` gone. Also new in PG16 and worth watching: `pg_stat_io` (new pgstat kind —
check the GGDB pgstat_gp.c port from PG15), meson build files (GGDB builds with
autoconf; meson conflicts are low-risk take-theirs/defer), ICU as default locale
provider, extended SQL/JSON.

### Cross-file dependencies to honor during the sweep

- **Keep GGDB's 3-arg `smgropen(RelFileLocator, BackendId, SMgrImpl which)`** in
  smgr.h/smgr.c — all AO storage depends on it; already-resolved callers
  (xlogprefetcher.c, xlogutils.c, rel.h with SMGR_AO/SMGR_MD) use the 3-arg form.
  Do NOT take PG16's 2-arg.
- `lwlock.h` BuiltinTrancheIds (GGDB tranches) — resolve together with lwlock.c.
- `equivclass.c` asserts allow `RELOPT_DEADREL`/`RELOPT_OTHER_MEMBER_REL` — consistent
  with the pathnodes.h RELOPT_DEADREL graft.
- `StoreAttrDefault` stays in `catalog/heap.c` (don't follow any relocation).
- `dropcmds.c` keeps the `OBJECT_RESQUEUE`/`OBJECT_RESGROUP` switch cases.

## Deferred items from the resolution stage (all closed during bring-up)

- `portalmem.c`: GGDB `pg_cursor` SRF has an extra column (7 vs 6) — catalog-coupled,
  resolve with the catalog conflicts, not with the allocator cluster.
- aset.c debug-only `#ifdef`s not re-grafted: MPP-4923 thread-call asserts,
  `CDB_PALLOC_CALLER_ID`/`CDB_MCXT_WHERE` — add back if a debug build needs them.
- Accounting INC/DEC balance verified by analysis only — confirm at build with
  memory-quota / tuplesort-peak / resgroup tests.
- outfast.c/readfast.c bodies still carry PG15 field lists — reconcile at build phase.
- Test `.out`/`.sql` conflicts (≈70) deferred to the regress phase entirely.

---

# Post-merge bring-up (the campaign after the conflicts cleared)

The conflict resolution is maybe a third of the work. These are the classes the phased
bring-up surfaced — **most are recurring and will bite PG17 too.** Ordered by value.

## ⭐ The GUC-table relocation audit (the single most expensive PG16 lesson)

PG16 relocated the GUC definition tables `guc.c` → **`guc_tables.c`** and took the
**upstream `boot_val`s wholesale**, silently reverting ~15 GGDB overrides **with zero
conflict markers** (the file was clean-merged; the values just came from theirs).

- **Symptom:** a recursive-CTE query *hung for 13 minutes* (interconnect deadlock) plus
  wide plan-shape drift across the suite. **Root cause:** `enable_mergejoin`'s default
  flipped back to `on` → the PG16 planner picked a merge-join over a Broadcast Motion
  *inside a recursive-union recursive term* → the Sort re-drives the motion each iteration
  → interconnect deadlock. This is a **pre-existing GGDB executor limitation** that
  `enable_mergejoin=off` (a GGDB default) had merely been *costing out*; cm3 hangs too if
  you force the same plan. The fix was purely restoring the GUC default — **not** executor
  surgery.
- **The audit (do this proactively after any GUC-table move):** extract EVERY `boot_val`
  (bool/int/real/string) from the old file on the PG-N-green reference branch and from the
  new file on the merge branch, join on GUC name, and diff. Restore each GGDB override.
  `check_GUC_init` (cassert) catches *some* (bool: C-var `!= 0 && != boot_val`; enum needs
  the C-var static-init to equal boot_val, e.g. `plancat.c` `constraint_exclusion`, `jit.c`
  `jit_enabled`) but **NOT** cases where the C-var was also reset to the upstream value —
  so diff exhaustively, don't trust the assert.
- Restored in two commits: **planner** `ef55a525c13` (`enable_nestloop`/`enable_mergejoin`/
  `geqo`/`hot_standby`/`jit`→false, `logging_collector`→true, `from/join_collapse_limit`→20,
  `constraint_exclusion`→on, + `jit.c`/`plancat.c` C-vars) and **non-planner** `9a51407716f`
  (`max_connections`→200, `max_prepared_transactions`→50, `superuser_reserved_connections`→10,
  `work_mem`→32768, `max_locks_per_transaction`→128, `wal_sender_timeout`→300s,
  `wal_keep_size`→320MB, `log_filename`→`gpdb-*.csv`, `log_rotation_size`→1GB, + 4 C-var
  static inits for `check_GUC_init`). Restoring `enable_nestloop=off` also reshaped many
  plans back to the expected cm3 shape → *reduced* plan-shape drift suite-wide.
- **GENERALIZE:** whenever an upstream merge relocates or reshuffles a GUC table/entry,
  re-verify every GGDB `boot_val` survived. Silent behavior reversion, no markers. The
  PG-(N-1)-green docker container (a separate live cluster) is the ideal reference to diff
  live GUC values and plans against.

## ⭐ ORCA on PG16: the RTEPermissionInfo translator port (+ build flags)

PG16 moved `requiredPerms`/`checkAsUser`/`selectedCols` **out of `RangeTblEntry`** into a
separate **`RTEPermissionInfo`** list (`PlannedStmt->permInfos`, linked by
`rte->perminfoindex`). This is the PG16 analog of PG15's Value/SeqScan translator port: the
ORCA translator (`gpopt` `CTranslator*`) had to be re-grafted to build and thread perminfos
or ORCA crashes on GGDB's up-front `CheckRTPermissions`/`ExecCheckPermissions` bijection
assert. Ported `CContextDXLToPlStmt` perminfo list + Query→DXL threading of
`query->rteperminfos`; commit `69e8aca666e`. **Any merge that touches how permissions/RTEs
are represented is an ORCA-translator re-graft — budget for it.**

Two ORCA code bugs this surfaced in *core* regress (both real, not regen):

1. **`transformGroupedWindows` (orca.c) orphaned rteperminfos** — ORCA-only preprocessing
   splits a window+aggregate query into an outer window query `Q'` wrapping an aggregating
   subquery `Q''`; it moved base RTEs into `Q''` but left `qry->rteperminfos` on `Q'`
   (whose only RTE is now the synthetic `"Window"` wrapper) → orphan perminfo →
   `bms_num_members(indexset) == list_length(rteperminfos)` assert → SIGABRT on QD →
   **crash cascade** (dominated the first opt=on run: 33 tests). Trigger: any
   `SUM(SUM(c)) OVER (...) ... GROUP BY`. **FIX:** move it —
   `subq->rteperminfos = qry->rteperminfos; qry->rteperminfos = NIL;`. (Commit `85737e48a5f`.)
2. **`TranslateDXLTvf` coldeflist** — the RTE's `rtfunc` kept `funccolcount = 0` → `EXPLAIN
   VERBOSE` of a record-returning function with a column-def list → `invalid attnum`
   (ruleutils `expandRTE` truncates colnames to funccolcount). Execution was fine
   (deparse-only). **FIX:** populate the RTE rtfunc `funccol*` + `funccolcount`.

Build/answer-file specifics:
- **`gporca.mk` must `filter-out -Wshadow=compatible-local`** from BOTH `CXXFLAGS` and
  `BITCODE_CXXFLAGS` — PG16's `configure.ac` added it and it errors on ~18 harmless
  vendored-ORCA shadows. `g++ -Wno-shadow` does NOT override an explicit `-Wshadow=...` →
  you must *filter*, not append `-Wno-`.
- **`_optimizer.out` local regen is CORRECT** (ORCA-only files, not shared across the
  JIT/non-JIT base-`.out` jobs; db is always "regression", 3:1 seg count, addr/pid
  atmsort-stripped). The answer-file-regen skill's "regen from CI" rule is about SHARED
  base `.out`, not `_optimizer.out`. **GITIGNORE TRAP:** some tests' `_optimizer.out` is
  gitignored (`constraints`, `createdb`, `external_table`, `table_functions`, …) → opt=on
  falls back to the TRACKED base `.out`; before regenning, `git check-ignore
  expected/<t>_optimizer.out` and if ignored fix the base `.out` instead.

## ⭐ Missing-struct-field: `IndexVacuumInfo.heaprel` (a whole recurring class)

PG16 added `IndexVacuumInfo.heaprel` (btree page recycling needs it for the xmin horizon).
GGDB's `vacuum_ao.c` built `IndexVacuumInfo ivinfo = {0}` in `vacuum_appendonly_index` +
`scan_index` and never set it → `BTPageIsRecyclable(heaprel=NULL)` assert → segment SIGABRT
on any AO VACUUM that compacts a segfile and updates a btree index (a 19-test cluster).
**FIX:** `ivinfo.heaprel = <the AO relation>` (an AO table is the heap its indexes belong
to). Commit `096fe26bba7`. **GENERALIZE:** an upstream-added *required* struct field that
GGDB call sites zero-init (`= {0}`, `MemSet`) is a recurring class — after a merge, for each
struct whose definition upstream extended, grep GGDB code for `{0}`/`MemSet` initializers of
that struct and set the new field.

## ⭐ Upstream macro removal → GGDB dead `#ifdef` (`HAVE_UNIX_SOCKETS`)

PG16 **removed the `HAVE_UNIX_SOCKETS`** configure macro (Unix sockets are now
unconditional). GGDB's `internal_client_authentication()` wrapped its entry-db /
QE-at-coordinator AF_UNIX `FakeClientAuthentication` bypass in `#ifdef HAVE_UNIX_SOCKETS` →
the whole block **compiled out** → internal `[local]` connections fell through to normal
`pg_hba` and were rejected ("failed to acquire resources … entry db"). **FIX:** drop the
dead `#ifdef` (unconditional AF_UNIX). Same dead guard cleaned in `cdbutil.c`/`ic_udpifc.c`/
`pgstat.c`. Commit `a2503257ce2`. **GENERALIZE:** after a merge, for every configure macro
upstream *removed* (diff `pg_config.h.in`), grep `#ifdef <MACRO>` — every GGDB use is now
silently dead code.

## Executor / grouping bugs from the node-support migration + EC dedup

- **setrefs.c multi-DQA:** PG16's node-support migration dropped the `equal_ignore` on
  `Aggref.agg_expr_id` (and `SubPlan.is_initplan`) → a HAVING/qual multi-DQA combine-aggref
  can't `equal()`-match its stamped partial aggref. **FIX:** re-annotate those fields
  `equal_ignore` in the node header. Commit `987e469fedc`. (Node fields that used to be
  `equal_ignore` in the hand-written `equalfuncs.c` must become header annotations — audit
  them during the node layer.)
- **`ExecInitTupleSplit` off-by-one:** `all_input_attr_bms` was 0-based where it should be
  1-based → nulled the last passthrough column; masked before, *unmasked* by PG16's EC
  group-key dedup. **FIX:** 1-based. Commit `a72b31481b7`.
- **All-constant GROUP BY:** PG16 drops the sole provably-constant GROUP BY column →
  `cdbgroupingpaths` two-stage split's first-stage plain Agg projects a passthrough Var from
  an empty-input segment → `execExprInterp.c` crash. **FIX:** skip the multi-stage split for
  a degenerate all-constant GROUP BY (single-stage). Commit `1c169fde348`.

## Deparse drift (ruleutils) → base `.out` regens

PG16 `ruleutils` emits **unqualified** column refs in view-defs (`SELECT e FROM cte` not
`SELECT cte.e`; `WHERE id < 790` not `WHERE people.id < 790`; `ARRAY[fname,lname]` not
`[people.fname,…]`). `pg_get_viewdef` output is **optimizer-independent** → regen the
TRACKED base `.out` (fixes both matrices at once; verify identical opt on/off first).
Commit `205e76437a6`.

## Test-harness merge artifacts & direct-pg_regress run defects (not code, but they lie)

- **pg_regress pass/fail-by-psql-exit:** the PG16 merge reworked pg_regress result-reporting
  and nested it so pass/fail was decided by the psql **exit status** *before* checking the
  diff. Tests that intentionally exit psql `!= 0` (gp_connections' final `\connect` to a
  segment = exit 2) then "failed" with an **empty regression.diffs**. Upstream/cm3 decide
  from the DIFF alone. **FIX:** restore diff-only at both report sites (commit `b65f3d3864a`;
  strictly more lenient — can't break a passing test).
- Running `pg_regress` **directly** (not via `make installcheck-good`) in the container:
  `export USER=gpadmin LOGNAME=gpadmin` (docker `exec -u` leaves them empty → gpconfig/gpstop
  tests fail "USER must be set") and pass `--load-extension=gp_inject_fault` (Makefile.global
  injects it when `enable_debug_extensions=yes`; direct runs omit it → "gp_inject_fault does
  not exist" fails ALL fault tests). A killed run leaves leftover databases + fault
  injections that false-fail *later* runs — drop non-system dbs + reset all faults (or
  recreate the cluster) before a clean measure. Kill an in-container run as root:
  `sudo docker exec <c> pkill -9 -f pg_regress`.

## Still deferred at campaign end (follow-ups, not PG16-merge blockers)

- **qp_with_clause DML+CTE motion deadlock:** PG16 focuses a ~1-row correlated aggregate to
  a single node (`Gather 3:1 → Broadcast 1:3`) instead of cm3's distributed `Redistribute
  3:3` → contention-fragile under the full-regression interconnect load (completes in
  isolation, deadlocks in the parallel group). Deep MPP locus/planner work; **not** a GUC
  fix. Deferred.
- **AO-truncate mirror WAL-replay PANIC** (`cdbappendonlyxlog.c` → `XLogAOSegmentFile` →
  `log_invalid_page`) — a LATENT TIMING FLAKE **shared with cm3** (identical code): a mirror
  replays an AO-truncate of a missing segfile after reaching consistency → the upstream guard
  PANICs where GGDB expects self-heal. **By design; do NOT "fix" the assert** (grep for a
  validating test first — this bit PG14 too).
- **autovacuum fault-never-fires** hang (`gp_wait_until_triggered_fault('auto_vac_worker')`
  never fires — a PG15+ autovacuum-behavior-change class).
