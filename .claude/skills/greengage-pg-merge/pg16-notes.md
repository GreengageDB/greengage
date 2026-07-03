# PG16 merge notes (branch claude-merge-4)

> Status: this merge is IN PROGRESS at time of writing (conflict-resolution stage; node
> layer and memory-context cluster are done, a ~300-file long tail remains), so this file
> covers the classes known so far. General methodology lives in [SKILL.md](SKILL.md);
> PG15 traps that still apply (dup OIDs after genbki, WAL-section order, clean-merge
> signature traps, init_file regen) are in [pg15-notes.md](pg15-notes.md).

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

## Deferred / open items (as of the resolution stage)

- `portalmem.c`: GGDB `pg_cursor` SRF has an extra column (7 vs 6) — catalog-coupled,
  resolve with the catalog conflicts, not with the allocator cluster.
- aset.c debug-only `#ifdef`s not re-grafted: MPP-4923 thread-call asserts,
  `CDB_PALLOC_CALLER_ID`/`CDB_MCXT_WHERE` — add back if a debug build needs them.
- Accounting INC/DEC balance verified by analysis only — confirm at build with
  memory-quota / tuplesort-peak / resgroup tests.
- outfast.c/readfast.c bodies still carry PG15 field lists — reconcile at build phase.
- Test `.out`/`.sql` conflicts (≈70) deferred to the regress phase entirely.
