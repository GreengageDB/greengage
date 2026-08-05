# PG13→PG18 new statement constructions: AO/AOCS and ORCA support matrix

Audited on the PG 18.4-based line (claude-merge-7). For every statement-level feature upstream
added in PostgreSQL 13–18: does it work on append-optimized tables (AO row / AOCS column), and
does ORCA plan it or fall back? Line numbers are as of the audit; treat them as anchors, not
contracts.

## How features fail on AO (the mechanics)

Most gaps trace to a handful of stubbed tableam callbacks in `appendonlyam_handler.c` /
`aocsam_handler.c`, not to per-statement gates:

| callback | behavior | breaks |
|---|---|---|
| `tuple_fetch_row_version` | ereport "feature not supported on appendoptimized relations" | MERGE MATCHED, DELETE RETURNING, RETURNING OLD, EvalPlanQual refetch, ON CONFLICT DO UPDATE |
| `tuple_lock` | bare elog (copy-pasted "speculative insertion" text) | MERGE TM_Updated retry, ON CONFLICT DO UPDATE, LockRows (unreached — parse-time degrade) |
| `tuple_insert_speculative` / `tuple_complete_speculative` | bare elog | ON CONFLICT with any arbiter index |
| `tuple_satisfies_snapshot` | ereport | ExecCheckTupleVisible (ON CONFLICT under RR/SSI) |
| `scan_set_tidrange` (absent) | planner never builds TidRangeScan | ctid range quals degrade to seqscan filter |

Key structural facts:
- AO TID fetch requires a **block directory**, which exists only when the table has an index.
  Any general `fetch_row_version` implementation is therefore conditional on indexes.
- AO DML takes an **ExclusiveLock upgrade** on the QD (`CdbTryOpenTable`, `table.c`), serializing
  concurrent writers — concurrency-path landmines (TM_Updated handling, tmfd->xmax uninitialized)
  are mostly unreachable outside utility mode, but the code paths remain.
- The planner substitutes the old tuple for AO UPDATE via **full-targetlist expansion**
  (`preptlist.c`, CMD_UPDATE only) and, for inheritance, a **wholerow junk attr**
  (`add_row_identity_columns`, `appendinfo.c`). Neither is wired up for CMD_MERGE.

ORCA has **no AO gate at all** — it plans AO/AOCS scans and DML normally
(`CTranslatorRelcacheToDXL.cpp`, `ErelstorageAppendOnlyRows/Cols`); AO gaps above apply
identically under both optimizers. ORCA statement gaps are separate, listed below.

## The matrix

Status legend: ✅ works · 🚫 clean explicit gate · 💥 incidental/late error (bad UX) ·
🐛 wrong behavior · ❓ untested · ↩ ORCA falls back to planner cleanly.

| PG | Feature | Heap+MPP | AO/AOCS | ORCA |
|---|---|---|---|---|
| 13 | FETCH FIRST … WITH TIES | ✅ | ✅ (planner-level) | ↩ |
| 13 | ALTER COLUMN … DROP EXPRESSION | ✅ | ✅ (forces rewrite on AO) | n/a (DDL) |
| 14 | CTE SEARCH / CYCLE | ✅ | ✅ | ↩ (via WITH RECURSIVE gate) |
| 14 | GROUP BY DISTINCT | ✅ | ✅ | ↩ (explicit; silent-ignore would be wrong results) |
| 14 | TID range scans | ✅ | 🐛 silent: no TidRangeScan path; ctid quals filter AOTupleId-encoded ctids meaninglessly | planner-level |
| 14 | DETACH PARTITION CONCURRENTLY | ❓ zero test coverage in tree | ❓ no gate | n/a (utility) |
| 14 | REINDEX new forms | ✅ (CONCURRENTLY globally downgraded to non-concurrent NOTICE — GPDB-wide, not AO) | same | n/a |
| 14 | Multirange types | ✅ | ✅ | ↩ for multirange-returning aggs; columns/ops unverified |
| 15 | MERGE — NOT MATCHED INSERT | ✅ (co-location required; redistribution/replicated targets error) | ✅ (tested) | ↩ |
| 15 | MERGE — MATCHED / BY SOURCE actions | ✅ | 💥 data-dependent AM error (succeeds on empty match!) | ↩ |
| 15 | UNIQUE NULLS NOT DISTINCT | ✅ | ❓ (AO unique-index machinery exists; combo untested) | n/a |
| 15 | ALTER TABLE SET ACCESS METHOD | ✅ | ✅ all directions heap↔ao_row↔ao_column, well tested | n/a |
| 15 | COPY HEADER MATCH | ✅ | ✅ (❓ untested) | n/a |
| 15 | CLUSTER on partitioned | ✅ | 🚫 AO clusters only via B-tree sort-rewrite | n/a |
| 16 | COPY DEFAULT 'marker' | ✅ | ✅ (❓ untested) | n/a |
| 16 | SQL/JSON constructors, IS JSON | ✅ | ✅ (expressions, AM-agnostic) | ↩ (scalar-translator catch-all) |
| 17 | MERGE … RETURNING / merge_action() | ✅ | 💥 unreachable (same MATCHED blocker) | ↩ |
| 17 | MERGE WHEN NOT MATCHED BY SOURCE | ✅ | 💥 same | ↩ |
| 17 | JSON_TABLE | ✅ | ✅ | 🐛 **UB**: non-lateral RTE_TABLEFUNC hits `GPOS_ASSERT + __builtin_unreachable()` in `UnsupportedRTEKind()`; cassert builds accidentally fall back, production builds = UB. Same hole: XMLTABLE, trigger transition tables (RTE_NAMEDTUPLESTORE). |
| 17 | COPY ON_ERROR / LOG_VERBOSITY | ✅ (GPDB copy.c port; coexists with SREH, no mutual-exclusion validation) | ✅ via multi_insert (❓ untested) | n/a |
| 17 | EXPLAIN (MEMORY, SERIALIZE) | ✅ | ✅ | n/a |
| 18 | RETURNING NEW.* | ✅ | ✅ | ↩ (any RETURNING falls back) |
| 18 | RETURNING OLD.* — UPDATE | ✅ (gated for split updates) | 🐛 **silent all-NULL OLD values** (all-NULL oldSlot flows into RETURNING projection) | ↩ |
| 18 | RETURNING OLD.* — DELETE | ✅ | 💥 AM error from `fetch_row_version` (pre-existing, tested) | ↩ |
| 18 | Virtual generated columns | ✅ | ✅ in principle (❓ AOCS: vpinfo slot per attno, pg_attribute_encoding doesn't filter virtual — untested) | ✅ pre-expanded in `orca.c` |
| 18 | Temporal PK/UNIQUE WITHOUT OVERLAPS | ✅ | ❓ likely broken: forces GiST; AO unique enforcement (`index_fetch_tuple_exists`) is btree/blkdir-oriented; AO unique gate doesn't consider GiST | n/a |
| 18 | FOR PORTION OF | — not merged (absent from tree) | — | — |
| 18 | COPY REJECT_LIMIT | ✅ | ✅ (❓ untested) | n/a |
| 18 | NOT NULL constraints (named / NOT VALID) | ✅ | ✅ | safe (convalidated filter) |
| 18 | RTE_GROUP planner step | ✅ | ✅ | ✅ pre-flattened in `orca.c` (FROM-less grouping sets deliberately fall back) |

Adjacent pre-PG13 features re-audited because MERGE/RETURNING paths lean on them:

| Feature | AO/AOCS status |
|---|---|
| ON CONFLICT (any arbiter) | 💥 bare elog "speculative insertion not supported" from AM stub; no early gate, no errcode |
| SELECT … FOR UPDATE | degraded by design: parse-time ExclusiveLock table lock, no LockRows |
| WHERE CURRENT OF | 🚫 "is not simply updatable" |
| Row UPDATE/DELETE triggers | 🚫 rejected at CREATE TRIGGER |
| TABLESAMPLE | 🚫 "Sampling is only supported in heap tables" (untested) |
| Index-only scans | off by design (relallvisible kept 0) |
| Logical replication of AO | 🐛 silent: AO rmgr has NULL decode callback; publication accepts AO tables and replicates nothing |
| Parallel seqscan | 🚫 elog stub |

## Dispositions and implementation routes

### 1. Implementable on AO — the wholerow route (MERGE MATCHED, RETURNING OLD)

The old tuple can be carried through the plan instead of fetched by TID (which needs a blockdir):

- Extend the wholerow junk-attr addition in `add_row_identity_columns()` (`appendinfo.c`) to
  CMD_MERGE for AO targets (today: CMD_UPDATE + inheritance only), and the AO full-targetlist
  expansion in `preprocess_targetlist()` (`preptlist.c`) to CMD_MERGE.
- The executor deform machinery already exists (`ExecModifyTable`'s AO wholerow branch in
  `nodeModifyTable.c`); `ExecMergeMatched` already has an `oldtuple != NULL` path.
- Also needed: give `ExecMergeMatched` the AO TM_SelfModified exemption `ExecUpdate` already has
  (AO sets only `tmfd->cmax`; `tmfd->xmax` is uninitialized), and gate MERGE UPDATE of the
  distribution key (MERGE never builds a SplitUpdate — see caveat below).
- Same route gives `UPDATE … RETURNING OLD.*` real values on AO: only the planner-side junk-col
  addition is missing.

⚠ Caveat found during the audit, heap too: **MERGE UPDATE that changes the distribution key is
silently mis-distributing rows** — `is_split_update` is only computed for CMD_UPDATE, MERGE
performs an in-place update with `GpIdentity.segindex` passed as segid so the wrong-segment check
cannot fire. Needs its own fix (gate or split-MERGE).

### 2. Implementable on AO — ON CONFLICT

Feasible because the pieces line up: an arbiter requires a unique index ⇒ blockdir exists ⇒ TID
fetch possible; the ExclusiveLock upgrade serializes writers ⇒ the speculative protocol
degenerates to check-then-insert (`index_fetch_tuple_exists` is already the AO unique check);
`tuple_lock` can be fetch-and-return. Until then the stubs should ereport properly.

### 3. Test-first (unknown correctness)

WITHOUT OVERLAPS on AO (GiST uniqueness), virtual generated columns on AOCS (encoding/vpinfo
interaction), UNIQUE NULLS NOT DISTINCT on AO, COPY ON_ERROR/REJECT_LIMIT on AO (+ SREH
interaction and the missing ON_ERROR-vs-SREH mutual-exclusion validation), multirange columns
under ORCA, DETACH PARTITION CONCURRENTLY (untested even on heap).

### 4. Keep as-is, documented

TID/TID-range scans (AO ctids are AOTupleIds; a range scan could be implemented over segfile
offsets but has little value), TABLESAMPLE (ANALYZE has its own AO sampler), index-only scans,
parallel AO seqscan, REINDEX CONCURRENTLY (GPDB-wide downgrade), FOR PORTION OF (not merged).
Logical replication of AO deserves at least a publication-time gate — today it silently
replicates nothing.

### ORCA follow-ups

The one real defect is `UnsupportedRTEKind()`'s default arm (`CTranslatorQueryToDXL.cpp`):
missing cases for RTE_TABLEFUNC / RTE_NAMEDTUPLESTORE / RTE_RESULT / RTE_GROUP end in
`__builtin_unreachable()` — same latent-crash shape the CMD_MERGE fallback fix (`b223ed37429`)
removed for statement types. Everything else PG13-18 falls back cleanly; there are no
`optimizer_enable_*` GUCs for these features (all unconditional), and several fallback reasons
(GROUP BY DISTINCT, WITH TIES, multirange-agg, TABLESAMPLE, ON CONFLICT) are asserted by no test
because nothing enables `optimizer_trace_fallback` for them.
