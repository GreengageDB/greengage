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
| 15 | MERGE — MATCHED / BY SOURCE actions | ✅ (dist-key UPDATE actions gated — no SplitUpdate for MERGE) | ✅ **implemented** via wholerow junk column (tested, incl. isolation2) | ↩ |
| 15 | UNIQUE NULLS NOT DISTINCT | ✅ | ❓ (AO unique-index machinery exists; combo untested) | n/a |
| 15 | ALTER TABLE SET ACCESS METHOD | ✅ | ✅ all directions heap↔ao_row↔ao_column, well tested | n/a |
| 15 | COPY HEADER MATCH | ✅ | ✅ (❓ untested) | n/a |
| 15 | CLUSTER on partitioned | ✅ | 🚫 AO clusters only via B-tree sort-rewrite | n/a |
| 16 | COPY DEFAULT 'marker' | ✅ | ✅ (❓ untested) | n/a |
| 16 | SQL/JSON constructors, IS JSON | ✅ | ✅ (expressions, AM-agnostic) | ↩ (scalar-translator catch-all) |
| 17 | MERGE … RETURNING / merge_action() | ✅ | ✅ **implemented** (tested incl. OLD/NEW) | ↩ |
| 17 | MERGE WHEN NOT MATCHED BY SOURCE | ✅ | ✅ **implemented** | ↩ |
| 17 | JSON_TABLE | ✅ | ✅ | 🐛 **UB**: non-lateral RTE_TABLEFUNC hits `GPOS_ASSERT + __builtin_unreachable()` in `UnsupportedRTEKind()`; cassert builds accidentally fall back, production builds = UB. Same hole: XMLTABLE, trigger transition tables (RTE_NAMEDTUPLESTORE). |
| 17 | COPY ON_ERROR / LOG_VERBOSITY | ✅ (GPDB copy.c port; coexists with SREH, no mutual-exclusion validation) | ✅ via multi_insert (❓ untested) | n/a |
| 17 | EXPLAIN (MEMORY, SERIALIZE) | ✅ | ✅ | n/a |
| 18 | RETURNING NEW.* | ✅ | ✅ | ↩ (any RETURNING falls back) |
| 18 | RETURNING OLD.* — UPDATE | ✅ (gated for split updates) | ✅ **implemented** via wholerow junk column (split-update case still gated) | ↩ |
| 18 | RETURNING OLD.* — DELETE | ✅ | 💥 AM error from `fetch_row_version` (pre-existing, tested; wholerow route applicable as follow-up) | ↩ |
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

### 1. ✅ IMPLEMENTED on AO — the wholerow route (MERGE MATCHED, RETURNING OLD)

The old tuple is carried through the plan instead of fetched by TID (which would need a blockdir):

- `add_row_identity_columns()` (`appendinfo.c`) adds the RECORD wholerow junk Var for CMD_MERGE
  on an AO target when any MATCHED / NOT MATCHED BY SOURCE action exists (or RETURNING references
  OLD), and for CMD_UPDATE on AO when RETURNING references OLD (`contain_vars_returning_old()`,
  `var.c`).  INSERT-only MERGE skips the overhead.
- `ExecMergeMatched` restores the old tuple from the junk column via
  `ExecAORestoreOldTupleFromWholerow()` (`nodeModifyTable.c`) instead of `fetch_row_version`;
  the same helper serves the AO UPDATE old-slot path.  No preptlist expansion is needed for
  MERGE: action projections read the old slot directly.
- AO TM_SelfModified in `ExecMergeMatched` maps to the cardinality violation ("cannot affect row
  a second time"): the AO visimap reports SelfModified only for same-command modifications
  (`tmfd.cmax` = current cid, `tmfd.xmax` never filled).
- MERGE UPDATE actions that modify a distribution key column are rejected at plan time
  (`merge_updates_distribution_key()` in `cdbpath.c` — note: action tlist resnos are RENUMBERED
  by preprocess_targetlist; the real attnos live in `action->updateColnos`).  This also fixed the
  silent heap mis-distribution found in the audit.  Self-assignment (`SET k = t.k`) is allowed.
- MERGE update/delete actions on AO under SERIALIZABLE are rejected (same visimap limitation as
  plain UPDATE/DELETE).
- Tests: `gp_pg15_merge_regress` §7 (both AMs, RETURNING merge_action()/OLD/NEW, cardinality,
  gates), `returning_gp`, isolation2 `merge_ao` (ExclusiveLock serialization of concurrent MERGE).
- Follow-up left open: `DELETE … RETURNING` on AO (same wholerow route would work; today it
  errors via the AM callback), and cross-partition MERGE + RETURNING OLD (errors via AM callback).

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
