# Merge PostgreSQL 14 into Greengage

## Summary

Brings the upstream PostgreSQL 14 commit range `d259afa736..e1c1c30f635` into the
Greengage MPP fork and carries it to a green regression matrix. The raw merge
(`8597e91cda3`) resolved ~700 conflict files; **356 follow-up commits** then take the
tree through every bring-up phase — compile → mock unit tests → `initdb`/bootstrap →
the JIT × {ORCA, Postgres-planner} regression matrix — fixing each distinct class of
breakage, and add new regression coverage plus reusable tooling. Scale layered on the
raw merge: 636 files, +58k/−39k.

## 1. Merge & conflict resolution

Resolved semantically — adopt the upstream API shape, then re-graft the GGDB/MPP logic;
never blind `ours`/`theirs`. Used both merge parents (`^2` = upstream PG14, `^1` =
pre-merge GGDB) and Apache Cloudberry's PG14+Greenplum tree as a *shape* reference.
Largest structural re-grafts:

- **PGXACT elimination** → dense `PROC_HDR` arrays; GGDB distributed-snapshot and
  reader/writer XID sharing re-grafted onto `ProcGlobal->xids[pgxactoff]`.
- **`relkind` → `objtype`** enum in CTAS / RefreshMatView / IntoClause.
- **`copy.c` split** (copyfrom/copyto) — kept GGDB's monolithic `copy.c`, re-grafted
  only the protocol change.
- **Grammar**: PG14 `bare_label_keyword`/`BareColLabel` superseding GGDB's `ColLabelNoAs`.
- Catalog/genbki tightening (oid_symbol rejection, per-catalog DECLAREs, OID-range
  collisions), `errstart`/`errstart_cold`, long-lived `WaitEventSet`, `GlobalVis*`
  horizon API, bulk `pg_attribute` insert + `attcompression`.

## 2. Bring-up by phase

- **Compile/link** — mechanical API-shape fixes.
- **Mock unit tests** (`make unittest-check`) — `errstart`/`errstart_cold` mock split,
  new-GUC coverage lists, `mock.mk` link order; the mock suites pass.
- **initdb/bootstrap** — catalog/BKI/planner regressions neither the compiler nor the
  mocks catch: BKI single-quote convention, genbki `PGUID` substitution, catalog
  header order, missing index DECLAREs, `pg_proc.dat` last-wins duplicates, row-identity
  wiring for UPDATE/DELETE.
- **Regression** — answer-file reconciliation + the fixes below.

## 3. MPP/ORCA bug fixes (the dominant class)

PG14 added fields/types that ORCA's translator silently omitted or mishandled, causing
**crashes or wrong results only under `optimizer=on`** (ADB's default). Fixed:

- UPDATE `ModifyTable.updateColnosLists` (segment SIGSEGV) + partitioned U/D fallback
- `Aggref.aggno/aggtransno` — every aggregate returned the first one's value
- `SubscriptingRef.refrestype` — "cache lookup failed for type 0"
- `GROUP BY DISTINCT` ignored (37 vs 9 grouping-set rows) → planner fallback
- `range_agg` `anymultirange` unresolved ("type 4537 is not a multirange type") → fallback
- correlated EXISTS/scalar subplan in the target list (cdbllize crash + lost correlation)
- CTAS/matview `DISTRIBUTED BY (<aggregate>)` (Motion-on-Motion, MIN/MAX planagg guard)
- multi-DQA + FILTER, split-update target list, binary-dispatch JoinExpr `join_using_alias`
- matview `REFRESH … CONCURRENTLY` (prefixed transient heap, whole-row `.*` refs)
- COPY/sequence wire protocol (`pq_getmessage` maxlen), nested InitPlan across a Motion
- HA: coordinator SyncRep QD exemption, `pg_rewind` segment `gp_dbid` preservation
- assert-build crashers (syncrep ×2, AO-update slot, over-strict ORCA asserts, …)

## 4. Test reconciliation & the CI matrix

CI runs the same `expected/*.out` across four jobs — {JIT, non-JIT} × {ORCA, Postgres
planner}. Principles applied (and captured in the tooling): regenerate answer files only
from the *failing job's CI result* (local runs bake env-specific values like db name /
segment count); a shared base `.out` is used by ORCA too, so regenerating it to the
Postgres-planner output breaks ORCA; JIT-only EXPLAIN output (the `Settings:` line) must
not be baked into a file a non-JIT job compares. Representative fixes: `explain`
stabilized via `SET jit = off` + boot-default cost GUCs; `table_functions`/`direct_dispatch`
matchsubs; `_optimizer.out` splits for shared optimizer-dependent tests.

## 5. New regression coverage

- **`gp_pg14_merge_regress`** — lock-in test: one minimal reproducer per MPP/ORCA bug
  class above, so a revert re-introduces the crash/wrong-result. Runs under both optimizers.
- **`gp_pg14_features`** — MPP/ORCA coverage for under-tested PG14 features (GROUP BY
  DISTINCT, recursive-CTE SEARCH/CYCLE, multirange, subscripting in SELECT/WHERE/JOIN,
  `extract`/`date_bin`, `range_agg`, NOT MATERIALIZED). **This test caught the two new
  ORCA bugs** (GROUP BY DISTINCT and range_agg), both fixed in this PR.

## 6. Tooling / docs

Seven reusable Claude Code skills under `.claude/skills/` distilled from the campaign
(build, regress-tests, answer-file-regen, cluster-ops, debug, internals, **pg-merge**),
replacing two ad-hoc merge-notes files.

## Verification

- Compile + mock unit-test suites green.
- JIT × ORCA and JIT × Postgres-optimizer regression jobs green; the deterministic
  opt=off/ORCA long tail is closed.
- Both new tests pass under `optimizer=off` and `optimizer=on`.

## Known remaining (not regressions)

- isolation2 HA/fault-injection specs (`fts_session_reset`, `segwalrep/*`,
  `pg_rewind_*`) fail **only in CI** because the small gpdemo runs **out of memory**
  during the full ~250-spec schedule — OOM events time-correlate with the failures and
  all specs pass locally in isolation. CI cluster-capacity, not code.
- `unnest(anymultirange)` was not carried into the branch (no C `multirange_unnest` /
  catalog entry) — the new test covers multirange via the constituent-range operators;
  porting it is a candidate follow-up.
