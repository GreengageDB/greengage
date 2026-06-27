-- Regression guards for PostgreSQL 14 -> 15 feature coverage and bugs fixed
-- during the PG15 -> Greengage merge.  Each section is a minimal MPP reproducer
-- for a new PG15 planner/executor/DML feature or for an MPP/ORCA-path defect the
-- merge introduced and we fixed; reverting a fix re-introduces a crash, a clean
-- error turning into silent corruption, or a wrong result here.  The test is
-- intentionally data-only (no EXPLAIN) so the output is stable across the
-- optimizer x JIT CI matrix, and it is meant to run under BOTH optimizer=off and
-- optimizer=on (ORCA falls back to the planner for MERGE/window run conditions).

create schema merge_pg15;
set search_path = merge_pg15, public;

-- ============================================================
-- 1. PG15 WindowAgg run condition (monotonic window pushdown,
--    upstream 9d9c02ccd2).  A constant-per-partition window
--    aggregate (count OVER (PARTITION BY ...) with no frame) is
--    MONOTONICFUNC_BOTH; an EQUALITY qual on it builds a run
--    condition whose operator previously came back as opno=0
--    ("cache lookup failed for function 0").  Guards 06423bc8457.
-- ============================================================
create table w_rc (a int, b int) distributed by (a);
insert into w_rc values (1,10),(1,20),(1,30),(2,10),(2,20),(3,10),(3,20),(3,30),(3,40);
select a, b, cnt from (
  select a, b, count(*) over (partition by a) as cnt from w_rc
) q where cnt = 3 order by a, b;

-- ============================================================
-- 2. PG15 MERGE (a2f3ead6320 MPP wiring): a co-located MERGE
--    (target and source DISTRIBUTED BY the join key) must run all
--    three action kinds correctly across segments and report the
--    right "MERGE N" tag.
-- ============================================================
create table m_tgt (k int, v int) distributed by (k);
create table m_src (k int, v int) distributed by (k);
insert into m_tgt select i, i from generate_series(1, 10) i;
insert into m_src select i, i * 100 from generate_series(6, 15) i;
merge into m_tgt t using m_src s on t.k = s.k
  when matched and s.k <= 8 then update set v = t.v + s.v
  when matched then delete
  when not matched then insert (k, v) values (s.k, s.v);
select k, v from m_tgt order by k;

-- ============================================================
-- 3. MERGE distributed-target routing: a MERGE that would move a
--    matched target row to another segment (it updates the target
--    distribution key, and the source is distributed on the join
--    key so the target cannot stay put) must raise a clean error
--    instead of silently corrupting rows.  Guards cdbpath.c
--    create_motion_path_for_merge / can_elide_explicit_motion.
-- ============================================================
create table mr_tgt (a int, b int) distributed by (a);
create table mr_src (a int, b int) distributed by (b);
insert into mr_tgt select i, i from generate_series(1, 5) i;
insert into mr_src select i, i from generate_series(1, 5) i;
merge into mr_tgt t using mr_src s on t.b = s.b
  when matched then update set a = s.a + 100;
select a, b from mr_tgt order by a, b;   -- unchanged: the MERGE was rejected

-- ============================================================
-- 4. MERGE with a SubPlan in a WHEN action targetlist.  The
--    subplan is referenced only from a MERGE action, so it must be
--    initialized per slice on the QE; otherwise every segment
--    SIGSEGVs in heap_form_tuple.  Guards getLocallyExecutableSubplans
--    walking mergeActionLists.
-- ============================================================
create table ms_tgt (k int, v int) distributed by (k);
create table ms_src (k int, v int) distributed by (k);
create table ms_ref (k int, w int) distributed by (k);
insert into ms_tgt select i, i from generate_series(1, 5) i;
insert into ms_src select i, i from generate_series(1, 7) i;
insert into ms_ref select i, i * 1000 from generate_series(1, 5) i;
merge into ms_tgt t using ms_src s on t.k = s.k
  when matched then update set v = (select max(w) from ms_ref)
  when not matched then insert (k, v) values (s.k, (select min(w) from ms_ref));
select k, v from ms_tgt order by k;

-- ============================================================
-- 5. MERGE on a DISTRIBUTED REPLICATED target is deliberately not
--    supported and must reject cleanly (not corrupt every replica).
-- ============================================================
create table mrep (a int, b int) distributed replicated;
insert into mrep values (1, 1);
merge into mrep t using ms_src s on t.a = s.k
  when matched then update set b = s.v;

-- ============================================================
-- 6. MERGE into a co-located partitioned target: WHEN MATCHED
--    UPDATE of a non-partition-key column and WHEN NOT MATCHED
--    INSERT that tuple-routes into the correct child partition.
-- ============================================================
create table mp_tgt (k int, v int) distributed by (k)
  partition by range (k) (start (0) end (40) every (10));
create table mp_src (k int, v int) distributed by (k);
insert into mp_tgt select i, i from generate_series(0, 29) i;
insert into mp_src select i, i * 10 from generate_series(25, 34) i;
merge into mp_tgt t using mp_src s on t.k = s.k
  when matched then update set v = s.v
  when not matched then insert (k, v) values (s.k, s.v);
select k, v from mp_tgt order by k;

-- ============================================================
-- 7. MERGE into an append-optimized target.  AO supports INSERT, so
--    a WHEN NOT MATCHED action appends the not-matched source rows on
--    the segment where their distribution key hashes (AO update/delete
--    via MERGE is a separate, currently unsupported, path).
-- ============================================================
create table mao_tgt (k int, v int) with (appendonly = true) distributed by (k);
create table mao_src (k int, v int) distributed by (k);
insert into mao_tgt select i, i from generate_series(1, 5) i;
insert into mao_src select i, i * 100 from generate_series(1, 10) i;
merge into mao_tgt t using mao_src s on t.k = s.k
  when not matched then insert (k, v) values (s.k, s.v);
select k, v from mao_tgt order by k;

-- ============================================================
-- 8. PG15 enable_group_by_reordering (default on): the planner may
--    reorder GROUP BY keys by ndistinct/sort cost.  The reordered
--    grouping (including multi-DQA) must still produce correct
--    results across the redistribute/two-stage aggregation path.
-- ============================================================
create table gbr (a int, b int, c int) distributed by (a);
insert into gbr select i % 2, i % 4, i from generate_series(1, 40) i;
select a, b, count(*) as n, count(distinct c) as dc, sum(c) as s
from gbr group by a, b order by a, b;

-- ============================================================
-- 9. PG15 UNIQUE NULLS NOT DISTINCT.  On a distributed table the
--    distribution key must be a subset of the unique key.  Verify the
--    indnullsnotdistinct flag is parsed and stored (and thus the
--    constraint is dispatched to the segments): the default unique
--    index is NULLS DISTINCT (f), the explicit one NULLS NOT DISTINCT (t).
-- ============================================================
create table un_d  (a int, b int, unique (a, b)) distributed by (a);
create table un_nd (a int, b int, unique nulls not distinct (a, b)) distributed by (a);
select c.conrelid::regclass::text as tbl, i.indnullsnotdistinct as nulls_not_distinct
from pg_constraint c join pg_index i on i.indexrelid = c.conindid
where c.conrelid in ('un_d'::regclass, 'un_nd'::regclass) and c.contype = 'u'
order by 1;

-- ============================================================
-- 10. PG15 SQL/JSON constructors (JSON_OBJECT / JSON_ARRAY) and the
--     IS JSON predicate, evaluated segment-side over a distributed
--     table so the EEOP_JSON_CONSTRUCTOR / EEOP_IS_JSON opcodes are
--     driven (and JIT-compiled under the JIT matrix, jit_above_cost=0).
-- ============================================================
create table js_t (id int, k text, v int, doc text) distributed by (id);
insert into js_t values (1,'x',10,'{"a":1}'), (2,'y',20,'not json'), (3,'z',30,'[1,2,3]');
select id,
       json_object(k value v) as obj,
       json_array(id, v) as arr,
       doc is json as is_json
from js_t order by id;

-- ============================================================
-- 11. Hashed ScalarArrayOp (large IN-list past the hash threshold)
--     evaluated segment-side, driving EEOP_HASHED_SCALARARRAYOP and
--     the GGDB fast int/text array evaluators under the JIT matrix.
-- ============================================================
create table sao_t (id int, val int, txt text) distributed by (id);
insert into sao_t select i, i, 't' || i from generate_series(1, 100) i;
select count(*) as ints from sao_t
  where val in (1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39);
select count(*) as strs from sao_t
  where txt in ('t2','t4','t6','t8','t10','t12','t14','t16','t18','t20','t22','t24');

-- cleanup
set search_path = public;
drop schema merge_pg15 cascade;
