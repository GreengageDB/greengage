-- Regression guards for bugs fixed during the PostgreSQL 14 -> Greengage merge.
-- Each section is a minimal reproducer for an MPP/ORCA-path defect that the merge
-- introduced and we fixed; reverting a fix re-introduces a crash, a dispatch
-- deserialize error, or a wrong result here. The test is intentionally data-only
-- (no EXPLAIN) so the output is stable across the optimizer x JIT CI matrix, and
-- it is meant to run under BOTH optimizer=off and optimizer=on (ORCA).

create schema merge_pg14;
set search_path = merge_pg14, public;

-- ============================================================
-- 1. ORCA UPDATE updateColnosLists (UPDATE SIGSEGV) and the
--    partitioned UPDATE/DELETE planner fallback.
-- ============================================================
create table upd_dist (a int, b int, c text) distributed by (a);
insert into upd_dist select i, i * 10, 'v' || i from generate_series(1, 20) i;
update upd_dist set b = b + 1, c = c || '!' where a <= 5;
select a, b, c from upd_dist where a <= 5 order by a;

create table upd_part (a int, b int)
  distributed by (a)
  partition by range (a) (start (0) end (30) every (10));
insert into upd_part select i, i from generate_series(0, 29) i;
update upd_part set b = b * 2 where a >= 20;
delete from upd_part where a < 5;
select a, b from upd_part order by a;

-- ============================================================
-- 2. ORCA Aggref aggno/aggtransno: several different aggregates in one
--    query (the bug returned the first aggregate's value for all of them).
-- ============================================================
select count(*) as cnt, sum(b) as s, avg(b)::numeric(10,2) as a, min(b) as mn, max(b) as mx
from upd_dist;

-- ============================================================
-- 3. Correlated EXISTS and a correlated scalar subquery in the SELECT
--    list (AlternativeSubPlan cdbllize crash + lost correlation -> wrong rows).
-- ============================================================
create table corr_outer (id int, grp int) distributed by (id);
create table corr_inner (id int, ref int) distributed by (id);
insert into corr_outer select i, i % 3 from generate_series(1, 6) i;
insert into corr_inner select i, i * 100 from generate_series(1, 4) i;

select id,
       exists (select 1 from corr_inner ci where ci.id = co.id) as has_match,
       (select max(ref) from corr_inner ci where ci.id = co.id) as max_ref
from corr_outer co
order by id;

-- ============================================================
-- 4. CTAS / matview DISTRIBUTED BY (<aggregate>): Motion-on-Motion Result
--    interpose + the MIN/MAX planagg eq_classes guard. The matview with a
--    text column also exercises refresh_by_match_merge (alias.* / _$ prefix /
--    text = text comparison).
-- ============================================================
create table agg_src (g int, v int) distributed by (g);
insert into agg_src select i % 4, i from generate_series(1, 40) i;

create table ctas_sum as select sum(v) as total from agg_src distributed by (total);
select total from ctas_sum;
create table ctas_min as select min(v) as mn from agg_src distributed by (mn);
select mn from ctas_min;

create table mv_src (g int, label text, v int) distributed by (g);
insert into mv_src select i % 3, 'lbl' || (i % 3), i from generate_series(1, 30) i;
create materialized view mv_mm as
  select g, label, sum(v) as total from mv_src group by g, label distributed by (g);
create unique index mv_mm_uidx on mv_mm (g, label);
-- change one group so the concurrent refresh actually merges a diff
insert into mv_src values (0, 'lbl0', 1000);
refresh materialized view concurrently mv_mm;
select g, label, total from mv_mm order by g, label;

-- ============================================================
-- 5. CREATE VIEW / CTAS over JOIN USING (binary-dispatch JoinExpr
--    join_using_alias -> "could not deserialize unrecognized node type").
-- ============================================================
create table j_left (k int, x int) distributed by (k);
create table j_right (k int, y int) distributed by (k);
insert into j_left select i, i * 10 from generate_series(1, 5) i;
insert into j_right select i, i * 100 from generate_series(1, 5) i;

create view v_using as select k, x, y from j_left join j_right using (k);
select k, x, y from v_using order by k;
create table ctas_using as select k, x, y from j_left join j_right using (k) distributed by (k);
select k, x, y from ctas_using order by k;

-- ============================================================
-- 6. Subscripting assignment (ORCA SubscriptingRef.refrestype ->
--    "cache lookup failed for type 0").
-- ============================================================
create table sub_t (id int, j jsonb, arr int[]) distributed by (id);
insert into sub_t values (1, '{"a": 1}'::jsonb, array[10, 20, 30]);
update sub_t set j['b'] = '2'::jsonb, arr[2] = 99 where id = 1;
select id, j, arr from sub_t order by id;

-- cleanup
set search_path = public;
drop schema merge_pg14 cascade;
