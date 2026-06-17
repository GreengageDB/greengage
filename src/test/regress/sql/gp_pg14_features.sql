-- MPP/ORCA coverage for PostgreSQL 14 features that arrived in the merge with
-- little or no regression coverage: GROUP BY DISTINCT, recursive CTE
-- SEARCH/CYCLE, and multirange types. Data-only and ORDER BY-stable so a single
-- expected file works across the optimizer x JIT matrix; runs under both
-- optimizer=off and optimizer=on (ORCA), exercising each feature over
-- distributed data with the motions/redistribution that implies.

create schema pg14_feat;
set search_path = pg14_feat, public;
set timezone = 'UTC';            -- keep extract()/date_bin output deterministic
set datestyle = 'ISO, MDY';      -- portable timestamptz rendering

-- ============================================================
-- GROUP BY DISTINCT (PG14): dedup of overlapping grouping sets, over MPP
-- multi-stage grouping + redistribution.
-- ============================================================
create table gbd (a int, b int, c int) distributed by (a);
insert into gbd select i % 2, i % 3, i from generate_series(1, 12) i;

select a, b, sum(c) as s, count(*) as n
from gbd
group by distinct rollup(a, b), rollup(a, b)
order by a nulls last, b nulls last;

-- DISTINCT really removes the duplicate grouping sets (fewer groups than plain)
select 'distinct' as kind, count(*) as groups
  from (select a, b from gbd group by distinct rollup(a, b), rollup(a, b)) t
union all
select 'plain' as kind, count(*) as groups
  from (select a, b from gbd group by rollup(a, b), rollup(a, b)) t
order by kind;

-- ============================================================
-- Recursive CTE SEARCH / CYCLE (PG14) over a distributed graph.
-- ============================================================
create table graph (id int, parent int, label text) distributed by (id);
insert into graph values
  (1, null, 'root'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'c'), (5, 3, 'd');

-- SEARCH DEPTH FIRST: deterministic traversal order via the SET column
with recursive t (id, parent, label, lvl) as (
  select id, parent, label, 0 from graph where parent is null
  union all
  select g.id, g.parent, g.label, t.lvl + 1
    from graph g join t on g.parent = t.id
) search depth first by id set seq
select id, parent, label, lvl from t order by seq;

-- CYCLE: a cyclic edge set must terminate via cycle detection (would otherwise
-- recurse forever)
create table edges (src int, dst int) distributed by (src);
insert into edges values (1, 2), (2, 3), (3, 1);

with recursive walk (src, dst, depth) as (
  select src, dst, 1 from edges where src = 1
  union all
  select e.src, e.dst, w.depth + 1
    from edges e join walk w on e.src = w.dst
) cycle src set is_cycle using path
select src, dst, depth, is_cycle from walk order by depth, src, dst;

-- ============================================================
-- Multirange types (PG14): containment / overlap / bounds / unnest, distributed.
-- ============================================================
create table mr (id int, r int4multirange) distributed by (id);
insert into mr values
  (1, '{[1,5), [10,15)}'), (2, '{[3,8)}'), (3, '{[20,25)}');

select id from mr where r @> 4 order by id;
select id from mr where r && int4multirange(int4range(4, 12)) order by id;
select id, lower(r) as lo, upper(r) as hi from mr order by id;
select id, r * int4multirange(int4range(2, 12)) as isect from mr order by id;
-- NB: unnest(anymultirange) is intentionally not exercised here -- the PG14
-- multirange unnest function (C multirange_unnest + its pg_proc entry) was not
-- carried into this branch, so the constituent-range operators above provide the
-- multirange coverage instead.

-- ============================================================
-- Subscripting (PG14 SubscriptingRef) in projection / filter / join key over
-- distributed data (the refrestype class, beyond the UPDATE path).
-- ============================================================
create table sub2 (id int, arr int[], j jsonb) distributed by (id);
insert into sub2 values
  (1, array[10,20,30],    '{"a": 1, "b": {"c": 2}}'),
  (2, array[40,50],       '{"a": 9}'),
  (3, array[60,70,80,90], '{"x": [1,2,3]}');
select id, arr[2] as a2, j['a'] as ja, j['b']['c'] as jbc from sub2 order by id;
select id from sub2 where arr[1] = 40 order by id;
create table sub2k (id int, k int) distributed by (id);
insert into sub2k values (1, 20), (2, 40), (3, 999);
select s.id, k.k from sub2 s join sub2k k on s.arr[2] = k.k order by s.id;

-- ============================================================
-- extract() (PG14 numeric return) + date_bin (PG14) over distributed data.
-- ============================================================
create table tsd (id int, ts timestamptz) distributed by (id);
insert into tsd values
  (1, '2021-03-15 10:30:00+00'), (2, '2022-07-04 22:00:00+00'), (3, '2023-12-31 23:59:59+00');
select id, extract(year from ts) as yr, extract(epoch from ts)::bigint as ep from tsd order by id;
select extract(year from ts) as yr, count(*) from tsd group by extract(year from ts) order by yr;
select id, date_bin('1 hour', ts, timestamptz '2021-01-01') as binned from tsd order by id;

-- ============================================================
-- range_agg (PG14, returns anymultirange): ORCA can't resolve the polymorphic
-- multirange result type, so it falls back to the planner -> correct multirange.
-- ============================================================
select range_agg(int4range(id, id + 5)) as ra from tsd;

-- ============================================================
-- WITH ... NOT MATERIALIZED inlining over distributed CTEs.
-- ============================================================
with cte as not materialized (select id, arr[1] as a1 from sub2)
select c1.id from cte c1 join cte c2 on c1.a1 = c2.a1 where c1.id <= c2.id order by c1.id;

-- cleanup
reset timezone;
reset datestyle;
set search_path = public;
drop schema pg14_feat cascade;
