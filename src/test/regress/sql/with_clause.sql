-- start_matchsubs
--
-- m/ERROR:  could not devise a plan \([a-z]+\.c:\d+\)/
-- s/\d+/XXX/g
--
-- m/ERROR:  could not parallelize SubPlan \([a-z]+\.c:\d+\)/
-- s/\d+/XXX/g
--
-- m/ERROR:  could not build Motion path \([a-z]+\.c:\d+\)/
-- s/\d+/XXX/g
--
-- end_matchsubs
-- start_ignore
create extension if not exists gp_debug_numsegments;
-- end_ignore

drop table if exists with_test1 cascade;
create table with_test1 (i int, t text, value int) distributed by (i);
insert into with_test1 select i%10, 'text' || i%20, i%30 from generate_series(0, 99) i;
analyze with_test1;

drop table if exists with_test2 cascade;
create table with_test2 (i int, t text, value int);
insert into with_test2 select i%100, 'text' || i%200, i%300 from generate_series(0, 999) i;
analyze with_test2;

-- With clause with one common table expression
--begin_equivalent
with my_sum(total) as (select sum(value) from with_test1)
select *
from my_sum;

select sum(value) as total from with_test1;
--end_equivalent

-- With clause with two common table expression
--begin_equivalent
with my_sum(total) as (select sum(value) from with_test1),
     my_count(cnt) as (select count(*) from with_test1)
select cnt, total
from my_sum, my_count;

select cnt, total
from (select sum(value) as total from with_test1) tmp1,
     (select count(*) as cnt from with_test1) tmp2;
--end_equivalent

-- With clause with one common table expression that is referenced twice
--begin_equivalent
with my_group_sum(i, total) as (select i, sum(value) from with_test1 group by i)
select gs1.i, gs1.total, gs2.total
from my_group_sum gs1, my_group_sum gs2
where gs1.i = gs2.i + 1;

select gs1.i, gs1.total, gs2.total
from (select i, sum(value) as total from with_test1 group by i) gs1,
     (select i, sum(value) as total from with_test1 group by i) gs2
where gs1.i = gs2.i + 1;
--end_equivalent

-- With clause with one common table expression that contains the other common table expression
--begin_equivalent
with my_count(i, cnt) as (select i, count(*) from with_test1 group by i),
     my_sum(total) as (select sum(cnt) from my_count)
select *
from my_sum;

select sum(cnt) as total from (select i, count(*) as cnt from with_test1 group by i) my_count;
--end_equivalent

-- WITH query contains WITH
--begin_equivalent
with my_sum(total) as (
     with my_group_sum(total) as (select sum(value) from with_test1 group by i)
     select sum(total) from my_group_sum)
select *
from my_sum;

select sum(total) from (select sum(value) as total from with_test1 group by i) my_group_sum;
--end_equivalent

-- pathkeys
explain (costs off)
with my_order as (select * from with_test1 order by i)
select i, count(*)
from my_order
group by i order by i;

with my_order as (select * from with_test1 order by i)
select i, count(*)
from my_order
group by i order by i;


-- WITH query used in InitPlan
--begin_equivalent
with my_max(maximum) as (select max(value) from with_test1)
select * from with_test2
where value < (select * from my_max);

select * from with_test2
where value < (with my_max(maximum) as (select max(value) from with_test1)
               select * from my_max);

select * from with_test2
where value < (select max(value) from with_test1);
--end_equivalent

-- WITH query used in InitPlan and the main query at the same time
--begin_equivalent
with my_max(maximum) as (select max(value) from with_test1)
select with_test2.* from with_test2, my_max
where value < (select * from my_max)
and i < maximum and i > maximum - 10;

select with_test2.* from with_test2, (select max(value) as maximum from with_test1) as my_max
where value < (select max(value) from with_test1)
and i < maximum and i > maximum - 10;
--end_equivalent

-- WITH query used in subplan
--begin_equivalent
with my_groupmax(i, maximum) as (select i, max(value) from with_test1 group by i)
select * from with_test2
where value < all (select maximum from my_groupmax);

select * from with_test2
where value < all (select max(value) from with_test1 group by i);
--end_equivalent

-- WITH query used in subplan and the main query at the same time
--begin_equivalent
with my_groupmax(i, maximum) as (select i, max(value) from with_test1 group by i)
select * from with_test2, my_groupmax
where with_test2.i = my_groupmax.i
and value < all (select maximum from my_groupmax);

select * from with_test2, (select i, max(value) as maximum from with_test1 group by i) as my_groupmax
where with_test2.i = my_groupmax.i
and value < all (select max(value) from with_test1 group by i);
--end_equivalent

--begin_equivalent
with my_groupmax(i, maximum) as (select i, max(value) from with_test1 group by i)
SELECT count(*) FROM my_groupmax WHERE maximum > (SELECT sum(maximum)/100 FROM my_groupmax);

select count(*) from (select i, max(value) as maximum from with_test1 group by i) as my_groupmax
where maximum > (SELECT sum(maximum)/100 FROM (select i, max(value) as maximum from with_test1 group by i) as tmp);
--end_equivalent

-- name resolution
--begin_equivalent
with my_max(maximum) as (select max(value) from with_test2)
select * from with_test1, my_max
where value < (with my_max(maximum) as (select max(i) from with_test1)
               select * from my_max);

select * from with_test1, (select max(value) as maximum from with_test2) as my_max
where value < (select max(i) from with_test1);
--end_equivalent

-- INSERT
insert into with_test2
with my_sum(i, total) as (select i, sum(value) from with_test1 group by i)
select i, i || '', total
from my_sum;

-- CREATE TABLE AS
drop table if exists with_test3;
create table with_test3 as
with my_sum(i, total) as (select i, sum(value) from with_test1 group by i)
select *
from my_sum;

-- view
drop view if exists my_view;
create view my_view (total) as
with my_sum(total) as (select sum(value) from with_test1)
select *
from my_sum;

SELECT pg_get_viewdef('my_view'::regclass);
SELECT pg_get_viewdef('my_view'::regclass, true);

drop view if exists my_view;
create view my_view(total) as
with my_sum(total) as (
     with my_group_sum(total) as (select sum(value) from with_test1 group by i)
     select sum(total) from my_group_sum)
select *
from my_sum;

SELECT pg_get_viewdef('my_view'::regclass);
SELECT pg_get_viewdef('my_view'::regclass, true);

drop view if exists my_view;
create view my_view(i, total) as (
    select i, sum(value) from with_test1 group by i);
with my_sum(total) as (select sum(total) from my_view)
select * from my_sum;

-- WITH query not used in the main query
--begin_equivalent
with my_sum(total) as (select sum(value) from with_test1)
select count(*) from with_test2;

select count(*) from with_test2;
--end_equivalent

-- WITH used in CURSOR query
begin;
	declare c cursor for with my_sum(total) as (select sum(value) from with_test1 group by i) select * from my_sum order by 1;
	fetch 10 from c;
	close c;
end;

-- Returning
create temporary table y (i int);
insert into y
with t as (select i from with_test1)
select i+20 from t returning *;

select * from y;

drop table y;

-- WITH used in SETOP
with my_sum(total) as (select sum(value) from with_test1)
select * from my_sum
union all
select * from my_sum;

-- ERROR cases

-- duplicate CTE name
with my_sum(total) as (select sum(value) from with_test1),
     my_sum(group_total) as (select sum(value) from with_test1 group by i)
select *
from my_sum;

-- INTO clause
with my_sum(total) as (select sum(value) from with_test1 into total_value)
select *
from my_sum;

-- name resolution
select * from with_test1, my_max
where value < (with my_max(maximum) as (select max(i) from with_test1)
               select * from my_max);

with my_sum(total) as (select sum(total) from my_group_sum),
     my_group_sum(i, total) as (select i, sum(total) from with_test1 group by i)
select *
from my_sum;

-- two WITH clauses
with my_sum(total) as (select sum(total) from with_test1),
with my_group_sum(i, total) as (select i, sum(total) from with_test1 group by i)
select *
from my_sum;

-- Test behavior with an unknown-type literal in the WITH
WITH q AS (SELECT 'foo' AS x)
SELECT x, x IS OF (unknown) as is_unknown, x IS OF (text) as is_text FROM q;

with cte(foo) as ( select 42 ) select * from ((select foo from cte)) q;

select ( with cte(foo) as ( values(i) )
         select (select foo from cte) )
from with_test1
order by 1 limit 10;

select ( with cte(foo) as ( values(i) )
         values((select foo from cte)) )
from with_test1
order by 1 limit 10;

-- WITH query using Window functions
--begin_equivalent
with my_rank as (select i, t, value, rank() over (order by value) from with_test1)
select my_rank.* from with_test2, my_rank
where with_test2.i = my_rank.i
order by my_rank.i, my_rank.t, my_rank.value limit 100; -- order 1,2,3

select my_rank.* from with_test2, (select i, t, value, rank() over (order by value) from with_test1) as my_rank
where with_test2.i = my_rank.i
order by my_rank.i, my_rank.t, my_rank.value limit 100; -- order 1,2,3
--end_equivalent

-- WITH query and CSQ
--begin_equivalent
with my_group_sum(i, total) as (select i, sum(value) from with_test1 group by i)
select with_test2.* from with_test2
where value < any (select total from my_group_sum where my_group_sum.i = with_test2.i);

select with_test2.* from with_test2
where value < any (select total from (select i, sum(value) as total from with_test1 group by i) as tmp where tmp.i = with_test2.i);
--end_equivalent

--begin_equivalent
with my_group_sum(i, total) as (select i, sum(value) from with_test1 group by i)
select with_test2.* from with_test2, my_group_sum
where value < any (select total from my_group_sum where my_group_sum.i = with_test2.i)
and with_test2.i = my_group_sum.i;

select with_test2.* from with_test2, (select i, sum(value) from with_test1 group by i) as my_group_sum
where value < any (select total from (select i, sum(value) as total from with_test1 group by i) as tmp where tmp.i = with_test2.i)
and with_test2.i = my_group_sum.i;
--end_equivalent

--begin_equivalent
with my_group_sum(i, total) as (select i, sum(value) from with_test1 group by i)
select with_test2.* from with_test2
where value < all (select total from my_group_sum where my_group_sum.i = with_test2.i)
order by 1,2,3
limit 60; --order 1,2,3

select with_test2.* from with_test2
where value < all (select total from (select i, sum(value) as total from with_test1 group by i) as tmp where tmp.i = with_test2.i)
order by 1,2,3
limit 60; --order 1,2,3
--end_equivalent

drop table if exists d;
drop table if exists b;
create table with_b (i integer) distributed by (i);
insert into with_b values (1), (2);

--begin_equivalent
with b1 as (select * from with_b) select * from (select * from b1 where b1.i =1) AS FOO, b1 FOO2;

select * from (select * from (select * from with_b) as b1 where b1.i = 1) AS FOO, (select * from with_b) as foo2;
--end_equivalent
-- qual push down test
explain (costs off) with t as (select * from with_test1) select * from t where i = 10;

-- Test to validate an old bug which caused incorrect results when a subquery
-- in the WITH clause appears under a nested-loop join in the query plan when
-- gp_cte_sharing was set to off. (MPP-17848)
CREATE TABLE x (a integer) DISTRIBUTED BY (a);
insert into x values(1), (2);

CREATE TABLE y (m integer NOT NULL, n smallint) DISTRIBUTED BY (m);
insert into y values(10, 1);
insert into y values(20, 1);

with yy as (
   select m
   from y,
        (select 1 as p) iv
   where n = iv.p
)
select * from x, yy;

-- Check that WITH query is run to completion even if outer query isn't.
-- This is a test which exists in the upstream 'with' test suite in a section
-- which is currently under an ignore block. It has been copied here to avoid
-- merge conflicts since enabling it in the upstream test suite would require
-- altering the test output (as it depends on earlier tests which are failing
-- in GPDB currently).
DELETE FROM y;
INSERT INTO y SELECT generate_series(1,15) m;
WITH t AS (
    UPDATE y SET m = m * 100 RETURNING *
)
SELECT m BETWEEN 100 AND 1500 FROM t LIMIT 1;

SELECT * FROM y;

-- Nested RECURSIVE queries with double self-referential joins are planned by
-- joining two WorkTableScans, which GPDB cannot do yet. Ensure that we error
-- out with a descriptive message.
WITH RECURSIVE r1 AS (
	SELECT 1 AS a
	UNION ALL
	(
		WITH RECURSIVE r2 AS (
			SELECT 2 AS b
			UNION ALL
			SELECT b FROM r1, r2
		)
		SELECT b FROM r2
	)
)
SELECT * FROM r1 LIMIT 1;

-- GPDB
-- Greengage does not support window functions in recursive part's target list
-- See issue https://github.com/GreengageDB/greengage/issues/13299 for details.
-- Previously the following SQL will PANIC or Assert Fail if compiled with assert.
create table t_window_ordered_set_agg_rte(a bigint, b bigint, c bigint);

insert into t_window_ordered_set_agg_rte select i,i,i from generate_series(1, 10)i;

-- should error out during parse-analyze
with recursive rcte(x,y) as
(
  select a, b from t_window_ordered_set_agg_rte
  union all
  select (first_value(c) over (partition by b))::int, a+x
  from rcte,
       t_window_ordered_set_agg_rte as t
  where t.b = x
)
select * from rcte limit 10;

-- should error out during parse-analyze
with recursive rcte(x,y) as
(
  select a, b from t_window_ordered_set_agg_rte
  union all
  select first_value(c) over (partition by b), a+x
  from rcte,
       t_window_ordered_set_agg_rte as t
  where t.b = x
)
select * from rcte limit 10;

-- This used to deadlock, before the IPC between ShareInputScans across
-- slices was rewritten.
set gp_cte_sharing=on;

CREATE TEMP TABLE foo (i int, j int);
INSERT INTO foo SELECT g, g FROM generate_series(1, 2) g;
ANALYZE foo;

WITH a1 as (select * from foo),
     a2 as (select * from foo)
SELECT a1.i
  FROM a1
  INNER JOIN a2 ON a2.i = a1.i
UNION ALL
SELECT count(a1.i)
  FROM a1
  INNER JOIN a2 ON a2.i = a1.i;

explain (costs off)
WITH a1 as (select * from foo),
     a2 as (select * from foo)
SELECT a1.i
  FROM a1
  INNER JOIN a2 ON a2.i = a1.i
UNION ALL
SELECT count(a1.i)
  FROM a1
  INNER JOIN a2 ON a2.i = a1.i;

-- Another cross-slice ShareInputScan test. There is one producing slice,
-- and two consumers in second slice. Make sure the Share Input Scan
-- consumer slice doesn't prematurely notify the producer that it's done,
-- when one of the Scans in the consumer slice finishes, but there are still
-- Scans left in the same slice.
explain (costs off)
WITH cte AS (SELECT * FROM foo)
  -- This branch runs on different slice. It is the producer slice.
  (SELECT DISTINCT 'a' as branch, j FROM cte)
UNION ALL
  -- This branch runs in the consumer slice. It contains a join. A join
  -- causes the input to be squelched when it reaches the end.
  (SELECT 'b', x.i FROM cte x, cte y WHERE x.i = y.i)
UNION ALL
  -- Sleep a bit between executing the previous slice and the next slice,
  -- so that if the squelch from the join incorrectly sent a "done" message
  -- to the producer slice, the producer has a chance to finish and remove
  -- the tuplestore, before the next branch tries to open the shared
  -- tuplestore again.
  SELECT 'sleep', 1 where pg_sleep(1) is not null
UNION ALL
  -- Consumer, runs in same slice as the join above.
  SELECT 'c', j FROM cte;

WITH cte AS (SELECT * FROM foo)
  (SELECT DISTINCT 'a' as branch, j FROM cte)
UNION ALL
  (SELECT 'b', x.i FROM cte x, cte y WHERE x.i = y.i)
UNION ALL
  SELECT 'sleep', 1 where pg_sleep(1) is not null
UNION ALL
  SELECT 'c', j FROM cte;

--start_ignore
DROP TABLE IF EXISTS es1;
DROP TABLE IF EXISTS es2;
--end_ignore

CREATE TABLE es1 (i INT) DISTRIBUTED BY(i);
CREATE TABLE es2 (i INT) DISTRIBUTED BY(i);
SET gp_cte_sharing = ON;
WITH cte AS (
	UPDATE es1 SET i = es1.i FROM es2 WHERE es2.i = es1.i RETURNING *
)
SELECT * FROM cte UNION ALL SELECT * FROM cte;

EXPLAIN (VERBOSE ON, COSTS OFF) WITH cte AS (
	UPDATE es1 SET i = es1.i FROM es2 WHERE es2.i = es1.i RETURNING *
)
SELECT * FROM cte UNION ALL SELECT * FROM cte;

DROP TABLE es1;
DROP TABLE es2;

-- Test forced materialization (sharing) of CTEs with non-SELECT DML inside.
--start_ignore
drop table if exists with_dml;
--end_ignore
create table with_dml (i int, j int) distributed by (i);

explain (costs off)
with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte;

with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte;

explain (costs off)
with cte as (
    update with_dml
    set j = j + 1 where i <= 5
    returning j
) select count(*) from cte;

with cte as (
    update with_dml
    set j = j + 1 where i <= 5
    returning j
) select count(*) from cte;

explain (costs off)
with cte as (
    delete from with_dml where i > 0
    returning i
) select count(*) from cte;

with cte as (
    delete from with_dml where i > 0
    returning i
) select count(*) from cte;

-- Test join operations with shared modifying CTEs
--start_ignore
drop table if exists t_hashed;
drop table if exists t_strewn;
drop table if exists t_repl;
--end_ignore
create table t_hashed (i int, j int) distributed by (i);
create table t_strewn (i int, j int) distributed randomly;
create table t_repl (i int, j int) distributed replicated;

insert into t_hashed values (1,1), (2,2), (3,3);
insert into t_strewn values (1,1), (2,2), (3,3);
insert into t_repl values (1,1), (2,2), (3,3);

explain (costs off)
with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_repl using (i);

with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_repl using (i);

explain (costs off)
with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_hashed on cte.i = t_hashed.i;

with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_hashed on cte.i = t_hashed.i;

explain (costs off)
with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_strewn on cte.i = t_strewn.i;

with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_strewn on cte.i = t_strewn.i;

drop table t_strewn;

-- Test that shared modifying CTE work under SubPlan or InitPlan
explain (costs off)
with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t_hashed
where t_hashed.i in (select i from cte);

with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t_hashed
where t_hashed.i in (select i from cte);

explain (costs off)
with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t_hashed
where t_hashed.i in (select i from cte where cte.i = t_hashed.j);

with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t_hashed
where t_hashed.i in (select i from cte where cte.i = t_hashed.j);

explain (costs off)
with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t_hashed
where exists (select * from cte);

with cte as (
    insert into with_dml
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t_hashed
where exists (select * from cte);

drop table t_hashed;

-- Test complex recursive case
explain (costs off)
with recursive cte as (
    select 1 as i from t_repl where t_repl.j = 1
    union all
    select cte.i + 1 from cte join cte2 using(i) where cte.i<3),
cte2 as (
    insert into with_dml select i, i * 100 from generate_series(1,5) i
    returning i, j
) select * from cte;

with recursive cte as (
    select 1 as i from t_repl where t_repl.j = 1
    union all
    select cte.i + 1 from cte join cte2 using(i) where cte.i<3),
cte2 as (
    insert into with_dml select i, i * 100 from generate_series(1,5) i
    returning i, j
) select * from cte;

drop table t_repl;

-- Control tuple count in with_dml relation
select count(*) from with_dml;

-- Test multi reference cases
explain (costs off)
with cte as (
    insert into with_dml select i, i * 100 from generate_series(1,5) i
    returning i, j
) select count(*) from cte a join cte b on a.i=b.i;

with cte as (
    insert into with_dml select i, i * 100 from generate_series(1,5) i
    returning i, j
) select count(*) from cte a join cte b on a.i=b.i;

explain (costs off)
with cte as (
    update with_dml set j = j + 1
    returning i, j
) select count(*) from cte a join cte b on a.i=b.i;

with cte as (
    update with_dml set j = j + 1
    returning i, j
) select count(*) from cte a join cte b on a.i=b.i;

explain (costs off)
with cte as (
    delete from with_dml where i > 0
    returning i, j
) select count(*) from cte a join cte b on a.i=b.i;

with cte as (
    delete from with_dml where i > 0
    returning i, j
) select count(*) from cte a join cte b on a.i=b.i;

explain (costs off)
with cte as (
    insert into with_dml select i, i * 100 from generate_series(1,5) i
    returning i, j
), cte2 as (select * from cte)
select count(*) from cte2 a join cte2 b on a.i=b.i;

with cte as (
    insert into with_dml select i, i * 100 from generate_series(1,5) i
    returning i, j
), cte2 as (select * from cte)
select count(*) from cte2 a join cte2 b on a.i=b.i;

-- Test SELECT INTO and CTAS with modifying CTE
explain (costs off)
with cte as (
    insert into with_dml select i, i * 100 from generate_series(1, 5) i
    returning *
) select into t_new from cte;

explain (costs off)
with cte as
(
    update with_dml set j = j + 1
    returning *
) select into t_new from cte;

explain (costs off)
with cte as
(
    delete from with_dml where i > 0
    returning *
) select into t_new from cte;

explain (costs off)
create table t_new as
(
    with cte as
    (
        insert into with_dml select i, i * 100 from generate_series(1, 5) i
        returning *
    ) select * from cte
);

explain (costs off)
create table t_new as
(
    with cte as
    (
        update with_dml set j = j + 1
        returning *
    ) select * from cte
);

explain (costs off)
create table t_new as
(
    with cte as
    (
        delete from with_dml where i > 0
        returning *
    ) select * from cte
);

-- A bit more complex case with nested CTEs and LIMIT to test a plan
-- with different types of gangs (reader, singleton reader and writers)
explain (costs off)
with cte as
(
    with inner_cte as
    (
        select j from with_dml limit 1
    ) delete from with_dml where with_dml.i in (select * from inner_cte)
    returning *
) select * into t_new from cte;

-- Test queries with modifying CTE not referenced from the query
explain (costs off)
with cte as (
    insert into with_dml select i, i * 100 from generate_series(1, 5) i
    returning *
) select into t_new from (select i, i * 100 from generate_series(1, 5) i) t;

explain (costs off)
with cte as
(
    update with_dml set j = j + 1
    returning *
) select into t_new from (select i, i * 100 from generate_series(1, 5) i) t;

explain (costs off)
with cte as
(
    delete from with_dml where i > 0
    returning *
) select into t_new from (select i, i * 100 from generate_series(1, 5) i) t;

explain (costs off)
create table t_new as
(
    with cte as
    (
        insert into with_dml select i, i * 100 from generate_series(1, 5) i
        returning *
    ) select * from (select i, i * 100 from generate_series(1, 5) i) t
);

explain (costs off)
create table t_new as
(
    with cte as
    (
        update with_dml set j = j + 1
        returning *
    ) select * from (select i, i * 100 from generate_series(1, 5) i) t
);

explain (costs off)
create table t_new as
(
    with cte as
    (
        delete from with_dml where i > 0
        returning *
    ) select * from (select i, i * 100 from generate_series(1, 5) i) t
);

-- Test if second writer slice is in the InitPlan
explain (costs off)
create table t_new as
with cte1 as
(
	insert into with_dml values(1, 1) returning *
),
cte2 as
(
	select * from unnest(array(select cte1.i from cte1))
) select * from cte2;

drop table with_dml;

-- Test various SELECT statements from CTE with
-- modifying DML operations over replicated tables
--start_ignore
drop table if exists with_dml_dr;
--end_ignore
create table with_dml_dr(i int, j int) distributed replicated;

-- Test plain SELECT from CTE with modifying DML queries on replicated table.
-- Explicit Gather Motion should present at the top of the plan.
explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte;

explain (costs off)
with cte as (
    update with_dml_dr
    set j = j + 1 where i <= 5
    returning j
) select count(*) from cte;

with cte as (
    update with_dml_dr
    set j = j + 1 where i <= 5
    returning j
) select count(*) from cte;

explain (costs off)
with cte as (
    delete from with_dml_dr where i > 0
    returning i
) select count(*) from cte;

with cte as (
    delete from with_dml_dr where i > 0
    returning i
) select count(*) from cte;

-- Test ORDER BY clause is applied correctly to the result of modifying
-- CTE over replicated table.
explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select * from cte order by i;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select * from cte order by i;

-- Test join operations between CTE conaining various modifying DML operations
-- over replicated table and other tables. Ensure that CdbLocusType_Replicated
-- is compatible with other type of locuses during joins.
-- Test join CdbLocusType_Replicated with CdbLocusType_SegmentGeneral.
--start_ignore
drop table if exists t_repl;
--end_ignore
create table t_repl (i int, j int) distributed replicated;

insert into t_repl values (1, 1), (2, 2), (3, 3);

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_repl using (i);

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_repl using (i);

-- Test join CdbLocusType_Replicated with CdbLocusType_SegmentGeneral
-- in case when relations are propagated on different number of segments.
--start_ignore
drop table if exists with_dml_dr_seg2;
--end_ignore
select gp_debug_set_create_table_default_numsegments(2);
create table with_dml_dr_seg2 (i int, j int) distributed replicated;
select gp_debug_reset_create_table_default_numsegments();


-- SegmentGeneral's number of segments is larger than Replicated's,
-- the join is performed at number of segments of Replicated locus.
explain (costs off)
with cte as (
    insert into with_dml_dr_seg2
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_repl using (i);

with cte as (
    insert into with_dml_dr_seg2
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_repl using (i);

-- SegmentGeneral's number of segments is less than Replicated's,
-- the join is performed at SingleQE.
explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join with_dml_dr_seg2 using (i);

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join with_dml_dr_seg2 using (i);

drop table with_dml_dr_seg2;
drop table t_repl;

-- Test join CdbLocusType_Replicated with CdbLocusType_SingleQE.
explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte left join
  (select random() * 0 v from generate_series(1,5)) x on cte.i = x.v;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte left join
  (select random() * 0 v from generate_series(1,5)) x on cte.i = x.v;

-- Test join CdbLocusType_Replicated with CdbLocusType_Entry.
explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(-5,-1) i
    returning i
) select count(*) from cte left join gp_segment_configuration on cte.i = port;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(-5,-1) i
    returning i
) select count(*) from cte left join gp_segment_configuration on cte.i = port;

-- Test join CdbLocusType_Replicated with CdbLocusType_General.
explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i)
select count(*) from cte join
(select a from generate_series(1,5) a) x on cte.i = x.a;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i)
select count(*) from cte join
(select a from generate_series(1,5) a) x on cte.i = x.a;

-- Test join CdbLocusType_Replicated with CdbLocusType_Hashed
-- and CdbLocusType_Strewn.
--start_ignore
drop table if exists t_hashed;
drop table if exists t_strewn;
--end_ignore
create table t_hashed (i int, j int) distributed by (i);
create table t_strewn (i int, j int) distributed randomly;
insert into t_hashed select i, i * 2 from generate_series(1, 10) i;
insert into t_strewn select i, i * 2 from generate_series(1, 10) i;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_hashed on cte.i = t_hashed.i;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_hashed on cte.i = t_hashed.i;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte left join t_hashed on cte.i = t_hashed.i;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte left join t_hashed on cte.i = t_hashed.i;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_strewn on cte.i = t_strewn.i;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_strewn on cte.i = t_strewn.i;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte left join t_strewn on cte.i = t_strewn.i;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte left join t_strewn on cte.i = t_strewn.i;

drop table t_strewn;
drop table t_hashed;

-- Test join CdbLocusType_Replicated with CdbLocusType_Hashed and
-- CdbLocusType_Strewn in case when relations are propagated on
-- different number of segments.
select gp_debug_set_create_table_default_numsegments(2);
create table t_hashed_seg2 (i int, j int) distributed by (i);
create table t_strewn_seg2 (i int, j int) distributed randomly;
select gp_debug_reset_create_table_default_numsegments();

insert into t_hashed_seg2 select i, i * 2 from generate_series(1, 10) i;
insert into t_strewn_seg2 select i, i * 2 from generate_series(1, 10) i;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_hashed_seg2 on cte.i = t_hashed_seg2.i;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_hashed_seg2 on cte.i = t_hashed_seg2.i;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_strewn_seg2 on cte.i = t_strewn_seg2.i;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte join t_strewn_seg2 on cte.i = t_strewn_seg2.i;

drop table t_strewn_seg2;
drop table t_hashed_seg2;

-- Test join CdbLocusType_Replicated with CdbLocusType_Replicated.
-- Join can be performed correctly only when CTE is shared.
set gp_cte_sharing = 1;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte a join cte b using (i);

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i
) select count(*) from cte a join cte b using (i);

reset gp_cte_sharing;

-- Test prohibition of volatile functions applied to the
-- locus Replicated. The appropriate error should be thrown.
--start_ignore
drop table if exists t_repl;
--end_ignore
create table t_repl (i int, j int) distributed replicated;

-- Prohibit volatile qualifications.
explain (costs off, verbose)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i, j
) select * from cte where cte.j > random();

-- Prohibit volatile returning list
explain (costs off, verbose)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i, j * random()
) select * from cte;

-- Prohibit volatile targetlist.
explain (costs off, verbose)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i, j
) select i, j * random() from cte;

-- Prohibit volatile having qualifications.
explain (costs off, verbose)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i, j
) select i, sum(j) from cte group by i having sum(j) > random();

-- Prohibit volatile join qualifications.
explain (costs off, verbose)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,5) i
    returning i, j
) select * from cte join t_repl on cte.i = t_repl.j * random();

drop table t_repl;

-- Test that node with locus Replicated is not boradcasted inside
-- a correlated/uncorrlated SubPlan. In case of different number of
-- segments between replicated node inside the SubPlan and main plan
-- the proper error should be thrown.
--start_ignore
drop table if exists t1;
drop table if exists with_dml_dr_seg2;
--end_ignore

create table t1 (i int, j int) distributed by (i);
select gp_debug_set_create_table_default_numsegments(2);
create table with_dml_dr_seg2 (i int, j int) distributed replicated;
select gp_debug_reset_create_table_default_numsegments();

insert into t1 select i, i from generate_series(1, 6) i;

-- Case when number of segments is equal, no Broadcast at the top of CTE plan.
explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte)
order by 1;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte)
order by 1;

explain (costs off)
with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte where cte.i = t1.j)
order by 1;

with cte as (
    insert into with_dml_dr
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte where cte.i = t1.j)
order by 1;

-- Case with unequal number of segments between replicated node inside the
-- SubPlan and main plan, can be handled by Explicit Gather Motion.
explain (costs off)
with cte as (
    insert into with_dml_dr_seg2
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte)
order by 1;

with cte as (
    insert into with_dml_dr_seg2
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte)
order by 1;

-- Case with unequal number of segments between replicated node inside the
-- SubPlan and main plan, the error should be thrown.
explain (costs off)
with cte as (
    insert into with_dml_dr_seg2
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte where cte.i = t1.j)
order by 1;

with cte as (
    insert into with_dml_dr_seg2
    select i, i * 100 from generate_series(1,6) i
    returning i, j
) select * from t1
where t1.i in (select i from cte where cte.i = t1.j)
order by 1;

drop table t1;

-- Test UNION ALL command when combining SegmentGeneral locus and Replicated.
--start_ignore
drop table if exists t_repl;
drop table if exists t_repl_seg2;
--end_ignore
create table t_repl (i int, j int) distributed replicated;

select gp_debug_set_create_table_default_numsegments(2);
create table t_repl_seg2 (i int, j int) distributed replicated;
select gp_debug_reset_create_table_default_numsegments();

insert into t_repl values (2, 2);
insert into t_repl_seg2 values (2, 2);

explain (costs off)
with cte as (
    insert into with_dml_dr
    values (1,1)
    returning i, j
) select * from cte union all select * from t_repl
order by 1;

with cte as (
    insert into with_dml_dr
    values (1,1)
    returning i, j
) select * from cte union all select * from t_repl
order by 1;

-- Case when SegmentGeneral is originally propagated at less number
-- of segments.
explain (costs off)
with cte as (
    insert into with_dml_dr
    values (1,1)
    returning i, j
) select * from cte union all select * from t_repl_seg2
order by 1;

with cte as (
    insert into with_dml_dr
    values (1,1)
    returning i, j
) select * from cte union all select * from t_repl_seg2
order by 1;

-- Case when final number of segments is aligned to Replicated subplan.
explain (costs off)
with cte as (
    insert into with_dml_dr_seg2
    values (1,1)
    returning i, j
) select * from cte union all select * from t_repl
order by 1;

with cte as (
    insert into with_dml_dr_seg2
    values (1,1)
    returning i, j
) select * from cte union all select * from t_repl
order by 1;

drop table t_repl_seg2;
drop table t_repl;
drop table with_dml_dr_seg2;
drop table with_dml_dr;
