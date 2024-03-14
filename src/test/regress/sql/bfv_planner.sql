--start_ignore
drop table if exists testbadsql;
drop table if exists bfv_planner_x;
drop table if exists bfv_planner_foo;
--end_ignore
CREATE TABLE testbadsql(id int);
CREATE TABLE bfv_planner_x(i integer);
CREATE TABLE bfv_planner_foo AS SELECT i as a, i+1 as b from generate_series(1,10)i;

--
-- Test unexpected internal error (execQual.c:4413) when using subquery+window function+union in 4.2.6.x
--

-- Q1
select * from
(SELECT  MIN(id) OVER () minid FROM testbadsql
UNION
SELECT  MIN(id) OVER () minid FROM testbadsql
) tmp
where tmp.minid=123;

-- Q2
select * from
(SELECT  MIN(id) OVER () minid FROM testbadsql
UNION
SELECT 1
) tmp
where tmp.minid=123;

-- Q3
select * from
(SELECT  MIN(id) OVER () minid FROM testbadsql) tmp
where tmp.minid=123;

-- Q4
SELECT * from (
  SELECT max(i) over () as w from bfv_planner_x Union Select 1 as w)
as bfv_planner_foo where w > 0;

--
-- Test query when using median function with count(*)
--

--start_ignore
drop table if exists testmedian;
--end_ignore
CREATE TABLE testmedian
(
  a character(2) NOT NULL,
  b character varying(8) NOT NULL,
  c character varying(8) NOT NULL,
  value1 double precision,
  value2 double precision
)
WITH (
  OIDS=FALSE
)
DISTRIBUTED BY (b,c);

insert into testmedian
select i, i, i, i, i
from  (select * from generate_series(1, 99) i ) a ;

-- Test with count()
select median(value1), count(*)
from  testmedian
where c ='55'
group by b, c, value2;

-- Test with varying ORDER-BY clause
select median(value1), count(*)
from  testmedian
where c ='25'
group by a, b, c, value2
order by c,b;

-- Test with varying ORDER-BY clause
select median(value1), count(*)
from  testmedian
where c ='55'
group by b,c,value1
order by b, c, value1;

-- Test with sum()
select median(value1), sum(value2)
from  testmedian
where c ='55'
group by b, c, value2;

-- Test with min()
select median(value1), min(c)
from  testmedian
where c ='55'
group by b, c, value2;

-- Test with varying GROUP-BY clause
select median(value1), count(*)
from  testmedian
where c ='55'
group by b;

-- Test with varying GROUP-BY clause
select median(value1), count(*)
from  testmedian
where c ='55'
group by b,c;

-- Test with varying GROUP-BY clause
select median(value1), count(*)
from  testmedian
where c ='55'
group by c,b;

-- Test with varying GROUP-BY clause
select median(value1), count(*)
from  testmedian
where c ='55'
group by b,c,value1;

-- Test with varying ORDER-BY clause
select median(value1), count(*)
from  testmedian
where c ='25'
group by b, value1
order by b;

-- Test with varying ORDER-BY clause
select median(value1), count(*)
from  testmedian
where c ='25'
group by b, c, value2
order by b,c;


-- Test inheritance planning, when a SubPlan is duplicated for different
-- child tables.

create table r (a int) distributed by (a);
create table p (a int, b int) distributed by (a);
create table p2 (a int, b int) inherits (p) distributed by (b);

insert into r values (3);
insert into p select a, b from generate_series(1,3) a, generate_series(1,3) b;

delete from p where b = 1 or (b=2 and a in (select r.a from r));
select * from p;

delete from p where b = 1 or (b=2 and a in (select b from r));
select * from p;


-- Test planning of IS NOT FALSE. We used treat "(a = b) IS NOT FALSE" as
-- hash joinable, and create a plan with a hash join on "(a = b)". That
-- was wrong, because NULLs are treated differently.
create table booltest (b bool);
insert into booltest values ('t');
insert into booltest values (null);
select * from booltest a, booltest b where (a.b = b.b) is not false;

-- Lossy index qual, used as a partial index predicate, and same column is
-- used in FOR SHARE. Once upon a time, this happened to tickle a bug in the
-- planner at one point.
create table tstest (t tsvector);
create index i_tstest on tstest using gist (t) WHERE t @@ 'bar';
insert into tstest values ('foo');
insert into tstest values ('bar');

set enable_bitmapscan =off;
set enable_seqscan =off;
select * from tstest where t @@ 'bar' for share of tstest;


-- Stable (and volatile) functions need to be re-evaluated on every
-- execution of a prepared statement. There used to be a bug, where
-- they were evaluated once at planning time or at first execution,
-- and the same value was incorrectly reused on subsequent executions.
create function stabletestfunc() returns integer as $$
begin
  raise notice 'stabletestfunc executed';
  return 123;
end;
$$ language plpgsql stable;

create table stabletesttab (id integer);

insert into stabletesttab values (1);
insert into stabletesttab values (1000);

-- This might evaluate the function, for cost estimate purposes. That's
-- not of importance for this test.
prepare myprep as select * from stabletesttab where id < stabletestfunc();

-- Check that the stable function should be re-executed on every execution of the prepared statetement.
execute myprep;
execute myprep;
execute myprep;

-- Test that pl/pgsql simple expressions are not considered a
-- oneoffPlan.  We validate this by ensuring that a simple expression
-- involving a stable function is planned only once and the same plan
-- is re-executed for each tuple.  The NOTICE in the immutable
-- function allows us to detect when it is executed.  We assume that
-- the planner folds immutablefunc() into a const.
CREATE FUNCTION immutablefunc() RETURNS int2
LANGUAGE plpgsql IMMUTABLE STRICT AS
$$
BEGIN
	raise notice 'immutablefunc executed';
	return 42;
END
$$;
CREATE FUNCTION stablenow (dummy int2) RETURNS timestamp
LANGUAGE plpgsql STABLE STRICT AS
$fn$
BEGIN
	return now();
END
$fn$;

CREATE FUNCTION volatilefunc(a int) RETURNS int
LANGUAGE plpgsql VOLATILE STRICT AS
$fn$
DECLARE
  t timestamp;
BEGIN
	t := stablenow(immutablefunc());
	if date_part('month', t) > a then
		return 0;
	else
		return 1;
	end if;
END
$fn$;
CREATE TABLE oneoffplantest (a int) distributed by (a);
INSERT INTO oneoffplantest VALUES (0), (0), (0);

-- Plan for the following query should be cached such that the call to
-- immutablefun() is folded into a const.  Note that all the
-- statements within volatilefunc() are pl/pgsql simple expressions.
-- Their plans should NOT be classified as oneoffPlan and should be
-- cached.  So we expect the NOTICE to be printed only once,
-- regardless of the number of tuples in the table.
select volatilefunc(a) from oneoffplantest;

-- Test agg on top of join subquery on partition table with ORDER-BY clause
CREATE TABLE bfv_planner_t1 (a int, b int, c int) distributed by (c);
CREATE TABLE bfv_planner_t2 (f int,g int) DISTRIBUTED BY (f) PARTITION BY RANGE(g)
(
PARTITION "201612" START (1) END (10)
);
insert into bfv_planner_t1 values(1,2,3), (2,3,4), (3,4,5);
insert into bfv_planner_t2 values(3,1), (4,2), (5,2);

select count(*) from
(select a,b,c from bfv_planner_t1 order by c) T1
join
(select f,g from bfv_planner_t2) T2
on
T1.a=T2.g and T1.c=T2.f;


-- This produced a "could not find pathkey item to sort" error at one point.
-- The problem was that we stripped out column b from the SubqueryScan's
-- target list, as it's not needed in the final result, but we tried to
-- maintain the ordering (a,b) in the Gather Motion node, which would have
-- required column b to be present, at least as a junk column.
create table bfv_planner_t3 (a int4, b int4);
select a from (select * from bfv_planner_t3 order by a, b) as x limit 1;

-- Similar case, but when evaluating a window function rather than LIMIT
select first_value(a) over w, a
from (select * from bfv_planner_t3 order by a, b) as x
WINDOW w AS (order by a);

-- volatile general
-- General and segmentGeneral locus imply that if the corresponding
-- slice is executed in many different segments should provide the
-- same result data set. Thus, in some cases, General and segmentGeneral
-- can be treated like broadcast. But if the segmentGeneral and general
-- locus path contain volatile functions, they lose the property and
-- can only be treated as singleQE. The following cases are to check that
-- we correctly handle all these cases.

-- FIXME: for ORCA the following SQL does not consider this. We should
-- fix them when ORCA changes.
set optimizer = off;
create table t_hashdist(a int, b int, c int) distributed by (a);

---- pushed down filter
explain (costs off)
select * from
(select a from generate_series(1, 10)a) x, t_hashdist
where x.a > random();

---- join qual
explain (costs off) select * from
t_hashdist,
(select a from generate_series(1, 10) a) x,
(select a from generate_series(1, 10) a) y
where x.a + y.a > random();

---- sublink & subquery
explain (costs off) select * from t_hashdist where a > All (select random() from generate_series(1, 10));
explain (costs off) select * from t_hashdist where a in (select random()::int from generate_series(1, 10));

-- subplan
explain (costs off, verbose) select * from
t_hashdist left join (select a from generate_series(1, 10) a) x on t_hashdist.a > any (select random() from generate_series(1, 10));

-- targetlist
explain (costs off) select * from t_hashdist cross join (select random () from generate_series(1, 10))x;
explain (costs off) select * from t_hashdist cross join (select a, sum(random()) from generate_series(1, 10) a group by a) x;
explain (costs off) select * from t_hashdist cross join (select random() as k, sum(a) from generate_series(1, 10) a group by k) x;
explain (costs off) select * from t_hashdist cross join (select a, count(1) as s from generate_series(1, 10) a group by a having count(1) > random() order by a) x ;

-- limit
explain (costs off) select * from t_hashdist cross join (select * from generate_series(1, 10) limit 1) x;

set gp_cte_sharing = on;

-- ensure that the volatile function is executed on one segment if it is in the CTE target list
explain (costs off, verbose) with cte as (
    select a * random() as a from generate_series(1, 5) a
)
select * from cte join (select * from t_hashdist join cte using(a)) b using(a);

set gp_cte_sharing = off;

explain (costs off, verbose) with cte as (
    select a, a * random() from generate_series(1, 5) a
)
select * from cte join t_hashdist using(a);

reset gp_cte_sharing;

-- ensure that the volatile function is executed on one segment if it is in the union target list
explain (costs off, verbose) select * from (
    select random() as a from generate_series(1, 5)
    union
    select random() as a from generate_series(1, 5)
)
a join t_hashdist on a.a = t_hashdist.a;

-- ensure that the volatile function is executed on one segment if it is in target list of subplan of multiset function
explain (costs off, verbose) select * from (
    SELECT count(*) as a FROM anytable_out( TABLE( SELECT random()::int from generate_series(1, 5) a ) )
) a join t_hashdist using(a);

-- if there is a volatile function in the target list of a plan with the locus type
-- General or Segment General, then such a plan should be executed on single
-- segment, since it is assumed that nodes with such locus types will give the same
-- result on all segments, which is impossible for a volatile function.
-- start_ignore
drop table if exists d;
-- end_ignore
create table d (b int, a int default 1) distributed by (b);

insert into d select * from generate_series(0, 20) j;
-- change distribution without reorganize
alter table d set distributed randomly;

with cte as (
    select a as a, a * random() as rand from generate_series(0, 3)a
)
select count(distinct(rand)) from cte join d on cte.a = d.a;

drop table d;

-- CTAS on general locus into replicated table
create temp SEQUENCE test_seq;
explain (costs off) create table t_rep as select nextval('test_seq') from (select generate_series(1,10)) t1 distributed replicated;
create table t_rep1 as select nextval('test_seq') from (select generate_series(1,10)) t1 distributed replicated;
select count(*) from gp_dist_random('t_rep1');
select count(distinct nextval) from gp_dist_random('t_rep1');

-- CTAS on general locus into replicated table with HAVING
explain (costs off) create table t_rep as select i from generate_series(5,15) as i group by i having i < nextval('test_seq') distributed replicated;
create table t_rep2 as select i from generate_series(5,15) as i group by i having i < nextval('test_seq') distributed replicated;
select count(*) from gp_dist_random('t_rep2');
select count(distinct i) from gp_dist_random('t_rep2');

-- CTAS on general locus into replicated table with GROUP BY
explain (costs off) create table t_rep as select i > nextval('test_seq') from generate_series(5,15) as i group by i > nextval('test_seq') distributed replicated;
create table t_rep3 as select i > nextval('test_seq') as a from generate_series(5,15) as i group by i > nextval('test_seq') distributed replicated;
select count(*) from gp_dist_random('t_rep3');
select count(distinct a) from gp_dist_random('t_rep3');

-- CTAS on replicated table into replicated table
create table rep_tbl as select t1 from generate_series(1,10) t1 distributed replicated;
explain (costs off) create table t_rep as select nextval('test_seq') from rep_tbl distributed replicated;
create table t_rep4 as select nextval('test_seq') from rep_tbl distributed replicated;
select count(*) from gp_dist_random('t_rep4');
select count(distinct nextval) from gp_dist_random('t_rep4');

drop table rep_tbl, t_rep1, t_rep2, t_rep3, t_rep4;
reset optimizer;

--
-- Test append different numsegments tables work well
-- See Github issue: https://github.com/greenplum-db/gpdb/issues/12146
--
create table t1_12146 (a int, b int) distributed by (a);
create table t2_12146 (a int, b int) distributed by (a);
create table t3_12146 (a int, b int) distributed by (a);
create table t4_12146 (a int, b int) distributed by (a);

-- make t1_12146 and t2_12146 to partially-distributed
set allow_system_table_mods = on;
update gp_distribution_policy set numsegments = 2
where localoid in ('t1_12146'::regclass::oid, 't2_12146'::regclass::oid);

insert into t1_12146 select i,i from generate_series(1, 10000)i;
insert into t2_12146 select i,i from generate_series(1, 10000)i;
insert into t3_12146 select i,i from generate_series(1, 10)i;
insert into t4_12146 select i,i from generate_series(1, 10)i;

-- now set t1_12146 and t2_12146 randomly distributed;
update gp_distribution_policy
set distkey = '', distclass = ''
where localoid in ('t1_12146'::regclass::oid, 't2_12146'::regclass::oid);

analyze t1_12146;
analyze t2_12146;
analyze t3_12146;
analyze t4_12146;

explain select count(*)
from
(
  (
  -- t1 left join t3 to build broadcast t3 plan
  -- so that the join's locus is t1's locus:
  -- strewn on 2segs
  select
  *
  from t1_12146 left join t3_12146 on t1_12146.b = t3_12146.b
  )
  union all
  (
  -- t2 left join t4 to build broadcast t4 plan
  -- so that the join's locus is t2's locus:
  -- strewn on 2segs
  select
  *
  from t2_12146 left join t4_12146 on t2_12146.b = t4_12146.b
  ) -- the first subplan's locus is not the same
    -- because strewn locus always not the same
  union all
  (
  -- this will be a full to full redist
  select
  *
  from t3_12146 join t4_12146 on t3_12146.b = t4_12146.a
  )
) x;

select count(*)
from
(
  (
  -- t1 left join t3 to build broadcast t3 plan
  -- so that the join's locus is t1's locus:
  -- strewn on 2segs
  select
  *
  from t1_12146 left join t3_12146 on t1_12146.b = t3_12146.b
  )
  union all
  (
  -- t2 left join t4 to build broadcast t4 plan
  -- so that the join's locus is t2's locus:
  -- strewn on 2segs
  select
  *
  from t2_12146 left join t4_12146 on t2_12146.b = t4_12146.b
  ) -- the first subplan's locus is not the same
    -- because strewn locus always not the same
  union all
  (
  -- this will be a full to full redist
  select
  *
  from t3_12146 join t4_12146 on t3_12146.b = t4_12146.a
  )
) x;

drop table t1_12146;
drop table t2_12146;
drop table t3_12146;
drop table t4_12146;

reset allow_system_table_mods;

-- Test for ERRORDATA_STACK_SIZE exceeded issue
-- The following test is copied from the commit 17c19c3f
-- since that commit tests the code path that hash op
-- not found in the opfamily (cdbpullup.c:cdbpullup_findEclassInTargetList).
-- Commit 49ea956f refactor the cdbpullup.c:cdbpullup_findEclassInTargetList
-- a bit and introduces a bug during the code path same as above. The bug
-- is that it might leak ERRORDATA_STACK_SIZE.
CREATE TABLE gp_timestamp_err_stack1
(a int, d1 timestamp, e1 timestamptz,
        d2 timestamp, e2 timestamptz,
	d3 timestamp, e3 timestamptz,
	d4 timestamp, e4 timestamptz,
	d5 timestamp, e5 timestamptz,
	d6 timestamp, e6 timestamptz,
	d7 timestamp, e7 timestamptz,
	d8 timestamp, e8 timestamptz,
	d9 timestamp, e9 timestamptz,
	d10 timestamp, e10 timestamptz,
	d11 timestamp, e11 timestamptz)
DISTRIBUTED BY (a, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11);
CREATE TABLE gp_timestamp_err_stack2
(c int, f1 timestamp, g1 timestamptz,
        f2 timestamp, g2 timestamptz,
	f3 timestamp, g3 timestamptz,
	f4 timestamp, g4 timestamptz,
	f5 timestamp, g5 timestamptz,
	f6 timestamp, g6 timestamptz,
	f7 timestamp, g7 timestamptz,
	f8 timestamp, g8 timestamptz,
	f9 timestamp, g9 timestamptz,
	f10 timestamp, g10 timestamptz,
	f11 timestamp, g11 timestamptz)
DISTRIBUTED BY (c, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11);

SELECT 1 FROM gp_timestamp_err_stack1 JOIN gp_timestamp_err_stack2
ON a = c
AND d1 = g1 AND f1 = e1 AND e1 = timestamp '2016/11/11'
AND d2 = g2 AND f2 = e2 AND e2 = timestamp '2017/11/11'
AND d3 = g3 AND f3 = e3 AND e3 = timestamp '2018/11/11'
AND d4 = g4 AND f4 = e4 AND e4 = timestamp '2019/11/11'
AND d5 = g5 AND f5 = e5 AND e5 = timestamp '2006/11/11'
AND d6 = g6 AND f6 = e6 AND e6 = timestamp '2007/11/11'
AND d7 = g7 AND f7 = e7 AND e7 = timestamp '2008/11/11'
AND d8 = g8 AND f8 = e8 AND e8 = timestamp '2009/11/11'
AND d9 = g9 AND f9 = e9 AND e9 = timestamp '2010/11/11'
AND d10 = g10 AND f10 = e10 AND e10 = timestamp '2016/10/11'
AND d11 = g11 AND f11 = e11 AND e11 = timestamp '2016/09/11';

-- start_ignore
drop table if exists bfv_planner_x;
drop table if exists testbadsql;
drop table if exists bfv_planner_foo;
drop table if exists testmedian;
drop table if exists bfv_planner_t1;
drop table if exists bfv_planner_t2;
drop table if exists gp_timestamp_err_stack1;
drop table if exists gp_timestamp_err_stack2;
-- end_ignore
