-- Additional GPDB-added tests for UNION
-- start_matchsubs
-- m/\(cost=.*\)/
-- s/\(cost=.*\)//
-- end_matchsubs
SET optimizer_trace_fallback=on;

create temp table t_union1 (a int, b int);
select distinct a, null::integer as c from t_union1 union select a, b from t_union1;
drop table t_union1;

select null union select distinct null;

select 1 union select distinct null::integer;

select 1 a, NULL b, NULL c UNION SELECT 2, 3, NULL UNION SELECT 3, NULL, 4;

select ARRAY[1, 2, 3] union select distinct null::integer[];

select 1 intersect (select 1, 2 union all select 3, 4);
select 1 a, row_number() over (partition by 'a') union all (select 1 a , 2 b);

-- This should preserve domain types
select pg_typeof(a) from (select 'a'::information_schema.sql_identifier a union all
select 'b'::information_schema.sql_identifier)a;

(select * from (
     (select '1' as a union select null)
     union
     (select 1 union select distinct null::integer)
   )s)
  union
  (select * from (
     (select '1' union select null)
     union
     (select 1 union select distinct null::integer)
  )s2);

-- Yet, we keep behaviors on text-like columns
select pg_typeof(a) from(select 'foo' a union select 'foo'::name)s;
select pg_typeof(a) from(select 1 x, 'foo' a union
    select 1, 'foo' union select 1, 'foo'::name)s;
select pg_typeof(a) from(select 1 x, 'foo' a union
    (select 1, 'foo' union select 1, 'foo'::name))s;

CREATE TABLE union_ctas (a, b) AS SELECT 1, 2 UNION SELECT 1, 1 UNION SELECT 1, 1;
SELECT * FROM union_ctas;
DROP TABLE union_ctas;

-- MPP-21075: push quals below union
CREATE TABLE union_quals1 (a, b) AS SELECT i, i%2 from generate_series(1,10) i;
CREATE TABLE union_quals2 (a, b) AS SELECT i%2, i from generate_series(1,10) i;
SELECT * FROM (SELECT a, b from union_quals1 UNION SELECT b, a from union_quals2) as foo(a,b) where a > b order by a;
SELECT * FROM (SELECT a, max(b) over() from union_quals1 UNION SELECT * from union_quals2) as foo(a,b) where b > 6 order by a,b;

-- MPP-22266: different combinations of set operations and distinct
select * from ((select 1, 'A' from (select distinct 'B') as foo) union (select 1, 'C')) as bar;
select 1 union (select distinct null::integer union select '10');
select 1 union (select 2 from (select distinct null::integer union select 1) as x);
select 1 union (select distinct 10 from (select 1, 3.0 union select distinct 2, null::integer) as foo);
select 1 union (select distinct '10' from (select 1, 3.0 union select distinct 2, null::integer) as foo);
select distinct a from (select 'A' union select 'B') as foo(a);
select distinct a from (select distinct 'A' union select 'B') as foo(a);
select distinct a from (select distinct 'A' union select distinct 'B') as foo(a);
select distinct a from (select  'A' from (select distinct 'C' ) as bar union select distinct 'B') as foo(a);
select distinct a from (select  distinct 'A' from (select distinct 'C' ) as bar union select distinct 'B') as foo(a);
select distinct a from (select  distinct 'A' from (select 'C' from (select distinct 'D') as bar1 ) as bar union select distinct 'B') as foo(a);

-- Test case where input to one branch of UNION resides on a single segment, and another on the QE.
-- The external table resides on QD, and the LIMIT on the test1 table forces the plan to be focused
-- on a single QE.
--
CREATE TABLE test1 (id int);
insert into test1 values (1);
CREATE EXTERNAL WEB TABLE test2 (id int) EXECUTE 'echo 2' ON COORDINATOR FORMAT 'csv';

(SELECT 'test1' as branch, id FROM test1 LIMIT 1)
union
(SELECT 'test2' as branch, id FROM test2);

explain (SELECT 'test1' as branch, id FROM test1 LIMIT 1)
union
(SELECT 'test2' as branch, id FROM test2);

--
-- Test pulling up distribution key expression, when the different branches
-- of a UNION ALL have different typmods.
--
create table pullup_distkey_test(
    a character varying,
    b character varying(30)
) distributed by (b);

insert into pullup_distkey_test values ('foo', 'bar');

with base as
(
  select a, b from pullup_distkey_test
  union all
  select 'xx' as a, 'bar' as b
)
select a from base
union all
select a from base where a = 'foo';

--
-- Test union all two replicated tables with different numsegments
--
create table rep2(c1 int, c2 int) distributed replicated;
create table rep3(c1 int, c2 int) distributed replicated;
set allow_system_table_mods = on;
update gp_distribution_policy set numsegments = 2
  where localoid = 'rep2'::regclass;
select localoid::regclass, policytype, numsegments
  from gp_distribution_policy
  where localoid::regclass in ('rep2', 'rep3');
explain select * from rep2 union all select * from rep3;
select * from rep2 union all select * from rep3;
reset allow_system_table_mods;
drop table rep2;
drop table rep3;


--
-- Setup
--

--start_ignore
DROP TABLE IF EXISTS T_a1 CASCADE;
DROP TABLE IF EXISTS T_b2 CASCADE;
DROP TABLE IF EXISTS T_random CASCADE;
--end_ignore

CREATE TABLE T_a1 (a1 int, a2 int) DISTRIBUTED BY(a1);
INSERT INTO T_a1 SELECT i, i%5 from generate_series(1,10) i;

CREATE TABLE T_b2 (b1 int, b2 int) DISTRIBUTED BY(b2);
INSERT INTO T_b2 SELECT i, i%5 from generate_series(1,20) i;

CREATE TABLE T_random (c1 int, c2 int);
INSERT INTO T_random SELECT i, i%5 from generate_series(1,30) i;

--start_ignore
create language plpython3u;
--end_ignore

create or replace function count_operator(query text, operator text) returns int as
$$
rv = plpy.execute('EXPLAIN ' + query)
search_text = operator
result = 0
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        result = result+1
return result
$$
language plpython3u;

--
-- N-ary UNION ALL results
--

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select a1 from T_a1)
UNION ALL
(select b1 from T_b2)
UNION ALL
(select c1 from T_random)
UNION ALL
(select d1 from T_constant)
order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select b1 from T_b2)
UNION ALL
(select a1 from T_a1)
UNION ALL
(select c1 from T_random)
UNION ALL
(select d1 from T_constant)
order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select c1 from T_random)
UNION ALL
(select a1 from T_a1)
UNION ALL
(select b1 from T_b2)
UNION ALL
(select d1 from T_constant)
order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select d1 from T_constant)
UNION ALL
(select c1 from T_random)
UNION ALL
(select a1 from T_a1)
UNION ALL
(select b1 from T_b2)
order by 1;

--
-- N-ary UNION ALL explain
--

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select a1 from T_a1)
UNION ALL
(select b1 from T_b2)
UNION ALL
(select c1 from T_random)
UNION ALL
(select d1 from T_constant)
order by 1;'
, 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select b1 from T_b2)
UNION ALL
(select a1 from T_a1)
UNION ALL
(select c1 from T_random)
UNION ALL
(select d1 from T_constant)
order by 1;'
, 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select c1 from T_random)
UNION ALL
(select a1 from T_a1)
UNION ALL
(select b1 from T_b2)
UNION ALL
(select d1 from T_constant)
order by 1;'
, 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select d1 from T_constant)
UNION ALL
(select c1 from T_random)
UNION ALL
(select a1 from T_a1)
UNION ALL
(select b1 from T_b2)
order by 1;'
, 'APPEND');

--
-- N-ary UNION results
--

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select a1 from T_a1)
UNION
(select b1 from T_b2)
UNION
(select c1 from T_random)
UNION
(select d1 from T_constant)
order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select b1 from T_b2)
UNION
(select a1 from T_a1)
UNION
(select c1 from T_random)
UNION ALL
(select d1 from T_constant)
order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select c1 from T_random)
UNION
(select a1 from T_a1)
UNION
(select b1 from T_b2)
UNION ALL
(select d1 from T_constant)
order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select d1 from T_constant)
UNION ALL
(select c1 from T_random)
UNION
(select a1 from T_a1)
UNION
(select b1 from T_b2)
order by 1;

--
-- N-ary UNION explain
--

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select a1 from T_a1)
UNION
(select b1 from T_b2)
UNION
(select c1 from T_random)
UNION
(select d1 from T_constant)
order by 1;'
, 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select b1 from T_b2)
UNION
(select a1 from T_a1)
UNION
(select c1 from T_random)
UNION
(select d1 from T_constant)
order by 1;'
, 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select c1 from T_random)
UNION
(select a1 from T_a1)
UNION
(select b1 from T_b2)
UNION
(select d1 from T_constant)
order by 1;'
, 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select d1 from T_constant)
UNION
(select c1 from T_random)
UNION
(select a1 from T_a1)
UNION
(select b1 from T_b2)
order by 1;'
, 'APPEND');

--
-- Binary UNION ALL results
--

(select a1 from T_a1) UNION ALL (select b1 from T_b2) order by 1;

(select b1 from T_b2) UNION ALL (select a1 from T_a1) order by 1;

(select a1 from T_a1) UNION ALL (select c1 from T_random) order by 1;

(select c1 from T_random) UNION ALL (select a1 from T_a1) order by 1;

(select * from T_a1) UNION ALL (select * from T_b2) order by 1;

(select * from T_a1) UNION ALL (select * from T_random) order by 1;

(select * from T_b2) UNION ALL (select * from T_random) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select a1 from T_a1) UNION ALL (select d1 from T_constant) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select d1 from T_constant) UNION ALL (select a1 from T_a1) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select c1 from T_random) UNION ALL (select d1 from T_constant) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select d1 from T_constant) UNION ALL (select c1 from T_random) order by 1;

--
-- Binary UNION ALL explain
--

select count_operator('(select a1 from T_a1) UNION ALL (select b1 from T_b2) order by 1;', 'APPEND');

select count_operator('(select b1 from T_b2) UNION ALL (select a1 from T_a1) order by 1;', 'APPEND');

select count_operator('(select a1 from T_a1) UNION ALL (select c1 from T_random) order by 1;', 'APPEND');

select count_operator('(select c1 from T_random) UNION ALL (select a1 from T_a1) order by 1;', 'APPEND');

select count_operator('(select * from T_a1) UNION ALL (select * from T_b2) order by 1;', 'APPEND');

select count_operator('(select * from T_a1) UNION ALL (select * from T_random) order by 1;', 'APPEND');

select count_operator('(select * from T_b2) UNION ALL (select * from T_random) order by 1;', 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select a1 from T_a1) UNION ALL (select d1 from T_constant) order by 1;', 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select d1 from T_constant) UNION ALL (select a1 from T_a1) order by 1;', 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select c1 from T_random) UNION ALL (select d1 from T_constant) order by 1;', 'APPEND');

select count_operator('with T_constant (d1, d2) as(
SELECT 100, 100
UNION ALL SELECT 200, 200
UNION ALL SELECT 300, 300)
(select d1 from T_constant) UNION ALL (select c1 from T_random) order by 1;', 'APPEND');

--
-- Binary UNION results
--

(select a1 from T_a1) UNION (select b1 from T_b2) order by 1;

(select b1 from T_b2) UNION (select a1 from T_a1) order by 1;

(select a1 from T_a1) UNION (select c1 from T_random) order by 1;

(select c1 from T_random) UNION (select a1 from T_a1) order by 1;

(select * from T_a1) UNION (select * from T_b2) order by 1;

(select * from T_a1) UNION (select * from T_random) order by 1;

(select * from T_b2) UNION (select * from T_random) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select a1 from T_a1) UNION (select d1 from T_constant) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select d1 from T_constant) UNION (select a1 from T_a1) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select c1 from T_random) UNION (select d1 from T_constant) order by 1;

with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select d1 from T_constant) UNION (select c1 from T_random) order by 1;

--
-- Binary UNION explain
--

select count_operator('(select a1 from T_a1) UNION (select b1 from T_b2) order by 1;', 'APPEND');

select count_operator('(select b1 from T_b2) UNION (select a1 from T_a1) order by 1;', 'APPEND');

select count_operator('(select a1 from T_a1) UNION (select c1 from T_random) order by 1;', 'APPEND');

select count_operator('(select c1 from T_random) UNION (select a1 from T_a1) order by 1;', 'APPEND');

select count_operator('(select * from T_a1) UNION (select * from T_b2) order by 1;', 'APPEND');

select count_operator('(select * from T_a1) UNION (select * from T_random) order by 1;', 'APPEND');

select count_operator('(select * from T_b2) UNION (select * from T_random) order by 1;', 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select a1 from T_a1) UNION (select d1 from T_constant) order by 1;', 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select d1 from T_constant) UNION (select a1 from T_a1) order by 1;', 'APPEND');

select count_operator('
with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select c1 from T_random) UNION (select d1 from T_constant) order by 1;', 'APPEND');

select count_operator('with T_constant (d1, d2) as(
SELECT 100, 100
UNION SELECT 200, 200
UNION SELECT 300, 300)
(select d1 from T_constant) UNION (select c1 from T_random) order by 1;', 'APPEND');

CREATE TABLE t1_setop(a int) DISTRIBUTED BY (a);
CREATE TABLE t2_setop(a int) DISTRIBUTED BY (a);
INSERT INTO t1_setop VALUES (1), (2), (3);
INSERT INTO t2_setop VALUES (3), (4), (5);
(SELECT a FROM t1_setop EXCEPT SELECT a FROM t2_setop ORDER BY a)
UNION
(SELECT a FROM t2_setop EXCEPT SELECT a FROM t1_setop ORDER BY a)
ORDER BY a;

create table t1_ncols(a int, b int, c text, d date) distributed by (a);
create table t2_ncols(a smallint, b bigint, c varchar(20), d date) distributed by (c, b)
 partition by range (a) (start (0) end (8) every (4));
create view v1_ncols(id, a, b, c, d) as select 1,* from t1_ncols union all select 2,* from t2_ncols;

insert into t1_ncols values (1, 11, 'one', '2001-01-01');

insert into t2_ncols values (2, 22, 'two', '2002-02-02');
insert into t2_ncols values (4, 44, 'four','2004-04-04');

select b from t1_ncols union all select a from t2_ncols;
select a+100, b, d from t1_ncols union select b, a+200, d from t2_ncols order by 1;
select c, a from v1_ncols;

with cte1(aa, b, c, d) as (select a*100, b, c, d from t1_ncols union select * from t2_ncols)
select x.aa/100 aaa, x.c, y.c from cte1 x join cte1 y on x.aa=y.aa;

select from t2_ncols union select * from t2_ncols;

-- Test the result of union of 2 tables distributed on different number of segments
-- start_ignore
drop schema if exists union_schema CASCADE;
-- end_ignore
create schema union_schema;
create table union_schema.t1(a int, b int);
create table union_schema.t2(a int, b int);
create table union_schema.t3(a int, b int);

set allow_system_table_mods = on;
update gp_distribution_policy set numsegments = 1
  where localoid = 'union_schema.t1'::regclass::oid;
update gp_distribution_policy set numsegments = 2
  where localoid = 'union_schema.t2'::regclass::oid;
select relname, policytype, numsegments, distkey
  from pg_class, gp_distribution_policy, pg_namespace ns
  where pg_class.oid = localoid and relnamespace = ns.oid
    and nspname = 'union_schema'
    and relname in ('t1', 't2', 't3')
  order by relname;

insert into union_schema.t1 select i, i from generate_series(1,10)i;
insert into union_schema.t2 select i, i from generate_series(1,20)i;
analyze union_schema.t1;
analyze union_schema.t2;

explain select * from union_schema.t1 join union_schema.t2
    on union_schema.t1.a = union_schema.t2.b;
explain select union_schema.t1.a, union_schema.t2.b
  from union_schema.t1 join union_schema.t2
    on union_schema.t1.a = union_schema.t2.b
  union all
  select * from union_schema.t3;

select * from union_schema.t1 join union_schema.t2
  on union_schema.t1.a = union_schema.t2.b;
select union_schema.t1.a, union_schema.t2.b
  from union_schema.t1 join union_schema.t2
    on union_schema.t1.a = union_schema.t2.b
union all
select * from union_schema.t3;

truncate union_schema.t1, union_schema.t2;
insert into union_schema.t1 select i, i from generate_series(1,20)i;
insert into union_schema.t2 select i, i from generate_series(1,10)i;
analyze union_schema.t1;
analyze union_schema.t2;

explain select * from union_schema.t1 join union_schema.t2
    on union_schema.t1.a = union_schema.t2.b;
explain select union_schema.t1.a, union_schema.t2.b
    from union_schema.t1 join union_schema.t2
	  on union_schema.t1.a = union_schema.t2.b
  union all
  select * from union_schema.t3;

select * from union_schema.t1 join union_schema.t2
  on union_schema.t1.a = union_schema.t2.b;
select union_schema.t1.a, union_schema.t2.b
  from union_schema.t1 join union_schema.t2
    on union_schema.t1.a = union_schema.t2.b
union all
select * from union_schema.t3;

reset allow_system_table_mods;

-- The following tests demonstrate the plan alternative
-- where ORCA requests union all's outer child to become
-- a Non-Singleton. This alternative is at times costed
-- lower than aligning the inner child's distribution spec
-- with that delivered by the outer child. 
-- 
-- Replicated ∪ Hashed
create table rep (a int) distributed replicated;
insert into rep select i from generate_series (1, 10) i; 
create table dist (a int);
insert into dist select i from generate_series (1, 1000) i;
analyze dist;
analyze rep;
-- It's more cost-effective to apply a duplicate-sensitive
-- random motion (non-phyiscal) on a replicated table, 
-- than a broadcast motion on a distributed table.
explain select a from rep union all select a from dist;

-- Universal ∪ Random
create table rand (a int) distributed randomly;
insert into rand select i from generate_series (1, 10000) i;
analyze rand;
-- It's more cost-effective to apply a duplicate-sensitive
-- random motion (non-physical) on a universal TVF, than a
-- gather motion on a randomly distributed table.
explain select i from generate_series(1,1000) i union all select a from rand;

-------------------------------------------------------------------------------
--Test case to check parallel union all with 'json' type 1st column in project list
-------------------------------------------------------------------------------
set optimizer_parallel_union to on;
drop table if exists my_table;
create table my_table ( id serial  primary key, json_data json);
insert into my_table (json_data) values ('{"name": "Name1", "age": 10}');
insert into my_table (json_data) values ('{"name": "Name2", "age": 20}');
insert into my_table (json_data) values ('{"name": "Name3", "age": 30}');
insert into my_table (json_data) values ('{"name": "Name4", "age": 40}');

explain select json_data from my_table  where json_data->>'age' = '30' union all select json_data from my_table where json_data->>'age' = '40' ;
select json_data from my_table  where json_data->>'age' = '30' union all select json_data from my_table where json_data->>'age' = '40' ;

explain select json_data,id from my_table  where json_data->>'age' = '30' union all select json_data,id from my_table where json_data->>'age' = '40' ;
select json_data,id from my_table  where json_data->>'age' = '30' union all select json_data,id from my_table where json_data->>'age' = '40' ;

set optimizer_parallel_union to off;
drop table if exists my_table;

--
-- Clean up
--

DROP TABLE IF EXISTS T_a1 CASCADE;
DROP TABLE IF EXISTS T_b2 CASCADE;
DROP TABLE IF EXISTS T_random CASCADE;
DROP VIEW IF EXISTS v1_ncols CASCADE;
DROP TABLE IF EXISTS t1_ncols CASCADE;
DROP TABLE IF EXISTS t2_ncols CASCADE;
DROP SCHEMA IF EXISTS union_schema CASCADE;
