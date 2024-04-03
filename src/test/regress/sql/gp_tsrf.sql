--
-- targetlist set returning function tests
--

-- SRF is not under any other expression --
explain verbose select generate_series(1,4) as x;
select generate_series(1,4) as x;

-- SRF is present under a FUNCEXPR which is not a SRF
explain verbose select abs(generate_series(-5,-1)) as absolute;
select abs(generate_series(-5,-1)) as absolute;

-- SRF is present under a OPEXPR(+)
explain verbose select generate_series(1,4)+1 as output;
select generate_series(1,4)+1 as output;

-- SRF is present under an SRF expression
explain verbose select generate_series(generate_series(1,3),4);
select generate_series(generate_series(1,3),4) as output;

-- The inner SRF is present under an OPEXPR which in turn is under an SRF
explain verbose select generate_series(generate_series(1,2)+1,4) as output;
select generate_series(generate_series(1,2)+1,4) as output;

-- The outer SRF is present under an OPEXPR
explain verbose select generate_series(generate_series(1,2),4)+1 as output;
select generate_series(generate_series(1,2),4)+1 as output;

-- Both inner and outer SRF are present under OPEXPR
explain verbose select generate_series(generate_series(1,2)+1,4)+1 as output;
select generate_series(generate_series(1,2)+1,4)+1 as output;
explain verbose select generate_series(1,3)+1 as x from (select generate_series(1, 3)) as y;
select generate_series(1,3)+1 as x from (select generate_series(1, 3)) as y;

create table test_srf(a int,b int,c int) distributed by (a);
insert into test_srf values(2,2,2);
insert into test_srf values(3,2,2);
explain verbose select generate_series(1,a) as output,b,c from test_srf;
select generate_series(1,a) as output,b,c from test_srf;
explain verbose select generate_series(1,a+1),b+generate_series(1,4),c from test_srf;
select generate_series(1,a+1),b+generate_series(1,4),c from test_srf;
drop table test_srf;

-- Test that the preprocessor step where
-- IN subquery is converted to EXIST subquery with a predicate,
-- is not happening if inner sub query is SRF
-- Fixed as part of github issue #15644

explain verbose SELECT a IN (SELECT generate_series(1,a)) AS x FROM (SELECT generate_series(1, 3) AS a) AS s;
SELECT a IN (SELECT generate_series(1,a)) AS x FROM (SELECT generate_series(1, 3) AS a) AS s;

SELECT a FROM (values(1),(2),(3)) as t(a) where a IN (SELECT generate_series(1,a));
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT a FROM (values(1),(2),(3)) as t(a) where a IN (SELECT generate_series(1,a));

CREATE TABLE t_outer (a int, b int) DISTRIBUTED BY (a);
INSERT INTO t_outer SELECT i, i+1 FROM generate_series(1,3) as i;  
CREATE TABLE t_inner (a int, b int) DISTRIBUTED BY (a);
INSERT INTO t_inner SELECT i, i+1 FROM generate_series(1,3) as i;

SELECT * FROM t_outer WHERE t_outer.b IN (SELECT generate_series(1, t_outer.b) FROM t_inner);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM t_outer WHERE t_outer.b IN (SELECT generate_series(1, t_outer.b)  FROM t_inner);
DROP TABLE t_outer, t_inner;

-- Check for proper resource deallocation for SRF which has been squelched

-- start_ignore
drop table if exists ao1_srf_test;
drop table if exists ao2_srf_test;
drop table if exists srf_test_t1;
-- end_ignore

create table ao1_srf_test (a int primary key) with (appendonly=true);
insert into ao1_srf_test values (1);
select (gp_toolkit.__gp_aoblkdir('ao1_srf_test'::regclass)).* from gp_dist_random('gp_id') limit 1;

-- Check that SRF squelch performs when rescan is happens

create table ao2_srf_test (a int primary key) with (appendonly=true);

insert into ao1_srf_test select a from generate_series(2, 10000)a;
insert into ao2_srf_test select a from generate_series(1, 10000)a;

create table srf_test_t1(a oid primary key);

insert into srf_test_t1 values ('ao1_srf_test'::regclass::oid), ('ao2_srf_test'::regclass::oid);

select * from srf_test_t1 where a in 
       (select (gp_toolkit.__gp_aoblkdir(srf_test_t1.a)).row_count 
        from gp_dist_random('gp_id') limit 1);

drop table ao1_srf_test;
drop table ao2_srf_test;
drop table srf_test_t1;


-- Check various SRFs switched to squenched Value-Per-Call
-- start_ignore
drop table if exists test_ao1;
-- end_ignore

create table test_ao1(i int) with (appendonly=true) distributed by (i);
insert into test_ao1 values (generate_series(1,1000));
select count(*) from (select get_ao_distribution('test_ao1') limit 1) sdist;
drop table test_ao1;


-- start_ignore
drop table if exists test_ao2;
-- end_ignore

create table test_ao2 (a int, b int) with (appendonly=true, orientation=column) distributed by(a);
insert into test_ao2 select i, i from generate_series(1, 10) i;
update test_ao2 set b = 100 where a in (2, 5);
delete from test_ao2 where a in (4, 8);
select (gp_toolkit.__gp_aovisimap('test_ao2'::regclass)).* from gp_dist_random('gp_id') limit 1;

select count (*) from (
  select (gp_toolkit.__gp_aovisimap_entry('test_ao2'::regclass)).* from gp_dist_random('gp_id') limit 1) vme1;

select count(*) from (select * from (select gp_toolkit.__gp_aovisimap_hidden_info('test_ao2'::regclass)) hi limit 1) hi1;

drop table test_ao2;

-- start_ignore
drop table if exists test_ao3;
-- end_ignore

create table test_ao3(id int, key int) distributed by(id);

insert into test_ao3 values(1,2),(2,3),(3,4);

select count(*) from (select * from (select pg_catalog.gp_acquire_sample_rows('test_ao3'::regclass, 400, 'f')) ss limit 1) ss1;

drop table test_ao3;
