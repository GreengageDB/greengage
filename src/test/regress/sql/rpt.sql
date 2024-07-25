--
-- Basic tests for replicated table
--
-- start_matchsubs
-- m/\(cost=.*\)/
-- s/\(cost=.*\)//
-- end_matchsubs
create schema rpt;
set search_path to rpt;

---------
-- INSERT
---------
create table foo (x int, y int) distributed replicated;
create table foo1(like foo) distributed replicated;
create table bar (like foo) distributed randomly;
create table bar1 (like foo) distributed by (x);

-- values --> replicated table 
-- random partitioned table --> replicated table
-- hash partitioned table --> replicated table
-- singleQE --> replicated table
-- replicated --> replicated table
insert into bar values (1, 1), (3, 1);
insert into bar1 values (1, 1), (3, 1);
insert into foo1 values (1, 1), (3, 1);
insert into foo select * from bar;
insert into foo select * from bar1;
insert into foo select * from bar order by x limit 1;
insert into foo select * from foo;

select * from foo order by x;
select bar.x, bar.y from bar, (select * from foo) as t1 order by 1,2;
select bar.x, bar.y from bar, (select * from foo order by x limit 1) as t1 order by 1,2;

truncate foo;
truncate foo1;
truncate bar;
truncate bar1;

-- replicated table --> random partitioned table
-- replicated table --> hash partitioned table
insert into foo values (1, 1), (3, 1);
insert into bar select * from foo order by x limit 1;
insert into bar1 select * from foo order by x limit 1;

select * from foo order by x;
select * from bar order by x;
select * from bar1 order by x;

drop table if exists foo;
drop table if exists foo1;
drop table if exists bar;
drop table if exists bar1;

--
-- CREATE UNIQUE INDEX
--
-- create unique index on non-distributed key.
create table foo (x int, y int) distributed replicated;
create table bar (x int, y int) distributed randomly;

-- success
create unique index foo_idx on foo (y);
-- should fail
create unique index bar_idx on bar (y);

drop table if exists foo;
drop table if exists bar;

--
-- CREATE TABLE with both PRIMARY KEY and UNIQUE constraints
--
create table foo (id int primary key, name text unique) distributed replicated;

-- success
insert into foo values (1,'aaa');
insert into foo values (2,'bbb');

-- fail
insert into foo values (1,'ccc');
insert into foo values (3,'aaa');

drop table if exists foo;

--
-- CREATE TABLE
--
--
-- Like
CREATE TABLE parent (
        name            text,
        age                     int4,
        location        point
) distributed replicated;

CREATE TABLE child (like parent) distributed replicated;
CREATE TABLE child1 (like parent) DISTRIBUTED BY (name);
CREATE TABLE child2 (like parent);

-- should be replicated table
\d child
-- should distributed by name
\d child1
-- should be replicated table
\d child2

drop table if exists parent;
drop table if exists child;
drop table if exists child1;
drop table if exists child2;

-- Inherits
CREATE TABLE parent_rep (
        name            text,
        age                     int4,
        location        point
) distributed replicated;

CREATE TABLE parent_part (
        name            text,
        age                     int4,
        location        point
) distributed by (name);

-- inherits from a replicated table, should fail
CREATE TABLE child (
        salary          int4,
        manager         name
) INHERITS (parent_rep);

-- replicated table can not have parents, should fail
CREATE TABLE child (
        salary          int4,
        manager         name
) INHERITS (parent_part) DISTRIBUTED REPLICATED;

drop table if exists parent_rep;
drop table if exists parent_part;
drop table if exists child;

--
-- CTAS
--
-- CTAS from generate_series
create table foo as select i as c1, i as c2
from generate_series(1,3) i distributed replicated;

-- CTAS from replicated table 
create table bar as select * from foo distributed replicated;
select * from bar;

drop table if exists foo;
drop table if exists bar;

-- CTAS from partition table table
create table foo as select i as c1, i as c2
from generate_series(1,3) i;

create table bar as select * from foo distributed replicated;
select * from bar;

drop table if exists foo;
drop table if exists bar;

-- CTAS from singleQE 
create table foo as select i as c1, i as c2
from generate_series(1,3) i;
select * from foo;

create table bar as select * from foo order by c1 limit 1 distributed replicated;
select * from bar;

drop table if exists foo;
drop table if exists bar;

-- Create view can work
create table foo(x int, y int) distributed replicated;
insert into foo values(1,1);

create view v_foo as select * from foo;
select * from v_foo;

drop view v_foo;
drop table if exists foo;

---------
-- Alter
--------
-- Drop distributed key column
create table foo(x int, y int) distributed replicated;
create table bar(like foo) distributed by (x);

insert into foo values(1,1);
insert into bar values(1,1);

-- success
alter table foo drop column x;
-- fail
alter table bar drop column x;

drop table if exists foo;
drop table if exists foo1;
drop table if exists bar;
drop table if exists bar1;

-- Alter gp_distribution_policy
create table foo(x int, y int) distributed replicated;
create table foo1(x int, y int) distributed replicated;
create table bar(x int, y int) distributed by (x);
create table bar1(x int, y int) distributed randomly;

insert into foo select i,i from generate_series(1,10) i;
insert into foo1 select i,i from generate_series(1,10) i;
insert into bar select i,i from generate_series(1,10) i;
insert into bar1 select i,i from generate_series(1,10) i;

-- alter distribution policy of replicated table
alter table foo set distributed by (x);
alter table foo1 set distributed randomly;
-- alter a partitioned table to replicated table
alter table bar set distributed replicated;
alter table bar1 set distributed replicated;

-- verify the new policies
\d foo
\d foo1
\d bar
\d bar1

-- verify the reorganized data
select * from foo;
select * from foo1;
select * from bar;
select * from bar1;

-- alter back
alter table foo set distributed replicated;
alter table foo1 set distributed replicated;
alter table bar set distributed by (x);
alter table bar1 set distributed randomly;

-- verify the policies again
\d foo
\d foo1
\d bar
\d bar1

-- verify the reorganized data again
select * from foo;
select * from foo1;
select * from bar;
select * from bar1;

drop table if exists foo;
drop table if exists foo1;
drop table if exists bar;
drop table if exists bar1;

---------
-- UPDATE / DELETE
---------
create table foo(x int, y int) distributed replicated;
create table bar(x int, y int);
insert into foo values (1, 1), (2, 1);
insert into bar values (1, 2), (2, 2);
update foo set y = 2 where y = 1;
select * from foo;
update foo set y = 1 from bar where bar.y = foo.y;
select * from foo;
delete from foo where y = 1;
select * from foo;

-- Test replicate table within init plan
insert into foo values (1, 1), (2, 1);
select * from bar where exists (select * from foo);

------
-- Test Current Of is disabled for replicated table
------
begin;
declare c1 cursor for select * from foo;
fetch 1 from c1;
delete from foo where current of c1;
abort;

begin;
declare c1 cursor for select * from foo;
fetch 1 from c1;
update foo set y = 1 where current of c1;
abort;

-----
-- Test updatable view works for replicated table
----
truncate foo;
truncate bar;
insert into foo values (1, 1);
insert into foo values (2, 2);
insert into bar values (1, 1);
create view v_foo as select * from foo where y = 1;
begin;
update v_foo set y = 2; 
select * from gp_dist_random('foo');
abort;

update v_foo set y = 3 from bar where bar.y = v_foo.y; 
select * from gp_dist_random('foo');
-- Test gp_segment_id for replicated table
-- gp_segment_id is ambiguous for replicated table, it's been disabled now.
create table baz (c1 int, c2 int) distributed replicated;
create table qux (c1 int, c2 int);

select gp_segment_id from baz;
select xmin from baz;
select xmax from baz;
select ctid from baz;
select * from baz where c2 = gp_segment_id;
select * from baz, qux where baz.c1 = gp_segment_id;
update baz set c2 = gp_segment_id;
update baz set c2 = 1 where gp_segment_id = 1;
update baz set c2 = 1 from qux where gp_segment_id = baz.c1;
insert into baz select i, i from generate_series(1, 1000) i;
vacuum baz;
vacuum full baz;
analyze baz;

-- Test dependencies check when alter table to replicated table
create view v_qux as select ctid from qux;
alter table qux set distributed replicated;
drop view v_qux;
alter table qux set distributed replicated;

-- Test cursor for update also works for replicated table
create table cursor_update (c1 int, c2 int) distributed replicated;
insert into cursor_update select i, i from generate_series(1, 10) i;
begin;
declare c1 cursor for select * from cursor_update order by c2 for update;
fetch next from c1;
end;

-- Test MinMax path on replicated table
create table minmaxtest (x int, y int) distributed replicated;
create index on minmaxtest (x);
insert into minmaxtest select generate_series(1, 10);
set enable_seqscan=off;
select min(x) from minmaxtest;

-- Test replicated on partition table
-- should fail
CREATE TABLE foopart (a int4, b int4) DISTRIBUTED REPLICATED PARTITION BY RANGE (a) (START (1) END (10));
CREATE TABLE foopart (a int4, b int4) PARTITION BY RANGE (a) (START (1) END (10)) ;
-- should fail
ALTER TABLE foopart SET DISTRIBUTED REPLICATED;
ALTER TABLE foopart_1_prt_1 SET DISTRIBUTED REPLICATED;
DROP TABLE foopart;

-- Test that replicated table can't inherit a parent table, and it also
-- can't be inherited by a child table.
-- 1. Replicated table can't inherit a parent table.
CREATE TABLE parent (t text) DISTRIBUTED BY (t);
-- This is not allowed: should fail
CREATE TABLE child () INHERITS (parent) DISTRIBUTED REPLICATED;

CREATE TABLE child (t text) DISTRIBUTED REPLICATED;
-- should fail
ALTER TABLE child INHERIT parent;
DROP TABLE child, parent;

-- 2. Replicated table can't be inherited
CREATE TABLE parent (t text) DISTRIBUTED REPLICATED;
-- should fail
CREATE TABLE child () INHERITS (parent) DISTRIBUTED REPLICATED;
CREATE TABLE child () INHERITS (parent) DISTRIBUTED BY (t);

CREATE TABLE child (t text) DISTRIBUTED REPLICATED;
ALTER TABLE child INHERIT parent;

CREATE TABLE child2(t text) DISTRIBUTED BY (t);
ALTER TABLE child2 INHERIT parent;

DROP TABLE child, child2, parent;

-- volatile replicated
-- General and segmentGeneral locus imply that if the corresponding
-- slice is executed in many different segments should provide the
-- same result data set. Thus, in some cases, General and segmentGeneral
-- can be treated like broadcast. But if the segmentGeneral and general
-- locus path contain volatile functions, they lose the property and
-- can only be treated as singleQE. The following cases are to check that
-- we correctly handle all these cases.

-- FIXME: ORCA does not consider this, we need to fix the cases when ORCA
-- consider this.
create table t_hashdist(a int, b int, c int) distributed by (a);
create table t_replicate_volatile(a int, b int, c int) distributed replicated;

---- pushed down filter
explain (costs off) select * from t_replicate_volatile, t_hashdist where t_replicate_volatile.a > random();

-- join qual
explain (costs off) select * from t_hashdist, t_replicate_volatile x, t_replicate_volatile y where x.a + y.a > random();

-- sublink & subquery
explain (costs off) select * from t_hashdist where a > All (select random() from t_replicate_volatile);
explain (costs off) select * from t_hashdist where a in (select random()::int from t_replicate_volatile);

-- subplan
explain (costs off, verbose) select * from t_hashdist left join t_replicate_volatile on t_hashdist.a > any (select random() from t_replicate_volatile);

-- targetlist
explain (costs off) select * from t_hashdist cross join (select random () from t_replicate_volatile)x;
explain (costs off) select * from t_hashdist cross join (select a, sum(random()) from t_replicate_volatile group by a) x;
explain (costs off) select * from t_hashdist cross join (select random() as k, sum(a) from t_replicate_volatile group by k) x;
explain (costs off) select * from t_hashdist cross join (select a, sum(b) as s from t_replicate_volatile group by a having sum(b) > random() order by a) x ;

-- insert
explain (costs off) insert into t_replicate_volatile select random() from t_replicate_volatile;
explain (costs off) insert into t_replicate_volatile select random(), a, a from generate_series(1, 10) a;
create sequence seq_for_insert_replicated_table;
explain (costs off) insert into t_replicate_volatile select nextval('seq_for_insert_replicated_table');
explain (costs off) select a from t_replicate_volatile union all select * from nextval('seq_for_insert_replicated_table');

-- CTAS
explain (costs off) create table rpt_ctas as select random() from generate_series(1, 10) distributed replicated;
explain (costs off) create table rpt_ctas as select a from generate_series(1, 10) a group by a having sum(a) > random() distributed replicated;

-- update & delete
explain (costs off) update t_replicate_volatile set a = 1 where b > random();
explain (costs off) update t_replicate_volatile set a = 1 from t_replicate_volatile x where x.a + random() = t_replicate_volatile.b;
explain (costs off) update t_replicate_volatile set a = 1 from t_hashdist x where x.a + random() = t_replicate_volatile.b;
explain (costs off) delete from t_replicate_volatile where a < random();
explain (costs off) delete from t_replicate_volatile using t_replicate_volatile x where t_replicate_volatile.a + x.b < random();
explain (costs off) update t_replicate_volatile set a = random();

-- limit
explain (costs off) insert into t_replicate_volatile select * from t_replicate_volatile limit random();
explain (costs off) select * from t_hashdist cross join (select * from t_replicate_volatile limit random()) x;

-- ORCA
-- verify that JOIN derives the inner child distribution if the outer is tainted replicated (in this
-- case, the inner child is the hash distributed table, but the distribution is random because the
-- hash distribution key is not the JOIN key. we want to return the inner distribution because the
-- JOIN key determines the distribution of the JOIN output).
create table dist_tab (a integer, b integer) distributed by (a);
create table rep_tab (c integer) distributed replicated;
create index idx on dist_tab (b);
insert into dist_tab values (1, 2), (2, 2), (2, 1), (1, 1);
insert into rep_tab values (1), (2);
analyze dist_tab;
analyze rep_tab;
set optimizer_enable_hashjoin=off;
set enable_hashjoin=off;
set enable_nestloop=on;
explain select b from dist_tab where b in (select distinct c from rep_tab);
select b from dist_tab where b in (select distinct c from rep_tab);
reset optimizer_enable_hashjoin;
reset enable_hashjoin;
reset enable_nestloop;

create table rand_tab (d integer) distributed randomly;
insert into rand_tab values (1), (2);
analyze rand_tab;

-- Table	Side		Derives
-- rep_tab	pdsOuter	EdtTaintedReplicated
-- rep_tab	pdsInner	EdtHashed
--
-- join derives EdtHashed
explain select c from rep_tab where c in (select distinct c from rep_tab);
select c from rep_tab where c in (select distinct c from rep_tab);

-- Table	Side		Derives
-- dist_tab	pdsOuter	EdtHashed
-- rep_tab	pdsInner	EdtTaintedReplicated 
--
-- join derives EdtHashed
explain select a from dist_tab where a in (select distinct c from rep_tab);
select a from dist_tab where a in (select distinct c from rep_tab);

-- Table	Side		Derives
-- rand_tab	pdsOuter	EdtRandom
-- rep_tab	pdsInner	EdtTaintedReplicated
--
-- join derives EdtRandom
explain select d from rand_tab where d in (select distinct c from rep_tab);
select d from rand_tab where d in (select distinct c from rep_tab);

-- Table	Side		Derives
-- rep_tab	pdsOuter	EdtTaintedReplicated
-- dist_tab	pdsInner	EdtHashed
--
-- join derives EdtHashed
explain select c from rep_tab where c in (select distinct a from dist_tab);
select c from rep_tab where c in (select distinct a from dist_tab);

-- Table	Side		Derives
-- rep_tab	pdsOuter	EdtTaintedReplicated
-- rand_tab	pdsInner	EdtHashed
--
-- join derives EdtHashed
explain select c from rep_tab where c in (select distinct d from rand_tab);
select c from rep_tab where c in (select distinct d from rand_tab);

-- Github Issue 13532
create table t1_13532(a int, b int) distributed replicated;
create table t2_13532(a int, b int) distributed replicated;
create index idx_t2_13532 on t2_13532(b);
explain (costs off) select * from t1_13532 x, t2_13532 y where y.a < random() and x.b = y.b;
set enable_bitmapscan = off;
explain (costs off) select * from t1_13532 x, t2_13532 y where y.a < random() and x.b = y.b;

-- test for optimizer_enable_replicated_table
explain (costs off) select * from rep_tab;
set optimizer_enable_replicated_table=off;
set optimizer_trace_fallback=on;
explain (costs off) select * from rep_tab;
reset optimizer_trace_fallback;
reset optimizer_enable_replicated_table;

-- Ensure plan with Gather Motion node is generated.
drop table if exists t;
create table t (i int, j int) distributed replicated;
insert into t values (1, 2);
explain (costs off) select j, (select j) AS "Correlated Field" from t;
select j, (select j) AS "Correlated Field" from t;
explain (costs off) select j, (select 5) AS "Uncorrelated Field" from t;
select j, (select 5) AS "Uncorrelated Field" from t;

-- start_ignore
drop schema rpt cascade;
-- end_ignore
