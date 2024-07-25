--  MPP-21536: Duplicated rows inserted when ORCA is turned on
-- start_matchsubs
-- m/\(cost=.*\)/
-- s/\(cost=.*\)//
-- end_matchsubs

create schema bfv_dml;
set search_path=bfv_dml;

-- create test table
create table m();
alter table m add column a int;
alter table m add column b int;

-- generate data for m
insert into m select i, i%5 from generate_series(1,10)i;

-- INSERT and UPDATE
create table yyy(a int, b int) distributed randomly;

insert into yyy select a,b from m;
select * from yyy order by 1, 2;

update yyy set a=m.b from m where m.a=yyy.b;
select * from yyy order by 1, 2;

drop table yyy;


-- UPDATE with different values
create table yyy(a int, b int) distributed randomly;

insert into yyy select a,b from m;
update yyy set b=m.b from m where m.a=yyy.a;
select * from yyy order by 1, 2;

drop table yyy;


-- DELETE
create table yyy(a int, b int) distributed randomly;

insert into yyy select a,b from m;
delete from yyy where a in (select a from m);
select * from yyy order by 1, 2;

drop table yyy;

create table yyy(a int, b int) distributed randomly;
insert into yyy select a,b from m;
delete from yyy where b in (select a from m);
select * from yyy order by 1, 2;

drop table yyy;


-- Now repeat all the above tests, but using a hacked master-only 'm' table
drop table m;

set optimizer_enable_coordinator_only_queries=on;


-- create master-only table
create table m();
set allow_system_table_mods=true;
delete from gp_distribution_policy where localoid='m'::regclass;
reset allow_system_table_mods;
alter table m add column a int;
alter table m add column b int;

-- generate data for m
insert into m select i, i%5 from generate_series(1,10)i;

create table zzz(a int primary key, b int) distributed by (a);
insert into zzz select a,b from m;
select * from zzz order by 1, 2;

delete from zzz where a in (select a from m);
select * from zzz order by 1, 2;

drop table zzz;

create table zzz(a int primary key, b int) distributed by (a);

insert into zzz select a,b from m;
delete from zzz where b in (select a from m);
select * from zzz order by 1, 2;

drop table zzz;

create table zzz(a int primary key, b int) distributed by (a);
insert into zzz select a,b from m;

-- This update fails with duplicate key error, but it varies which segment
-- reports it first, i.e. it varies which row it complaints first. Silence
-- that difference in the error DETAIL line
\set VERBOSITY terse
update zzz set a=m.b from m where m.a=zzz.b;
select * from zzz order by 1, 2;

drop table zzz;

create table zzz(a int primary key, b int) distributed by (a);
insert into zzz select a,b from m;
update zzz set b=m.b from m where m.a=zzz.a;
select * from zzz order by 1, 2;

drop table zzz;
drop table m;


-- MPP-21622 Update with primary key: only sort if the primary key is updated
--
-- Aside from testing that bug, this also tests EXPLAIN of an DMLActionExpr
-- that ORCA generates for plans that update the primary key.
create table update_pk_test (a int primary key, b int) distributed by (a);
insert into update_pk_test values(1,1);

explain update update_pk_test set b = 5;
update update_pk_test set b = 5;
select * from update_pk_test order by 1,2;

explain update update_pk_test set a = 5;
update update_pk_test set a = 5;
select * from update_pk_test order by 1,2;


-- MPP-22599 DML queries that fallback to planner don't check for updates on
-- the distribution key.
--
-- So the bug was that if ORCA fell back to the planner, then the usual
-- check that prohibits updating the distribution key columns was not
-- performed like it should. So the idea of this test is to have an UPDATE
-- on distribution key column, with some features in the table or the query,
-- such that ORCA cannot produce a plan and it falls back to the Postgres
-- planner.
set optimizer_trace_fallback = on;

-- Subquery that returns a row rather than a single scalar isn't supported
-- in ORCA currently, so we can use that to trigger fallback.
update update_pk_test set a=1 where row(1,2) = (SELECT 1, 2);
select * from update_pk_test order by 1,2;
reset optimizer_trace_fallback;


--
-- Check that INSERT and DELETE triggers don't fire on UPDATE.
--
-- It may seem weird how that could happen, but with ORCA, UPDATEs are
-- implemented as a "split update", which is really a DELETE and an INSERT.
--
CREATE TABLE bfv_dml_trigger_test (id int4, t text);

INSERT INTO bfv_dml_trigger_test VALUES (1, 'foo');

CREATE OR REPLACE FUNCTION bfv_dml_error_func() RETURNS trigger AS
$$
BEGIN
   RAISE EXCEPTION 'trigger was called!';
   RETURN NEW;
END
$$ LANGUAGE 'plpgsql';

CREATE TRIGGER before_trigger BEFORE INSERT or DELETE ON bfv_dml_trigger_test
FOR EACH ROW
EXECUTE PROCEDURE bfv_dml_error_func();

CREATE TRIGGER after_trigger AFTER INSERT or DELETE ON bfv_dml_trigger_test
FOR EACH ROW
EXECUTE PROCEDURE bfv_dml_error_func();

UPDATE bfv_dml_trigger_test SET t = 'bar';
UPDATE bfv_dml_trigger_test SET id = id + 1;

--
-- Verify that ExecInsert doesn't scribble on the old tuple, when the new
-- tuple comes directly from the old table.
--
CREATE TABLE execinsert_test (id int4, t text) DISTRIBUTED BY (id);

INSERT INTO execinsert_test values (1, 'foo');

-- Insert another identical tuple, but roll it back. If the insertion
-- incorrectly modified the xmin on the old tuple, then it will become
-- invisible when we roll back.
begin;
INSERT INTO execinsert_test select * FROM execinsert_test;
rollback;
select * from execinsert_test;

drop table execinsert_test;

-- Repeat with a hacked master-only table, just in case the planner decides
-- to add a Motion node or something that hides the problem otherwise.

CREATE TABLE execinsert_test (id int4, t text) DISTRIBUTED BY (id);
set allow_system_table_mods=true;
delete from gp_distribution_policy where localoid='execinsert_test'::regclass;
reset allow_system_table_mods;

INSERT INTO execinsert_test values (1, 'foo');
begin;
INSERT INTO execinsert_test select * FROM execinsert_test;
rollback;
select * from execinsert_test;

drop table execinsert_test;

--
-- Verify that DELETE properly redistributes in the case of joins
--

drop table if exists foo;
drop table if exists bar;

create table foo (a int, b int);
create table bar(a int, b int);
insert into foo select generate_series(1,10);
insert into bar select generate_series(1,10);
-- Previously, table foo is defined as  randomly distributed and 
-- that might lead to flaky result of the explain statement
-- since random cost. We set policy to random without move the
-- data after data is all inserted. This method can both have
-- a random dist table and a stable test result.
-- Following cases are using the same skill here.
alter table foo set with(REORGANIZE=false) distributed randomly;
analyze foo;
analyze bar;
explain delete from foo using bar where foo.a=bar.a;
delete from foo using bar where foo.a=bar.a;
select * from foo;
drop table foo;
drop table bar;

create table foo (a int, b int);
create table bar(a int, b int);
insert into foo select generate_series(1,10);
insert into bar select generate_series(1,10);
alter table foo set with(REORGANIZE=false) distributed randomly;
analyze foo;
analyze bar;
explain delete from foo using bar where foo.a = bar.a returning foo.*;
delete from foo using bar where foo.a = bar.a returning foo.*;
select * from foo;
drop table foo;
drop table bar;

create table foo (a int, b int);
insert into foo select generate_series(1,10);
alter table foo set with(REORGANIZE=false) distributed randomly;
analyze foo;
explain delete from foo where foo.a=1;
delete from foo where foo.a=1;
drop table foo;

create table foo (a int, b int);
create table bar(a int, b int);
insert into foo select generate_series(1,10);
insert into bar select generate_series(1,10);
alter table foo set with(REORGANIZE=false) distributed randomly;
analyze foo;
analyze bar;
explain delete from foo using bar where foo.a=bar.b;
delete from foo using bar where foo.a=bar.b;
drop table foo;
drop table bar;

create table foo (a int, b int);
insert into foo select generate_series(1,10);
alter table foo set with(REORGANIZE=false) distributed randomly;
analyze foo;
-- Turn off redistribute motion for ORCA just for this case.
-- This is to get a broadcast motion over foo_1 so that no
-- motion is above the resultrelation foo thus no ExplicitMotion.
set optimizer_enable_motion_redistribute = off;
explain delete from foo using foo foo_1 where foo_1.a=foo.a;
delete from foo using foo foo_1 where foo_1.a=foo.a;
reset optimizer_enable_motion_redistribute;
drop table foo;

create table foo (a int, b int);
insert into foo select generate_series(1,10);
alter table foo set with(REORGANIZE=false) distributed randomly;
analyze foo;
explain delete from foo;
delete from foo;
drop table foo;

create table foo (a int, b int);
create table bar(a int, b int);
insert into foo select generate_series(1,10);
insert into bar select generate_series(1,10);
alter table foo set with(REORGANIZE=false) distributed randomly;
alter table bar set with(REORGANIZE=false) distributed randomly;
analyze foo;
analyze bar;
explain delete from foo using bar;
delete from foo using bar;
drop table foo;
drop table bar;

create table foo (a int, b int);
create table bar(a int, b int);
insert into bar select i, i from generate_series(1, 1000)i;
insert into foo select i,i from generate_series(1, 10)i;
alter table foo set with(REORGANIZE=false) distributed randomly;
alter table bar set with(REORGANIZE=false) distributed randomly;
analyze	foo;
analyze	bar;
set optimizer_enable_motion_redistribute=off;
explain delete from foo using bar where foo.b=bar.b;
delete from foo using bar where foo.b=bar.b;
drop table foo;
drop table bar;
reset optimizer_enable_motion_redistribute;

create table foo (a int, b int) distributed randomly;
create table bar (a int, b int) distributed randomly;
insert into foo (a, b) values (1, 2);
explain insert into bar select * from foo;
insert into bar select * from foo;
select * from bar;
drop table foo;
drop table bar;

create table foo (a int, b int) distributed randomly;
create table bar (a int, b int) distributed randomly;
insert into foo (a, b) values (1, 2);
insert into bar (a, b) values (1, 2);
explain update foo set a=4 from bar where foo.a=bar.a;
update foo set a=4 from bar where foo.a=bar.a;
select * from foo;
drop table foo;
drop table bar;

create table foo (a int, b int) distributed randomly;
create table bar (a int, b int) distributed randomly;
create table jazz (a int, b int) distributed randomly;
insert into foo (a, b) values (1, 2);
insert into bar (a, b) values (1, 2);
insert into jazz (a, b) values (1, 2);
explain insert into foo select bar.a from bar, jazz where bar.a=jazz.a;
insert into foo select bar.a from bar, jazz where bar.a=jazz.a;
select * from foo;
drop table foo;
drop table bar;
drop table jazz;

create table foo (a int);
create table bar (b int);
insert into foo select i from generate_series(1, 10)i;
insert into bar select i from generate_series(1, 10)i;
alter table foo set with(REORGANIZE=false) distributed randomly;
alter table bar set with(REORGANIZE=false) distributed randomly;
analyze foo;
analyze bar;
explain delete from foo using (select a from foo union all select b from bar) v;
delete from foo using (select a from foo union all select b from bar) v;
select * from foo;
drop table foo;
drop table bar;

-- This test is to verify ORCA can generate plans with empty
-- target lists. This can happen when inserting rows with no
-- columns into a table with no columns
create table test();
explain (analyze, costs off, timing off, summary off) insert into test default values;

-- Test delete on partition table with dropped/added columns
CREATE TABLE part (
    a int,
    b int,
    c text,
    d numeric)
DISTRIBUTED BY (b)
partition by range(a) (
    start(1) end(6) every(2),
    default partition def);
alter table part add column e int;
insert into part select i, i, 'abc', i*1.01,i from generate_series(1,10)i;
alter table part drop column b;
alter table part set WITH (reorganize=true) distributed by (e);
-- test delete with dropped column
explain delete from part where d>9;
delete from part where d>9;
select count(*) from part;
-- test delete with added partition key
explain delete from part where e=3;
delete from part where e=3;
select count(*) from part;
-- test delete from default partition
explain delete from part where a=8;
delete from part where a=8;
select count(*) from part;

DROP TABLE IF EXISTS part;
CREATE TABLE part (
    a int,
    b int,
    partkey int,
    c text,
    d numeric)
DISTRIBUTED BY (b)
partition by range(partkey) (
    start(1) end(6) every(2),
    default partition def);
alter table part add column e int;
insert into part select i, i, i, 'abc', i*1.01,i from generate_series(1,10)i;
alter table part drop column b;
alter table part set WITH (reorganize=true) distributed by (e);
-- test delete with column order change
explain delete from part where d>9;
delete from part where d>9;
select count(*) from part;

-- Test delete on mid-level partitions. Ensure Orca properly handles tuple routing
create table deep_part (
  i int,
  j int,
  k int,
  s char(5)
) distributed by (i) partition by list(s) subpartition by range (j) subpartition template (
  start(1) end(3) every(1)
) (
  partition p1
  values
    ('A'),
    partition p2
  values
    ('B')
);

insert into deep_part values (1,1,1,'A');
delete from deep_part_1_prt_p1 where j=1;
