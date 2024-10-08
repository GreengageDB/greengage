--
-- Test the query command identification
--
-- start_ignore
create extension if not exists gp_inject_fault;
drop function if exists sirv_function();
drop function if exists not_inlineable_sql_func(i int);
drop table if exists test_data;
drop table if exists t;
drop table if exists t1;
-- end_ignore

set client_min_messages = notice;
select gp_inject_fault('all', 'reset', dbid) from gp_segment_configuration;

create or replace function sirv_function() returns int as $$
declare
    result int;
begin
    create table test_data (x int, y int) with (appendonly=true) distributed by (x);
    insert into test_data values (1, 1);
    select count(1) into result from test_data;
    drop table test_data;
    return result;
end $$ language plpgsql;

\c

select gp_inject_fault_infinite('track_query_command_id', 'skip', dbid) from gp_segment_configuration
where role = 'p' and content = -1;

select sirv_function();

-- Test that the query command id is correct after execution of queries in the InitPlan
create table t as select (select sirv_function()) as res distributed by (res);

-- Test a simple query
select * from t;

drop table t;

-- Test a cursor
begin;
declare cur1 cursor for select sirv_function() as res;
fetch 1 from cur1;
fetch all from cur1;
commit;

-- Test two cursors
begin;
declare cur1_a cursor for select sirv_function() as res;
fetch 1 from cur1_a;
declare cur2_b cursor for select sirv_function() as res;
fetch 2 from cur2_b;
fetch all from cur2_b;
fetch all from cur1_a;
commit;

-- Test partitioned tables
create table t(i int) distributed by (i)
partition by range (i) (start (1) end (10) every (1), default partition extra);

alter table t rename to t1;

drop table t1;

-- Test a function written in sql language, that optimizers cannot inline
create or replace function not_inlineable_sql_func(i int) returns int 
immutable
security definer
as $$
select case when i > 5 then 1 else 0 end;
$$ language sql;

select not_inlineable_sql_func(i) from generate_series(1, 10)i;

select gp_inject_fault_infinite('track_query_command_id', 'reset', dbid) from gp_segment_configuration
where role = 'p' and content = -1;

-- Test the query command ids dispatched to segments
-- start_matchsubs
-- m/select pg_catalog.pg_relation_size\([0-9]+, \'.+\'\)/
-- s/select pg_catalog.pg_relation_size\([0-9]+, \'.+\'\)/select pg_catalog.pg_relation_size\(\)/
-- m/select pg_catalog.gp_acquire_sample_rows\([0-9]+, [0-9]+, \'.+'\)/
-- s/select pg_catalog.gp_acquire_sample_rows\([0-9]+, [0-9]+, \'.+'\)/select pg_catalog.gp_acquire_sample_rows\(\)/
-- m/FROM pg_aoseg.pg_aoseg_[0-9]+/
-- s/FROM pg_aoseg.pg_aoseg_[0-9]+/FROM pg_aoseg.pg_aoseg_OID/
-- end_matchsubs
select gp_inject_fault_infinite('track_query_command_id_at_start', 'skip', dbid) from gp_segment_configuration;

create table t as select 1;
drop table t;

create table t (i int, j text) with (appendonly = true) distributed by (i);
insert into t select i, (i + 1)::text from generate_series(1, 100) i;
vacuum analyze t;
drop table t;

select gp_inject_fault_infinite('track_query_command_id_at_start', 'reset', dbid) from gp_segment_configuration;

-- Test the query command id after an error has happened
select gp_inject_fault('appendonly_insert', 'panic', '', '', 'test_data', 1, -1, 0, dbid) from gp_segment_configuration
where role = 'p';

select gp_inject_fault_infinite('track_query_command_id', 'skip', dbid) from gp_segment_configuration
where role = 'p' and content = -1;

-- First query will fail with an error due to insert inside the function
select sirv_function();

select sirv_function();

select gp_inject_fault('appendonly_insert', 'reset', '', '', 'test_data', 1, -1, 0, dbid) from gp_segment_configuration
where role = 'p';

-- Test an exception caught inside the function
\c
create table t (i int);
insert into t values(0);

do $$
declare
j int;
begin
    select 1 / i from t into strict j;
    raise warning '%', j;
    exception when others then raise warning '%', sqlerrm;
    raise warning '%', 2;
    raise warning '%', 3;
end$$;

select gp_inject_fault_infinite('track_query_command_id', 'reset', dbid) from gp_segment_configuration
where role = 'p' and content = -1;

drop function sirv_function();
drop function not_inlineable_sql_func(i int);
reset client_min_messages;
