-- Test if ALTER RENAME followed by ANALYZE executed concurrently with
-- DROP does not introduce inconsistency between coordinator and segments.
-- DROP should be blocked because of ALTER-ANALYZE.  After being
-- unblocked, DROP should lookup the old name again and fail with
-- relation does not exist error.

1:drop table if exists t1;
1:drop table if exists newt1;
1:create table t1 (a int, b text) distributed by (a);
1:insert into t1 select i, 'abc '||i from generate_series(1,10)i;
1:begin;
1:alter table t1 rename to newt1;
1:analyze newt1;
-- this drop should block to acquire AccessExclusive lock on t1's OID.
2&:drop table t1;
1:commit;
2<:
2:select count(*) from newt1;

-- DROP is executed concurrently with ALTER RENAME but not ANALYZE.
1:drop table if exists t2;
1:drop table if exists newt2;
1:create table t2 (a int, b text) distributed by (a);
1:insert into t2 select i, 'pqr '||i from generate_series(1,10)i;
1:begin;
1:alter table t2 rename to newt2;
2&:drop table t2;
1:commit;
2<:
2:select count(*) from newt2;

-- The same, but with DROP IF EXISTS. (We used to have a bug, where the DROP
-- command found and drop the relation in the segments, but not in coordinator.)
1:drop table if exists t3;
1:create table t3 (a int, b text) distributed by (a);
1:insert into t3 select i, '123 '||i from generate_series(1,10)i;
1:begin;
1:alter table t3 rename to t3_new;
2&:drop table if exists t3;
1:commit;
2<:
2:select count(*) from t3;
2:select relname from pg_class where relname like 't3%';
2:select relname from gp_dist_random('pg_class') where relname like 't3%';

1:drop table if exists t3;
1:create table t3 (a int, b text) distributed by (a);
1:insert into t3 select i, '123 '||i from generate_series(1,10)i;
1:begin;
1:drop table t3;
2&:drop table if exists t3;
3&:drop table t3;
1:commit;
3<:
2<:
2:select count(*) from t3;

-- Ensure DROP doesn't make inconsistency
-- start_ignore
drop table if exists t4;
-- end_ignore
select gp_inject_fault('wait_before_drop_dispatch', 'suspend', 1);
1&:drop table if exists t4;
create table t4 (a int, b text) distributed by (a);
select gp_wait_until_triggered_fault('wait_before_drop_dispatch', 1, 1);
select gp_inject_fault('wait_before_drop_dispatch', 'reset', 1);
1<:
table t4;
drop table t4;

-- start_ignore
drop type if exists t5;
-- end_ignore
select gp_inject_fault('wait_before_drop_dispatch', 'suspend', 1);
1&:drop type if exists t5;
create type t5 as (a int, b text);
select gp_wait_until_triggered_fault('wait_before_drop_dispatch', 1, 1);
select gp_inject_fault('wait_before_drop_dispatch', 'reset', 1);
1<:
select null::t5 from gp_dist_random('gp_id');
drop type t5;

-- start_ignore
drop schema if exists t6;
-- end_ignore
select gp_inject_fault('wait_before_drop_dispatch', 'suspend', 1);
1&:drop schema if exists t6;
create schema t6;
select gp_wait_until_triggered_fault('wait_before_drop_dispatch', 1, 1);
select gp_inject_fault('wait_before_drop_dispatch', 'reset', 1);
1<:
create table t6.t7 (a int, b text) distributed by (a);
drop schema t6 cascade;
