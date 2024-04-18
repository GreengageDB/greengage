-- Test locking behaviour. When creating, dropping, querying or adding indexes
-- partitioned tables, we want to lock only the master, not the children.
--
-- Previously, we used to only lock the parent table in many DDL operations.
-- That was always a bit bogus, but we did it to avoid running out of lock space
-- when working on large partition hierarchies. We don't play fast and loose
-- like that anymore, but keep the tests. If a user runs out of lock space, you
-- can work around that by simply bumping up max_locks_per_transactions.
--
-- Show locks in master and in segments. Because the number of segments
-- in the cluster depends on configuration, we print only summary information
-- of the locks in segments. If a relation is locked only on one segment,
-- we print that as a special case, but otherwise we just print "n segments",
-- meaning the relation is locked on more than one segment.
create or replace view locktest_master as
select coalesce(
  case when relname like 'pg_toast%index' then 'toast index'
       when relname like 'pg_toast%' then 'toast table'
       when relname like 'pg_aoseg%' then 'aoseg table'
       when relname like 'pg_aovisimap%index' then 'aovisimap index'
       when relname like 'pg_aovisimap%' then 'aovisimap table'
       else relname end, 'dropped table'),
  mode,
  locktype,
  'master'::text as node
from pg_locks l
left outer join pg_class c on ((l.locktype = 'append-only segment file' and l.relation = c.relfilenode) or (l.locktype != 'append-only segment file' and l.relation = c.oid)),
pg_database d
where relation is not null
and l.database = d.oid
and (relname <> 'gp_fault_strategy' and relname != 'locktest_master' or relname is NULL)
and d.datname = current_database()
and l.gp_segment_id = -1
group by l.gp_segment_id, relation, relname, locktype, mode
order by 1, 3, 2;

create or replace view locktest_segments_dist as
select relname,
  mode,
  locktype,
  l.gp_segment_id as node,
  relation
from pg_locks l
left outer join pg_class c on ((l.locktype = 'append-only segment file' and l.relation = c.relfilenode) or (l.locktype != 'append-only segment file' and l.relation = c.oid)),
pg_database d
where relation is not null
and l.database = d.oid
and (relname <> 'gp_fault_strategy' and relname != 'locktest_segments_dist' or relname is NULL)
and d.datname = current_database()
and l.gp_segment_id > -1
group by l.gp_segment_id, relation, relname, locktype, mode;

create or replace view locktest_segments as
SELECT coalesce(
  case when relname like 'pg_toast%index' then 'toast index'
       when relname like 'pg_toast%' then 'toast table'
       when relname like 'pg_aoseg%' then 'aoseg table'
       when relname like 'pg_aovisimap%index' then 'aovisimap index'
       when relname like 'pg_aovisimap%' then 'aovisimap table'
       else relname end, 'dropped table'),
  mode,
  locktype,
  case when count(*) = 1 then '1 segment'
       else 'n segments' end as node
  FROM gp_dist_random('locktest_segments_dist')
  group by relname, relation, mode, locktype;

-- Partitioned table with toast table
begin;

-- creation
create table partlockt (i int, t text) partition by range(i)
(start(1) end(10) every(1));

select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- drop
begin;
drop table partlockt;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- AO table (ao segments, block directory won't exist after create)
begin;
-- creation
create table partlockt (i int, t text, n numeric)
with (appendonly = true)
partition by list(i)
(values(1), values(2), values(3));
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;
begin;

-- add a little data
insert into partlockt values(1), (2), (3);
insert into partlockt values(1), (2), (3);
insert into partlockt values(1), (2), (3);
insert into partlockt values(1), (2), (3);
insert into partlockt values(1), (2), (3);
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- drop
begin;
drop table partlockt;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- Indexing
create table partlockt (i int, t text) partition by range(i)
(start(1) end(10) every(1));

begin;
create index partlockt_idx on partlockt(i);
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- Force use of the index in the select and delete below. We're not interested
-- in the plan we get, but a seqscan will not lock the index while an index
-- scan will, and we want to avoid the plan-dependent difference in the
-- expected output of this test.
set enable_seqscan=off;

-- test select locking
begin;
select * from partlockt where i = 1;
-- Known_opt_diff: MPP-20936
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

begin;
-- insert locking
insert into partlockt values(3, 'f');
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- delete locking
begin;
delete from partlockt where i = 4;
-- Known_opt_diff: MPP-20936
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- drop index
begin;
drop table partlockt;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
select * from locktest_segments where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- Test locking behaviour for SELECT from root partition. The locks are
-- acquired when gp_keep_partition_children_locks is on from both planner and
-- ORCA. In case of ORCA the locks are taken only on statically selected leafs
-- or on all leafs otherwise similar to planner.
create table t_part_multi_heap (a int, b int, c int)
distributed randomly
partition by range(b)
  subpartition by list(c) subpartition template
  (
    values(0),
    values(1),
    values(2)
  )
(start(0) end(2) every(1));

-- ORCA takes locks on selected leafs and planner takes on every leaf partition
begin;
select * from t_part_multi_heap where c=2;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- ORCA and planner take locks on all leafs.
begin;
select * from t_part_multi_heap;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- The same behaviour. Testing that ORCA could identify the range table inside
-- the cte.
begin;
with cte as (select * from t_part_multi_heap) select * from cte;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

-- The same behaviour. Testing that ORCA could identify the range table inside
-- the SubLink.
create table bar (p int) distributed by (p);

begin;
select bar.p from bar where bar.p in (select a from t_part_multi_heap);
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

drop table bar;

-- Test ORCA taking the locks in case of Partition Selector for join.
create table t_list_multi (a int, b int, c timestamp) distributed by (a)
partition by list(b)
  subpartition by list(c) 
  (partition part1 values (1)
    (subpartition s_part1 values ('2020-06-01', '2020-07-01', '2020-08-01'),
     subpartition s_part2 values ('2020-09-01', '2020-10-01', '2020-11-01')));
create table t_inner_to_join (b int, c date) distributed by (c);

begin;
select count(*) from (
    select distinct c
    from t_inner_to_join
    where c = '2020-07-01'
) tf, t_list_multi
where t_list_multi.c = tf.c;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

drop table t_inner_to_join;
drop table t_list_multi;

-- Test SELECT FOR UPDATE for acquiring RowShareLock lock on QD
begin;
select * from t_part_multi_heap where c=2 for update;
select * from locktest_master where coalesce not like 'gp_%' and coalesce not like 'pg_%';
commit;

drop table t_part_multi_heap;
