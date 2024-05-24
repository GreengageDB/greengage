-- Test locking behaviour for concurrency between SELECT from root partition
-- and TRUNCATE on leaf partition. If gp_keep_partition_children_locks is on,
-- ORCA takes the locks on leaf partitions to be scanned (in static selection
-- case). Without locks TRUNCATE on leaf partition could break the atomicity of
-- SELECT.
create table t_part (a int, b int, c text) distributed by (a)
    partition by list(b) (partition part1 values (1));
insert into t_part select g, 1, g || 'line' from generate_series(1, 10) g;

-- Wait for some time on 2 segments to give the way to TRUNCATE.
1: select gp_inject_fault('locks_check_at_select_portal_create', 'sleep', '', '', '', 1, 1, 10, dbid)
   from gp_segment_configuration c where role='p' and (content=0 or content=2);
2: set gp_keep_partition_children_locks = on;
2&: select count(*) from t_part;

-- If SELECT had acquired the lock only on root, TRUNCATE would have passed
-- straight up and could have truncated the leaf on first and third segments, while
-- SELECT could have already scanned the tuple from second segment. Expected
-- behavior is to make the TRUNCATE wait on QD until SELECT is ended.
select pg_sleep(2);
truncate t_part_1_prt_part1;

1: select gp_inject_fault('locks_check_at_select_portal_create', 'reset', c.dbid)
   from gp_segment_configuration c where role='p' and (content=0 or content=2);

2<:

drop table t_part;
