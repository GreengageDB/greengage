-- For DDL op running over partitioned table we should acquire AccessExclusive
-- locks on root relation and child partitions and keep them until transaction
-- completion. Otherwise if locks on partitions are not kept on QD, failure
-- might occur from concurrent queries running on the leaf partitions.

-- Prepare setup: create appendonly partitioned table with two partitions
CREATE extension if NOT EXISTS gp_inject_fault;
DROP TABLE IF EXISTS ptest;
CREATE TABLE ptest(i int) WITH(appendonly=true) PARTITION BY RANGE (i) (START (0) END (2) EXCLUSIVE EVERY (1));

-- The first session removes partitioned table inside transaction block
-- When locks on partitions are not set (buggy case), the second session that
-- works with specific partition will have progress
1: BEGIN;
1: DROP TABLE ptest;

-- The second session executes `pg_total_relation_size()` up to accessing to
-- pg_aoseg relation for some partition on some segment node if its progress is
-- not restricted by locks on partitions from concurrent DROP stmt.
-- After fixing of partition locking issue the second session have to stuck on
-- acquiring of AccessShare lock and take advance only after commit in the first
-- session completes.
SELECT gp_inject_fault('pg_calculate_table_size_before_pg_aoseg_estimation', 'suspend', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
2&: SELECT pg_total_relation_size('ptest_1_prt_1'::regclass);

-- Wait some amount of time so that second session have reached the fault point
-- if it has progress
SELECT pg_sleep(1);

-- On some segment try to stop commit process in the place after passing the
-- removing PGPROC entry from ProcArray and not achieving catalog cache
-- invalidation routine. All renewed snapshots instantiating in concurrent
-- sessions already have to treat this transaction as completed
SELECT gp_inject_fault('finish_prepared_after_pgproc_removal_before_cache_invalidation', 'suspend', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
1&: COMMIT;

SELECT gp_wait_until_triggered_fault('finish_prepared_after_pgproc_removal_before_cache_invalidation', 1, dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;

-- Issue some dummy DDL query to inspire invalidation of catalog snapshot inside
-- catalog cache invalidation routine later in the second session
CREATE TABLE dummy_test(i int);

-- Resume second session
-- Cache invalidation occurs within `LockRelationOid()` function on pg_aoseg oid
-- inside `try_relation_open()` call that returns abnormal NULL value. Failure
-- occurs later.
SELECT gp_inject_fault('pg_calculate_table_size_before_pg_aoseg_estimation', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
-- Wait some amount time so that the second session eventually have executed
-- `try_relation_open()` function if it has progress.
SELECT pg_sleep(1);
SELECT gp_inject_fault('finish_prepared_after_pgproc_removal_before_cache_invalidation', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
1<:
2<:

1q:
2q:
