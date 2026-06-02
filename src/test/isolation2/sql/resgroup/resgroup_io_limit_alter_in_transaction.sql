-- ALTER RESOURCE GROUP IO_LIMIT inside a transaction.

!\retcode gpconfig -c gp_resource_group_enable_alter_in_transaction -v on;

!\retcode gpstop -raq -M fast;

-- start_ignore
DROP VIEW rg_alter_tran_status_io_limit;
DROP VIEW rg_alter_tran_heap_status_io_limit;
DROP VIEW rg_alter_tran_heap_status_io_limit_local;
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;
CREATE EXTENSION gp_inject_fault;
-- end_ignore


-- Show IO_LIMIT catalog state from QD and all segments.

-- gp_resgroup_config shows IO_LIMIT in catalog format, with tablespace OID
-- and MB/s values.
CREATE OR REPLACE VIEW rg_alter_tran_heap_status_io_limit_local AS
SELECT gp_id.gp_segment_id,
       groupname::text,
       io_limit::text
FROM gp_toolkit.gp_resgroup_config c, gp_id
WHERE c.groupname IN ('rg_alter_tran', 'rg_alter_tran_b');

CREATE OR REPLACE VIEW rg_alter_tran_heap_status_io_limit AS
SELECT -1::int AS gp_segment_id,
       groupname::text,
       io_limit::text
FROM gp_toolkit.gp_resgroup_config
WHERE groupname IN ('rg_alter_tran', 'rg_alter_tran_b')
UNION ALL
SELECT gp_segment_id,
       groupname,
       io_limit::text
FROM gp_dist_random('rg_alter_tran_heap_status_io_limit_local')
ORDER BY groupname, gp_segment_id;

-- Group matching data into one line per group; inconsistent values produce
-- multiple lines per group and make the test fail by output diff.
CREATE OR REPLACE VIEW rg_alter_tran_status_io_limit AS
SELECT groupname,
       io_limit
FROM rg_alter_tran_heap_status_io_limit
GROUP BY groupname, io_limit
ORDER BY groupname;

CREATE RESOURCE GROUP rg_alter_tran
WITH (concurrency=10, cpu_max_percent=10);

CREATE RESOURCE GROUP rg_alter_tran_b
WITH (concurrency=10, cpu_max_percent=10);

SELECT * FROM rg_alter_tran_status_io_limit;

-- 1 Applying settings if there is a COMMIT.
-- Multiple resource groups are changed to verify atomic apply.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=1000,wbps=1000,riops=1000,wiops=1000';
ALTER RESOURCE GROUP rg_alter_tran_b
  SET io_limit 'pg_default:rbps=2000,wbps=2000';
COMMIT;

SELECT * FROM rg_alter_tran_status_io_limit;

-- Check runtime cgroup v2 io.max on disk.
-- rbps/wbps are stored in io.max as bytes/s:
-- 1000 * 1024 * 1024 = 1048576000.
SELECT check_cgroup_io_max('rg_alter_tran', 'pg_default',
    'rbps=1048576000 wbps=1048576000 riops=1000 wiops=1000');

-- Unspecified riops/wiops are written as max:
-- 2000 * 1024 * 1024 = 2097152000.
SELECT check_cgroup_io_max('rg_alter_tran_b', 'pg_default',
    'rbps=2097152000 wbps=2097152000 riops=max wiops=max');

-- 2 Transaction rollback.
-- Rolled back ALTER must not be applied.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=3000,wbps=3000,riops=3000,wiops=3000';
ROLLBACK;

SELECT * FROM rg_alter_tran_status_io_limit;

SELECT check_cgroup_io_max('rg_alter_tran', 'pg_default',
    'rbps=1048576000 wbps=1048576000 riops=1000 wiops=1000');

-- 3 Subtransaction rollback with explicit SAVEPOINT.
-- Rolled back ALTER must not be applied.
BEGIN;
SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=3000,wbps=3000,riops=3000,wiops=3000';
ROLLBACK TO SAVEPOINT s1;
COMMIT;

SELECT * FROM rg_alter_tran_status_io_limit;

SELECT check_cgroup_io_max('rg_alter_tran', 'pg_default',
    'rbps=1048576000 wbps=1048576000 riops=1000 wiops=1000');

-- 4 Several ALTERs of the same limit type in one transaction.
-- Only the final catalog value should be applied.
SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

SELECT gp_inject_fault('resgroup_alter_on_commit',
                       'skip', '', '', '', 1, 100, 0, dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

BEGIN;
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=3000,wbps=3000,riops=3000,wiops=3000';
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=4000,wbps=4000';
COMMIT;

SELECT * FROM rg_alter_tran_status_io_limit;

SELECT check_cgroup_io_max('rg_alter_tran', 'pg_default',
    'rbps=4194304000 wbps=4194304000 riops=max wiops=max');

-- Should be one in `num times hit`
SELECT gp_inject_fault('resgroup_alter_on_commit', 'status', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

-- clean
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;
DROP VIEW rg_alter_tran_status_io_limit;
DROP VIEW rg_alter_tran_heap_status_io_limit;
DROP VIEW rg_alter_tran_heap_status_io_limit_local;

!\retcode gpconfig -r gp_resource_group_enable_alter_in_transaction;

!\retcode gpstop -raq -M fast;
