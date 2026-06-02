-- ALTER RESOURCE GROUP IO_LIMIT inside a transaction.

!\retcode gpconfig -c gp_resource_group_enable_alter_in_transaction -v on;

!\retcode gpstop -raq -M fast;

-- start_ignore
DROP FUNCTION rg_iolimit_tran_status(text, text);
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;
CREATE EXTENSION gp_inject_fault;
-- end_ignore

/*
 * Show IO_LIMIT catalog state from QD and compare it with the runtime
 * cgroup v2 io.max value on disk.
 *
 * gp_resgroup_config shows IO_LIMIT in catalog format, with tablespace OID
 * and MB/s values. check_cgroup_io_max() checks cgroup v2 io.max, where
 * rbps/wbps are stored as bytes/s for the real block device.
 */
CREATE OR REPLACE FUNCTION rg_iolimit_tran_status(group_name text,
                                                  expected_io_max text)
RETURNS TABLE(groupname text,
              io_limit text,
              cgroup_ok bool)
AS $$
    SELECT c.groupname::text, /* inside a function */
           c.io_limit::text, /* inside a function */
           check_cgroup_io_max(group_name, /* inside a function */
                               'pg_default', /* inside a function */
                               expected_io_max) /* inside a function */
    FROM gp_toolkit.gp_resgroup_config c /* inside a function */
    WHERE c.groupname = group_name; /* inside a function */
$$ LANGUAGE SQL;

CREATE RESOURCE GROUP rg_alter_tran
WITH (concurrency=10, cpu_max_percent=10);

CREATE RESOURCE GROUP rg_alter_tran_b
WITH (concurrency=10, cpu_max_percent=10);

-- 1 Applying settings if there is a COMMIT.
-- Multiple resource groups are changed to verify atomic apply.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=1000,wbps=1000,riops=1000,wiops=1000';
ALTER RESOURCE GROUP rg_alter_tran_b
  SET io_limit 'pg_default:rbps=2000,wbps=2000';
COMMIT;

-- rbps/wbps are stored in io.max as bytes/s:
-- 1000 * 1024 * 1024 = 1048576000.
SELECT *
FROM rg_iolimit_tran_status(
    'rg_alter_tran',
    'rbps=1048576000 wbps=1048576000 riops=1000 wiops=1000');

-- Unspecified riops/wiops are written as max:
-- 2000 * 1024 * 1024 = 2097152000.
SELECT *
FROM rg_iolimit_tran_status(
    'rg_alter_tran_b',
    'rbps=2097152000 wbps=2097152000 riops=max wiops=max');

-- 2 Transaction rollback.
-- Rolled back ALTER must not be applied.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=3000,wbps=3000,riops=3000,wiops=3000';
ROLLBACK;

SELECT *
FROM rg_iolimit_tran_status(
    'rg_alter_tran',
    'rbps=1048576000 wbps=1048576000 riops=1000 wiops=1000');

-- 3 Subtransaction rollback with explicit SAVEPOINT.
-- Rolled back ALTER must not be applied.
BEGIN;
SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran
  SET io_limit 'pg_default:rbps=3000,wbps=3000,riops=3000,wiops=3000';
ROLLBACK TO SAVEPOINT s1;
COMMIT;

SELECT *
FROM rg_iolimit_tran_status(
    'rg_alter_tran',
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

SELECT *
FROM rg_iolimit_tran_status(
    'rg_alter_tran',
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
DROP FUNCTION rg_iolimit_tran_status(text, text);

!\retcode gpconfig -r gp_resource_group_enable_alter_in_transaction;

!\retcode gpstop -raq -M fast;
