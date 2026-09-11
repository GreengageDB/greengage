-- ------------------------------------------------------
-- Test: ALTER RESOURCE GROUP inside a transaction block.
-- ------------------------------------------------------

!\retcode gpconfig -c gp_resource_group_enable_alter_in_transaction -v on;

!\retcode gpstop -raq -M fast;

-- start_ignore
DROP VIEW IF EXISTS rg_alter_tran_status;
DROP VIEW IF EXISTS rg_alter_tran_runtime_status;
DROP FUNCTION IF EXISTS rg_alter_tran_func();
DROP FUNCTION IF EXISTS rg_alter_tran_func_sub();
DROP FUNCTION IF EXISTS rg_alter_tran_func_sub_fail();
DROP FUNCTION IF EXISTS rg_alter_tran_func_own_group();
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;
CREATE LANGUAGE plpython3u;
CREATE EXTENSION gp_inject_fault;
-- end_ignore

CREATE RESOURCE GROUP rg_alter_tran
  WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);
CREATE RESOURCE GROUP rg_alter_tran_b
  WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);

CREATE OR REPLACE VIEW rg_alter_tran_heap_status_local AS
SELECT gp_id.gp_segment_id,
       c.groupname::text,
       c.concurrency::int,
       c.cpu_rate_limit::int,
       c.memory_limit::int
FROM gp_toolkit.gp_resgroup_config c, gp_id
WHERE c.groupname IN ('rg_alter_tran', 'rg_alter_tran_b');

CREATE OR REPLACE VIEW rg_alter_tran_heap_status AS
SELECT -1::int AS gp_segment_id,
       groupname::text,
       concurrency::int,
       cpu_rate_limit::int,
       memory_limit::int
FROM gp_toolkit.gp_resgroup_config
WHERE groupname IN ('rg_alter_tran', 'rg_alter_tran_b')
UNION ALL
SELECT gp_segment_id,
       groupname,
       concurrency,
       cpu_rate_limit,
       memory_limit
FROM gp_dist_random('rg_alter_tran_heap_status_local')
ORDER BY groupname, gp_segment_id;

CREATE OR REPLACE VIEW rg_alter_tran_runtime_status AS
SELECT (seg->>'segid')::int AS gp_segment_id,
       r.rsgname::text AS groupname,
       (
           SELECT cap.value::int
           FROM json_array_elements(grp->'caps') AS cap_obj,
                json_each_text(cap_obj) AS cap
           WHERE cap.key::int = 1
       ) AS concurrency,
       (
           SELECT cap.value::int
           FROM json_array_elements(grp->'caps') AS cap_obj,
                json_each_text(cap_obj) AS cap
           WHERE cap.key::int = 2
       ) AS cpu_rate_limit,
       (
           SELECT cap.value::int
           FROM json_array_elements(grp->'caps') AS cap_obj,
                json_each_text(cap_obj) AS cap
           WHERE cap.key::int = 3
       ) AS memory_limit
FROM pg_resgroup_get_status_kv('dump') d,
     json_array_elements((d.value::json)->'info') AS seg,
     json_array_elements(seg->'groups') AS grp,
     pg_resgroup r
WHERE r.rsgname IN ('rg_alter_tran', 'rg_alter_tran_b')
  AND (grp->>'group_id')::oid = r.oid
ORDER BY groupname, gp_segment_id;

-- Use PL/pgSQL to run heap and runtime checks as separate statements;
-- one SQL query may fail with "multiple segworker groups is not supported".
CREATE OR REPLACE FUNCTION rg_alter_tran_all_status()
RETURNS TABLE(gp_segment_id int,
              groupname text,
              concurrency int,
              cpu_rate_limit int,
              memory_limit int)
AS $$
BEGIN /* inside a function */
  RETURN QUERY EXECUTE /* inside a function */
    'SELECT gp_segment_id, groupname, concurrency, cpu_rate_limit, memory_limit FROM rg_alter_tran_heap_status'; /* inside a function */

  RETURN QUERY EXECUTE /* inside a function */
    'SELECT gp_segment_id, groupname, concurrency, cpu_rate_limit, memory_limit FROM rg_alter_tran_runtime_status'; /* inside a function */
END; /* inside a function */
$$ LANGUAGE plpgsql EXECUTE ON MASTER;

-- Group matching data into one line per group; inconsistent values produce
-- multiple lines per group and make the test fail by output diff.
CREATE OR REPLACE VIEW rg_alter_tran_status AS
SELECT groupname,
       concurrency,
       cpu_rate_limit,
       memory_limit
FROM rg_alter_tran_all_status()
GROUP BY groupname, concurrency, cpu_rate_limit, memory_limit
ORDER BY groupname;

SELECT * FROM rg_alter_tran_status;

-- 1 Applying settings if there is a COMMIT.
-- Multiple resource groups are changed to verify atomic apply.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran   SET CONCURRENCY 5;
ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 6;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 2 Not applying settings if there is a ROLLBACK.
-- Multiple resource groups are changed to verify atomic rollback.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran   SET CONCURRENCY 7;
ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 8;
ROLLBACK;
SELECT * FROM rg_alter_tran_status;

-- 3 Not applying settings if transaction is aborted by error.
-- COMMIT after an aborted transaction should behave as ROLLBACK.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran   SET CONCURRENCY 9;
ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 10;
SELECT 1/0;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 4 Applying settings with calling function.
CREATE OR REPLACE FUNCTION rg_alter_tran_func()
RETURNS void AS $$
BEGIN /* inside a function */
  ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 11; /* inside a function */
END; /* inside a function */
$$ LANGUAGE plpgsql;

SELECT rg_alter_tran_func();
SELECT * FROM rg_alter_tran_status;

-- 5 Applying settings with using subtransactions in DO block.
-- No error, subtransaction commits.
DO $$
BEGIN /* inside a function */
  BEGIN /* inside a function */
    ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 13; /* inside a function */
  EXCEPTION WHEN OTHERS THEN /* inside a function */
    NULL; /* inside a function */
  END; /* inside a function */
END; /* inside a function */
$$ LANGUAGE plpgsql;

SELECT * FROM rg_alter_tran_status;

-- 6 Not applying settings with rollback subtransactions in DO block.
-- ALTER happens inside a subtransaction that is rolled back.
DO $$
BEGIN /* inside a function */
  BEGIN /* inside a function */
    ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 15; /* inside a function */
    RAISE EXCEPTION 'rollback the subxact'; /* inside a function */
  EXCEPTION WHEN OTHERS THEN /* inside a function */
    NULL; /* inside a function */
  END; /* inside a function */
END; /* inside a function */
$$ LANGUAGE plpgsql;

SELECT * FROM rg_alter_tran_status;

-- 7 Applying settings with using subtransactions in function.
CREATE OR REPLACE FUNCTION rg_alter_tran_func_sub()
RETURNS void AS $$
BEGIN /* inside a function */
  BEGIN /* inside a function */
    ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 17; /* inside a function */
  EXCEPTION WHEN OTHERS THEN /* inside a function */
    NULL; /* inside a function */
  END; /* inside a function */
END; /* inside a function */
$$ LANGUAGE plpgsql;

SELECT rg_alter_tran_func_sub();
SELECT * FROM rg_alter_tran_status;

-- 8 Not applying settings with rollback subtransactions in function.
CREATE OR REPLACE FUNCTION rg_alter_tran_func_sub_fail()
RETURNS void AS $$
BEGIN /* inside a function */
  BEGIN /* inside a function */
    ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 19; /* inside a function */
    RAISE EXCEPTION 'rollback the subxact'; /* inside a function */
  EXCEPTION WHEN OTHERS THEN /* inside a function */
    NULL; /* inside a function */
  END; /* inside a function */
END; /* inside a function */
$$ LANGUAGE plpgsql;

SELECT rg_alter_tran_func_sub_fail();
SELECT * FROM rg_alter_tran_status;

-- 9 Multi-ALTER: several ALTERs in one transaction touching different
-- fields and different resource groups.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran   SET CONCURRENCY 20;
ALTER RESOURCE GROUP rg_alter_tran   SET MEMORY_LIMIT 7;
ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 21;
ALTER RESOURCE GROUP rg_alter_tran_b SET MEMORY_LIMIT 8;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- Test ALTER current resource group

-- NOTE: Use nested psql because isolation2 does not emit WARNING messages.
-- Also, change the resource group of the existing test user instead of
-- creating and connecting as a new role, so the test does not require
-- pg_hba.conf changes. A new nested psql connection is required because
-- resource group is assigned at session start.

-- 10 WARNING during ALTER in transaction of the resource group
-- under which the request is being executed.
! curuser="$(psql -At -d isolation2resgrouptest -c 'SELECT current_user')" && psql -d isolation2resgrouptest -c "ALTER ROLE \"$curuser\" RESOURCE GROUP rg_alter_tran" >/dev/null && psql -d isolation2resgrouptest -c "BEGIN; ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 22; COMMIT;"; psql -d isolation2resgrouptest -c "ALTER ROLE \"$curuser\" RESOURCE GROUP admin_group" >/dev/null;
SELECT * FROM rg_alter_tran_status;

-- 11 WARNING during ALTER in function of the resource group under
-- which the request is being executed.
CREATE OR REPLACE FUNCTION rg_alter_tran_func_own_group()
RETURNS void AS $$
BEGIN /* inside a function */
  ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 23; /* inside a function */
END; /* inside a function */
$$ LANGUAGE plpgsql;

! curuser="$(psql -At -d isolation2resgrouptest -c 'SELECT current_user')" && psql -d isolation2resgrouptest -c "ALTER ROLE \"$curuser\" RESOURCE GROUP rg_alter_tran" >/dev/null && psql -d isolation2resgrouptest -c "BEGIN; SELECT rg_alter_tran_func_own_group(); COMMIT;"; psql -d isolation2resgrouptest -c "ALTER ROLE \"$curuser\" RESOURCE GROUP admin_group" >/dev/null;
SELECT * FROM rg_alter_tran_status;

-- 12 Success executing without explicit transaction ALTER of the
-- resource group under which the request is being executed.
! curuser="$(psql -At -d isolation2resgrouptest -c 'SELECT current_user')" && psql -d isolation2resgrouptest -c "ALTER ROLE \"$curuser\" RESOURCE GROUP rg_alter_tran" >/dev/null && psql -d isolation2resgrouptest -c "ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 24;"; psql -d isolation2resgrouptest -c "ALTER ROLE \"$curuser\" RESOURCE GROUP admin_group" >/dev/null;
SELECT * FROM rg_alter_tran_status;


-- Test subtransaction rollbacks

-- 13 Subtransaction rollback with explicit SAVEPOINT.
-- Rolled back ALTER must not be applied.
BEGIN;
SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 10;
ROLLBACK TO SAVEPOINT s1;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 14 Subtransaction rollback followed by the same final value.
-- The rolled back callback and the real callback have the same target value;
-- only one apply should matter.
SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

SELECT gp_inject_fault('resgroup_alter_on_commit',
                       'skip', '', '', '', 1, 100, 0, dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

BEGIN;
SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 11;
ROLLBACK TO SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 11;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- Should be only one in `num times hit`
SELECT gp_inject_fault('resgroup_alter_on_commit', 'status', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

-- 15 Several ALTERs of the same limit type in one transaction.
-- Only the final catalog value should be applied.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 12;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 13;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 16 ALTER changes value and then changes it back in the same transaction.
-- Final runtime state should stay equal to the committed catalog value.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 14;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 13;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 17 Multiple rolled back MEMORY_LIMIT callbacks must be ignored and must
-- not overwrite the committed CONCURRENCY with stale full snapshots.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET MEMORY_LIMIT 9;
ALTER RESOURCE GROUP rg_alter_tran SET MEMORY_LIMIT 10;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 15;
SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran SET MEMORY_LIMIT 11;
ALTER RESOURCE GROUP rg_alter_tran SET MEMORY_LIMIT 12;
ROLLBACK TO SAVEPOINT s1;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 18 CPUSET and CPU_RATE_LIMIT are mutually exclusive.
-- Only the final CPU_RATE_LIMIT apply callback should run.
SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

SELECT gp_inject_fault('resgroup_alter_on_commit',
                       'skip', '', '', '', 1, 100, 0, dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CPUSET '0';
ALTER RESOURCE GROUP rg_alter_tran SET CPU_RATE_LIMIT 30;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- Should be only one in `num times hit`
SELECT gp_inject_fault('resgroup_alter_on_commit', 'status', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';

SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid)
FROM gp_segment_configuration
WHERE content = -1 AND role = 'p';


-- Concurrent ALTERs

-- 19 Verify pg_resgroup_move_query error if an uncommitted ALTER affects
-- the same resource group
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 16;
2: SELECT gp_toolkit.pg_resgroup_move_query(999999999, 'rg_alter_tran_b');
1: ROLLBACK;
SELECT * FROM rg_alter_tran_status;

-- 20 Verify pg_resgroup_move_query error if an uncommitted ALTER affects
-- different resource group
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 17;
2: SELECT gp_toolkit.pg_resgroup_move_query(999999999, 'rg_alter_tran_b');
1: ROLLBACK;
SELECT * FROM rg_alter_tran_status;

-- 21 Verify pg_resgroup_move_query releases its lock after transaction abort.
-- After the error aborts transaction 1, parallel ALTER must not wait.
1: BEGIN;
1: SELECT gp_toolkit.pg_resgroup_move_query(999999999, 'rg_alter_tran_b');
2: ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 18;
1: ROLLBACK;
SELECT * FROM rg_alter_tran_status;

-- 22 DROP RESOURCE GROUP waits while ALTER is uncommitted.
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 19;
2&: DROP RESOURCE GROUP rg_alter_tran_b;
1: ROLLBACK;
2<:
SELECT * FROM rg_alter_tran_status;

-- 23 CREATE RESOURCE GROUP waits while ALTER is uncommitted.
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 20;
2&: CREATE RESOURCE GROUP rg_alter_tran_b WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);
1: ROLLBACK;
2<:
SELECT * FROM rg_alter_tran_status;

1q:
2q:


-- Crash recovery

-- 24 Coordinator segfault before COMMIT must not apply ALTERs.
1: SELECT gp_inject_fault('exec_simple_query_start', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 1;
1: SELECT gp_inject_fault('exec_simple_query_start', 'segv', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
-- The backend dies before COMMIT is issued.
1: @post_run 'echo ""': SELECT 1;
-- Wait until coordinator accepts connections again.
! while [ `psql -tc "SELECT 1;" postgres 2>/dev/null | wc -l` != '2' ]; do sleep 1; done;
1q:
1: SELECT * FROM rg_alter_tran_status;

-- 25 Segment segfault before COMMIT must not apply ALTERs.
1: SELECT gp_inject_fault('qe_exec_finished', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 2;
1: SELECT gp_inject_fault('qe_exec_finished', 'segv', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
-- Crash a segment before COMMIT is issued.
1: @post_run 'echo ""': SELECT 1 FROM gp_dist_random('gp_id');
1: COMMIT;
-- Wait until distributed queries work again.
! while [ `psql -tc "SELECT count(*) FROM gp_dist_random('gp_id');" postgres 2>/dev/null | wc -l` != '2' ]; do sleep 1; done;
1: SELECT * FROM rg_alter_tran_status;

-- 26 Coordinator segfault after COMMIT must replay ALTERs.
1: SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 3;
1: SELECT gp_inject_fault('resgroup_alter_on_commit', 'segv', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
-- The backend dies after COMMIT while applying runtime state.
1: @post_run 'echo ""' : COMMIT;
-- Wait until coordinator accepts connections again.
! while [ `psql -tc "SELECT 1;" postgres 2>/dev/null | wc -l` != '2' ]; do sleep 1; done;
1q:
1: SELECT * FROM rg_alter_tran_status;

-- 27 Segment segfault after COMMIT must replay ALTERs.
1: SELECT gp_inject_fault('resgroup_alter_on_commit', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 4;
1: SELECT gp_inject_fault('resgroup_alter_on_commit', 'segv', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
-- The segment dies after COMMIT while applying runtime state.
1: @post_run 'echo ""' : COMMIT;
-- Wait until distributed queries work again.
! while [ `psql -tc "SELECT count(*) FROM gp_dist_random('gp_id');" postgres 2>/dev/null | wc -l` != '2' ]; do sleep 1; done;
1q:
1: SELECT * FROM rg_alter_tran_status;

-- cleanup
1: DROP VIEW rg_alter_tran_status;
1: DROP VIEW rg_alter_tran_runtime_status;
1: DROP FUNCTION rg_alter_tran_func();
1: DROP FUNCTION rg_alter_tran_func_sub();
1: DROP FUNCTION rg_alter_tran_func_sub_fail();
1: DROP FUNCTION rg_alter_tran_func_own_group();
1: DROP RESOURCE GROUP rg_alter_tran;
1: DROP RESOURCE GROUP rg_alter_tran_b;

!\retcode gpconfig -r gp_resource_group_enable_alter_in_transaction;

!\retcode gpstop -raq -M fast;
