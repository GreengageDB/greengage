-- ------------------------------------------------------
-- Test: ALTER RESOURCE GROUP inside a transaction block.
-- ------------------------------------------------------

-- start_ignore
! gpconfig -c gp_resource_group_enable_alter_in_transaction -v on;
! gpstop -arf;

DROP VIEW IF EXISTS rg_alter_tran_status;
DROP VIEW IF EXISTS rg_alter_tran_runtime_status;
DROP FUNCTION IF EXISTS rg_alter_tran_runtime_cap(group_name text, cap_id int);
DROP FUNCTION IF EXISTS rg_alter_tran_func();
DROP FUNCTION IF EXISTS rg_alter_tran_func_sub();
DROP FUNCTION IF EXISTS rg_alter_tran_func_sub_fail();
DROP FUNCTION IF EXISTS rg_alter_tran_func_own_group();
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;
CREATE LANGUAGE plpython3u;
-- end_ignore

CREATE RESOURCE GROUP rg_alter_tran   WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);
CREATE RESOURCE GROUP rg_alter_tran_b WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);

CREATE OR REPLACE VIEW rg_alter_tran_status AS SELECT groupname, concurrency, cpu_rate_limit, memory_limit FROM gp_toolkit.gp_resgroup_config WHERE groupname IN ('rg_alter_tran', 'rg_alter_tran_b') ORDER BY groupname;

-- Return a resource group runtime cap from QD shared-memory dump.
--
-- cap_id values:
--   1 - concurrency
--   2 - cpu_rate_limit
--   3 - memory_limit
--   4 - memory_shared_quota
--   5 - memory_spill_ratio
--   6 - memory_auditor
CREATE OR REPLACE FUNCTION rg_alter_tran_runtime_cap(group_name text, cap_id int)
RETURNS int
AS $$
import json

res = plpy.execute("""
    SELECT oid FROM pg_resgroup WHERE rsgname = %s
""" % plpy.quote_literal(group_name))

if len(res) != 1:
    return None

group_id = int(res[0]["oid"])
cap_key = str(cap_id)

res = plpy.execute("SELECT value FROM pg_resgroup_get_status_kv('dump')")
dump = json.loads(res[0]["value"])

for seg in dump["info"]:
    if seg["segid"] != -1:
        continue

    for group in seg["groups"]:
        if int(group["group_id"]) != group_id:
            continue

        for cap in group["caps"]:
            if cap_key in cap:
                return int(cap[cap_key])

return None
$$ LANGUAGE plpython3u;

CREATE OR REPLACE VIEW rg_alter_tran_runtime_status AS SELECT groupname, rg_alter_tran_runtime_cap(groupname, 1) AS concurrency, rg_alter_tran_runtime_cap(groupname, 2) AS cpu_rate_limit, rg_alter_tran_runtime_cap(groupname, 3) AS memory_limit FROM (VALUES ('rg_alter_tran'), ('rg_alter_tran_b')) AS v(groupname) ORDER BY groupname;

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
CREATE OR REPLACE FUNCTION rg_alter_tran_func() RETURNS void AS $body$ BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 11; END; $body$ LANGUAGE plpgsql;
SELECT rg_alter_tran_func();
SELECT * FROM rg_alter_tran_status;

-- 5 Applying settings with using subtransactions in DO block.
-- No error, subtransaction commits.
DO $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 13; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
SELECT * FROM rg_alter_tran_status;

-- 6 Not applying settings with rollback subtransactions in DO block.
-- ALTER happens inside a subtransaction that is rolled back.
DO $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 15; RAISE EXCEPTION 'rollback the subxact'; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
SELECT * FROM rg_alter_tran_status;

-- 7 Applying settings with using subtransactions in function.
CREATE OR REPLACE FUNCTION rg_alter_tran_func_sub() RETURNS void AS $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 17; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
SELECT rg_alter_tran_func_sub();
SELECT * FROM rg_alter_tran_status;

-- 8 Not applying settings with rollback subtransactions in function.
CREATE OR REPLACE FUNCTION rg_alter_tran_func_sub_fail() RETURNS void AS $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 19; RAISE EXCEPTION 'rollback the subxact'; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
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
SELECT * FROM rg_alter_tran_runtime_status;


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
CREATE OR REPLACE FUNCTION rg_alter_tran_func_own_group() RETURNS void AS $body$ BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 23; END; $body$ LANGUAGE plpgsql;
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
SELECT * FROM rg_alter_tran_runtime_status;

-- 14 Subtransaction rollback followed by the same final value.
-- The rolled back callback and the real callback have the same target value;
-- only one apply should matter.
BEGIN;
SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 11;
ROLLBACK TO SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 11;
COMMIT;
SELECT * FROM rg_alter_tran_status;
SELECT * FROM rg_alter_tran_runtime_status;

-- 15 Several ALTERs of the same limit type in one transaction.
-- Only the final catalog value should be applied.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 12;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 13;
COMMIT;
SELECT * FROM rg_alter_tran_status;
SELECT * FROM rg_alter_tran_runtime_status;

-- 16 ALTER changes value and then changes it back in the same transaction.
-- Final runtime state should stay equal to the committed catalog value.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 14;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 13;
COMMIT;
SELECT * FROM rg_alter_tran_status;
SELECT * FROM rg_alter_tran_runtime_status;

-- 17 Rolled back callback with stale full snapshot must not overwrite
-- another committed field.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET MEMORY_LIMIT 9;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 15;
SAVEPOINT s1;
ALTER RESOURCE GROUP rg_alter_tran SET MEMORY_LIMIT 9;
ROLLBACK TO SAVEPOINT s1;
COMMIT;
SELECT * FROM rg_alter_tran_status;
SELECT * FROM rg_alter_tran_runtime_status;


-- Concurrent ALTERs

-- 18 Error during pg_resgroup_move_query when ALTER is uncommitted.
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 16;
2: SELECT gp_toolkit.pg_resgroup_move_query(pg_backend_pid(), 'rg_alter_tran_b');
1: ROLLBACK;
SELECT * FROM rg_alter_tran_status;

-- 19 DROP RESOURCE GROUP waits while ALTER is uncommitted.
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 17;
2&: DROP RESOURCE GROUP rg_alter_tran_b;
1: ROLLBACK;
2<:
SELECT * FROM rg_alter_tran_status;

-- 20 CREATE RESOURCE GROUP waits while ALTER is uncommitted.
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 18;
2&: CREATE RESOURCE GROUP rg_alter_tran_b WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);
1: ROLLBACK;
2<:
SELECT * FROM rg_alter_tran_status;

1q:
2q:

-- cleanup
DROP VIEW rg_alter_tran_status;
DROP VIEW rg_alter_tran_runtime_status;
DROP FUNCTION rg_alter_tran_runtime_cap(group_name text, cap_id int);
DROP FUNCTION rg_alter_tran_func();
DROP FUNCTION rg_alter_tran_func_sub();
DROP FUNCTION rg_alter_tran_func_sub_fail();
DROP FUNCTION rg_alter_tran_func_own_group();
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;

-- start_ignore
! gpconfig -r gp_resource_group_enable_alter_in_transaction;
! gpstop -arf;
-- end_ignore
