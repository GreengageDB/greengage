-- ------------------------------------------------------
-- Test: ALTER RESOURCE GROUP inside a transaction block.
-- ------------------------------------------------------

-- start_ignore
! gpconfig -c gp_resource_group_enable_alter_in_transaction -v on;
! gpstop -arf;

DROP VIEW IF EXISTS rg_alter_tran_status;
DROP FUNCTION IF EXISTS rg_alter_tran_func();
DROP FUNCTION IF EXISTS rg_alter_tran_func_sub();
DROP FUNCTION IF EXISTS rg_alter_tran_func_sub_fail();
DROP FUNCTION IF EXISTS rg_alter_tran_func_own_group();
DROP ROLE IF EXISTS rg_alter_tran_role;
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;
-- end_ignore

CREATE RESOURCE GROUP rg_alter_tran   WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);
CREATE RESOURCE GROUP rg_alter_tran_b WITH (cpu_rate_limit=10, memory_limit=10, concurrency=2);

CREATE OR REPLACE VIEW rg_alter_tran_status AS SELECT groupname, concurrency, cpu_rate_limit, memory_limit FROM gp_toolkit.gp_resgroup_config WHERE groupname IN ('rg_alter_tran', 'rg_alter_tran_b') ORDER BY groupname;

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

-- 9 WARNING during ALTER in transaction of the resource group
-- under which the request is being executed. 
-- ALTER RESOURCE GROUP is superuser-only. Use a dedicated superuser role.
CREATE ROLE rg_alter_tran_role SUPERUSER RESOURCE GROUP rg_alter_tran;

SET ROLE rg_alter_tran_role;
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 21;
COMMIT;
RESET ROLE;
SELECT * FROM rg_alter_tran_status;

-- 10 Success executing without explicit transaction ALTER of the
-- resource group under which the request is being executed.
SET ROLE rg_alter_tran_role;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 22;
RESET ROLE;
SELECT * FROM rg_alter_tran_status;

-- 11 Error during pg_resgroup_move_query when any resource group
-- is edited in an uncommitted transaction.
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 23;
2: SELECT gp_toolkit.pg_resgroup_move_query(pg_backend_pid(), 'rg_alter_tran_b');
1: ROLLBACK;
1q:
2q:
SELECT * FROM rg_alter_tran_status;

-- 12 WARNING during ALTER in function of the resource group under
-- which the request is being executed.
CREATE OR REPLACE FUNCTION rg_alter_tran_func_own_group() RETURNS void AS $body$ BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 24; END; $body$ LANGUAGE plpgsql;
SET ROLE rg_alter_tran_role;
SELECT rg_alter_tran_func_own_group();
RESET ROLE;
SELECT * FROM rg_alter_tran_status;

-- 13 Multi-ALTER: several ALTERs in one transaction touching different
-- fields and different resource groups.
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran   SET CONCURRENCY 3;
ALTER RESOURCE GROUP rg_alter_tran   SET MEMORY_LIMIT 7;
ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 4;
ALTER RESOURCE GROUP rg_alter_tran_b SET MEMORY_LIMIT 8;
COMMIT;

SELECT * FROM rg_alter_tran_status;

-- cleanup
DROP VIEW rg_alter_tran_status;
DROP FUNCTION rg_alter_tran_func();
DROP FUNCTION rg_alter_tran_func_sub();
DROP FUNCTION rg_alter_tran_func_sub_fail();
DROP FUNCTION rg_alter_tran_func_own_group();
DROP ROLE rg_alter_tran_role;
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;

-- start_ignore
! gpconfig -r gp_resource_group_enable_alter_in_transaction;
! gpstop -arf;
-- end_ignore
