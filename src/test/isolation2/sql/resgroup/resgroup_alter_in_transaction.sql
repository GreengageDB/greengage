-- ------------------------------------------------------
-- Test: ALTER RESOURCE GROUP inside a transaction block.
-- ------------------------------------------------------

-- start_ignore
! gpconfig -c gp_resource_group_enable_alter_in_transaction -v on;
! gpstop -rai;
-- end_ignore

-- start_ignore
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;
DROP ROLE rg_alter_tran_role;
-- end_ignore

CREATE RESOURCE GROUP rg_alter_tran   WITH (cpu_rate_limit=10, memory_limit=10);
CREATE RESOURCE GROUP rg_alter_tran_b WITH (cpu_rate_limit=10, memory_limit=10);

CREATE OR REPLACE VIEW rg_alter_tran_status AS SELECT groupname, concurrency, cpu_rate_limit, memory_limit FROM gp_toolkit.gp_resgroup_config WHERE groupname IN ('rg_alter_tran', 'rg_alter_tran_b') ORDER BY groupname;

SELECT * FROM rg_alter_tran_status;

-- 1.1 BEGIN; ALTER; COMMIT; settings applied
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 5;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 1.2 BEGIN; ALTER; ROLLBACK; settings not applied
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 7;
ROLLBACK;
SELECT * FROM rg_alter_tran_status;

-- 1.3 BEGIN; ALTER; <error>; COMMIT; settings not applied
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 8;
SELECT 1/0;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 1.4 plpgsql function calling ALTER; settings applied
CREATE OR REPLACE FUNCTION rg_alter_tran_func() RETURNS VOID AS $body$ BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 6; END; $body$ LANGUAGE plpgsql;
SELECT rg_alter_tran_func();
SELECT * FROM rg_alter_tran_status;

-- 1.5 DO block, subtransaction wrapping ALTER, no error; settings applied
DO $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 9; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
SELECT * FROM rg_alter_tran_status;

-- 1.6 DO block, subtransaction with ALTER plus error caught; settings not applied
DO $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 11; RAISE EXCEPTION 'rollback the subxact'; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
SELECT * FROM rg_alter_tran_status;

-- 1.7 plpgsql function with subtransaction, no error; settings applied
CREATE OR REPLACE FUNCTION rg_alter_tran_func_sub() RETURNS VOID AS $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 12; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
SELECT rg_alter_tran_func_sub();
SELECT * FROM rg_alter_tran_status;

-- 1.8 plpgsql function with subtransaction plus caught error; settings not applied
CREATE OR REPLACE FUNCTION rg_alter_tran_func_sub_fail() RETURNS VOID AS $body$ BEGIN BEGIN ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 14; RAISE EXCEPTION 'rollback the subxact'; EXCEPTION WHEN OTHERS THEN NULL; END; END; $body$ LANGUAGE plpgsql;
SELECT rg_alter_tran_func_sub_fail();
SELECT * FROM rg_alter_tran_status;

-- 1.9 / 1.10 ALTER on the executing session's own group requires a
-- superuser session bound to that group, ALTER RESOURCE GROUP is
-- superuser-only. Use a dedicated superuser role for the test.
CREATE ROLE rg_alter_tran_role SUPERUSER RESOURCE GROUP rg_alter_tran;

-- 1.9 ALTER on the session's own group inside a transaction errors
1: SET ROLE rg_alter_tran_role;
1: BEGIN;
1: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 13;
1: ROLLBACK;
1q:

-- 1.10 ALTER on the session's own group outside a transaction succeeds
2: SET ROLE rg_alter_tran_role;
2: ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 4;
2q:
SELECT * FROM rg_alter_tran_status;

-- Multi-ALTER: two ALTERs in one transaction touching different fields
BEGIN;
ALTER RESOURCE GROUP rg_alter_tran SET CONCURRENCY 3;
ALTER RESOURCE GROUP rg_alter_tran SET MEMORY_LIMIT 7;
COMMIT;
SELECT * FROM rg_alter_tran_status;

-- 1.11 pg_resgroup_move_query while another session has uncommitted ALTER
3: BEGIN;
3: ALTER RESOURCE GROUP rg_alter_tran_b SET CONCURRENCY 9;
4: SELECT gp_toolkit.pg_resgroup_move_query(pg_backend_pid(), 'rg_alter_tran_b');
3: ROLLBACK;
3q:
4q:

-- cleanup
DROP VIEW rg_alter_tran_status;
DROP FUNCTION rg_alter_tran_func();
DROP FUNCTION rg_alter_tran_func_sub();
DROP FUNCTION rg_alter_tran_func_sub_fail();
DROP ROLE rg_alter_tran_role;
DROP RESOURCE GROUP rg_alter_tran;
DROP RESOURCE GROUP rg_alter_tran_b;

-- start_ignore
! gpconfig -r gp_resource_group_enable_alter_in_transaction;
! gpstop -rai;
-- end_ignore
