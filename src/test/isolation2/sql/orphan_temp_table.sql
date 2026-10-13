-- Test orphan temp table on coordinator. 

-- case 1: Before the fix, when backend process panic on the segment, the temp table will be left on the coordinator.
-- create a temp table
-- (the temporary tables must keep their catalog rows on disk to be orphaned)
1: SET gp_enable_temp_memory_catalog = off;
1: CREATE TEMP TABLE test_temp_table_cleanup(a int);

-- panic on segment 0
1: SELECT gp_inject_fault('before_exec_scan', 'panic', dbid) FROM gp_segment_configuration WHERE role='p' AND content = 0;

-- trigger 'before_exec_scan' panic in ExecScan
1: SELECT * FROM test_temp_table_cleanup;

-- we should not see the temp table on the coordinator
1: SELECT oid, relname, relnamespace FROM pg_class where relname = 'test_temp_table_cleanup';
-- we should not see the temp namespace on the coordinator
1: SELECT count(*) FROM pg_namespace where (nspname like '%pg_temp_%' or nspname like '%pg_toast_temp_%') and oid > 16386;


-- the temp table is left on segment 0, it should be dropped by autovacuum later
0U: SELECT relname FROM pg_class where relname = 'test_temp_table_cleanup';

-- no temp table left on other segments
1U: SELECT oid, relname, relnamespace FROM pg_class where relname = 'test_temp_table_cleanup';

1: SELECT gp_inject_fault('before_exec_scan', 'reset', dbid) FROM gp_segment_configuration WHERE role='p' AND content = 0;
1q:

-- case 2: Test if temp table will be left on the coordinator, when session exits in coordinator within a transaction block.
2: SET gp_enable_temp_memory_catalog = off;
2: CREATE TEMP TABLE test_temp_table_cleanup(a int);
2: begin;
2: select * from test_temp_table_cleanup;
2q:

3: select count(*) from pg_class where relname = 'test_temp_table_cleanup';
3q:

-- case 3: The roles granted privileges on a temp namespace do not depend on it
-- once the session removed it at exit, on any segment (DROP ROLE failed with
-- "cache lookup failed for namespace").
CREATE ROLE temp_schema_grantee;
6: CREATE TEMP TABLE test_temp_schema_grant(a int);
6: DO $$ BEGIN EXECUTE format('GRANT USAGE ON SCHEMA %I TO temp_schema_grantee', (SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema())); END $$;
6: SELECT count(*) FROM gp_dist_random('pg_shdepend') WHERE refobjid = 'temp_schema_grantee'::regrole;
6q:
DO $$ BEGIN FOR i IN 1..600 LOOP EXIT WHEN (SELECT count(*) FROM pg_shdepend WHERE refobjid = 'temp_schema_grantee'::regrole) + (SELECT count(*) FROM gp_dist_random('pg_shdepend') WHERE refobjid = 'temp_schema_grantee'::regrole) = 0; PERFORM pg_sleep(0.1); END LOOP; END $$;
SELECT count(*) FROM pg_shdepend WHERE refobjid = 'temp_schema_grantee'::regrole;
SELECT count(*) FROM gp_dist_random('pg_shdepend') WHERE refobjid = 'temp_schema_grantee'::regrole;
DROP ROLE temp_schema_grantee;

-- Nor once a session removed a temp namespace of its name left by an earlier
-- one (here made up, with the name of session 7).
CREATE ROLE temp_schema_grantee;
7: SET application_name = 'temp_schema_left';
SET allow_system_table_mods = on;
CREATE SCHEMA temp_schema_left;
DO $$ BEGIN EXECUTE format('ALTER SCHEMA temp_schema_left RENAME TO %I', 'pg_temp_' || (SELECT sess_id FROM pg_stat_activity WHERE application_name = 'temp_schema_left')); END $$;
RESET allow_system_table_mods;
DO $$ BEGIN EXECUTE format('GRANT USAGE ON SCHEMA %I TO temp_schema_grantee', 'pg_temp_' || (SELECT sess_id FROM pg_stat_activity WHERE application_name = 'temp_schema_left')); END $$;
SELECT count(*) FROM pg_shdepend WHERE refobjid = 'temp_schema_grantee'::regrole;
SELECT count(*) FROM gp_dist_random('pg_shdepend') WHERE refobjid = 'temp_schema_grantee'::regrole;
7: CREATE TEMP TABLE test_temp_schema_left(a int);
SELECT count(*) FROM pg_shdepend WHERE refobjid = 'temp_schema_grantee'::regrole;
SELECT count(*) FROM gp_dist_random('pg_shdepend') WHERE refobjid = 'temp_schema_grantee'::regrole;
7q:
DROP ROLE temp_schema_grantee;

-- case 4: Test if temp namespace will be left if session exits during a long insert operation

4: SET gp_enable_temp_memory_catalog = off;
4: CREATE TEMP TABLE test_temp_table_cleanup(a int);
4: BEGIN;
-- simulate a long insert query
4: SELECT gp_inject_fault('heap_insert', 'infinite_loop', '', '',
   'test_temp_table_cleanup', 1, 1, 0, dbid) FROM gp_segment_configuration
   WHERE content = 0 AND role = 'p';
4&: INSERT INTO test_temp_table_cleanup SELECT generate_series(1, 100);

-- trigger a panic on the segment
5: SELECT gp_inject_fault('create_function_fail', 'panic', dbid) FROM gp_segment_configuration WHERE role='p' AND content = 0;
5: CREATE FUNCTION my_function() RETURNS void AS $$ BEGIN END; $$ LANGUAGE plpgsql;

-- the insert query should have failed
4<:
4q:

5: CREATE TABLE ensure_segment_is_up(a int);
5: DROP TABLE ensure_segment_is_up;

5: SELECT gp_inject_fault('create_function_fail', 'reset', dbid) FROM gp_segment_configuration WHERE role='p' AND content = 0;
5: SELECT gp_inject_fault('heap_insert', 'reset', dbid) FROM gp_segment_configuration WHERE role='p' AND content = 0;
-- there shouldn't be any temporary namespaces left after the session quits
5: SELECT count(*) FROM pg_namespace where (nspname like '%pg_temp_%' or nspname like '%pg_toast_temp_%') and oid > 16386;
5q:
-- start_ignore
-- After error, temp schemas may still exist at segments
-- Let's remove all such temporary schemas for inactive connections
! psql -d isolation2test -f ../regress/sql/remove_temp_schemas.sql;
-- end_ignore
