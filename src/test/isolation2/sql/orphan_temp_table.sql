-- Test orphan temp table on coordinator. 

-- case 1: Before the fix, when backend process panic on the segment, the temp table will be left on the coordinator.
-- create a temp table
1: CREATE TEMP TABLE test_temp_table_cleanup(a int);

-- panic on segment 0
1: SELECT gp_inject_fault('before_exec_scan', 'panic', dbid) FROM gp_segment_configuration WHERE role='p' AND content = 0;

-- trigger 'before_exec_scan' panic in ExecScan
1: SELECT * FROM test_temp_table_cleanup;

-- we should not see the temp table on the coordinator
1: SELECT oid, relname, relnamespace FROM pg_class where relname = 'test_temp_table_cleanup';
-- we should not see the temp namespace on the coordinator
1: SELECT count(*) FROM pg_namespace where (nspname like '%pg_temp_%' or nspname like '%pg_toast_temp_%') and oid > 16386;


-- the temp table is left on segment 0, it should be dropped by gpcheckcat later
0U: SELECT relname FROM pg_class where relname = 'test_temp_table_cleanup';

-- no temp table left on other segments
1U: SELECT oid, relname, relnamespace FROM pg_class where relname = 'test_temp_table_cleanup';

1: SELECT gp_inject_fault('before_exec_scan', 'reset', dbid) FROM gp_segment_configuration WHERE role='p' AND content = 0;
1q:

!\retcode gpcheckcat -O -R orphaned_toast_tables isolation2test;

-- no temp table left
0U: SELECT relname FROM pg_class where relname = 'test_temp_table_cleanup';

-- case 2: Test if temp table will be left on the coordinator, when session exits in coordinator within a transaction block.
2: CREATE FUNCTION wait_until_backend_exits(query_pattern text) RETURNS void AS $$ DECLARE count int; BEGIN loop PERFORM pg_stat_clear_snapshot(); SELECT count(*) FROM pg_stat_activity WHERE query = query_pattern INTO count; EXIT WHEN count = 0; PERFORM pg_sleep(0.1); end loop; END; $$ LANGUAGE plpgsql;

2: CREATE TEMP TABLE test_temp_table_cleanup(a int);
2: begin;
2: select * from test_temp_table_cleanup;
2q:

-- wait until cleanup is complete
3: select wait_until_backend_exits('select * from test_temp_table_cleanup;');

3: select count(*) from pg_class where relname = 'test_temp_table_cleanup';
3: drop function wait_until_backend_exits(query_pattern text);
3q:

-- case 3: Test if temp namespace will be left if session exits during a long insert operation

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
