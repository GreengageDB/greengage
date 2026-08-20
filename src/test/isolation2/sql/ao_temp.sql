-- This test checks for correct AO temp relfile deletion after session end.

-- Create table to hold relfile paths
CREATE TABLE ao_temp_relfile_paths (segno int, relpath text) DISTRIBUTED REPLICATED;

-- Create helper functions to get relfiles
CREATE OR REPLACE FUNCTION ao_temp_get_relfile_path_on_segs(tbl regclass)
RETURNS TABLE (segno int, relpath text)
AS $$
    SELECT gp_execution_segment(), pg_relation_filepath(tbl)
$$ LANGUAGE sql EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION ao_temp_get_relfile_path_on_master(tbl regclass)
RETURNS TABLE (segno int, relpath text)
AS $$
    SELECT -1, pg_relation_filepath(tbl)
$$ LANGUAGE sql EXECUTE ON MASTER;

-- Create helper functions to count existing relfiles
CREATE OR REPLACE FUNCTION ao_temp_count_files_on_segs()
RETURNS TABLE (n bigint)
AS $$
    SELECT count(st.size) from ao_temp_relfile_paths fp, pg_stat_file(fp.relpath, true) st 
    WHERE fp.segno = gp_execution_segment()
$$ LANGUAGE sql EXECUTE ON ALL SEGMENTS;

-- This is required to confidently run pg_stat_file on master
-- As there may be a situation when optimizer decides to run it on segments,
-- because pg_stat_file is PROEXECLOCATION_ANY. Therefore it could check master
-- relfiles on segments, resulting in false positive
CREATE OR REPLACE FUNCTION ao_temp_count_files_on_master_impl(paths text[])
RETURNS TABLE (n bigint)
AS $$
    SELECT count(st.size) FROM unnest(paths) p, pg_stat_file(p, true) st
$$ LANGUAGE sql EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION ao_temp_count_files_on_master()
RETURNS TABLE (n bigint)
AS $$
    SELECT c.n FROM ao_temp_count_files_on_master_impl(
        (SELECT array_agg(relpath) FROM ao_temp_relfile_paths WHERE segno = -1)) c
$$ LANGUAGE sql;

-- Create function to wait until relfiles are gone
CREATE OR REPLACE FUNCTION ao_temp_wait_files_gone(timeout_s int) RETURNS bool AS $$ DECLARE total bigint; BEGIN FOR i IN 1 .. timeout_s * 10 LOOP SELECT (SELECT sum(n) FROM ao_temp_count_files_on_segs()) + (SELECT n FROM ao_temp_count_files_on_master()) INTO total; IF total = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- Create tables and get their relfiles
1: CREATE TEMP TABLE temp_relfile_aoro (a int) WITH (APPENDONLY = TRUE, ORIENTATION = ROW);
1: CREATE TEMP TABLE temp_relfile_aoco (a int) WITH (APPENDONLY = TRUE, ORIENTATION = COLUMN);

-- start_ignore
1: INSERT INTO ao_temp_relfile_paths SELECT * FROM ao_temp_get_relfile_path_on_segs('temp_relfile_aoro');
1: INSERT INTO ao_temp_relfile_paths SELECT * FROM ao_temp_get_relfile_path_on_master('temp_relfile_aoro');

1: INSERT INTO ao_temp_relfile_paths SELECT * FROM ao_temp_get_relfile_path_on_segs('temp_relfile_aoco');
1: INSERT INTO ao_temp_relfile_paths SELECT * FROM ao_temp_get_relfile_path_on_master('temp_relfile_aoco');
-- end_ignore

-- Check for relfiles existence
1: SELECT sum(n) = 2 * (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) AS val FROM ao_temp_count_files_on_segs();
1: SELECT n = 2 AS val FROM ao_temp_count_files_on_master();

1q:

-- Wait some time until file cleanup
2: SELECT ao_temp_wait_files_gone(60) AS val;

-- Check for relfiles absence
2: SELECT sum(n) = 0 AS val FROM ao_temp_count_files_on_segs();
2: SELECT n = 0 AS val FROM ao_temp_count_files_on_master();

2q:

-- Cleanup
DROP TABLE IF EXISTS ao_temp_relfile_paths;
DROP FUNCTION IF EXISTS ao_temp_get_relfile_path_on_segs(tbl regclass);
DROP FUNCTION IF EXISTS ao_temp_get_relfile_path_on_master(tbl regclass);
DROP FUNCTION IF EXISTS ao_temp_count_files_on_segs();
DROP FUNCTION IF EXISTS ao_temp_count_files_on_master_impl();
DROP FUNCTION IF EXISTS ao_temp_count_files_on_master();
DROP FUNCTION IF EXISTS ao_temp_wait_files_gone(timeout_s int);
