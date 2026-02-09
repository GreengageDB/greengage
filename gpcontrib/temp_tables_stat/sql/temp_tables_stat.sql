-- start_ignore
0: ! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'temp_tables_stat'), ',')" postgres)";
0: ! gpstop -raiq;

1: CREATE EXTENSION IF NOT EXISTS temp_tables_stat;
-- end_ignore

1: CREATE OR REPLACE FUNCTION get_files
(OUT user_id_ok bool, OUT cur_sess_id bool, OUT content int2, OUT size int8)
RETURNS SETOF record
AS $$
    SELECT (SELECT a.oid = f.user_id
             FROM pg_authid a
            WHERE a.rolname = current_user) user_id_ok,
          (SELECT s.setting::int = f.sess_id
             FROM pg_settings s
            WHERE name = 'gp_session_id') cur_sess_id,
          content,
          size
     FROM tts_get_seg_files() f; --
$$ LANGUAGE SQL;

1: ! mkdir -p /tmp/tts_tblspace;
1: CREATE TABLESPACE tts_tblspace LOCATION '/tmp/tts_tblspace';

-- No tables, the files list is empty
1: SELECT * FROM tts_get_seg_files();

--
-- Ordinary heap tables

-- We can see tables created in the current session
1: CREATE TEMP TABLE t1(i INT) DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: CREATE TEMP TABLE ts(i INT)
   TABLESPACE tts_tblspace
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t2(i INT) DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t3(i INT) DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: DROP TABLE ts;

-- We can see tables created in other sessions
2: CREATE TEMP TABLE t1(i INT) DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
2: SELECT * FROM get_files();
3: SELECT * FROM get_files();

-- Dropped tables are removed from the list in all sessions
1: DROP TABLE t2;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
2: DROP TABLE t1;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
1q:
2q:
3q:

--
-- Heap tables, on commit drop 

-- We can see tables created in the current session
1: BEGIN;
1: CREATE TEMP TABLE t1(i INT) ON COMMIT DROP DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: CREATE TEMP TABLE ts(i INT)
   ON COMMIT DROP
   TABLESPACE tts_tblspace
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t2(i INT) ON COMMIT DROP DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t3(i INT) ON COMMIT DROP DISTRIBUTED BY (i);
1: SELECT * FROM get_files();

-- We can see tables created in other sessions
2: BEGIN;
2: CREATE TEMP TABLE t1(i INT) ON COMMIT DROP DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
2: SELECT * FROM get_files();
3: SELECT * FROM get_files();

-- Dropped tables are removed from the list in all sessions
1: ROLLBACK;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
2: COMMIT;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
1q:
2q:
3q:

--
-- Ordinary AO tables
-- 4 files per AO table: data file, pg_aoseg, pg_aovisimap, pg_aovisimap_index

-- We can see tables created in the current session
1: CREATE TEMP TABLE t1(i INT) WITH (APPENDOPTIMIZED = TRUE) DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: CREATE TEMP TABLE ts(i INT)
   WITH (APPENDOPTIMIZED = TRUE)
   TABLESPACE tts_tblspace
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t2(i INT) WITH (APPENDOPTIMIZED = TRUE) DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t3(i INT) WITH (APPENDOPTIMIZED = TRUE) DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: DROP TABLE ts;


-- We can see tables created in other sessions
2: CREATE TEMP TABLE t1(i INT) WITH (APPENDOPTIMIZED = TRUE) DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
2: SELECT * FROM get_files();
3: SELECT * FROM get_files();

-- Dropped tables are removed from the list in all sessions
1: DROP TABLE t2;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
2: DROP TABLE t1;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
1q:
2q:
3q:

--
-- AO tables, on commit drop 

-- We can see tables created in the current session
1: BEGIN;
1: CREATE TEMP TABLE t1(i INT) WITH (APPENDOPTIMIZED = TRUE) ON COMMIT DROP DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: CREATE TEMP TABLE ts(i INT)
   WITH (APPENDOPTIMIZED = TRUE)
   ON COMMIT DROP
   TABLESPACE tts_tblspace
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t2(i INT) WITH (APPENDOPTIMIZED = TRUE) ON COMMIT DROP DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t3(i INT) WITH (APPENDOPTIMIZED = TRUE) ON COMMIT DROP DISTRIBUTED BY (i);
1: SELECT * FROM get_files();

-- We can see tables created in other sessions
2: BEGIN;
2: CREATE TEMP TABLE t1(i INT) WITH (APPENDOPTIMIZED = TRUE) ON COMMIT DROP DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
2: SELECT * FROM get_files();
3: SELECT * FROM get_files();

-- Dropped tables are removed from the list in all sessions
1: ROLLBACK;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
2: COMMIT;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
1q:
2q:
3q:

--
-- Ordinary AOCO tables
-- 4 files per AOCO table: data file, pg_aocsseg, pg_aovisimap, pg_aovisimap_index

-- We can see tables created in the current session
1: CREATE TEMP TABLE t1(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: CREATE TEMP TABLE ts(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   TABLESPACE tts_tblspace
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t2(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t3(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: DROP TABLE ts;

-- We can see tables created in other sessions
2: CREATE TEMP TABLE t1(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
2: SELECT * FROM get_files();
3: SELECT * FROM get_files();

-- Dropped tables are removed from the list in all sessions
1: DROP TABLE t2;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
2: DROP TABLE t1;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
1q:
2q:
3q:


--
-- AOCO tables, on commit drop 

-- We can see tables created in the current session
1: BEGIN;
1: CREATE TEMP TABLE t1(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   ON COMMIT DROP
   DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
1: CREATE TEMP TABLE ts(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   ON COMMIT DROP
   TABLESPACE tts_tblspace
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t2(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   ON COMMIT DROP
   DISTRIBUTED BY (i);
1: CREATE TEMP TABLE t3(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   ON COMMIT DROP
   DISTRIBUTED BY (i);
1: SELECT * FROM get_files();

-- We can see tables created in other sessions
2: BEGIN;
2: CREATE TEMP TABLE t1(i INT, j INT)
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   ON COMMIT DROP
   DISTRIBUTED BY (i);
1: SELECT * FROM get_files();
2: SELECT * FROM get_files();
3: SELECT * FROM get_files();

-- Dropped tables are removed from the list in all sessions
1: ROLLBACK;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
2: COMMIT;
1: SELECT COUNT(*) FROM tts_get_seg_files();
2: SELECT COUNT(*) FROM tts_get_seg_files();
3: SELECT COUNT(*) FROM tts_get_seg_files();
1q:
2q:
3q:

--
-- Check that files size calculation takes into account all the forks
CREATE TEMP TABLE t1
   WITH (APPENDOPTIMIZED = TRUE, ORIENTATION = COLUMN)
   AS SELECT i, i j FROM generate_series(1, 100) i
   DISTRIBUTED BY (i);
-- t1 consists of two colums. Both column files are taken into account
SELECT content, size FROM tts_get_seg_files();
-- Vaccum adds FSM and VM
VACUUM t1;
SELECT content, size FROM tts_get_seg_files();

--
-- Cleanup
DROP TABLESPACE tts_tblspace;
DROP FUNCTION get_files
(OUT user_id_ok bool, OUT cur_sess_id bool, OUT content int2, OUT size int8);

DROP EXTENSION temp_tables_stat;

-- start_ignore
! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_remove(string_to_array(current_setting('shared_preload_libraries'), ','), 'temp_tables_stat'), ',')" postgres)";
! gpstop -raiq;
-- end_ignore
