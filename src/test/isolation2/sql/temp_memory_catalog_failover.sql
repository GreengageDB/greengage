-- Temporary objects kept in memory (catalog/tempcat.c) when processes or
-- nodes fail: a writer QE dies, a segment fails over to its mirror, the
-- coordinator restarts after a crash.  Rows in memory are lost with their
-- process and are not replicated; what the session or the cluster sees
-- afterwards must be as with temporary objects on disk, and the dependency
-- rows left on disk (replicated to the mirrors as other catalog rows) must
-- not keep anything.

-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
!\retcode gpconfig -c gp_gang_creation_retry_count -v 120 --skipvalidation --coordinatoronly;
!\retcode gpconfig -c gp_gang_creation_retry_timer -v 1000 --skipvalidation --coordinatoronly;
!\retcode gpstop -u;
-- end_ignore

CREATE TABLE tcf_sess (id int) DISTRIBUTED REPLICATED;

-- 1. The writer QE of a session dies on segment 0: the gang is reset, and
-- the session's temporary objects are gone, as on disk; it makes new ones.
CREATE TYPE tcf_comp AS (x int);
1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tcf_t (c tcf_comp, a int) DISTRIBUTED BY (a);
1: INSERT INTO tcf_t SELECT ROW(i)::tcf_comp, i FROM generate_series(1, 30) i;
1: INSERT INTO tcf_sess VALUES (current_setting('gp_session_id')::int);
0U: SELECT bool_and(pg_terminate_backend(pid)) AS terminated FROM pg_stat_activity WHERE sess_id IN (SELECT id FROM tcf_sess);
1: SELECT count(*) FROM tcf_t;
1: SELECT to_regclass('tcf_t') IS NULL AS gone;
1: CREATE TEMP TABLE tcf_t2 (a int) DISTRIBUTED BY (a);
1: INSERT INTO tcf_t2 SELECT generate_series(1, 30);
1: SELECT count(*), 'tcf_t2'::regclass::oid >= 4026531840 AS reserved FROM tcf_t2;
2: DROP TYPE tcf_comp;
1q:
DELETE FROM tcf_sess;

-- 2. Segment 0 fails over to its mirror while a session keeps temporary
-- objects in memory there: they are gone with the old primary, the
-- dependency rows replicated to the mirror are leftovers on the new
-- primary, and the session goes on with new ones.
CREATE TYPE tcf_comp AS (x int);
3: SET gp_enable_temp_memory_catalog = on;
3: CREATE TEMP TABLE tcf_m (c tcf_comp, a int) DISTRIBUTED BY (a);
3: INSERT INTO tcf_m SELECT ROW(i)::tcf_comp, i FROM generate_series(1, 30) i;
3: CHECKPOINT;
-1U: SELECT pg_ctl((SELECT datadir FROM gp_segment_configuration c WHERE c.role = 'p' AND c.content = 0), 'stop', 'immediate');
4: SELECT gp_request_fts_probe_scan();
4: SELECT role, preferred_role FROM gp_segment_configuration WHERE content = 0 ORDER BY role;
4: SELECT count(*) > 0 AS leftovers FROM gp_dist_random('pg_depend') WHERE gp_segment_id = 0 AND refobjid = 'tcf_comp'::regtype AND objid >= 4026531840;
3: SELECT count(*) FROM tcf_m;
3: SELECT to_regclass('tcf_m') IS NULL AS gone;
4: DROP TYPE tcf_comp;
4: SELECT count(*) AS leftovers FROM gp_dist_random('pg_depend') WHERE objid >= 4026531840 AND refobjid < 4026531840 AND refclassid = 'pg_type'::regclass;
3: CREATE TEMP TABLE tcf_m2 (a int) DISTRIBUTED BY (a);
3: INSERT INTO tcf_m2 SELECT generate_series(1, 30);
3: SELECT count(*), 'tcf_m2'::regclass::oid >= 4026531840 AS reserved FROM tcf_m2;
3q:
4q:
!\retcode gprecoverseg -a;
SELECT wait_until_all_segments_synchronized();
!\retcode gprecoverseg -ar;
SELECT wait_until_all_segments_synchronized();
SELECT count(*) FROM gp_segment_configuration WHERE status = 'd' OR role <> preferred_role;

-- 3. The coordinator restarts after a crash (the fault makes the next
-- query PANIC there) while a session keeps temporary objects in memory:
-- the writer QEs of the session drop theirs as they exit, the dependency
-- rows left on the coordinator are leftovers, and new sessions keep their
-- temporary objects in memory as before.
CREATE TYPE tcf_comp AS (x int);
5: SET gp_enable_temp_memory_catalog = on;
5: CREATE TEMP TABLE tcf_c (c tcf_comp, a int) DISTRIBUTED BY (a);
5: INSERT INTO tcf_c SELECT ROW(i)::tcf_comp, i FROM generate_series(1, 30) i;
5: INSERT INTO tcf_sess VALUES (current_setting('gp_session_id')::int);
6&: SELECT wait_till_master_shutsdown();
-1U: SELECT gp_inject_fault('exec_simple_query_start', 'panic', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
-1Uq:
5: SELECT 1;
6<:
5q:
6q:
7: DO $$ BEGIN FOR i IN 1..600 LOOP PERFORM pg_stat_clear_snapshot(); IF NOT EXISTS (SELECT 1 FROM gp_dist_random('pg_stat_activity') a JOIN tcf_sess s ON a.sess_id = s.id) THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'the writer QEs did not end'; END $$;
7: SELECT count(*) > 0 AS leftovers FROM pg_depend WHERE refobjid = 'tcf_comp'::regtype AND objid >= 4026531840;
7: DROP TYPE tcf_comp;
7: SELECT count(*) AS leftovers FROM pg_depend WHERE objid >= 4026531840 AND refclassid = 'pg_type'::regclass AND refobjid < 4026531840;
7: SET gp_enable_temp_memory_catalog = on;
7: CREATE TEMP TABLE tcf_c2 (a int) DISTRIBUTED BY (a);
7: INSERT INTO tcf_c2 SELECT generate_series(1, 30);
7: SELECT count(*), 'tcf_c2'::regclass::oid >= 4026531840 AS reserved FROM tcf_c2;
7q:

DROP TABLE tcf_sess;
-- start_ignore
!\retcode gpconfig -r gp_gang_creation_retry_count --skipvalidation --coordinatoronly;
!\retcode gpconfig -r gp_gang_creation_retry_timer --skipvalidation --coordinatoronly;
!\retcode gpstop -u;
-- end_ignore
