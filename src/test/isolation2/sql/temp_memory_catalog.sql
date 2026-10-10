-- Catalog rows of temporary objects kept in memory (catalog/tempcat.c):
-- behaviour that involves several sessions or fault injection.  Single-
-- session behaviour is covered by the temp_memory_catalog regress test.

CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

--
-- 1. Ordinary objects that another session's in-memory temporary objects
-- depend on cannot be dropped.  The dependency rows stay on disk because
-- they reference ordinary objects.  Error messages carry OIDs, so only
-- report whether a drop was refused.
--
CREATE FUNCTION tc_try(cmd text) RETURNS text AS $$ BEGIN EXECUTE cmd; RETURN 'done'; EXCEPTION WHEN dependent_objects_still_exist THEN RETURN 'refused'; END $$ LANGUAGE plpgsql;
CREATE FUNCTION tc_retry(cmd text) RETURNS text AS $$ BEGIN FOR i IN 1..600 LOOP IF tc_try(cmd) = 'done' THEN RETURN 'done'; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN 'refused'; END $$ LANGUAGE plpgsql;
CREATE TYPE tc_type AS (x int);
CREATE FUNCTION tc_func(int) RETURNS int IMMUTABLE LANGUAGE sql AS 'SELECT $1 + 1';
CREATE ROLE tc_role;
DO $$ BEGIN EXECUTE format('GRANT TEMP ON DATABASE %I TO tc_role', current_database()); END $$;
GRANT USAGE ON TYPE tc_type TO tc_role;

1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_dep (a int, c tc_type) DISTRIBUTED BY (a);
1: INSERT INTO tc_dep VALUES (1, ROW(1));
1: CREATE INDEX tc_dep_i ON tc_dep (tc_func(a));
1: SET ROLE tc_role;
1: CREATE TEMP TABLE tc_dep_owned (a int) DISTRIBUTED BY (a);
1: RESET ROLE;

-- No pg_shdepend rows on disk either: the session publishes the roles its
-- in-memory objects refer to (owner, grantees) for DROP ROLE to see.
2: SELECT count(*) AS disk_shdepend FROM pg_shdepend WHERE objid >= 4026531840;
-- (CASCADE reports the dependent objects by OID in a NOTICE)
2: SET client_min_messages = warning;
2: SELECT tc_try('DROP TYPE tc_type');
2: SELECT tc_try('DROP TYPE tc_type CASCADE');
2: SELECT tc_try('DROP FUNCTION tc_func(int)');
2: DO $$ BEGIN EXECUTE format('REVOKE TEMP ON DATABASE %I FROM tc_role', current_database()); END $$;
2: REVOKE USAGE ON TYPE tc_type FROM tc_role;
2: SELECT tc_try('DROP ROLE tc_role');

-- The temporary objects are intact.
1: SELECT (c).x, tc_func(a) FROM tc_dep;
1: SET enable_seqscan = off;
1: SELECT a FROM tc_dep WHERE tc_func(a) = 2;
1: RESET enable_seqscan;
1q:

-- Once session 1 is gone, the objects can be dropped.  Its temporary
-- objects are removed asynchronously at backend exit, so retry for a while.
2: SELECT tc_retry('DROP TYPE tc_type');
2: SELECT tc_retry('DROP FUNCTION tc_func(int)');
2: SELECT tc_retry('DROP ROLE tc_role');
2q:

--
-- 2. Two-phase commit.  A transaction that drops an in-memory temporary
-- table and writes an ordinary table is prepared on all segments; when
-- PREPARE fails on one of them, the others roll the prepared transaction
-- back, and the table must survive everywhere.
--
CREATE TABLE tc_perm (a int) DISTRIBUTED BY (a);
1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_2pc (a int) DISTRIBUTED BY (a);
1: INSERT INTO tc_2pc SELECT generate_series(1, 30);
SELECT gp_inject_fault('start_prepare', 'error', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
1: BEGIN;
1: DROP TABLE tc_2pc;
1: INSERT INTO tc_perm SELECT generate_series(1, 10);
1: COMMIT;
SELECT gp_inject_fault('start_prepare', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
1: SELECT count(*) FROM tc_2pc;
1: INSERT INTO tc_2pc VALUES (100);
1: SELECT count(*) FROM tc_2pc x JOIN tc_2pc y ON x.a = y.a + 1;
1: SELECT count(*) FROM tc_perm;

-- A temporary table created in a rolled back subtransaction of a prepared
-- transaction does not come back.
1: BEGIN;
1: SAVEPOINT s1;
1: CREATE TEMP TABLE tc_2pc_sub (a int) DISTRIBUTED BY (a);
1: ROLLBACK TO SAVEPOINT s1;
1: INSERT INTO tc_perm SELECT generate_series(1, 10);
1: COMMIT;
1: SELECT to_regclass('tc_2pc_sub') IS NULL AS not_created;
1: CREATE TEMP TABLE tc_2pc_sub (a int) DISTRIBUTED BY (a);
1q:
DROP TABLE tc_perm;

--
-- 3. Concurrent sessions get temporary OIDs from separate slices of the
-- reserved range: same names, different OIDs, and locks on their own
-- temporary tables do not conflict.
--
CREATE TABLE tc_oids (session int, reloid oid) DISTRIBUTED REPLICATED;
1: SET gp_enable_temp_memory_catalog = on;
2: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_conc (a int) DISTRIBUTED BY (a);
2: CREATE TEMP TABLE tc_conc (a int) DISTRIBUTED BY (a);
1: INSERT INTO tc_conc SELECT generate_series(1, 10);
2: INSERT INTO tc_conc SELECT generate_series(1, 20);
1: INSERT INTO tc_oids SELECT 1, 'tc_conc'::regclass;
2: INSERT INTO tc_oids SELECT 2, 'tc_conc'::regclass;
SELECT count(DISTINCT reloid) AS distinct_oids, bool_and(reloid >= 4026531840) AS in_reserved_range FROM tc_oids;
1: BEGIN;
1: LOCK TABLE tc_conc IN ACCESS EXCLUSIVE MODE;
2: BEGIN;
2: LOCK TABLE tc_conc IN ACCESS EXCLUSIVE MODE;
2: SELECT count(*) FROM tc_conc;
1: SELECT count(*) FROM tc_conc;
1: COMMIT;
2: COMMIT;
1q:
2q:
DROP TABLE tc_oids;

--
-- 4. Reader gangs that are destroyed when idle and recreated later attach
-- to the in-memory catalog again and see later changes.
--
1: SET gp_enable_temp_memory_catalog = on;
1: SET gp_vmem_idle_resource_timeout = 500;
1: CREATE TEMP TABLE tc_idle (a int, b int) DISTRIBUTED BY (a);
1: INSERT INTO tc_idle SELECT i, i FROM generate_series(1, 100) i;
1: SELECT count(*) FROM tc_idle x JOIN tc_idle y ON x.b = y.a + 1;
!\retcode sleep 2;
1: ALTER TABLE tc_idle ADD COLUMN c int DEFAULT 7;
!\retcode sleep 2;
1: SELECT sum(x.c) FROM tc_idle x JOIN tc_idle y ON x.b = y.a + 1;
1q:

--
-- 5. Ordinary objects can have OIDs in the reserved range (they may have
-- been created before it was reserved, or restored by pg_upgrade).  Catalog
-- rows written for them by a session with in-memory temporary objects must
-- still go to disk.  The fault hands out such OIDs to ordinary objects.
--
SELECT gp_inject_fault_infinite('oid_in_tempcat_range', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TABLE tc_legacy (a int, b int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('oid_in_tempcat_range', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT 'tc_legacy'::regclass::oid >= 4026531840 AS in_reserved_range;
INSERT INTO tc_legacy SELECT i, i FROM generate_series(1, 100) i;

1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_tmp (a int) DISTRIBUTED BY (a);
1: SELECT 'tc_tmp'::regclass::oid >= 4026531840 AS in_reserved_range;
1: ANALYZE tc_legacy;
1: ALTER TABLE tc_legacy ADD COLUMN c int DEFAULT 7;
1: CREATE INDEX tc_legacy_i ON tc_legacy (a);
1: COMMENT ON TABLE tc_legacy IS 'ordinary table';
1q:

SELECT attname FROM pg_attribute WHERE attrelid = 'tc_legacy'::regclass AND attnum > 0 ORDER BY attnum;
SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE attrelid = 'tc_legacy'::regclass AND attname = 'c';
SELECT count(*) FROM pg_index WHERE indrelid = 'tc_legacy'::regclass;
SELECT count(*) > 0 AS has_stats FROM pg_statistic WHERE starelid = 'tc_legacy'::regclass;
SELECT obj_description('tc_legacy'::regclass);
SELECT sum(c) FROM tc_legacy;
DROP TABLE tc_legacy;

--
-- 6. At session exit the temporary tables' files are removed and no
-- catalog rows of objects with reserved OIDs are left on disk.
--
CREATE TABLE tc_relfile_paths (segno int, relpath text) DISTRIBUTED REPLICATED;
CREATE FUNCTION tc_relfile_path_on_segs(tbl regclass) RETURNS TABLE (segno int, relpath text) AS $$ SELECT gp_execution_segment(), pg_relation_filepath(tbl) $$ LANGUAGE sql EXECUTE ON ALL SEGMENTS;
CREATE FUNCTION tc_relfile_path_on_coordinator(tbl regclass) RETURNS TABLE (segno int, relpath text) AS $$ SELECT -1, pg_relation_filepath(tbl) $$ LANGUAGE sql EXECUTE ON COORDINATOR;
CREATE FUNCTION tc_count_files_on_segs() RETURNS TABLE (n bigint) AS $$ SELECT count(st.size) FROM tc_relfile_paths fp, pg_stat_file(fp.relpath, true) st WHERE fp.segno = gp_execution_segment() $$ LANGUAGE sql EXECUTE ON ALL SEGMENTS;
CREATE FUNCTION tc_count_files_on_coordinator() RETURNS TABLE (n bigint) AS $$ SELECT count(st.size) FROM tc_relfile_paths fp, pg_stat_file(fp.relpath, true) st WHERE fp.segno = -1 $$ LANGUAGE sql EXECUTE ON COORDINATOR;
CREATE FUNCTION tc_wait_files_gone(timeout_s int) RETURNS bool AS $$ DECLARE total bigint; BEGIN FOR i IN 1 .. timeout_s * 10 LOOP SELECT (SELECT sum(n) FROM tc_count_files_on_segs()) + (SELECT n FROM tc_count_files_on_coordinator()) INTO total; IF total = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
CREATE FUNCTION tc_disk_rows() RETURNS bigint AS $$ SELECT (SELECT count(*) FROM pg_class WHERE oid >= 4026531840) + (SELECT count(*) FROM gp_dist_random('pg_class') WHERE oid >= 4026531840) + (SELECT count(*) FROM pg_attribute WHERE attrelid >= 4026531840) + (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE attrelid >= 4026531840) + (SELECT count(*) FROM pg_depend WHERE objid >= 4026531840 OR refobjid >= 4026531840) + (SELECT count(*) FROM gp_dist_random('pg_depend') WHERE objid >= 4026531840 OR refobjid >= 4026531840) $$ LANGUAGE sql;

1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_files_heap (a int, b text) DISTRIBUTED BY (a);
1: CREATE INDEX ON tc_files_heap (a);
1: CREATE TEMP TABLE tc_files_ao (a int) WITH (appendonly = true) DISTRIBUTED BY (a);
1: INSERT INTO tc_files_heap SELECT i, repeat('x', 3000) FROM generate_series(1, 10) i;
1: INSERT INTO tc_files_ao SELECT generate_series(1, 10);
-- start_ignore
1: INSERT INTO tc_relfile_paths SELECT * FROM tc_relfile_path_on_segs('tc_files_heap');
1: INSERT INTO tc_relfile_paths SELECT * FROM tc_relfile_path_on_coordinator('tc_files_heap');
1: INSERT INTO tc_relfile_paths SELECT * FROM tc_relfile_path_on_segs('tc_files_ao');
1: INSERT INTO tc_relfile_paths SELECT * FROM tc_relfile_path_on_coordinator('tc_files_ao');
-- end_ignore
1: SELECT sum(n) = 2 * (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) AS files_on_segs FROM tc_count_files_on_segs();
1: SELECT n = 2 AS files_on_coordinator FROM tc_count_files_on_coordinator();
1: SET gp_temp_memory_catalog_disk_only = on;
1: SELECT tc_disk_rows() AS disk_rows;
1q:

2: SELECT tc_wait_files_gone(60) AS files_gone;
2: SELECT tc_disk_rows() AS disk_rows;
2q:

--
-- 7. VACUUM cannot see the pg_class rows of other sessions' in-memory
-- temporary tables, so the sessions publish the oldest relfrozenxid of their
-- tables, and the database's datfrozenxid must not advance past it.  Use a
-- fresh database, so that VACUUM FREEZE is quick.
--
CREATE DATABASE tc_frozen_db;
1:@db_name tc_frozen_db: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_frozen (a int) DISTRIBUTED BY (a);
1: INSERT INTO tc_frozen VALUES (1);
1: CREATE TABLE tc_xid (x bigint) DISTRIBUTED REPLICATED;
-- tc_frozen's relfrozenxid is not newer than this transaction's XID
1: INSERT INTO tc_xid SELECT txid_current() % 4294967296;
1: SET application_name = 'tc_frozen_session';
2:@db_name tc_frozen_db: DO $$ BEGIN FOR i IN 1..100 LOOP PERFORM txid_current(); COMMIT; END LOOP; END $$;
2: VACUUM FREEZE;
2: SELECT datfrozenxid::text::bigint <= (SELECT x FROM tc_xid) AS held_back FROM pg_database WHERE datname = current_database();
1q:
2: DO $$ BEGIN FOR i IN 1..600 LOOP IF NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = 'tc_frozen_session') THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; END $$;
2: VACUUM FREEZE;
2: SELECT datfrozenxid::text::bigint > (SELECT x FROM tc_xid) AS advanced FROM pg_database WHERE datname = current_database();
2q:
DROP DATABASE tc_frozen_db;

--
-- 8. Leftovers of crashed sessions.  A crashed session cannot remove the
-- on-disk rows of its in-memory temporary objects: dependencies on ordinary
-- objects (pg_depend, pg_shdepend) and rows that went to disk when the
-- in-memory area was full.  The first session of the database that keeps
-- temporary objects in memory after the node started removes them; the
-- faults simulate a crashed session and force that cleanup.  (Autovacuum
-- would remove them too, at its own pace; keep it out of the way.)
--
SELECT gp_inject_fault_infinite('tempcat_skip_autovacuum_sweep', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
CREATE TYPE tc_sw_type AS (x int);
CREATE ROLE tc_sw_role;
DO $$ BEGIN EXECUTE format('GRANT TEMP ON DATABASE %I TO tc_sw_role', current_database()); END $$;
GRANT USAGE ON TYPE tc_sw_type TO tc_sw_role;
-- an ordinary table with reserved OIDs must survive the cleanup
SELECT gp_inject_fault_infinite('oid_in_tempcat_range', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TABLE tc_legacy2 (a int, b int) DISTRIBUTED BY (a);
CREATE INDEX tc_legacy2_i ON tc_legacy2 (a);
SELECT gp_inject_fault('oid_in_tempcat_range', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
INSERT INTO tc_legacy2 SELECT i, i FROM generate_series(1, 10) i;
COMMENT ON TABLE tc_legacy2 IS 'ordinary table';

SELECT gp_inject_fault_infinite('skip_temp_relations_cleanup', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
1: SET gp_enable_temp_memory_catalog = on;
1: SET gp_temp_memory_catalog_max_size = '1MB';
1: SET ROLE tc_sw_role;
1: CREATE TEMP TABLE tc_sw_dep (a int, c tc_sw_type) DISTRIBUTED BY (a);
1: DO $$ BEGIN FOR i IN 1..300 LOOP EXECUTE format('CREATE TEMP TABLE tc_sw_%s (a int, b text) DISTRIBUTED BY (a)', i); END LOOP; END $$;
1q:
SELECT gp_wait_until_triggered_fault('skip_temp_relations_cleanup', 1, dbid) FROM gp_segment_configuration WHERE role = 'p';
SELECT gp_inject_fault('skip_temp_relations_cleanup', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';

-- The leftovers are on disk.
SELECT tc_disk_rows() > 0 AS leftovers;
SELECT count(*) > 0 AS leftover_shdepend FROM pg_shdepend WHERE objid >= 4026531840;
DO $$ BEGIN EXECUTE format('REVOKE TEMP ON DATABASE %I FROM tc_sw_role', current_database()); END $$;
REVOKE USAGE ON TYPE tc_sw_type FROM tc_sw_role;

SELECT gp_inject_fault_infinite('tempcat_force_sweep', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
2: SET gp_enable_temp_memory_catalog = on;
2: CREATE TEMP TABLE tc_sw_new (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_force_sweep', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';

SELECT count(*) AS leftover_shdepend FROM pg_shdepend WHERE objid >= 4026531840;
SELECT tc_try('DROP TYPE tc_sw_type');
SELECT tc_try('DROP ROLE tc_sw_role');
2: INSERT INTO tc_sw_new VALUES (1);
2: SELECT count(*) FROM tc_sw_new;
2q:
-- A session that ends without dropping its temporary objects (here the
-- fault skips that; it happens when the cleanup fails) leaves the same
-- leftovers, but no node restart follows.  It asks for another sweep, which
-- the next DROP runs before looking at dependencies.  (Retry: the session
-- asks at the very end of its exit.)  The sweep then runs in a session
-- that has temporary objects of its own, in memory and on disk, and must
-- leave them alone.
CREATE TYPE tc_sw_type2 AS (x int);
4: SET gp_enable_temp_memory_catalog = on;
4: SET gp_temp_memory_catalog_max_size = 1024;
4: DO $$ BEGIN FOR i IN 1..150 LOOP EXECUTE format('CREATE TEMP TABLE tc_own_%s (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)', i); END LOOP; END $$;
SELECT gp_inject_fault_infinite('skip_temp_relations_cleanup', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
3: SET gp_enable_temp_memory_catalog = on;
3: CREATE TEMP TABLE tc_sw_dep2 (a int, c tc_sw_type2) DISTRIBUTED BY (a);
3q:
SELECT gp_wait_until_triggered_fault('skip_temp_relations_cleanup', 1, dbid) FROM gp_segment_configuration WHERE role = 'p';
SELECT gp_inject_fault('skip_temp_relations_cleanup', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';
SELECT count(*) > 0 AS qd_leftovers FROM pg_depend WHERE refobjid = 'tc_sw_type2'::regtype AND objid >= 4026531840;
SELECT count(*) > 0 AS segs_leftovers FROM gp_dist_random('pg_depend') WHERE refobjid = 'tc_sw_type2'::regtype AND objid >= 4026531840;
-- A sweep whose transaction rolls back leaves the leftovers in place and
-- asks for another sweep.  (The query on the segments starts a transaction
-- in the writers that swept there.)
BEGIN;
SELECT tc_retry('DROP TYPE tc_sw_type2');
ROLLBACK;
SELECT count(*) > 0 AS qd_leftovers_again FROM pg_depend WHERE refobjid = 'tc_sw_type2'::regtype AND objid >= 4026531840;
SELECT count(*) > 0 AS segs_leftovers_again FROM gp_dist_random('pg_depend') WHERE refobjid = 'tc_sw_type2'::regtype AND objid >= 4026531840;
4: SELECT tc_retry('DROP TYPE tc_sw_type2');
4: INSERT INTO tc_own_1 VALUES (1, 'x');
4: INSERT INTO tc_own_150 VALUES (1, 'x');
4: SELECT count(*) FROM tc_own_1 JOIN tc_own_150 USING (a);
4: CREATE TEMP TABLE tc_own_new (a int) DISTRIBUTED BY (a);
4: DROP TABLE tc_own_new, tc_own_1, tc_own_150;
4q:
SELECT gp_inject_fault('tempcat_skip_autovacuum_sweep', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';

-- The ordinary table with reserved OIDs is intact; after dropping it,
-- nothing with reserved OIDs is left on disk.
SELECT count(*), sum(b) FROM tc_legacy2;
SELECT count(*) FROM pg_attribute WHERE attrelid = 'tc_legacy2'::regclass AND attnum > 0;
SELECT count(*) FROM pg_index WHERE indrelid = 'tc_legacy2'::regclass;
SELECT obj_description('tc_legacy2'::regclass);
DROP TABLE tc_legacy2;
-- (session 4 removes its on-disk rows asynchronously at exit)
DO $$ BEGIN FOR i IN 1..600 LOOP EXIT WHEN tc_disk_rows() = 0; PERFORM pg_sleep(0.1); END LOOP; END $$;
SELECT tc_disk_rows() AS disk_rows;

--
-- 9. When dynamic shared memory runs short (here the fault pretends so on
-- the coordinator), the session keeps the catalog rows of its temporary
-- objects in the on-disk catalog instead of failing; the segments, which
-- have space, keep theirs in memory.
--
SELECT gp_inject_fault_infinite('tempcat_dsm_space_low', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_noshm (a int) DISTRIBUTED BY (a);
1: INSERT INTO tc_noshm SELECT generate_series(1, 10);
1: SELECT count(*) FROM tc_noshm x JOIN tc_noshm y ON x.a = y.a + 1;
1: SELECT 'tc_noshm'::regclass::oid >= 4026531840 AS in_reserved_range;
SELECT count(*) AS qd_on_disk FROM pg_class WHERE relname = 'tc_noshm';
SELECT count(*) AS segs_on_disk FROM gp_dist_random('pg_class') WHERE relname = 'tc_noshm';
1: CREATE DOMAIN pg_temp.tc_noshm_dom AS int CHECK (VALUE > 0);
1: CREATE TEMP TABLE tc_noshm2 (a pg_temp.tc_noshm_dom CHECK (a < 100)) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_dsm_space_low', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- If such a session crashes, everything it put on disk is left behind, its
-- temporary schema included; the sweep removes all of it, table and domain
-- constraints too.
CREATE TABLE tc_sess_ids (id int) DISTRIBUTED RANDOMLY;
1: INSERT INTO tc_sess_ids SELECT current_setting('gp_session_id')::int;
SELECT gp_inject_fault_infinite('tempcat_skip_autovacuum_sweep', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
SELECT gp_inject_fault_infinite('skip_temp_relations_cleanup', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
1q:
SELECT gp_wait_until_triggered_fault('skip_temp_relations_cleanup', 1, dbid) FROM gp_segment_configuration WHERE role = 'p';
SELECT gp_inject_fault('skip_temp_relations_cleanup', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';
DO $$ BEGIN FOR i IN 1..600 LOOP EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess_ids s ON a.sess_id = s.id); PERFORM pg_sleep(0.1); END LOOP; END $$;
SELECT count(*) AS qd_leftover_schemas FROM pg_namespace WHERE oid >= 4026531840;
SELECT count(*) > 0 AS qd_leftover_constraints FROM pg_constraint WHERE conrelid >= 4026531840 OR contypid >= 4026531840;
SELECT gp_inject_fault_infinite('tempcat_force_sweep', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
2: SET gp_enable_temp_memory_catalog = on;
2: CREATE TEMP TABLE tc_noshm_sweeper (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_force_sweep', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';
2q:
SELECT count(*) AS qd_leftover_schemas FROM pg_namespace WHERE oid >= 4026531840;
SELECT count(*) AS qd_leftover_constraints FROM pg_constraint WHERE conrelid >= 4026531840 OR contypid >= 4026531840;
SELECT gp_inject_fault('tempcat_skip_autovacuum_sweep', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';
DROP TABLE tc_sess_ids;

--
-- 10. Some rows of in-memory temporary objects are on disk: dependencies on
-- ordinary objects, and rows that did not fit in memory.  To other sessions
-- they point at objects that do not exist, which gpcheckcat would report as
-- catalog corruption, so it sets gp_temp_memory_catalog_hide_others: SQL
-- scans then skip on-disk rows of other live sessions' temporary objects.
--
CREATE FUNCTION tc_def() RETURNS int IMMUTABLE LANGUAGE sql AS 'SELECT 1';
CREATE TYPE tc_hide_type AS (x int);
CREATE FUNCTION tc_orphan_deps() RETURNS bigint AS $$ SELECT count(*) FROM pg_depend d LEFT JOIN pg_class c ON d.objid = c.oid WHERE d.classid = 'pg_class'::regclass AND c.oid IS NULL $$ LANGUAGE sql;
1: SET gp_enable_temp_memory_catalog = on;
1: SET gp_temp_memory_catalog_max_size = 1024;
1: CREATE TEMP TABLE tc_hide (a int DEFAULT tc_def(), c tc_hide_type) DISTRIBUTED BY (a);
1: DO $$ BEGIN FOR i IN 1..150 LOOP EXECUTE format('CREATE TEMP TABLE tc_hide_%s (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)', i); END LOOP; END $$;

-- A column default of a temporary table depends on the function.
2: SET client_min_messages = warning;
2: SELECT tc_try('DROP FUNCTION tc_def()');
2: SELECT tc_try('DROP FUNCTION tc_def() CASCADE');

2: SELECT count(*) > 0 AS on_disk FROM pg_class WHERE oid >= 4026531840;
2: SELECT count(*) > 0 AS on_disk FROM gp_dist_random('pg_class') WHERE oid >= 4026531840;
2: SELECT tc_orphan_deps() > 0 AS orphan_deps;
-- (TID scans and COPY TO as well; tc_hide_type is an ordinary object)
2: CREATE TEMP TABLE tc_ctids_hide (t tid) DISTRIBUTED RANDOMLY;
2: DO $$ BEGIN INSERT INTO tc_ctids_hide SELECT ctid FROM pg_class WHERE relname LIKE 'tc\_hide\_%' AND oid >= 4026531840; END $$;
2: CREATE FUNCTION tc_copy_count() RETURNS int LANGUAGE plpgsql AS $$ BEGIN EXECUTE $c$COPY pg_class (relname) TO PROGRAM 'grep -c "^tc_hide_[0-9]" > /tmp/tc_hide_copy.out; true'$c$; RETURN trim(pg_read_file('/tmp/tc_hide_copy.out'))::int; END $$;
2: SET enable_seqscan = off;
2: SELECT count(*) > 0 AS tid_scan FROM pg_class WHERE ctid = ANY (ARRAY(SELECT t FROM tc_ctids_hide));
2: SELECT tc_copy_count() > 0 AS copy_to;
2: SET gp_temp_memory_catalog_hide_others = on;
2: SELECT count(*) AS tid_scan FROM pg_class WHERE ctid = ANY (ARRAY(SELECT t FROM tc_ctids_hide));
2: RESET enable_seqscan;
2: SELECT tc_copy_count() AS copy_to;
2: DROP FUNCTION tc_copy_count();
2: SELECT count(*) FROM pg_class WHERE oid >= 4026531840;
2: SELECT count(*) FROM gp_dist_random('pg_class') WHERE oid >= 4026531840;
2: SELECT count(*) FROM pg_attribute WHERE attrelid >= 4026531840;
2: SELECT count(*) FROM pg_depend WHERE objid >= 4026531840 OR refobjid >= 4026531840;
2: SELECT count(*) FROM gp_dist_random('pg_depend') WHERE objid >= 4026531840 OR refobjid >= 4026531840;
2: SELECT tc_orphan_deps() AS orphan_deps;
-- index scans too (the option rules out index-only and bitmap scans)
2: SET enable_seqscan = off;
2: SELECT count(*) FROM pg_class WHERE oid >= 4026531840;
2: SELECT count(*) FROM pg_type WHERE oid >= 4026531840;
2: SELECT count(*) FROM gp_dist_random('pg_class') WHERE oid >= 4026531840;
-- (pg_dump looks objects up by tableoid)
2: SELECT relname, tableoid::regclass FROM pg_class WHERE oid IN ('pg_am'::regclass, 'pg_class'::regclass) ORDER BY 1;
2: RESET enable_seqscan;
2q:

-- The session itself still sees its rows, wherever they are.
1: SET gp_temp_memory_catalog_hide_others = on;
1: SELECT count(*) FROM pg_class WHERE relname LIKE 'tc_hide%' AND relkind = 'r';
1: SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname LIKE 'tc_hide%' AND relkind = 'r';
1q:

SELECT tc_retry('DROP FUNCTION tc_def()');
SELECT tc_retry('DROP TYPE tc_hide_type');
DROP FUNCTION tc_orphan_deps();

--
-- 11. Parallel retrieve cursors.  The retrieve connections are sessions of
-- their own, yet output values of the cursor session's temporary types
-- (composite, enum, a temporary table's row type): they look the types up
-- in that session's in-memory catalog, on the segments and on the
-- coordinator.
--
1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TYPE pg_temp.tc_prc_comp AS (x int, y text);
1: CREATE TYPE pg_temp.tc_prc_enum AS ENUM ('red', 'green');
1: CREATE TEMP TABLE tc_prc (a int, c pg_temp.tc_prc_comp, e pg_temp.tc_prc_enum) DISTRIBUTED BY (a);
1: INSERT INTO tc_prc VALUES (1, ROW(1, 'v1'), 'green'), (2, ROW(2, 'v2'), 'red'), (3, ROW(3, 'v3'), 'green'), (4, ROW(4, 'v4'), 'red'), (5, ROW(5, 'v5'), 'green'), (6, ROW(6, 'v6'), 'red');
1: SELECT 'tc_prc'::regclass::oid >= 4026531840 AS in_reserved_range;
1: BEGIN;
1: DECLARE c1 PARALLEL RETRIEVE CURSOR FOR SELECT a, c, e, t FROM tc_prc t;
1: @post_run 'parse_endpoint_info 1 1 2 3 4': SELECT endpointname,auth_token,hostname,port,state FROM gp_get_endpoints() WHERE cursorname='c1';
*R: @pre_run 'set_endpoint_variable @ENDPOINT1': RETRIEVE ALL FROM ENDPOINT "@ENDPOINT1";
1: SELECT * FROM gp_wait_parallel_retrieve_cursor('c1', 0);
1: COMMIT;
1: BEGIN;
1: DECLARE c2 PARALLEL RETRIEVE CURSOR FOR SELECT a, c, e FROM tc_prc ORDER BY a;
1: @post_run 'parse_endpoint_info 2 1 2 3 4': SELECT endpointname,auth_token,hostname,port,state FROM gp_get_endpoints() WHERE cursorname='c2';
-1R: @pre_run 'set_endpoint_variable @ENDPOINT2': RETRIEVE ALL FROM ENDPOINT "@ENDPOINT2";
1: SELECT * FROM gp_wait_parallel_retrieve_cursor('c2', 0);
1: COMMIT;
1q:

--
-- 12. pg_dump sets gp_temp_memory_catalog_hide_others too: rows of another
-- session's temporary objects that did not fit in memory point at its
-- temporary schema, and made pg_dump fail ("schema with OID ... does not
-- exist").  The cleanup of crashed sessions' leftovers can run while other
-- sessions keep temporary objects in memory (autovacuum runs it as well),
-- and must leave their on-disk rows alone.
--
CREATE FUNCTION tc_dump_f() RETURNS int IMMUTABLE LANGUAGE sql AS 'SELECT 1';
1: SET gp_enable_temp_memory_catalog = on;
1: SET gp_temp_memory_catalog_max_size = 1024;
1: CREATE TEMP TABLE tc_dump (a int DEFAULT tc_dump_f()) DISTRIBUTED BY (a);
1: DO $$ BEGIN FOR i IN 1..150 LOOP EXECUTE format('CREATE TEMP TABLE tc_dump_%s (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)', i); END LOOP; END $$;
CREATE TABLE tc_spill_counts AS SELECT (SELECT count(*) FROM pg_class WHERE relname LIKE 'tc\_dump\_%') AS qd, (SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname LIKE 'tc\_dump\_%') AS segs DISTRIBUTED RANDOMLY;
SELECT qd > 0 AS qd_spilled, segs > 0 AS segs_spilled FROM tc_spill_counts;
!\retcode pg_dump --schema-only isolation2test > /dev/null;

SELECT gp_inject_fault_infinite('tempcat_force_sweep', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p';
2: SET gp_enable_temp_memory_catalog = on;
2: CREATE TEMP TABLE tc_sweeper (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_force_sweep', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p';
2q:
SELECT (SELECT count(*) FROM pg_class WHERE relname LIKE 'tc\_dump\_%') = qd AS qd_kept, (SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname LIKE 'tc\_dump\_%') = segs AS segs_kept FROM tc_spill_counts;
SELECT tc_try('DROP FUNCTION tc_dump_f()');
1: INSERT INTO tc_dump_150 VALUES (1, 'x');
1: INSERT INTO tc_dump DEFAULT VALUES;
1: SELECT count(*) FROM tc_dump_150 JOIN tc_dump USING (a);
1q:
SELECT tc_retry('DROP FUNCTION tc_dump_f()');
DROP TABLE tc_spill_counts;

--
-- 13. PREPARE TRANSACTION by a user (possible in utility mode only; the
-- fault lifts the check of the simple query protocol, which the extended
-- protocol does not have) is refused for transactions that changed the
-- catalog rows of temporary objects kept in memory: if the session ended
-- before the prepared transaction finished, those changes would be lost
-- while its on-disk effects would still be committed.
--
SELECT gp_inject_fault_infinite('enable_prepare_transaction', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
-1U: SET gp_enable_temp_memory_catalog = on;
-1U: CREATE TEMP TABLE tc_prep (a int);
-1U: SELECT 'tc_prep'::regclass::oid >= 4026531840 AS in_reserved_range;
-1U: BEGIN;
-1U: CREATE TEMP TABLE tc_prep2 (a int);
-1U: PREPARE TRANSACTION 'tc_prep_ddl';
-1U: BEGIN;
-1U: ALTER TABLE tc_prep ADD COLUMN b int;
-1U: PREPARE TRANSACTION 'tc_prep_alter';
-- changing only the data of temporary tables is fine
-1U: BEGIN;
-1U: INSERT INTO tc_prep VALUES (1);
-1U: PREPARE TRANSACTION 'tc_prep_data';
-1U: COMMIT PREPARED 'tc_prep_data';
-1U: SELECT * FROM tc_prep;
-1U: SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'tc\_prep%';
-1Uq:
SELECT gp_inject_fault('enable_prepare_transaction', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

--
-- 14. A session publishes up to 8 roles that the owner and privilege rows
-- (pg_shdepend) of its in-memory objects refer to; the rows for further
-- roles go to disk.  Either way, other sessions cannot drop those roles
-- while the references exist, and can once they are gone: the published
-- list is recomputed at the session's next transaction.  gpcheckcat's
-- hiding covers pg_shdepend rows on disk too.
--
DO $$ BEGIN FOR i IN 1..9 LOOP EXECUTE format('CREATE ROLE tc_r%s', i); END LOOP; END $$;
1: SET gp_enable_temp_memory_catalog = on;
1: CREATE TEMP TABLE tc_acl (a int) DISTRIBUTED BY (a);
1: DO $$ BEGIN FOR i IN 1..9 LOOP EXECUTE format('GRANT SELECT ON tc_acl TO tc_r%s', i); END LOOP; END $$;
2: SELECT count(*) AS shdepend_on_disk FROM pg_shdepend WHERE objid >= 4026531840;
2: SELECT tc_try('DROP ROLE tc_r1');
2: SELECT tc_try('DROP ROLE tc_r9');
2: SET gp_temp_memory_catalog_hide_others = on;
2: SELECT count(*) AS shdepend_on_disk FROM pg_shdepend WHERE objid >= 4026531840;
2: RESET gp_temp_memory_catalog_hide_others;
1: REVOKE SELECT ON tc_acl FROM tc_r1;
1: REVOKE SELECT ON tc_acl FROM tc_r9;
1: SELECT count(*) FROM tc_acl;
2: SELECT tc_try('DROP ROLE tc_r1');
2: SELECT tc_try('DROP ROLE tc_r9');
2q:
1q:
SELECT tc_retry(format('DROP ROLE tc_r%s', i)) FROM generate_series(2, 8) i;

DROP TABLE tc_relfile_paths;
DROP FUNCTION tc_relfile_path_on_segs(regclass);
DROP FUNCTION tc_relfile_path_on_coordinator(regclass);
DROP FUNCTION tc_count_files_on_segs();
DROP FUNCTION tc_count_files_on_coordinator();
DROP FUNCTION tc_wait_files_gone(int);
DROP FUNCTION tc_disk_rows();
DROP FUNCTION tc_retry(text);
DROP FUNCTION tc_try(text);
