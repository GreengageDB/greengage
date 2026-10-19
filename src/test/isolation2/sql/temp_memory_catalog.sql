-- Temporary objects kept in memory (catalog/tempcat.c) as other sessions
-- see them.
--
-- All their catalog rows are in their session's memory, also their
-- dependencies on ordinary objects: other sessions cannot see the objects.
-- The session counts itself among the users of each ordinary object they
-- use instead, in shared memory, which keeps other sessions from dropping
-- or changing it: they fail, as they would on dependent objects they could
-- see, naming the session's process.

-- Sessions that quit are gone only once their processes on all nodes are.
CREATE TABLE tc_sess (id int) DISTRIBUTED REPLICATED;
CREATE PROCEDURE tc_wait_gone() LANGUAGE plpgsql AS $$ BEGIN FOR i IN 1..600 LOOP IF NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess s ON a.sess_id = s.id) AND NOT EXISTS (SELECT 1 FROM gp_dist_random('pg_stat_activity') a JOIN tc_sess s ON a.sess_id = s.id) THEN RETURN; END IF; COMMIT; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'session did not end'; END $$;

CREATE TYPE tc_comp AS (x int, y int);
CREATE DOMAIN tc_dom AS int;
CREATE DOMAIN tc_rb AS int;
CREATE TABLE tc_ord (a int, b int) DISTRIBUTED BY (a);
CREATE FUNCTION tc_fn(int) RETURNS int LANGUAGE sql AS 'SELECT $1';
CREATE ROLE tc_role;
CREATE ROLE tc_grantee;
GRANT ALL ON tc_ord TO tc_role;

1: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
1: SET gp_enable_temp_memory_catalog = on;
1: SET ROLE tc_role;
1: CREATE TEMP TABLE tc_t (c tc_comp, d tc_dom, e int DEFAULT tc_fn(1)) DISTRIBUTED BY (e);
1: CREATE TEMP VIEW tc_v AS SELECT a, b FROM tc_ord;
1: GRANT SELECT ON tc_t TO tc_grantee;
1: INSERT INTO tc_t VALUES ((1, 2), 3);
1: SELECT 'tc_t'::regclass::oid >= 4026531840 AS reserved;

-- Other sessions see neither the objects nor their schema, nor their
-- dependencies.
2: SELECT count(*) FROM pg_class WHERE relname IN ('tc_t', 'tc_v');
2: SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname IN ('tc_t', 'tc_v');
2: SELECT count(*) FROM gp_dist_random('pg_depend') WHERE objid >= 4026531840;
2: SELECT count(*) FROM gp_dist_random('pg_shdepend') WHERE objid >= 4026531840;

-- They cannot drop what the objects use, also with CASCADE, which cannot
-- reach into the session's memory.
2: DROP TYPE tc_comp;
2: DROP TYPE tc_comp CASCADE;
2: DROP DOMAIN tc_dom CASCADE;
2: DROP FUNCTION tc_fn(int) CASCADE;
2: DROP TABLE tc_ord CASCADE;
2: ALTER TABLE tc_ord DROP COLUMN b CASCADE;
-- Nor change it so that the objects would not work any more.
2: ALTER TYPE tc_comp ALTER ATTRIBUTE y TYPE text;
2: ALTER DOMAIN tc_dom ADD CONSTRAINT tc_dom_check CHECK (VALUE > 0);
2: ALTER TABLE tc_ord ALTER COLUMN b TYPE text;
-- Nor drop the roles owning them or granted privileges on them.  DROP
-- OWNED and REASSIGN OWNED leave them to their session, with a warning.
2: DROP ROLE tc_role;
2: DROP ROLE tc_grantee;
2: REASSIGN OWNED BY tc_role TO CURRENT_USER;
1: SELECT * FROM tc_t;
1: SELECT count(*) FROM tc_v;
2: DROP OWNED BY tc_role;
2: DROP ROLE tc_role;
1: SELECT * FROM tc_t;
-- Other changes work, also of the columns that the objects do not use, as
-- on disk.
2: ALTER TYPE tc_comp RENAME ATTRIBUTE x TO x1;
2: ALTER TABLE tc_ord ADD COLUMN c int;
2: ALTER TABLE tc_ord ALTER COLUMN c TYPE bigint;
2: ALTER TABLE tc_ord DROP COLUMN c;
2: ALTER TABLE tc_ord ADD COLUMN c int;
2: COMMENT ON FUNCTION tc_fn(int) IS 'used';

-- The counts survive errors of the session's transactions.
1: BEGIN;
1: SELECT 1/0;
1: ROLLBACK;
2: DROP TYPE tc_comp;
-- They go with the last object using what they count, once the session
-- goes idle, also when its creation rolled back.
1: DROP VIEW tc_v;
2: DROP TABLE tc_ord;
1: BEGIN;
1: SAVEPOINT s1;
1: CREATE TEMP TABLE tc_t2 (a int, d tc_rb) DISTRIBUTED BY (a);
1: ROLLBACK TO SAVEPOINT s1;
1: COMMIT;
2: DROP DOMAIN tc_rb;
-- They take no room in the lock table, nor go with pg_advisory_unlock_all().
2: SELECT count(*) FROM pg_locks WHERE locktype = 'object' AND objid = 'tc_fn(int)'::regprocedure;
1: SELECT pg_advisory_unlock_all();
2: DROP FUNCTION tc_fn(int);

-- Once the session is gone, so is all of that.
1q:
2: CALL tc_wait_gone();
2: DROP TYPE tc_comp;
2: DROP DOMAIN tc_dom;
2: DROP FUNCTION tc_fn(int);
2: DROP ROLE tc_role;
2: DROP ROLE tc_grantee;
2q:

-- A session that does not drop its temporary objects at exit (here as if
-- it crashed) leaves nothing behind either.
CREATE TYPE tc_comp AS (x int, y int);
3: SET gp_enable_temp_memory_catalog = on;
3: CREATE TEMP TABLE tc_t (c tc_comp, e int) DISTRIBUTED BY (e);
3: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
3: SELECT gp_inject_fault('skip_temp_relations_cleanup', 'skip', dbid)
     FROM gp_segment_configuration WHERE role = 'p';
3q:
4: SELECT gp_wait_until_triggered_fault('skip_temp_relations_cleanup', 1, dbid)
     FROM gp_segment_configuration WHERE role = 'p';
4: SELECT gp_inject_fault('skip_temp_relations_cleanup', 'reset', dbid)
     FROM gp_segment_configuration WHERE role = 'p';
4: CALL tc_wait_gone();
4: DROP TYPE tc_comp;
4: SELECT count(*) FROM gp_dist_random('pg_depend') WHERE objid >= 4026531840;

-- A session takes a slice of the reserved OIDs that no session took since
-- the last one let go of it: not the one of a session that just ended,
-- whose processes on the segments may still be dropping its objects.
CREATE TABLE tc_slices (n int, slice bigint) DISTRIBUTED REPLICATED;
5: SET gp_enable_temp_memory_catalog = on;
5: CREATE TEMP TABLE tc_s (a int) DISTRIBUTED BY (a);
5: INSERT INTO tc_slices VALUES (5, ('tc_s'::regclass::oid::bigint - 4026531840) >> 16);
5q:
6: SET gp_enable_temp_memory_catalog = on;
6: CREATE TEMP TABLE tc_s (a int) DISTRIBUTED BY (a);
6: INSERT INTO tc_slices VALUES (6, ('tc_s'::regclass::oid::bigint - 4026531840) >> 16);
6: SELECT count(DISTINCT slice) AS slices FROM tc_slices;
6q:
DROP TABLE tc_slices;

-- Ordinary objects with reserved OIDs (created before the range was
-- reserved, or carried over by pg_upgrade) keep their dependencies, also
-- in catalogs whose objects cannot be temporary objects kept in memory: the
-- function's support entry goes with it, as it would otherwise.
CREATE FUNCTION tc_cmp(int4, int4) RETURNS int LANGUAGE internal IMMUTABLE AS 'btint4cmp';
CREATE OPERATOR FAMILY tc_fam USING btree;
SELECT gp_inject_fault('oid_in_tempcat_range', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
ALTER OPERATOR FAMILY tc_fam USING btree ADD FUNCTION 1 (int4, int4) tc_cmp(int4, int4);
SELECT gp_inject_fault('oid_in_tempcat_range', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT count(*) AS reserved FROM pg_amproc
 WHERE oid >= 4026531840 AND amprocfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'tc_fam');
DROP FUNCTION tc_cmp(int4, int4);
SELECT count(*) AS left_entries FROM pg_amproc
 WHERE amprocfamily = (SELECT oid FROM pg_opfamily WHERE opfname = 'tc_fam');
DROP OPERATOR FAMILY tc_fam USING btree;

-- Only the coordinator leaves room in dynamic shared memory to others when
-- a session starts to keep temporary objects in memory: a segment cannot
-- decide otherwise, and makes do with what there is.
SELECT gp_inject_fault_infinite('dsm_slots_low', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
11: SET gp_enable_temp_memory_catalog = on;
11: CREATE TEMP TABLE tc_t (a int PRIMARY KEY) DISTRIBUTED BY (a);
11: INSERT INTO tc_t SELECT generate_series(1, 10);
11: SELECT count(*), 'tc_t'::regclass::oid >= 4026531840 AS reserved FROM tc_t;
SELECT gp_inject_fault('dsm_slots_low', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
11q:

-- Two-phase commit: a transaction that drops a temporary table kept in
-- memory and writes an ordinary table is prepared on all segments; when
-- PREPARE fails on one of them, the others roll the prepared transaction
-- back, and the table must survive everywhere.
CREATE TABLE tc_perm (a int) DISTRIBUTED BY (a);
14: SET gp_enable_temp_memory_catalog = on;
14: CREATE TEMP TABLE tc_2pc (a int) DISTRIBUTED BY (a);
14: INSERT INTO tc_2pc SELECT generate_series(1, 30);
SELECT gp_inject_fault('start_prepare', 'error', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
14: BEGIN;
14: DROP TABLE tc_2pc;
14: INSERT INTO tc_perm SELECT generate_series(1, 10);
14: COMMIT;
SELECT gp_inject_fault('start_prepare', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
14: SELECT count(*) FROM tc_2pc;
14: INSERT INTO tc_2pc VALUES (100);
14: SELECT count(*) FROM tc_2pc x JOIN tc_2pc y ON x.a = y.a + 1;
14: SELECT count(*) FROM tc_perm;
-- A temporary table created in a rolled back subtransaction of a prepared
-- transaction does not come back.
14: BEGIN;
14: SAVEPOINT s1;
14: CREATE TEMP TABLE tc_2pc_sub (a int) DISTRIBUTED BY (a);
14: ROLLBACK TO SAVEPOINT s1;
14: INSERT INTO tc_perm SELECT generate_series(1, 10);
14: COMMIT;
14: SELECT to_regclass('tc_2pc_sub') IS NULL AS not_created;
14: CREATE TEMP TABLE tc_2pc_sub (a int) DISTRIBUTED BY (a);
14q:
DROP TABLE tc_perm;

-- Concurrent sessions get OIDs from separate slices: same names, different
-- OIDs, and locks on their own temporary tables do not conflict.
CREATE TABLE tc_oids (session int, reloid oid) DISTRIBUTED REPLICATED;
15: SET gp_enable_temp_memory_catalog = on;
16: SET gp_enable_temp_memory_catalog = on;
15: CREATE TEMP TABLE tc_conc (a int) DISTRIBUTED BY (a);
16: CREATE TEMP TABLE tc_conc (a int) DISTRIBUTED BY (a);
15: INSERT INTO tc_conc SELECT generate_series(1, 10);
16: INSERT INTO tc_conc SELECT generate_series(1, 20);
15: INSERT INTO tc_oids SELECT 1, 'tc_conc'::regclass;
16: INSERT INTO tc_oids SELECT 2, 'tc_conc'::regclass;
SELECT count(DISTINCT reloid) AS distinct_oids, bool_and(reloid >= 4026531840) AS reserved FROM tc_oids;
15: BEGIN;
15: LOCK TABLE tc_conc IN ACCESS EXCLUSIVE MODE;
16: BEGIN;
16: LOCK TABLE tc_conc IN ACCESS EXCLUSIVE MODE;
16: SELECT count(*) FROM tc_conc;
15: SELECT count(*) FROM tc_conc;
15: COMMIT;
16: COMMIT;
15q:
16q:
DROP TABLE tc_oids;

-- A utility-mode session on a segment keeps its temporary objects on disk:
-- OIDs from the reserved range could be the ones that the coordinator gives
-- the temporary objects of its sessions there.
0U: SET gp_enable_temp_memory_catalog = on;
0U: CREATE TEMP TABLE tc_utility (a int);
0U: SELECT 'tc_utility'::regclass::oid >= 4026531840 AS reserved;
0Uq:
-- A utility-mode session on the coordinator keeps them in memory, but
-- cannot PREPARE a transaction that changed them (which utility mode allows
-- through the extended query protocol, or as here with the fault): the
-- changes would be lost with the session.
SELECT gp_inject_fault_infinite('enable_prepare_transaction', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
-1U: SET gp_enable_temp_memory_catalog = on;
-1U: CREATE TEMP TABLE tc_utility (a int);
-1U: SELECT 'tc_utility'::regclass::oid >= 4026531840 AS reserved;
-1U: BEGIN;
-1U: ALTER TABLE tc_utility ADD COLUMN b int;
-1U: PREPARE TRANSACTION 'tc_utility_prep';
-1U: SELECT attname FROM pg_attribute WHERE attrelid = 'tc_utility'::regclass AND attnum > 0;
-1U: SELECT count(*) FROM pg_prepared_xacts WHERE gid = 'tc_utility_prep';
-1Uq:
SELECT gp_inject_fault('enable_prepare_transaction', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

-- Reader gangs that are destroyed when idle and created again later attach
-- to the writer's rows again, and see later changes.
17: SET gp_enable_temp_memory_catalog = on;
17: SET gp_vmem_idle_resource_timeout = 500;
17: CREATE TEMP TABLE tc_idle (a int, b int) DISTRIBUTED BY (a);
17: INSERT INTO tc_idle SELECT i, i FROM generate_series(1, 100) i;
17: SELECT count(*) FROM tc_idle x JOIN tc_idle y ON x.b = y.a + 1;
!\retcode sleep 2;
17: ALTER TABLE tc_idle ADD COLUMN c int DEFAULT 7;
!\retcode sleep 2;
17: SELECT sum(x.c) FROM tc_idle x JOIN tc_idle y ON x.b = y.a + 1;
17q:

-- A segment refuses a reserved OID that a row of its own already has (here
-- a made-up pg_class row of a transaction in progress in utility mode, as
-- of an ordinary object that exists on this node only): two rows with one
-- OID would corrupt its catalog.  (The session's OIDs follow one another:
-- the next one goes to the next table.)
CREATE TABLE tc_ref_oid (x oid) DISTRIBUTED REPLICATED;
18: SET gp_enable_temp_memory_catalog = on;
18: CREATE TEMP TABLE tc_ref1 (a int) DISTRIBUTED BY (a);
18: INSERT INTO tc_ref_oid SELECT (max(oid)::bigint + 1)::oid FROM (SELECT oid FROM pg_class WHERE relnamespace = pg_my_temp_schema() UNION ALL SELECT oid FROM pg_type WHERE typnamespace = pg_my_temp_schema()) s;
0U: BEGIN;
0U: SET allow_system_table_mods = on;
0U: DO $$ BEGIN EXECUTE (SELECT format('INSERT INTO pg_class SELECT %s FROM pg_class WHERE oid = %s', string_agg(CASE attname WHEN 'oid' THEN (SELECT x FROM tc_ref_oid)::text || '::oid' WHEN 'relname' THEN '''tc_ref_fake''' ELSE quote_ident(attname) END, ', ' ORDER BY attnum), 'pg_am'::regclass::oid) FROM pg_attribute WHERE attrelid = 'pg_class'::regclass AND attnum > 0); END $$;
18: CREATE TEMP TABLE tc_ref2 (a int) DISTRIBUTED BY (a);
0U: ROLLBACK;
0Uq:
18: CREATE TEMP TABLE tc_ref2 (a int) DISTRIBUTED BY (a);
18: INSERT INTO tc_ref2 SELECT generate_series(1, 10);
18: SELECT count(*), 'tc_ref2'::regclass::oid > (SELECT x FROM tc_ref_oid) AS next_oid FROM tc_ref2;
18q:
DROP TABLE tc_ref_oid;

-- A writer QE waits for the writer QE of the session that held the slice
-- before, which may still be on its way out: here, on segment 0, a fault
-- holds it there, and another makes the next session take the same slice.
12: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
12: SET gp_enable_temp_memory_catalog = on;
12: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
12: INSERT INTO tc_t SELECT generate_series(1, 10);
SELECT gp_inject_fault('tempcat_release_slice', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
12q:
SELECT gp_wait_until_triggered_fault('tempcat_release_slice', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
DO $$ BEGIN FOR i IN 1..600 LOOP PERFORM pg_stat_clear_snapshot(); IF NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess s ON a.sess_id = s.id) THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'session did not end on the coordinator'; END $$;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT gp_inject_fault('tempcat_claim_slice_wait', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
13: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
13: SET gp_enable_temp_memory_catalog = on;
13&: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
SELECT gp_wait_until_triggered_fault('tempcat_claim_slice_wait', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_release_slice', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
13<:
13: INSERT INTO tc_t SELECT generate_series(1, 10);
13: SELECT count(*), 'tc_t'::regclass::oid >= 4026531840 AS reserved FROM tc_t;
SELECT gp_inject_fault('tempcat_claim_slice_wait', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
13q:
4: CALL tc_wait_gone();

-- A slice that a segment refuses, as its earlier holder's writer QE
-- lingers there (here a fault holds it on segment 0, and another makes the
-- wait give up at once), is given up when the transaction that started to
-- use it rolls back, with nothing kept: the next attempt takes another one.
19: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
19: SET gp_enable_temp_memory_catalog = on;
19: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_release_slice', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
19q:
SELECT gp_wait_until_triggered_fault('tempcat_release_slice', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
DO $$ BEGIN FOR i IN 1..600 LOOP PERFORM pg_stat_clear_snapshot(); IF NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess s ON a.sess_id = s.id) THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'session did not end on the coordinator'; END $$;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT gp_inject_fault('tempcat_claim_slice_timeout', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
20: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
20: SET gp_enable_temp_memory_catalog = on;
20: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
20: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
20: INSERT INTO tc_t SELECT generate_series(1, 10);
20: SELECT count(*), 'tc_t'::regclass::oid >= 4026531840 AS reserved FROM tc_t;
SELECT gp_inject_fault('tempcat_release_slice', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_claim_slice_timeout', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
20q:
4: CALL tc_wait_gone();

-- The same when PL/pgSQL catches the error in a subtransaction: the retry
-- in the same transaction takes another slice, rather than waiting for the
-- segment again.
39: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
39: SET gp_enable_temp_memory_catalog = on;
39: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_release_slice', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
39q:
SELECT gp_wait_until_triggered_fault('tempcat_release_slice', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
DO $$ BEGIN FOR i IN 1..600 LOOP PERFORM pg_stat_clear_snapshot(); IF NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess s ON a.sess_id = s.id) THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'session did not end on the coordinator'; END $$;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT gp_inject_fault('tempcat_claim_slice_timeout', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
40: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
40: SET gp_enable_temp_memory_catalog = on;
40: DO $$ BEGIN BEGIN CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a); EXCEPTION WHEN object_in_use THEN RAISE NOTICE 'refused'; END; CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a); END $$;
40: SELECT 'tc_t'::regclass::oid >= 4026531840 AS reserved;
SELECT gp_inject_fault('tempcat_release_slice', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_claim_slice_timeout', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
40q:
4: CALL tc_wait_gone();

-- And when the statement is canceled as the segment waits (here by
-- statement_timeout).
41: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
41: SET gp_enable_temp_memory_catalog = on;
41: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_release_slice', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
41q:
SELECT gp_wait_until_triggered_fault('tempcat_release_slice', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
DO $$ BEGIN FOR i IN 1..600 LOOP PERFORM pg_stat_clear_snapshot(); IF NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess s ON a.sess_id = s.id) THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'session did not end on the coordinator'; END $$;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
42: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
42: SET gp_enable_temp_memory_catalog = on;
42: SET statement_timeout = '2s';
42: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
42: RESET statement_timeout;
42: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
42: SELECT 'tc_t'::regclass::oid >= 4026531840 AS reserved;
SELECT gp_inject_fault('tempcat_release_slice', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
42q:
4: CALL tc_wait_gone();

-- And when the cancel goes through a PL/pgSQL block with an exception
-- handler of other errors, which rolls back its subtransaction first.
43: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
43: SET gp_enable_temp_memory_catalog = on;
43: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_release_slice', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
43q:
SELECT gp_wait_until_triggered_fault('tempcat_release_slice', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
DO $$ BEGIN FOR i IN 1..600 LOOP PERFORM pg_stat_clear_snapshot(); IF NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess s ON a.sess_id = s.id) THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'session did not end on the coordinator'; END $$;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
44: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
44: SET gp_enable_temp_memory_catalog = on;
44: SET statement_timeout = '2s';
44: DO $$ BEGIN CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a); EXCEPTION WHEN unique_violation THEN NULL; END $$;
44: RESET statement_timeout;
44: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
44: SELECT 'tc_t'::regclass::oid >= 4026531840 AS reserved;
SELECT gp_inject_fault('tempcat_release_slice', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 0;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
44q:
4: CALL tc_wait_gone();

-- A writer QE whose prepared transaction started to use its slice is
-- rolled back (here as PREPARE fails on segment 1): it lets go of the slice
-- once the rows of the transaction are gone, and takes it again with the
-- next OIDs, which the coordinator keeps giving from it (only a slice that
-- a segment refused is given up).
SELECT gp_inject_fault('start_prepare', 'error', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
23: SET gp_enable_temp_memory_catalog = on;
23: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('start_prepare', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = 1;
23: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
23: INSERT INTO tc_t SELECT generate_series(1, 10);
23: SELECT count(*), 'tc_t'::regclass::oid >= 4026531840 AS reserved FROM tc_t;
23q:

-- A session that cannot make its area (here as dynamic shared memory runs
-- short on the coordinator) keeps its temporary objects on disk, and lets
-- the slice it took go: the next session (made to try it) takes the slice
-- after the one taken before.
CREATE TABLE tc_slices (session int, slice bigint) DISTRIBUTED REPLICATED;
24: SET gp_enable_temp_memory_catalog = on;
24: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
24: INSERT INTO tc_slices SELECT 24, ('tc_t'::regclass::oid::bigint - 4026531840) / 65536;
SELECT gp_inject_fault_infinite('tempcat_dsm_space_low', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
25: SET gp_enable_temp_memory_catalog = on;
25: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
25: SELECT 'tc_t'::regclass::oid >= 4026531840 AS reserved;
SELECT gp_inject_fault('tempcat_dsm_space_low', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
26: SET gp_enable_temp_memory_catalog = on;
26: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
26: INSERT INTO tc_slices SELECT 26, ('tc_t'::regclass::oid::bigint - 4026531840) / 65536;
SELECT gp_inject_fault('tempcat_claim_slice_again', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT (max(slice) FILTER (WHERE session = 26) - max(slice) FILTER (WHERE session = 24) + 4096) % 4096
       AS slices_apart FROM tc_slices;
24q:
25q:
26q:
DROP TABLE tc_slices;

-- DROP of what a temporary object of another session depends on fails
-- also while that session is dropping the object (here in a transaction in
-- progress), and works once the object is gone: the session counts itself
-- out when it goes idle, before it reports the end of the transaction.
CREATE TYPE tc_wait_comp AS (x int);
27: SET gp_enable_temp_memory_catalog = on;
27: CREATE TEMP TABLE tc_w (c tc_wait_comp) DISTRIBUTED RANDOMLY;
27: BEGIN;
27: DROP TABLE tc_w;
28: DROP TYPE tc_wait_comp;
27: COMMIT;
28: DROP TYPE tc_wait_comp;
27q:
28q:

-- A transaction that made temporary objects rolls back: their rows are
-- freed only when the session goes idle, also in the aborted transaction,
-- or starts its next one (here freeing them at idle fails, as the fault
-- makes it).  Until then their xids keep
-- datfrozenxid, and so clog truncation, from passing them.
CREATE DATABASE tc_frozen;
30:@db_name tc_frozen: VACUUM FREEZE;
29:@db_name tc_frozen: SET gp_enable_temp_memory_catalog = on;
SELECT gp_inject_fault_infinite('tempcat_resolve_pending', 'error', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
29: BEGIN;
29: CREATE TEMP TABLE tc_f (a int) DISTRIBUTED BY (a);
29: SELECT 1/0;
30: CREATE TABLE tc_f_burn (a int) DISTRIBUTED BY (a);
30: DO $$ BEGIN FOR i IN 1..100 LOOP BEGIN COMMENT ON TABLE tc_f_burn IS 'x'; EXCEPTION WHEN others THEN NULL; END; END LOOP; END $$;
30: VACUUM FREEZE;
30: SELECT age(datfrozenxid) >= 100 AS held FROM pg_database WHERE datname = 'tc_frozen';
SELECT gp_inject_fault('tempcat_resolve_pending', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
29: ROLLBACK;
30: VACUUM FREEZE;
30: SELECT age(datfrozenxid) < 100 AS held_no_more FROM pg_database WHERE datname = 'tc_frozen';
-- Without the fault, idle in the aborted transaction frees them already.
29: BEGIN;
29: CREATE TEMP TABLE tc_f (a int) DISTRIBUTED BY (a);
29: SELECT 1/0;
30: DO $$ BEGIN FOR i IN 1..100 LOOP BEGIN COMMENT ON TABLE tc_f_burn IS 'x'; EXCEPTION WHEN others THEN NULL; END; END LOOP; END $$;
30: VACUUM FREEZE;
30: SELECT age(datfrozenxid) < 100 AS not_held FROM pg_database WHERE datname = 'tc_frozen';
29: ROLLBACK;
29q:
30q:
DROP DATABASE tc_frozen;

-- A session whose wait for a DROP in progress of an object it uses was
-- canceled in a subtransaction that rolled back waits for it again at the
-- next use in the transaction (here for a DROP in progress, of a session that used the
-- object itself).
CREATE TYPE tc_sub_comp AS (x int);
32: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
32: SET gp_enable_temp_memory_catalog = on;
32: CREATE TEMP TABLE tc_sx (c tc_sub_comp) DISTRIBUTED RANDOMLY;
32: BEGIN;
32: DROP TYPE tc_sub_comp CASCADE;
33: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
33: SET gp_enable_temp_memory_catalog = on;
33: SET lock_timeout = '500ms';
33: BEGIN;
33: SAVEPOINT s1;
33: CREATE TEMP TABLE tc_sa (c tc_sub_comp) DISTRIBUTED RANDOMLY;
33: ROLLBACK TO SAVEPOINT s1;
33: CREATE TEMP TABLE tc_sb (c tc_sub_comp) DISTRIBUTED RANDOMLY;
33: ROLLBACK;
32: ROLLBACK;
33: CREATE TEMP TABLE tc_sb (c tc_sub_comp) DISTRIBUTED RANDOMLY;
33q:
32q:
4: CALL tc_wait_gone();
DROP TYPE tc_sub_comp;

-- While gpexpand runs (here its catalog lock is taken as it does), a
-- session cannot change its temporary objects kept in memory, as on disk:
-- its gangs and the numsegments of its new tables are those of the cluster
-- before.
34: SET gp_enable_temp_memory_catalog = on;
34: CREATE TEMP TABLE tc_exp (a int) DISTRIBUTED BY (a);
35: BEGIN;
35: SELECT gp_expand_lock_catalog();
34: CREATE TEMP TABLE tc_exp2 (a int) DISTRIBUTED BY (a);
34: DROP TABLE tc_exp;
35: END;
34: CREATE TEMP TABLE tc_exp2 (a int) DISTRIBUTED BY (a);
34: DROP TABLE tc_exp;
34q:
35q:

-- When the shared table counting the objects that sessions use is full (the
-- fault pretends so), a session locks them instead: DROP fails the same.
CREATE TYPE tc_full_comp AS (x int);
36: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
36: SET gp_enable_temp_memory_catalog = on;
SELECT gp_inject_fault_infinite('tempcat_used_objects_full', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
36: CREATE TEMP TABLE tc_full_t (c tc_full_comp) DISTRIBUTED RANDOMLY;
SELECT gp_inject_fault('tempcat_used_objects_full', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
37: SELECT count(*) FROM pg_locks WHERE locktype = 'object' AND objid = 'tc_full_comp'::regtype AND mode = 'ShareLock';
37: DROP TYPE tc_full_comp;
36: DROP TABLE tc_full_t;
37: DROP TYPE tc_full_comp;
36q:
37q:

-- A session that goes away in the middle of the transaction that made its
-- temporary schema, with changes of catalogs on disk in it too, ends
-- cleanly (the statistics are sent once the transaction is rolled back).
38: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
38: SET gp_enable_temp_memory_catalog = on;
38: BEGIN;
38: CREATE TEMP TABLE tc_gone (a int) DISTRIBUTED BY (a);
38: CREATE TABLE tc_gone_ord (a int) DISTRIBUTED BY (a);
38q:
4: CALL tc_wait_gone();
4: SELECT to_regclass('tc_gone_ord') IS NULL AS rolled_back;

-- CREATE DATABASE copies the template's pg_shdepend rows, but not those of
-- temporary objects kept in memory of the session connected to it, whose
-- objects do not exist in the new database.
CREATE DATABASE tc_tmpl;
CREATE ROLE tc_copy_role;
31:@db_name tc_tmpl: SET gp_enable_temp_memory_catalog = on;
31: CREATE TEMP TABLE tc_c (a int) DISTRIBUTED BY (a);
31: GRANT SELECT ON tc_c TO tc_copy_role;
31: CREATE DATABASE tc_copy TEMPLATE tc_tmpl;
31: SELECT count(*) FROM pg_shdepend WHERE refobjid = 'tc_copy_role'::regrole
      AND dbid = (SELECT oid FROM pg_database WHERE datname = 'tc_copy');
31: SELECT count(*) FROM gp_dist_random('pg_shdepend') WHERE refobjid = 'tc_copy_role'::regrole
      AND dbid = (SELECT oid FROM pg_database WHERE datname = 'tc_copy');
4: INSERT INTO tc_sess SELECT sess_id FROM pg_stat_activity WHERE datname = 'tc_tmpl';
31q:
4: CALL tc_wait_gone();
DROP DATABASE tc_copy;
DROP DATABASE tc_tmpl;
DROP ROLE tc_copy_role;

4: DROP TABLE tc_sess;
4: DROP PROCEDURE tc_wait_gone();
4q:
