-- Temporary objects kept in memory (catalog/tempcat.c) as other sessions
-- see them.
--
-- Their catalog rows are in their session's memory, but for dependencies on
-- ordinary objects, which stay on disk: other sessions cannot see the
-- objects, but must neither drop nor change what they depend on.

-- Sessions that quit are gone only once their processes on all nodes are.
CREATE TABLE tc_sess (id int) DISTRIBUTED REPLICATED;
CREATE PROCEDURE tc_wait_gone() LANGUAGE plpgsql AS $$ BEGIN FOR i IN 1..600 LOOP IF NOT EXISTS (SELECT 1 FROM pg_stat_activity a JOIN tc_sess s ON a.sess_id = s.id) AND NOT EXISTS (SELECT 1 FROM gp_dist_random('pg_stat_activity') a JOIN tc_sess s ON a.sess_id = s.id) THEN RETURN; END IF; COMMIT; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'session did not end'; END $$;

CREATE TYPE tc_comp AS (x int, y int);
CREATE DOMAIN tc_dom AS int;
CREATE TABLE tc_ord (a int, b int) DISTRIBUTED BY (a);
CREATE FUNCTION tc_fn(int) RETURNS int LANGUAGE sql AS 'SELECT $1';
CREATE ROLE tc_role;
GRANT ALL ON tc_ord TO tc_role;

1: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
1: SET gp_enable_temp_memory_catalog = on;
1: SET ROLE tc_role;
1: CREATE TEMP TABLE tc_t (c tc_comp, d tc_dom, e int DEFAULT tc_fn(1)) DISTRIBUTED BY (e);
1: CREATE TEMP VIEW tc_v AS SELECT a, b FROM tc_ord;
1: INSERT INTO tc_t VALUES ((1, 2), 3);
1: SELECT 'tc_t'::regclass::oid >= 4026531840 AS reserved;

-- Other sessions see neither the objects nor their schema, and describe
-- their dependencies on ordinary objects by nulls.
2: SELECT count(*) FROM pg_class WHERE relname IN ('tc_t', 'tc_v');
2: SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname IN ('tc_t', 'tc_v');
2: SELECT pg_describe_object(classid, objid, objsubid), (pg_identify_object(classid, objid, objsubid)).*
     FROM pg_depend WHERE refobjid = 'tc_comp'::regtype AND objid >= 4026531840;

-- They cannot drop what the objects depend on, not even with CASCADE.
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
-- Nor drop the role owning them.  DROP OWNED and REASSIGN OWNED leave them
-- to their session.
2: DROP ROLE tc_role;
2: REASSIGN OWNED BY tc_role TO CURRENT_USER;
1: SELECT * FROM tc_t;
1: SELECT count(*) FROM tc_v;
2: DROP OWNED BY tc_role;
2: DROP ROLE tc_role;
1: SELECT * FROM tc_t;

-- Once the session is gone, so is all of that.
1q:
2: CALL tc_wait_gone();
2: ALTER TYPE tc_comp ALTER ATTRIBUTE y TYPE text;
2: DROP TABLE tc_ord CASCADE;
2: DROP TYPE tc_comp;
2: DROP DOMAIN tc_dom;
2: DROP FUNCTION tc_fn(int);
2: DROP ROLE tc_role;
2q:

-- A session that does not drop its temporary objects (here as if it
-- crashed) leaves their on-disk dependency rows behind.  Other sessions take
-- them for leftovers: they are no reason to keep anything.
CREATE TYPE tc_comp AS (x int, y int);
CREATE ROLE tc_role;
3: SET gp_enable_temp_memory_catalog = on;
3: CREATE TEMP TABLE tc_t (c tc_comp, e int) DISTRIBUTED BY (e);
3: ALTER TABLE tc_t OWNER TO tc_role;
3: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
3: SELECT gp_inject_fault('skip_temp_relations_cleanup', 'skip', dbid)
     FROM gp_segment_configuration WHERE role = 'p';
3q:
4: SELECT gp_wait_until_triggered_fault('skip_temp_relations_cleanup', 1, dbid)
     FROM gp_segment_configuration WHERE role = 'p';
4: SELECT gp_inject_fault('skip_temp_relations_cleanup', 'reset', dbid)
     FROM gp_segment_configuration WHERE role = 'p';
4: CALL tc_wait_gone();
4: SELECT count(*) > 0 AS leftovers FROM pg_depend
    WHERE refobjid = 'tc_comp'::regtype AND objid >= 4026531840;
4: SELECT count(*) > 0 AS leftovers FROM gp_dist_random('pg_depend')
    WHERE refobjid = 'tc_comp'::regtype AND objid >= 4026531840;
4: DROP TYPE tc_comp;
4: SELECT count(*) FROM pg_depend WHERE objid >= 4026531840;
4: SELECT count(*) FROM gp_dist_random('pg_depend') WHERE objid >= 4026531840;
4: DROP ROLE tc_role;

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

-- Owner and privilege rows of roles beyond those a session publishes go to
-- disk.  Left behind by a session that is gone, they are no reason to keep
-- a role, and go with DROP ROLE rather than refer to a dropped role.
DO $$ BEGIN FOR i IN 1..18 LOOP EXECUTE format('CREATE ROLE tc_g%s', i); END LOOP; END $$;
7: SET gp_enable_temp_memory_catalog = on;
7: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
7: DO $$ BEGIN FOR i IN 1..18 LOOP EXECUTE format('GRANT SELECT ON tc_t TO tc_g%s', i); END LOOP; END $$;
7: SELECT count(*) AS on_disk FROM pg_shdepend
    WHERE refobjid IN (SELECT oid FROM pg_roles WHERE rolname LIKE 'tc_g%') AND objid >= 4026531840
      AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1;
7: INSERT INTO tc_sess VALUES (current_setting('gp_session_id')::int);
7: SELECT gp_inject_fault('skip_temp_relations_cleanup', 'skip', dbid)
     FROM gp_segment_configuration WHERE role = 'p';
7q:
8: SELECT gp_wait_until_triggered_fault('skip_temp_relations_cleanup', 1, dbid)
     FROM gp_segment_configuration WHERE role = 'p';
8: SELECT gp_inject_fault('skip_temp_relations_cleanup', 'reset', dbid)
     FROM gp_segment_configuration WHERE role = 'p';
8: CALL tc_wait_gone();
8: DO $$ BEGIN FOR i IN 1..18 LOOP EXECUTE format('DROP ROLE tc_g%s', i); END LOOP; END $$;
8: SELECT count(*) AS left_rows FROM pg_shdepend
    WHERE objid >= 4026531840 AND refobjid NOT IN (SELECT oid FROM pg_authid);
8q:

-- A role published for a row that failed to be stored is published no more
-- once the session goes idle.
CREATE ROLE tc_role;
9: SET gp_enable_temp_memory_catalog = on;
9: CREATE TEMP TABLE tc_t (a int) DISTRIBUTED BY (a);
9: SET allow_system_table_mods = on;
9: INSERT INTO pg_shdepend SELECT d.oid, 'pg_class'::regclass, 'tc_t'::regclass, 0,
     'pg_authid'::regclass, r.oid, 'a'
     FROM pg_database d, pg_roles r WHERE d.datname = current_database() AND r.rolname = 'tc_role';
10: DROP ROLE tc_role;
9q:
10q:

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

4: DROP TABLE tc_sess;
4: DROP PROCEDURE tc_wait_gone();
4q:
