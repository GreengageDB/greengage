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

-- Other sessions see neither the objects nor their schema.
2: SELECT count(*) FROM pg_class WHERE relname IN ('tc_t', 'tc_v');
2: SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname IN ('tc_t', 'tc_v');

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
4: DROP TABLE tc_sess;
4: DROP PROCEDURE tc_wait_gone();
4q:
