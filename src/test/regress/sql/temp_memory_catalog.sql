--
-- Catalog rows of temporary objects kept in memory (catalog/tempcat.c).
--
-- With gp_enable_temp_memory_catalog on when the session's temporary schema
-- is created, temporary objects get OIDs from the reserved range starting at
-- 0xF0000000 (4026531840) and their catalog rows never reach the on-disk
-- catalogs.  Catalog lookups and SQL queries on the catalogs see the
-- in-memory rows too; the developer option gp_temp_memory_catalog_disk_only
-- hides them from SQL, which the tests below use to look at the on-disk
-- catalogs.
--

-- Count on-disk catalog rows that belong to objects with reserved OIDs, on
-- the coordinator and on the segments.  Dependencies of such objects on
-- ordinary objects (a column of a user-defined type, a function in a
-- procedural language, ...) are kept on disk on purpose, to protect the
-- ordinary objects from DROP in other sessions; they are not counted.
CREATE FUNCTION tempcat_disk_rows_qd() RETURNS bigint LANGUAGE sql AS $$
  SELECT (SELECT count(*) FROM pg_class WHERE oid >= 4026531840)
       + (SELECT count(*) FROM pg_type WHERE oid >= 4026531840)
       + (SELECT count(*) FROM pg_namespace WHERE oid >= 4026531840)
       + (SELECT count(*) FROM pg_attribute WHERE attrelid >= 4026531840)
       + (SELECT count(*) FROM pg_depend WHERE refobjid >= 4026531840)
       + (SELECT count(*) FROM pg_index WHERE indrelid >= 4026531840)
       + (SELECT count(*) FROM pg_attrdef WHERE adrelid >= 4026531840)
       + (SELECT count(*) FROM pg_constraint WHERE conrelid >= 4026531840)
       + (SELECT count(*) FROM pg_statistic WHERE starelid >= 4026531840)
       + (SELECT count(*) FROM pg_description WHERE objoid >= 4026531840)
       + (SELECT count(*) FROM pg_rewrite WHERE ev_class >= 4026531840)
       + (SELECT count(*) FROM pg_sequence WHERE seqrelid >= 4026531840)
       + (SELECT count(*) FROM pg_inherits WHERE inhrelid >= 4026531840)
       + (SELECT count(*) FROM gp_distribution_policy WHERE localoid >= 4026531840)
       + (SELECT count(*) FROM pg_appendonly WHERE relid >= 4026531840)
       + (SELECT count(*) FROM gp_fastsequence WHERE objid >= 4026531840)
       + (SELECT count(*) FROM pg_attribute_encoding WHERE attrelid >= 4026531840)
       + (SELECT count(*) FROM pg_policy WHERE polrelid >= 4026531840)
       + (SELECT count(*) FROM pg_shdepend WHERE objid >= 4026531840
            AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database()))
       + (SELECT count(*) FROM pg_proc WHERE oid >= 4026531840 OR pronamespace >= 4026531840)
       + (SELECT count(*) FROM pg_aggregate WHERE aggfnoid >= 4026531840)
       + (SELECT count(*) FROM pg_operator WHERE oprnamespace >= 4026531840)
       + (SELECT count(*) FROM pg_enum WHERE enumtypid >= 4026531840)
       + (SELECT count(*) FROM pg_range WHERE rngtypid >= 4026531840)
       + (SELECT count(*) FROM pg_statistic_ext WHERE stxrelid >= 4026531840)
$$;

CREATE FUNCTION tempcat_disk_rows_segs() RETURNS bigint LANGUAGE sql AS $$
  SELECT (SELECT count(*) FROM gp_dist_random('pg_class') WHERE oid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_type') WHERE oid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_namespace') WHERE oid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_attribute') WHERE attrelid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_depend') WHERE refobjid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_index') WHERE indrelid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_attrdef') WHERE adrelid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_constraint') WHERE conrelid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_statistic') WHERE starelid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_appendonly') WHERE relid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('gp_fastsequence') WHERE objid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_attribute_encoding') WHERE attrelid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_proc') WHERE oid >= 4026531840 OR pronamespace >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_operator') WHERE oprnamespace >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_enum') WHERE enumtypid >= 4026531840)
       + (SELECT count(*) FROM gp_dist_random('pg_range') WHERE rngtypid >= 4026531840)
$$;

-- Index-only and bitmap scans cannot return in-memory rows, so the planner
-- keeps away from them for the catalogs in sessions that keep temporary
-- objects in memory, or hide other sessions' ones.  Plans made before live
-- on where the plan cache does not hold them, e.g. the plans of a SQL
-- function for the rest of the calling statement: the executor refuses them.
SET gp_enable_temp_memory_catalog = off;
SET optimizer = off;
SET enable_seqscan = off;
CREATE FUNCTION tc_count_rel(n name) RETURNS bigint LANGUAGE sql VOLATILE AS
  $$ SELECT count(*) FROM pg_class WHERE relname = n $$;
CREATE FUNCTION tc_make_early(i int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  IF i = 1 THEN
    CREATE TEMP TABLE tc_early (a int) DISTRIBUTED BY (a);
  END IF;
  RETURN i;
END $$;
SELECT i, tc_count_rel('pg_class'),
       set_config('gp_temp_memory_catalog_hide_others', 'on', false)
  FROM generate_series(1, 2) i;
RESET enable_seqscan;
RESET optimizer;

-- Plain index scans of such plans merge the in-memory rows, and keep the
-- state of that merge when a merge join marks and restores them.  The plan
-- of tc_join_attrs() is made before the setting is turned on, and its later
-- calls see the table that tc_make_early() creates in memory.
SET optimizer = off;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_material = off;
SET enable_sort = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
CREATE FUNCTION tc_join_attrs(n name) RETURNS bigint LANGUAGE sql VOLATILE AS $$
  SELECT count(*) FROM pg_attribute a JOIN pg_attribute b ON a.attrelid = b.attrelid
   WHERE a.attrelid = ANY (ARRAY[(SELECT oid FROM pg_class WHERE relname = n), 'pg_class'::regclass])
     AND b.attrelid = ANY (ARRAY[(SELECT oid FROM pg_class WHERE relname = n), 'pg_class'::regclass])
$$;
SELECT i, tc_join_attrs('tc_early'),
       set_config('gp_enable_temp_memory_catalog', 'on', false), tc_make_early(i)
  FROM generate_series(1, 3) i;
SELECT tc_join_attrs('tc_early') AS new_plan;
DROP FUNCTION tc_join_attrs(name);
\c

-- Ordinary temporary tables are not affected.
SET gp_enable_temp_memory_catalog = off;
CREATE TEMP TABLE tc_ordinary (a int) DISTRIBUTED BY (a);
SELECT 'tc_ordinary'::regclass::oid >= 4026531840 AS in_reserved_range;
SELECT count(*) > 0 AS on_disk FROM pg_class WHERE relname = 'tc_ordinary';
-- The temporary schema was created on disk, so the setting has no effect
-- for the rest of this session: changes to the existing table, and new
-- tables, keep using the on-disk catalog (including in-place updates of
-- the pg_class row, e.g. relhasindex).
SET gp_enable_temp_memory_catalog = on;
INSERT INTO tc_ordinary SELECT generate_series(1, 100);
CREATE INDEX tc_ordinary_a ON tc_ordinary (a);
ALTER TABLE tc_ordinary ADD COLUMN b int DEFAULT 5;
SELECT relhasindex, relnatts FROM pg_class WHERE relname = 'tc_ordinary';
SELECT count(*), sum(b) FROM tc_ordinary;
CREATE TEMP TABLE tc_ordinary2 (a int) DISTRIBUTED BY (a);
SELECT 'tc_ordinary2'::regclass::oid >= 4026531840 AS in_reserved_range;
DROP TABLE tc_ordinary, tc_ordinary2;

-- Start a new session: its temporary schema is created in memory.
\c
SET gp_enable_temp_memory_catalog = on;

-- With the setting on, the planner keeps away from those scans before the
-- first temporary object already: the plan of tc_count_rel(), made before
-- tc_make_early() creates one, finds it in the later calls.  (The functions
-- were created in the previous session, so that their on-disk catalog rows
-- do not show in the statistics checked below.)
SET enable_seqscan = off;
EXPLAIN (costs off) SELECT count(*) FROM pg_class WHERE relname = 'tc_early';
SELECT i, tc_count_rel('tc_early'), tc_make_early(i) FROM generate_series(1, 3) i;
RESET enable_seqscan;
DROP TABLE tc_early;
DROP FUNCTION tc_make_early(int);
DROP FUNCTION tc_count_rel(name);

--
-- Basic usage
--
CREATE TEMP TABLE tc_basic (a int, b text) DISTRIBUTED BY (a);
SELECT 'tc_basic'::regclass::oid >= 4026531840 AS in_reserved_range;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows, tempcat_disk_rows_segs() AS seg_disk_rows;
RESET gp_temp_memory_catalog_disk_only;
SELECT count(*) AS visible_to_sql FROM pg_class WHERE relname = 'tc_basic';
SET gp_temp_memory_catalog_disk_only = on;
SELECT count(*) AS on_disk FROM pg_class WHERE relname = 'tc_basic';
RESET gp_temp_memory_catalog_disk_only;
INSERT INTO tc_basic SELECT i, 'v' || i FROM generate_series(1, 100) i;
SELECT count(*), sum(a) FROM tc_basic;
-- Reader gangs attach to the writer's in-memory catalog.
SELECT count(*) FROM tc_basic x JOIN tc_basic y ON x.a = y.a + 1;
-- Cursors run on reader gangs as well.
BEGIN;
DECLARE c CURSOR FOR SELECT a, b FROM tc_basic ORDER BY a;
FETCH 2 FROM c;
CLOSE c;
COMMIT;
DO $$
DECLARE
  r record;
  n int := 0;
BEGIN
  FOR r IN SELECT * FROM tc_basic LOOP
    n := n + 1;
  END LOOP;
  RAISE NOTICE 'rows seen by FOR loop: %', n;
END $$;

-- In-memory rows are not counted as changes of the catalog tables, so they
-- do not make autovacuum process the catalogs.
BEGIN;
CREATE TEMP TABLE tc_stat (a int, b text) DISTRIBUTED BY (a);
DROP TABLE tc_stat;
SELECT pg_stat_get_xact_tuples_inserted('pg_class'::regclass) AS class_ins,
       pg_stat_get_xact_tuples_deleted('pg_class'::regclass) AS class_del,
       pg_stat_get_xact_tuples_inserted('pg_attribute'::regclass) AS attr_ins,
       pg_stat_get_xact_tuples_inserted('pg_type'::regclass) AS type_ins,
       pg_stat_get_xact_tuples_inserted('pg_depend'::regclass) AS depend_ins;
COMMIT;

-- Catalog queries planned in reader gangs (e.g. in functions run there) do
-- not use index-only or bitmap scans, which would miss the in-memory rows;
-- nor does ORCA, which leaves queries on such catalogs to the planner.
CREATE FUNCTION pg_temp.tc_att_count(r oid) RETURNS bigint LANGUAGE plpgsql VOLATILE AS
$$ BEGIN RETURN (SELECT count(*) FROM pg_attribute WHERE attrelid = r AND attnum > 0); END $$;
SET enable_seqscan = off;
SET enable_indexscan = off;
SELECT DISTINCT pg_temp.tc_att_count('tc_basic'::regclass) FROM gp_dist_random('gp_id');
RESET enable_seqscan;
RESET enable_indexscan;
SET optimizer_enable_coordinator_only_queries = on;
SET optimizer_enable_tablescan = off;
SELECT count(*) FROM pg_attribute WHERE attrelid = 'tc_basic'::regclass AND attnum > 0;
RESET optimizer_enable_coordinator_only_queries;
RESET optimizer_enable_tablescan;
DROP FUNCTION pg_temp.tc_att_count(oid);

-- The statistics of dropped temporary tables are purged while the session
-- lives: pgstat_vacuum_stat() keeps those of the session's OIDs, as it
-- cannot see which of its temporary tables still exist.
-- (Checked on the coordinator; the segments purge theirs at the writer's
-- next transaction.)
CREATE FUNCTION pg_temp.tc_wait_analyze_count(r oid, n bigint) RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM pg_stat_clear_snapshot();
    EXIT WHEN pg_stat_get_analyze_count(r) = n;
    PERFORM pg_sleep(0.1);
  END LOOP;
  RETURN pg_stat_get_analyze_count(r);
END $$;
CREATE TEMP TABLE tc_stat_purge (a int) DISTRIBUTED BY (a);
ANALYZE tc_stat_purge;
SELECT 'tc_stat_purge'::regclass::oid AS tc_stat_purge_oid \gset
SELECT pg_temp.tc_wait_analyze_count(:tc_stat_purge_oid, 1) AS analyze_count;
DROP TABLE tc_stat_purge;
-- (the purge goes with the next statistics report, at most every 500ms)
SELECT pg_sleep(0.6);
SELECT pg_temp.tc_wait_analyze_count(:tc_stat_purge_oid, 0) AS analyze_count;
DROP FUNCTION pg_temp.tc_wait_analyze_count(oid, bigint);
-- Likewise those of dropped functions.
CREATE FUNCTION pg_temp.tc_wait_function_calls(f oid, n bigint) RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
  FOR i IN 1..100 LOOP
    PERFORM pg_stat_clear_snapshot();
    EXIT WHEN pg_stat_get_function_calls(f) IS NOT DISTINCT FROM n;
    PERFORM pg_sleep(0.1);
  END LOOP;
  RETURN pg_stat_get_function_calls(f);
END $$;
SET track_functions = 'pl';
CREATE FUNCTION pg_temp.tc_stat_fn() RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN 1; END';
SELECT pg_temp.tc_stat_fn();
SELECT pg_sleep(0.6);
SELECT 'pg_temp.tc_stat_fn'::regproc::oid AS tc_stat_fn_oid \gset
SELECT pg_temp.tc_wait_function_calls(:tc_stat_fn_oid, 1) AS calls;
DROP FUNCTION pg_temp.tc_stat_fn();
SELECT pg_sleep(0.6);
SELECT pg_temp.tc_wait_function_calls(:tc_stat_fn_oid, NULL) AS calls;
RESET track_functions;
DROP FUNCTION pg_temp.tc_wait_function_calls(oid, bigint);

-- A function evaluated in a separate slice on the coordinator (entry-db
-- reader) sees the temporary table too.
CREATE FUNCTION tempcat_count_basic() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM tc_basic';
CREATE TEMP TABLE tc_entrydb (a int, n bigint) DISTRIBUTED BY (a);
INSERT INTO tc_entrydb SELECT g, s.n FROM generate_series(1, 3) g, (SELECT tempcat_count_basic() AS n) s;
SELECT a, n FROM tc_entrydb ORDER BY a;
DROP FUNCTION tempcat_count_basic();

-- A temporary table hides an ordinary table of the same name; name lookups
-- merge in-memory and on-disk rows.
CREATE TABLE tc_same (a int) DISTRIBUTED BY (a);
INSERT INTO tc_same VALUES (1);
CREATE TEMP TABLE tc_same (a int) DISTRIBUTED BY (a);
INSERT INTO tc_same VALUES (2), (3);
SELECT count(*) AS temp_rows FROM tc_same;
SELECT count(*) AS ordinary_rows FROM public.tc_same;
DROP TABLE tc_same;
SELECT count(*) AS ordinary_rows FROM tc_same;
DROP TABLE tc_same;

-- Out-of-line (TOASTed) values of a temporary table.
CREATE TEMP TABLE tc_toast (a int, t text) DISTRIBUTED BY (a);
INSERT INTO tc_toast SELECT i, repeat(md5(i::text), 10000) FROM generate_series(1, 3) i;
SELECT a, length(t) FROM tc_toast ORDER BY a;
UPDATE tc_toast SET t = t || 'x' WHERE a = 2;
SELECT a, length(t) FROM tc_toast ORDER BY a;

--
-- DDL on temporary tables
--
CREATE INDEX tc_basic_a ON tc_basic (a);
SET enable_seqscan = off;
SELECT b FROM tc_basic WHERE a = 42;
RESET enable_seqscan;
ALTER TABLE tc_basic ADD COLUMN c int DEFAULT 3;
ALTER TABLE tc_basic ALTER COLUMN c TYPE bigint;
ALTER TABLE tc_basic ADD CONSTRAINT tc_basic_chk CHECK (a > 0);
INSERT INTO tc_basic VALUES (-1, 'bad');
ALTER TABLE tc_basic SET DISTRIBUTED BY (b);
ALTER TABLE tc_basic SET WITH (reorganize = true);
CLUSTER tc_basic USING tc_basic_a;
VACUUM FULL tc_basic;
REINDEX TABLE tc_basic;
ALTER TABLE tc_basic RENAME COLUMN b TO bb;
ALTER TABLE tc_basic DROP COLUMN c;
ANALYZE tc_basic;
COMMENT ON TABLE tc_basic IS 'kept in memory';
SELECT count(*), sum(a), min(bb) FROM tc_basic;

CREATE TEMP TABLE tc_serial (id serial, v text) DISTRIBUTED BY (id);
INSERT INTO tc_serial (v) SELECT 'x' FROM generate_series(1, 10);
SELECT max(id) FROM tc_serial;
CREATE TEMP VIEW tc_view AS SELECT id FROM tc_serial WHERE id > 5;
SELECT count(*) FROM tc_view;
CREATE TEMP SEQUENCE tc_seq;
SELECT nextval('tc_seq'), nextval('tc_seq');

CREATE TEMP TABLE tc_part (a int, d date) DISTRIBUTED BY (a)
  PARTITION BY RANGE (d) (START ('2026-01-01') END ('2026-04-01') EVERY (interval '1 month'));
INSERT INTO tc_part SELECT i, '2026-01-01'::date + i FROM generate_series(0, 80) i;
ALTER TABLE tc_part ADD PARTITION START ('2026-04-01') END ('2026-05-01');
ALTER TABLE tc_part DROP PARTITION FOR ('2026-01-15');
SELECT count(*) FROM tc_part;

CREATE TEMP TABLE tc_inh_parent (a int) DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_inh_child (b int) INHERITS (tc_inh_parent) DISTRIBUTED BY (a);
INSERT INTO tc_inh_child VALUES (1, 2);
SELECT count(*) FROM tc_inh_parent;

CREATE TEMP TABLE tc_aoco (a int, b text)
  WITH (appendonly = true, orientation = column, compresstype = zlib) DISTRIBUTED BY (a);
INSERT INTO tc_aoco SELECT i, i::text FROM generate_series(1, 1000) i;
CREATE INDEX tc_aoco_a ON tc_aoco (a);
DELETE FROM tc_aoco WHERE a % 2 = 0;
UPDATE tc_aoco SET b = 'u' WHERE a < 100;
VACUUM tc_aoco;
ALTER TABLE tc_aoco ADD COLUMN c int DEFAULT 1;
SELECT count(*), sum(c) FROM tc_aoco;
SET enable_seqscan = off;
SELECT count(*) FROM tc_aoco WHERE a BETWEEN 1 AND 500;
RESET enable_seqscan;
TRUNCATE tc_aoco;
INSERT INTO tc_aoco SELECT i, 'x', 2 FROM generate_series(1, 10) i;
SELECT count(*), sum(c) FROM tc_aoco;

-- TRUNCATE of a column-oriented table followed by ADD COLUMN with a default
-- (needs the attribute encoding rows of the truncated table).
CREATE TEMP TABLE tc_aoco2 (a int, b int)
  WITH (appendonly = true, orientation = column) DISTRIBUTED BY (a);
INSERT INTO tc_aoco2 SELECT i, i FROM generate_series(1, 100) i;
TRUNCATE tc_aoco2;
INSERT INTO tc_aoco2 SELECT i, i FROM generate_series(1, 10) i;
ALTER TABLE tc_aoco2 ADD COLUMN c int DEFAULT 7;
SELECT count(*), sum(c) FROM tc_aoco2;

CREATE TEMP TABLE tc_ctas AS SELECT * FROM tc_serial DISTRIBUTED BY (id);
SELECT count(*) FROM tc_ctas;

-- Utility commands that scan the catalogs directly (heap_getnext()) see the
-- in-memory rows too.
CREATE TYPE pg_temp.tc_ct AS (a int);
CREATE TEMP TABLE tc_typed_of OF pg_temp.tc_ct DISTRIBUTED RANDOMLY;
ALTER TYPE pg_temp.tc_ct ADD ATTRIBUTE b int;
ALTER TYPE pg_temp.tc_ct ADD ATTRIBUTE b int CASCADE;
SELECT attname FROM pg_attribute WHERE attrelid = 'tc_typed_of'::regclass AND attnum > 0 ORDER BY attnum;
DROP TABLE tc_typed_of;
DROP TYPE pg_temp.tc_ct;
CREATE TEMP TABLE tc_grant (a int) DISTRIBUTED BY (a);
CREATE FUNCTION pg_temp.tc_grant_func() RETURNS int LANGUAGE sql AS 'SELECT 1';
GRANT SELECT ON ALL TABLES IN SCHEMA pg_temp TO PUBLIC;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA pg_temp FROM PUBLIC;
SELECT relacl IS NOT NULL AS table_acl FROM pg_class WHERE oid = 'tc_grant'::regclass;
SELECT proacl IS NOT NULL AS function_acl FROM pg_proc WHERE proname = 'tc_grant_func';
DROP TABLE tc_grant;
DROP FUNCTION pg_temp.tc_grant_func();

-- SQL queries on the catalogs see the in-memory rows, on the coordinator and
-- on the segments.
SELECT relname, relkind, relpersistence FROM pg_class
WHERE relname IN ('tc_basic', 'tc_basic_a', 'tc_view', 'tc_serial_id_seq') ORDER BY 1;
SELECT attname, atttypid::regtype FROM pg_attribute
WHERE attrelid = 'tc_basic'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
SELECT pg_get_viewdef('tc_view');
SELECT obj_description('tc_basic'::regclass);
SELECT table_name, table_type FROM information_schema.tables
WHERE table_name IN ('tc_basic', 'tc_view') ORDER BY 1;
SELECT count(*) > 0 AS has_stats FROM pg_stats WHERE tablename = 'tc_basic';
SELECT count(*) AS seg_rows FROM gp_dist_random('pg_class') WHERE relname = 'tc_basic';
-- TABLESAMPLE reads the heap's pages only, and refuses to miss them.
SELECT count(*) FROM pg_class TABLESAMPLE SYSTEM (100) WHERE relname = 'tc_basic';
SELECT count(*) FROM pg_class TABLESAMPLE BERNOULLI (100) WHERE relname = 'tc_basic';
-- TID scans and COPY TO of a catalog see them too.
SELECT relname FROM pg_class WHERE ctid = (SELECT ctid FROM pg_class WHERE relname = 'tc_basic');
\copy pg_class (relname) TO PROGRAM 'grep -c ^tc_basic$'
-- Plain index scans merge the in-memory rows in index order; index-only and
-- bitmap scans, which cannot return them, are not used for such catalogs.
SET enable_seqscan = off;
EXPLAIN (costs off) SELECT relname FROM pg_class
WHERE relname IN ('tc_basic', 'tc_view', 'pg_am', 'pg_class') ORDER BY relname;
SELECT relname FROM pg_class
WHERE relname IN ('tc_basic', 'tc_view', 'pg_am', 'pg_class') ORDER BY relname;
EXPLAIN (costs off) SELECT relname FROM pg_class
WHERE relname IN ('tc_basic', 'tc_view', 'pg_am', 'pg_class') ORDER BY relname DESC;
SELECT relname FROM pg_class
WHERE relname IN ('tc_basic', 'tc_view', 'pg_am', 'pg_class') ORDER BY relname DESC;
-- system columns of merged on-disk and in-memory rows
SELECT relname, tableoid::regclass FROM pg_class
WHERE relname IN ('tc_basic', 'pg_am') ORDER BY relname;
-- a parameterized inner index scan
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SELECT c.relname, a.attname FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
WHERE c.relname IN ('tc_basic', 'pg_am') AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY 1, 2;
RESET enable_hashjoin;
RESET enable_mergejoin;
SET enable_indexscan = off;
EXPLAIN (costs off) SELECT count(*) FROM pg_class WHERE relname = 'tc_basic';
RESET enable_indexscan;
RESET enable_seqscan;

-- Nothing of the above reached the on-disk catalogs.
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows, tempcat_disk_rows_segs() AS seg_disk_rows;
RESET gp_temp_memory_catalog_disk_only;

--
-- An insert into an append-only table created in the same transaction uses
-- the reserved segment file 0 (needs the real xmin of the pg_class row).
--
BEGIN;
CREATE TEMP TABLE tc_ao_segno (a int) WITH (appendonly = true) DISTRIBUTED BY (a);
INSERT INTO tc_ao_segno SELECT generate_series(1, 30);
COMMIT;
SELECT DISTINCT segno FROM gp_toolkit.__gp_aoseg('tc_ao_segno');

-- gp_fastsequence stays non-transactional for in-memory rows: rolled back
-- inserts do not let later inserts reuse their row numbers, which would make
-- index scans return wrong rows.  Cover both the segment file used in the
-- creating transaction (whose gp_fastsequence row is created in memory) and
-- later ones.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
BEGIN;
CREATE TEMP TABLE tc_ao_seq (a int, b int) WITH (appendonly = true) DISTRIBUTED BY (b);
CREATE INDEX tc_ao_seq_a ON tc_ao_seq (a);
INSERT INTO tc_ao_seq SELECT i, 1 FROM generate_series(1, 10) i;
SAVEPOINT s;
INSERT INTO tc_ao_seq SELECT i, 1 FROM generate_series(100, 199) i;
ROLLBACK TO SAVEPOINT s;
INSERT INTO tc_ao_seq SELECT i, 1 FROM generate_series(1000, 1099) i;
COMMIT;
BEGIN;
INSERT INTO tc_ao_seq SELECT i, 1 FROM generate_series(200, 299) i;
ROLLBACK;
INSERT INTO tc_ao_seq SELECT i, 1 FROM generate_series(2000, 2099) i;
BEGIN;
SAVEPOINT s;
INSERT INTO tc_ao_seq SELECT i, 1 FROM generate_series(300, 399) i;
ROLLBACK TO SAVEPOINT s;
INSERT INTO tc_ao_seq SELECT i, 1 FROM generate_series(3000, 3099) i;
COMMIT;
SELECT count(*) AS rolled_back_rows FROM tc_ao_seq WHERE a BETWEEN 100 AND 399;
SELECT count(*) AS committed_rows FROM tc_ao_seq WHERE a BETWEEN 1000 AND 3099;
RESET enable_seqscan;
RESET enable_bitmapscan;
SET enable_indexscan = off;
SELECT count(*) AS all_rows FROM tc_ao_seq;
RESET enable_indexscan;

--
-- Transactions
--
BEGIN;
CREATE TEMP TABLE tc_rollback (a int) DISTRIBUTED BY (a);
INSERT INTO tc_rollback VALUES (1);
ROLLBACK;
SELECT to_regclass('tc_rollback') IS NULL AS rolled_back;
CREATE TEMP TABLE tc_rollback (a int) DISTRIBUTED BY (a);
DROP TABLE tc_rollback;

BEGIN;
DROP TABLE tc_serial CASCADE;
ROLLBACK;
SELECT count(*) FROM tc_serial;
SELECT count(*) FROM tc_view;

BEGIN;
CREATE TEMP TABLE tc_sp1 (a int) DISTRIBUTED BY (a);
SAVEPOINT s1;
CREATE TEMP TABLE tc_sp2 (a int) DISTRIBUTED BY (a);
ALTER TABLE tc_sp1 ADD COLUMN b int;
ROLLBACK TO SAVEPOINT s1;
SAVEPOINT s2;
CREATE TEMP TABLE tc_sp3 (a int) DISTRIBUTED BY (a);
RELEASE SAVEPOINT s2;
COMMIT;
SELECT to_regclass('tc_sp1') IS NOT NULL AS sp1, to_regclass('tc_sp2') IS NOT NULL AS sp2,
       to_regclass('tc_sp3') IS NOT NULL AS sp3;
SELECT * FROM tc_sp1;
CREATE TEMP TABLE tc_sp2 (a int) DISTRIBUTED BY (a);

DO $$
BEGIN
  FOR i IN 1..5 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE tc_exc_%s (a int) DISTRIBUTED BY (a)', i);
      IF i % 2 = 0 THEN
        RAISE EXCEPTION 'undo %', i;
      END IF;
    EXCEPTION WHEN raise_exception THEN
      NULL;
    END;
  END LOOP;
END $$;
SELECT g, to_regclass('tc_exc_' || g) IS NOT NULL AS exists
FROM generate_series(1, 5) g ORDER BY g;

-- A failing CREATE TABLE AS leaves nothing behind.
CREATE TEMP TABLE tc_ctas_fail AS SELECT 1 / (i - 5) AS x FROM generate_series(1, 10) i DISTRIBUTED BY (x);
SELECT to_regclass('tc_ctas_fail') IS NULL AS not_created;
CREATE TEMP TABLE tc_ctas_fail AS SELECT i AS x FROM generate_series(1, 10) i DISTRIBUTED BY (x);
SELECT count(*) FROM tc_ctas_fail;

-- Changes of in-memory rows on the segments are committed in two phases
-- even though they write no WAL: a deferred unique check that fails at
-- commit on one segment must not leave the new table on the others.
CREATE TEMP TABLE tc_defer (i int UNIQUE DEFERRABLE INITIALLY DEFERRED) DISTRIBUTED BY (i);
INSERT INTO tc_defer VALUES (1);
BEGIN;
CREATE TEMP TABLE tc_defer_new (a int) DISTRIBUTED BY (a);
INSERT INTO tc_defer VALUES (1);
COMMIT;
SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname = 'tc_defer_new';
CREATE TEMP TABLE tc_defer_new (a int) DISTRIBUTED BY (a);
DROP TABLE tc_defer_new, tc_defer;

BEGIN;
CREATE TEMP TABLE tc_oc_drop (a int) ON COMMIT DROP DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_oc_delete (a int) ON COMMIT DELETE ROWS DISTRIBUTED BY (a);
INSERT INTO tc_oc_delete VALUES (1);
COMMIT;
SELECT to_regclass('tc_oc_drop') IS NULL AS dropped, (SELECT count(*) FROM tc_oc_delete) AS rows;

PREPARE tc_prep AS SELECT count(*) FROM tc_basic;
EXECUTE tc_prep;
ALTER TABLE tc_basic ADD COLUMN d int;
EXECUTE tc_prep;
DEALLOCATE tc_prep;

--
-- Turning the setting off does not affect the session's temporary objects:
-- they stay in memory, and new ones join them.
--
SET gp_enable_temp_memory_catalog = off;
SELECT count(*) FROM tc_basic x JOIN tc_basic y ON x.a = y.a + 1;
CREATE TEMP TABLE tc_after_off (a int) DISTRIBUTED BY (a);
SELECT 'tc_after_off'::regclass::oid >= 4026531840 AS in_reserved_range;
SET gp_enable_temp_memory_catalog = on;

SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows, tempcat_disk_rows_segs() AS seg_disk_rows;
RESET gp_temp_memory_catalog_disk_only;

DISCARD TEMP;
SELECT to_regclass('tc_basic') IS NULL AS discarded;
CREATE TEMP TABLE tc_after_discard (a int) DISTRIBUTED BY (a);
INSERT INTO tc_after_discard VALUES (1);
SELECT count(*) FROM tc_after_discard;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows, tempcat_disk_rows_segs() AS seg_disk_rows;
RESET gp_temp_memory_catalog_disk_only;

--
-- Functions, aggregates, operators, and enum, range and domain types can be
-- created in a temporary schema kept in memory, and statistics objects on
-- temporary tables; their catalog rows stay in memory too, also on the
-- segments.  Other objects (operator classes, collations, text search
-- objects, ...) and casts or transforms of such types are not supported:
-- their on-disk rows would point at objects other sessions cannot see.
--
CREATE FUNCTION pg_temp.tc_func(int) RETURNS int IMMUTABLE LANGUAGE sql AS 'SELECT $1 * 10';
SELECT pg_temp.tc_func(2);
CREATE TEMP TABLE tc_fn_data (a int, b int) DISTRIBUTED BY (a);
INSERT INTO tc_fn_data SELECT i, i % 5 FROM generate_series(1, 100) i;
-- executed on the segments, also by reader gangs
SELECT sum(pg_temp.tc_func(a)) FROM tc_fn_data;
SELECT count(*) FROM tc_fn_data x JOIN tc_fn_data y ON pg_temp.tc_func(x.b) = y.a;
CREATE FUNCTION pg_temp.tc_plfunc(n int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN n + 1; END $$;
SELECT pg_temp.tc_plfunc(41);
CREATE AGGREGATE pg_temp.tc_sum(int) (sfunc = int4pl, stype = int, initcond = '0');
SELECT pg_temp.tc_sum(a) FROM tc_fn_data;
CREATE OPERATOR pg_temp.=== (PROCEDURE = int4eq, LEFTARG = int, RIGHTARG = int);
SELECT 1 OPERATOR(pg_temp.===) 1 AS eq, 1 OPERATOR(pg_temp.===) 2 AS ne;
-- Commutators and negators among them are fine, but not with ordinary
-- operators: their rows would point at each other without a dependency.
CREATE OPERATOR pg_temp.<<< (PROCEDURE = int4lt, LEFTARG = int, RIGHTARG = int,
                             COMMUTATOR = OPERATOR(pg_temp.>>>),
                             NEGATOR = OPERATOR(pg_temp.>>=));
SELECT o.oprname, c.oprname AS commutator, n.oprname AS negator
  FROM pg_operator o LEFT JOIN pg_operator c ON c.oid = o.oprcom
       LEFT JOIN pg_operator n ON n.oid = o.oprnegate
 WHERE o.oprnamespace = pg_my_temp_schema() AND o.oprname LIKE '%>%' ORDER BY 1;
CREATE OPERATOR public.<<<< (PROCEDURE = int4lt, LEFTARG = int, RIGHTARG = int);
CREATE OPERATOR pg_temp.>>>> (PROCEDURE = int4gt, LEFTARG = int, RIGHTARG = int,
                              COMMUTATOR = OPERATOR(public.<<<<));
CREATE OPERATOR pg_temp.>>>> (PROCEDURE = int4gt, LEFTARG = int, RIGHTARG = int,
                              NEGATOR = OPERATOR(public.!>>>));
CREATE OPERATOR public.!<<< (PROCEDURE = int4ge, LEFTARG = int, RIGHTARG = int,
                             NEGATOR = OPERATOR(pg_temp.<<<));
SELECT oprcom, oprnegate FROM pg_operator WHERE oid = 'public.<<<<(int, int)'::regoperator;
SELECT count(*) AS shells FROM pg_operator WHERE oprname IN ('!>>>', '!<<<');
DROP OPERATOR public.<<<< (int, int);
-- enum labels are also read by ordered (forward and backward) catalog scans
CREATE TYPE pg_temp.tc_mood AS ENUM ('sad', 'ok', 'happy');
ALTER TYPE pg_temp.tc_mood ADD VALUE 'meh' AFTER 'sad';
CREATE TEMP TABLE tc_moods (m pg_temp.tc_mood) DISTRIBUTED RANDOMLY;
INSERT INTO tc_moods VALUES ('happy'), ('sad'), ('meh'), ('ok');
SELECT m FROM tc_moods ORDER BY m;
SELECT enum_first(NULL::pg_temp.tc_mood), enum_last(NULL::pg_temp.tc_mood),
       enum_range(NULL::pg_temp.tc_mood);
-- Their labels get reserved OIDs too, the same on the segments.
SELECT count(*) AS labels, bool_and(oid >= 4026531840) AS reserved
  FROM pg_enum WHERE enumtypid = 'pg_temp.tc_mood'::regtype;
SELECT count(*) AS seg_labels_differing FROM gp_dist_random('pg_enum') s
 WHERE s.enumtypid = 'pg_temp.tc_mood'::regtype
   AND s.oid NOT IN (SELECT oid FROM pg_enum WHERE enumtypid = 'pg_temp.tc_mood'::regtype);
SELECT count(*) AS seg_labels FROM gp_dist_random('pg_enum')
 WHERE enumtypid = 'pg_temp.tc_mood'::regtype;
CREATE TYPE pg_temp.tc_range AS RANGE (subtype = int4);
SELECT pg_temp.tc_range(1, 5) @> 3 AS contains;
CREATE STATISTICS pg_temp.tc_stx (dependencies, ndistinct) ON a, b FROM tc_fn_data;
ANALYZE tc_fn_data;
SELECT stxname, stxkind FROM pg_statistic_ext WHERE stxrelid = 'tc_fn_data'::regclass;
SELECT d.stxdndistinct IS NOT NULL AS has_ndistinct
FROM pg_statistic_ext_data d JOIN pg_statistic_ext s ON s.oid = d.stxoid
WHERE s.stxname = 'tc_stx';
-- not supported
CREATE STATISTICS tc_stx_public ON a, b FROM tc_fn_data;
CREATE COLLATION pg_temp.tc_coll FROM "C";
CREATE CAST (pg_temp.tc_mood AS int) WITH INOUT;
-- nor ordinary objects that depend on temporary objects kept in memory
CREATE TEMP SEQUENCE tc_dep_seq;
CREATE FUNCTION pg_temp.tc_trgfunc() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE tc_perm_col (a int, c tc_fn_data) DISTRIBUTED BY (a);
CREATE TABLE tc_perm_def (a int DEFAULT nextval('tc_dep_seq')) DISTRIBUTED BY (a);
CREATE FUNCTION public.tc_perm_func(pg_temp.tc_mood) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE TABLE tc_perm_trg (a int) DISTRIBUTED BY (a);
-- (the detail names the session's temporary schema)
\set VERBOSITY terse
CREATE TRIGGER tc_trg BEFORE INSERT ON tc_perm_trg FOR EACH ROW EXECUTE FUNCTION pg_temp.tc_trgfunc();
\set VERBOSITY default
DROP TABLE tc_perm_trg;
DROP SEQUENCE tc_dep_seq;
-- nor temporary tables inheriting from ordinary ones, which other sessions
-- could not see among the ordinary table's children
CREATE TABLE tc_perm_parent (a int) DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_inh_perm () INHERITS (tc_perm_parent) DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_inh_perm (a int) DISTRIBUTED BY (a);
ALTER TABLE tc_inh_perm INHERIT tc_perm_parent;
DROP TABLE tc_inh_perm, tc_perm_parent;
-- nor typed tables of ordinary composite types, which other sessions could
-- not see among the type's typed tables (those of temporary types are fine)
CREATE TYPE tc_perm_comp AS (x int);
CREATE TEMP TABLE tc_typed OF tc_perm_comp DISTRIBUTED BY (x);
CREATE TEMP TABLE tc_typed (x int) DISTRIBUTED BY (x);
ALTER TABLE tc_typed OF tc_perm_comp;
CREATE TYPE pg_temp.tc_temp_comp AS (x int);
ALTER TABLE tc_typed OF pg_temp.tc_temp_comp;
CREATE TEMP TABLE tc_typed2 OF pg_temp.tc_temp_comp DISTRIBUTED BY (x);
DROP TABLE tc_typed, tc_typed2;
DROP TYPE pg_temp.tc_temp_comp;
DROP TYPE tc_perm_comp;
CREATE TEMP TABLE tc_rls (a int, b int) DISTRIBUTED BY (a);
-- Foreign (external) tables, security labels, type encodings and the DDL
-- tracking rows (pg_stat_last_operation) of temporary objects are kept in
-- memory too.  (Security labels need a label provider: dummy_seclabel, from
-- src/test/modules, which not every installation has; without it, that part
-- is skipped.)
CREATE TEMP TABLE tc_labeled (a int) DISTRIBUTED BY (a);
DO $$
BEGIN
  EXECUTE 'LOAD ''dummy_seclabel''';
  SECURITY LABEL ON TABLE tc_labeled IS 'classified';
  IF (SELECT label FROM pg_seclabel WHERE objoid = 'tc_labeled'::regclass)
     IS DISTINCT FROM 'classified' THEN
    RAISE EXCEPTION 'security label of an in-memory table not found';
  END IF;
EXCEPTION WHEN undefined_file THEN
  NULL;
END $$;
TRUNCATE tc_labeled;
CREATE POLICY tc_labeled_pol ON tc_labeled USING (a > 0);
CREATE DOMAIN pg_temp.tc_enc_dom AS int;
ALTER TYPE pg_temp.tc_enc_dom SET DEFAULT ENCODING (compresstype = zlib);
CREATE EXTERNAL WEB TEMP TABLE tc_ext (a int) EXECUTE 'echo 1' FORMAT 'text';
SELECT count(*) AS encodings FROM pg_type_encoding WHERE typid = 'pg_temp.tc_enc_dom'::regtype;
SELECT count(*) AS ext_rows FROM tc_ext;
SELECT classid::regclass, staactionname FROM pg_stat_last_operation
WHERE objid IN ('tc_labeled'::regclass, (SELECT oid FROM pg_policy WHERE polname = 'tc_labeled_pol')) ORDER BY 1, 2;
SET gp_temp_memory_catalog_disk_only = on;
SELECT (SELECT count(*) FROM pg_seclabel WHERE objoid >= 4026531840)
     + (SELECT count(*) FROM pg_type_encoding WHERE typid >= 4026531840)
     + (SELECT count(*) FROM pg_foreign_table WHERE ftrelid >= 4026531840)
     + (SELECT count(*) FROM gp_dist_random('pg_foreign_table') WHERE ftrelid >= 4026531840)
     + (SELECT count(*) FROM pg_stat_last_operation WHERE objid >= 4026531840) AS on_disk;
RESET gp_temp_memory_catalog_disk_only;
DROP TABLE tc_labeled;
DROP EXTERNAL TABLE tc_ext;
DROP DOMAIN pg_temp.tc_enc_dom;
-- Domains, composite types and row-level security policies are supported.
CREATE DOMAIN pg_temp.tc_dom AS int CHECK (VALUE > 0);
CREATE TYPE pg_temp.tc_comp AS (x int, y text);
CREATE TEMP TABLE tc_typed (d pg_temp.tc_dom, c pg_temp.tc_comp) DISTRIBUTED RANDOMLY;
INSERT INTO tc_typed VALUES (1, ROW(1, 'a'));
INSERT INTO tc_typed VALUES (-1, ROW(2, 'b'));
SELECT d, (c).y FROM tc_typed;
INSERT INTO tc_rls SELECT i, i FROM generate_series(1, 10) i;
CREATE POLICY tc_rls_pol ON tc_rls USING (a > 5);
ALTER TABLE tc_rls ENABLE ROW LEVEL SECURITY;
CREATE ROLE tc_rls_user;
GRANT SELECT ON tc_rls TO tc_rls_user;
SET ROLE tc_rls_user;
SELECT count(*) AS visible_rows FROM tc_rls;
RESET ROLE;
SELECT count(*) AS all_rows FROM tc_rls;
DROP POLICY tc_rls_pol ON tc_rls;
SET ROLE tc_rls_user;
SELECT count(*) AS visible_rows FROM tc_rls;
RESET ROLE;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows, tempcat_disk_rows_segs() AS seg_disk_rows;
RESET gp_temp_memory_catalog_disk_only;
DROP TABLE tc_rls;
DROP ROLE tc_rls_user;

-- SQL DML on catalog rows kept in memory (allow_system_table_mods): they
-- have no place in the heap, so updates, deletes and row locks must not
-- touch it (the gangsize test changes numsegments this way).
SET allow_system_table_mods = on;
CREATE TEMP TABLE tc_dml (a int, b int) DISTRIBUTED BY (a);
UPDATE gp_distribution_policy SET numsegments = 2 WHERE localoid = 'tc_dml'::regclass;
UPDATE gp_distribution_policy SET numsegments = 3 WHERE localoid = 'tc_dml'::regclass;
UPDATE gp_distribution_policy SET numsegments = 2 WHERE localoid = 'tc_dml'::regclass;
SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
INSERT INTO tc_dml SELECT i, i FROM generate_series(1, 10) i;
SELECT count(*) FROM tc_dml;
BEGIN;
SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass FOR UPDATE;
DELETE FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
SELECT count(*) FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
ROLLBACK;
-- A command that meets a row twice changes it once, as on disk.
BEGIN;
DELETE FROM gp_distribution_policy p USING generate_series(1, 2) g WHERE p.localoid = 'tc_dml'::regclass;
SELECT count(*) FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
ROLLBACK;
BEGIN;
UPDATE gp_distribution_policy p SET numsegments = 3 FROM generate_series(1, 2) g WHERE p.localoid = 'tc_dml'::regclass;
SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
ROLLBACK;
-- DELETE ... RETURNING fetches the old row by its TID
BEGIN;
DELETE FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass RETURNING numsegments;
ROLLBACK;
SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
SET gp_temp_memory_catalog_disk_only = on;
SELECT count(*) AS on_disk FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
RESET gp_temp_memory_catalog_disk_only;
-- When the area cannot take the new version of a row (the fault makes an
-- allocation fail), the row moves to the on-disk catalog: through SQL, and
-- through internal catalog updates (VACUUM FULL swaps pg_class rows).
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
RESET client_min_messages;
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'skip', '', '', '', 1, 1, 0, dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
UPDATE gp_distribution_policy SET numsegments = 3 WHERE localoid = 'tc_dml'::regclass;
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
SET gp_temp_memory_catalog_disk_only = on;
SELECT count(*) AS on_disk FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
RESET gp_temp_memory_catalog_disk_only;
SELECT gp_inject_fault_infinite('tempcat_hash_node_alloc', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
VACUUM FULL tc_dml;
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT count(*), sum(b) FROM tc_dml;
SET gp_temp_memory_catalog_disk_only = on;
SELECT count(*) AS on_disk FROM pg_class WHERE oid = 'tc_dml'::regclass;
RESET gp_temp_memory_catalog_disk_only;
RESET allow_system_table_mods;
DROP TABLE tc_dml;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows;
RESET gp_temp_memory_catalog_disk_only;

-- An allocation failing in the middle of adding a row (here the second
-- one) leaves nothing behind in memory; the row goes to disk.
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'skip', '', '', '', 2, 2, 0, dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TEMP TABLE tc_oom (a int, b text) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
INSERT INTO tc_oom SELECT i, 'x' FROM generate_series(1, 10) i;
SELECT count(*) FROM tc_oom;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() > 0 AS some_on_disk;
RESET gp_temp_memory_catalog_disk_only;
DROP TABLE tc_oom;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows;
RESET gp_temp_memory_catalog_disk_only;

-- An error while the outcome of earlier transactions is applied to the rows
-- (here after the first dropped row is freed, at the start of the next
-- transaction) leaves the rows still to be done consistent: the next
-- transaction finishes them.
CREATE TEMP TABLE tc_pend (a int, b text) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_forget_if_gone', 'error', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
DROP TABLE tc_pend;
SELECT 1 AS fails_to_start;
SELECT gp_inject_fault('tempcat_forget_if_gone', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT to_regclass('tc_pend') IS NULL AS dropped;
CREATE TEMP TABLE tc_pend (a int, b text) DISTRIBUTED BY (a);
INSERT INTO tc_pend VALUES (1, 'x');
SELECT * FROM tc_pend;
DROP TABLE tc_pend;

-- A sequential scan that stops (LIMIT) or restarts (a correlated subquery)
-- while returning in-memory rows.
CREATE TEMP TABLE tc_seq1 (a int) DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_seq2 (a int) DISTRIBUTED BY (a);
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM (SELECT relname FROM pg_class WHERE relname IN ('tc_seq1', 'tc_seq2') LIMIT 1) s;
SELECT x, (SELECT count(*) FROM (SELECT 1 FROM pg_class WHERE relname IN ('tc_seq1', 'tc_seq2') AND x > 0 LIMIT 1) s) AS found
FROM generate_series(1, 3) x ORDER BY x;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE tc_seq1, tc_seq2;

-- Casts and transforms of temporary types (target types too) are refused.
CREATE TYPE pg_temp.tc_cast_t AS (x int);
CREATE CAST (text AS pg_temp.tc_cast_t) WITH INOUT;
CREATE CAST (pg_temp.tc_cast_t AS text) WITH INOUT;
CREATE TRANSFORM FOR pg_temp.tc_cast_t LANGUAGE plpgsql (FROM SQL WITH FUNCTION abs(int));
DROP TYPE pg_temp.tc_cast_t;

--
-- Rows that do not fit into gp_temp_memory_catalog_max_size go to the
-- on-disk catalog.  The limit applies when the session's in-memory catalog
-- is created, so start a new session.
--
\c
SET gp_enable_temp_memory_catalog = on;
SET gp_temp_memory_catalog_max_size = '1MB';

-- Rows that the transaction inserted and deleted are freed early when the
-- area is full, so a transaction creating and dropping many tables does not
-- overflow to disk: neither at top level, nor when each iteration runs in a
-- subtransaction.  The transaction's own statistics count on-disk inserts.
BEGIN;
DO $$
BEGIN
  FOR i IN 1..400 LOOP
    EXECUTE format('CREATE TEMP TABLE tc_loop_%s (a int, b text) DISTRIBUTED BY (a)', i);
    EXECUTE format('INSERT INTO tc_loop_%s VALUES (%s, ''x'')', i, i);
    EXECUTE format('DROP TABLE tc_loop_%s', i);
  END LOOP;
END $$;
DO $$
BEGIN
  FOR i IN 1..150 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE tc_subloop_%s (a int, b text) DISTRIBUTED BY (a)', i);
      EXECUTE format('DROP TABLE tc_subloop_%s', i);
      IF i % 3 = 0 THEN
        RAISE EXCEPTION 'undo %', i;
      END IF;
    EXCEPTION WHEN raise_exception THEN
      NULL;
    END;
  END LOOP;
END $$;
SELECT pg_stat_get_xact_tuples_inserted('pg_class'::regclass) AS class_disk_ins,
       pg_stat_get_xact_tuples_inserted('pg_attribute'::regclass) AS attr_disk_ins;
-- A deletion that can still be rolled back keeps its row.
CREATE TEMP TABLE tc_keep (a int) DISTRIBUTED BY (a);
INSERT INTO tc_keep VALUES (42);
DO $$
BEGIN
  BEGIN
    DROP TABLE tc_keep;
    FOR i IN 1..150 LOOP
      EXECUTE format('CREATE TEMP TABLE tc_fill_%s (a int) DISTRIBUTED BY (a)', i);
      EXECUTE format('DROP TABLE tc_fill_%s', i);
    END LOOP;
    RAISE EXCEPTION 'undo the drop';
  EXCEPTION WHEN raise_exception THEN
    NULL;
  END;
END $$;
SELECT sum(a) FROM tc_keep;
COMMIT;
SELECT sum(a) FROM tc_keep;
DROP TABLE tc_keep;

-- Rows of live tables that do not fit go to disk.
DO $$
BEGIN
  FOR i IN 1..300 LOOP
    EXECUTE format('CREATE TEMP TABLE tc_spill_%s (a int, b text, c int) DISTRIBUTED BY (a)', i);
    EXECUTE format('INSERT INTO tc_spill_%s VALUES (%s, ''x'', 1)', i, i);
  END LOOP;
END $$;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() > 0 AS spilled_to_disk;
RESET gp_temp_memory_catalog_disk_only;
SELECT sum(a) FROM tc_spill_1;
SELECT sum(a) FROM tc_spill_300;
SELECT count(*) FROM tc_spill_10 x JOIN tc_spill_299 y ON x.c = y.c;
ALTER TABLE tc_spill_300 ADD COLUMN d int DEFAULT 5;
SELECT sum(d) FROM tc_spill_300;
DO $$
BEGIN
  FOR i IN 1..300 LOOP
    EXECUTE format('DROP TABLE tc_spill_%s', i);
  END LOOP;
END $$;
SET gp_temp_memory_catalog_disk_only = on;
SELECT tempcat_disk_rows_qd() AS qd_disk_rows, tempcat_disk_rows_segs() AS seg_disk_rows;
RESET gp_temp_memory_catalog_disk_only;

\c
DROP FUNCTION tempcat_disk_rows_qd();
DROP FUNCTION tempcat_disk_rows_segs();

-- Only superusers can change the size limit of the in-memory catalog: its
-- memory is shared memory of the node, if charged to the session's vmem.
CREATE ROLE tc_guc_user;
SET ROLE tc_guc_user;
SET gp_temp_memory_catalog_max_size = 1048576;
RESET ROLE;
DROP ROLE tc_guc_user;

-- When ordinary objects take every OID of the session's slice of the
-- reserved range (the fault pretends so), a new session keeps its temporary
-- objects on disk, with a warning.  (isolation2 temp_memory_catalog covers
-- the rest; its output has no warnings.)
\c
SET gp_enable_temp_memory_catalog = on;
SELECT gp_inject_fault_infinite('tempcat_oid_exhausted', 'skip', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TEMP TABLE tc_noid (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_oid_exhausted', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT 'tc_noid'::regclass::oid >= 4026531840 AS in_reserved_range;
DROP TABLE tc_noid;
