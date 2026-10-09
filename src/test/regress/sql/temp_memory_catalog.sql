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

-- Ordinary temporary tables are not affected.
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
-- enum labels are also read by ordered (forward and backward) catalog scans
CREATE TYPE pg_temp.tc_mood AS ENUM ('sad', 'ok', 'happy');
ALTER TYPE pg_temp.tc_mood ADD VALUE 'meh' AFTER 'sad';
CREATE TEMP TABLE tc_moods (m pg_temp.tc_mood) DISTRIBUTED RANDOMLY;
INSERT INTO tc_moods VALUES ('happy'), ('sad'), ('meh'), ('ok');
SELECT m FROM tc_moods ORDER BY m;
SELECT enum_first(NULL::pg_temp.tc_mood), enum_last(NULL::pg_temp.tc_mood),
       enum_range(NULL::pg_temp.tc_mood);
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
CREATE TEMP TABLE tc_rls (a int, b int) DISTRIBUTED BY (a);
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
