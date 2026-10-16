--
-- Catalog rows of temporary objects kept in memory (catalog/tempcat.c).
--
-- With gp_enable_temp_memory_catalog on when the session's temporary schema
-- is created, temporary objects get OIDs from the reserved range starting at
-- 0xF0000000 (4026531840), and their catalog rows never reach the on-disk
-- catalogs, but for dependencies on ordinary objects.  Catalog lookups and
-- scans of the catalogs see the rows in memory.
--

-- On-disk catalog rows that belong to objects with reserved OIDs, on the
-- coordinator and on the segments.  Scans see the rows in memory too, which
-- have TIDs with offsets from 0x8000 (32768) up.  Dependencies of such
-- objects on ordinary objects (a column of a user-defined type, ...) are
-- kept on disk on purpose, to protect the ordinary objects from DROP in
-- other sessions; they are not counted.
CREATE FUNCTION tc_disk_rows() RETURNS bigint LANGUAGE sql AS $$
  SELECT sum(n)::bigint FROM (
    SELECT count(*) AS n FROM pg_class
     WHERE oid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
    UNION ALL SELECT count(*) FROM gp_dist_random('pg_class')
     WHERE oid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
    UNION ALL SELECT count(*) FROM pg_type
     WHERE oid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
    UNION ALL SELECT count(*) FROM gp_dist_random('pg_type')
     WHERE oid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
    UNION ALL SELECT count(*) FROM pg_attribute
     WHERE attrelid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
    UNION ALL SELECT count(*) FROM gp_dist_random('pg_attribute')
     WHERE attrelid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
    UNION ALL SELECT count(*) FROM pg_depend
     WHERE refobjid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
    UNION ALL SELECT count(*) FROM gp_dist_random('pg_depend')
     WHERE refobjid >= 4026531840 AND (ctid::text::point)[1] < 32768 AND gp_segment_id >= -1
  ) s
$$;

SET gp_enable_temp_memory_catalog = on;
SET optimizer = off;

CREATE TEMP TABLE tc_t (a int PRIMARY KEY, b text DEFAULT 'x' CHECK (b <> ''))
  DISTRIBUTED BY (a);
SELECT 'tc_t'::regclass::oid >= 4026531840 AS reserved,
       pg_my_temp_schema()::oid >= 4026531840 AS reserved_namespace;
SELECT relname, relkind, relpersistence FROM pg_class
 WHERE relnamespace = pg_my_temp_schema() ORDER BY relname;
SELECT attname, atttypid::regtype FROM pg_attribute
 WHERE attrelid = 'tc_t'::regclass AND attnum > 0 ORDER BY attnum;
SELECT conname, contype FROM pg_constraint WHERE conrelid = 'tc_t'::regclass ORDER BY 1;
SELECT column_name, data_type FROM information_schema.columns
 WHERE table_name = 'tc_t' ORDER BY ordinal_position;
SELECT indisprimary, indisunique, indkey FROM pg_index WHERE indexrelid = 'tc_t_pkey'::regclass;
-- Index, index-only and bitmap scans of the catalogs in plans see the rows
-- in memory too: index and index-only scans in index order, also for merge
-- joins, which mark and restore positions.  (tc_explain() hides reserved
-- OIDs, which differ from run to run.)
CREATE FUNCTION tc_explain(q text) RETURNS SETOF text LANGUAGE plpgsql AS $$
DECLARE l text;
BEGIN
  FOR l IN EXECUTE 'EXPLAIN (costs off) ' || q LOOP
    RETURN NEXT regexp_replace(l, '\m4[0-9]{9}\M', 'N', 'g');
  END LOOP;
END $$;
SET enable_seqscan = off;
SELECT tc_explain($$SELECT relkind FROM pg_class WHERE relname = 'tc_t'$$);
SELECT relkind FROM pg_class WHERE relname = 'tc_t';
SELECT relkind FROM pg_class WHERE relname = 'tc_t'::text;
SELECT tc_explain($$SELECT count(*) FROM pg_class WHERE relname IN ('tc_t', 'tc_t_pkey', 'pg_class')$$);
SELECT count(*) FROM pg_class WHERE relname IN ('tc_t', 'tc_t_pkey', 'pg_class');
SELECT tc_explain($$SELECT relname FROM pg_class WHERE relname >= 'tc_' AND relname < 'tc`' ORDER BY relname DESC$$);
SELECT relname FROM pg_class WHERE relname >= 'tc_' AND relname < 'tc`' ORDER BY relname DESC;
SELECT tc_explain($$SELECT attname FROM pg_attribute WHERE attrelid = 'tc_t'::regclass AND attnum > 0 ORDER BY attnum DESC$$);
SELECT attname FROM pg_attribute WHERE attrelid = 'tc_t'::regclass AND attnum > 0 ORDER BY attnum DESC;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SELECT tc_explain($$SELECT relname FROM pg_class WHERE relname = 'tc_t' AND relnamespace = pg_my_temp_schema()$$);
SELECT relname FROM pg_class WHERE relname = 'tc_t' AND relnamespace = pg_my_temp_schema();
SELECT tc_explain($$SELECT relname FROM pg_class WHERE relname = 'tc_t' OR relname = 'pg_class'$$);
SELECT relname FROM pg_class WHERE relname = 'tc_t' OR relname = 'pg_class' ORDER BY 1;
RESET enable_indexonlyscan;
RESET enable_indexscan;
RESET enable_seqscan;
-- The same joins with sequential scans, and with merge joins of index and
-- index-only scans, over ordinary and temporary relations.
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS n, sum(a1.atttypid::int8 * a2.attnum) AS s1,
       sum(a1.attnum * a2.atttypid::int8) AS s2
  FROM pg_attribute a1 JOIN pg_attribute a2 ON a1.attrelid = a2.attrelid
 WHERE a1.attrelid IN ('pg_class'::regclass, 'tc_t'::regclass, 'pg_type'::regclass, 'tc_t_pkey'::regclass) \gset ref_
RESET enable_bitmapscan;
RESET enable_indexonlyscan;
RESET enable_indexscan;
SET enable_seqscan = off;
SET enable_hashjoin = off;
SET enable_nestloop = off;
SET enable_mergejoin = on;
SET enable_sort = off;
SET enable_material = off;
SELECT tc_explain($$SELECT count(*), sum(a1.attnum * a2.atttypid::int8)
  FROM pg_attribute a1 JOIN pg_attribute a2 ON a1.attrelid = a2.attrelid
 WHERE a1.attrelid IN ('pg_class'::regclass, 'tc_t'::regclass, 'pg_type'::regclass, 'tc_t_pkey'::regclass)$$);
SELECT count(*) = :ref_n AND sum(a1.attnum * a2.atttypid::int8) = :ref_s2 AS same
  FROM pg_attribute a1 JOIN pg_attribute a2 ON a1.attrelid = a2.attrelid
 WHERE a1.attrelid IN ('pg_class'::regclass, 'tc_t'::regclass, 'pg_type'::regclass, 'tc_t_pkey'::regclass);
SELECT tc_explain($$SELECT count(*), sum(a1.atttypid::int8 * a2.attnum)
  FROM pg_attribute a1 JOIN (SELECT attrelid, attnum FROM pg_attribute) a2 ON a1.attrelid = a2.attrelid
 WHERE a1.attrelid IN ('pg_class'::regclass, 'tc_t'::regclass, 'pg_type'::regclass, 'tc_t_pkey'::regclass)$$);
SELECT count(*) = :ref_n AND sum(a1.atttypid::int8 * a2.attnum) = :ref_s1 AS same
  FROM pg_attribute a1 JOIN (SELECT attrelid, attnum FROM pg_attribute) a2 ON a1.attrelid = a2.attrelid
 WHERE a1.attrelid IN ('pg_class'::regclass, 'tc_t'::regclass, 'pg_type'::regclass, 'tc_t_pkey'::regclass);
RESET enable_material;
RESET enable_sort;
RESET enable_mergejoin;
RESET enable_nestloop;
RESET enable_hashjoin;
RESET enable_seqscan;
DROP FUNCTION tc_explain(text);
-- So do the plans of GPORCA, if it plans catalog queries: its index-only
-- scans recheck the rows in memory too, here against the array.
SET optimizer = on;
SET optimizer_enable_coordinator_only_queries = on;
SELECT count(*) FROM pg_attribute
 WHERE attrelid IN ('tc_t'::regclass, 'tc_t_pkey'::regclass) AND attnum > 0;
SELECT relname FROM pg_class WHERE relname IN ('tc_t', 'tc_t_pkey', 'pg_class') ORDER BY 1;
RESET optimizer_enable_coordinator_only_queries;
SET optimizer = off;
-- The segments have them too.
SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname IN ('tc_t', 'tc_t_pkey');
-- And nothing of them is on disk.
SELECT tc_disk_rows();
SELECT count(*) AS in_memory FROM pg_class
 WHERE oid >= 4026531840 AND (ctid::text::point)[1] >= 32768 AND gp_segment_id >= -1;

INSERT INTO tc_t SELECT g, g::text FROM generate_series(1, 100) g;
INSERT INTO tc_t VALUES (1, 'dup');
INSERT INTO tc_t VALUES (101, '');
SELECT count(*), sum(a) FROM tc_t;
-- A plan with motions, run by reader gangs, which see the writer's rows
SELECT count(*) FROM tc_t t1 JOIN tc_t t2 ON t1.a = t2.a + 1;
SELECT count(*) FROM tc_t t JOIN gp_dist_random('pg_attribute') a
  ON a.attrelid = 'tc_t'::regclass AND a.attnum = t.a;

-- TID scans of the catalogs find the rows in memory; sample scans do not
-- see them, and are refused.
SET enable_seqscan = off;
EXPLAIN (costs off)
SELECT relname FROM pg_class WHERE ctid = (SELECT ctid FROM pg_class WHERE relname = 'tc_t');
SELECT relname FROM pg_class WHERE ctid = (SELECT ctid FROM pg_class WHERE relname = 'tc_t');
RESET enable_seqscan;
SELECT count(*) FROM pg_class TABLESAMPLE SYSTEM (100) WHERE relname = 'tc_t';

-- DDL and maintenance
ALTER TABLE tc_t ADD COLUMN c int DEFAULT 7;
ALTER TABLE tc_t RENAME COLUMN b TO bb;
ALTER TABLE tc_t ALTER COLUMN c TYPE bigint;
ALTER TABLE tc_t SET WITH (reorganize = true);
CREATE INDEX tc_t_bb ON tc_t (bb);
ANALYZE tc_t;
SELECT attname, null_frac FROM pg_stats WHERE tablename = 'tc_t' ORDER BY attname;
VACUUM tc_t;
TRUNCATE tc_t;
COMMENT ON TABLE tc_t IS 'kept in memory';
SELECT obj_description('tc_t'::regclass, 'pg_class');
SELECT tc_disk_rows();

-- Other kinds of temporary objects
CREATE TEMP SEQUENCE tc_seq;
SELECT nextval('tc_seq'), nextval('tc_seq');
CREATE TEMP VIEW tc_v AS SELECT a, bb FROM tc_t WHERE a > 0;
CREATE TYPE pg_temp.tc_comp AS (x int, y text);
CREATE TYPE pg_temp.tc_enum AS ENUM ('a', 'b', 'c');
CREATE TYPE pg_temp.tc_enum_dup AS ENUM ('a', 'a');
CREATE DOMAIN pg_temp.tc_dom AS int CHECK (VALUE > 0);
CREATE FUNCTION pg_temp.tc_f(x pg_temp.tc_dom) RETURNS int LANGUAGE sql AS 'SELECT x * 2';
SELECT pg_temp.tc_f(21);
SELECT pg_temp.tc_f(-1);
CREATE TEMP TABLE tc_typed (c pg_temp.tc_comp, e pg_temp.tc_enum, d pg_temp.tc_dom)
  DISTRIBUTED RANDOMLY;
INSERT INTO tc_typed VALUES ((1, 'one'), 'b', 5);
SELECT * FROM tc_typed;
SELECT 'b'::pg_temp.tc_enum < 'c'::pg_temp.tc_enum AS ordered;
CREATE TEMP TABLE tc_ao (a int, b text) WITH (appendonly = true) DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_aocs (a int, b text) WITH (appendonly = true, orientation = column)
  DISTRIBUTED BY (a);
INSERT INTO tc_ao SELECT g, g::text FROM generate_series(1, 1000) g;
INSERT INTO tc_aocs SELECT g, g::text FROM generate_series(1, 1000) g;
DELETE FROM tc_ao WHERE a <= 500;
VACUUM tc_ao;
SELECT count(*) FROM tc_ao;
SELECT count(*) FROM tc_aocs;
CREATE TEMP TABLE tc_part (a int, b int) DISTRIBUTED BY (a)
  PARTITION BY RANGE (b) (START (0) END (10) EVERY (5));
INSERT INTO tc_part SELECT g, g % 10 FROM generate_series(1, 100) g;
SELECT count(*) FROM tc_part;
SELECT tc_disk_rows();

-- Ordinary objects cannot depend on temporary objects kept in memory, nor
-- can those inherit from or be typed by ordinary ones.
CREATE TYPE tc_ord_comp AS (x int);
CREATE TABLE tc_ord_parent (a int) DISTRIBUTED BY (a);
CREATE FUNCTION public.tc_bad(pg_temp.tc_comp) RETURNS int LANGUAGE sql AS 'SELECT 1';
CREATE TABLE tc_bad_t (c pg_temp.tc_comp) DISTRIBUTED RANDOMLY;
CREATE TEMP TABLE tc_child () INHERITS (tc_ord_parent);
CREATE TEMP TABLE tc_of OF tc_ord_comp;
CREATE CAST (pg_temp.tc_comp AS text) WITH INOUT;
ALTER EXTENSION plpgsql ADD FUNCTION pg_temp.tc_f(pg_temp.tc_dom);
CREATE STATISTICS pg_temp.tc_stx ON a FROM tc_ord_parent;
CREATE TEMP TABLE tc_inh_parent (a int) DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_inh_child () INHERITS (tc_inh_parent);
-- A dependency on an ordinary object stays on disk.
CREATE TEMP TABLE tc_uses_ord (c tc_ord_comp) DISTRIBUTED RANDOMLY;
SELECT classid::regclass, refobjid::regtype FROM pg_depend
 WHERE objid = 'tc_uses_ord'::regclass AND refobjid = 'tc_ord_comp'::regtype;

-- Subtransactions and aborts
BEGIN;
CREATE TEMP TABLE tc_rb (a int) DISTRIBUTED BY (a);
SAVEPOINT s;
CREATE TEMP TABLE tc_rb2 (a int) DISTRIBUTED BY (a);
ROLLBACK TO s;
SELECT relname FROM pg_class WHERE relname LIKE 'tc_rb%' ORDER BY 1;
ROLLBACK;
SELECT relname FROM pg_class WHERE relname LIKE 'tc_rb%' ORDER BY 1;
DO $$
BEGIN
  FOR i IN 1..20 LOOP
    BEGIN
      EXECUTE format('CREATE TEMP TABLE tc_loop%s (a int) DISTRIBUTED BY (a)', i);
      IF i % 2 = 0 THEN
        RAISE EXCEPTION 'roll back %', i;
      END IF;
    EXCEPTION WHEN raise_exception THEN
      NULL;
    END;
  END LOOP;
END $$;
SELECT count(*) FROM pg_class WHERE relname LIKE 'tc_loop%';

-- Creating and dropping temporary tables leaves no dead rows behind in the
-- on-disk catalogs.
SELECT pg_stat_get_xact_tuples_inserted('pg_class'::regclass) AS ins_before \gset
BEGIN;
CREATE TEMP TABLE tc_churn (a int PRIMARY KEY, b text) DISTRIBUTED BY (a);
DROP TABLE tc_churn;
SELECT pg_stat_get_xact_tuples_inserted('pg_class'::regclass) - :ins_before AS pg_class_inserts;
COMMIT;

-- Catalog rows of temporary objects cannot be added with SQL.
SET allow_system_table_mods = on;
INSERT INTO pg_description VALUES ('tc_t'::regclass, 'pg_class'::regclass, 1, 'x');
UPDATE pg_description SET description = 'updated'
 WHERE objoid = 'tc_t'::regclass AND objsubid = 0;
SELECT obj_description('tc_t'::regclass, 'pg_class');
RESET allow_system_table_mods;

-- Plans made before the session kept temporary objects in memory see them
-- as well: that of a statement that creates the session's first temporary
-- object does not see what it creates itself, and one kept for the rest of
-- such a statement sees what it created.
\c
SET gp_enable_temp_memory_catalog = on;
SET optimizer = off;
CREATE TEMP TABLE tc_cols AS
  SELECT attname FROM pg_attribute WHERE attrelid = 'pg_class'::regclass AND attnum = 1
  DISTRIBUTED RANDOMLY;
SELECT * FROM tc_cols;
\c
SET gp_enable_temp_memory_catalog = on;
SET optimizer = off;
SET enable_seqscan = off;
CREATE FUNCTION tc_count_rel(n name) RETURNS bigint LANGUAGE sql VOLATILE AS
  $$ SELECT count(*) FROM pg_class WHERE relname = n $$;
CREATE FUNCTION tc_kind_rel(n name) RETURNS "char" LANGUAGE sql VOLATILE AS
  $$ SELECT relkind FROM pg_class WHERE relname = n $$;
CREATE FUNCTION tc_make(i int) RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  IF i = 1 THEN
    CREATE TEMP TABLE tc_early (a int) DISTRIBUTED BY (a);
  END IF;
  RETURN i;
END $$;
SELECT i, tc_count_rel('tc_early'), tc_kind_rel('tc_early'), tc_make(i)
  FROM generate_series(1, 2) i;
DROP FUNCTION tc_count_rel(name);
DROP FUNCTION tc_kind_rel(name);
DROP FUNCTION tc_make(int);

-- The setting has no effect on a session whose temporary schema is on disk
\c
SET gp_enable_temp_memory_catalog = off;
CREATE TEMP TABLE tc_disk (a int) DISTRIBUTED BY (a);
SELECT 'tc_disk'::regclass::oid >= 4026531840 AS reserved;
SET gp_enable_temp_memory_catalog = on;
CREATE TEMP TABLE tc_disk2 (a int) DISTRIBUTED BY (a);
SELECT 'tc_disk2'::regclass::oid >= 4026531840 AS reserved;

-- Beyond gp_temp_memory_catalog_max_size, the rows go to files in
-- the directory for temporary files, counted as work files, and beyond
-- gp_temp_memory_catalog_max_file_size more, the
-- session runs out of room.
\c
SET gp_enable_temp_memory_catalog = on;
SET gp_temp_memory_catalog_max_size = '1MB';
SET gp_temp_memory_catalog_max_file_size = '8MB';
CREATE TEMP TABLE tc_first (a int) DISTRIBUTED BY (a);
SELECT count(*) AS files_before FROM pg_ls_dir('base/pgsql_tmp', true, false) f
 WHERE f LIKE 'pgsql_tmpdsm.%' \gset
DO $$
BEGIN
  FOR i IN 1..200 LOOP
    EXECUTE format('CREATE TEMP TABLE tc_many%s (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)', i);
  END LOOP;
END $$;
SELECT count(*) > :files_before AS more_files FROM pg_ls_dir('base/pgsql_tmp', true, false) f
 WHERE f LIKE 'pgsql_tmpdsm.%';
SELECT segid, count(*) > 0 AS work_files FROM gp_toolkit.gp_workfile_entries
 WHERE sess_id = current_setting('gp_session_id')::int AND prefix LIKE 'dsm%'
 GROUP BY segid ORDER BY segid;
SELECT count(*) FROM pg_class WHERE relname LIKE 'tc_many%';
INSERT INTO tc_many200 VALUES (1, 'x');
SELECT * FROM tc_many200;
SELECT count(*) FROM gp_dist_random('pg_class') WHERE relname LIKE 'tc_many%';
DO $$
BEGIN
  FOR i IN 201..2000 LOOP
    EXECUTE format('CREATE TEMP TABLE tc_many%s (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)', i);
  END LOOP;
END $$;
SELECT count(*) FROM pg_class WHERE relname LIKE 'tc_many%';

\c
SET gp_enable_temp_memory_catalog = on;
SET gp_temp_memory_catalog_max_size = '1MB';
SET gp_temp_memory_catalog_max_file_size = 0;
DO $$
BEGIN
  FOR i IN 1..500 LOOP
    EXECUTE format('CREATE TEMP TABLE tc_full%s (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)', i);
  END LOOP;
END $$;
-- Creating and dropping in one transaction frees the dead rows on the way.
DO $$
BEGIN
  FOR i IN 1..500 LOOP
    EXECUTE format('CREATE TEMP TABLE tc_cycle (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)');
    EXECUTE 'DROP TABLE tc_cycle';
  END LOOP;
END $$;
SELECT count(*) FROM pg_class WHERE relpersistence = 't';

-- More of what temporary objects kept in memory do as those on disk do.
\c
SET gp_enable_temp_memory_catalog = on;
SET optimizer = off;
CREATE TEMP TABLE tc_basic (a int, b text) DISTRIBUTED BY (a);
INSERT INTO tc_basic SELECT i, 'v' || i FROM generate_series(1, 100) i;
-- Cursors and loops over queries run on reader gangs as well.
BEGIN;
DECLARE c CURSOR FOR SELECT a, b FROM tc_basic ORDER BY a;
FETCH 2 FROM c;
CLOSE c;
COMMIT;
DO $$ DECLARE r record; n int := 0; BEGIN FOR r IN SELECT * FROM tc_basic LOOP n := n + 1; END LOOP; RAISE NOTICE 'rows seen by FOR loop: %', n; END $$;
-- Catalog queries planned on the segments (in functions run there), with
-- any kind of scan, and in a separate slice on the coordinator (entry-db)
-- see the rows in memory.
CREATE FUNCTION pg_temp.tc_att_count(r oid) RETURNS bigint LANGUAGE plpgsql VOLATILE AS
$$ BEGIN RETURN (SELECT count(*) FROM pg_attribute WHERE attrelid = r AND attnum > 0); END $$;
SELECT DISTINCT pg_temp.tc_att_count('tc_basic'::regclass) FROM gp_dist_random('gp_id');
SET enable_seqscan = off;
SELECT DISTINCT pg_temp.tc_att_count('tc_basic'::regclass) FROM gp_dist_random('gp_id');
SET enable_indexonlyscan = off;
SELECT DISTINCT pg_temp.tc_att_count('tc_basic'::regclass) FROM gp_dist_random('gp_id');
SET enable_indexscan = off;
SELECT DISTINCT pg_temp.tc_att_count('tc_basic'::regclass) FROM gp_dist_random('gp_id');
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_seqscan;
CREATE FUNCTION tc_count_basic() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM tc_basic';
CREATE TEMP TABLE tc_entrydb (a int, n bigint) DISTRIBUTED BY (a);
INSERT INTO tc_entrydb SELECT g, s.n FROM generate_series(1, 3) g, (SELECT tc_count_basic() AS n) s;
SELECT a, n FROM tc_entrydb ORDER BY a;
DROP FUNCTION tc_count_basic();

-- A temporary table hides an ordinary table of the same name.
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
UPDATE tc_toast SET t = t || 'x' WHERE a = 2;
SELECT a, length(t) FROM tc_toast ORDER BY a;

-- TRUNCATE of a column-oriented table followed by ADD COLUMN with a default
-- (needs the attribute encoding rows of the truncated table).
CREATE TEMP TABLE tc_aoco2 (a int, b int)
  WITH (appendonly = true, orientation = column) DISTRIBUTED BY (a);
INSERT INTO tc_aoco2 SELECT i, i FROM generate_series(1, 100) i;
TRUNCATE tc_aoco2;
INSERT INTO tc_aoco2 SELECT i, i FROM generate_series(1, 10) i;
ALTER TABLE tc_aoco2 ADD COLUMN c int DEFAULT 7;
SELECT count(*), sum(c) FROM tc_aoco2;
-- The AO functions that query the aux relations of an AO table.
SELECT get_ao_compression_ratio('tc_aoco2') > 0 AS ratio;
SELECT count(*) > 0 AS segfiles FROM gp_toolkit.__gp_aocsseg('tc_aoco2');
SELECT sum(tupcount) FROM get_ao_distribution('tc_aoco2');

-- An insert into an append-only table created in the same transaction uses
-- the reserved segment file 0 (needs the real xmin of the pg_class row).
BEGIN;
CREATE TEMP TABLE tc_ao_segno (a int) WITH (appendonly = true) DISTRIBUTED BY (a);
INSERT INTO tc_ao_segno SELECT generate_series(1, 30);
COMMIT;
SELECT DISTINCT segno FROM gp_toolkit.__gp_aoseg('tc_ao_segno');
SELECT sum(tupcount) FROM gp_toolkit.__gp_aoseg('tc_ao_segno');

-- gp_fastsequence stays non-transactional for rows in memory: rolled back
-- inserts do not let later inserts reuse their row numbers, which would make
-- index scans return wrong rows.  Both for the segment file used in the
-- creating transaction (whose gp_fastsequence row is created in memory) and
-- for later ones.
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

-- Utility commands that scan the catalogs directly
-- (table_beginscan_catalog()) see the rows in memory too.
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

-- Catalog views and functions, and COPY TO of a catalog.
CREATE TEMP VIEW tc_view AS SELECT a FROM tc_basic WHERE a > 50;
COMMENT ON TABLE tc_basic IS 'basic';
SELECT pg_get_viewdef('tc_view');
SELECT obj_description('tc_basic'::regclass);
SELECT table_name, table_type FROM information_schema.tables
 WHERE table_name IN ('tc_basic', 'tc_view') ORDER BY 1;
SELECT column_name, data_type FROM information_schema.columns
 WHERE table_name = 'tc_view' ORDER BY ordinal_position;
\copy pg_class (relname) TO PROGRAM 'grep -c ^tc_basic$'

-- Transactions
BEGIN;
DROP TABLE tc_basic CASCADE;
ROLLBACK;
SELECT count(*) FROM tc_basic;
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
-- A failing CREATE TABLE AS leaves nothing behind.
CREATE TEMP TABLE tc_ctas_fail AS SELECT 1 / (i - 5) AS x FROM generate_series(1, 10) i DISTRIBUTED BY (x);
SELECT to_regclass('tc_ctas_fail') IS NULL AS not_created;
CREATE TEMP TABLE tc_ctas_fail AS SELECT i AS x FROM generate_series(1, 10) i DISTRIBUTED BY (x);
SELECT count(*) FROM tc_ctas_fail;
-- Changes of rows in memory on the segments are committed in two phases
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
INSERT INTO tc_sp1 VALUES (1);
PREPARE tc_prep AS SELECT count(*) FROM tc_sp1;
EXECUTE tc_prep;
ALTER TABLE tc_sp1 SET DISTRIBUTED RANDOMLY;
EXECUTE tc_prep;
DEALLOCATE tc_prep;

-- Functions, aggregates and operators run on the segments too.
CREATE FUNCTION pg_temp.tc_func(int) RETURNS int IMMUTABLE LANGUAGE sql AS 'SELECT $1 * 10';
CREATE TEMP TABLE tc_fn_data (a int, b int) DISTRIBUTED BY (a);
INSERT INTO tc_fn_data SELECT i, i % 5 FROM generate_series(1, 100) i;
SELECT sum(pg_temp.tc_func(a)) FROM tc_fn_data;
SELECT count(*) FROM tc_fn_data x JOIN tc_fn_data y ON pg_temp.tc_func(x.b) = y.a;
CREATE AGGREGATE pg_temp.tc_sum(int) (sfunc = int4pl, stype = int, initcond = '0');
SELECT pg_temp.tc_sum(a) FROM tc_fn_data;
CREATE OPERATOR pg_temp.=== (PROCEDURE = int4eq, LEFTARG = int, RIGHTARG = int);
SELECT count(*) FROM tc_fn_data WHERE a OPERATOR(pg_temp.===) 5;
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
DROP OPERATOR public.<<<< (int, int);
-- Enum labels are read by ordered catalog scans, forward and backward;
-- they get reserved OIDs too, the same on the segments.
CREATE TYPE pg_temp.tc_mood AS ENUM ('sad', 'ok', 'happy');
ALTER TYPE pg_temp.tc_mood ADD VALUE 'meh' AFTER 'sad';
CREATE TEMP TABLE tc_moods (m pg_temp.tc_mood) DISTRIBUTED RANDOMLY;
INSERT INTO tc_moods VALUES ('happy'), ('sad'), ('meh'), ('ok');
SELECT m FROM tc_moods ORDER BY m;
SELECT enum_first(NULL::pg_temp.tc_mood), enum_last(NULL::pg_temp.tc_mood),
       enum_range(NULL::pg_temp.tc_mood);
SELECT count(*) AS labels, bool_and(oid >= 4026531840) AS reserved
  FROM pg_enum WHERE enumtypid = 'pg_temp.tc_mood'::regtype;
SELECT count(*) AS seg_labels_differing FROM gp_dist_random('pg_enum') s
 WHERE s.enumtypid = 'pg_temp.tc_mood'::regtype
   AND s.oid NOT IN (SELECT oid FROM pg_enum WHERE enumtypid = 'pg_temp.tc_mood'::regtype);
CREATE TYPE pg_temp.tc_range AS RANGE (subtype = int4);
SELECT pg_temp.tc_range(1, 5) @> 3 AS contains;
CREATE STATISTICS pg_temp.tc_stx (dependencies, ndistinct) ON a, b FROM tc_fn_data;
ANALYZE tc_fn_data;
SELECT stxname, stxkind FROM pg_statistic_ext WHERE stxrelid = 'tc_fn_data'::regclass;
SELECT d.stxdndistinct IS NOT NULL AS has_ndistinct
  FROM pg_statistic_ext_data d JOIN pg_statistic_ext s ON s.oid = d.stxoid
 WHERE s.stxname = 'tc_stx';
-- Row-level security policies, external tables and type encodings.
CREATE TEMP TABLE tc_rls (a int, b int) DISTRIBUTED BY (a);
INSERT INTO tc_rls SELECT i, i FROM generate_series(1, 10) i;
CREATE POLICY tc_rls_pol ON tc_rls USING (a > 5);
ALTER TABLE tc_rls ENABLE ROW LEVEL SECURITY;
CREATE ROLE tc_rls_user;
GRANT SELECT ON tc_rls TO tc_rls_user;
SET ROLE tc_rls_user;
SELECT count(*) AS visible_rows FROM tc_rls;
RESET ROLE;
DROP POLICY tc_rls_pol ON tc_rls;
SET ROLE tc_rls_user;
SELECT count(*) AS visible_rows FROM tc_rls;
RESET ROLE;
DROP TABLE tc_rls;
DROP ROLE tc_rls_user;
CREATE DOMAIN pg_temp.tc_enc_dom AS int;
ALTER TYPE pg_temp.tc_enc_dom SET DEFAULT ENCODING (compresstype = zlib);
SELECT count(*) AS encodings FROM pg_type_encoding WHERE typid = 'pg_temp.tc_enc_dom'::regtype;
CREATE EXTERNAL WEB TEMP TABLE tc_ext (a int) EXECUTE 'echo 1' FORMAT 'text';
SELECT count(*) AS ext_rows FROM tc_ext;
DROP EXTERNAL TABLE tc_ext;
DROP DOMAIN pg_temp.tc_enc_dom;
-- SQL DML on catalog rows kept in memory (allow_system_table_mods): they
-- have no place in the heap, so updates, deletes and row locks must not
-- touch it (the gangsize test changes numsegments this way).
SET allow_system_table_mods = on;
CREATE TEMP TABLE tc_dml (a int, b int) DISTRIBUTED BY (a);
UPDATE gp_distribution_policy SET numsegments = 2 WHERE localoid = 'tc_dml'::regclass;
UPDATE gp_distribution_policy SET numsegments = 3 WHERE localoid = 'tc_dml'::regclass;
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
UPDATE gp_distribution_policy p SET numsegments = 2 FROM generate_series(1, 2) g WHERE p.localoid = 'tc_dml'::regclass;
SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
ROLLBACK;
-- DELETE ... RETURNING fetches the old row by its TID
BEGIN;
DELETE FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass RETURNING numsegments;
ROLLBACK;
SELECT numsegments FROM gp_distribution_policy WHERE localoid = 'tc_dml'::regclass;
RESET allow_system_table_mods;

-- The statistics of dropped temporary tables and functions are purged
-- while the session lives: pgstat_vacuum_stat() keeps those of live
-- sessions' OIDs, as it cannot see which of their objects still exist.
-- (Checked on the coordinator, where the purge goes with the next
-- statistics report, at most every 500ms.)
CREATE FUNCTION pg_temp.tc_wait_analyze_count(r oid, n bigint) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN FOR i IN 1..100 LOOP PERFORM pg_stat_clear_snapshot(); EXIT WHEN pg_stat_get_analyze_count(r) = n; PERFORM pg_sleep(0.1); END LOOP; RETURN pg_stat_get_analyze_count(r); END $$;
CREATE FUNCTION pg_temp.tc_wait_function_calls(f oid, n bigint) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN FOR i IN 1..100 LOOP PERFORM pg_stat_clear_snapshot(); EXIT WHEN pg_stat_get_function_calls(f) IS NOT DISTINCT FROM n; PERFORM pg_sleep(0.1); END LOOP; RETURN pg_stat_get_function_calls(f); END $$;
CREATE TEMP TABLE tc_stat_purge (a int) DISTRIBUTED BY (a);
ANALYZE tc_stat_purge;
SELECT 'tc_stat_purge'::regclass::oid AS tc_stat_purge_oid \gset
SELECT pg_temp.tc_wait_analyze_count(:tc_stat_purge_oid, 1) AS analyze_count;
DROP TABLE tc_stat_purge;
SELECT pg_sleep(0.6);
SELECT pg_temp.tc_wait_analyze_count(:tc_stat_purge_oid, 0) AS analyze_count;
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
-- Also of those dropped ON COMMIT.
BEGIN;
CREATE TEMP TABLE tc_stat_oncommit (a int) ON COMMIT DROP DISTRIBUTED BY (a);
ANALYZE tc_stat_oncommit;
SELECT 'tc_stat_oncommit'::regclass::oid AS tc_stat_oncommit_oid \gset
SELECT pg_temp.tc_wait_analyze_count(:tc_stat_oncommit_oid, 1) AS analyze_count;
COMMIT;
SELECT pg_sleep(0.6);
SELECT pg_temp.tc_wait_analyze_count(:tc_stat_oncommit_oid, 0) AS analyze_count;

-- Turning the setting off does not affect the session's temporary objects:
-- they stay in memory, and new ones join them.  DISCARD TEMP drops them.
SET gp_enable_temp_memory_catalog = off;
SELECT count(*) FROM tc_basic x JOIN tc_basic y ON x.a = y.a + 1;
CREATE TEMP TABLE tc_after_off (a int) DISTRIBUTED BY (a);
SELECT 'tc_after_off'::regclass::oid >= 4026531840 AS reserved;
SET gp_enable_temp_memory_catalog = on;
SELECT tc_disk_rows();
DISCARD TEMP;
SELECT to_regclass('tc_basic') IS NULL AS discarded;
CREATE TEMP TABLE tc_after_discard (a int) DISTRIBUTED BY (a);
INSERT INTO tc_after_discard VALUES (1);
SELECT count(*) FROM tc_after_discard;
SELECT tc_disk_rows();

-- Failures, made by faults on the coordinator.
\c
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
RESET client_min_messages;
SET gp_enable_temp_memory_catalog = on;
\set VERBOSITY terse
-- Allocations failing in the middle of adding a row (here from the tenth
-- hash node of the statement on) fail the statement, and leave nothing
-- behind; so do those failing for a new version of a row.
CREATE TEMP TABLE tc_f0 (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'skip', '', '', '', 10, -1, 0, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TEMP TABLE tc_oom (a int, b text) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT count(*) FROM pg_class WHERE relname = 'tc_oom';
SELECT gp_inject_fault_infinite('tempcat_hash_node_alloc', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
ALTER TABLE tc_f0 ALTER COLUMN a SET STATISTICS 5;
SELECT gp_inject_fault('tempcat_hash_node_alloc', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT attname, attstattarget FROM pg_attribute WHERE attrelid = 'tc_f0'::regclass AND attnum > 0;
CREATE TEMP TABLE tc_oom (a int, b text) DISTRIBUTED BY (a);
INSERT INTO tc_oom SELECT i, 'x' FROM generate_series(1, 10) i;
SELECT count(*) FROM tc_oom;
DROP TABLE tc_oom, tc_f0;

-- An error while the outcome of earlier transactions is applied to the rows
-- (here as the first row of the dropped table is freed, when the session
-- goes idle after the drop) is only logged, and leaves the rows still to be
-- done consistent: the next transaction finishes them.  (At the start of a
-- transaction, it would fail the transaction.)
CREATE TEMP TABLE tc_pend (a int, b text) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_forget_if_gone', 'error', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
DROP TABLE tc_pend;
SELECT gp_wait_until_triggered_fault('tempcat_forget_if_gone', 1, dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT gp_inject_fault('tempcat_forget_if_gone', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT to_regclass('tc_pend') IS NULL AS dropped;
CREATE TEMP TABLE tc_pend (a int, b text) DISTRIBUTED BY (a);
INSERT INTO tc_pend VALUES (1, 'x');
SELECT * FROM tc_pend;
DROP TABLE tc_pend;

-- A subtransaction abort applies outcomes only if the subtransaction, or
-- one committed into it (here s1), changed rows: exception blocks that
-- change none do not walk the pending rows of the whole transaction.
BEGIN;
CREATE TEMP TABLE tc_sx1 (a int) DISTRIBUTED BY (a);
SAVEPOINT s1;
CREATE TEMP TABLE tc_sx2 (a int) DISTRIBUTED BY (a);
RELEASE SAVEPOINT s1;
SELECT gp_inject_fault_infinite('tempcat_resolve_pending', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
DO $$ BEGIN FOR i IN 1..3 LOOP BEGIN PERFORM 1 / 0; EXCEPTION WHEN division_by_zero THEN NULL; END; END LOOP; END $$;
SELECT substring(gp_inject_fault('tempcat_resolve_pending', 'status', dbid) from 'num times hit:''(\d+)''') AS walks
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SAVEPOINT s2;
CREATE TEMP TABLE tc_sx3 (a int) DISTRIBUTED BY (a);
ROLLBACK TO SAVEPOINT s2;
SELECT substring(gp_inject_fault('tempcat_resolve_pending', 'status', dbid) from 'num times hit:''(\d+)''') AS walks
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT gp_inject_fault('tempcat_resolve_pending', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT relname FROM pg_class WHERE relname LIKE 'tc\_sx_' ORDER BY 1;
COMMIT;
DROP TABLE tc_sx1, tc_sx2;

-- When ordinary objects take every OID of the session's slice of the
-- reserved range (the fault pretends so), the session keeps its temporary
-- objects on disk, with a warning.
\c
SET gp_enable_temp_memory_catalog = on;
\set VERBOSITY terse
SELECT gp_inject_fault_infinite('tempcat_oid_exhausted', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TEMP TABLE tc_noid (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_oid_exhausted', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TEMP TABLE tc_noid2 (a int) DISTRIBUTED BY (a);
SELECT 'tc_noid'::regclass::oid >= 4026531840 AS reserved,
       'tc_noid2'::regclass::oid >= 4026531840 AS reserved2;

-- The OIDs of a slice wrap around at its end (the fault puts the session
-- near it) and go on from its start.
\c
SET gp_enable_temp_memory_catalog = on;
SELECT gp_inject_fault('tempcat_slice_near_end', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
CREATE TEMP TABLE tc_wrap1 (a int) DISTRIBUTED BY (a);
CREATE TEMP TABLE tc_wrap2 (a int) DISTRIBUTED BY (a);
SELECT gp_inject_fault('tempcat_slice_near_end', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT 'tc_wrap1'::regclass::oid > 'tc_wrap2'::regclass::oid AS wrapped,
       'tc_wrap2'::regclass::oid >= 4026531840 AS reserved;
INSERT INTO tc_wrap1 SELECT generate_series(1, 10);
INSERT INTO tc_wrap2 SELECT generate_series(1, 10);
SELECT count(*) FROM tc_wrap1 JOIN tc_wrap2 USING (a);

-- When dynamic shared memory runs short for others (the fault pretends
-- so), a new session keeps the catalog rows of its temporary objects in
-- files from the start; without files, its temporary schema is on disk.
\c
SET gp_enable_temp_memory_catalog = on;
SELECT gp_inject_fault_infinite('tempcat_dsm_space_low', 'skip', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
SELECT count(*) AS files_before FROM pg_ls_dir('base/pgsql_tmp', true, false) f
 WHERE f LIKE 'pgsql_tmpdsm.%' \gset
CREATE TEMP TABLE tc_low (a int) DISTRIBUTED BY (a);
SELECT 'tc_low'::regclass::oid >= 4026531840 AS reserved;
SELECT count(*) > :files_before AS in_files FROM pg_ls_dir('base/pgsql_tmp', true, false) f
 WHERE f LIKE 'pgsql_tmpdsm.%';
\c
SET gp_enable_temp_memory_catalog = on;
SET gp_temp_memory_catalog_max_file_size = 0;
CREATE TEMP TABLE tc_low (a int) DISTRIBUTED BY (a);
SELECT 'tc_low'::regclass::oid >= 4026531840 AS reserved;
SELECT gp_inject_fault('tempcat_dsm_space_low', 'reset', dbid)
  FROM gp_segment_configuration WHERE role = 'p' AND content = -1;

\c
DROP FUNCTION tc_disk_rows();
DROP TYPE tc_ord_comp;
DROP TABLE tc_ord_parent;
