--
-- Catalog rows of temporary objects kept in memory (catalog/tempcat.c).
--
-- With gp_enable_temp_memory_catalog on when the session's temporary schema
-- is created, temporary objects get OIDs from the reserved range starting at
-- 0xF0000000 (4026531840), and their catalog rows never reach the on-disk
-- catalogs, but for dependencies on ordinary objects.  Catalog lookups and
-- sequential scans of the catalogs see the rows in memory.
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
-- Sequential scans of the catalogs see the rows in memory, index scans of
-- plans would not: the planner leaves the catalogs' indexes alone once the
-- session keeps temporary objects in memory.
EXPLAIN (costs off) SELECT relname FROM pg_class WHERE oid = 'pg_class'::regclass;
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
SELECT count(*) AS files_before FROM pg_ls_dir('base/pgsql_tmp') f
 WHERE f LIKE 'pgsql_tmpdsm.%' \gset
DO $$
BEGIN
  FOR i IN 1..200 LOOP
    EXECUTE format('CREATE TEMP TABLE tc_many%s (a int PRIMARY KEY, b text) DISTRIBUTED BY (a)', i);
  END LOOP;
END $$;
SELECT count(*) > :files_before AS more_files FROM pg_ls_dir('base/pgsql_tmp') f
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

\c
DROP FUNCTION tc_disk_rows();
DROP TYPE tc_ord_comp;
DROP TABLE tc_ord_parent;
