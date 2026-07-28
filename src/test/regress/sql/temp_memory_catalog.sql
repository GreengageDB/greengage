--
-- TEMP_MEMORY_CATALOG
-- Test in-memory virtual catalog for temporary tables
--
-- This test exercises the tempcat subsystem which stores temp table
-- metadata in backend-private memory instead of on-disk pg_catalog.
--

-- ============================================================
-- GUC is recognized and can be set
-- ============================================================
SHOW gp_enable_temp_memory_catalog;
SET gp_enable_temp_memory_catalog = on;
SHOW gp_enable_temp_memory_catalog;

-- ============================================================
-- CREATE TEMP TABLE and basic DML
-- ============================================================
CREATE TEMP TABLE tempcat_test1 (id int);

INSERT INTO tempcat_test1 VALUES (1), (2), (3);
SELECT * FROM tempcat_test1 ORDER BY id;

UPDATE tempcat_test1 SET id = id * 10;
SELECT * FROM tempcat_test1 ORDER BY id;

DELETE FROM tempcat_test1 WHERE id = 20;
SELECT * FROM tempcat_test1 ORDER BY id;

-- ============================================================
-- Multiple columns, various types
-- ============================================================
CREATE TEMP TABLE tempcat_test2 (
    a int,
    b text,
    c float8,
    d boolean
);

INSERT INTO tempcat_test2 VALUES (1, 'hello', 3.14, true);
INSERT INTO tempcat_test2 VALUES (2, 'world', 2.72, false);
SELECT * FROM tempcat_test2 ORDER BY a;

-- ============================================================
-- Index on temp table
-- ============================================================
CREATE INDEX tempcat_idx1 ON tempcat_test1 (id);

-- Use the index
SET enable_seqscan = off;
-- start_ignore
EXPLAIN (COSTS OFF) SELECT * FROM tempcat_test1 WHERE id = 10;
-- end_ignore
SELECT * FROM tempcat_test1 WHERE id = 10;
RESET enable_seqscan;

-- ============================================================
-- Transaction rollback
-- ============================================================
BEGIN;
    CREATE TEMP TABLE tempcat_test_rollback (x int);
    INSERT INTO tempcat_test_rollback VALUES (42);
    SELECT * FROM tempcat_test_rollback;
ROLLBACK;

-- Table should not exist after rollback
SELECT * FROM tempcat_test_rollback;

-- ============================================================
-- ON COMMIT DROP
-- ============================================================
BEGIN;
    CREATE TEMP TABLE tempcat_test_rollback (x int) ON COMMIT DROP;
    INSERT INTO tempcat_test_rollback VALUES (42);
    SELECT * FROM tempcat_test_rollback;
COMMIT;

-- Table should not exist after COMMIT
SELECT * FROM tempcat_test_rollback;

-- ============================================================
-- Savepoints
-- ============================================================
BEGIN;
    CREATE TEMP TABLE tempcat_test_sp (x int);
    INSERT INTO tempcat_test_sp VALUES (1);

    SAVEPOINT sp1;
        INSERT INTO tempcat_test_sp VALUES (2);
        SELECT * FROM tempcat_test_sp ORDER BY x;
        
        CREATE TEMP TABLE tempcat_test_sp_2 (x int);

    ROLLBACK TO sp1;
    -- Value 2 should be gone
    SELECT * FROM tempcat_test_sp ORDER BY x;

COMMIT;
-- Table sp should still exist after commit
SELECT * FROM tempcat_test_sp ORDER BY x;
-- Table sp_2 should not exist (was rolled back with savepoint)
SELECT * FROM tempcat_test_sp_2;

-- ============================================================
-- Release savepoint
-- ============================================================
BEGIN;
    SAVEPOINT sp_rel;
        CREATE TEMP TABLE tempcat_test_released (x int);
    RELEASE SAVEPOINT sp_rel;

    -- Table should be visible after release
    SELECT * FROM tempcat_test_released;
COMMIT;

-- Table should still exist after commit
SELECT * FROM tempcat_test_released;
DROP TABLE tempcat_test_released;

-- ============================================================
-- Implicit subtransaction rollback (PL/pgSQL EXCEPTION)
-- ============================================================
DO $$
BEGIN
    CREATE TEMP TABLE tempcat_test_exc_fail (x int);
    RAISE EXCEPTION 'test error';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'caught: %', SQLERRM;
END;
$$;

-- Table should not exist (rolled back with the implicit subtransaction)
SELECT * FROM tempcat_test_exc_fail;

-- ============================================================
-- Implicit subtransaction commit (PL/pgSQL EXCEPTION, no error)
-- ============================================================
DO $$
BEGIN
    CREATE TEMP TABLE tempcat_test_exc_ok (x int);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'should not reach here';
END;
$$;

-- Table should exist (implicit subtransaction committed)
SELECT * FROM tempcat_test_exc_ok;
DROP TABLE tempcat_test_exc_ok;

-- ============================================================
-- Nested savepoints: ROLLBACK TO an outer savepoint must discard the
-- catalog rows created in every inner subtransaction level at once
-- ============================================================
BEGIN;
    CREATE TEMP TABLE tcn_base (x int);       -- top level, survives
    SAVEPOINT s1;
        CREATE TEMP TABLE tcn_l1 (x int);
        SAVEPOINT s2;
            CREATE TEMP TABLE tcn_l2 (x int);
            SAVEPOINT s3;
                CREATE TEMP TABLE tcn_l3 (x int);
    -- unwinds s3, s2 and the work done under s1 in one statement
    ROLLBACK TO s1;
    -- s1 is still active: base is visible, keep working under it
    SELECT * FROM tcn_base;
    CREATE TEMP TABLE tcn_after (x int);
COMMIT;
-- Committed catalog: tcn_base and tcn_after exist ...
SELECT * FROM tcn_base;
SELECT * FROM tcn_after;
-- ... tcn_l1 / tcn_l2 / tcn_l3 were discarded by ROLLBACK TO s1
SELECT * FROM tcn_l1;
SELECT * FROM tcn_l2;
SELECT * FROM tcn_l3;
DROP TABLE tcn_base, tcn_after;

-- ============================================================
-- Repeated ROLLBACK TO the same savepoint
-- (each rollback re-enters via TBLOCK_SUBRESTART -> StartSubTransaction)
-- ============================================================
BEGIN;
    CREATE TEMP TABLE tcr_base (x int);
    SAVEPOINT sp;
        CREATE TEMP TABLE tcr_a (x int);
    ROLLBACK TO sp;
        CREATE TEMP TABLE tcr_b (x int);
    ROLLBACK TO sp;
        CREATE TEMP TABLE tcr_c (x int);
    ROLLBACK TO sp;
    CREATE TEMP TABLE tcr_final (x int);
COMMIT;
-- Only the pre-savepoint table and the final one survive
SELECT * FROM tcr_base;
SELECT * FROM tcr_final;
SELECT * FROM tcr_a;
SELECT * FROM tcr_b;
SELECT * FROM tcr_c;
DROP TABLE tcr_base, tcr_final;

-- ============================================================
-- RELEASE a nested savepoint (merge into parent), then ROLLBACK TO the
-- outer one (the merged catalog rows must be discarded too)
-- ============================================================
BEGIN;
    CREATE TEMP TABLE tcm_base (x int);
    SAVEPOINT a;
        CREATE TEMP TABLE tcm_a (x int);
        SAVEPOINT b;
            CREATE TEMP TABLE tcm_b (x int);
        RELEASE SAVEPOINT b;    -- tcm_b merges up into a's level
        -- after the merge both tables are visible
        SELECT * FROM tcm_a;
        SELECT * FROM tcm_b;
    ROLLBACK TO a;             -- discards tcm_a and the merged tcm_b
    SELECT * FROM tcm_base;    -- base survives
    CREATE TEMP TABLE tcm_after (x int);
COMMIT;
SELECT * FROM tcm_base;
SELECT * FROM tcm_after;
SELECT * FROM tcm_a;
SELECT * FROM tcm_b;
DROP TABLE tcm_base, tcm_after;

-- ============================================================
-- PL/pgSQL EXCEPTION subtransaction nested inside a user savepoint
-- ============================================================
BEGIN;
    CREATE TEMP TABLE tcp_base (x int);
    SAVEPOINT outer_sp;
        CREATE TEMP TABLE tcp_s1 (x int);
        DO $$
        BEGIN
            CREATE TEMP TABLE tcp_pl (y int);
            RAISE EXCEPTION 'boom';
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'caught: %', SQLERRM;
        END;
        $$;
        -- implicit subxact rolled back tcp_pl; tcp_s1 still visible
        SELECT * FROM tcp_s1;
    ROLLBACK TO outer_sp;
    SELECT * FROM tcp_base;    -- base survives
COMMIT;
SELECT * FROM tcp_base;
SELECT * FROM tcp_s1;
SELECT * FROM tcp_pl;
DROP TABLE tcp_base;

-- ============================================================
-- DROP TEMP TABLE
-- ============================================================
DROP TABLE tempcat_test1;

-- Should no longer exist
SELECT * FROM tempcat_test1;

-- Other temp tables should still work
SELECT * FROM tempcat_test2 ORDER BY a;

-- ============================================================
-- Regular tables are unaffected
-- ============================================================
CREATE TABLE tempcat_permanent (id int);
INSERT INTO tempcat_permanent VALUES (100);
SELECT * FROM tempcat_permanent;

-- Verify it's in pg_class with relpersistence = 'p'
SELECT relname, relpersistence
  FROM pg_class
 WHERE relname = 'tempcat_permanent';

DROP TABLE tempcat_permanent;

-- ============================================================
-- Append-optimized temp tables
-- ============================================================

CREATE TEMP TABLE tempcat_ao (a int, b text) WITH (appendonly=true);
INSERT INTO tempcat_ao SELECT i, 'v' || i FROM generate_series(1, 10) i;
CREATE INDEX tempcat_ao_idx ON tempcat_ao (a);
-- Read through the index: the block directory rows must have gone to the
-- block directory's own storage, not into the in-memory catalog.
SET enable_seqscan = off;
SELECT count(*) FROM tempcat_ao WHERE a = 5;
RESET enable_seqscan;
ALTER TABLE tempcat_ao ADD COLUMN c int;
SELECT count(*), sum(a) FROM tempcat_ao;
TRUNCATE tempcat_ao;
SELECT count(*) FROM tempcat_ao;
DROP TABLE tempcat_ao;

CREATE TEMP TABLE tempcat_aoco (a int, b text)
    WITH (appendonly=true, orientation=column, compresstype=zlib);
INSERT INTO tempcat_aoco SELECT i, 'v' || i FROM generate_series(1, 10) i;
SELECT count(*), sum(a) FROM tempcat_aoco;
DROP TABLE tempcat_aoco;

-- Dropping an AO temp table after the GUC was switched off: the delete has
-- to dispatch on the item pointer, not on the GUC.
CREATE TEMP TABLE tempcat_ao_off (a int) WITH (appendonly=true);
SET gp_enable_temp_memory_catalog = off;
DROP TABLE tempcat_ao_off;
SET gp_enable_temp_memory_catalog = on;

-- ============================================================
-- TEMP CATALOG is still respected when switched off
-- ============================================================

SET gp_enable_temp_memory_catalog = off;

CREATE TEMP TABLE tempcat_test2 (id int);
SELECT * FROM tempcat_test2 ORDER BY a;

DROP TABLE tempcat_test2;
SELECT * FROM tempcat_test2 ORDER BY a;

SET gp_enable_temp_memory_catalog = on;

CREATE TEMP TABLE tempcat_test2 (id int);
SELECT * FROM tempcat_test2 ORDER BY id;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE IF EXISTS tempcat_test2;
DROP TABLE IF EXISTS tempcat_test_sp;

RESET gp_enable_temp_memory_catalog;
