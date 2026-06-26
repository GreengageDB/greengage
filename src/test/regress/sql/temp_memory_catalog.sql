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
