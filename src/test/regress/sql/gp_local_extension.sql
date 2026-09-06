-- Exercises the local (QD-only/coordinator-only) extension mechanism
-- (the gg_local control-file flag) end to end, using the gp_local_test
-- sample extension in contrib/.

-- This test deliberately leaves the extension installed at the end (see
-- the closing comment below), so guard against a prior run's leftover
-- state to stay safely re-runnable against the same database.
-- start_ignore
DROP EXTENSION IF EXISTS gp_local_test;
-- end_ignore

CREATE EXTENSION gp_local_test;

-- Every table the extension's install script created must have no
-- distribution policy at all - that's the defining property of a
-- QD-only table.
SELECT count(*) FROM gp_distribution_policy
 WHERE localoid IN ('ext_local_test.state'::regclass,
                     'ext_local_test.settings'::regclass);

-- QD-only tables live only on the coordinator (segment id -1), never
-- dispatched to segments. EXPLAIN confirms the plan itself has no Motion
-- node moving data from segments - it's a plain local scan.
EXPLAIN (COSTS OFF) SELECT gp_segment_id, code, value FROM ext_local_test.state;
SELECT gp_segment_id, code, value FROM ext_local_test.state;

-- settings is registered via pg_extension_config_dump() with an empty
-- (unfiltered) condition - the standard "config table" convention.
SELECT extconfig IS NOT NULL AS has_config_tables, extcondition
  FROM pg_extension WHERE extname = 'gp_local_test';
EXPLAIN (COSTS OFF) SELECT key, value FROM ext_local_test.settings ORDER BY key;
SELECT key, value FROM ext_local_test.settings ORDER BY key;

-- Exercise the extension's functions against the QD-only table, neither
-- carrying any EXECUTE ON clause. Again, no Motion in the plan.
EXPLAIN (COSTS OFF) SELECT ext_local_test.bump_counter('counter');
SELECT ext_local_test.bump_counter('counter');
SELECT ext_local_test.bump_counter('counter');
EXPLAIN (COSTS OFF) SELECT ext_local_test.current_counter('counter');
SELECT ext_local_test.current_counter('counter');
EXPLAIN (COSTS OFF) SELECT code, value FROM ext_local_test.state;
SELECT code, value FROM ext_local_test.state;

-- Schema evolution: ALTER EXTENSION UPDATE must be able to add a column,
-- a new table, and replace a function's language (SQL -> plpgsql) on a
-- QD-only table, and the table must remain QD-only afterward.
ALTER EXTENSION gp_local_test UPDATE TO '1.1';

SELECT count(*) FROM gp_distribution_policy
 WHERE localoid IN ('ext_local_test.state'::regclass,
                     'ext_local_test.history'::regclass);

EXPLAIN (COSTS OFF) SELECT ext_local_test.bump_counter('counter');
SELECT ext_local_test.bump_counter('counter');
EXPLAIN (COSTS OFF) SELECT code, old_value, new_value FROM ext_local_test.history ORDER BY id;
SELECT code, old_value, new_value FROM ext_local_test.history ORDER BY id;

EXPLAIN (COSTS OFF) SELECT ext_local_test.reset_counter('counter');
SELECT ext_local_test.reset_counter('counter');
EXPLAIN (COSTS OFF) SELECT code, value FROM ext_local_test.state;
SELECT code, value FROM ext_local_test.state;

-- And back down again.
ALTER EXTENSION gp_local_test UPDATE TO '1.0';
EXPLAIN (COSTS OFF) SELECT code, value FROM ext_local_test.state;
SELECT code, value FROM ext_local_test.state;

-- Drop and re-create from scratch: the schema/tables/functions must not
-- linger past DROP EXTENSION, and a fresh CREATE EXTENSION afterward must
-- produce a fully working, QD-only extension again, not inherit any state
-- from the dropped one.
DROP EXTENSION gp_local_test;

CREATE EXTENSION gp_local_test;

SELECT count(*) FROM gp_distribution_policy
 WHERE localoid IN ('ext_local_test.state'::regclass,
                     'ext_local_test.settings'::regclass);

EXPLAIN (COSTS OFF) SELECT gp_segment_id, code, value FROM ext_local_test.state;
SELECT gp_segment_id, code, value FROM ext_local_test.state;
EXPLAIN (COSTS OFF) SELECT ext_local_test.bump_counter('counter');
SELECT ext_local_test.bump_counter('counter');
EXPLAIN (COSTS OFF) SELECT ext_local_test.current_counter('counter');
SELECT ext_local_test.current_counter('counter');

-- Deliberately left installed (not dropped) here: gpcheckcat's normal
-- installcheck-world pass and the pg_upgrade test (test_gpdb.sh), which
-- both examine the "regression" database as it stands at the end of this
-- suite, then get to exercise a real QD-only extension's catalog state
-- and pg_upgrade path too, not just this test's own assertions.
