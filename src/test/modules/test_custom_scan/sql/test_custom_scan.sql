--
-- A custom scan with children of its own. test_custom_scan wraps each
-- subquery aliased "cscan" in a custom scan that passes the rows of its
-- children through, so every query here returns the rows it would
-- return without the provider.
--
-- setup has loaded the provider in every new backend.
SELECT 'test_custom_scan' = ANY (string_to_array(current_setting('session_preload_libraries'), ','))
  AS loaded;

-- ORCA does not ask custom scan providers for paths.
SET optimizer = off;

CREATE TABLE cs_a (a int, b int) DISTRIBUTED BY (a);
CREATE TABLE cs_b (a int, b int) DISTRIBUTED BY (a);
INSERT INTO cs_a SELECT i, i % 10 FROM generate_series(1, 100) i;
INSERT INTO cs_b SELECT i, i % 10 FROM generate_series(1, 100) i;
ANALYZE cs_a, cs_b;

-- A plan with a custom scan, which keeps a value node in custom_private.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
SELECT count(*), sum(a), sum(b) FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;

-- A custom scan turns targeted dispatch off: the query goes to every
-- segment.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5 OFFSET 0) cscan;
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5 OFFSET 0) cscan;

-- A Motion under the custom scan.
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM (SELECT x.a FROM cs_a x JOIN cs_b y ON x.a = y.b OFFSET 0) cscan;
SELECT count(*)
FROM (SELECT x.a FROM cs_a x JOIN cs_b y ON x.a = y.b OFFSET 0) cscan;

-- EXPLAIN ANALYZE gathers from the segments the statistics of the nodes
-- under the custom scan. The scan, on the coordinator, reads its child to
-- the end and is shut down once, when the plan ends.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*) FROM (SELECT a FROM cs_a LIMIT 1000) cscan;

-- A LIMIT that stops the scan also shuts it down, so the provider can
-- release what it holds: twice in all. All of it runs on the
-- coordinator, so how far the segments got does not show.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM (SELECT g FROM generate_series(1, 100) g LIMIT 1000) cscan
LIMIT 1;

-- A LIMIT above the custom scan stops it and the Motion under it.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM
  (SELECT * FROM
     (SELECT x.a FROM cs_a x JOIN cs_b y ON x.a = y.b OFFSET 0) cscan
   LIMIT 5) s;
SELECT count(*) FROM
  (SELECT * FROM
     (SELECT x.a FROM cs_a x JOIN cs_b y ON x.a = y.b OFFSET 0) cscan
   LIMIT 5) s;

-- A SubPlan under the custom scan gets the Motion it needs.
EXPLAIN (COSTS OFF)
SELECT sum(s) FROM
  (SELECT (SELECT y.a FROM cs_b y WHERE y.b = x.b ORDER BY y.a DESC LIMIT 1) s
   FROM cs_a x OFFSET 0) cscan;
SELECT sum(s) FROM
  (SELECT (SELECT y.a FROM cs_b y WHERE y.b = x.b ORDER BY y.a DESC LIMIT 1) s
   FROM cs_a x OFFSET 0) cscan;

-- A CTE shared under the custom scan: one of its scans produces it.
SET gp_cte_sharing = on;
EXPLAIN (COSTS OFF)
WITH c AS (SELECT a, b FROM cs_a)
SELECT count(*) FROM (SELECT x.a FROM c x JOIN c y ON x.a = y.a OFFSET 0) cscan;
WITH c AS (SELECT a, b FROM cs_a)
SELECT count(*) FROM (SELECT x.a FROM c x JOIN c y ON x.a = y.a OFFSET 0) cscan;

-- A CTE shared between slices: one branch reads it in the custom scan's
-- slice, the other below a Motion, in another slice. The scan runs its
-- last child first and says so with CUSTOMPATH_GP_LAST_CHILD_FIRST, so
-- the CTE is produced in the slice that runs first. Produced in the
-- other one, it would never be: each slice would wait for the other.
SET test_custom_scan.last_child_first = on;
EXPLAIN (COSTS OFF)
WITH c AS (SELECT a, b FROM cs_a)
SELECT count(*), sum(x)
FROM (SELECT a AS x FROM c UNION ALL SELECT b FROM c GROUP BY b) cscan;
WITH c AS (SELECT a, b FROM cs_a)
SELECT count(*), sum(x)
FROM (SELECT a AS x FROM c UNION ALL SELECT b FROM c GROUP BY b) cscan;
RESET test_custom_scan.last_child_first;
RESET gp_cte_sharing;

-- A custom scan computes expressions only with
-- CUSTOMPATH_SUPPORT_PROJECTION; without it, a Result above it does.
EXPLAIN (COSTS OFF)
SELECT a + b FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
SET test_custom_scan.projection = on;
EXPLAIN (COSTS OFF)
SELECT a + b FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
SELECT sum(a + b) FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
RESET test_custom_scan.projection;

-- A custom scan with CUSTOMPATH_GP_MEMORY_INTENSIVE gets a share of the
-- query's memory, as a Hash or a Sort does, and shows it in EXPLAIN.
-- Without the flag, it would get the small share of an operator that
-- needs none. Without resource groups, the scan gets the 10 MB of
-- statement_mem less the fixed 100 kB of each of the two other nodes.
SET statement_mem = '10MB';
SET test_custom_scan.memory_intensive = on;
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
RESET test_custom_scan.memory_intensive;
RESET statement_mem;

DROP TABLE cs_a, cs_b;
