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

-- A custom scan that reads rows only from its children says so with
-- CUSTOMPATH_GP_CHILD_ROWS_ONLY, and targeted dispatch follows them, as
-- it follows the children of an Agg: the query goes to the one segment
-- with a = 5.
SET test_custom_scan.child_rows_only = on;
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5 OFFSET 0) cscan;
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5 OFFSET 0) cscan;

-- The branches of a UNION ALL are children too, and both go to the one
-- segment with a = 5.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5
               UNION ALL SELECT a, b FROM cs_b WHERE a = 5) cscan;
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5
               UNION ALL SELECT a, b FROM cs_b WHERE a = 5) cscan;

-- The children set the segments as the children of any node do: branches
-- on two segments send the query to both, and a branch without a
-- restriction, or with a Motion that brings it rows, to every segment.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5
               UNION ALL SELECT a, b FROM cs_b WHERE a = 7) cscan;
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5
               UNION ALL SELECT a, b FROM cs_b WHERE a = 7) cscan;
EXPLAIN (COSTS OFF)
SELECT count(*), sum(a) FROM (SELECT a, b FROM cs_a WHERE a = 5
                              UNION ALL SELECT a, b FROM cs_b) cscan;
SELECT count(*), sum(a) FROM (SELECT a, b FROM cs_a WHERE a = 5
                              UNION ALL SELECT a, b FROM cs_b) cscan;
EXPLAIN (COSTS OFF)
SELECT count(*), sum(a) FROM (SELECT a, b FROM cs_a WHERE a = 5
                              UNION ALL SELECT b, count(*)::int FROM cs_b GROUP BY b) cscan;
SELECT count(*), sum(a) FROM (SELECT a, b FROM cs_a WHERE a = 5
                              UNION ALL SELECT b, count(*)::int FROM cs_b GROUP BY b) cscan;
RESET test_custom_scan.child_rows_only;

-- Without the flag, the provider may read rows itself, from segments the
-- core cannot tell, so the query goes to every segment.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5 OFFSET 0) cscan;
SELECT * FROM (SELECT a, b FROM cs_a WHERE a = 5 OFFSET 0) cscan;

-- A table aliased "cscan" gets a custom scan of the table, which keeps its
-- child, the table's Seq Scan, in lefttree. The scan returns only rows
-- that pass the table's restrictions, so targeted dispatch follows them:
-- the query goes to the one segment with a = 5.
EXPLAIN (COSTS OFF)
SELECT * FROM cs_a cscan WHERE a = 5;
SELECT * FROM cs_a cscan WHERE a = 5;

-- A provider that reads the rows of a table from elsewhere, laid out
-- otherwise, says so in the path's locus: with test_custom_scan.strewn,
-- the scan says its rows are strewn, and the query goes to every segment.
-- So it does with CUSTOMPATH_GP_CHILD_ROWS_ONLY, which counts only for a
-- custom scan that names no table.
SET test_custom_scan.strewn = on;
EXPLAIN (COSTS OFF)
SELECT * FROM cs_a cscan WHERE a = 5;
SELECT * FROM cs_a cscan WHERE a = 5;
SET test_custom_scan.child_rows_only = on;
EXPLAIN (COSTS OFF)
SELECT * FROM cs_a cscan WHERE a = 5;
RESET test_custom_scan.child_rows_only;
RESET test_custom_scan.strewn;

-- A HashedOJ locus on the table's key, as after an outer join, may put a
-- NULL key on any segment: the query for a NULL key goes to every segment.
SET test_custom_scan.outer_join_locus = on;
EXPLAIN (COSTS OFF)
SELECT * FROM cs_a cscan WHERE a IS NULL;
RESET test_custom_scan.outer_join_locus;

-- The parent of an inheritance tree gets a custom scan of the table too.
-- Its child is the tree's Append, whose locus the scan takes. Where every
-- table of the tree is distributed by the parent's key, the rows with
-- a = 5 lie on one segment in all of them, and the query goes there.
CREATE TABLE cs_p (a int, b int) DISTRIBUTED BY (a);
CREATE TABLE cs_pa () INHERITS (cs_p) DISTRIBUTED BY (a);
INSERT INTO cs_p SELECT i, i FROM generate_series(1, 10) i;
INSERT INTO cs_pa SELECT i, i + 100 FROM generate_series(1, 10) i;
EXPLAIN (COSTS OFF)
SELECT * FROM cs_p cscan WHERE a = 5;
SELECT * FROM cs_p cscan WHERE a = 5 ORDER BY b;

-- A child distributed by another key makes the Append's locus strewn: its
-- rows with a = 5 may lie on any segment, and the query goes to every
-- segment. The test cannot fail on the scan's own rule alone: the scan
-- of the child goes to every segment anyway.
CREATE TABLE cs_pb () INHERITS (cs_p) DISTRIBUTED BY (b);
INSERT INTO cs_pb SELECT i, i + 200 FROM generate_series(1, 10) i;
EXPLAIN (COSTS OFF)
SELECT * FROM cs_p cscan WHERE a = 5;
SELECT * FROM cs_p cscan WHERE a = 5 ORDER BY b;
DROP TABLE cs_pa, cs_pb, cs_p;

-- A partitioned table gets a custom scan of the table too, over the
-- Append of its partitions. The planner drops the paths of a partitioned
-- table that is the whole of FROM and makes its Append anew
-- (apply_scanjoin_target_to_paths), so these queries join it with cs_b. A
-- partition is always distributed as its parent, so the query goes to the
-- one segment with a = 5.
CREATE TABLE cs_q (a int, b int) DISTRIBUTED BY (a) PARTITION BY RANGE (b);
CREATE TABLE cs_q1 PARTITION OF cs_q FOR VALUES FROM (0) TO (100);
CREATE TABLE cs_q2 PARTITION OF cs_q FOR VALUES FROM (100) TO (200);
INSERT INTO cs_q SELECT i, i FROM generate_series(1, 10) i;
INSERT INTO cs_q SELECT i, i + 100 FROM generate_series(1, 10) i;
EXPLAIN (COSTS OFF)
SELECT * FROM cs_q cscan JOIN cs_b USING (a) WHERE a = 5;
SELECT * FROM cs_q cscan JOIN cs_b USING (a) WHERE a = 5 ORDER BY cscan.b;

-- An external partition may return its rows on any segment, which makes
-- the Append's locus strewn, and the query goes to every segment. Each of
-- the three segments runs the command, so the partition returns three
-- rows. As above, the scan of the partition goes to every segment anyway.
CREATE EXTERNAL WEB TABLE cs_qx (a int, b int)
  EXECUTE 'echo 5,250' ON ALL FORMAT 'csv';
ALTER TABLE cs_q ATTACH PARTITION cs_qx FOR VALUES FROM (200) TO (300);
EXPLAIN (COSTS OFF)
SELECT * FROM cs_q cscan JOIN cs_b USING (a) WHERE a = 5;
SELECT * FROM cs_q cscan JOIN cs_b USING (a) WHERE a = 5 ORDER BY cscan.b;
DROP TABLE cs_q;

-- A scan of a table that returns no columns is not wrapped.
EXPLAIN (COSTS OFF)
SELECT count(*) FROM cs_a cscan;

-- Nor is a table in DML, whose rows EvalPlanQual may recheck through the
-- scan, which the test provider does not implement.
EXPLAIN (COSTS OFF)
DELETE FROM cs_b cscan WHERE a = 5;

-- A Motion under the custom scan.
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM (SELECT x.a FROM cs_a x JOIN cs_b y ON x.a = y.b OFFSET 0) cscan;
SELECT count(*)
FROM (SELECT x.a FROM cs_a x JOIN cs_b y ON x.a = y.b OFFSET 0) cscan;

-- EXPLAIN ANALYZE gathers from the segments the statistics of the nodes
-- under the custom scan. The scan, on the coordinator, reads its child to
-- the end, is never squelched, and is shut down once, when the plan ends.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*) FROM (SELECT a FROM cs_a LIMIT 1000) cscan;

-- A LIMIT that stops the scan squelches it once and does not shut it
-- down: a rescan may follow a squelch, and a provider may release in its
-- shutdown callback what a rescan needs. The scan is shut down once, when
-- the plan ends; only these counts show, as all of it runs on the
-- coordinator.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM (SELECT g FROM generate_series(1, 100) g LIMIT 1000) cscan
LIMIT 1;

-- A scan with the squelch callback squelches its children itself, as a
-- provider must: the outer of two scans squelches the inner one.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM
  (SELECT * FROM (SELECT g FROM generate_series(1, 100) g OFFSET 0) cscan
   OFFSET 0) cscan
LIMIT 1;

-- A scan without the squelch callback, aliased "cscan_plain", counts no
-- squelch, and the core squelches its children: the inner scan counts
-- one.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM
  (SELECT * FROM (SELECT g FROM generate_series(1, 100) g OFFSET 0) cscan
   OFFSET 0) cscan_plain
LIMIT 1;

-- A Nested Loop squelches itself each time it runs out of rows, and the
-- SubPlan rescans it for the next row of t. The scan is squelched each
-- time, ten times in all, but not shut down: all of it runs on the
-- coordinator, where EXPLAIN shows that the scan was shut down once, when
-- the plan ended. Without a Material, which would rescan in its place,
-- the scan is the outer side of the Nested Loop.
SET enable_material = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT sum(c) FROM
  (SELECT (SELECT count(*)
           FROM (SELECT g FROM generate_series(1, 3) g OFFSET 0) cscan
           JOIN generate_series(1, t.a) h ON h <= cscan.g) c
   FROM generate_series(1, 10) t(a)) s;
SELECT sum(c) FROM
  (SELECT (SELECT count(*)
           FROM (SELECT g FROM generate_series(1, 3) g OFFSET 0) cscan
           JOIN generate_series(1, t.a) h ON h <= cscan.g) c
   FROM generate_series(1, 10) t(a)) s;

-- A buffered scan reads its child into a buffer first, as a Material
-- does. Each squelch releases the buffer, and the scan fills it again
-- after the next rescan: ten times each, and the query returns the same
-- sum.
SET test_custom_scan.buffer = on;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT sum(c) FROM
  (SELECT (SELECT count(*)
           FROM (SELECT g FROM generate_series(1, 3) g OFFSET 0) cscan
           JOIN generate_series(1, t.a) h ON h <= cscan.g) c
   FROM generate_series(1, 10) t(a)) s;
SELECT sum(c) FROM
  (SELECT (SELECT count(*)
           FROM (SELECT g FROM generate_series(1, 3) g OFFSET 0) cscan
           JOIN generate_series(1, t.a) h ON h <= cscan.g) c
   FROM generate_series(1, 10) t(a)) s;
RESET test_custom_scan.buffer;
RESET enable_material;

-- A LIMIT above the custom scan squelches it on the segments, and the
-- squelch passes through the scan to the Motion under it without an
-- error. The rows are too few for the test to tell whether the Motion
-- stopped.
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
-- slice, the other below a Motion, in another slice. The scan reads the
-- last branch first and lists its children in that order, so the CTE is
-- produced in the slice that runs first. Produced in the other one, it
-- would never be: each slice would wait for the other, until
-- statement_timeout cancels the query.
SET test_custom_scan.last_branch_first = on;
EXPLAIN (COSTS OFF)
WITH c AS (SELECT a, b FROM cs_a)
SELECT count(*), sum(x)
FROM (SELECT a AS x FROM c UNION ALL SELECT b FROM c GROUP BY b) cscan;
SET statement_timeout = '15s';
WITH c AS (SELECT a, b FROM cs_a)
SELECT count(*), sum(x)
FROM (SELECT a AS x FROM c UNION ALL SELECT b FROM c GROUP BY b) cscan;
RESET statement_timeout;
RESET test_custom_scan.last_branch_first;
RESET gp_cte_sharing;

-- A custom scan computes expressions only with
-- CUSTOMPATH_SUPPORT_PROJECTION; without it, a Result above it does.
EXPLAIN (COSTS OFF)
SELECT a + b FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
SET test_custom_scan.projection = on;
EXPLAIN (COSTS OFF)
SELECT a + b FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
-- The scan computes a + b itself under the outer aggregate, which would
-- take a and b and add them on its own if the sum were of a + b.
EXPLAIN (COSTS OFF, VERBOSE)
SELECT sum(x) FROM (SELECT a + b AS x FROM (SELECT a, b FROM cs_a OFFSET 0) cscan OFFSET 0) s;
SELECT sum(x) FROM (SELECT a + b AS x FROM (SELECT a, b FROM cs_a OFFSET 0) cscan OFFSET 0) s;
RESET test_custom_scan.projection;

-- A custom scan with CUSTOMPATH_GP_MEMORY_INTENSIVE gets a share of the
-- query's memory, as a Hash or a Sort does, and shows it in EXPLAIN.
-- Without the flag, it would get the small share of an operator that
-- needs none. The settings below give the query 10 MB, which eager free
-- shares out with 100 kB for each operator that needs none, whichever
-- resource manager the cluster runs: the scan gets the 10 MB less the
-- 100 kB of each of the two other nodes.
SET statement_mem = '10MB';
SET gp_resgroup_memory_query_fixed_mem = '10MB';
SET gp_resqueue_memory_policy = 'eager_free';
SET gp_resgroup_memory_policy = 'eager_free';
SET gp_resqueue_memory_policy_auto_fixed_mem = '100kB';
SET gp_resgroup_memory_policy_auto_fixed_mem = '100kB';
SET test_custom_scan.memory_intensive = on;
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
-- Eager free counts such a scan as it counts a WindowAgg: the scan takes
-- memory only once rows reach it, when the Sort under it has read its
-- input. So the scan above the Sort and the operators under the Sort use
-- the same memory in turn: the scan above the Sort, the Sort and the scan
-- under it get half of it each, less the fixed shares of the operators
-- that need none.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM (SELECT a, b FROM cs_a OFFSET 0) cscan
               ORDER BY a OFFSET 0) cscan;
-- A scan with CUSTOMPATH_GP_MEMORY_OVERLAPS_CHILDREN needs its memory at
-- the same time as the operators under it, as a hash join keeps its
-- Hash's table while it reads its outer side. The flag holds the bit of
-- CUSTOMPATH_GP_MEMORY_INTENSIVE, so the scan gets a share without the
-- other setting. The scan above the Sort now shares the memory with the
-- Sort and the scan under it, which get a third of it each, less the
-- fixed share of the Seq Scan.
RESET test_custom_scan.memory_intensive;
SET test_custom_scan.memory_overlaps_children = on;
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM (SELECT a, b FROM cs_a OFFSET 0) cscan
               ORDER BY a OFFSET 0) cscan;
-- A scan with both flags is counted as one with
-- CUSTOMPATH_GP_MEMORY_OVERLAPS_CHILDREN alone.
SET test_custom_scan.memory_intensive = on;
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM (SELECT a, b FROM cs_a OFFSET 0) cscan
               ORDER BY a OFFSET 0) cscan;
RESET test_custom_scan.memory_intensive;
-- With no blocking operator under it, the scan's own group splits the
-- memory with none, and the scan gets what it gets without the flag: the
-- 10 MB less the 100 kB of each of the two other nodes.
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a OFFSET 0) cscan;
-- On the outer side of a hash join, the scan and the Hash split the
-- memory of their group, half each less the fixed shares of the three
-- operators that need none, as they do without the flag. The scan's own
-- group lies beside the Hash's, which the join keeps while it reads the
-- scan's rows, and does not lower that share.
SET enable_nestloop = off;
SET enable_mergejoin = off;
EXPLAIN (COSTS OFF)
SELECT * FROM (SELECT a, b FROM cs_a OFFSET 0) cscan JOIN cs_b USING (a);
RESET enable_mergejoin;
RESET enable_nestloop;
RESET test_custom_scan.memory_overlaps_children;
RESET gp_resgroup_memory_policy_auto_fixed_mem;
RESET gp_resqueue_memory_policy_auto_fixed_mem;
RESET gp_resgroup_memory_policy;
RESET gp_resqueue_memory_policy;
RESET gp_resgroup_memory_query_fixed_mem;
RESET statement_mem;

DROP TABLE cs_a, cs_b;
