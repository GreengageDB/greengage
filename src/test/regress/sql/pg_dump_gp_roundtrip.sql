-- pg_dump / restore Greengage MPP round-trip regression test.
--
-- Builds a schema exercising the Greengage-specific storage and distribution
-- features, dumps it with pg_dump, drops it, restores it from the plain-SQL
-- dump, and asserts that the restored schema is byte-for-byte equivalent to the
-- original in every MPP-relevant attribute:
--   * distribution policy (hash / random / replicated) and distribution key
--   * table access method (heap / append-only row / append-only column)
--   * storage / compression reloptions (e.g. AOCO zstd)
--   * range-partition hierarchy (root + child partitions)
--   * all row data (per-table counts)
--
-- It is self-checking: a "signature" is captured before and after the round
-- trip and the two EXCEPT diffs must come back empty. This guards the ~118
-- pg_dump merge hunks (distribution clauses, AO/AOCO options, partition DDL)
-- that the PG15 merge resolved -- a regression in any of them makes a diff
-- non-empty.

CREATE SCHEMA dump_roundtrip;

-- hash-distributed heap
CREATE TABLE dump_roundtrip.t_hash (a int, b text) DISTRIBUTED BY (a);
INSERT INTO dump_roundtrip.t_hash SELECT g, 'h'||g FROM generate_series(1,500) g;

-- multi-column hash key
CREATE TABLE dump_roundtrip.t_hash2 (a int, b int, c text) DISTRIBUTED BY (a, b);
INSERT INTO dump_roundtrip.t_hash2 SELECT g, g*2, 'h2'||g FROM generate_series(1,200) g;

-- randomly-distributed heap
CREATE TABLE dump_roundtrip.t_random (a int, b text) DISTRIBUTED RANDOMLY;
INSERT INTO dump_roundtrip.t_random SELECT g, 'r'||g FROM generate_series(1,300) g;

-- replicated heap
CREATE TABLE dump_roundtrip.t_replicated (a int, b text) DISTRIBUTED REPLICATED;
INSERT INTO dump_roundtrip.t_replicated SELECT g, 'rep'||g FROM generate_series(1,100) g;

-- append-only, row-oriented
CREATE TABLE dump_roundtrip.t_ao (a int, b text)
    WITH (appendonly=true, orientation=row) DISTRIBUTED BY (a);
INSERT INTO dump_roundtrip.t_ao SELECT g, 'ao'||g FROM generate_series(1,400) g;

-- append-only, column-oriented, zstd compression
CREATE TABLE dump_roundtrip.t_aoco (a int, b text)
    WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
    DISTRIBUTED BY (a);
INSERT INTO dump_roundtrip.t_aoco SELECT g, 'aoco'||g FROM generate_series(1,400) g;

-- range-partitioned table (root + child partitions)
CREATE TABLE dump_roundtrip.t_part (id int, d int, v text)
    DISTRIBUTED BY (id)
    PARTITION BY RANGE (d) (START (0) END (30) EVERY (10));
INSERT INTO dump_roundtrip.t_part SELECT g, g % 30, 'p'||g FROM generate_series(1,600) g;

-- ---------------------------------------------------------------------------
-- Capture the BEFORE signature.
-- numsegments is intentionally kept only in the diff (not the displayed
-- listing) so the expected output is independent of the cluster size.
-- reloptions are sorted so the comparison is order-independent.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE rt_struct_before AS
SELECT c.relname,
       c.relkind,
       coalesce(p.policytype, '-') AS policytype,
       coalesce(p.distkey::text, '') AS distkey,
       coalesce(p.numsegments, -1) AS numsegments,
       coalesce(am.amname, '-') AS amname,
       coalesce(array_to_string(ARRAY(SELECT unnest(c.reloptions) ORDER BY 1), ','), '') AS reloptions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'dump_roundtrip'
LEFT JOIN gp_distribution_policy p ON p.localoid = c.oid
LEFT JOIN pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r', 'p');

CREATE TEMP TABLE rt_rows_before AS
SELECT 't_hash' AS tbl, count(*) AS n FROM dump_roundtrip.t_hash
UNION ALL SELECT 't_hash2', count(*) FROM dump_roundtrip.t_hash2
UNION ALL SELECT 't_random', count(*) FROM dump_roundtrip.t_random
UNION ALL SELECT 't_replicated', count(*) FROM dump_roundtrip.t_replicated
UNION ALL SELECT 't_ao', count(*) FROM dump_roundtrip.t_ao
UNION ALL SELECT 't_aoco', count(*) FROM dump_roundtrip.t_aoco
UNION ALL SELECT 't_part', count(*) FROM dump_roundtrip.t_part;

-- ---------------------------------------------------------------------------
-- Round trip: dump -> drop -> restore.
-- ---------------------------------------------------------------------------
\! pg_dump -n dump_roundtrip -f /tmp/dump_roundtrip.sql regression

DROP SCHEMA dump_roundtrip CASCADE;

\! psql -X -q -d regression -f /tmp/dump_roundtrip.sql > /dev/null 2>&1

-- ---------------------------------------------------------------------------
-- Capture the AFTER signature (identical queries).
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE rt_struct_after AS
SELECT c.relname,
       c.relkind,
       coalesce(p.policytype, '-') AS policytype,
       coalesce(p.distkey::text, '') AS distkey,
       coalesce(p.numsegments, -1) AS numsegments,
       coalesce(am.amname, '-') AS amname,
       coalesce(array_to_string(ARRAY(SELECT unnest(c.reloptions) ORDER BY 1), ','), '') AS reloptions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'dump_roundtrip'
LEFT JOIN gp_distribution_policy p ON p.localoid = c.oid
LEFT JOIN pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r', 'p');

CREATE TEMP TABLE rt_rows_after AS
SELECT 't_hash' AS tbl, count(*) AS n FROM dump_roundtrip.t_hash
UNION ALL SELECT 't_hash2', count(*) FROM dump_roundtrip.t_hash2
UNION ALL SELECT 't_random', count(*) FROM dump_roundtrip.t_random
UNION ALL SELECT 't_replicated', count(*) FROM dump_roundtrip.t_replicated
UNION ALL SELECT 't_ao', count(*) FROM dump_roundtrip.t_ao
UNION ALL SELECT 't_aoco', count(*) FROM dump_roundtrip.t_aoco
UNION ALL SELECT 't_part', count(*) FROM dump_roundtrip.t_part;

-- ---------------------------------------------------------------------------
-- Show the restored structure for the named tables (deterministic; partition
-- children are excluded from the listing to avoid depending on generated child
-- names, but they are still covered by the diff below).
-- ---------------------------------------------------------------------------
SELECT relname, relkind, policytype, distkey, amname, reloptions
FROM rt_struct_after
WHERE relname NOT LIKE 't_part!_%' ESCAPE '!'
ORDER BY relname;

-- Assertions: both diffs must be empty (restored == original, including the
-- partition children).
SELECT 'struct mismatch' AS what, relname, relkind, policytype, distkey, numsegments, amname, reloptions
FROM ((SELECT * FROM rt_struct_before EXCEPT SELECT * FROM rt_struct_after)
      UNION ALL
      (SELECT * FROM rt_struct_after EXCEPT SELECT * FROM rt_struct_before)) d
ORDER BY relname, relkind;

SELECT 'rows mismatch' AS what, tbl, n
FROM ((SELECT * FROM rt_rows_before EXCEPT SELECT * FROM rt_rows_after)
      UNION ALL
      (SELECT * FROM rt_rows_after EXCEPT SELECT * FROM rt_rows_before)) d
ORDER BY tbl;

-- Guard against a false "empty diff" pass (e.g. restore silently produced
-- nothing): the relation count and total row count must be as expected.
SELECT count(*) AS total_relations FROM rt_struct_after;
SELECT tbl, n FROM rt_rows_after ORDER BY tbl;

-- Cleanup.
DROP SCHEMA dump_roundtrip CASCADE;
\! rm -f /tmp/dump_roundtrip.sql
