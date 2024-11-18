CREATE TABLE ao_table (id int,
 fname text,
 lname text,
 address1 text,
 address2 text,
 city text,
 state text,
 zip text)
WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (id);
CREATE INDEX ON ao_table (id);

-- Bunch insert into AOCS table that affects auxiliary relations
INSERT INTO ao_table (id, fname, lname, address1, address2, city, state, zip)
SELECT i, 'Jon_' || i, 'Roberts_' || i, i || ' Main Street', 'Apartment ' || i, 'New York', 'NY', i::text
FROM generate_series(1, 100) AS i;

-- Wait for current backend to send locally accumulated statistics to collector
SELECT pg_sleep(1);

-- Wait for stats collector to update accumulated statistics
DO $$
DECLARE
    start_time timestamptz := clock_timestamp();
    updated bool;
BEGIN
    FOR i IN 1 .. 300 LOOP
        SELECT (sum(seq_scan) > 0) INTO updated
        FROM gp_dist_random('pg_stat_user_tables') pgsut
        JOIN pg_appendonly pgao ON pgsut.relid = pgao.segrelid
        WHERE pgao.relid = 'ao_table'::regclass;

        EXIT WHEN updated;

        PERFORM pg_sleep(0.1);
        PERFORM pg_stat_clear_snapshot();
    END LOOP;
    RAISE log 'wait_for_stats delayed % seconds',
        EXTRACT(epoch FROM clock_timestamp() - start_time);
END;
$$ LANGUAGE plpgsql;

-- Check statistics on auxiliary relations
WITH aux_relids_row AS (
    SELECT
        segrelid::regclass, blkdirrelid::regclass, visimaprelid::regclass
    FROM pg_appendonly
    WHERE relid = 'ao_table'::regclass
), aux_relids AS (
    SELECT segrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT blkdirrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT visimaprelid AS relid FROM aux_relids_row
), sum_relstats AS (
    SELECT
        regexp_replace(relname, '_[0-9]+$', '') AS relation,
        sum(seq_scan) AS seq_scan,
        sum(seq_tup_read) AS seq_tup_read,
        sum(idx_scan) AS idx_scan,
        sum(idx_tup_fetch) AS idx_tup_fetch,
        sum(n_tup_ins) AS n_tup_ins,
        sum(n_tup_upd) AS n_tup_upd,
        sum(n_tup_del) AS n_tup_del,
        sum(n_tup_hot_upd) AS n_tup_hot_upd,
        sum(n_live_tup) AS n_live_tup,
        sum(n_dead_tup) AS n_dead_tup,
        sum(n_mod_since_analyze) AS n_mod_since_analyze
    FROM gp_dist_random('pg_stat_user_tables')
    WHERE relid IN (SELECT relid FROM aux_relids)
    GROUP BY relation
)
SELECT
    relation,
    seq_scan > 0 AS seq_scan,
    CASE WHEN relation = 'pg_aocsseg' THEN seq_tup_read > 0
         ELSE seq_tup_read = 0
    END AS seq_tup_read,
    CASE WHEN relation = 'pg_aocsseg' THEN idx_scan IS null
         ELSE idx_scan > 0
    END AS idx_scan,
    CASE WHEN relation = 'pg_aocsseg' THEN idx_tup_fetch IS null
         ELSE idx_tup_fetch = 0
    END AS idx_tup_fetch,
    CASE WHEN relation = 'pg_aovisimap' THEN n_tup_ins = 0
         ELSE n_tup_ins > 0
    END AS n_tup_ins,
    CASE WHEN relation = 'pg_aocsseg' THEN n_tup_upd > 0
         ELSE n_tup_upd = 0
    END AS n_tup_upd,
    n_tup_del = 0,
    CASE WHEN relation = 'pg_aocsseg' THEN n_tup_hot_upd > 0
         ELSE n_tup_hot_upd = 0
    END AS n_tup_hot_upd,
    CASE WHEN relation = 'pg_aovisimap' THEN n_live_tup = 0
         ELSE n_live_tup > 0
    END AS n_live_tup,
    CASE WHEN relation = 'pg_aocsseg' THEN n_dead_tup > 0
         ELSE n_dead_tup = 0
    END AS n_dead_tup,
    CASE WHEN relation = 'pg_aovisimap' THEN n_mod_since_analyze = 0
         ELSE n_mod_since_analyze > 0
    END AS n_mod_since_analyze
FROM sum_relstats
ORDER BY relation;

WITH aux_relids_row AS (
    SELECT
        segrelid::regclass, blkdirrelid::regclass, visimaprelid::regclass
    FROM pg_appendonly
    WHERE relid = 'ao_table'::regclass
), aux_relids AS (
    SELECT segrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT blkdirrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT visimaprelid AS relid FROM aux_relids_row
), sum_relstats AS (
    SELECT
        regexp_replace(relname, '_[0-9]+$', '') AS relation,
        sum(heap_blks_read) AS heap_blks_read,
        sum(heap_blks_hit) AS heap_blks_hit,
        sum(idx_blks_read) AS idx_blks_read,
        sum(idx_blks_hit) AS idx_blks_hit
    FROM gp_dist_random('pg_statio_user_tables') pgs_ut
    WHERE relid IN (SELECT relid FROM aux_relids)
    GROUP BY relation
)
SELECT
    relation,
    CASE WHEN relation = 'pg_aovisimap' THEN heap_blks_read + heap_blks_hit = 0
         ELSE heap_blks_read + heap_blks_hit > 0
    END AS heap_blks_access,
    CASE WHEN relation = 'pg_aocsseg' THEN idx_blks_read + idx_blks_hit IS null
         ELSE idx_blks_read + idx_blks_hit > 0
    END AS idx_blks_access
FROM sum_relstats
ORDER BY relation;

WITH aux_relids_row AS (
    SELECT
        segrelid::regclass, blkdirrelid::regclass, visimaprelid::regclass
    FROM pg_appendonly
    WHERE relid = 'ao_table'::regclass
), aux_relids AS (
    SELECT segrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT blkdirrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT visimaprelid AS relid FROM aux_relids_row
), sum_indstats AS (
    SELECT
        regexp_replace(indexrelname, '_[0-9]+', '') AS index,
        sum(idx_scan) AS idx_scan,
        sum(idx_tup_read) AS idx_tup_read,
        sum(idx_tup_fetch) AS idx_tup_fetch
    FROM gp_dist_random('pg_stat_user_indexes')
    WHERE relid IN (SELECT relid FROM aux_relids)
    GROUP BY index
)
SELECT
    index,
    idx_scan > 0 AS idx_scan,
    idx_tup_read = 0 AS idx_tup_read,
    idx_tup_fetch = 0 AS idx_tup_fetch
FROM sum_indstats
ORDER BY index;

WITH aux_relids_row AS (
    SELECT
        segrelid::regclass, blkdirrelid::regclass, visimaprelid::regclass
    FROM pg_appendonly
    WHERE relid = 'ao_table'::regclass
), aux_relids AS (
    SELECT segrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT blkdirrelid AS relid FROM aux_relids_row
    UNION ALL
    SELECT visimaprelid AS relid FROM aux_relids_row
), sum_indstats AS (
    SELECT
        regexp_replace(indexrelname, '_[0-9]+', '') AS index,
        sum(idx_blks_read) AS idx_blks_read,
        sum(idx_blks_hit) AS idx_blks_hit
    FROM gp_dist_random('pg_statio_user_indexes')
    WHERE relid IN (SELECT relid FROM aux_relids)
    GROUP BY index
)
SELECT
    index,
    idx_blks_hit + idx_blks_read > 0 AS idx_blks_access
FROM sum_indstats
ORDER BY index;

-- Drop AO and aux tables
DROP TABLE ao_table;

-- Test the fix: Adding data to a partitioned AO table using `COPY FROM`  
-- does not change the `tupcount` in the segment table on the QD.
CREATE OR REPLACE FUNCTION get_total_tupcount(table_oid oid)
RETURNS bigint AS $$
DECLARE
    aoseg_table_schema text;
    aoseg_table_name text;
    tupcount_result bigint;
BEGIN
    -- Get the AO segment table name for the given partition
    SELECT n.nspname, c.relname INTO aoseg_table_schema, aoseg_table_name
    FROM pg_appendonly AS a
        JOIN pg_class AS c ON c.oid = a.segrelid
        JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE a.relid = table_oid;

    -- Sum tupcount from all segments
    EXECUTE format('SELECT SUM(tupcount) FROM %I.%I',
                    aoseg_table_schema, aoseg_table_name)
        INTO tupcount_result;

    RETURN tupcount_result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_tupcounts_for_partitions(input_name text)
RETURNS TABLE (table_name name, tupcount bigint)
AS $$
DECLARE
    main_table_oid oid;
    main_table_name name;
    schema_name name;
BEGIN
    -- Determine oid, schema and name of input table
    SELECT c.oid, c.relname, n.nspname INTO main_table_oid, main_table_name, schema_name
    FROM pg_class AS c JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE c.oid = input_name::regclass;

    -- Return main table name and tupcount
    RETURN QUERY SELECT main_table_name, get_total_tupcount(main_table_oid);

    -- Return partition name and tupcount
    RETURN QUERY
    SELECT p.partitiontablename, get_total_tupcount(c.oid)
    FROM pg_partitions AS p
    JOIN pg_class AS c ON c.relname = p.partitiontablename
    JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE p.tablename = main_table_name
        AND p.schemaname = schema_name
        AND n.nspname = schema_name
    ORDER BY p.partitiontablename;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE t_row (id int)
    WITH (appendoptimized=true, orientation=row) DISTRIBUTED BY (id);

CREATE TABLE t_col (id int)
    WITH (appendoptimized=true, orientation=column) DISTRIBUTED BY (id);

CREATE TABLE t_row_lev1 (id int, data int)
    WITH (appendoptimized=true, orientation=row) DISTRIBUTED BY (id)
    PARTITION BY RANGE (data)
    (PARTITION p1 START (1) INCLUSIVE END (4) INCLUSIVE,
     PARTITION p2 START (5) INCLUSIVE END (8) INCLUSIVE);

CREATE TABLE t_col_lev1 (id int, data int)
    WITH (appendoptimized=true, orientation=column) DISTRIBUTED BY (id)
    PARTITION BY RANGE (data)
    (PARTITION p1 START (1) INCLUSIVE END (4) INCLUSIVE,
     PARTITION p2 START (5) INCLUSIVE END (8) INCLUSIVE);

CREATE TABLE t_row_lev2 (id int, data1 int, data2 int)
    WITH (appendoptimized=true, orientation=row) DISTRIBUTED BY (id)
    PARTITION BY RANGE (data1)
        SUBPARTITION BY LIST (data2) SUBPARTITION TEMPLATE 
        (SUBPARTITION sp1 VALUES (1, 2), SUBPARTITION sp2 VALUES (3, 4))
    (PARTITION p1 START (1) INCLUSIVE END (4) INCLUSIVE,
     PARTITION p2 START (5) INCLUSIVE END (8) INCLUSIVE);

CREATE TABLE t_col_lev2 (id int, data1 int, data2 int)
    WITH (appendoptimized=true, orientation=column) DISTRIBUTED BY (id)
    PARTITION BY RANGE (data1)
        SUBPARTITION BY LIST (data2) SUBPARTITION TEMPLATE 
        (SUBPARTITION sp1 VALUES (1, 2), SUBPARTITION sp2 VALUES (3, 4))
    (PARTITION p1 START (1) INCLUSIVE END (4) INCLUSIVE,
     PARTITION p2 START (5) INCLUSIVE END (8) INCLUSIVE);

-- Test copying data to an empty table
COPY t_row (id) FROM stdin;
1
2
3
4
5
\.
COPY t_col (id) FROM stdin;
1
2
3
4
5
\.
COPY t_row_lev1 (id, data) FROM stdin;
1	1
2	2
3	3
4	4
5	5
\.
COPY t_col_lev1 (id, data) FROM stdin;
1	1
2	2
3	3
4	4
5	5
\.
COPY t_row_lev2 (id, data1, data2) FROM stdin;
1	1	1
2	2	2
3	3	3
4	4	4
5	5	1
\.
COPY t_col_lev2 (id, data1, data2) FROM stdin;
1	1	1
2	2	2
3	3	3
4	4	4
5	5	1
\.

SELECT * FROM get_tupcounts_for_partitions('t_row');
SELECT * FROM get_tupcounts_for_partitions('t_col');
SELECT * FROM get_tupcounts_for_partitions('t_row_lev1');
SELECT * FROM get_tupcounts_for_partitions('t_col_lev1');
SELECT * FROM get_tupcounts_for_partitions('t_row_lev2');
SELECT * FROM get_tupcounts_for_partitions('t_col_lev2');

-- Test copying data to a non-empty table
COPY t_row (id) FROM stdin;
6
7
8
\.
COPY t_col (id) FROM stdin;
6
7
8
\.
COPY t_row_lev1 (id, data) FROM stdin;
6	6
7	7
8	8
\.
COPY t_col_lev1 (id, data) FROM stdin;
6	6
7	7
8	8
\.
COPY t_row_lev2 (id, data1, data2) FROM stdin;
6	6	2
7	7	3
8	8	4
\.
COPY t_col_lev2 (id, data1, data2) FROM stdin;
6	6	2
7	7	3
8	8	4
\.

SELECT * FROM get_tupcounts_for_partitions('t_row');
SELECT * FROM get_tupcounts_for_partitions('t_col');
SELECT * FROM get_tupcounts_for_partitions('t_row_lev1');
SELECT * FROM get_tupcounts_for_partitions('t_col_lev1');
SELECT * FROM get_tupcounts_for_partitions('t_row_lev2');
SELECT * FROM get_tupcounts_for_partitions('t_col_lev2');

DROP TABLE t_row, t_col, t_row_lev1, t_col_lev1, t_row_lev2, t_col_lev2;
DROP FUNCTION get_total_tupcount(oid);
DROP FUNCTION get_tupcounts_for_partitions(text);
