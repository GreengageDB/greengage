-- start_matchsubs
-- m/ERROR:  Track for database \d+ is being acquired in other transaction/
-- s/\d+/XXX/g
-- end_matchsubs
-- Test concurrent track acquisition.
1: CREATE EXTENSION IF NOT EXISTS gg_tables_tracking;
1: SELECT gg_tables_tracking.tracking_register_db();
1: SELECT gg_tables_tracking.tracking_trigger_initial_snapshot();
1: BEGIN;
1: WITH segment_counts AS (
    SELECT tt.segid, COUNT(*) AS cnt 
    FROM gg_tables_tracking.tables_track tt 
    GROUP BY tt.segid
),
pg_class_count AS (
    SELECT COUNT(*) AS cnt FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE nspname = ANY (string_to_array(current_setting('gg_tables_tracking.tracking_schemas'), ','))
    AND c.relkind = ANY (string_to_array(current_setting('gg_tables_tracking.tracking_relkinds'), ','))
)
SELECT bool_and(sc.cnt = pc.cnt)
FROM segment_counts sc, pg_class_count pc;

2: SELECT tt.segid, count(*) FROM gg_tables_tracking.tables_track tt GROUP BY tt.segid;

1: ROLLBACK;

2: WITH segment_counts AS (
    SELECT tt.segid, COUNT(*) AS cnt 
    FROM gg_tables_tracking.tables_track tt 
    GROUP BY tt.segid
),
pg_class_count AS (
    SELECT COUNT(*) AS cnt FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE nspname = ANY (string_to_array(current_setting('gg_tables_tracking.tracking_schemas'), ','))
    AND c.relkind = ANY (string_to_array(current_setting('gg_tables_tracking.tracking_relkinds'), ','))
)
SELECT bool_and(sc.cnt = pc.cnt)
FROM segment_counts sc, pg_class_count pc;

-- Test uncommited file creation is not seen from other transaction until the
-- first one is commited.
1: BEGIN;
1: CREATE TABLE tracking_t1 AS SELECT generate_series (1, 100) i DISTRIBUTED BY (i);

2: SELECT relname, size, state, segid, relkind, relam FROM gg_tables_tracking.tables_track;

1: COMMIT;

2: SELECT relname, size, state, segid, relkind, relam FROM gg_tables_tracking.tables_track;

-- Test file creation is seen from other transaction after the first transaction
-- has taken the track.
1: BEGIN;
1: CREATE TABLE tracking_t2 AS SELECT generate_series (1, 100) i DISTRIBUTED BY (i);
1: SELECT relname, size, state, segid, relkind, relam FROM gg_tables_tracking.tables_track;
1: COMMIT;

2: SELECT relname, size, state, segid, relkind, relam FROM gg_tables_tracking.tables_track;

1: DROP TABLE tracking_t1;
1: DROP TABLE tracking_t2;
1: SELECT gg_tables_tracking.tracking_unregister_db();
1: DROP EXTENSION gg_tables_tracking;

1q:
2q:
