-- This test triggers failover of content 1 and checks
-- the correct tracking state behaviour after recovery
!\retcode gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'gg_tables_tracking'), ',')" postgres)";
-- Allow extra time for mirror promotion to complete recovery
!\retcode gpconfig -c gp_fts_probe_timeout -v 5 --masteronly;
!\retcode gpconfig -c gp_fts_probe_retries -v 2 --masteronly;
!\retcode gpstop -raq -M fast;
!\retcode gpconfig -c gg_tables_tracking.tracking_worker_naptime_sec -v '5';
!\retcode gpstop -u;

CREATE EXTENSION IF NOT EXISTS gg_tables_tracking;

!\retcode gpconfig -c gg_tables_tracking.tracking_worker_naptime_sec -v '5';
!\retcode gpstop -u;

SELECT gg_tables_tracking.wait_for_worker_initialize();
SELECT gg_tables_tracking.tracking_register_db();
SELECT gg_tables_tracking.tracking_trigger_initial_snapshot();

-- Test track acquisition returns the same count of tuples as pg_class has with
-- default filter options.
WITH segment_counts AS (
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

-- Helper functions
CREATE OR REPLACE FUNCTION tracking_is_segment_initialized_master() /* in func */
RETURNS TABLE(segindex INT, is_initialized BOOL) AS $$ /* in func */
SELECT segindex, is_initialized /* in func */
FROM gg_tables_tracking.tracking_is_segment_initialized(); /* in func */
$$ LANGUAGE SQL EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION tracking_is_segment_initialized_segments() /* in func */
RETURNS TABLE(segindex INT, is_initialized BOOL) AS $$ /* in func */
SELECT segindex, is_initialized /* in func */
FROM gg_tables_tracking.tracking_is_segment_initialized(); /* in func */
$$ LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

-- no segment down.
SELECT count(*) FROM gp_segment_configuration WHERE status = 'd';

SELECT pg_ctl((select datadir FROM gp_segment_configuration c
WHERE c.role='p' AND c.content=1), 'stop');

SELECT wait_until_segments_are_down(1);

1: SELECT gg_tables_tracking.wait_for_worker_initialize();
1q:
SELECT * FROM tracking_is_segment_initialized_master()
UNION ALL
SELECT * FROM tracking_is_segment_initialized_segments();

-- Track acquisition should return full snapshot from promoted mirror since
-- initial snapshot is activated on recovery by default.
WITH segment_counts AS (
    SELECT COUNT(*) AS cnt 
    FROM gg_tables_tracking.tables_track tt WHERE tt.segid = 1
    GROUP BY tt.segid
),
pg_class_count AS (
    SELECT COUNT(*) AS cnt FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE nspname = ANY (string_to_array(current_setting('gg_tables_tracking.tracking_schemas'), ','))
    AND c.relkind = ANY (string_to_array(current_setting('gg_tables_tracking.tracking_relkinds'), ','))
)
SELECT bool_and(sc.cnt = pc.cnt)
FROM segment_counts sc, pg_class_count pc;

-- fully recover the failed primary as new mirror
!\retcode gprecoverseg -aF --no-progress;

-- loop while segments come in sync
SELECT wait_until_all_segments_synchronized();

!\retcode gprecoverseg -ar;

-- loop while segments come in sync
SELECT wait_until_all_segments_synchronized();

-- verify no segment is down after recovery
SELECT count(*) FROM gp_segment_configuration WHERE status = 'd';

SELECT gg_tables_tracking.wait_for_worker_initialize();

-- Track should be returned only from recovered segment since
-- initial snapshot is activated on recovery by default.
WITH segment_counts AS (
    SELECT COUNT(*) AS cnt 
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

SELECT gg_tables_tracking.tracking_unregister_db();

!\retcode gpconfig -r gp_fts_probe_retries --masteronly;
!\retcode gpconfig -r gp_gang_creation_retry_count --skipvalidation --masteronly;
!\retcode gpconfig -r gp_gang_creation_retry_timer --skipvalidation --masteronly;
!\retcode gpstop -u;
