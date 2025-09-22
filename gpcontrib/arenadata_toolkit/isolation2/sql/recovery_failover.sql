-- This test triggers failover of content 1 and checks
-- the correct tracking state behaviour after recovery
!\retcode gpconfig -c shared_preload_libraries -v 'arenadata_toolkit';
!\retcode gpconfig -c gp_fts_probe_retries -v 2 --masteronly;
-- Allow extra time for mirror promotion to complete recovery
!\retcode gpconfig -c gp_gang_creation_retry_count -v 120 --skipvalidation --masteronly;
!\retcode gpconfig -c gp_gang_creation_retry_timer -v 1000 --skipvalidation --masteronly;
!\retcode gpstop -raq -M fast;

CREATE EXTENSION IF NOT EXISTS arenadata_toolkit;

!\retcode gpconfig -c arenadata_toolkit.tracking_worker_naptime_sec -v '5';
!\retcode gpstop -u;

SELECT pg_sleep(current_setting('arenadata_toolkit.tracking_worker_naptime_sec')::int);
SELECT arenadata_toolkit.tracking_register_db();
SELECT arenadata_toolkit.tracking_trigger_initial_snapshot();

-- Test track acquisition returns the same count of tuples as pg_class has with
-- default filter options.
WITH segment_counts AS (
    SELECT tt.segid, COUNT(*) AS cnt 
    FROM arenadata_toolkit.tables_track tt 
    GROUP BY tt.segid
),
pg_class_count AS (
    SELECT COUNT(*) AS cnt FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE nspname = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_schemas'), ','))
    AND c.relstorage = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_relstorages'), ','))
    AND c.relkind = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_relkinds'), ','))
)
SELECT bool_and(sc.cnt = pc.cnt)
FROM segment_counts sc, pg_class_count pc;

include: helpers/server_helpers.sql;

-- Helper functions
CREATE OR REPLACE FUNCTION tracking_is_segment_initialized_master() /* in func */
RETURNS TABLE(segindex INT, is_initialized BOOL) AS $$ /* in func */
SELECT segindex, is_initialized /* in func */
FROM arenadata_toolkit.tracking_is_segment_initialized(); /* in func */
$$ LANGUAGE SQL EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION tracking_is_segment_initialized_segments() /* in func */
RETURNS TABLE(segindex INT, is_initialized BOOL) AS $$ /* in func */
SELECT segindex, is_initialized /* in func */
FROM arenadata_toolkit.tracking_is_segment_initialized(); /* in func */
$$ LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

CREATE or REPLACE FUNCTION wait_until_segments_are_down(num_segs int)
RETURNS BOOL AS
$$
DECLARE
retries int; /* in func */
BEGIN /* in func */
  retries := 1200; /* in func */
  loop /* in func */
    IF (select count(*) = num_segs FROM gp_segment_configuration WHERE status = 'd') THEN /* in func */
      return TRUE; /* in func */
    END IF; /* in func */
    IF retries <= 0 THEN /* in func */
      return FALSE; /* in func */
    END IF; /* in func */
    perform pg_sleep(0.1); /* in func */
    retries := retries - 1; /* in func */
  END loop; /* in func */
END; /* in func */
$$ language plpgsql;

-- no segment down.
SELECT count(*) FROM gp_segment_configuration WHERE status = 'd';

SELECT pg_ctl((select datadir FROM gp_segment_configuration c
WHERE c.role='p' AND c.content=1), 'stop');

SELECT wait_until_segments_are_down(1);

SELECT pg_sleep(current_setting('arenadata_toolkit.tracking_worker_naptime_sec')::int);
SELECT * FROM tracking_is_segment_initialized_master()
UNION ALL
SELECT * FROM tracking_is_segment_initialized_segments();

-- Track acquisition should return full snapshot from promoted mirror since
-- initial snapshot is activated on recovery by default.
WITH segment_counts AS (
    SELECT COUNT(*) AS cnt 
    FROM arenadata_toolkit.tables_track tt WHERE tt.segid = 1
    GROUP BY tt.segid
),
pg_class_count AS (
    SELECT COUNT(*) AS cnt FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE nspname = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_schemas'), ','))
    AND c.relstorage = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_relstorages'), ','))
    AND c.relkind = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_relkinds'), ','))
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

-- Track should be returned only from recovered segment since
-- initial snapshot is activated on recovery by default.
WITH segment_counts AS (
    SELECT COUNT(*) AS cnt 
    FROM arenadata_toolkit.tables_track tt
    GROUP BY tt.segid
),
pg_class_count AS (
    SELECT COUNT(*) AS cnt FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE nspname = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_schemas'), ','))
    AND c.relstorage = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_relstorages'), ','))
    AND c.relkind = ANY (string_to_array(current_setting('arenadata_toolkit.tracking_relkinds'), ','))
)
SELECT bool_and(sc.cnt = pc.cnt)
FROM segment_counts sc, pg_class_count pc;

SELECT arenadata_toolkit.tracking_unregister_db();

!\retcode gpconfig -r gp_fts_probe_retries --masteronly;
!\retcode gpconfig -r gp_gang_creation_retry_count --skipvalidation --masteronly;
!\retcode gpconfig -r gp_gang_creation_retry_timer --skipvalidation --masteronly;
!\retcode gpstop -u;
