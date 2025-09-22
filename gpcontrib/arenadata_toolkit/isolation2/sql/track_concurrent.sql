-- start_matchsubs
-- m/ERROR:  Track for database \d+ is being acquired in other transaction/
-- s/\d+/XXX/g
-- end_matchsubs
-- Test concurrent track acquisition.
1: CREATE EXTENSION IF NOT EXISTS arenadata_toolkit;
1: SELECT arenadata_toolkit.tracking_register_db();
1: SELECT arenadata_toolkit.tracking_trigger_initial_snapshot();
1: BEGIN;
1: WITH segment_counts AS (
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

2: SELECT tt.segid, count(*) FROM arenadata_toolkit.tables_track tt GROUP BY tt.segid;

1: ROLLBACK;

2: WITH segment_counts AS (
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

-- Test uncommited file creation is not seen from other transaction until the
-- first one is commited.
1: BEGIN;
1: CREATE TABLE tracking_t1 AS SELECT generate_series (1, 100) i DISTRIBUTED BY (i);

2: SELECT relname, size, state, segid, relkind, relstorage FROM arenadata_toolkit.tables_track;

1: COMMIT;

2: SELECT relname, size, state, segid, relkind, relstorage FROM arenadata_toolkit.tables_track;

-- Test file creation is seen from other transaction after the first transaction
-- has taken the track.
1: BEGIN;
1: CREATE TABLE tracking_t2 AS SELECT generate_series (1, 100) i DISTRIBUTED BY (i);
1: SELECT relname, size, state, segid, relkind, relstorage FROM arenadata_toolkit.tables_track;
1: COMMIT;

2: SELECT relname, size, state, segid, relkind, relstorage FROM arenadata_toolkit.tables_track;

1: DROP TABLE tracking_t1;
1: DROP TABLE tracking_t2;
1: SELECT arenadata_toolkit.tracking_unregister_db();
1: DROP EXTENSION arenadata_toolkit;

1q:
2q:

!\retcode gpconfig -r shared_preload_libraries;
!\retcode gpconfig -r arenadata_toolkit.tracking_worker_naptime_sec;
!\retcode gpstop -raq -M fast;
