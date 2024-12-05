-- Tests for size tracking logic introduced in version 1.7
-- start_ignore
\! gpconfig -c arenadata_toolkit.tracking_worker_naptime_sec -v '5'
\! gpstop -u
\c
-- end_ignore
-- start_matchsubs
-- m/ERROR:  database \d+ is not tracked/
-- s/\d+/XXX/g
-- m/for database \d+ is empty/
-- s/\d+/XXX/g
-- end_matchsubs
--start_ignore
DROP DATABASE IF EXISTS tracking_db1;
--end_ignore
CREATE DATABASE tracking_db1;
\c tracking_db1;
CREATE EXTENSION arenadata_toolkit;

-- 1. Test getting track on not registered database;
SELECT * FROM arenadata_toolkit.tables_track;

SELECT pg_sleep(current_setting('arenadata_toolkit.tracking_worker_naptime_sec')::int * 2);
SELECT arenadata_toolkit.tracking_register_db();

-- 2. Test initial snapshot behaviour. Triggering initial snapshot leads to
-- setting up the bloom filter such that all relfilenodes are considered.
SELECT arenadata_toolkit.tracking_trigger_initial_snapshot();
SELECT is_triggered FROM arenadata_toolkit.is_initial_snapshot_triggered;

-- 3. If user hasn't registered any schema, the default schemas are used.
-- See arenadata_toolkit_guc.c. At commit the bloom filter is cleared. The next
-- track acquisition will return nothing if database is not modified in between. 
-- Test track acquisition returns the same count of tuples as pg_class when
-- initial snapshot is triggered.
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

-- 4. Create table in one of default schemas. Then unregister all
-- default schemas except this one.
CREATE TABLE arenadata_toolkit.tracking_t1 (i INT)
WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);

SELECT arenadata_toolkit.tracking_unregister_schema('information_schema');
SELECT arenadata_toolkit.tracking_unregister_schema('pg_aoseg');
SELECT arenadata_toolkit.tracking_unregister_schema('pg_toast');
SELECT arenadata_toolkit.tracking_unregister_schema('pg_catalog');
SELECT arenadata_toolkit.tracking_unregister_schema('public');

-- Getting the track. Only created table with size 0 is expected;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

SELECT arenadata_toolkit.tracking_unregister_schema('arenadata_toolkit');
SELECT arenadata_toolkit.tracking_trigger_initial_snapshot();

--Empty track is expected
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

SELECT arenadata_toolkit.tracking_register_schema('arenadata_toolkit');

-- 5. Test data extending event. Bloom should capture it.
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,100000);
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

-- 6. Dropping table. The track shows only relfilenodes without names and other additional info with status 'd'.
DROP TABLE arenadata_toolkit.tracking_t1;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

-- 8. Test actions on commit and rollback
CREATE TABLE arenadata_toolkit.tracking_t1 (i INT)
WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,100000);

-- If the wrapping transaction rollbacks, the Bloom filter is not cleared up.
BEGIN;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;
ROLLBACK;

-- If commits, filter is cleared.
BEGIN;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;
COMMIT;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

-- 9. Test repetitive track call within the same transaction. All the
-- calls should return the same relation set.
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,10000);
BEGIN;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

CREATE TABLE arenadata_toolkit.tracking_t2 (j BIGINT) DISTRIBUTED BY (j);
INSERT INTO arenadata_toolkit.tracking_t2 SELECT generate_series(1,10000);
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,10000);

SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;
ROLLBACK;
SELECT relname, size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

-- 10. Test relkind filtering.
INSERT INTO arenadata_toolkit.tracking_t1 SELECT generate_series(1,100000);
CREATE INDEX ON arenadata_toolkit.tracking_t1(i);

-- Want to see index and block dir relation.
SELECT arenadata_toolkit.tracking_register_schema('pg_aoseg');
SELECT arenadata_toolkit.tracking_set_relkinds('o,i');

SELECT  size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

DROP TABLE arenadata_toolkit.tracking_t1;

-- Set empty relkinds. The track result set should be empty.
SELECT arenadata_toolkit.tracking_set_relkinds('');

SELECT arenadata_toolkit.tracking_trigger_initial_snapshot();

SELECT  size, state, segid, relkind, relstorage
FROM arenadata_toolkit.tables_track;

-- Clean up
SELECT arenadata_toolkit.tracking_unregister_db();

\c contrib_regression;
DROP DATABASE tracking_db1;
-- start_ignore
\! gpconfig -r shared_preload_libraries
\! gpconfig -r arenadata_toolkit.tracking_worker_naptime_sec
\! gpstop -u
-- end_ignore
