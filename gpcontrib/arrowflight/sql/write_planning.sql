-- start_matchsubs
-- m/Redistribute Motion 1:\d+/
-- s/Redistribute Motion 1:\d+/Redistribute Motion 1:N/
-- end_matchsubs

SET optimizer=off;

CREATE SERVER arrowflight_fdw_plan_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    write_mode 'staging',
    batch_rows '8',
    max_batch_bytes '1024',
    timeout_ms '1000',
    retry_count '0',
    retry_backoff_ms '10'
);

CREATE FOREIGN TABLE arrowflight_fdw_read_only_check (
    id int4,
    label text
)
SERVER arrowflight_fdw_plan_srv
OPTIONS (
    path 'dataset/read_plan',
    rows '10'
);

-- start_ignore
EXPLAIN (COSTS OFF)
SELECT id
FROM arrowflight_fdw_read_only_check;
-- end_ignore

CREATE FOREIGN TABLE arrowflight_fdw_write_check (
    id int4,
    label text
)
SERVER arrowflight_fdw_plan_srv
OPTIONS (
    path 'dataset/events',
    operation_metadata 'static.source=regress,static.job_id=afw1'
);

SELECT (pg_relation_is_updatable('arrowflight_fdw_read_only_check'::regclass, false) & 8) <> 0
       AS read_table_insertable;
SELECT (pg_relation_is_updatable('arrowflight_fdw_write_check'::regclass, false) & 8) <> 0
       AS write_table_insertable;

EXPLAIN (COSTS OFF, VERBOSE)
INSERT INTO arrowflight_fdw_write_check
SELECT 1, 'one';

CREATE FOREIGN TABLE arrowflight_fdw_bad_metadata (
    id int4
)
SERVER arrowflight_fdw_plan_srv
OPTIONS (
    operation_metadata 'af.operation.id=bad'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_type (
    n numeric
)
SERVER arrowflight_fdw_plan_srv
OPTIONS (
    path 'dataset/events'
);

EXPLAIN (COSTS OFF)
INSERT INTO arrowflight_fdw_bad_type
SELECT 1.0::numeric;

DROP FOREIGN TABLE arrowflight_fdw_bad_type;

CREATE FOREIGN TABLE arrowflight_fdw_missing_write_path (
    id int4
)
SERVER arrowflight_fdw_plan_srv;

EXPLAIN (COSTS OFF)
INSERT INTO arrowflight_fdw_missing_write_path
SELECT 1;

DROP FOREIGN TABLE arrowflight_fdw_missing_write_path;
DROP FOREIGN TABLE arrowflight_fdw_read_only_check;
DROP FOREIGN TABLE arrowflight_fdw_write_check;
DROP SERVER arrowflight_fdw_plan_srv;
