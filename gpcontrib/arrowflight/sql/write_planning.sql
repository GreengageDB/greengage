-- start_matchsubs
-- m/Redistribute Motion 1:\d+/
-- s/Redistribute Motion 1:\d+/Redistribute Motion 1:N/
-- end_matchsubs

SET optimizer=off;

CREATE SERVER flightsql_plan_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    batch_rows '8',
    max_batch_bytes '4096',
    timeout_ms '1000'
);

CREATE FOREIGN TABLE flightsql_plan_check (
    id int4,
    label text
)
SERVER flightsql_plan_srv
OPTIONS (
    table_name 'events',
    rows '10'
);

SELECT (pg_relation_is_updatable(
            'flightsql_plan_check'::regclass, false) & 8) <> 0
       AS table_insertable;

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_plan_check;

EXPLAIN (COSTS OFF, VERBOSE)
INSERT INTO flightsql_plan_check
SELECT 1, 'one';

CREATE FOREIGN TABLE flightsql_bad_numeric (
    n numeric
)
SERVER flightsql_plan_srv
OPTIONS (
    table_name 'bad_numeric'
);

EXPLAIN (COSTS OFF)
INSERT INTO flightsql_bad_numeric
SELECT 1.0::numeric;

DROP FOREIGN TABLE flightsql_bad_numeric;
DROP FOREIGN TABLE flightsql_plan_check;
DROP SERVER flightsql_plan_srv;
