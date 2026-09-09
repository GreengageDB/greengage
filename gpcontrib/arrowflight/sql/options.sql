CREATE SERVER flightsql_options_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    timeout_ms '1000',
    max_endpoints '100',
    max_plan_bytes '1048576',
    batch_rows '8',
    max_batch_bytes '4096',
    endpoint_location_allowlist 'grpc+tcp://worker1.example:9005,grpc+tcp://worker2.example:9005'
);

CREATE SERVER flightsql_bad_auth_no_tls_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    auth_token_file '/tmp/token'
);

CREATE SERVER flightsql_bad_cert_only_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    tls 'true',
    tls_client_cert_file '/tmp/client.pem'
);

CREATE SERVER flightsql_bad_max_endpoints_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    max_endpoints '0'
);

CREATE SERVER flightsql_bad_endpoint_scheme_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    tls 'true',
    endpoint_location_allowlist 'grpc+tcp://worker.example:9005'
);

CREATE SERVER flightsql_bad_endpoint_path_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    endpoint_location_allowlist 'grpc+tcp://worker.example:9005/path'
);

CREATE SERVER flightsql_bad_max_plan_bytes_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    max_plan_bytes '1000'
);

CREATE SERVER flightsql_bad_row_count_check_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    ingest_row_count_check 'relaxed'
);

CREATE SERVER flightsql_bad_transaction_mode_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    write_transaction_mode 'best_effort'
);

CREATE SERVER flightsql_bad_routing_mode_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    write_routing_mode 'automatic'
);

CREATE SERVER flightsql_bad_predicate_pushdown_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    predicate_pushdown 'sometimes'
);

CREATE SERVER flightsql_bad_segment_count_srv
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    num_segments '2147483647'
);

CREATE FOREIGN TABLE flightsql_bad_segment_count (
    id int4
)
SERVER flightsql_bad_segment_count_srv
OPTIONS (
    table_name 'bad_segment_count'
);

DO $$
BEGIN
    EXECUTE 'EXPLAIN SELECT * FROM flightsql_bad_segment_count';
    RAISE EXCEPTION 'expected num_segments validation failure';
EXCEPTION
    WHEN invalid_parameter_value THEN
        RAISE NOTICE 'mismatched Flight SQL num_segments rejected';
END
$$;

DROP FOREIGN TABLE flightsql_bad_segment_count;
DROP SERVER flightsql_bad_segment_count_srv;

CREATE FOREIGN TABLE flightsql_missing_table (
    id int4
)
SERVER flightsql_options_srv;

CREATE FOREIGN TABLE flightsql_options_check (
    id int4 OPTIONS (column_name 'remote_id'),
    label text
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'events',
    schema_name 'default',
    rows '10'
);

CREATE FOREIGN TABLE flightsql_pushdown_off_check (
    id int4 OPTIONS (column_name 'remote_id'),
    label text
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'events',
    schema_name 'default',
    rows '10',
    predicate_pushdown 'false'
);

CREATE FOREIGN TABLE flightsql_predicate_types_check (
    id int4,
    active bool,
    amount float8,
    exact_amount numeric(12, 2),
    event_date date,
    event_time time,
    event_timestamp timestamp,
    zoned_timestamp timestamptz,
    fixed_label char(8)
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'predicate_types',
    schema_name 'default',
    rows '10'
);

CREATE FOREIGN TABLE flightsql_required_check (
    id int4
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'required_check',
    write_transaction_mode 'required'
);

CREATE FOREIGN TABLE flightsql_bad_security_table_option (
    id int4
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'bad_security',
    auth_token_file '/tmp/token'
);

CREATE FOREIGN TABLE flightsql_bad_endpoint_table_option (
    id int4
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'bad_endpoint',
    endpoint_location_allowlist 'grpc+tcp://worker.example:9005'
);

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE label = 'one';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE id >= 1
  AND id < 10
  AND label = 'one'
  AND label IS NOT NULL;

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE id IN (1, 3, 5);

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE id NOT IN (1, NULL);

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE label = 'O''Reilly';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE label = E'path\\value';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE id = 1 OR id = 2;

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE id = 1 OR lower(label) = 'one';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE id > 0
  AND lower(label) = 'one';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_options_check
WHERE label > 'one';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_pushdown_off_check
WHERE id > 0
  AND label = 'one';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_predicate_types_check
WHERE active
  AND amount = 1.5
  AND exact_amount >= 10.25
  AND event_date >= DATE '2025-01-02'
  AND event_timestamp < TIMESTAMP '2025-01-02 12:34:56';

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_predicate_types_check
WHERE active IS UNKNOWN;

EXPLAIN (COSTS OFF)
SELECT id
FROM flightsql_predicate_types_check
WHERE amount > 1.5
  AND active IS TRUE
  AND event_time < TIME '12:34:56.123456'
  AND event_timestamp < TIMESTAMP '2025-01-02 12:34:56.123456'
  AND zoned_timestamp IS NOT NULL
  AND fixed_label = 'fixed';

EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO flightsql_options_check
VALUES (1, 'one');

CREATE FOREIGN TABLE flightsql_bad_type (
    payload bit(8)
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'bad_type'
);

EXPLAIN (COSTS OFF)
SELECT * FROM flightsql_bad_type;

CREATE FOREIGN TABLE flightsql_bad_array (
    payload int4[]
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'bad_array'
);

EXPLAIN (COSTS OFF)
SELECT * FROM flightsql_bad_array;

CREATE FOREIGN TABLE flightsql_bad_batch_rows (
    id int4
)
SERVER flightsql_options_srv
OPTIONS (
    table_name 'bad_batch_rows',
    batch_rows '0'
);

DROP FOREIGN TABLE flightsql_bad_array;
DROP FOREIGN TABLE flightsql_bad_type;
DROP FOREIGN TABLE flightsql_required_check;
DROP FOREIGN TABLE flightsql_predicate_types_check;
DROP FOREIGN TABLE flightsql_pushdown_off_check;
DROP FOREIGN TABLE flightsql_options_check;
DROP SERVER flightsql_options_srv;
