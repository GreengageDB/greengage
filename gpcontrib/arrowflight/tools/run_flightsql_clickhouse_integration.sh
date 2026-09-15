#!/usr/bin/env bash
set -euo pipefail

: "${FLIGHTSQL_HOST:=host.docker.internal}"
: "${FLIGHTSQL_PORT:=19005}"
: "${CLICKHOUSE_HTTP_URL:=http://host.docker.internal:18123/}"
: "${CLICKHOUSE_HTTP_URL_2:=http://host.docker.internal:18124/}"

suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
read_table="afsql_read_${suffix}"
read_local_table="${read_table}_local"
write_table="afsql_write_${suffix}"
write_local_table="${write_table}_local"
foreign_read="flightsql_read_${suffix}"
foreign_write="flightsql_write_${suffix}"
foreign_required_write="flightsql_required_write_${suffix}"
source_table="flightsql_source_${suffix}"
server_name="flightsql_clickhouse_${suffix}"

clickhouse_query_at() {
  local url="$1"
  local query="$2"

  curl --fail --silent --show-error \
    "${url}" \
    --data-binary "${query}"
}

clickhouse_query() {
  clickhouse_query_at "${CLICKHOUSE_HTTP_URL}" "$1"
}

clickhouse_query_2() {
  clickhouse_query_at "${CLICKHOUSE_HTTP_URL_2}" "$1"
}

cleanup() {
  psql -X -v ON_ERROR_STOP=0 postgres <<SQL >/dev/null 2>&1 || true
DROP FOREIGN TABLE IF EXISTS ${foreign_read};
DROP FOREIGN TABLE IF EXISTS ${foreign_write};
DROP FOREIGN TABLE IF EXISTS ${foreign_required_write};
DROP TABLE IF EXISTS ${source_table};
DROP SERVER IF EXISTS ${server_name} CASCADE;
SQL
  clickhouse_query "DROP TABLE IF EXISTS default.${read_table}" \
    >/dev/null 2>&1 || true
  clickhouse_query "DROP TABLE IF EXISTS default.${write_table}" \
    >/dev/null 2>&1 || true
  clickhouse_query "DROP TABLE IF EXISTS default.${read_local_table}" \
    >/dev/null 2>&1 || true
  clickhouse_query "DROP TABLE IF EXISTS default.${write_local_table}" \
    >/dev/null 2>&1 || true
  clickhouse_query_2 "DROP TABLE IF EXISTS default.${read_local_table}" \
    >/dev/null 2>&1 || true
  clickhouse_query_2 "DROP TABLE IF EXISTS default.${write_local_table}" \
    >/dev/null 2>&1 || true
}
trap cleanup EXIT

create_read_local="
CREATE TABLE default.${read_local_table}
(
    id Int32,
    source_node Int32,
    label String,
    active Bool,
    amount Float64,
    d Date,
    ts DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY id
"
clickhouse_query "${create_read_local}" >/dev/null
clickhouse_query_2 "${create_read_local}" >/dev/null

clickhouse_query "
INSERT INTO default.${read_local_table}
SELECT
    toInt32(number),
    1,
    concat('row-', toString(number)),
    number % 2 = 0,
    number / 10.0,
    toDate('2024-01-01') + number % 30,
    toDateTime64('2024-01-01 00:00:00', 6, 'UTC') + number
FROM numbers(500000)
" >/dev/null

clickhouse_query_2 "
INSERT INTO default.${read_local_table}
SELECT
    toInt32(number + 500000),
    2,
    concat('row-', toString(number + 500000)),
    number % 2 = 0,
    (number + 500000) / 10.0,
    toDate('2024-01-01') + (number + 500000) % 30,
    toDateTime64('2024-01-01 00:00:00', 6, 'UTC') + number + 500000
FROM numbers(500000)
" >/dev/null

clickhouse_query "
CREATE TABLE default.${read_table}
AS default.${read_local_table}
ENGINE = Distributed(
    flightsql_test_cluster,
    default,
    ${read_local_table},
    cityHash64(id))
" >/dev/null

create_write_local="
CREATE TABLE default.${write_local_table}
(
    id Int32,
    label String,
    active Bool,
    amount Float64,
    d Date,
    ts DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY id
"
clickhouse_query "${create_write_local}" >/dev/null
clickhouse_query_2 "${create_write_local}" >/dev/null

clickhouse_query "
CREATE TABLE default.${write_table}
AS default.${write_local_table}
ENGINE = Distributed(
    flightsql_test_cluster,
    default,
    ${write_local_table},
    cityHash64(id))
" >/dev/null

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;
SET optimizer = off;
SET TIME ZONE 'UTC';

CREATE SERVER ${server_name}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    mpp_execute 'all segments',
    host '${FLIGHTSQL_HOST}',
    port '${FLIGHTSQL_PORT}',
    timeout_ms '60000',
    max_endpoints '1000',
    max_plan_bytes '16777216',
    batch_rows '4096',
    max_batch_bytes '4194304',
    ingest_row_count_check 'off'
);

CREATE FOREIGN TABLE ${foreign_read}
(
    id int4,
    source_node int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER ${server_name}
OPTIONS (
    table_name '${read_table}',
    schema_name 'default',
    rows '1000000'
);

CREATE FOREIGN TABLE ${foreign_write}
(
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER ${server_name}
OPTIONS (
    table_name '${write_table}',
    schema_name 'default',
    rows '20000'
);

CREATE FOREIGN TABLE ${foreign_required_write}
(
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER ${server_name}
OPTIONS (
    table_name '${write_table}',
    schema_name 'default',
    rows '20000',
    write_transaction_mode 'required'
);

CREATE TABLE ${source_table}
(
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
DISTRIBUTED RANDOMLY;

INSERT INTO ${source_table}
SELECT
    i,
    'written-' || i,
    i % 2 = 0,
    i / 10.0,
    DATE '2025-01-01' + i % 30,
    TIMESTAMPTZ '2025-01-01 00:00:00+00' + i * INTERVAL '1 second'
FROM generate_series(1, 20000) AS i;
SQL

# Planning must not execute CommandStatementQuery on the external server.
clickhouse_query "SYSTEM FLUSH LOGS" >/dev/null
psql -X -v ON_ERROR_STOP=1 postgres <<SQL >/tmp/flightsql_explain_${suffix}.log
SET optimizer = off;
EXPLAIN (COSTS OFF)
SELECT sum(id) FROM ${foreign_read};
SQL
explain_remote_queries="$(
  clickhouse_query "
    SELECT count()
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND is_initial_query = 1
      AND query = 'SELECT id AS id FROM \"default\".${read_table}'
  " | tr -d '[:space:]'
)"
test "${explain_remote_queries}" = "0"

read_result="$(
  PGOPTIONS='-c optimizer=off -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres \
    -c "SELECT count(*) || ':' || sum(id) || ':' || min(id) || ':' || max(id) || ':' || count(*) FILTER (WHERE source_node = 1) || ':' || count(*) FILTER (WHERE source_node = 2) FROM ${foreign_read}"
)"
test "${read_result}" = "1000000:499999500000:0:999999:500000:500000"

# The coordinator must discover one FlightInfo for the statement and dispatch
# its opaque endpoints. A per-QE discovery implementation would produce one
# ClickHouse query-log entry per segment here.
clickhouse_query "SYSTEM FLUSH LOGS" >/dev/null
statement_query_count="$(
  clickhouse_query "
    SELECT count()
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND is_initial_query = 1
      AND query = 'SELECT id AS id, source_node AS source_node FROM \"default\".${read_table}'
  " | tr -d '[:space:]'
)"
test "${statement_query_count}" = "1"

clickhouse_query "SYSTEM FLUSH LOGS" >/dev/null
predicate_result="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*) || ':' || sum(id)
      FROM ${foreign_read}
      WHERE id >= 123450
        AND id < 123550
        AND active
        AND label IS NOT NULL
        AND label <> 'missing'
        AND d >= DATE '2024-01-01'"
)"
test "${predicate_result}" = "50:6174950"

clickhouse_query "SYSTEM FLUSH LOGS" >/dev/null
predicate_query_count="$(
  clickhouse_query "
    SELECT count()
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND is_initial_query = 1
      AND query LIKE 'SELECT id AS id FROM \"default\".${read_table} WHERE %'
      AND query LIKE '%id >= 123450%'
      AND query LIKE '%id < 123550%'
      AND query LIKE '%active%'
      AND query LIKE '%label <> ''missing''%'
      AND query LIKE '%d >= DATE ''2024-01-01''%'
      AND (
          query LIKE '%label IS NOT NULL%'
          OR query LIKE '%NOT (label IS NULL)%'
      )
  " | tr -d '[:space:]'
)"
test "${predicate_query_count}" = "1"

predicate_remote_rows="$(
  clickhouse_query "
    SELECT max(result_rows)
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND is_initial_query = 1
      AND query LIKE 'SELECT id AS id FROM \"default\".${read_table} WHERE %'
      AND query LIKE '%id >= 123450%'
  " | tr -d '[:space:]'
)"
test "${predicate_remote_rows}" = "50"

clickhouse_query "SYSTEM FLUSH LOGS" >/dev/null
predicate_postgres_result="$(
  PGOPTIONS='-c optimizer=off -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*) || ':' || sum(id)
      FROM ${foreign_read}
      WHERE id >= 223450
        AND id < 223550
        AND active
        AND label <> 'missing'"
)"
test "${predicate_postgres_result}" = "50:11174950"

clickhouse_query "SYSTEM FLUSH LOGS" >/dev/null
predicate_postgres_remote="$(
  clickhouse_query "
    SELECT concat(toString(count()), ':', toString(max(result_rows)))
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND is_initial_query = 1
      AND query LIKE 'SELECT id AS id FROM \"default\".${read_table} WHERE %'
      AND query LIKE '%id >= 223450%'
      AND query LIKE '%id < 223550%'
      AND query LIKE '%label <> ''missing''%'
  " | tr -d '[:space:]'
)"
test "${predicate_postgres_remote}" = "1:50"

full_type_result="$(
  PGOPTIONS='-c optimizer=off -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${foreign_read}
      WHERE
          (
              id = 0
              AND source_node = 1
              AND label = 'row-0'
              AND active
              AND amount = 0.0
              AND d = DATE '2024-01-01'
              AND ts = TIMESTAMPTZ '2024-01-01 00:00:00+00'
          )
          OR
          (
              id = 999999
              AND source_node = 2
              AND label = 'row-999999'
              AND NOT active
              AND abs(amount - 99999.9) < 0.000000001
              AND d = DATE '2024-01-10'
              AND ts = TIMESTAMPTZ '2024-01-12 13:46:39+00'
          )"
)"
test "${full_type_result}" = "2"

segment_count="$(
  PGOPTIONS='-c optimizer=off -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres \
    -c "SELECT gp_execution_segment() FROM ${foreign_read}" |
    sort -u |
    wc -l |
    tr -d '[:space:]'
)"
if [ "${segment_count}" -lt 2 ]; then
  echo "expected Flight SQL endpoints to be consumed by at least two segments" >&2
  exit 1
fi

source_segment_count="$(
  psql -X -qAt -v ON_ERROR_STOP=1 postgres \
    -c "SELECT count(DISTINCT gp_segment_id) FROM ${source_table}"
)"
if [ "${source_segment_count}" -lt 2 ]; then
  echo "expected Flight SQL ingest input on at least two segments" >&2
  exit 1
fi

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
SET optimizer = off;
INSERT INTO ${foreign_write}
SELECT id, label, active, amount, d, ts
FROM ${source_table};
SQL

clickhouse_query "SYSTEM FLUSH DISTRIBUTED default.${write_table}" >/dev/null

write_result="$(
  clickhouse_query "
    SELECT concat(
        toString(count()), ':',
        toString(sum(id)), ':',
        toString(min(id)), ':',
        toString(max(id)))
    FROM default.${write_table}
  " | tr -d '[:space:]'
)"
test "${write_result}" = "20000:200010000:1:20000"

write_node_1_count="$(
  clickhouse_query "SELECT count() FROM default.${write_local_table}" |
    tr -d '[:space:]'
)"
write_node_2_count="$(
  clickhouse_query_2 "SELECT count() FROM default.${write_local_table}" |
    tr -d '[:space:]'
)"
if [ "${write_node_1_count}" -le 0 ] || [ "${write_node_2_count}" -le 0 ]; then
  echo "expected Flight SQL ingest to reach both ClickHouse shards" >&2
  exit 1
fi
test "$((write_node_1_count + write_node_2_count))" = "20000"

readback_result="$(
  PGOPTIONS='-c optimizer=off -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres \
    -c "SELECT count(*) || ':' || sum(id) FROM ${foreign_write}"
)"
test "${readback_result}" = "20000:200010000"

set +e
required_error="$(
  PGOPTIONS='-c optimizer=off -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres \
    -c "INSERT INTO ${foreign_required_write} SELECT * FROM ${source_table}" \
    2>&1
)"
required_status=$?
set -e
if [ "${required_status}" -eq 0 ]; then
  echo "expected write_transaction_mode=required to reject ClickHouse" >&2
  exit 1
fi
grep -q "write_transaction_mode=required" <<<"${required_error}"

clickhouse_query "SYSTEM FLUSH DISTRIBUTED default.${write_table}" >/dev/null
required_row_count="$(
  clickhouse_query "SELECT count() FROM default.${write_table}" |
    tr -d '[:space:]'
)"
test "${required_row_count}" = "20000"

echo "flightsql_clickhouse_read=ok"
echo "flightsql_clickhouse_predicate_pushdown=ok"
echo "flightsql_clickhouse_write=ok"
echo "flightsql_clickhouse_required_fail_fast=ok"
echo "flightsql_clickhouse_segments=${segment_count}"
echo "flightsql_clickhouse_write_segments=${source_segment_count}"
echo "flightsql_clickhouse_nodes=2"
echo "flightsql_clickhouse_write_node_rows=${write_node_1_count},${write_node_2_count}"
