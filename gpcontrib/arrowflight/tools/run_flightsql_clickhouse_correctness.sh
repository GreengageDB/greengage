#!/usr/bin/env bash
set -euo pipefail

# Two ClickHouse nodes and a running multi-segment Greengage demo cluster.
: "${FLIGHTSQL_HOST:=clickhouse1}"
: "${FLIGHTSQL_PORT:=9005}"
: "${CLICKHOUSE_HTTP_URL:=http://clickhouse1:8123/}"
: "${CLICKHOUSE_HTTP_URL_2:=http://clickhouse2:8123/}"
suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
if [[ ! "${suffix}" =~ ^[a-z0-9_]{1,30}$ ]]; then
  echo 'Invalid FLIGHTSQL_TEST_SUFFIX' >&2
  exit 1
fi
name="flightsql_correctness_${suffix}"
psql_cmd=(psql -X -q -v ON_ERROR_STOP=1 postgres)

ch() {
  curl --fail --silent --show-error "$1" --data-binary "$2"
}

"${psql_cmd[@]}" -v name="${name}" -v host="${FLIGHTSQL_HOST}" \
  -v port="${FLIGHTSQL_PORT}" <<'SQL'
BEGIN;
CREATE EXTENSION IF NOT EXISTS arrowflight;
CREATE SCHEMA :"name";
CREATE SERVER :"name" FOREIGN DATA WRAPPER flightsql_fdw
  OPTIONS (host :'host', port :'port', timeout_ms '10000');
COMMIT;
SQL

cleanup() {
  "${psql_cmd[@]}" -v name="${name}" >/dev/null 2>&1 <<'SQL' || true
DROP SCHEMA :"name" CASCADE;
DROP SERVER :"name";
SQL
  ch "${CLICKHOUSE_HTTP_URL}" "DROP TABLE IF EXISTS default.${name}" >/dev/null 2>&1 || true
  ch "${CLICKHOUSE_HTTP_URL}" "DROP VIEW IF EXISTS default.${name}_slow" >/dev/null 2>&1 || true
  for url in "${CLICKHOUSE_HTTP_URL}" "${CLICKHOUSE_HTTP_URL_2}"; do
    ch "${url}" "DROP TABLE IF EXISTS default.${name}_local" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

id=1
for url in "${CLICKHOUSE_HTTP_URL}" "${CLICKHOUSE_HTTP_URL_2}"; do
  ch "${url}" "
    CREATE TABLE default.${name}_local
      (id Int32, f4 Float32, f8 Float64, n Decimal128(20), big_n Decimal128(1),
       tokyo DateTime64(6, 'Asia/Tokyo')) ENGINE = MergeTree ORDER BY id
  " >/dev/null
  ch "${url}" "
    INSERT INTO default.${name}_local VALUES
      (${id}, 0.1, 0.30000000000000004, '0.1', '12345678901234567.8', '2024-01-01 00:00:00')
  " >/dev/null
  id=$((id + 1))
done
ch "${CLICKHOUSE_HTTP_URL}" "
  CREATE TABLE default.${name} AS default.${name}_local
  ENGINE = Distributed(flightsql_test_cluster, default, ${name}_local, cityHash64(id))
" >/dev/null

"${psql_cmd[@]}" <<SQL
CREATE FOREIGN TABLE ${name}.remote
  (id int4, f4 real, f8 float8, n numeric(38,20), big_n numeric(38,1))
  SERVER ${name} OPTIONS (table_name '${name}', schema_name 'default');
CREATE FOREIGN TABLE ${name}.bad_timestamp (id int4, tokyo timestamp)
  SERVER ${name} OPTIONS (table_name '${name}', schema_name 'default');
CREATE FOREIGN TABLE ${name}.bad_text (id int4, f4 text)
  SERVER ${name} OPTIONS (table_name '${name}', schema_name 'default');
CREATE FOREIGN TABLE ${name}.required (id int4)
  SERVER ${name} OPTIONS (table_name '${name}', schema_name 'default',
                         write_transaction_mode 'required');
CREATE TABLE ${name}.reference
  (id int4, f4 real, f8 float8, n numeric(38,20), big_n numeric(38,1))
  DISTRIBUTED RANDOMLY;
INSERT INTO ${name}.reference VALUES
  (1, 0.1, 0.30000000000000004, 0.1, 12345678901234567.8),
  (2, 0.1, 0.30000000000000004, 0.1, 12345678901234567.8);
SQL

query() {
  PGOPTIONS="${PGOPTIONS:-} -c optimizer=${optimizer} -c extra_float_digits=0" \
    "${psql_cmd[@]}" -At <<< "$1"
}

expect_failure() {
  local label="$1" sql="$2" expected="$3" output
  if output="$(query "${sql}" 2>&1)" || [[ "${output}" != *"${expected}"* ]]; then
    printf 'FAIL: %s\n%s\n' "${label}" "${output}" >&2
    exit 1
  fi
  printf 'PASS (optimizer=%s): %s\n' "${optimizer}" "${label}"
}

for optimizer in on off; do
  for predicate in \
    "f4 = '0.1'" "f4 = 0.1::real" "f4 IN (0.1::real, 0.2::real)" \
    "f4 <> 0.2::real" "f8 = 0.30000000000000000004::float8" \
    "f8 IN (0.30000000000000004::float8)" \
    "n = 0.10000000000000000001" "n <> 0.10000000000000000001" \
    "n < 0.10000000000000000001" "n <= 0.10000000000000000001" \
    "n > 0.09999999999999999999" "n >= 0.10000000000000000001" \
    "n IN (0.10000000000000000001, 0.2)" \
    "big_n > 12345678901234567.7" "big_n = 12345678901234567.8"; do
    expected="$(query "SELECT id FROM ${name}.reference WHERE ${predicate} ORDER BY id")"
    actual="$(query "SELECT id FROM ${name}.remote WHERE ${predicate} ORDER BY id")"
    if [[ "${actual}" != "${expected}" ]]; then
      printf 'FAIL (optimizer=%s): %s\nExpected: %s\nActual: %s\n' \
        "${optimizer}" "${predicate}" "${expected}" "${actual}" >&2
      exit 1
    fi
    plan="$(query "EXPLAIN (COSTS OFF) SELECT id FROM ${name}.remote WHERE ${predicate}")"
    if grep 'Flight SQL query:' <<< "${plan}" | grep -q ' WHERE '; then
      printf 'Unsafe predicate pushed: %s\n%s\n' "${predicate}" "${plan}" >&2
      exit 1
    fi
    printf 'PASS (optimizer=%s): %s\n' "${optimizer}" "${predicate}"
  done
  actual="$(query "PREPARE f4_lookup(real) AS SELECT id FROM ${name}.remote WHERE f4 = \$1 ORDER BY id;
    EXECUTE f4_lookup(0.1::real); DEALLOCATE f4_lookup;")"
  if [[ "${actual}" != $'1\n2' ]]; then
    echo "Prepared float4 comparison failed: ${actual}" >&2
    exit 1
  fi
  expect_failure 'predicate-only timestamp mapping' \
    "SELECT id FROM ${name}.bad_timestamp WHERE tokyo IS NOT NULL" 'timezone'
  expect_failure 'predicate-only timestamp comparison' \
    "SELECT id FROM ${name}.bad_timestamp WHERE tokyo > TIMESTAMP '2023-01-01 00:00:00'" 'timezone'
  expect_failure 'predicate-only timestamp with an empty result' \
    "SELECT id FROM ${name}.bad_timestamp WHERE tokyo > TIMESTAMP '2099-01-01 00:00:00'" 'timezone'
  expect_failure 'predicate-only text mapping' \
    "SELECT id FROM ${name}.bad_text WHERE f4 IS NOT NULL" 'f4'
done

# Utility mode bypasses QD ExecutorStart. It must not downgrade required ingest.
IFS='|' read -r segment_host segment_port <<< "$(
  query "SELECT hostname, port FROM gp_segment_configuration
    WHERE content = 0 AND role = 'p'"
)"
before="$(ch "${CLICKHOUSE_HTTP_URL}" "SELECT count() FROM default.${name}")"
if output="$(PGOPTIONS='-c gp_role=utility' \
    "${psql_cmd[@]}" -h "${segment_host}" -p "${segment_port}" 2>&1 <<SQL
BEGIN;
INSERT INTO ${name}.required VALUES (99);
ROLLBACK;
SQL
)" || [[ "${output}" != *'required transaction was not prepared on the dispatcher'* ]]; then
  printf 'Utility required INSERT was not rejected: %s\n' "${output}" >&2
  exit 1
fi
after="$(ch "${CLICKHOUSE_HTTP_URL}" "SELECT count() FROM default.${name}")"
if [[ "${before}" != "${after}" ]]; then
  echo 'Utility required INSERT changed remote rows' >&2
  exit 1
fi
echo 'PASS: utility required INSERT rejected without remote rows'

# ClickHouse runs this expression during Execute/GetFlightInfo. Without RPC
# cancellation the 200 ms statement timeout would wait for the full two seconds.
ch "${CLICKHOUSE_HTTP_URL}" "CREATE VIEW default.${name}_slow AS
  SELECT toInt32(number + sleepEachRow(0.2)) AS id FROM numbers(10)" >/dev/null
"${psql_cmd[@]}" <<SQL
ALTER SERVER ${name} OPTIONS (SET timeout_ms '0');
CREATE FOREIGN TABLE ${name}.slow (id int4) SERVER ${name}
  OPTIONS (schema_name 'default', table_name '${name}_slow');
SQL
python3 - "${name}" <<'PY'
import sys
import time

import psycopg2
from psycopg2 import sql

connection = psycopg2.connect(dbname='postgres')
connection.autocommit = True
try:
    for optimizer in ('on', 'off'):
        with connection.cursor() as cursor:
            cursor.execute(f'SET optimizer = {optimizer}')
            cursor.execute('SET statement_timeout = 200')
            started = time.monotonic()
            try:
                cursor.execute(sql.SQL('SELECT count(*) FROM {}.slow').format(
                    sql.Identifier(sys.argv[1])))
                raise AssertionError('expected discovery cancellation')
            except psycopg2.Error as error:
                elapsed = time.monotonic() - started
                assert error.pgcode == '57014', error
                assert elapsed < 1.5, f'discovery cancellation took {elapsed:.3f}s'
            cursor.execute('SET statement_timeout = 0')
            cursor.execute('SELECT 1')
            assert cursor.fetchone() == (1,)
            print(f'PASS (optimizer={optimizer}): ClickHouse discovery cancelled in {elapsed:.3f}s')
finally:
    connection.close()
PY
