#!/usr/bin/env bash
set -euo pipefail

# Run against the synthetic compose server after installing arrowflight.
# Fixtures preserve the standard IPC data in both FlightInfo and DoGet:
#   af_perf_read_schema_duplicate: rename segid to id (same field count).
#   af_perf_read_schema_alias: rename id to local_id (same field count).
: "${FLIGHTSQL_SYNTHETIC_HOST:=flightsql-synthetic}"
: "${FLIGHTSQL_SYNTHETIC_PORT:=9015}"
: "${FLIGHTSQL_SCHEMA_EXPECTED_ROWS:=3000}"

suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
if [[ ! "${suffix}" =~ ^[a-z0-9_]{1,30}$ ]]; then
  printf 'Invalid FLIGHTSQL_TEST_SUFFIX: use 1-30 lowercase ASCII letters, digits or underscores\n' >&2
  exit 1
fi
schema="flightsql_schema_${suffix}"
server="${schema}_server"
psql_cmd=(psql -X -q -v ON_ERROR_STOP=1 postgres)

# Only register cleanup once the names are successfully reserved by this run.
"${psql_cmd[@]}" -v test_schema="${schema}" -v test_server="${server}" \
  -v remote_host="${FLIGHTSQL_SYNTHETIC_HOST}" \
  -v remote_port="${FLIGHTSQL_SYNTHETIC_PORT}" <<'SQL'
BEGIN;
CREATE EXTENSION IF NOT EXISTS arrowflight;
CREATE SCHEMA :"test_schema";
CREATE SERVER :"test_server"
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (host :'remote_host', port :'remote_port', predicate_pushdown 'false');
COMMIT;
SQL

cleanup() {
  "${psql_cmd[@]}" -v test_schema="${schema}" -v test_server="${server}" \
    >/dev/null 2>&1 <<'SQL' || true
DROP SCHEMA :"test_schema" CASCADE;
DROP SERVER :"test_server";
SQL
}
trap cleanup EXIT

"${psql_cmd[@]}" <<SQL
CREATE FOREIGN TABLE ${schema}.baseline
  (id int4, segid int4, label text, active bool, amount float8, d date, ts timestamp)
  SERVER ${server} OPTIONS (table_name 'af_perf_read');

CREATE FOREIGN TABLE ${schema}.dropped
  (drop_first int4, id int4, drop_middle text, segid int4, label text,
   active bool, amount float8, d date, ts timestamp, drop_last bool)
  SERVER ${server} OPTIONS (table_name 'af_perf_read');
ALTER FOREIGN TABLE ${schema}.dropped DROP COLUMN drop_first;
ALTER FOREIGN TABLE ${schema}.dropped DROP COLUMN drop_middle;
ALTER FOREIGN TABLE ${schema}.dropped DROP COLUMN drop_last;

-- The first two remote fields have identical types, so positional matching
-- silently returns wrong values instead of reporting a type mismatch.
CREATE FOREIGN TABLE ${schema}.reordered
  (segid int4, id int4, label text, active bool, amount float8, d date, ts timestamp)
  SERVER ${server} OPTIONS (table_name 'af_perf_read');

CREATE FOREIGN TABLE ${schema}.extra_fields
  (segid int4, id int4)
  SERVER ${server} OPTIONS (table_name 'af_perf_read');

CREATE FOREIGN TABLE ${schema}.unrequested_type
  (id int4, segid text)
  SERVER ${server} OPTIONS (table_name 'af_perf_read');

CREATE FOREIGN TABLE ${schema}.missing
  (missing_id int4, segid int4, label text, active bool, amount float8, d date, ts timestamp)
  SERVER ${server} OPTIONS (table_name 'af_perf_read');

CREATE FOREIGN TABLE ${schema}.duplicate
  (id int4, segid int4, label text, active bool, amount float8, d date, ts timestamp)
  SERVER ${server} OPTIONS (table_name 'af_perf_read_schema_duplicate');

CREATE FOREIGN TABLE ${schema}.type_mismatch
  (segid int4, id text, label text, active bool, amount float8, d date, ts timestamp)
  SERVER ${server} OPTIONS (table_name 'af_perf_read');

CREATE FOREIGN TABLE ${schema}.aliased
  (local_id int4 OPTIONS (column_name 'id'), segid int4, label text,
   active bool, amount float8, d date, ts timestamp)
  SERVER ${server} OPTIONS (table_name 'af_perf_read_schema_alias');
SQL

query() {
  # Stdin preserves every result in cursor/prepared batches; -c keeps only the last.
  PGOPTIONS="${PGOPTIONS:-} -c optimizer=${optimizer} -c timezone=UTC" \
    "${psql_cmd[@]}" -At -F '|' <<< "$1"
}

assert_equal() {
  local label="$1" expected="$2" actual="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    printf 'FAIL (optimizer=%s): %s\n' "${optimizer}" "${label}" >&2
    printf 'Expected prefix: %.200s\nActual prefix: %.200s\n' \
      "${expected}" "${actual}" >&2
    exit 1
  fi
  printf 'PASS (optimizer=%s): %s\n' "${optimizer}" "${label}"
}

expect_failure() {
  local label="$1" sql="$2" expected="$3" output
  if output="$(query "${sql}" 2>&1)"; then
    printf 'FAIL (optimizer=%s): %s unexpectedly succeeded\n' \
      "${optimizer}" "${label}" >&2
    exit 1
  fi
  if [[ "${output}" != *"${expected}"* ]]; then
    printf 'FAIL (optimizer=%s): %s\nExpected error: %s\nActual error: %s\n' \
      "${optimizer}" "${label}" "${expected}" "${output}" >&2
    exit 1
  fi
  printf 'PASS (optimizer=%s): %s\n' "${optimizer}" "${label}"
}

for optimizer in on off; do
  assert_equal 'fixture row count' "${FLIGHTSQL_SCHEMA_EXPECTED_ROWS}" \
    "$(query "SELECT count(*) FROM ${schema}.baseline")"
  assert_equal 'fixture first id and segid' '1|0' \
    "$(query "SELECT id, segid FROM ${schema}.baseline ORDER BY id LIMIT 1")"
  baseline="$(query "SELECT * FROM ${schema}.baseline ORDER BY id")"
  ids="$(query "SELECT id FROM ${schema}.baseline ORDER BY id")"

  # No sort: FETCH must leave the portal only partially consumed.
  cursor_row="$(query "
    BEGIN;
    DECLARE flightsql_schema_cursor NO SCROLL CURSOR FOR
      SELECT * FROM ${schema}.baseline;
    FETCH FORWARD 1 FROM flightsql_schema_cursor;
    CLOSE flightsql_schema_cursor;
    COMMIT;
  ")"
  cursor_row_matches=false
  if [[ -n "${cursor_row}" && "${cursor_row}" != *$'\n'* &&
        $'\n'"${baseline}"$'\n' == *$'\n'"${cursor_row}"$'\n'* ]]; then
    cursor_row_matches=true
  fi
  assert_equal 'partial cursor FETCH 1 then CLOSE and COMMIT' true "${cursor_row_matches}"

  prepared_rows="$(query "
    PREPARE flightsql_schema_read AS
      SELECT * FROM ${schema}.baseline ORDER BY id;
    EXECUTE flightsql_schema_read;
    EXECUTE flightsql_schema_read;
    DEALLOCATE flightsql_schema_read;
  ")"
  assert_equal 'parameter-free prepared read executes twice' \
    "${baseline}"$'\n'"${baseline}" "${prepared_rows}"

  assert_equal 'SELECT * after dropping first, middle and last columns' "${baseline}" \
    "$(query "SELECT * FROM ${schema}.dropped ORDER BY id")"
  assert_equal 'projection after dropping columns' "${ids}" \
    "$(query "SELECT id FROM ${schema}.dropped ORDER BY id")"
  assert_equal 'equal-width reordered schema' \
    "$(query "SELECT segid, id, label, active, amount, d, ts FROM ${schema}.baseline ORDER BY id")" \
    "$(query "SELECT * FROM ${schema}.reordered ORDER BY id")"
  assert_equal 'extra fields on SELECT *' \
    "$(query "SELECT segid, id FROM ${schema}.baseline ORDER BY id")" \
    "$(query "SELECT * FROM ${schema}.extra_fields ORDER BY id")"
  assert_equal 'unrequested incompatible type is ignored' "${ids}" \
    "$(query "SELECT id FROM ${schema}.unrequested_type ORDER BY id")"
  assert_equal 'local column alias on SELECT *' "${baseline}" \
    "$(query "SELECT * FROM ${schema}.aliased ORDER BY local_id")"
  assert_equal 'local column alias on projection' "${ids}" \
    "$(query "SELECT local_id FROM ${schema}.aliased ORDER BY local_id")"
  assert_equal 'unrequested duplicate fields are ignored' \
    "$(query "SELECT label FROM ${schema}.baseline ORDER BY label")" \
    "$(query "SELECT label FROM ${schema}.duplicate ORDER BY label")"

  expect_failure 'equal-width missing requested field' \
    "SELECT * FROM ${schema}.missing" \
    'did not return requested column "missing_id"'
  expect_failure 'missing projected field' \
    "SELECT missing_id FROM ${schema}.missing" \
    'did not return requested column "missing_id"'
  expect_failure 'equal-width duplicate requested field' \
    "SELECT * FROM ${schema}.duplicate" \
    'returned duplicate column "id"'
  expect_failure 'duplicate projected field' \
    "SELECT id FROM ${schema}.duplicate" \
    'returned duplicate column "id"'
  expect_failure 'reordered requested field type mismatch' \
    "SELECT * FROM ${schema}.type_mismatch" \
    'column "id" expects Greengage type oid 25 but Arrow Flight stream returned int32'
  expect_failure 'projected field type mismatch' \
    "SELECT id FROM ${schema}.type_mismatch" \
    'column "id" expects Greengage type oid 25 but Arrow Flight stream returned int32'
done

printf 'Flight SQL schema integration checks passed\n'
