#!/usr/bin/env bash
set -euo pipefail

: "${FLIGHTSQL_SYNTHETIC_HOST:=flightsql-synthetic}"
: "${FLIGHTSQL_SYNTHETIC_PORT:=9015}"
: "${FLIGHTSQL_SYNTHETIC_AUTO_FAIL_HOST:=flightsql-synthetic-auto-fail}"
: "${FLIGHTSQL_SYNTHETIC_AUTO_FAIL_PORT:=9016}"
: "${FLIGHTSQL_SYNTHETIC_REQUIRED_FAIL_HOST:=flightsql-synthetic-required-fail}"
: "${FLIGHTSQL_SYNTHETIC_REQUIRED_FAIL_PORT:=9017}"
: "${FLIGHTSQL_SYNTHETIC_DELAY_HOST:=flightsql-synthetic-delay}"
: "${FLIGHTSQL_SYNTHETIC_DELAY_PORT:=9018}"

suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
source_table="flightsql_synthetic_source_${suffix}"
cancel_source_table="flightsql_synthetic_cancel_source_${suffix}"
main_server="flightsql_synthetic_main_${suffix}"
auto_fail_server="flightsql_synthetic_auto_fail_${suffix}"
required_fail_server="flightsql_synthetic_required_fail_${suffix}"
delay_server="flightsql_synthetic_delay_${suffix}"
write_table="flightsql_synthetic_write_${suffix}"
readback_table="flightsql_synthetic_readback_${suffix}"
auto_fail_write="flightsql_synthetic_auto_fail_write_${suffix}"
auto_fail_readback="flightsql_synthetic_auto_fail_readback_${suffix}"
required_fail_write="flightsql_synthetic_required_fail_write_${suffix}"
required_fail_readback="flightsql_synthetic_required_fail_readback_${suffix}"
delay_read="flightsql_synthetic_delay_read_${suffix}"
temporal_write="flightsql_synthetic_temporal_write_${suffix}"
temporal_readback="flightsql_synthetic_temporal_readback_${suffix}"
cancel_write="flightsql_synthetic_cancel_write_${suffix}"
cancel_tag="flightsql_synthetic_cancel_${suffix}"
cancel_log="/tmp/${cancel_tag}.log"
cancel_status="/tmp/${cancel_tag}.status"

foreign_tables=()

cleanup() {
  {
    for table in "${foreign_tables[@]}"; do
      printf 'DROP FOREIGN TABLE IF EXISTS %s;\n' "${table}"
    done
    printf 'DROP TABLE IF EXISTS %s;\n' "${source_table}"
    printf 'DROP TABLE IF EXISTS %s;\n' "${cancel_source_table}"
    printf 'DROP SERVER IF EXISTS %s CASCADE;\n' "${main_server}"
    printf 'DROP SERVER IF EXISTS %s CASCADE;\n' "${auto_fail_server}"
    printf 'DROP SERVER IF EXISTS %s CASCADE;\n' "${required_fail_server}"
    printf 'DROP SERVER IF EXISTS %s CASCADE;\n' "${delay_server}"
  } | psql -X -q -v ON_ERROR_STOP=0 postgres >/dev/null 2>&1 || true
}
trap cleanup EXIT

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;

CREATE SERVER ${main_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SYNTHETIC_HOST}',
    port '${FLIGHTSQL_SYNTHETIC_PORT}',
    predicate_pushdown 'false',
    write_transaction_mode 'required'
);

CREATE SERVER ${auto_fail_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SYNTHETIC_AUTO_FAIL_HOST}',
    port '${FLIGHTSQL_SYNTHETIC_AUTO_FAIL_PORT}',
    predicate_pushdown 'false'
);

CREATE SERVER ${required_fail_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SYNTHETIC_REQUIRED_FAIL_HOST}',
    port '${FLIGHTSQL_SYNTHETIC_REQUIRED_FAIL_PORT}',
    predicate_pushdown 'false',
    write_transaction_mode 'required'
);

CREATE SERVER ${delay_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SYNTHETIC_DELAY_HOST}',
    port '${FLIGHTSQL_SYNTHETIC_DELAY_PORT}',
    predicate_pushdown 'false'
);

CREATE TABLE ${source_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
DISTRIBUTED RANDOMLY;

INSERT INTO ${source_table}
SELECT
    i,
    i % 3,
    'txn-' || i,
    i % 2 = 0,
    i / 10.0,
    DATE '2026-01-01' + i % 30,
    TIMESTAMP '2026-01-01' + i * INTERVAL '1 second'
FROM generate_series(1, 300) AS i;
SQL

for endpoints in 1 2 3 5; do
  table="flightsql_synthetic_ep${endpoints}_${suffix}"
  foreign_tables+=("${table}")
  psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE FOREIGN TABLE ${table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${main_server}
OPTIONS (
    table_name 'af_perf_read_ep${endpoints}',
    rows '3000'
);
SQL

  result="$(
    PGOPTIONS='-c optimizer=on -c timezone=UTC' \
      psql -X -qAt -F ':' -v ON_ERROR_STOP=1 postgres -c "
        SELECT
            count(*),
            min(id),
            max(id),
            sum(segid),
            min(label),
            bool_or(active),
            sum(amount),
            min(d),
            min(ts)
        FROM ${table}"
  )"
  IFS=':' read -r rows min_id max_id _ <<<"${result}"
  test "${rows}:${min_id}:${max_id}" = "3000:1:3000"
  execution_segments="$(
    PGOPTIONS='-c optimizer=on -c timezone=UTC' \
      psql -X -qAt -F ':' -v ON_ERROR_STOP=1 postgres -c "
        SELECT
            gp_execution_segment(),
            id,
            segid,
            label,
            active,
            amount,
            d,
            ts
        FROM ${table}" |
      cut -d ':' -f 1 |
      sort -u |
      wc -l |
      tr -d '[:space:]'
  )"
  expected_segments="${endpoints}"
  if [ "${expected_segments}" -gt 3 ]; then
    expected_segments=3
  fi
  test "${execution_segments}" = "${expected_segments}"
done

foreign_tables+=("${write_table}" "${readback_table}")
psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE FOREIGN TABLE ${write_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${main_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${readback_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${main_server}
OPTIONS (table_name 'af_perf_write');

INSERT INTO ${write_table}
SELECT * FROM ${source_table};
SQL

committed_rows="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${readback_table}
      WHERE
          id IS NOT NULL
          AND segid IS NOT NULL
          AND label IS NOT NULL
          AND active IS NOT NULL
          AND amount IS NOT NULL
          AND d IS NOT NULL
          AND ts IS NOT NULL"
)"
test "${committed_rows}" = "300"

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
BEGIN;
INSERT INTO ${write_table}
SELECT id + 1000, segid, label, active, amount, d, ts
FROM ${source_table};
ROLLBACK;
SQL

rows_after_rollback="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${readback_table}
      WHERE
          id IS NOT NULL
          AND segid IS NOT NULL
          AND label IS NOT NULL
          AND active IS NOT NULL
          AND amount IS NOT NULL
          AND d IS NOT NULL
          AND ts IS NOT NULL"
)"
test "${rows_after_rollback}" = "300"

prepare_gid="flightsql_synthetic_prepare_${suffix}"
set +e
prepare_error="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres 2>&1 <<SQL
BEGIN;
INSERT INTO ${write_table}
SELECT id + 2000, segid, label, active, amount, d, ts
FROM ${source_table};
PREPARE TRANSACTION '${prepare_gid}';
SQL
)"
prepare_status=$?
set -e
test "${prepare_status}" -ne 0
if ! grep -Eq \
  "PREPARE TRANSACTION is not yet supported in Greengage Database|cannot prepare a Greengage transaction with an active Flight SQL transaction" \
  <<<"${prepare_error}"; then
  printf '%s\n' "${prepare_error}" >&2
  exit 1
fi

rows_after_prepare="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres \
      -c "
        SELECT count(*)
        FROM ${readback_table}
        WHERE
            id IS NOT NULL
            AND segid IS NOT NULL
            AND label IS NOT NULL
            AND active IS NOT NULL
            AND amount IS NOT NULL
            AND d IS NOT NULL
            AND ts IS NOT NULL"
)"
test "${rows_after_prepare}" = "300"

foreign_tables+=("${auto_fail_write}" "${auto_fail_readback}")
psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE FOREIGN TABLE ${auto_fail_write}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${auto_fail_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${auto_fail_readback}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${auto_fail_server}
OPTIONS (table_name 'af_perf_write');
SQL

set +e
auto_fail_error="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres \
    -c "INSERT INTO ${auto_fail_write} SELECT * FROM ${source_table}" \
    2>&1
)"
auto_fail_status=$?
set -e
test "${auto_fail_status}" -ne 0
grep -q "injected benchmark ingest failure" <<<"${auto_fail_error}"

auto_fail_rows="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${auto_fail_readback}
      WHERE
          id IS NOT NULL
          AND segid IS NOT NULL
          AND label IS NOT NULL
          AND active IS NOT NULL
          AND amount IS NOT NULL
          AND d IS NOT NULL
          AND ts IS NOT NULL"
)"
test "${auto_fail_rows}" -gt 0
test "${auto_fail_rows}" -lt 300

foreign_tables+=("${required_fail_write}" "${required_fail_readback}")
psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE FOREIGN TABLE ${required_fail_write}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${required_fail_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${required_fail_readback}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${required_fail_server}
OPTIONS (table_name 'af_perf_write');
SQL

set +e
required_fail_error="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres \
    -c "INSERT INTO ${required_fail_write} SELECT * FROM ${source_table}" \
    2>&1
)"
required_fail_status=$?
set -e
test "${required_fail_status}" -ne 0
grep -q "injected benchmark ingest failure" <<<"${required_fail_error}"

required_fail_rows="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${required_fail_readback}
      WHERE
          id IS NOT NULL
          AND segid IS NOT NULL
          AND label IS NOT NULL
          AND active IS NOT NULL
          AND amount IS NOT NULL
          AND d IS NOT NULL
          AND ts IS NOT NULL"
)"
test "${required_fail_rows}" = "0"

foreign_tables+=(
  "${delay_read}"
  "${temporal_write}"
  "${temporal_readback}"
  "${cancel_write}"
)
psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE FOREIGN TABLE ${delay_read}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${delay_server}
OPTIONS (
    table_name 'af_perf_read_ep3',
    rows '3000'
);

CREATE FOREIGN TABLE ${temporal_write}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER ${delay_server}
OPTIONS (
    table_name 'af_perf_write',
    write_transaction_mode 'required'
);

CREATE FOREIGN TABLE ${temporal_readback}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER ${delay_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${cancel_write}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER ${delay_server}
OPTIONS (
    table_name 'af_perf_write',
    write_transaction_mode 'required',
    batch_rows '128'
);

INSERT INTO ${temporal_write}
VALUES
    (5001, 0, 'boundary-1900', true, 1.0,
     DATE '1900-01-01', TIMESTAMPTZ '1900-01-01 00:00:00+00'),
    (5002, 1, 'boundary-1970', false, 2.0,
     DATE '1969-12-31', TIMESTAMPTZ '1969-12-31 23:59:59.999999+00'),
    (5003, 2, 'boundary-2000', true, 3.0,
     DATE '2000-01-01', TIMESTAMPTZ '2000-01-01 00:00:00+00'),
    (5004, 0, 'boundary-2038', false, 4.0,
     DATE '2038-01-19', TIMESTAMPTZ '2038-01-19 03:14:07+00'),
    (5005, 1, 'boundary-2100', true, 5.0,
     DATE '2100-12-31', TIMESTAMPTZ '2100-12-31 23:59:59.999999+00'),
    (6001, NULL, NULL, NULL, NULL, NULL, NULL),
    (6002, NULL, NULL, NULL, NULL, NULL, NULL),
    (6003, NULL, NULL, NULL, NULL, NULL, NULL),
    (6004, NULL, NULL, NULL, NULL, NULL, NULL);
SQL

temporal_result="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -F ':' -v ON_ERROR_STOP=1 postgres -c "
      SELECT
          count(*),
          min(id),
          max(id),
          sum(segid),
          min(label),
          bool_or(active),
          sum(amount),
          min(d),
          max(d),
          min(ts),
          max(ts)
      FROM ${temporal_readback}
      WHERE id BETWEEN 5001 AND 5005"
)"
test "${temporal_result}" = \
  "5:5001:5005:4:boundary-1900:t:15:1900-01-01:2100-12-31:1900-01-01 00:00:00+00:2100-12-31 23:59:59.999999+00"

null_result="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -F ':' -v ON_ERROR_STOP=1 postgres -c "
      SELECT
          count(*),
          count(segid),
          count(label),
          count(active),
          count(amount),
          count(d),
          count(ts)
      FROM ${temporal_readback}
      WHERE id BETWEEN 6001 AND 6004"
)"
test "${null_result}" = "4:0:0:0:0:0:0"

(
  set +e
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres -c "
      SELECT /* ${cancel_tag} */
          count(*),
          min(id),
          max(id),
          sum(segid),
          min(label),
          bool_or(active),
          sum(amount),
          min(d),
          min(ts)
      FROM ${delay_read}" >"${cancel_log}" 2>&1
  echo "$?" >"${cancel_status}"
) &
cancel_client_pid=$!

backend_pid=""
for _ in $(seq 1 100); do
  backend_pid="$(
    psql -X -qAt postgres -c "
      SELECT pid
      FROM pg_stat_activity
      WHERE query LIKE '%${cancel_tag}%'
        AND pid <> pg_backend_pid()
      LIMIT 1"
  )"
  if [ -n "${backend_pid}" ]; then
    break
  fi
  sleep 0.05
done
test -n "${backend_pid}"
test "$(
  psql -X -qAt postgres \
    -c "SELECT pg_cancel_backend(${backend_pid})"
)" = "t"
wait "${cancel_client_pid}" || true
test "$(cat "${cancel_status}")" -ne 0
grep -q "canceling statement due to user request" "${cancel_log}"

set +e
motion_error="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${delay_read}
      WHERE id + segid + label::int + active::int + amount::int +
            (d - DATE '2000-01-01') +
            extract(epoch FROM ts)::int > 0" \
    2>&1
)"
motion_status=$?
set -e
test "${motion_status}" -ne 0
grep -q "invalid input syntax for type integer" <<<"${motion_error}"

post_cancel_rows="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${delay_read}
      WHERE
          id IS NOT NULL
          AND segid IS NOT NULL
          AND label IS NOT NULL
          AND active IS NOT NULL
          AND amount IS NOT NULL
          AND d IS NOT NULL
          AND ts IS NOT NULL"
)"
test "${post_cancel_rows}" = "3000"

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE TABLE ${cancel_source_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
DISTRIBUTED RANDOMLY;

INSERT INTO ${cancel_source_table}
SELECT
    i,
    i % 3,
    'cancel-' || i,
    i % 2 = 0,
    i / 10.0,
    DATE '2026-03-01' + i % 30,
    TIMESTAMPTZ '2026-03-01 00:00:00+00' +
        i * INTERVAL '1 second'
FROM generate_series(10000, 39999) AS i;
SQL

write_cancel_tag="flightsql_synthetic_write_cancel_${suffix}"
write_cancel_log="/tmp/${write_cancel_tag}.log"
write_cancel_status="/tmp/${write_cancel_tag}.status"
(
  set +e
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres -c "
      INSERT /* ${write_cancel_tag} */ INTO ${cancel_write}
      SELECT * FROM ${cancel_source_table}" \
    >"${write_cancel_log}" 2>&1
  echo "$?" >"${write_cancel_status}"
) &
write_cancel_client_pid=$!

write_backend_pid=""
for _ in $(seq 1 100); do
  write_backend_pid="$(
    psql -X -qAt postgres -c "
      SELECT pid
      FROM pg_stat_activity
      WHERE query LIKE '%${write_cancel_tag}%'
        AND pid <> pg_backend_pid()
      LIMIT 1"
  )"
  if [ -n "${write_backend_pid}" ]; then
    break
  fi
  sleep 0.05
done
test -n "${write_backend_pid}"
sleep 0.2
test "$(
  psql -X -qAt postgres \
    -c "SELECT pg_cancel_backend(${write_backend_pid})"
)" = "t"
wait "${write_cancel_client_pid}" || true
test "$(cat "${write_cancel_status}")" -ne 0
grep -q "canceling statement due to user request" "${write_cancel_log}"

rows_after_write_cancel="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      SELECT count(*)
      FROM ${temporal_readback}
      WHERE id IS NOT NULL OR
            segid IS NULL OR
            label IS NULL OR
            active IS NULL OR
            amount IS NULL OR
            d IS NULL OR
            ts IS NULL"
)"
test "${rows_after_write_cancel}" = "9"

echo "flightsql_synthetic_endpoints=1,2,3,5"
echo "flightsql_synthetic_transaction_commit=ok"
echo "flightsql_synthetic_transaction_rollback=ok"
echo "flightsql_synthetic_prepare_transaction_rejected=ok"
echo "flightsql_synthetic_opaque_transaction_id=ok"
echo "flightsql_synthetic_auto_commit_partial_failure_rows=${auto_fail_rows}"
echo "flightsql_synthetic_required_failure_rollback=ok"
echo "flightsql_synthetic_temporal_utc=ok"
echo "flightsql_synthetic_null_heavy=ok"
echo "flightsql_synthetic_cancel_cleanup=ok"
echo "flightsql_synthetic_motion_cleanup=ok"
echo "flightsql_synthetic_write_cancel_cleanup=ok"
