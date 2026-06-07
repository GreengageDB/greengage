#!/usr/bin/env bash
set -euo pipefail

: "${ARROWFLIGHTD_SERVER:=/tmp/arrowflightd}"
: "${ARROWFLIGHT_SMOKE_PORT:=8815}"
: "${GPDEMO_DIR:=/home/gpadmin/gpdb_src/gpAux/gpdemo}"
: "${GPHOME:=/home/gpadmin/greengage-db-devel}"

source "${GPHOME}/greengage_path.sh"

cd "${GPDEMO_DIR}"
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
  make destroy-demo-cluster >/tmp/arrowflight_fdw_destroy_before.log 2>&1 || true

if ! WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
  make create-demo-cluster >/tmp/arrowflight_fdw_create.log 2>&1; then
  cat /tmp/arrowflight_fdw_create.log
  exit 1
fi

source "${GPDEMO_DIR}/gpdemo-env.sh"

"${ARROWFLIGHTD_SERVER}" "${ARROWFLIGHT_SMOKE_PORT}" \
  >/tmp/arrowflight_fdw_server.log 2>&1 &
arrowflight_pid=$!

cleanup() {
  kill "${arrowflight_pid}" >/dev/null 2>&1 || true
  wait "${arrowflight_pid}" >/dev/null 2>&1 || true
  cd "${GPDEMO_DIR}"
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
    make destroy-demo-cluster >/tmp/arrowflight_fdw_destroy_after.log 2>&1 || true
}
trap cleanup EXIT

sleep 1
kill -0 "${arrowflight_pid}"

psql -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION arrowflight;
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE SERVER arrowflight_fdw_smoke_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    mpp_execute 'all segments',
    host '127.0.0.1',
    port '${ARROWFLIGHT_SMOKE_PORT}',
    timeout_ms '5000',
    retry_count '1',
    retry_backoff_ms '10',
    use_get_flight_info 'true',
    flight_endpoint_policy 'segment_index'
);

CREATE FOREIGN TABLE arrowflight_fdw_smoke (
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER arrowflight_fdw_smoke_srv
OPTIONS (
    path 'dataset/smoke',
    rows '4'
);

EXPLAIN (COSTS OFF)
SELECT count(*) FROM arrowflight_fdw_smoke;

SELECT count(*) AS row_count,
       sum(id) AS id_sum,
       bool_and(label LIKE 'arrowflight-seg-%') AS labels_are_segmented
FROM arrowflight_fdw_smoke;

SELECT CASE
       WHEN count(*) = 4
            AND sum(id) = 206
            AND bool_and(label LIKE 'arrowflight-seg-%')
       THEN 'ok'
       ELSE 'bad'
       END AS fdw_smoke_result
FROM arrowflight_fdw_smoke;

CREATE FOREIGN TABLE arrowflight_fdw_projection_smoke (
    id int4,
    label text,
    active bool,
    amount numeric,
    d date,
    ts timestamp
)
SERVER arrowflight_fdw_smoke_srv
OPTIONS (
    path 'dataset/smoke',
    rows '4'
);

SELECT CASE
       WHEN count(*) = 4 THEN 'ok'
       ELSE 'bad'
       END AS fdw_projection_count_result
FROM arrowflight_fdw_projection_smoke;

DROP FOREIGN TABLE arrowflight_fdw_smoke;
DROP FOREIGN TABLE arrowflight_fdw_projection_smoke;
DROP SERVER arrowflight_fdw_smoke_srv;
DROP EXTENSION arrowflight CASCADE;
SQL

psql -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION arrowflight;
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE SERVER arrowflight_fdw_smoke_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    mpp_execute 'all segments',
    host '127.0.0.1',
    port '${ARROWFLIGHT_SMOKE_PORT}',
    timeout_ms '5000',
    retry_count '1',
    retry_backoff_ms '10',
    use_get_flight_info 'true',
    flight_endpoint_policy 'segment_index'
);

CREATE FOREIGN TABLE arrowflight_fdw_projection_error_smoke (
    id int4,
    label text,
    active bool,
    amount numeric,
    d date,
    ts timestamp
)
SERVER arrowflight_fdw_smoke_srv
OPTIONS (
    path 'dataset/smoke',
    rows '4'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_date64_alignment (
    d date
)
SERVER arrowflight_fdw_smoke_srv
OPTIONS (
    path 'dataset/bad_date64_alignment',
    rows '1'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_date32_range (
    d date
)
SERVER arrowflight_fdw_smoke_srv
OPTIONS (
    path 'dataset/bad_date32_range',
    rows '1'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_time64_range (
    t time
)
SERVER arrowflight_fdw_smoke_srv
OPTIONS (
    path 'dataset/bad_time64_range',
    rows '1'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_timestamp_overflow (
    ts timestamp
)
SERVER arrowflight_fdw_smoke_srv
OPTIONS (
    path 'dataset/bad_timestamp_overflow',
    rows '1'
);
SQL

set +e
psql -v ON_ERROR_STOP=1 postgres <<SQL >/tmp/arrowflight_fdw_projection_error.log 2>&1
SELECT sum(amount) FROM arrowflight_fdw_projection_error_smoke;
SQL
projection_error_rc=$?
set -e

cat /tmp/arrowflight_fdw_projection_error.log

if [ "${projection_error_rc}" -eq 0 ]; then
  echo "expected projected unsupported type query to fail" >&2
  exit 1
fi

if ! grep -Eqi "unsupported type oid|expects Greengage type oid" \
  /tmp/arrowflight_fdw_projection_error.log; then
  echo "expected projected unsupported type message was not found" >&2
  exit 1
fi

expect_temporal_error() {
  local name="$1"
  local query="$2"
  local pattern="$3"
  local logfile="/tmp/arrowflight_fdw_${name}.log"

  set +e
  psql -v ON_ERROR_STOP=1 postgres <<SQL >"${logfile}" 2>&1
${query}
SQL
  local query_rc=$?
  set -e

  cat "${logfile}"

  if [ "${query_rc}" -eq 0 ]; then
    echo "expected ${name} query to fail" >&2
    exit 1
  fi

  if ! grep -Eqi "${pattern}" "${logfile}"; then
    echo "expected ${name} diagnostic was not found" >&2
    exit 1
  fi
}

expect_temporal_error \
  "bad_date64_alignment" \
  "SELECT * FROM arrowflight_fdw_bad_date64_alignment;" \
  "date64 value is not day-aligned"
expect_temporal_error \
  "bad_date32_range" \
  "SELECT * FROM arrowflight_fdw_bad_date32_range;" \
  "date value is out of range"
expect_temporal_error \
  "bad_time64_range" \
  "SELECT * FROM arrowflight_fdw_bad_time64_range;" \
  "time value is out of range"
expect_temporal_error \
  "bad_timestamp_overflow" \
  "SELECT * FROM arrowflight_fdw_bad_timestamp_overflow;" \
  "timestamp value is out of range"

psql -v ON_ERROR_STOP=1 postgres <<SQL
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_projection_error_smoke;
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_date64_alignment;
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_date32_range;
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_time64_range;
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_timestamp_overflow;
DROP SERVER IF EXISTS arrowflight_fdw_smoke_srv;
DROP EXTENSION IF EXISTS arrowflight CASCADE;
SQL

cat /tmp/arrowflight_fdw_server.log
