#!/usr/bin/env bash
set -euo pipefail

: "${ARROWFLIGHTD_SERVER:=/tmp/arrowflightd}"
: "${ARROWFLIGHT_SMOKE_PORT:=8815}"
: "${GPDEMO_DIR:=/home/gpadmin/gpdb_src/gpAux/gpdemo}"
: "${GPHOME:=/home/gpadmin/greengage-db-devel}"

source "${GPHOME}/greengage_path.sh"

cd "${GPDEMO_DIR}"
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
  make destroy-demo-cluster >/tmp/arrowflight_fdw_orca_destroy_before.log 2>&1 || true

if ! WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
  make create-demo-cluster >/tmp/arrowflight_fdw_orca_create.log 2>&1; then
  cat /tmp/arrowflight_fdw_orca_create.log
  exit 1
fi

source "${GPDEMO_DIR}/gpdemo-env.sh"

"${ARROWFLIGHTD_SERVER}" "${ARROWFLIGHT_SMOKE_PORT}" \
  >/tmp/arrowflight_fdw_orca_server.log 2>&1 &
arrowflight_pid=$!

cleanup() {
  kill "${arrowflight_pid}" >/dev/null 2>&1 || true
  wait "${arrowflight_pid}" >/dev/null 2>&1 || true
  cd "${GPDEMO_DIR}"
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
    make destroy-demo-cluster >/tmp/arrowflight_fdw_orca_destroy_after.log 2>&1 || true
}
trap cleanup EXIT

sleep 1
kill -0 "${arrowflight_pid}"

psql -v ON_ERROR_STOP=1 postgres <<SQL >/tmp/arrowflight_fdw_orca_query.log 2>&1
CREATE EXTENSION arrowflight;
SET TIME ZONE 'UTC';
SET optimizer=on;

CREATE SERVER arrowflight_fdw_orca_srv
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

CREATE FOREIGN TABLE arrowflight_fdw_orca_smoke (
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER arrowflight_fdw_orca_srv
OPTIONS (
    path 'dataset/smoke',
    rows '4'
);

EXPLAIN SELECT count(*) FROM arrowflight_fdw_orca_smoke;

SELECT count(*) AS row_count,
       sum(id) AS id_sum,
       bool_and(label LIKE 'arrowflight-seg-%') AS labels_are_segmented
FROM arrowflight_fdw_orca_smoke;

SELECT CASE
         WHEN count(*) = 4
          AND sum(id) = 206
          AND bool_and(label LIKE 'arrowflight-seg-%')
         THEN 'ok'
         ELSE 'mismatch'
       END AS fdw_orca_smoke_result
FROM arrowflight_fdw_orca_smoke;

DROP FOREIGN TABLE arrowflight_fdw_orca_smoke;
DROP SERVER arrowflight_fdw_orca_srv;
DROP EXTENSION arrowflight CASCADE;
SQL

cat /tmp/arrowflight_fdw_orca_query.log
cat /tmp/arrowflight_fdw_orca_server.log

if ! grep -q "Optimizer: GPORCA" /tmp/arrowflight_fdw_orca_query.log; then
  echo "expected GPORCA plan" >&2
  exit 1
fi

if ! grep -q "Gather Motion 2:1.*segments: 2" \
  /tmp/arrowflight_fdw_orca_query.log; then
  echo "expected ORCA FDW plan to run on 2 segments" >&2
  exit 1
fi

if ! grep -q "Foreign Scan on arrowflight_fdw_orca_smoke" \
  /tmp/arrowflight_fdw_orca_query.log; then
  echo "expected Foreign Scan in ORCA FDW plan" >&2
  exit 1
fi

if ! grep -q "^ ok" /tmp/arrowflight_fdw_orca_query.log; then
  echo "expected ORCA FDW smoke result ok" >&2
  exit 1
fi
