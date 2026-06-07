#!/usr/bin/env bash
set -euo pipefail

: "${ARROWFLIGHT_DUCKDB_HOST:=duckdb-flight}"
: "${ARROWFLIGHT_DUCKDB_PORT:=8815}"
: "${ARROWFLIGHT_DUCKDB_SEGMENTS:=2}"
: "${GPDEMO_DIR:=/home/gpadmin/gpdb_src/gpAux/gpdemo}"
: "${GPHOME:=/home/gpadmin/greengage-db-devel}"

source "${GPHOME}/greengage_path.sh"

cd "${GPDEMO_DIR}"
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_DUCKDB_SEGMENTS}" \
  make destroy-demo-cluster >/tmp/arrowflight_duckdb_destroy_before.log 2>&1 || true

if ! WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_DUCKDB_SEGMENTS}" \
  make create-demo-cluster >/tmp/arrowflight_duckdb_create.log 2>&1; then
  cat /tmp/arrowflight_duckdb_create.log
  exit 1
fi

source "${GPDEMO_DIR}/gpdemo-env.sh"

cleanup() {
  cd "${GPDEMO_DIR}"
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_DUCKDB_SEGMENTS}" \
    make destroy-demo-cluster >/tmp/arrowflight_duckdb_destroy_after.log 2>&1 || true
}
trap cleanup EXIT

psql -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION arrowflight;
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE SERVER arrowflight_duckdb_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    mpp_execute 'all segments',
    host '${ARROWFLIGHT_DUCKDB_HOST}',
    port '${ARROWFLIGHT_DUCKDB_PORT}',
    write_mode 'staging',
    timeout_ms '10000',
    retry_count '1',
    retry_backoff_ms '50',
    use_get_flight_info 'true',
    flight_endpoint_policy 'segment_index',
    projection_pushdown 'require'
);

CREATE FOREIGN TABLE duckdb_sales (
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER arrowflight_duckdb_srv
OPTIONS (
    path 'dataset/sales',
    rows '8'
);

CREATE FOREIGN TABLE duckdb_sales_write (
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER arrowflight_duckdb_srv
OPTIONS (
    path 'dataset/sales_write',
    operation_metadata 'static.source=duckdb_compose_integration,static.job_id=duckdb_af_1',
    rows '6',
    batch_rows '2',
    max_batch_bytes '4096',
    retry_count '0'
);

INSERT INTO duckdb_sales_write
SELECT gp_segment_id * 100 + n AS id,
       gp_segment_id AS segid,
       'gg-write-seg-' || gp_segment_id::text || '-row-' || n::text AS label,
       (n % 2 = 1) AS active,
       n::float8 + 10.5 AS amount,
       DATE '2026-06-07' + n AS d,
       TIMESTAMP '2026-06-07 13:00:00' + (n || ' seconds')::interval AS ts
FROM gp_dist_random('gp_id'), generate_series(1, 3) AS g(n);

SQL

read_result="$(psql -X -qAt -F '|' -v ON_ERROR_STOP=1 postgres <<'SQL'
SET optimizer=off;
SET TIME ZONE 'UTC';
SELECT count(*)::bigint,
       coalesce(sum(id), 0)::bigint,
       coalesce(sum(segid), 0)::bigint,
       coalesce(round(sum(amount)::numeric, 1), 0)::text,
       min(label),
       max(label),
       to_char(max(ts), 'YYYY-MM-DD HH24:MI:SS')
FROM duckdb_sales;
SQL
)"
expected_read="8|420|4|24.0|duckdb-seg-0-row-1|duckdb-seg-1-row-4|2026-06-07 12:00:04"
if [ "${read_result}" != "${expected_read}" ]; then
  echo "unexpected DuckDB read result: ${read_result}" >&2
  echo "expected: ${expected_read}" >&2
  exit 1
fi

write_result="$(psql -X -qAt -F '|' -v ON_ERROR_STOP=1 postgres <<'SQL'
SET optimizer=off;
SET TIME ZONE 'UTC';
SELECT count(*)::bigint,
       coalesce(sum(id), 0)::bigint,
       coalesce(sum(segid), 0)::bigint,
       coalesce(round(sum(amount)::numeric, 1), 0)::text,
       min(label),
       max(label),
       to_char(max(ts), 'YYYY-MM-DD HH24:MI:SS')
FROM duckdb_sales_write;
SQL
)"
expected_write="6|312|3|75.0|gg-write-seg-0-row-1|gg-write-seg-1-row-3|2026-06-07 13:00:03"
if [ "${write_result}" != "${expected_write}" ]; then
  echo "unexpected DuckDB write readback result: ${write_result}" >&2
  echo "expected: ${expected_write}" >&2
  exit 1
fi

psql -v ON_ERROR_STOP=1 postgres <<'SQL'
DROP FOREIGN TABLE IF EXISTS duckdb_sales_write;
DROP FOREIGN TABLE IF EXISTS duckdb_sales;
DROP SERVER IF EXISTS arrowflight_duckdb_srv;
DROP EXTENSION IF EXISTS arrowflight CASCADE;
SQL

echo "duckdb_read_result=${read_result}"
echo "duckdb_write_readback_result=${write_result}"
echo "arrowflight_duckdb_integration=ok"
