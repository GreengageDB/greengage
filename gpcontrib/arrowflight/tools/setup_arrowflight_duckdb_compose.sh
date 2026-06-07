#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLIGHT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${ARROWFLIGHT_DIR}/docker-compose.duckdb.yml"
PROJECT_NAME="${ARROWFLIGHT_DUCKDB_COMPOSE_PROJECT:-arrowflight-duckdb}"

cd "${ARROWFLIGHT_DIR}"

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up --build -d duckdb-flight greengage

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T greengage bash -lc "
set -euo pipefail
/usr/sbin/sshd
rm -rf /home/gpadmin/gpdb_src/gpcontrib/arrowflight
mkdir -p /home/gpadmin/gpdb_src/gpcontrib/arrowflight
cp -a /workspace/gpcontrib/arrowflight/. /home/gpadmin/gpdb_src/gpcontrib/arrowflight/
chown -R gpadmin:gpadmin /home/gpadmin/gpdb_src/gpcontrib/arrowflight
runuser -u gpadmin -- bash -lc '
set -euo pipefail
source /home/gpadmin/greengage-db-devel/greengage_path.sh
cd /home/gpadmin/gpdb_src/gpcontrib/arrowflight
make clean USE_ARROW_FLIGHT=1 >/tmp/arrowflight_duckdb_make_clean.log 2>&1 || true
make USE_ARROW_FLIGHT=1
make install USE_ARROW_FLIGHT=1
cd /home/gpadmin/gpdb_src/gpAux/gpdemo
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 make destroy-demo-cluster >/tmp/arrowflight_duckdb_manual_destroy_before.log 2>&1 || true
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 make create-demo-cluster
source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
psql -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;
SET TIME ZONE '\\''UTC'\\'';
SET optimizer=off;

DROP FOREIGN TABLE IF EXISTS duckdb_sales_write;
DROP FOREIGN TABLE IF EXISTS duckdb_sales;
DROP TABLE IF EXISTS duckdb_write_source;
DROP SERVER IF EXISTS arrowflight_duckdb_srv CASCADE;

CREATE SERVER arrowflight_duckdb_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    mpp_execute '\\''all segments'\\'',
    host '\\''duckdb-flight'\\'',
    port '\\''8815'\\'',
    write_mode '\\''staging'\\'',
    timeout_ms '\\''10000'\\'',
    retry_count '\\''1'\\'',
    retry_backoff_ms '\\''50'\\'',
    use_get_flight_info '\\''true'\\'',
    flight_endpoint_policy '\\''segment_index'\\'',
    projection_pushdown '\\''try'\\''
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
    path '\\''dataset/sales'\\'',
    rows '\\''8'\\''
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
    path '\\''dataset/sales_write'\\'',
    operation_metadata '\\''static.source=manual_compose,static.job_id=manual_1'\\'',
    rows '\\''6'\\'',
    batch_rows '\\''2'\\'',
    max_batch_bytes '\\''4096'\\'',
    retry_count '\\''0'\\''
);

CREATE TABLE duckdb_write_source (
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
DISTRIBUTED BY (id);

INSERT INTO duckdb_write_source
SELECT segid * 100 + n AS id,
       segid,
       '\\''manual-write-seg-'\\'' || segid::text || '\\''-row-'\\'' || n::text AS label,
       (n % 2 = 1) AS active,
       n::float8 + 20.5 AS amount,
       DATE '\\''2026-06-07'\\'' + n AS d,
       TIMESTAMP '\\''2026-06-07 14:00:00'\\'' + (n || '\\'' seconds'\\'')::interval AS ts
FROM generate_series(0, 1) AS s(segid),
     generate_series(1, 3) AS g(n);
SQL
'
"

cat <<EOF
arrowflight_duckdb_compose_setup=ok

Greengage:
  docker compose -p ${PROJECT_NAME} -f ${COMPOSE_FILE} exec greengage runuser -u gpadmin -- bash -lc 'source /home/gpadmin/greengage-db-devel/greengage_path.sh; source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh; psql postgres'

Host psql, if your local psql can reach the mapped port and pg_hba permits it:
  psql -h localhost -p ${ARROWFLIGHT_GG_PORT:-7000} -U gpadmin -d postgres

DuckDB debug SQL endpoint:
  curl -s -X POST http://localhost:8816/query --data 'SELECT count(*) FROM sales'

Stop and remove:
  docker compose -p ${PROJECT_NAME} -f ${COMPOSE_FILE} down -v
EOF
