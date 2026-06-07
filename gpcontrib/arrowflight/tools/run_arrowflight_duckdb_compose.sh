#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLIGHT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${ARROWFLIGHT_DIR}/docker-compose.duckdb.yml"
PROJECT_NAME="${ARROWFLIGHT_DUCKDB_COMPOSE_PROJECT:-arrowflight-duckdb}"

cd "${ARROWFLIGHT_DIR}"

cleanup() {
  if [ "${ARROWFLIGHT_DUCKDB_KEEP_COMPOSE:-0}" != "1" ]; then
    docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v
  fi
}
trap cleanup EXIT

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
bash tools/run_arrowflight_duckdb_integration.sh
'
"

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" logs --no-color duckdb-flight |
  grep -Ev 'source_node\.cc:77|input buffer was poorly aligned|Please ensure that all Acero sources generate aligned buffers' || true

echo "arrowflight_duckdb_compose=ok"
