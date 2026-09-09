#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROWFLIGHT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${ARROWFLIGHT_DIR}/docker-compose.flightsql.yml"
PROJECT_NAME="${FLIGHTSQL_COMPOSE_PROJECT:-arrowflight-sql}"

cd "${ARROWFLIGHT_DIR}"

cleanup() {
  if [ "${FLIGHTSQL_KEEP_COMPOSE:-0}" != "1" ]; then
    docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v
  fi
}
trap cleanup EXIT

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" build greengage
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up -d \
  clickhouse1 clickhouse2 greengage

for service in clickhouse1 clickhouse2 greengage; do
  machine="$(
    docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" \
      exec -T "${service}" uname -m
  )"
  if [ "${machine}" != "aarch64" ] && [ "${machine}" != "arm64" ]; then
    echo "${service} is ${machine}; the Flight SQL integration requires ARM64" >&2
    exit 1
  fi
done

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T \
  greengage bash -lc "
    set -euo pipefail
    mkdir -p /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test
    cp -a /workspace/gpcontrib/arrowflight/. \
      /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test/
    chown -R gpadmin:gpadmin \
      /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test
  "

docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T \
  -e FLIGHTSQL_HOST=clickhouse1 \
  -e FLIGHTSQL_PORT=9005 \
  -e CLICKHOUSE_HTTP_URL=http://clickhouse1:8123/ \
  -e CLICKHOUSE_HTTP_URL_2=http://clickhouse2:8123/ \
  greengage bash -lc "
    set -euo pipefail
    /usr/sbin/sshd
    runuser -u gpadmin -- bash -lc '
      set -euo pipefail
      source /home/gpadmin/greengage-db-devel/greengage_path.sh
      cd /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test
      make clean USE_ARROW_FLIGHT=1 >/dev/null 2>&1 || true
      make -s USE_ARROW_FLIGHT=1
      make -s install USE_ARROW_FLIGHT=1

      cd /home/gpadmin/gpdb_src/gpAux/gpdemo
      WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=3 \
        make destroy-demo-cluster >/dev/null 2>&1 || true
      WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=3 \
        make create-demo-cluster
      source gpdemo-env.sh

      cd /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test
      make -s installcheck USE_ARROW_FLIGHT=1
      bash tools/run_flightsql_clickhouse_integration.sh
    '
  "

echo "flightsql_clickhouse_compose=ok"
