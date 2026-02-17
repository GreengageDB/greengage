#!/bin/bash
set -eox pipefail

echo "[log-sync] started on $(hostname) at $(date)" >&2

LOG_SYNC_INTERVAL=10
HOST_LOG_DIR="/logs"
BASE_DIR="/home/gpadmin"
GPDB_SRC_DIR="gpdb_src"

mkdir -p "$HOST_LOG_DIR"

LOG_DIRS=(
  "${BASE_DIR}/gpAdminLogs"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/gpAdminLogs"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/standby/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/src/test/isolation2/results/resgroup"
  "${BASE_DIR}/${GPDB_SRC_DIR}/src/test/isolation2/regression.diffs"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast_mirror1/demoDataDir0/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast_mirror2/demoDataDir1/pg_log"
  "${BASE_DIR}/${GPDB_SRC_DIR}/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2/pg_log"
)

sync_logs(){
  rsync -a --relative --inplace --whole-file \
    --ignore-missing-args \
    "${LOG_DIRS[@]}" \
    "$HOST_LOG_DIR/" 2>/dev/null || true
}

if [[ "$LOG_SYNC_MODE" = "once" ]]; then
  sync_logs
  exit 0
fi

while true; do
  sync_logs
  sleep $LOG_SYNC_INTERVAL
done &
