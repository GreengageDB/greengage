#!/usr/bin/env bash
set -euo pipefail

# Usage: run_flightsql_transaction_fixture.sh /path/to/compiled-server
# Compose healthcheck: bash tools/run_flightsql_transaction_fixture.sh --healthcheck
# The caller generates IPC once and mounts the MPP workers' shared state volume.
if [ "${1:-}" = --healthcheck ]; then
  for port in 9030 9031 9032 9033 9034; do
    (exec 3<>"/dev/tcp/127.0.0.1/${port}") || exit 1
  done
  exit 0
fi

binary="${1:?provide the already compiled benchmark server binary}"
: "${ARROWFLIGHT_BENCH_IPC_DIR:?generate benchmark IPC before launching fixtures}"
: "${ARROWFLIGHT_MPP_STATE_DIR:?mount the shared MPP state directory}"
: "${FLIGHTSQL_TXN_LOG_DIR:=/tmp/flightsql-txn-fixtures}"
: "${FLIGHTSQL_TXN_FAULT_WORKERS:=grpc+tcp://flightsql-mpp-worker-0:9021,grpc+tcp://flightsql-mpp-worker-1:9022,grpc+tcp://flightsql-mpp-worker-fail:9024}"
[ -x "${binary}" ] || { echo "server is not executable: ${binary}" >&2; exit 1; }
mkdir -p "${FLIGHTSQL_TXN_LOG_DIR}"
pids=()

# Called indirectly by the EXIT trap.
# shellcheck disable=SC2329
cleanup() {
  if [ "${#pids[@]}" -gt 0 ]; then
    kill "${pids[@]}" 2>/dev/null || true
    for pid in "${pids[@]}"; do wait "${pid}" 2>/dev/null || true; done
  fi
}
trap 'cleanup' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

launch() {
  local port="$1"
  shift
  env -u ARROWFLIGHT_MPP_STATE_DIR -u ARROWFLIGHT_MPP_WORKERS \
    -u ARROWFLIGHT_MPP_WORKER_ID -u ARROWFLIGHT_MPP_ROUTE_LOCATION_OVERRIDE \
    -u ARROWFLIGHT_BENCH_ADVERTISED_LOCATION \
    ARROWFLIGHT_BENCH_TRANSACTIONAL=true ARROWFLIGHT_BENCH_SAVEPOINTS=true \
    ARROWFLIGHT_BENCH_END_TRANSACTION_DELAY_MS=0 \
    ARROWFLIGHT_BENCH_END_SAVEPOINT_DELAY_MS=0 \
    ARROWFLIGHT_BENCH_FAIL_SAVEPOINT_ROLLBACK=false \
    ARROWFLIGHT_BENCH_FAIL_INGEST_STREAM=-1 ARROWFLIGHT_MPP_FAIL_SEGMENT=-1 \
    ARROWFLIGHT_MPP_ABORT_DELAY_MS=0 \
    "$@" "${binary}" "${port}" >"${FLIGHTSQL_TXN_LOG_DIR}/${port}.log" 2>&1 &
  pids+=("$!")
  echo "transaction fixture port=${port} pid=$! log=${FLIGHTSQL_TXN_LOG_DIR}/${port}.log"
}

launch 9030
launch 9031 ARROWFLIGHT_BENCH_END_TRANSACTION_DELAY_MS=15000
launch 9032 ARROWFLIGHT_MPP_ABORT_DELAY_MS=15000 \
  "ARROWFLIGHT_MPP_STATE_DIR=${ARROWFLIGHT_MPP_STATE_DIR}" \
  "ARROWFLIGHT_MPP_WORKERS=${FLIGHTSQL_TXN_FAULT_WORKERS}"
launch 9033 ARROWFLIGHT_BENCH_FAIL_SAVEPOINT_ROLLBACK=true
launch 9034 ARROWFLIGHT_BENCH_END_SAVEPOINT_DELAY_MS=3500

status=0
wait -n "${pids[@]}" || status=$?
echo "transaction fixture child exited with status ${status}" >&2
# Even an unexpected clean exit leaves the required five-port fixture incomplete.
[ "${status}" -ne 0 ] || status=1
exit "${status}"
