#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
arrowflight_dir="$(cd "${script_dir}/.." && pwd)"
base_compose="${arrowflight_dir}/docker-compose.flightsql.yml"
synthetic_compose="${arrowflight_dir}/docker-compose.flightsql-synthetic.yml"
project="${FLIGHTSQL_SYNTHETIC_COMPOSE_PROJECT:-arrowflight-sql-synthetic}"
security_dir="$(mktemp -d "${TMPDIR:-/tmp}/flightsql-security.XXXXXX")"
export FLIGHTSQL_SECURITY_DIR="${security_dir}"

bash "${script_dir}/generate_flightsql_test_certificates.sh" "${security_dir}"

compose() {
  docker compose -p "${project}" \
    -f "${base_compose}" -f "${synthetic_compose}" "$@"
}

require_log() {
  local service="$1"
  local pattern="$2"
  local logs
  local attempt

  for attempt in $(seq 1 50); do
    logs="$(compose logs --no-color "${service}")"
    if grep -q "${pattern}" <<<"${logs}"; then
      return 0
    fi
    sleep 0.1
  done

  echo "${service} log does not contain: ${pattern}" >&2
  return 1
}

require_log_from_any() {
  local pattern="$1"
  shift
  local logs
  local attempt

  for attempt in $(seq 1 50); do
    logs="$(compose logs --no-color "$@")"
    if grep -q "${pattern}" <<<"${logs}"; then
      return 0
    fi
    sleep 0.1
  done

  echo "control logs do not contain: ${pattern}" >&2
  return 1
}

cleanup() {
  if [ "${FLIGHTSQL_KEEP_COMPOSE:-0}" != "1" ]; then
    compose down -v
    rm -rf "${security_dir}"
  else
    echo "Flight SQL test certificates retained at ${security_dir}" >&2
  fi
}
trap cleanup EXIT

compose build greengage
compose up -d --no-deps --wait --wait-timeout 300 \
  flightsql-synthetic \
  flightsql-synthetic-auto-fail \
  flightsql-synthetic-required-fail \
  flightsql-synthetic-delay \
  flightsql-synthetic-secure \
  flightsql-mpp-control \
  flightsql-mpp-fail-control \
  flightsql-mpp-no-cluster-control \
  flightsql-mpp-worker-0 \
  flightsql-mpp-worker-1 \
  flightsql-mpp-worker-2 \
  flightsql-mpp-worker-fail \
  greengage

for service in \
  flightsql-synthetic \
  flightsql-synthetic-auto-fail \
  flightsql-synthetic-required-fail \
  flightsql-synthetic-delay \
  flightsql-synthetic-secure \
  flightsql-mpp-control \
  flightsql-mpp-fail-control \
  flightsql-mpp-no-cluster-control \
  flightsql-mpp-worker-0 \
  flightsql-mpp-worker-1 \
  flightsql-mpp-worker-2 \
  flightsql-mpp-worker-fail \
  greengage; do
  machine="$(compose exec -T "${service}" uname -m)"
  if [ "${machine}" != "aarch64" ] && [ "${machine}" != "arm64" ]; then
    echo "${service} is ${machine}; the Flight SQL integration requires ARM64" >&2
    exit 1
  fi
done

compose exec -T greengage bash -lc "
  set -euo pipefail
  install -d -o gpadmin -g gpadmin -m 0700 /tmp/flightsql-security
  cp /run/flightsql-security-source/* /tmp/flightsql-security/
  chown gpadmin:gpadmin /tmp/flightsql-security/*
  chmod 0600 /tmp/flightsql-security/*
  mkdir -p /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test
  cp -a /workspace/gpcontrib/arrowflight/. \
    /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test/
  chown -R gpadmin:gpadmin \
    /home/gpadmin/gpdb_src/gpcontrib/arrowflight_test
"

compose exec -T greengage bash -lc "
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
    bash tools/run_flightsql_synthetic_integration.sh
    bash tools/run_flightsql_mpp_integration.sh
    bash tools/run_flightsql_security_integration.sh
  '
"

abandoned_operation="$(
  compose exec -T flightsql-mpp-control \
    /tmp/flightsql-benchmark-server \
      --create-abandoned-mpp-plan flightsql-mpp-control 9020 3 |
    tr -d '[:space:]'
)"
if [ -z "${abandoned_operation}" ]; then
  echo "MPP lease test did not return an operation id" >&2
  exit 1
fi
abandoned_status=""
for _ in $(seq 1 50); do
  abandoned_status="$(
    compose exec -T flightsql-mpp-control \
      cat "/var/lib/flightsql-mpp/plans/${abandoned_operation}/status"
  )"
  if [ "${abandoned_status}" = "expired" ]; then
    break
  fi
  sleep 0.1
done
if [ "${abandoned_status}" != "expired" ]; then
  echo "abandoned MPP plan remained ${abandoned_status}" >&2
  exit 1
fi

for service in \
  flightsql-synthetic \
  flightsql-synthetic-auto-fail \
  flightsql-synthetic-required-fail \
  flightsql-synthetic-delay \
  flightsql-synthetic-secure \
  flightsql-mpp-control \
  flightsql-mpp-fail-control \
  flightsql-mpp-no-cluster-control \
  flightsql-mpp-worker-0 \
  flightsql-mpp-worker-1 \
  flightsql-mpp-worker-2 \
  flightsql-mpp-worker-fail; do
  last_active="$(
    compose logs --no-color "${service}" |
      grep 'active=' |
      tail -1 || true
  )"
  if [ -n "${last_active}" ] &&
     ! grep -q 'active=0$' <<<"${last_active}"; then
    echo "${service} retained an active Flight stream" >&2
    exit 1
  fi
done

for worker in 0 1 2; do
  if ! compose exec -T flightsql-mpp-control bash -lc \
      "grep -l ' worker-${worker}$' \
        /var/lib/flightsql-mpp/plans/*/route_${worker}.done \
        >/dev/null 2>&1"; then
    echo "MPP route ${worker} was not completed by worker ${worker}" >&2
    exit 1
  fi
done

control_logs="$(compose logs --no-color flightsql-mpp-control)"
if grep -q "flightsql_benchmark_write" <<<"${control_logs}"; then
  echo "MPP control service received an Arrow ingest payload" >&2
  exit 1
fi

completed_plans="$(
  compose exec -T flightsql-mpp-control bash -lc \
    "grep -rl '^completed$' /var/lib/flightsql-mpp/plans/*/status 2>/dev/null | wc -l" |
    tr -d '[:space:]'
)"
aborted_plans="$(
  compose exec -T flightsql-mpp-control bash -lc \
    "grep -rl '^aborted$' /var/lib/flightsql-mpp/plans/*/status 2>/dev/null | wc -l" |
    tr -d '[:space:]'
)"
if [ "${completed_plans}" -lt 4 ]; then
  echo "expected at least four completed MPP plans, got ${completed_plans}" >&2
  exit 1
fi
if [ "${aborted_plans}" -lt 2 ]; then
  echo "expected top-level and savepoint MPP plan aborts, got ${aborted_plans}" >&2
  exit 1
fi

fail_control_logs="$(compose logs --no-color flightsql-mpp-fail-control)"
if ! awk '
    /flightsql_mpp_plan event=abort/ {
      aborts++
    }
    /action=rollback scope=cluster/ {
      if (aborts <= rollbacks) {
        exit 1
      }
      rollbacks++
    }
    END {
      if (aborts < 2 || rollbacks < 2) {
        exit 1
      }
    }
  ' <<<"${fail_control_logs}"; then
  echo "unfinished MPP plan abort did not precede remote transaction rollback" >&2
  exit 1
fi

require_log flightsql-mpp-control "action=commit scope=cluster"
require_log flightsql-mpp-control "action=rollback scope=cluster"
require_log flightsql-mpp-fail-control "flightsql_mpp_plan event=abort"
require_log flightsql-mpp-fail-control "action=rollback scope=cluster"
require_log flightsql-mpp-no-cluster-control "flightsql_mpp_plan event=abort"
require_log flightsql-mpp-no-cluster-control "action=rollback scope=cluster"
require_log_from_any \
  "flightsql_mpp_plan event=expire operation=${abandoned_operation}" \
  flightsql-mpp-control \
  flightsql-mpp-fail-control \
  flightsql-mpp-no-cluster-control

for endpoints in 1 2 3 5; do
  query_count="$(
    compose logs --no-color flightsql-synthetic |
      grep -c "af_perf_read_ep${endpoints}" || true
  )"
  if [ "${query_count}" != "2" ]; then
    echo "expected two endpoint-${endpoints} read queries, got ${query_count}" >&2
    exit 1
  fi
done

echo "flightsql_synthetic_compose=ok"
