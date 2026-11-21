#!/usr/bin/env bash
set -euo pipefail

# Run against an installed extension and dedicated, otherwise idle fixtures.
# Required: origin and MPP control servers with ARROWFLIGHT_BENCH_SAVEPOINTS=true.
# MPP workers share ARROWFLIGHT_MPP_STATE_DIR with their control server.
# Optional fault fixtures (set the corresponding *_HOST to enable):
#   END_DELAY: ARROWFLIGHT_BENCH_END_TRANSACTION_DELAY_MS=15000
#   MPP_DELAY: ARROWFLIGHT_MPP_ABORT_DELAY_MS=15000 and one worker with
#              ARROWFLIGHT_MPP_FAIL_SEGMENT=2 (or another participating segment)
#   SP_FAIL:   ARROWFLIGHT_BENCH_SAVEPOINTS=true,
#              ARROWFLIGHT_BENCH_FAIL_SAVEPOINT_ROLLBACK=true
#   SP_DELAY:  ARROWFLIGHT_BENCH_SAVEPOINTS=true,
#              ARROWFLIGHT_BENCH_END_SAVEPOINT_DELAY_MS=3500
# NO_SP must advertise transactions but ARROWFLIGHT_BENCH_SAVEPOINTS=false.
# No Docker/compose operations or fixture log access are needed by this script.
# Missing optional cases print SKIP; FLIGHTSQL_TXN_REQUIRE_FAULTS=1 forbids skips.

: "${FLIGHTSQL_TXN_DATABASE:=postgres}"
: "${FLIGHTSQL_TXN_HOST:=flightsql-txn-savepoints}"
: "${FLIGHTSQL_TXN_PORT:=9030}"
: "${FLIGHTSQL_TXN_MPP_HOST:=flightsql-mpp-control}"
: "${FLIGHTSQL_TXN_MPP_PORT:=9020}"
: "${FLIGHTSQL_TXN_MPP_ALLOWLIST:=grpc+tcp://flightsql-mpp-worker-0:9021,grpc+tcp://flightsql-mpp-worker-1:9022,grpc+tcp://flightsql-mpp-worker-2:9023}"
: "${FLIGHTSQL_TXN_NO_SP_HOST:=}"
: "${FLIGHTSQL_TXN_NO_SP_PORT:=9015}"
: "${FLIGHTSQL_TXN_END_DELAY_HOST:=}"
: "${FLIGHTSQL_TXN_END_DELAY_PORT:=9031}"
: "${FLIGHTSQL_TXN_MPP_DELAY_HOST:=}"
: "${FLIGHTSQL_TXN_MPP_DELAY_PORT:=9032}"
: "${FLIGHTSQL_TXN_MPP_DELAY_ALLOWLIST:=${FLIGHTSQL_TXN_MPP_ALLOWLIST}}"
: "${FLIGHTSQL_TXN_SP_FAIL_HOST:=}"
: "${FLIGHTSQL_TXN_SP_FAIL_PORT:=9033}"
: "${FLIGHTSQL_TXN_SP_DELAY_HOST:=}"
: "${FLIGHTSQL_TXN_SP_DELAY_PORT:=9034}"
: "${FLIGHTSQL_TXN_CLEANUP_LIMIT_SECONDS:=8}"
: "${FLIGHTSQL_TXN_WATCHDOG_SECONDS:=25}"
: "${FLIGHTSQL_TXN_REQUIRE_FAULTS:=0}"

suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
if [[ ! "${suffix}" =~ ^[a-zA-Z0-9_]{1,16}$ ]]; then
  echo 'FLIGHTSQL_TEST_SUFFIX must be 1-16 alphanumeric/underscore characters' >&2
  exit 1
fi
for value in "${FLIGHTSQL_TXN_CLEANUP_LIMIT_SECONDS}" "${FLIGHTSQL_TXN_WATCHDOG_SECONDS}"; do
  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo 'cleanup and watchdog limits must be positive integer seconds' >&2
    exit 1
  fi
done
ns="af_txn_${suffix}"
tag="txn_${suffix}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/flightsql-txn.XXXXXX")"
servers=()
active_pid=''
watchdog_pid=''
export PGOPTIONS="${PGOPTIONS:-} -c optimizer=on -c timezone=UTC"

sql() {
  psql -X -qAt -v ON_ERROR_STOP=1 "${FLIGHTSQL_TXN_DATABASE}" "$@"
}

cleanup() {
  if [ -n "${active_pid}" ]; then kill "${active_pid}" 2>/dev/null || true; fi
  if [ -n "${watchdog_pid}" ]; then kill "${watchdog_pid}" 2>/dev/null || true; fi
  {
    printf 'DROP SCHEMA IF EXISTS %s CASCADE;\n' "${ns}"
    for server in "${servers[@]}"; do
      printf 'DROP SERVER IF EXISTS %s CASCADE;\n' "${server}"
    done
  } | PGOPTIONS="${PGOPTIONS} -c statement_timeout=10000" \
    sql >/dev/null 2>&1 || true
  rm -rf "${scratch}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() { echo "$*" >&2; exit 1; }
assert_eq() { [ "$1" = "$2" ] || fail "$3: expected $2, got $1"; }
metric() { sql -c "SELECT $2 FROM ${ns}.$1_metrics"; }
delta() { assert_eq "$(( $(metric "$1" "$2") - $3 ))" "$4" "$1 $2 delta"; }

create_target() {
  local name="$1" host="$2" port="$3" routing="${4:-origin}" allowlist="${5:-}"
  local server="${ns}_${name}"
  if [ -z "${allowlist}" ]; then
    local uri_host="${host}"
    if [[ "${uri_host}" == *:* && "${uri_host}" != \[*\] ]]; then
      uri_host="[${uri_host}]"
    fi
    allowlist="grpc+tcp://${uri_host}:${port}"
  fi
  servers+=("${server}")
  sql -v host="${host}" -v port="${port}" -v routing="${routing}" \
    -v allowlist="${allowlist}" <<SQL
CREATE SERVER ${server} FOREIGN DATA WRAPPER flightsql_fdw OPTIONS (
  host :'host', port :'port', write_transaction_mode 'required',
  write_routing_mode :'routing', endpoint_location_allowlist :'allowlist',
  predicate_pushdown 'false', timeout_ms '-1');
CREATE FOREIGN TABLE ${ns}.${name} (
  id int4, segid int4, label text, active bool, amount float8, d date, ts timestamp
) SERVER ${server} OPTIONS (table_name 'af_perf_write');
CREATE FOREIGN TABLE ${ns}.${name}_metrics (
  begin_count int8, commit_count int8, rollback_count int8, ingest_count int8,
  savepoint_begin_count int8, savepoint_release_count int8, savepoint_rollback_count int8,
  mpp_create_count int8, mpp_abort_count int8, end_started_count int8, abort_started_count int8
) SERVER ${server} OPTIONS (table_name 'af_txn_metrics');
SQL
}

rows_sql() {
  printf "SELECT id + %s, segid, '%s_%s'::text, active, amount, d, ts FROM %s.source" \
    "$2" "${tag}" "$1" "${ns}"
}

insert_sql() {
  printf 'INSERT INTO %s.%s %s;\n' "${ns}" "$1" "$(rows_sql "$2" "$3")"
}

assert_data() {
  local target="$1" label="$2" expected='' offset
  shift 2
  for offset in "$@"; do
    if [ -n "${expected}" ]; then expected+=' UNION ALL '; fi
    expected+="$(rows_sql "${label}" "${offset}")"
  done
  if [ -z "${expected}" ]; then
    expected="SELECT * FROM ${ns}.source WHERE false"
  fi
  local differences
  differences="$(sql -c "
    WITH actual AS (SELECT * FROM ${ns}.${target} WHERE label = '${tag}_${label}'),
         expected AS (${expected})
    SELECT count(*) FROM (
      (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
      UNION ALL
      (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
    ) AS differences")"
  assert_eq "${differences}" 0 "${target} persisted data for ${label}"
}

run_bounded() {
  local name="$1" input="$2" started=$SECONDS status=0
  sql <"${input}" >"${scratch}/${name}.out" 2>&1 &
  active_pid=$!
  (
    sleep "${FLIGHTSQL_TXN_WATCHDOG_SECONDS}"
    kill "${active_pid}" 2>/dev/null || true
  ) &
  watchdog_pid=$!
  wait "${active_pid}" || status=$?
  active_pid=''
  kill "${watchdog_pid}" 2>/dev/null || true
  wait "${watchdog_pid}" 2>/dev/null || true
  watchdog_pid=''
  local elapsed=$((SECONDS - started))
  cat "${scratch}/${name}.out"
  [ "${elapsed}" -le "${FLIGHTSQL_TXN_CLEANUP_LIMIT_SECONDS}" ] || \
    fail "${name}: cleanup took ${elapsed}s (limit ${FLIGHTSQL_TXN_CLEANUP_LIMIT_SECONDS}s)"
  [ "${status}" -eq 0 ] || fail "${name}: psql exited ${status}"
  echo "${name}: cleanup bounded at ${elapsed}s"
}

sql <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;
CREATE SCHEMA ${ns};
CREATE TABLE ${ns}.source (
  id int4, segid int4, label text, active bool, amount float8, d date, ts timestamp
) DISTRIBUTED RANDOMLY;
INSERT INTO ${ns}.source
SELECT i, i % 3, 'source', i % 2 = 0, i / 10.0,
  DATE '2026-01-01' + i % 30, TIMESTAMP '2026-01-01' + i * INTERVAL '1 second'
FROM generate_series(1, 300) i;
SQL

create_target origin "${FLIGHTSQL_TXN_HOST}" "${FLIGHTSQL_TXN_PORT}"
create_target planned "${FLIGHTSQL_TXN_MPP_HOST}" "${FLIGHTSQL_TXN_MPP_PORT}" \
  planned "${FLIGHTSQL_TXN_MPP_ALLOWLIST}"

for target in origin planned; do
  label="${target}_repeat"
  begins="$(metric "${target}" begin_count)"
  commits="$(metric "${target}" commit_count)"
  sql <<SQL
BEGIN;
$(insert_sql "${target}" "${label}" 0)
$(insert_sql "${target}" "${label}" 1000)
$(insert_sql "${target}" "${label}" 2000)
COMMIT;
SQL
  assert_data "${target}" "${label}" 0 1000 2000
  delta "${target}" begin_count "${begins}" 1
  delta "${target}" commit_count "${commits}" 1

  label="${target}_prepared"
  begins="$(metric "${target}" begin_count)"
  commits="$(metric "${target}" commit_count)"
  plans="$(metric "${target}" mpp_create_count)"
  # No parameters: both executions reuse the cached plan, not custom plans.
  sql <<SQL
PREPARE repeat_insert AS
$(insert_sql "${target}" "${label}" 0)
EXECUTE repeat_insert;
EXECUTE repeat_insert;
DEALLOCATE repeat_insert;
SQL
  assert_data "${target}" "${label}" 0 0
  delta "${target}" begin_count "${begins}" 2
  delta "${target}" commit_count "${commits}" 2
  if [ "${target}" = planned ]; then
    delta "${target}" mpp_create_count "${plans}" 2
  fi

  label="${target}_exception"
  begins="$(metric "${target}" begin_count)"
  commits="$(metric "${target}" commit_count)"
  sp_rollbacks="$(metric "${target}" savepoint_rollback_count)"
  sql <<SQL
BEGIN;
$(insert_sql "${target}" "${label}" 0)
DO \$body\$
BEGIN
  BEGIN
    $(insert_sql "${target}" "${label}" 1000)
    PERFORM 1 / 0;
  EXCEPTION WHEN division_by_zero THEN
    NULL;
  END;
END
\$body\$;
$(insert_sql "${target}" "${label}" 2000)
COMMIT;
SQL
  assert_data "${target}" "${label}" 0 2000
  delta "${target}" begin_count "${begins}" 1
  delta "${target}" commit_count "${commits}" 1
  delta "${target}" savepoint_rollback_count "${sp_rollbacks}" 1

  label="${target}_rollback"
  rollbacks="$(metric "${target}" rollback_count)"
  sql <<SQL
BEGIN;
$(insert_sql "${target}" "${label}" 0)
$(insert_sql "${target}" "${label}" 1000)
ROLLBACK;
SQL
  assert_data "${target}" "${label}"
  delta "${target}" rollback_count "${rollbacks}" 1

  label="${target}_nested"
  begins="$(metric "${target}" begin_count)"
  commits="$(metric "${target}" commit_count)"
  sp_rollbacks="$(metric "${target}" savepoint_rollback_count)"
  sql <<SQL
BEGIN;
$(insert_sql "${target}" "${label}" 0)
SAVEPOINT outer_sp;
$(insert_sql "${target}" "${label}" 1000)
SAVEPOINT inner_sp;
$(insert_sql "${target}" "${label}" 2000)
ROLLBACK TO inner_sp;
$(insert_sql "${target}" "${label}" 3000)
RELEASE inner_sp;
ROLLBACK TO outer_sp;
$(insert_sql "${target}" "${label}" 4000)
RELEASE outer_sp;
$(insert_sql "${target}" "${label}" 5000)
COMMIT;
SQL
  assert_data "${target}" "${label}" 0 4000 5000
  delta "${target}" begin_count "${begins}" 1
  delta "${target}" commit_count "${commits}" 1
  delta "${target}" savepoint_rollback_count "${sp_rollbacks}" 2

  label="${target}_first_nested"
  begins="$(metric "${target}" begin_count)"
  rollbacks="$(metric "${target}" rollback_count)"
  sql <<SQL
BEGIN;
SAVEPOINT first_write;
$(insert_sql "${target}" "${label}" 0)
ROLLBACK TO first_write;
$(insert_sql "${target}" "${label}" 1000)
RELEASE first_write;
COMMIT;
SQL
  assert_data "${target}" "${label}" 1000
  delta "${target}" begin_count "${begins}" 2
  delta "${target}" rollback_count "${rollbacks}" 1

  # A distinct foreign SERVER is rejected even when it points at the same URI.
  host="${FLIGHTSQL_TXN_HOST}"; port="${FLIGHTSQL_TXN_PORT}"; allowlist=''
  if [ "${target}" = planned ]; then
    host="${FLIGHTSQL_TXN_MPP_HOST}"; port="${FLIGHTSQL_TXN_MPP_PORT}"
    allowlist="${FLIGHTSQL_TXN_MPP_ALLOWLIST}"
  fi
  create_target "${target}_second" "${host}" "${port}" "${target}" "${allowlist}"
  label="${target}_second_server"
  output="$(sql 2>&1 <<SQL
BEGIN;
$(insert_sql "${target}" "${label}" 0)
SELECT ingest_count AS before_ingest, mpp_create_count AS before_plan
FROM ${ns}.${target}_metrics \gset
SAVEPOINT rejected_server;
\set ON_ERROR_STOP off
$(insert_sql "${target}_second" "${label}" 1000)
\set ON_ERROR_STOP on
ROLLBACK TO rejected_server;
SELECT 'no_second_stream=' || (ingest_count = :before_ingest AND mpp_create_count = :before_plan)::text
FROM ${ns}.${target}_metrics;
RELEASE rejected_server;
COMMIT;
SQL
)"
  grep -q 'cannot use multiple foreign servers' <<<"${output}" || fail "${output}"
  grep -qx 'no_second_stream=true' <<<"${output}" || fail "${output}"
  assert_data "${target}" "${label}" 0
  echo "${target}: transaction reuse, prepared INSERT, exception rollback, nested savepoints, second-server rejection ok"
done

for fixture in NO_SP END_DELAY MPP_DELAY SP_FAIL SP_DELAY; do
  variable="FLIGHTSQL_TXN_${fixture}_HOST"
  if [ -z "${!variable}" ]; then
    [ "${FLIGHTSQL_TXN_REQUIRE_FAULTS}" != 1 ] || fail "${variable} is required"
    echo "SKIP ${fixture}: ${variable} is unset"
    continue
  fi
  port_variable="FLIGHTSQL_TXN_${fixture}_PORT"
  target="$(tr '[:upper:]' '[:lower:]' <<<"${fixture}")"
  routing=origin; allowlist=''
  if [ "${fixture}" = MPP_DELAY ]; then
    routing=planned; allowlist="${FLIGHTSQL_TXN_MPP_DELAY_ALLOWLIST}"
  fi
  create_target "${target}" "${!variable}" "${!port_variable}" "${routing}" "${allowlist}"
  label="${target}_fault"
  case "${fixture}" in
    NO_SP)
      before="$(metric "${target}" ingest_count)"
      output="$(sql 2>&1 <<SQL
BEGIN;
$(insert_sql "${target}" "${label}" 0)
SELECT ingest_count AS before_ingest FROM ${ns}.${target}_metrics \gset
SAVEPOINT unsupported;
\set ON_ERROR_STOP off
$(insert_sql "${target}" "${label}" 1000)
\set ON_ERROR_STOP on
ROLLBACK TO unsupported;
SELECT 'no_nested_stream=' || (ingest_count = :before_ingest)::text FROM ${ns}.${target}_metrics;
RELEASE unsupported;
COMMIT;
BEGIN;
SAVEPOINT first_write;
$(insert_sql "${target}" "${label}" 2000)
ROLLBACK TO first_write;
COMMIT;
SQL
)"
      grep -q 'does not support savepoints' <<<"${output}" || fail "${output}"
      grep -qx 'no_nested_stream=true' <<<"${output}" || fail "${output}"
      assert_data "${target}" "${label}" 0
      ;;
    END_DELAY)
      before="$(metric "${target}" end_started_count)"
      {
        echo BEGIN\;
        insert_sql "${target}" "${label}" 0
        echo ROLLBACK\;
        echo "SELECT 'session_alive';"
      } >"${scratch}/input.sql"
      run_bounded "${target}" "${scratch}/input.sql"
      grep -qx 'session_alive' "${scratch}/${target}.out"
      delta "${target}" end_started_count "${before}" 1
      assert_data "${target}" "${label}"
      ;;
    MPP_DELAY)
      before="$(metric "${target}" abort_started_count)"
      {
        echo '\set ON_ERROR_STOP off'
        insert_sql "${target}" "${label}" 0
        echo '\set ON_ERROR_STOP on'
        echo "SELECT 'session_alive';"
      } >"${scratch}/input.sql"
      run_bounded "${target}" "${scratch}/input.sql"
      grep -q 'injected benchmark ingest failure' "${scratch}/${target}.out"
      grep -qx 'session_alive' "${scratch}/${target}.out"
      delta "${target}" abort_started_count "${before}" 1
      assert_data "${target}" "${label}"
      ;;
    SP_FAIL|SP_DELAY)
      commits="$(metric "${target}" commit_count)"
      {
        echo BEGIN\;
        insert_sql "${target}" "${label}" 0
        echo SAVEPOINT failed_rollback\;
        insert_sql "${target}" "${label}" 1000
        echo ROLLBACK TO failed_rollback\;
        echo '\set ON_ERROR_STOP off'
        echo COMMIT\;
        echo '\set ON_ERROR_STOP on'
        echo "SELECT 'session_alive';"
      } >"${scratch}/input.sql"
      run_bounded "${target}" "${scratch}/input.sql"
      grep -qi 'uncertain\|rollback.*fail\|roll back.*fail' "${scratch}/${target}.out"
      grep -q 'ERROR:' "${scratch}/${target}.out"
      grep -qx 'session_alive' "${scratch}/${target}.out"
      delta "${target}" commit_count "${commits}" 0
      assert_data "${target}" "${label}"
      if [ "${fixture}" = SP_DELAY ]; then
        label="${target}_full_abort"
        commits="$(metric "${target}" commit_count)"
        {
          echo BEGIN\;
          insert_sql "${target}" "${label}" 0
          echo SAVEPOINT outer_abort\;
          insert_sql "${target}" "${label}" 1000
          echo SAVEPOINT inner_abort\;
          insert_sql "${target}" "${label}" 2000
          echo ROLLBACK\;
          echo "SELECT 'session_alive';"
        } >"${scratch}/input.sql"
        run_bounded "${target}_full_abort" "${scratch}/input.sql"
        grep -qx 'session_alive' "${scratch}/${target}_full_abort.out"
        delta "${target}" commit_count "${commits}" 0
        assert_data "${target}" "${label}"
      fi
      ;;
  esac
  echo "${fixture}: persisted data and cleanup checks ok"
done

echo flightsql_transaction_integration=ok
