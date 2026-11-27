#!/usr/bin/env bash
set -euo pipefail

# The delay fixture emits eight batches per endpoint, each delayed by 100 ms.
: "${FLIGHTSQL_DELAY_HOST:=flightsql-synthetic-delay}"
: "${FLIGHTSQL_DELAY_PORT:=9018}"
suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
if [[ ! "${suffix}" =~ ^[a-z0-9_]{1,30}$ ]]; then
  echo 'Invalid FLIGHTSQL_TEST_SUFFIX' >&2
  exit 1
fi
name="flightsql_timeout_${suffix}"
psql_cmd=(psql -X -q -v ON_ERROR_STOP=1 postgres)

"${psql_cmd[@]}" -v name="${name}" -v host="${FLIGHTSQL_DELAY_HOST}" \
  -v port="${FLIGHTSQL_DELAY_PORT}" <<'SQL'
BEGIN;
CREATE EXTENSION IF NOT EXISTS arrowflight;
CREATE SCHEMA :"name";
CREATE SERVER :"name" FOREIGN DATA WRAPPER flightsql_fdw
  OPTIONS (host :'host', port :'port', timeout_ms '500');
COMMIT;
SQL

cleanup() {
  "${psql_cmd[@]}" -v name="${name}" >/dev/null 2>&1 <<'SQL' || true
DROP SCHEMA :"name" CASCADE;
DROP SERVER :"name";
SQL
}
trap cleanup EXIT

"${psql_cmd[@]}" <<SQL
CREATE FOREIGN TABLE ${name}.remote (id int4)
  SERVER ${name} OPTIONS (table_name 'af_perf_read');
CREATE FOREIGN TABLE ${name}.slow (id int4)
  SERVER ${name} OPTIONS (table_name 'af_perf_read_slow_batch');
SQL

for optimizer in on off; do
  # The stream outlives timeout_ms even without the slow local consumer.
  rows="$(PGOPTIONS="-c optimizer=${optimizer} -c statement_timeout=15000" \
    "${psql_cmd[@]}" -At <<SQL
SELECT count(*) FROM (SELECT id, pg_sleep(0.001) FROM ${name}.remote) s;
SQL
  )"
  if [[ "${rows}" != '3000' ]]; then
    echo "Read stream was truncated: ${rows}" >&2
    exit 1
  fi
  echo "PASS (optimizer=${optimizer}): timeout_ms does not limit the read stream"
done

# Exercise a user cancellation while a QE is waiting in Next(), not just the
# statement timeout signal. A separate connection issues pg_cancel_backend.
python3 - "${name}" <<'PY'
import sys
import threading
import time

import psycopg2
from psycopg2 import sql

connection = psycopg2.connect(dbname='postgres')
connection.autocommit = True
control = psycopg2.connect(dbname='postgres')
control.autocommit = True
result = []

def read():
    try:
        with connection.cursor() as cursor:
            cursor.execute('SET statement_timeout = 15000')
            cursor.execute(sql.SQL('SELECT count(*) FROM {}.slow').format(sql.Identifier(sys.argv[1])))
            result.append('unexpected success')
    except psycopg2.Error as error:
        result.append(error.pgcode)

worker = threading.Thread(target=read)
worker.start()
try:
    time.sleep(0.3)
    started = time.monotonic()
    with control.cursor() as cursor:
        cursor.execute('SELECT pg_cancel_backend(%s)', (connection.get_backend_pid(),))
        assert cursor.fetchone() == (True,)
    worker.join(3)
    assert not worker.is_alive(), 'user cancellation did not interrupt the stream'
    assert result == ['57014'], result
    with connection.cursor() as cursor:
        cursor.execute('SELECT 1')
        assert cursor.fetchone() == (1,)
    print(f'PASS: user cancellation and session recovery in {time.monotonic() - started:.3f} s')
finally:
    worker.join()
    connection.close()
    control.close()
PY

"${psql_cmd[@]}" -c "ALTER SERVER ${name} OPTIONS (ADD read_timeout_ms '250')"
if output="$("${psql_cmd[@]}" -At -c "SELECT count(*) FROM ${name}.remote" 2>&1)"; then
  echo 'read_timeout_ms did not expire the delayed stream' >&2
  exit 1
fi
if ! grep -qiE 'deadline|timed out|timeout' <<< "${output}"; then
  printf 'Unexpected read timeout failure: %s\n' "${output}" >&2
  exit 1
fi
echo 'PASS: read_timeout_ms limits the complete stream'

"${psql_cmd[@]}" -c "ALTER SERVER ${name} OPTIONS (SET read_timeout_ms '0')"
rows="$("${psql_cmd[@]}" -At -c "SELECT count(*) FROM ${name}.remote")"
test "${rows}" = 3000
echo 'PASS: read_timeout_ms=0 disables the stream deadline; fresh scan succeeds'

# The next batch takes ten seconds. Cancellation must not wait for that batch.
for optimizer in on off; do
  start="$(date +%s%N)"
  if output="$(PGOPTIONS="-c optimizer=${optimizer} -c statement_timeout=500" \
      "${psql_cmd[@]}" -At -c "SELECT count(*) FROM ${name}.slow" 2>&1)"; then
    echo 'statement_timeout did not cancel the delayed read' >&2
    exit 1
  fi
  elapsed_ms=$(( ($(date +%s%N) - start) / 1000000 ))
  if [[ "${output}" != *'canceling statement due to statement timeout'* ]] || \
     (( elapsed_ms > 3000 )); then
    printf 'Cancellation failed or took too long (%s ms): %s\n' \
      "${elapsed_ms}" "${output}" >&2
    exit 1
  fi
  echo "PASS (optimizer=${optimizer}): blocked Next cancelled in ${elapsed_ms} ms"
done
