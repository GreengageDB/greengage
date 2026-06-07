#!/usr/bin/env bash
set -euo pipefail

: "${ARROWFLIGHTD_SERVER:=/tmp/arrowflightd}"
: "${ARROWFLIGHT_SMOKE_PORT:=8815}"
: "${GPDEMO_DIR:=/home/gpadmin/gpdb_src/gpAux/gpdemo}"
: "${GPHOME:=/home/gpadmin/greengage-db-devel}"

source "${GPHOME}/greengage_path.sh"

cd "${GPDEMO_DIR}"
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
  make destroy-demo-cluster >/tmp/arrowflight_fdw_write_destroy_before.log 2>&1 || true

if ! WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
  make create-demo-cluster >/tmp/arrowflight_fdw_write_create.log 2>&1; then
  cat /tmp/arrowflight_fdw_write_create.log
  exit 1
fi

source "${GPDEMO_DIR}/gpdemo-env.sh"

"${ARROWFLIGHTD_SERVER}" "${ARROWFLIGHT_SMOKE_PORT}" \
  >/tmp/arrowflight_fdw_write_server.log 2>&1 &
arrowflight_pid=$!

cleanup() {
  kill "${arrowflight_pid}" >/dev/null 2>&1 || true
  wait "${arrowflight_pid}" >/dev/null 2>&1 || true
  cd "${GPDEMO_DIR}"
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=2 \
    make destroy-demo-cluster >/tmp/arrowflight_fdw_write_destroy_after.log 2>&1 || true
}
trap cleanup EXIT

sleep 1
kill -0 "${arrowflight_pid}"

psql -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION arrowflight;
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE SERVER arrowflight_fdw_write_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    mpp_execute 'all segments',
    host '127.0.0.1',
    port '${ARROWFLIGHT_SMOKE_PORT}',
    write_mode 'staging',
    timeout_ms '5000',
    retry_count '0',
    use_get_flight_info 'true',
    flight_endpoint_policy 'segment_index'
);

CREATE TYPE arrowflight_fdw_state_smoke AS ENUM ('queued', 'done');

CREATE FOREIGN TABLE arrowflight_fdw_write_smoke (
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp,
    tstz timestamptz
)
SERVER arrowflight_fdw_write_srv
OPTIONS (
    path 'events',
    operation_metadata 'static.source=write_smoke,static.job_id=afw_smoke',
    batch_rows '2',
    max_batch_bytes '256'
);

INSERT INTO arrowflight_fdw_write_smoke
SELECT gp_segment_id * 100 + n AS id,
       'write-seg-' || gp_segment_id::text || '-' || n::text AS label,
       (n % 2) = 0 AS active,
       n::float8 + 0.25 AS amount,
       DATE '2026-06-07' + n AS d,
       TIMESTAMP '2026-06-07 12:00:00' + (n || ' seconds')::interval AS ts,
       TIMESTAMPTZ '2026-06-07 12:00:00+00' + (n || ' seconds')::interval AS tstz
FROM gp_dist_random('gp_id'), generate_series(1, 3) AS g(n);

DROP FOREIGN TABLE arrowflight_fdw_write_smoke;
SQL

operation_id=$(python3 - <<'PY'
import re
import sys
import time

for _ in range(50):
    text = open("/tmp/arrowflight_fdw_write_server.log", encoding="utf-8").read()
    ids = re.findall(r"arrowflightd_write_profile .*operation_id=([^ ]+)", text)
    if ids:
        print(ids[-1])
        sys.exit(0)
    time.sleep(0.1)

print("could not find write operation id in server log", file=sys.stderr)
sys.exit(1)
PY
)

psql -v ON_ERROR_STOP=1 postgres <<SQL
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE FOREIGN TABLE arrowflight_fdw_write_readback (
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp,
    tstz timestamptz
)
SERVER arrowflight_fdw_write_srv
OPTIONS (
    path 'written/${operation_id}',
    rows '6'
);
SQL

readback_result=$(psql -v ON_ERROR_STOP=1 -At postgres <<'SQL'
SELECT count(*)::text || '|' ||
       coalesce(sum(id), 0)::text || '|' ||
       count(*) FILTER (WHERE active)::text || '|' ||
       min(label) || '|' ||
       max(label) || '|' ||
       to_char(min(d), 'YYYY-MM-DD') || '|' ||
       to_char(max(ts), 'YYYY-MM-DD HH24:MI:SS') || '|' ||
       to_char(max(tstz AT TIME ZONE 'UTC'), 'YYYY-MM-DD HH24:MI:SS')
FROM arrowflight_fdw_write_readback;
SQL
)

echo "fdw_write_readback=${readback_result}"
expected_readback="6|312|2|write-seg-0-1|write-seg-1-3|2026-06-08|2026-06-07 12:00:03|2026-06-07 12:00:03"
if [[ "${readback_result}" != "${expected_readback}" ]]; then
  echo "unexpected write readback result: ${readback_result}" >&2
  exit 1
fi

psql -v ON_ERROR_STOP=1 postgres <<SQL
DROP FOREIGN TABLE arrowflight_fdw_write_readback;
SQL

psql -v ON_ERROR_STOP=1 postgres <<SQL
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE FOREIGN TABLE arrowflight_fdw_write_types (
    id int4,
    js json,
    jb jsonb,
    uid uuid,
    nums int4[],
    tags text[],
    state arrowflight_fdw_state_smoke,
    duration interval,
    limited varchar(8),
    unlimited varchar
)
SERVER arrowflight_fdw_write_srv
OPTIONS (
    path 'types_write',
    operation_metadata 'static.source=write_types_smoke',
    batch_rows '2',
    max_batch_bytes '1024'
);

INSERT INTO arrowflight_fdw_write_types
SELECT gp_segment_id * 100 + n AS id,
       ('{"row":' || n::text || ',"seg":' || gp_segment_id::text || '}')::json AS js,
       ('{"kind":"write","row":' || n::text || '}')::jsonb AS jb,
       ('00000000-0000-0000-0000-' ||
        lpad((gp_segment_id * 100 + n)::text, 12, '0'))::uuid AS uid,
       ARRAY[n, n + 1]::int4[] AS nums,
       ARRAY['seg' || gp_segment_id::text, 'row' || n::text]::text[] AS tags,
       CASE WHEN n = 1 THEN 'queued' ELSE 'done' END::arrowflight_fdw_state_smoke AS state,
       (n || ' seconds')::interval AS duration,
       ('v' || n::text)::varchar(8) AS limited,
       ('open-' || gp_segment_id::text || '-' || n::text)::varchar AS unlimited
FROM gp_dist_random('gp_id'), generate_series(1, 2) AS g(n);

DROP FOREIGN TABLE arrowflight_fdw_write_types;
SQL

operation_id_types=$(python3 - <<'PY'
import re
import sys
import time

for _ in range(50):
    text = open("/tmp/arrowflight_fdw_write_server.log", encoding="utf-8").read()
    ids = []
    for line in text.splitlines():
        if "arrowflightd_write_profile" not in line or "dataset=types_write" not in line:
            continue
        match = re.search(r"operation_id=([^ ]+)", line)
        if match:
            ids.append(match.group(1))
    if ids:
        print(ids[-1])
        sys.exit(0)
    time.sleep(0.1)

print("could not find types write operation id in server log", file=sys.stderr)
sys.exit(1)
PY
)

psql -v ON_ERROR_STOP=1 postgres <<SQL
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE FOREIGN TABLE arrowflight_fdw_write_types_readback (
    id int4,
    js json,
    jb jsonb,
    uid uuid,
    nums int4[],
    tags text[],
    state arrowflight_fdw_state_smoke,
    duration interval,
    limited varchar(8),
    unlimited varchar
)
SERVER arrowflight_fdw_write_srv
OPTIONS (
    path 'written/${operation_id_types}',
    rows '4'
);
SQL

types_readback_result=$(psql -v ON_ERROR_STOP=1 -At postgres <<'SQL'
SELECT count(*)::text || '|' ||
       coalesce(sum(id), 0)::text || '|' ||
       coalesce(sum((js->>'row')::int), 0)::text || '|' ||
       count(*) FILTER (WHERE jb->>'kind' = 'write')::text || '|' ||
       min(uid::text) || '|' ||
       max(uid::text) || '|' ||
       coalesce(sum(nums[2]), 0)::text || '|' ||
       string_agg(DISTINCT state::text, ',' ORDER BY state::text) || '|' ||
       coalesce(sum(extract(epoch from duration))::int, 0)::text || '|' ||
       max(length(limited))::text || '|' ||
       min(unlimited) || '|' ||
       max(unlimited)
FROM arrowflight_fdw_write_types_readback;
SQL
)

echo "fdw_write_types_readback=${types_readback_result}"
expected_types_readback="4|206|6|4|00000000-0000-0000-0000-000000000001|00000000-0000-0000-0000-000000000102|10|done,queued|6|2|open-0-1|open-1-2"
if [[ "${types_readback_result}" != "${expected_types_readback}" ]]; then
  echo "unexpected types write readback result: ${types_readback_result}" >&2
  exit 1
fi

psql -v ON_ERROR_STOP=1 postgres <<SQL
DROP FOREIGN TABLE arrowflight_fdw_write_types_readback;
SQL

expect_write_temporal_error() {
  local name="$1"
  local ddl="$2"
  local insert_sql="$3"
  local pattern="$4"
  local logfile="/tmp/arrowflight_fdw_write_${name}.log"

  set +e
  psql -v ON_ERROR_STOP=1 postgres <<SQL >"${logfile}" 2>&1
SET TIME ZONE 'UTC';
SET optimizer=off;
${ddl}
${insert_sql}
SQL
  local query_rc=$?
  set -e

  cat "${logfile}"

  if [[ "${query_rc}" -eq 0 ]]; then
    echo "expected ${name} write to fail" >&2
    exit 1
  fi

  if ! grep -Eqi "${pattern}" "${logfile}"; then
    echo "expected ${name} diagnostic was not found" >&2
    exit 1
  fi
}

expect_write_temporal_error \
  "bad_date_infinity" \
  "CREATE FOREIGN TABLE arrowflight_fdw_bad_date_infinity (d date) SERVER arrowflight_fdw_write_srv OPTIONS (path 'bad_date_infinity');" \
  "INSERT INTO arrowflight_fdw_bad_date_infinity VALUES (DATE 'infinity');" \
  "date infinity cannot be represented"
expect_write_temporal_error \
  "bad_timestamp_infinity" \
  "CREATE FOREIGN TABLE arrowflight_fdw_bad_timestamp_infinity (ts timestamp) SERVER arrowflight_fdw_write_srv OPTIONS (path 'bad_timestamp_infinity');" \
  "INSERT INTO arrowflight_fdw_bad_timestamp_infinity VALUES (TIMESTAMP 'infinity');" \
  "timestamp infinity cannot be represented"
expect_write_temporal_error \
  "bad_timestamptz_infinity" \
  "CREATE FOREIGN TABLE arrowflight_fdw_bad_timestamptz_infinity (tstz timestamptz) SERVER arrowflight_fdw_write_srv OPTIONS (path 'bad_timestamptz_infinity');" \
  "INSERT INTO arrowflight_fdw_bad_timestamptz_infinity VALUES (TIMESTAMPTZ 'infinity');" \
  "timestamp infinity cannot be represented"
expect_write_temporal_error \
  "bad_timestamp_arrow_range" \
  "CREATE FOREIGN TABLE arrowflight_fdw_bad_timestamp_arrow_range (ts timestamp) SERVER arrowflight_fdw_write_srv OPTIONS (path 'bad_timestamp_arrow_range');" \
  "INSERT INTO arrowflight_fdw_bad_timestamp_arrow_range VALUES (TIMESTAMP '294276-12-31 23:59:59.999999');" \
  "timestamp value is out of Arrow timestamp range"

psql -v ON_ERROR_STOP=1 postgres <<SQL
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_date_infinity;
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_timestamp_infinity;
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_timestamptz_infinity;
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_bad_timestamp_arrow_range;
SQL

set +e
psql -v ON_ERROR_STOP=1 postgres >/tmp/arrowflight_fdw_write_abort_query.log 2>&1 <<SQL
SET TIME ZONE 'UTC';
SET optimizer=off;

CREATE FOREIGN TABLE arrowflight_fdw_write_abort (
    id int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp,
    tstz timestamptz
)
SERVER arrowflight_fdw_write_srv
OPTIONS (
    path 'events_abort',
    operation_metadata 'static.source=write_abort,static.fail_after_batches=1',
    batch_rows '1',
    max_batch_bytes '256'
);

INSERT INTO arrowflight_fdw_write_abort
SELECT gp_segment_id * 100 + n AS id,
       'abort-seg-' || gp_segment_id::text || '-' || n::text AS label,
       (n % 2) = 0 AS active,
       n::float8 + 0.25 AS amount,
       DATE '2026-06-07' + n AS d,
       TIMESTAMP '2026-06-07 12:00:00' + (n || ' seconds')::interval AS ts,
       TIMESTAMPTZ '2026-06-07 12:00:00+00' + (n || ' seconds')::interval AS tstz
FROM gp_dist_random('gp_id'), generate_series(1, 2) AS g(n);
SQL
abort_status=$?
set -e

cat /tmp/arrowflight_fdw_write_abort_query.log
if [[ "${abort_status}" -eq 0 ]]; then
  echo "expected injected write failure" >&2
  exit 1
fi
if ! grep -Eq "injected write failure|did not return final ack" \
    /tmp/arrowflight_fdw_write_abort_query.log; then
  echo "expected injected write failure diagnostic or missing final ack" >&2
  exit 1
fi

psql -v ON_ERROR_STOP=1 postgres <<SQL
DROP FOREIGN TABLE IF EXISTS arrowflight_fdw_write_abort;
DROP SERVER arrowflight_fdw_write_srv;
DROP TYPE IF EXISTS arrowflight_fdw_state_smoke;
DROP EXTENSION arrowflight CASCADE;
SQL

cat /tmp/arrowflight_fdw_write_server.log

python3 - <<'PY'
import re
import sys

text = open("/tmp/arrowflight_fdw_write_server.log", encoding="utf-8").read()
profiles = []
actions = []
for line in text.splitlines():
    if "arrowflightd_write_profile" not in line:
        if "arrowflightd_write_action" in line:
            actions.append(dict(re.findall(r"([A-Za-z0-9_.]+)=([^ ]+)", line)))
        continue
    profiles.append(dict(re.findall(r"([A-Za-z0-9_.]+)=([^ ]+)", line)))

event_profiles = [p for p in profiles if p.get("dataset") == "events"]
type_profiles = [p for p in profiles if p.get("dataset") == "types_write"]

if len(event_profiles) != 2:
    print(f"expected 2 event segment write profiles, got {len(event_profiles)}", file=sys.stderr)
    sys.exit(1)

rows = sum(int(p.get("rows", "0")) for p in event_profiles)
if rows != 6:
    print(f"expected 6 written rows, got {rows}", file=sys.stderr)
    sys.exit(1)

if len(type_profiles) != 2:
    print(f"expected 2 types_write segment write profiles, got {len(type_profiles)}", file=sys.stderr)
    sys.exit(1)

type_rows = sum(int(p.get("rows", "0")) for p in type_profiles)
if type_rows != 4:
    print(f"expected 4 types_write rows, got {type_rows}", file=sys.stderr)
    sys.exit(1)

if any(int(p.get("batches", "0")) < 1 for p in event_profiles + type_profiles):
    print(f"expected every segment to write at least one batch: {profiles}", file=sys.stderr)
    sys.exit(1)

finalize_actions = [
    a for a in actions if a.get("action") == "FinalizeOperation" and
    a.get("dataset") == "events"
]
if len(finalize_actions) != 2:
    print(f"expected 2 finalize actions, got {len(finalize_actions)}: {actions}", file=sys.stderr)
    sys.exit(1)

if not any(a.get("finalize_state") == "complete" for a in finalize_actions):
    print(f"expected one complete finalize action: {finalize_actions}", file=sys.stderr)
    sys.exit(1)

if any(a.get("expected_segments") != "2" for a in finalize_actions):
    print(f"unexpected expected_segments in finalize actions: {finalize_actions}", file=sys.stderr)
    sys.exit(1)

abort_actions = [
    a for a in actions if a.get("action") == "AbortOperation"
]
if not abort_actions:
    print(f"expected at least one abort action: {actions}", file=sys.stderr)
    sys.exit(1)

if "arrowflightd_write_inject_failure" not in text:
    print("expected injected server failure in write log", file=sys.stderr)
    sys.exit(1)

print("fdw_write_smoke_result=ok")
PY
