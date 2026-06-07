#!/usr/bin/env bash
set -euo pipefail

: "${ARROWFLIGHTD_SERVER:=/tmp/arrowflightd}"
: "${ARROWFLIGHT_SMOKE_PORT:=8815}"
: "${ARROWFLIGHT_BENCH_GPFDIST_PORT:=8816}"
: "${ARROWFLIGHT_BENCH_ARROW_HOST:=127.0.0.1}"
: "${ARROWFLIGHT_BENCH_GPFDIST_HOST:=127.0.0.1}"
: "${ARROWFLIGHT_BENCH_NET_DEV:=}"
: "${ARROWFLIGHT_BENCH_SEGMENTS:=2}"
: "${ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT:=100000}"
: "${ARROWFLIGHT_BENCH_BATCH_ROWS:=8192}"
: "${ARROWFLIGHT_BENCH_LABEL_WIDTH:=32}"
: "${ARROWFLIGHT_BENCH_SCHEMA:=mixed}"
: "${ARROWFLIGHT_BENCH_SOURCE:=}"
: "${ARROWFLIGHT_BENCH_PREBUILD:=0}"
: "${ARROWFLIGHT_BENCH_PROFILE:=0}"
: "${ARROWFLIGHT_BENCH_REPEATS:=3}"
: "${ARROWFLIGHT_BENCH_WARMUPS:=1}"
: "${ARROWFLIGHT_BENCH_MAX_BATCH_BYTES:=4194304}"
: "${ARROWFLIGHT_BENCH_ROOT:=/tmp/arrowflight_benchmark}"
: "${ARROWFLIGHT_BENCH_RESULT_DIR:=${ARROWFLIGHT_BENCH_ROOT}/results}"
: "${GPDEMO_DIR:=/home/gpadmin/gpdb_src/gpAux/gpdemo}"
: "${GPHOME:=/home/gpadmin/greengage-db-devel}"

DATA_DIR="${ARROWFLIGHT_BENCH_ROOT}/data"
IPC_DIR="${DATA_DIR}/ipc"
RUNS_CSV="${ARROWFLIGHT_BENCH_RESULT_DIR}/runs.csv"
LATENCY_CSV="${ARROWFLIGHT_BENCH_RESULT_DIR}/latency.csv"
PROFILE_CSV="${ARROWFLIGHT_BENCH_RESULT_DIR}/profiles.csv"
SERVER_PROFILE_CSV="${ARROWFLIGHT_BENCH_RESULT_DIR}/server_profiles.csv"
SUMMARY_JSON="${ARROWFLIGHT_BENCH_RESULT_DIR}/summary.json"
SUMMARY_MD="${ARROWFLIGHT_BENCH_RESULT_DIR}/summary.md"

source "${GPHOME}/greengage_path.sh"

if [ -z "${ARROWFLIGHT_BENCH_SOURCE}" ]; then
  if [ "${ARROWFLIGHT_BENCH_PREBUILD}" = "1" ] ||
     [ "${ARROWFLIGHT_BENCH_PREBUILD}" = "true" ] ||
     [ "${ARROWFLIGHT_BENCH_PREBUILD}" = "yes" ]; then
    ARROWFLIGHT_BENCH_SOURCE="prebuilt"
  else
    ARROWFLIGHT_BENCH_SOURCE="generated"
  fi
fi

case "${ARROWFLIGHT_BENCH_SOURCE}" in
  generated|prebuilt|ipc)
    ;;
  *)
    echo "invalid ARROWFLIGHT_BENCH_SOURCE=${ARROWFLIGHT_BENCH_SOURCE}" >&2
    echo "use generated, prebuilt, or ipc" >&2
    exit 1
    ;;
esac

if [ "${ARROWFLIGHT_BENCH_SOURCE}" = "prebuilt" ]; then
  ARROWFLIGHT_BENCH_PREBUILD=1
else
  ARROWFLIGHT_BENCH_PREBUILD=0
fi

af_bench_script_source="${BASH_SOURCE[0]:-$0}"
AF_BENCH_SCRIPT_DIR="$(cd "$(dirname "${af_bench_script_source}")" && pwd)"

mkdir -p "${DATA_DIR}" "${ARROWFLIGHT_BENCH_RESULT_DIR}"
rm -f "${RUNS_CSV}" "${LATENCY_CSV}" "${PROFILE_CSV}" \
  "${SERVER_PROFILE_CSV}" "${SUMMARY_JSON}" "${SUMMARY_MD}"

python3 "${AF_BENCH_SCRIPT_DIR}/arrowflight_benchmark_data.py" \
  --output-dir "${DATA_DIR}" \
  --segments "${ARROWFLIGHT_BENCH_SEGMENTS}" \
  --rows-per-segment "${ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT}" \
  --batch-rows "${ARROWFLIGHT_BENCH_BATCH_ROWS}" \
  --label-width "${ARROWFLIGHT_BENCH_LABEL_WIDTH}" \
  --schema "${ARROWFLIGHT_BENCH_SCHEMA}" \
  >"${ARROWFLIGHT_BENCH_RESULT_DIR}/data_generation.json"

if [ "${ARROWFLIGHT_BENCH_SOURCE}" = "ipc" ]; then
  rm -rf "${IPC_DIR}"
  mkdir -p "${IPC_DIR}"
  ARROWFLIGHT_BENCH_SEGMENTS="${ARROWFLIGHT_BENCH_SEGMENTS}" \
  ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT="${ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT}" \
  ARROWFLIGHT_BENCH_BATCH_ROWS="${ARROWFLIGHT_BENCH_BATCH_ROWS}" \
  ARROWFLIGHT_BENCH_LABEL_WIDTH="${ARROWFLIGHT_BENCH_LABEL_WIDTH}" \
    "${ARROWFLIGHTD_SERVER}" --write-bench-ipc "${IPC_DIR}" \
    >"${ARROWFLIGHT_BENCH_RESULT_DIR}/ipc_generation.log" 2>&1
fi

if [ ! -x "${GPHOME}/bin/gpfdist" ]; then
  echo "gpfdist binary is not executable at ${GPHOME}/bin/gpfdist" >&2
  exit 1
fi

cd "${GPDEMO_DIR}"
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_BENCH_SEGMENTS}" \
  make destroy-demo-cluster >/tmp/arrowflight_benchmark_destroy_before.log 2>&1 || true

if ! WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_BENCH_SEGMENTS}" \
  make create-demo-cluster >/tmp/arrowflight_benchmark_create.log 2>&1; then
  cat /tmp/arrowflight_benchmark_create.log
  exit 1
fi

source "${GPDEMO_DIR}/gpdemo-env.sh"

"${GPHOME}/bin/gpfdist" -d "${DATA_DIR}" \
  -p "${ARROWFLIGHT_BENCH_GPFDIST_PORT}" \
  -m 67108864 \
  >"${ARROWFLIGHT_BENCH_RESULT_DIR}/gpfdist.log" 2>&1 &
gpfdist_pid=$!

ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT="${ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT}" \
ARROWFLIGHT_BENCH_BATCH_ROWS="${ARROWFLIGHT_BENCH_BATCH_ROWS}" \
ARROWFLIGHT_BENCH_LABEL_WIDTH="${ARROWFLIGHT_BENCH_LABEL_WIDTH}" \
ARROWFLIGHT_BENCH_PREBUILD="${ARROWFLIGHT_BENCH_PREBUILD}" \
ARROWFLIGHT_BENCH_SOURCE="${ARROWFLIGHT_BENCH_SOURCE}" \
ARROWFLIGHT_BENCH_IPC_DIR="${IPC_DIR}" \
  "${ARROWFLIGHTD_SERVER}" "${ARROWFLIGHT_SMOKE_PORT}" \
  >"${ARROWFLIGHT_BENCH_RESULT_DIR}/arrowflightd.log" 2>&1 &
arrowflight_pid=$!

cleanup() {
  kill "${arrowflight_pid}" >/dev/null 2>&1 || true
  wait "${arrowflight_pid}" >/dev/null 2>&1 || true
  kill "${gpfdist_pid}" >/dev/null 2>&1 || true
  wait "${gpfdist_pid}" >/dev/null 2>&1 || true
  cd "${GPDEMO_DIR}"
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_BENCH_SEGMENTS}" \
    make destroy-demo-cluster >/tmp/arrowflight_benchmark_destroy_after.log 2>&1 || true
}
trap cleanup EXIT

sleep 1
kill -0 "${gpfdist_pid}"
kill -0 "${arrowflight_pid}"

metadata_json="$(cat "${DATA_DIR}/metadata.json")"
expected_rows="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["rows"])' <<<"${metadata_json}")"
expected_id_sum="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id_sum"])' <<<"${metadata_json}")"
expected_segid_sum="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["segid_sum"])' <<<"${metadata_json}")"
logical_bytes="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["logical_bytes"])' <<<"${metadata_json}")"
csv_bytes="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["csv_bytes"])' <<<"${metadata_json}")"
arrow_raw_bytes="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["arrow_raw_bytes"])' <<<"${metadata_json}")"
if [ "${ARROWFLIGHT_BENCH_SCHEMA}" = "fixed" ]; then
  arrow_dataset="bench_fixed"
  arrow_ipc_glob="bench_fixed_*.arrow"
else
  arrow_dataset="bench"
  arrow_ipc_glob="bench_[0-9]*.arrow"
fi
arrow_source_bytes="${arrow_raw_bytes}"
arrow_source_bytes_kind="estimated_arrow_array_buffers_no_ipc_or_grpc_framing"
if [ "${ARROWFLIGHT_BENCH_SOURCE}" = "ipc" ]; then
  arrow_source_bytes="$(
    python3 - "${IPC_DIR}" "${arrow_ipc_glob}" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
pattern = sys.argv[2]
paths = list(root.glob(pattern))
if not paths:
    raise SystemExit(f"no Arrow IPC files match {root}/{pattern}")
print(sum(path.stat().st_size for path in paths))
PY
  )"
  arrow_source_bytes_kind="arrow_ipc_stream_file_bytes"
fi
make_gpfdist_locations() {
  local sep=""
  local segid

  for ((segid = 0; segid < ARROWFLIGHT_BENCH_SEGMENTS; segid++)); do
    printf "%s'gpfdist://%s:%s/bench_%d.csv'" \
      "${sep}" "${ARROWFLIGHT_BENCH_GPFDIST_HOST}" \
      "${ARROWFLIGHT_BENCH_GPFDIST_PORT}" "${segid}"
    sep=", "
  done
}

profile_query=""
if [ "${ARROWFLIGHT_BENCH_PROFILE}" = "1" ] ||
   [ "${ARROWFLIGHT_BENCH_PROFILE}" = "true" ] ||
   [ "${ARROWFLIGHT_BENCH_PROFILE}" = "yes" ]; then
  profile_query="&profile=true"
fi

arrow_location_fdw="'arrowflight://${ARROWFLIGHT_BENCH_ARROW_HOST}:${ARROWFLIGHT_SMOKE_PORT}/dataset/${arrow_dataset}/segments/${ARROWFLIGHT_BENCH_SEGMENTS}?use_get_flight_info=true&flight_endpoint_policy=segment_index&max_batch_bytes=${ARROWFLIGHT_BENCH_MAX_BATCH_BYTES}&timeout_ms=60000${profile_query}&profile_label=arrow_flight_fdw'"
gpfdist_locations="$(make_gpfdist_locations)"

if [ "${ARROWFLIGHT_BENCH_SCHEMA}" = "fixed" ]; then
  table_columns="
    id int4,
    segid int4,
    active bool,
    amount float8,
    d date,
    ts timestamp"
else
  table_columns="
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp"
fi

psql -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;
SET TIME ZONE 'UTC';
SET optimizer=off;

DROP FOREIGN TABLE IF EXISTS af_bench_arrow_fdw;
DROP EXTERNAL TABLE IF EXISTS af_bench_gpfdist_csv;
DROP SERVER IF EXISTS af_bench_arrow_srv CASCADE;

CREATE SERVER af_bench_arrow_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (mpp_execute 'all segments');

CREATE FOREIGN TABLE af_bench_arrow_fdw (${table_columns})
SERVER af_bench_arrow_srv
OPTIONS (url ${arrow_location_fdw}, rows '${expected_rows}');

CREATE READABLE EXTERNAL TABLE af_bench_gpfdist_csv (${table_columns})
LOCATION (${gpfdist_locations})
FORMAT 'CSV';
SQL

printf "method,phase,run,row_count,id_sum,segid_sum,wall_sec,cpu_sec,logical_bytes,source_bytes,source_bytes_kind,source_bytes_per_row,net_bytes,net_bytes_per_row,rows_per_sec,cpu_sec_per_gb,cpu_sec_per_source_gb\n" \
  >"${RUNS_CSV}"
printf "method,p50_segment_latency_ms,p95_segment_latency_ms,segments,source\n" \
  >"${LATENCY_CSV}"
printf "method,phase,run,consumer,label,segment,endpoint_index,rows,batches,next_calls,open_total_us,connect_us,get_flight_info_us,endpoint_connect_us,doget_us,get_schema_us,validate_schema_us,next_us,fdw_decode_us,slot_store_us,varlena_us,fdw_rows,varlena_values,varlena_bytes\n" \
  >"${PROFILE_CSV}"

parse_client_profiles() {
  local method="$1"
  local phase="$2"
  local run_id="$3"
  local log_file="$4"

  python3 - "$PROFILE_CSV" "$method" "$phase" "$run_id" "$log_file" <<'PY'
import csv
import re
import sys

out_path, method, phase, run_id, log_path = sys.argv[1:6]
columns = [
    "consumer", "label", "segment", "endpoint_index", "rows", "batches",
    "next_calls", "open_total_us", "connect_us", "get_flight_info_us",
    "endpoint_connect_us", "doget_us", "get_schema_us",
    "validate_schema_us", "next_us", "fdw_decode_us",
    "slot_store_us", "varlena_us", "fdw_rows", "varlena_values",
    "varlena_bytes",
]
text = open(log_path, encoding="utf-8", errors="replace").read()
rows = []
for match in re.finditer(r"arrowflight_profile\s+([^\n\r]+)", text):
    values = {}
    for token in match.group(1).split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        values[key] = value
    rows.append([method, phase, run_id] + [values.get(col, "") for col in columns])

if rows:
    with open(out_path, "a", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerows(rows)
PY
}

read_cpu_usec() {
  if [ -r /sys/fs/cgroup/cpu.stat ]; then
    awk '$1 == "usage_usec" { print $2 }' /sys/fs/cgroup/cpu.stat
    return
  fi

  if [ -r /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
    awk '{ printf "%.0f\n", $1 / 1000 }' /sys/fs/cgroup/cpuacct/cpuacct.usage
    return
  fi

  awk '
    /^cpu / {
      total = 0;
      for (i = 2; i <= NF; i++) {
        total += $i;
      }
      printf "%.0f\n", total * 1000000 / hz;
    }
  ' hz="$(getconf CLK_TCK)" /proc/stat
}

read_net_bytes() {
  local dev="${ARROWFLIGHT_BENCH_NET_DEV}"
  local rx_path
  local tx_path
  local rx
  local tx

  if [ -z "${dev}" ]; then
    echo 0
    return
  fi

  rx_path="/sys/class/net/${dev}/statistics/rx_bytes"
  tx_path="/sys/class/net/${dev}/statistics/tx_bytes"
  if [ ! -r "${rx_path}" ] || [ ! -r "${tx_path}" ]; then
    echo "network interface ${dev} does not expose byte counters" >&2
    exit 1
  fi

  rx="$(cat "${rx_path}")"
  tx="$(cat "${tx_path}")"
  echo $((rx + tx))
}

run_scan() {
  local method="$1"
  local table_name="$2"
  local phase="$3"
  local run_id="$4"
  local source_bytes="$5"
  local source_bytes_kind="$6"
  local before_cpu
  local after_cpu
  local before_net
  local after_net
  local before_ns
  local after_ns
  local output
  local stderr_file="${ARROWFLIGHT_BENCH_RESULT_DIR}/psql_${method}_${phase}_${run_id}.stderr"

  before_cpu="$(read_cpu_usec)"
  before_net="$(read_net_bytes)"
  before_ns="$(date +%s%N)"
  output="$(psql -X -qAt -F '|' -v ON_ERROR_STOP=1 postgres \
    2>"${stderr_file}" <<SQL
SET optimizer=off;
SET TIME ZONE 'UTC';
SELECT count(*)::bigint,
       coalesce(sum(id), 0)::bigint,
       coalesce(sum(segid), 0)::bigint
FROM ${table_name};
SQL
)"
  after_ns="$(date +%s%N)"
  after_net="$(read_net_bytes)"
  after_cpu="$(read_cpu_usec)"
  parse_client_profiles "${method}" "${phase}" "${run_id}" "${stderr_file}"

  IFS='|' read -r row_count id_sum segid_sum <<<"${output}"
  if [ "${row_count}" != "${expected_rows}" ] ||
     [ "${id_sum}" != "${expected_id_sum}" ] ||
     [ "${segid_sum}" != "${expected_segid_sum}" ]; then
    echo "benchmark validation failed for ${method}: ${output}" >&2
    echo "expected ${expected_rows}|${expected_id_sum}|${expected_segid_sum}" >&2
    exit 1
  fi

  python3 - "$RUNS_CSV" "$method" "$phase" "$run_id" "$row_count" \
    "$id_sum" "$segid_sum" "$before_ns" "$after_ns" "$before_cpu" \
    "$after_cpu" "$before_net" "$after_net" "$logical_bytes" "$source_bytes" \
    "$source_bytes_kind" <<'PY'
import csv
import sys

path, method, phase, run_id = sys.argv[1:5]
row_count, id_sum, segid_sum = sys.argv[5:8]
before_ns, after_ns, before_cpu, after_cpu = map(int, sys.argv[8:12])
before_net, after_net, logical_bytes, source_bytes = map(int, sys.argv[12:16])
source_bytes_kind = sys.argv[16]
wall_sec = (after_ns - before_ns) / 1_000_000_000
cpu_sec = (after_cpu - before_cpu) / 1_000_000
rows = int(row_count)
logical_gb = logical_bytes / (1024 ** 3)
source_gb = source_bytes / (1024 ** 3)
rows_per_sec = rows / wall_sec if wall_sec > 0 else 0.0
cpu_sec_per_gb = cpu_sec / logical_gb if logical_gb > 0 else 0.0
cpu_sec_per_source_gb = cpu_sec / source_gb if source_gb > 0 else 0.0
source_bytes_per_row = source_bytes / rows if rows > 0 else 0.0
net_bytes = max(after_net - before_net, 0)
net_bytes_per_row = net_bytes / rows if rows > 0 else 0.0

with open(path, "a", newline="", encoding="utf-8") as out:
    writer = csv.writer(out)
    writer.writerow([
        method,
        phase,
        run_id,
        row_count,
        id_sum,
        segid_sum,
        f"{wall_sec:.6f}",
        f"{cpu_sec:.6f}",
        logical_bytes,
        source_bytes,
        source_bytes_kind,
        f"{source_bytes_per_row:.2f}",
        net_bytes,
        f"{net_bytes_per_row:.2f}",
        f"{rows_per_sec:.2f}",
        f"{cpu_sec_per_gb:.2f}",
        f"{cpu_sec_per_source_gb:.2f}",
    ])
PY
}

run_latency_explain() {
  local method="$1"
  local table_name="$2"
  local explain_file="${ARROWFLIGHT_BENCH_RESULT_DIR}/explain_${method}.txt"
  local stderr_file="${ARROWFLIGHT_BENCH_RESULT_DIR}/psql_${method}_explain_0.stderr"

  psql -X -qAt -v ON_ERROR_STOP=1 postgres >"${explain_file}" \
    2>"${stderr_file}" <<SQL
SET optimizer=off;
SET gp_enable_explain_allstat=on;
SET TIME ZONE 'UTC';
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, SUMMARY OFF)
SELECT count(*)::bigint,
       coalesce(sum(id), 0)::bigint,
       coalesce(sum(segid), 0)::bigint
FROM ${table_name};
SQL
  parse_client_profiles "${method}" "explain" "0" "${stderr_file}"

  python3 - "$LATENCY_CSV" "$method" "$explain_file" \
    "$ARROWFLIGHT_BENCH_ROWS_PER_SEGMENT" <<'PY'
import csv
import math
import re
import sys

latency_csv, method, explain_file, rows_per_segment = sys.argv[1:5]
rows_per_segment = int(rows_per_segment)
text = open(explain_file, encoding="utf-8").read()
groups = []

for line in text.splitlines():
    if "allstat:" not in line:
        continue

    entries = []
    for segid, first_ms, total_ms, tuples in re.findall(
            r"/seg(-?\d+)_([^_]+)_([^_]+)_([0-9.]+)", line):
        entries.append({
            "segid": int(segid),
            "first_ms": float(first_ms.replace(" ms", "")),
            "total_ms": float(total_ms.replace(" ms", "")),
            "tuples": float(tuples),
        })

    if entries:
        groups.append(entries)

candidate = None
for entries in groups:
    if max(item["tuples"] for item in entries) >= rows_per_segment:
        candidate = entries
        break

if candidate is None and groups:
    candidate = max(groups, key=lambda items: max(item["tuples"] for item in items))

if candidate is None:
    p50 = ""
    p95 = ""
    count = 0
    source = "missing_allstat"
else:
    values = sorted(item["total_ms"] for item in candidate)
    count = len(values)
    mid = count // 2
    p50 = values[mid] if count % 2 else (values[mid - 1] + values[mid]) / 2
    p95 = values[max(0, math.ceil(count * 0.95) - 1)]
    source = "explain_allstat"

with open(latency_csv, "a", newline="", encoding="utf-8") as out:
    writer = csv.writer(out)
    writer.writerow([
        method,
        "" if p50 == "" else f"{p50:.3f}",
        "" if p95 == "" else f"{p95:.3f}",
        count,
        source,
    ])
PY
}

for method_table_source in \
  "arrow_flight_fdw af_bench_arrow_fdw ${arrow_source_bytes} ${arrow_source_bytes_kind}" \
  "gpfdist_csv af_bench_gpfdist_csv ${csv_bytes} csv_file_bytes"; do
  read -r method table_name source_bytes source_bytes_kind <<<"${method_table_source}"

  for ((run_id = 1; run_id <= ARROWFLIGHT_BENCH_WARMUPS; run_id++)); do
    run_scan "${method}" "${table_name}" "warmup" "${run_id}" \
      "${source_bytes}" "${source_bytes_kind}"
  done

  for ((run_id = 1; run_id <= ARROWFLIGHT_BENCH_REPEATS; run_id++)); do
    run_scan "${method}" "${table_name}" "run" "${run_id}" \
      "${source_bytes}" "${source_bytes_kind}"
  done

  run_latency_explain "${method}" "${table_name}"
done

python3 - "$SERVER_PROFILE_CSV" \
  "${ARROWFLIGHT_BENCH_RESULT_DIR}/arrowflightd.log" <<'PY'
import csv
import re
import sys

out_path, log_path = sys.argv[1:3]
columns = ["event", "dataset", "segment", "prebuilt", "source", "rows",
           "build_us", "file"]
rows = []
text = open(log_path, encoding="utf-8", errors="replace").read()

for match in re.finditer(r"arrowflightd_profile\s+([^\n\r]+)", text):
    values = {}
    for token in match.group(1).split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        values[key] = value
    rows.append([values.get(col, "") for col in columns])

with open(out_path, "w", newline="", encoding="utf-8") as out:
    writer = csv.writer(out)
    writer.writerow(columns)
    writer.writerows(rows)
PY

python3 - "$RUNS_CSV" "$LATENCY_CSV" "$DATA_DIR/metadata.json" \
  "$PROFILE_CSV" "$SERVER_PROFILE_CSV" "$SUMMARY_JSON" "$SUMMARY_MD" \
  "$ARROWFLIGHT_BENCH_REPEATS" "$ARROWFLIGHT_BENCH_WARMUPS" \
  "$ARROWFLIGHT_BENCH_BATCH_ROWS" "$ARROWFLIGHT_BENCH_MAX_BATCH_BYTES" \
  "$ARROWFLIGHT_BENCH_PREBUILD" "$ARROWFLIGHT_BENCH_PROFILE" \
  "$ARROWFLIGHT_BENCH_SOURCE" "$ARROWFLIGHT_BENCH_NET_DEV" <<'PY'
import csv
import json
import statistics
import sys

runs_csv, latency_csv, metadata_path, profile_csv, server_profile_csv, \
    summary_json, summary_md = sys.argv[1:8]
repeats, warmups, batch_rows, max_batch_bytes = map(int, sys.argv[8:12])
prebuild, profile_enabled, source_mode, net_dev = sys.argv[12:16]
metadata = json.load(open(metadata_path, encoding="utf-8"))

with open(runs_csv, encoding="utf-8") as source:
    runs = list(csv.DictReader(source))

with open(latency_csv, encoding="utf-8") as source:
    latencies = {row["method"]: row for row in csv.DictReader(source)}

with open(profile_csv, encoding="utf-8") as source:
    profiles = list(csv.DictReader(source))

with open(server_profile_csv, encoding="utf-8") as source:
    server_profiles = list(csv.DictReader(source))

methods = []
for method in ["arrow_flight_fdw", "gpfdist_csv"]:
    method_runs = [row for row in runs
                   if row["method"] == method and row["phase"] == "run"]
    wall = [float(row["wall_sec"]) for row in method_runs]
    cpu = [float(row["cpu_sec"]) for row in method_runs]
    rows_per_sec = [float(row["rows_per_sec"]) for row in method_runs]
    cpu_per_gb = [float(row["cpu_sec_per_gb"]) for row in method_runs]
    cpu_per_source_gb = [float(row["cpu_sec_per_source_gb"])
                         for row in method_runs]
    net_bytes = [int(row.get("net_bytes") or 0) for row in method_runs]
    net_bytes_per_row = [float(row.get("net_bytes_per_row") or 0)
                         for row in method_runs]
    latency = latencies.get(method, {})

    methods.append({
        "method": method,
        "runs": len(method_runs),
        "median_wall_sec": statistics.median(wall),
        "median_cpu_sec": statistics.median(cpu),
        "median_rows_per_sec": statistics.median(rows_per_sec),
        "median_cpu_sec_per_gb": statistics.median(cpu_per_gb),
        "median_cpu_sec_per_source_gb": statistics.median(cpu_per_source_gb),
        "median_net_bytes": statistics.median(net_bytes),
        "median_net_bytes_per_row": statistics.median(net_bytes_per_row),
        "p50_segment_latency_ms": (
            None if not latency.get("p50_segment_latency_ms")
            else float(latency["p50_segment_latency_ms"])
        ),
        "p95_segment_latency_ms": (
            None if not latency.get("p95_segment_latency_ms")
            else float(latency["p95_segment_latency_ms"])
        ),
        "latency_source": latency.get("source"),
        "source_bytes": int(method_runs[0]["source_bytes"]),
        "source_bytes_kind": method_runs[0]["source_bytes_kind"],
        "source_bytes_per_row": float(method_runs[0]["source_bytes_per_row"]),
    })

profile_fields = [
    "rows", "batches", "next_calls", "open_total_us", "connect_us",
    "get_flight_info_us", "endpoint_connect_us", "doget_us",
    "get_schema_us", "validate_schema_us", "next_us", "fdw_decode_us",
    "slot_store_us", "varlena_us", "fdw_rows", "varlena_values",
    "varlena_bytes",
]

client_profiles = []
for method in ["arrow_flight_fdw"]:
    method_profiles = [row for row in profiles
                       if row["method"] == method and row["phase"] == "run"]
    if not method_profiles:
        continue

    totals = {}
    for field in profile_fields:
        totals[field] = sum(int(float(row[field] or 0))
                            for row in method_profiles)

    accounted_us = totals["open_total_us"] + totals["next_us"] + \
        totals["fdw_decode_us"] + totals["slot_store_us"]

    rows_total = max(totals["rows"], 1)
    client_profiles.append({
        "method": method,
        "profile_rows": len(method_profiles),
        "totals": totals,
        "accounted_us": accounted_us,
        "accounted_us_per_row": accounted_us / rows_total,
        "next_us_per_batch": (
            totals["next_us"] / totals["next_calls"]
            if totals["next_calls"] else None
        ),
        "fdw_decode_us_per_row": (
            totals["fdw_decode_us"] / totals["fdw_rows"]
            if totals["fdw_rows"] else None
        ),
        "varlena_us_per_value": (
            totals["varlena_us"] / totals["varlena_values"]
            if totals["varlena_values"] else None
        ),
    })

source_profile_events = {"build", "ipc_read"}
server_totals = {
    "profile_rows": len(server_profiles),
    "build_us": sum(int(float(row["build_us"] or 0))
                    for row in server_profiles),
    "build_rows": sum(int(float(row["rows"] or 0))
                      for row in server_profiles
                      if row.get("event") == "build"),
    "source_us": sum(int(float(row["build_us"] or 0))
                     for row in server_profiles
                     if row.get("event") in source_profile_events),
    "source_rows": sum(int(float(row["rows"] or 0))
                       for row in server_profiles
                       if row.get("event") in source_profile_events),
    "ipc_reads": sum(1 for row in server_profiles
                     if row.get("event") == "ipc_read"),
    "cache_hits": sum(1 for row in server_profiles
                      if row.get("event") == "cache_hit"),
}

summary = {
    "config": {
        "repeats": repeats,
        "warmups": warmups,
        "bench_batch_rows": batch_rows,
        "max_batch_bytes": max_batch_bytes,
        "prebuild": prebuild,
        "profile": profile_enabled,
        "source": source_mode,
        "net_dev": net_dev,
        "optimizer": "off",
    },
    "dataset": metadata,
    "methods": methods,
    "client_profiles": client_profiles,
    "server_profiles": server_totals,
    "runs": runs,
    "profiles": profiles,
    "server_profile_rows": server_profiles,
}

def fmt_optional(value):
    return "" if value is None else f"{value:.3f}"

with open(summary_json, "w", encoding="utf-8") as out:
    json.dump(summary, out, indent=2)
    out.write("\n")

with open(summary_md, "w", encoding="utf-8") as out:
    out.write("# Arrow Flight Benchmark Summary\n\n")
    out.write(
        f"Dataset: {metadata['rows']} rows, "
        f"{metadata['segments']} segments, "
        f"{metadata['rows_per_segment']} rows/segment, "
        f"schema={metadata.get('schema', 'mixed')}, "
        f"logical CSV bytes={metadata['logical_bytes']}, "
        f"estimated Arrow raw bytes={metadata['arrow_raw_bytes']}, "
        f"source={source_mode}, prebuild={prebuild}, "
        f"profile={profile_enabled}, "
        f"net_dev={net_dev or 'disabled'}, "
        "optimizer=off.\n\n"
    )
    out.write(
        "| method | rows/s | CPU sec/logical GB | CPU sec/source GB | source MB | source B/row | net MB | net B/row | wall sec | "
        "p50 segment ms | p95 segment ms |\n"
    )
    out.write("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    for row in methods:
        p50 = row["p50_segment_latency_ms"]
        p95 = row["p95_segment_latency_ms"]
        out.write(
            f"| {row['method']} | "
            f"{row['median_rows_per_sec']:.2f} | "
            f"{row['median_cpu_sec_per_gb']:.2f} | "
            f"{row['median_cpu_sec_per_source_gb']:.2f} | "
            f"{row['source_bytes'] / (1024 ** 2):.2f} | "
            f"{row['source_bytes_per_row']:.2f} | "
            f"{row['median_net_bytes'] / (1024 ** 2):.2f} | "
            f"{row['median_net_bytes_per_row']:.2f} | "
            f"{row['median_wall_sec']:.3f} | "
            f"{'' if p50 is None else f'{p50:.3f}'} | "
            f"{'' if p95 is None else f'{p95:.3f}'} |\n"
        )

    out.write("\nSource byte notes:\n\n")
    out.write("- `arrow_flight_fdw` source bytes are estimated Arrow array buffers unless `source=ipc`, where they are Arrow IPC stream file bytes without TCP/gRPC framing.\n")
    out.write("- `gpfdist_csv` source bytes are CSV file bytes.\n")
    out.write("- Network bytes are rx+tx deltas from `/sys/class/net/$ARROWFLIGHT_BENCH_NET_DEV/statistics/*_bytes`; they are `0` when the env var is empty.\n")

    if client_profiles:
        out.write("\nClient profile, summed across run-phase QE profiles:\n\n")
        out.write(
            "| method | profiles | accounted us/row | next us/batch | fdw decode us/row | varlena us/value |\n"
        )
        out.write("|---|---:|---:|---:|---:|---:|\n")
        for row in client_profiles:
            out.write(
                f"| {row['method']} | "
                f"{row['profile_rows']} | "
                f"{row['accounted_us_per_row']:.3f} | "
                f"{fmt_optional(row['next_us_per_batch'])} | "
                f"{fmt_optional(row['fdw_decode_us_per_row'])} | "
                f"{fmt_optional(row['varlena_us_per_value'])} |\n"
            )

    if server_profiles:
        out.write("\nServer profile:\n\n")
        out.write(
            f"- source rows: {server_totals['source_rows']}\n"
            f"- source us: {server_totals['source_us']}\n"
            f"- build rows: {server_totals['build_rows']}\n"
            f"- ipc reads: {server_totals['ipc_reads']}\n"
            f"- cache hits: {server_totals['cache_hits']}\n"
        )

print(open(summary_md, encoding="utf-8").read())
PY

psql -v ON_ERROR_STOP=1 postgres <<SQL
DROP FOREIGN TABLE IF EXISTS af_bench_arrow_fdw;
DROP EXTERNAL TABLE IF EXISTS af_bench_gpfdist_csv;
DROP SERVER IF EXISTS af_bench_arrow_srv CASCADE;
DROP EXTENSION IF EXISTS arrowflight CASCADE;
SQL

cat "${ARROWFLIGHT_BENCH_RESULT_DIR}/arrowflightd.log"
