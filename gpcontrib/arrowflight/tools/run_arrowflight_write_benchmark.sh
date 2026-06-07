#!/usr/bin/env bash
set -euo pipefail

: "${ARROWFLIGHTD_SERVER:=/tmp/arrowflightd}"
: "${ARROWFLIGHT_SMOKE_PORT:=8815}"
: "${ARROWFLIGHT_WRITE_BENCH_GPFDIST_PORT:=8816}"
: "${ARROWFLIGHT_WRITE_BENCH_ARROW_HOST:=127.0.0.1}"
: "${ARROWFLIGHT_WRITE_BENCH_GPFDIST_HOST:=127.0.0.1}"
: "${ARROWFLIGHT_WRITE_BENCH_NET_DEV:=}"
: "${ARROWFLIGHT_WRITE_BENCH_SEGMENTS:=2}"
: "${ARROWFLIGHT_WRITE_BENCH_ROWS_PER_SEGMENT:=100000}"
: "${ARROWFLIGHT_WRITE_BENCH_BATCH_ROWS:=8192}"
: "${ARROWFLIGHT_WRITE_BENCH_LABEL_WIDTH:=32}"
: "${ARROWFLIGHT_WRITE_BENCH_SCHEMA:=mixed}"
: "${ARROWFLIGHT_WRITE_BENCH_REPEATS:=3}"
: "${ARROWFLIGHT_WRITE_BENCH_WARMUPS:=1}"
: "${ARROWFLIGHT_WRITE_BENCH_MAX_BATCH_BYTES:=4194304}"
: "${ARROWFLIGHT_WRITE_BENCH_ROOT:=/tmp/arrowflight_write_benchmark}"
: "${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR:=${ARROWFLIGHT_WRITE_BENCH_ROOT}/results}"
: "${GPDEMO_DIR:=/home/gpadmin/gpdb_src/gpAux/gpdemo}"
: "${GPHOME:=/home/gpadmin/greengage-db-devel}"

DATA_DIR="${ARROWFLIGHT_WRITE_BENCH_ROOT}/data"
RUNS_CSV="${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/runs.csv"
LATENCY_CSV="${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/latency.csv"
SERVER_PROFILE_CSV="${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/server_profiles.csv"
SUMMARY_JSON="${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/summary.json"
SUMMARY_MD="${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/summary.md"

source "${GPHOME}/greengage_path.sh"

case "${ARROWFLIGHT_WRITE_BENCH_SCHEMA}" in
  mixed|fixed)
    ;;
  *)
    echo "invalid ARROWFLIGHT_WRITE_BENCH_SCHEMA=${ARROWFLIGHT_WRITE_BENCH_SCHEMA}" >&2
    echo "use mixed or fixed" >&2
    exit 1
    ;;
esac

af_bench_script_source="${BASH_SOURCE[0]:-$0}"
AF_BENCH_SCRIPT_DIR="$(cd "$(dirname "${af_bench_script_source}")" && pwd)"

mkdir -p "${DATA_DIR}" "${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}"
rm -f "${RUNS_CSV}" "${LATENCY_CSV}" "${SERVER_PROFILE_CSV}" \
  "${SUMMARY_JSON}" "${SUMMARY_MD}"
rm -f "${DATA_DIR}"/write_gpfdist_*.csv*

python3 "${AF_BENCH_SCRIPT_DIR}/arrowflight_benchmark_data.py" \
  --output-dir "${DATA_DIR}" \
  --segments "${ARROWFLIGHT_WRITE_BENCH_SEGMENTS}" \
  --rows-per-segment "${ARROWFLIGHT_WRITE_BENCH_ROWS_PER_SEGMENT}" \
  --batch-rows "${ARROWFLIGHT_WRITE_BENCH_BATCH_ROWS}" \
  --label-width "${ARROWFLIGHT_WRITE_BENCH_LABEL_WIDTH}" \
  --schema "${ARROWFLIGHT_WRITE_BENCH_SCHEMA}" \
  >"${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/data_generation.json"

if [ ! -x "${GPHOME}/bin/gpfdist" ]; then
  echo "gpfdist binary is not executable at ${GPHOME}/bin/gpfdist" >&2
  exit 1
fi

cd "${GPDEMO_DIR}"
WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_WRITE_BENCH_SEGMENTS}" \
  make destroy-demo-cluster >/tmp/arrowflight_write_benchmark_destroy_before.log 2>&1 || true

if ! WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_WRITE_BENCH_SEGMENTS}" \
  make create-demo-cluster >/tmp/arrowflight_write_benchmark_create.log 2>&1; then
  cat /tmp/arrowflight_write_benchmark_create.log
  exit 1
fi

source "${GPDEMO_DIR}/gpdemo-env.sh"

"${GPHOME}/bin/gpfdist" -d "${DATA_DIR}" \
  -p "${ARROWFLIGHT_WRITE_BENCH_GPFDIST_PORT}" \
  -m 67108864 \
  >"${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/gpfdist.log" 2>&1 &
gpfdist_pid=$!

"${ARROWFLIGHTD_SERVER}" "${ARROWFLIGHT_SMOKE_PORT}" \
  >"${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/arrowflightd.log" 2>&1 &
arrowflight_pid=$!

cleanup() {
  kill "${arrowflight_pid}" >/dev/null 2>&1 || true
  wait "${arrowflight_pid}" >/dev/null 2>&1 || true
  kill "${gpfdist_pid}" >/dev/null 2>&1 || true
  wait "${gpfdist_pid}" >/dev/null 2>&1 || true
  cd "${GPDEMO_DIR}"
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS="${ARROWFLIGHT_WRITE_BENCH_SEGMENTS}" \
    make destroy-demo-cluster >/tmp/arrowflight_write_benchmark_destroy_after.log 2>&1 || true
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

if [ "${ARROWFLIGHT_WRITE_BENCH_SCHEMA}" = "fixed" ]; then
  table_columns="
    id int4,
    segid int4,
    active bool,
    amount float8,
    d date,
    ts timestamp"
  source_select="
SELECT gp_segment_id * ${ARROWFLIGHT_WRITE_BENCH_ROWS_PER_SEGMENT} + n AS id,
       gp_segment_id AS segid,
       (n % 2 = 1) AS active,
       (n + 0.5)::float8 AS amount,
       DATE '2000-01-01' + ((n % 365) + 1) AS d,
       TIMESTAMP '2000-01-01' + (n * INTERVAL '1 second') AS ts
FROM gp_dist_random('gp_id'), generate_series(1, ${ARROWFLIGHT_WRITE_BENCH_ROWS_PER_SEGMENT}) AS g(n)"
else
  table_columns="
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp"
  source_select="
SELECT gp_segment_id * ${ARROWFLIGHT_WRITE_BENCH_ROWS_PER_SEGMENT} + n AS id,
       gp_segment_id AS segid,
       rpad('bench-seg-' || gp_segment_id::text || '-row-' || n::text,
            ${ARROWFLIGHT_WRITE_BENCH_LABEL_WIDTH}, 'x') AS label,
       (n % 2 = 1) AS active,
       (n + 0.5)::float8 AS amount,
       DATE '2000-01-01' + ((n % 365) + 1) AS d,
       TIMESTAMP '2000-01-01' + (n * INTERVAL '1 second') AS ts
FROM gp_dist_random('gp_id'), generate_series(1, ${ARROWFLIGHT_WRITE_BENCH_ROWS_PER_SEGMENT}) AS g(n)"
fi

arrow_write_url="'arrowflight://${ARROWFLIGHT_WRITE_BENCH_ARROW_HOST}:${ARROWFLIGHT_SMOKE_PORT}/bench_write?timeout_ms=60000&retry_count=0'"

psql -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;
SET TIME ZONE 'UTC';
SET optimizer=off;

DROP FOREIGN TABLE IF EXISTS af_bench_arrow_write;
DROP EXTERNAL TABLE IF EXISTS af_bench_gpfdist_write;
DROP TABLE IF EXISTS af_bench_write_source;
DROP SERVER IF EXISTS af_bench_arrow_write_srv CASCADE;

CREATE TABLE af_bench_write_source (${table_columns})
DISTRIBUTED BY (id);

INSERT INTO af_bench_write_source
${source_select};

ANALYZE af_bench_write_source;

CREATE SERVER af_bench_arrow_write_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    write_mode 'staging'
);

CREATE FOREIGN TABLE af_bench_arrow_write (${table_columns})
SERVER af_bench_arrow_write_srv
OPTIONS (
    url ${arrow_write_url},
    operation_metadata 'static.source=write_benchmark',
    batch_rows '${ARROWFLIGHT_WRITE_BENCH_BATCH_ROWS}',
    max_batch_bytes '${ARROWFLIGHT_WRITE_BENCH_MAX_BATCH_BYTES}'
);
SQL

source_validation="$(psql -X -qAt -F '|' -v ON_ERROR_STOP=1 postgres <<'SQL'
SET optimizer=off;
SELECT count(*)::bigint,
       coalesce(sum(id), 0)::bigint,
       coalesce(sum(segid), 0)::bigint
FROM af_bench_write_source;
SQL
)"
if [ "${source_validation}" != "${expected_rows}|${expected_id_sum}|${expected_segid_sum}" ]; then
  echo "write source validation failed: ${source_validation}" >&2
  echo "expected ${expected_rows}|${expected_id_sum}|${expected_segid_sum}" >&2
  exit 1
fi

printf "method,phase,run,row_count,id_sum,segid_sum,wall_sec,cpu_sec,logical_bytes,write_bytes,write_bytes_kind,write_bytes_per_row,net_bytes,net_bytes_per_row,rows_per_sec,cpu_sec_per_gb,cpu_sec_per_write_gb\n" \
  >"${RUNS_CSV}"
printf "method,p50_segment_latency_ms,p95_segment_latency_ms,segments,source\n" \
  >"${LATENCY_CSV}"

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
  local dev="${ARROWFLIGHT_WRITE_BENCH_NET_DEV}"
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

record_run() {
  local method="$1"
  local phase="$2"
  local run_id="$3"
  local row_count="$4"
  local id_sum="$5"
  local segid_sum="$6"
  local before_ns="$7"
  local after_ns="$8"
  local before_cpu="$9"
  local after_cpu="${10}"
  local before_net="${11}"
  local after_net="${12}"
  local write_bytes="${13}"
  local write_bytes_kind="${14}"

  python3 - "$RUNS_CSV" "$method" "$phase" "$run_id" "$row_count" \
    "$id_sum" "$segid_sum" "$before_ns" "$after_ns" "$before_cpu" \
    "$after_cpu" "$before_net" "$after_net" "$logical_bytes" "$write_bytes" \
    "$write_bytes_kind" <<'PY'
import csv
import sys

path, method, phase, run_id = sys.argv[1:5]
row_count, id_sum, segid_sum = sys.argv[5:8]
before_ns, after_ns, before_cpu, after_cpu = map(int, sys.argv[8:12])
before_net, after_net, logical_bytes, write_bytes = map(int, sys.argv[12:16])
write_bytes_kind = sys.argv[16]
wall_sec = (after_ns - before_ns) / 1_000_000_000
cpu_sec = (after_cpu - before_cpu) / 1_000_000
rows = int(row_count)
logical_gb = logical_bytes / (1024 ** 3)
write_gb = write_bytes / (1024 ** 3)
rows_per_sec = rows / wall_sec if wall_sec > 0 else 0.0
cpu_sec_per_gb = cpu_sec / logical_gb if logical_gb > 0 else 0.0
cpu_sec_per_write_gb = cpu_sec / write_gb if write_gb > 0 else 0.0
write_bytes_per_row = write_bytes / rows if rows > 0 else 0.0
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
        write_bytes,
        write_bytes_kind,
        f"{write_bytes_per_row:.2f}",
        net_bytes,
        f"{net_bytes_per_row:.2f}",
        f"{rows_per_sec:.2f}",
        f"{cpu_sec_per_gb:.2f}",
        f"{cpu_sec_per_write_gb:.2f}",
    ])
PY
}

insert_row_count() {
  python3 - "$1" <<'PY'
import re
import sys

text = sys.argv[1]
match = re.search(r"INSERT\s+0\s+(\d+)", text)
if not match:
    raise SystemExit(f"could not parse INSERT row count from: {text!r}")
print(match.group(1))
PY
}

latest_arrow_operation() {
  python3 - "${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/arrowflightd.log" \
    "${expected_rows}" "${ARROWFLIGHT_WRITE_BENCH_SEGMENTS}" <<'PY'
import re
import sys

path, expected_rows, expected_segments = sys.argv[1:4]
expected_rows = int(expected_rows)
expected_segments = int(expected_segments)
groups = {}
order = 0
text = open(path, encoding="utf-8", errors="replace").read()

for line in text.splitlines():
    if "arrowflightd_write_profile" not in line or "dataset=bench_write" not in line:
        continue
    values = {}
    for token in line.split():
        if "=" in token:
            key, value = token.split("=", 1)
            values[key] = value
    opid = values.get("operation_id")
    if not opid:
        continue
    group = groups.setdefault(opid, {
        "last": order,
        "rows": 0,
        "batches": 0,
        "elapsed_us": 0,
        "segments": set(),
    })
    group["last"] = order
    group["rows"] += int(values.get("rows", "0"))
    group["batches"] += int(values.get("batches", "0"))
    group["elapsed_us"] += int(values.get("elapsed_us", "0"))
    if "segment" in values:
        group["segments"].add(int(values["segment"]))
    order += 1

candidates = [
    (data["last"], opid, data)
    for opid, data in groups.items()
    if data["rows"] == expected_rows and len(data["segments"]) == expected_segments
]
if not candidates:
    raise SystemExit("could not find complete bench_write operation in Arrow Flight server log")

_, opid, data = max(candidates)
print(f"{opid}|{data['rows']}|{data['batches']}|{data['elapsed_us']}")
PY
}

validate_arrow_readback() {
  local operation_id="$1"
  local result

  psql -X -q -v ON_ERROR_STOP=1 postgres <<SQL
SET optimizer=off;
DROP FOREIGN TABLE IF EXISTS af_bench_arrow_write_readback;
CREATE FOREIGN TABLE af_bench_arrow_write_readback (${table_columns})
SERVER af_bench_arrow_write_srv
OPTIONS (
    url 'arrowflight://${ARROWFLIGHT_WRITE_BENCH_ARROW_HOST}:${ARROWFLIGHT_SMOKE_PORT}/written/${operation_id}/segments/${ARROWFLIGHT_WRITE_BENCH_SEGMENTS}?use_get_flight_info=true&flight_endpoint_policy=segment_index&timeout_ms=60000',
    rows '${expected_rows}'
);
SQL

  result="$(psql -X -qAt -F '|' -v ON_ERROR_STOP=1 postgres <<'SQL'
SET optimizer=off;
SELECT count(*)::bigint,
       coalesce(sum(id), 0)::bigint,
       coalesce(sum(segid), 0)::bigint
FROM af_bench_arrow_write_readback;
SQL
)"

  psql -X -q -v ON_ERROR_STOP=1 postgres <<'SQL'
DROP FOREIGN TABLE IF EXISTS af_bench_arrow_write_readback;
SQL

  if [ "${result}" != "${expected_rows}|${expected_id_sum}|${expected_segid_sum}" ]; then
    echo "Arrow write readback validation failed for ${operation_id}: ${result}" >&2
    exit 1
  fi
}

gpfdist_file_stats() {
  python3 - "${DATA_DIR}" "$1" \
    "${expected_rows}" "${expected_id_sum}" "${expected_segid_sum}" <<'PY'
import csv
from pathlib import Path
import sys

root, prefix, expected_rows, expected_id_sum, expected_segid_sum = sys.argv[1:6]
root = Path(root)
paths = sorted(path for path in root.glob(prefix + "*") if path.is_file())
if not paths:
    raise SystemExit(f"no gpfdist output files match {root}/{prefix}*")

rows = 0
id_sum = 0
segid_sum = 0
bytes_total = 0
for path in paths:
    bytes_total += path.stat().st_size
    with path.open(newline="", encoding="utf-8") as source:
        for row in csv.reader(source):
            if not row:
                continue
            rows += 1
            id_sum += int(row[0])
            segid_sum += int(row[1])

actual = f"{rows}|{id_sum}|{segid_sum}"
expected = f"{expected_rows}|{expected_id_sum}|{expected_segid_sum}"
if actual != expected:
    raise SystemExit(f"gpfdist write validation failed: {actual}, expected {expected}")

print(f"{bytes_total}|{rows}|{id_sum}|{segid_sum}")
PY
}

create_gpfdist_target() {
  local file_name="$1"

  psql -X -q -v ON_ERROR_STOP=1 postgres <<SQL
SET optimizer=off;
DROP EXTERNAL TABLE IF EXISTS af_bench_gpfdist_write;
CREATE WRITABLE EXTERNAL TABLE af_bench_gpfdist_write (${table_columns})
LOCATION ('gpfdist://${ARROWFLIGHT_WRITE_BENCH_GPFDIST_HOST}:${ARROWFLIGHT_WRITE_BENCH_GPFDIST_PORT}/${file_name}')
FORMAT 'CSV';
SQL
}

run_insert() {
  local method="$1"
  local table_name="$2"
  local phase="$3"
  local run_id="$4"
  local write_bytes="$5"
  local write_bytes_kind="$6"
  local before_cpu
  local after_cpu
  local before_net
  local after_net
  local before_ns
  local after_ns
  local output
  local row_count

  before_cpu="$(read_cpu_usec)"
  before_net="$(read_net_bytes)"
  before_ns="$(date +%s%N)"
  output="$(psql -X -v ON_ERROR_STOP=1 postgres <<SQL
SET optimizer=off;
SET TIME ZONE 'UTC';
INSERT INTO ${table_name}
SELECT * FROM af_bench_write_source;
SQL
)"
  after_ns="$(date +%s%N)"
  after_net="$(read_net_bytes)"
  after_cpu="$(read_cpu_usec)"
  row_count="$(insert_row_count "${output}")"
  if [ "${row_count}" != "${expected_rows}" ]; then
    echo "${method} ${phase} ${run_id} inserted ${row_count}, expected ${expected_rows}" >&2
    exit 1
  fi

  record_run "${method}" "${phase}" "${run_id}" "${row_count}" \
    "${expected_id_sum}" "${expected_segid_sum}" "${before_ns}" "${after_ns}" \
    "${before_cpu}" "${after_cpu}" "${before_net}" "${after_net}" \
    "${write_bytes}" "${write_bytes_kind}"
}

run_arrow_write() {
  local phase="$1"
  local run_id="$2"
  local op_line
  local operation_id

  run_insert "arrow_flight_fdw_write" "af_bench_arrow_write" "${phase}" \
    "${run_id}" "${arrow_raw_bytes}" "estimated_arrow_array_buffers_no_ipc_or_grpc_framing"
  op_line="$(latest_arrow_operation)"
  IFS='|' read -r operation_id _rows _batches _elapsed_us <<<"${op_line}"
  validate_arrow_readback "${operation_id}"
}

run_gpfdist_write() {
  local phase="$1"
  local run_id="$2"
  local file_name="write_gpfdist_${phase}_${run_id}.csv"
  local stats
  local write_bytes

  rm -f "${DATA_DIR}/${file_name}"*
  create_gpfdist_target "${file_name}"
  run_insert "gpfdist_csv_write" "af_bench_gpfdist_write" "${phase}" \
    "${run_id}" "${csv_bytes}" "csv_file_bytes_estimated"
  stats="$(gpfdist_file_stats "${file_name}")"
  IFS='|' read -r write_bytes _rows _id_sum _segid_sum <<<"${stats}"
  python3 - "$RUNS_CSV" "gpfdist_csv_write" "${phase}" "${run_id}" \
    "${write_bytes}" "csv_file_bytes_actual" <<'PY'
import csv
import sys

path, method, phase, run_id, write_bytes, kind = sys.argv[1:7]
rows = []
with open(path, newline="", encoding="utf-8") as source:
    reader = csv.DictReader(source)
    fieldnames = reader.fieldnames
    for row in reader:
        if (row["method"], row["phase"], row["run"]) == (method, phase, run_id):
            row["write_bytes"] = write_bytes
            row["write_bytes_kind"] = kind
            row_count = int(row["row_count"])
            logical_bytes = int(row["logical_bytes"])
            cpu_sec = float(row["cpu_sec"])
            write_bytes_int = int(write_bytes)
            write_gb = write_bytes_int / (1024 ** 3)
            logical_gb = logical_bytes / (1024 ** 3)
            row["write_bytes_per_row"] = f"{write_bytes_int / row_count:.2f}"
            row["cpu_sec_per_gb"] = f"{cpu_sec / logical_gb:.2f}"
            row["cpu_sec_per_write_gb"] = f"{cpu_sec / write_gb:.2f}" if write_gb else "0.00"
        rows.append(row)

with open(path, "w", newline="", encoding="utf-8") as out:
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
PY
}

run_latency_explain() {
  local method="$1"
  local table_name="$2"
  local explain_file="${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/explain_${method}.txt"
  local file_name

  if [ "${method}" = "gpfdist_csv_write" ]; then
    file_name="write_gpfdist_explain_0.csv"
    rm -f "${DATA_DIR}/${file_name}"*
    create_gpfdist_target "${file_name}"
  fi

  psql -X -qAt -v ON_ERROR_STOP=1 postgres >"${explain_file}" <<SQL
SET optimizer=off;
SET gp_enable_explain_allstat=on;
SET TIME ZONE 'UTC';
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON, SUMMARY OFF)
INSERT INTO ${table_name}
SELECT * FROM af_bench_write_source;
SQL

  if [ "${method}" = "arrow_flight_fdw_write" ]; then
    local op_line
    local operation_id

    op_line="$(latest_arrow_operation)"
    IFS='|' read -r operation_id _rows _batches _elapsed_us <<<"${op_line}"
    validate_arrow_readback "${operation_id}"
  else
    gpfdist_file_stats "${file_name}" >/dev/null
  fi

  python3 - "$LATENCY_CSV" "$method" "$explain_file" \
    "$ARROWFLIGHT_WRITE_BENCH_ROWS_PER_SEGMENT" <<'PY'
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

for ((run_id = 1; run_id <= ARROWFLIGHT_WRITE_BENCH_WARMUPS; run_id++)); do
  run_arrow_write "warmup" "${run_id}"
done
for ((run_id = 1; run_id <= ARROWFLIGHT_WRITE_BENCH_REPEATS; run_id++)); do
  run_arrow_write "run" "${run_id}"
done
run_latency_explain "arrow_flight_fdw_write" "af_bench_arrow_write"

for ((run_id = 1; run_id <= ARROWFLIGHT_WRITE_BENCH_WARMUPS; run_id++)); do
  run_gpfdist_write "warmup" "${run_id}"
done
for ((run_id = 1; run_id <= ARROWFLIGHT_WRITE_BENCH_REPEATS; run_id++)); do
  run_gpfdist_write "run" "${run_id}"
done
run_latency_explain "gpfdist_csv_write" "af_bench_gpfdist_write"

python3 - "$SERVER_PROFILE_CSV" \
  "${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/arrowflightd.log" <<'PY'
import csv
import re
import sys

out_path, log_path = sys.argv[1:3]
columns = ["dataset", "operation_id", "segment", "rows", "batches",
           "fields", "elapsed_us"]
rows = []
text = open(log_path, encoding="utf-8", errors="replace").read()

for match in re.finditer(r"arrowflightd_write_profile\s+([^\n\r]+)", text):
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
  "$SERVER_PROFILE_CSV" "$SUMMARY_JSON" "$SUMMARY_MD" \
  "$ARROWFLIGHT_WRITE_BENCH_REPEATS" "$ARROWFLIGHT_WRITE_BENCH_WARMUPS" \
  "$ARROWFLIGHT_WRITE_BENCH_BATCH_ROWS" \
  "$ARROWFLIGHT_WRITE_BENCH_MAX_BATCH_BYTES" \
  "$ARROWFLIGHT_WRITE_BENCH_NET_DEV" <<'PY'
import csv
import json
import statistics
import sys

runs_csv, latency_csv, metadata_path, server_profile_csv, summary_json, \
    summary_md = sys.argv[1:7]
repeats, warmups, batch_rows, max_batch_bytes = map(int, sys.argv[7:11])
net_dev = sys.argv[11]
metadata = json.load(open(metadata_path, encoding="utf-8"))

with open(runs_csv, encoding="utf-8") as source:
    runs = list(csv.DictReader(source))

with open(latency_csv, encoding="utf-8") as source:
    latencies = {row["method"]: row for row in csv.DictReader(source)}

with open(server_profile_csv, encoding="utf-8") as source:
    server_profiles = list(csv.DictReader(source))

methods = []
for method in ["arrow_flight_fdw_write", "gpfdist_csv_write"]:
    method_runs = [row for row in runs
                   if row["method"] == method and row["phase"] == "run"]
    wall = [float(row["wall_sec"]) for row in method_runs]
    cpu = [float(row["cpu_sec"]) for row in method_runs]
    rows_per_sec = [float(row["rows_per_sec"]) for row in method_runs]
    cpu_per_gb = [float(row["cpu_sec_per_gb"]) for row in method_runs]
    cpu_per_write_gb = [float(row["cpu_sec_per_write_gb"])
                        for row in method_runs]
    net_bytes = [int(row.get("net_bytes") or 0) for row in method_runs]
    net_bytes_per_row = [float(row.get("net_bytes_per_row") or 0)
                         for row in method_runs]
    write_bytes = [int(row["write_bytes"]) for row in method_runs]
    latency = latencies.get(method, {})

    methods.append({
        "method": method,
        "runs": len(method_runs),
        "median_wall_sec": statistics.median(wall),
        "median_cpu_sec": statistics.median(cpu),
        "median_rows_per_sec": statistics.median(rows_per_sec),
        "median_cpu_sec_per_gb": statistics.median(cpu_per_gb),
        "median_cpu_sec_per_write_gb": statistics.median(cpu_per_write_gb),
        "median_write_bytes": statistics.median(write_bytes),
        "median_write_bytes_per_row": statistics.median(write_bytes) /
            metadata["rows"],
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
        "write_bytes_kind": method_runs[0]["write_bytes_kind"],
    })

server_totals = {
    "profile_rows": len(server_profiles),
    "rows": sum(int(float(row["rows"] or 0)) for row in server_profiles),
    "batches": sum(int(float(row["batches"] or 0)) for row in server_profiles),
    "elapsed_us": sum(int(float(row["elapsed_us"] or 0))
                      for row in server_profiles),
}

summary = {
    "config": {
        "repeats": repeats,
        "warmups": warmups,
        "bench_batch_rows": batch_rows,
        "max_batch_bytes": max_batch_bytes,
        "net_dev": net_dev,
        "optimizer": "off",
    },
    "dataset": metadata,
    "methods": methods,
    "server_profiles": server_totals,
    "runs": runs,
    "server_profile_rows": server_profiles,
}

with open(summary_json, "w", encoding="utf-8") as out:
    json.dump(summary, out, indent=2)
    out.write("\n")

with open(summary_md, "w", encoding="utf-8") as out:
    out.write("# Arrow Flight Write Benchmark Summary\n\n")
    out.write(
        f"Dataset: {metadata['rows']} rows, "
        f"{metadata['segments']} segments, "
        f"{metadata['rows_per_segment']} rows/segment, "
        f"schema={metadata.get('schema', 'mixed')}, "
        f"logical CSV bytes={metadata['logical_bytes']}, "
        f"estimated Arrow raw bytes={metadata['arrow_raw_bytes']}, "
        f"net_dev={net_dev or 'disabled'}, "
        "optimizer=off.\n\n"
    )
    out.write(
        "| method | rows/s | CPU sec/logical GB | CPU sec/write GB | write MB | write B/row | net MB | net B/row | wall sec | "
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
            f"{row['median_cpu_sec_per_write_gb']:.2f} | "
            f"{row['median_write_bytes'] / (1024 ** 2):.2f} | "
            f"{row['median_write_bytes_per_row']:.2f} | "
            f"{row['median_net_bytes'] / (1024 ** 2):.2f} | "
            f"{row['median_net_bytes_per_row']:.2f} | "
            f"{row['median_wall_sec']:.3f} | "
            f"{'' if p50 is None else f'{p50:.3f}'} | "
            f"{'' if p95 is None else f'{p95:.3f}'} |\n"
        )

    out.write("\nWrite byte notes:\n\n")
    out.write("- `arrow_flight_fdw_write` write bytes are estimated Arrow array buffers without IPC/gRPC framing.\n")
    out.write("- `gpfdist_csv_write` write bytes are actual CSV files emitted by writable gpfdist.\n")
    out.write("- Network bytes are rx+tx deltas from `/sys/class/net/$ARROWFLIGHT_WRITE_BENCH_NET_DEV/statistics/*_bytes`; they are `0` when the env var is empty.\n")

    if server_profiles:
        out.write("\nArrow Flight server profile:\n\n")
        out.write(
            f"- profile rows: {server_totals['profile_rows']}\n"
            f"- rows: {server_totals['rows']}\n"
            f"- batches: {server_totals['batches']}\n"
            f"- elapsed us: {server_totals['elapsed_us']}\n"
        )

print(open(summary_md, encoding="utf-8").read())
PY

psql -v ON_ERROR_STOP=1 postgres <<SQL
DROP FOREIGN TABLE IF EXISTS af_bench_arrow_write_readback;
DROP FOREIGN TABLE IF EXISTS af_bench_arrow_write;
DROP EXTERNAL TABLE IF EXISTS af_bench_gpfdist_write;
DROP TABLE IF EXISTS af_bench_write_source;
DROP SERVER IF EXISTS af_bench_arrow_write_srv CASCADE;
DROP EXTENSION IF EXISTS arrowflight CASCADE;
SQL

cat "${ARROWFLIGHT_WRITE_BENCH_RESULT_DIR}/arrowflightd.log"
