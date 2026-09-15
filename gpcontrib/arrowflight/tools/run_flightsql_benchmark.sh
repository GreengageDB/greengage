#!/usr/bin/env bash
set -euo pipefail

: "${ARROWFLIGHT_PERF_GG_BASE_IMAGE:=greengage7-u22-arm64-dev:latest}"
: "${ARROWFLIGHT_PERF_GG_IMAGE:=greengage-u22-arm64-arrowflight-benchmark:latest}"
: "${ARROWFLIGHT_PERF_GG_PLATFORM:=linux/arm64}"
: "${ARROWFLIGHT_PERF_FLIGHTSQL_SYNTHETIC_IMAGE:=greengage-u22-arm64-flightsql-synthetic-perf:latest}"
: "${ARROWFLIGHT_PERF_GPFDIST_IMAGE:=greengage7-u22-arm64-dev:latest}"
: "${ARROWFLIGHT_PERF_PROJECT:=flightsql-benchmark}"
: "${ARROWFLIGHT_PERF_SEGMENTS:=3}"
: "${ARROWFLIGHT_PERF_ROWS_PER_SEGMENT:=1000000}"
: "${ARROWFLIGHT_PERF_BATCH_ROWS:=8192}"
: "${ARROWFLIGHT_PERF_LABEL_WIDTH:=32}"
: "${ARROWFLIGHT_PERF_WARMUPS:=1}"
: "${ARROWFLIGHT_PERF_REPEATS:=3}"
: "${ARROWFLIGHT_PERF_SAMPLE_INTERVAL:=0.2}"
: "${ARROWFLIGHT_PERF_ROOT:=/tmp/flightsql_benchmark}"
: "${ARROWFLIGHT_PERF_KEEP_COMPOSE:=0}"

if [ "${ARROWFLIGHT_PERF_SEGMENTS}" -ne 3 ]; then
  echo "the benchmark currently requires exactly 3 Greengage segments" >&2
  exit 1
fi
if [ -z "${ARROWFLIGHT_PERF_ROOT}" ] ||
   [ "${ARROWFLIGHT_PERF_ROOT}" = "/" ]; then
  echo "refusing unsafe ARROWFLIGHT_PERF_ROOT=${ARROWFLIGHT_PERF_ROOT}" >&2
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
arrowflight_dir="$(cd "${script_dir}/.." && pwd)"
compose_file="${arrowflight_dir}/docker-compose.benchmark.yml"
data_dir="${ARROWFLIGHT_PERF_ROOT}/data"
result_dir="${ARROWFLIGHT_PERF_ROOT}/results"
raw_dir="${result_dir}/raw"
sql_dir="${ARROWFLIGHT_PERF_ROOT}/sql"
manifest="${result_dir}/manifest.csv"
config_json="${result_dir}/config.json"

export ARROWFLIGHT_PERF_GG_IMAGE
export ARROWFLIGHT_PERF_GG_BASE_IMAGE
export ARROWFLIGHT_PERF_GG_PLATFORM
export ARROWFLIGHT_PERF_FLIGHTSQL_SYNTHETIC_IMAGE
export ARROWFLIGHT_PERF_GPFDIST_IMAGE
export ARROWFLIGHT_PERF_PROJECT
export ARROWFLIGHT_PERF_SEGMENTS
export ARROWFLIGHT_PERF_ROWS_PER_SEGMENT
export ARROWFLIGHT_PERF_BATCH_ROWS
export ARROWFLIGHT_PERF_LABEL_WIDTH
export ARROWFLIGHT_PERF_ROOT
export ARROWFLIGHT_PERF_DATA_DIR="${data_dir}"

compose() {
  docker compose -p "${ARROWFLIGHT_PERF_PROJECT}" -f "${compose_file}" "$@"
}

cleanup() {
  if [ "${ARROWFLIGHT_PERF_KEEP_COMPOSE}" != "1" ]; then
    compose down -v
  fi
}
trap cleanup EXIT

wait_healthy() {
  local container="$1"
  local deadline=$((SECONDS + 300))
  local status

  while [ "${SECONDS}" -lt "${deadline}" ]; do
    status="$(
      docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "${container}"
    )"
    if [ "${status}" = "healthy" ] || [ "${status}" = "running" ]; then
      return
    fi
    if [ "${status}" = "unhealthy" ] || [ "${status}" = "exited" ]; then
      docker logs "${container}" >&2 || true
      echo "container ${container} entered ${status}" >&2
      exit 1
    fi
    sleep 1
  done

  docker logs "${container}" >&2 || true
  echo "timed out waiting for container ${container}" >&2
  exit 1
}

rm -rf "${ARROWFLIGHT_PERF_ROOT}"
mkdir -p "${data_dir}" "${raw_dir}" "${sql_dir}"

python3 "${script_dir}/arrowflight_benchmark_data.py" \
  --output-dir "${data_dir}" \
  --segments "${ARROWFLIGHT_PERF_SEGMENTS}" \
  --rows-per-segment "${ARROWFLIGHT_PERF_ROWS_PER_SEGMENT}" \
  --batch-rows "${ARROWFLIGHT_PERF_BATCH_ROWS}" \
  --label-width "${ARROWFLIGHT_PERF_LABEL_WIDTH}" \
  --schema mixed \
  >"${result_dir}/data_generation.json"

compose build greengage
compose build flightsql_synthetic
compose up -d

greengage_id="$(compose ps -q greengage)"
flightsql_synthetic_id="$(compose ps -q flightsql_synthetic)"
flightsql_mpp_control_id="$(compose ps -q flightsql_mpp_control)"
flightsql_mpp_worker_0_id="$(compose ps -q flightsql_mpp_worker_0)"
flightsql_mpp_worker_1_id="$(compose ps -q flightsql_mpp_worker_1)"
flightsql_mpp_worker_2_id="$(compose ps -q flightsql_mpp_worker_2)"
gpfdist_id="$(compose ps -q gpfdist)"
clickhouse1_id="$(compose ps -q clickhouse1)"
clickhouse2_id="$(compose ps -q clickhouse2)"

for container in \
  "${greengage_id}" \
  "${flightsql_synthetic_id}" \
  "${flightsql_mpp_control_id}" \
  "${flightsql_mpp_worker_0_id}" \
  "${flightsql_mpp_worker_1_id}" \
  "${flightsql_mpp_worker_2_id}" \
  "${gpfdist_id}" \
  "${clickhouse1_id}" \
  "${clickhouse2_id}"; do
  wait_healthy "${container}"
  machine="$(docker exec "${container}" uname -m)"
  if [ "${machine}" != "aarch64" ] && [ "${machine}" != "arm64" ]; then
    echo "container ${container} is ${machine}, expected ARM64" >&2
    exit 1
  fi
done

docker exec "${greengage_id}" bash -lc "
set -euo pipefail
rm -rf /home/gpadmin/gpdb_src/gpcontrib/arrowflight
mkdir -p /home/gpadmin/gpdb_src/gpcontrib/arrowflight
cp -a /workspace/gpcontrib/arrowflight/. \
  /home/gpadmin/gpdb_src/gpcontrib/arrowflight/
"

if docker exec "${greengage_id}" \
     test -f /home/gpadmin/greengage-db-devel/greengage_path.sh; then
  docker exec "${greengage_id}" bash -lc "
  set -euo pipefail
  /usr/sbin/sshd || true
  chown -R gpadmin:gpadmin /home/gpadmin/gpdb_src/gpcontrib/arrowflight
  runuser -u gpadmin -- bash -lc '
  set -euo pipefail
  source /home/gpadmin/greengage-db-devel/greengage_path.sh
  cd /home/gpadmin/gpdb_src/gpAux/gpdemo
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=${ARROWFLIGHT_PERF_SEGMENTS} \
    make destroy-demo-cluster >/tmp/arrowflight_perf_destroy.log 2>&1 || true
  WITH_MIRRORS=false NUM_PRIMARY_MIRROR_PAIRS=${ARROWFLIGHT_PERF_SEGMENTS} \
    make create-demo-cluster >/tmp/arrowflight_perf_create.log 2>&1
  source gpdemo-env.sh
  cd /home/gpadmin/gpdb_src/gpcontrib/arrowflight
  make clean USE_ARROW_FLIGHT=1 \
    >/tmp/arrowflight_perf_make_clean.log 2>&1 || true
  make USE_ARROW_FLIGHT=1
  make install USE_ARROW_FLIGHT=1
  '
  "
  gg_gphome="/home/gpadmin/greengage-db-devel"
else
  docker exec \
    -e TEST_OS=ubuntu \
    -e MAKE_TEST_COMMAND="-C gpcontrib/arrowflight clean USE_ARROW_FLIGHT=1 && make -C gpcontrib/arrowflight install USE_ARROW_FLIGHT=1" \
    "${greengage_id}" \
    /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb.bash
  gg_gphome="/usr/local/greengage-db-devel"
fi

gg_psql_file() {
  local file="$1"

  docker exec "${greengage_id}" \
    runuser -u gpadmin -- bash -lc "
      source ${gg_gphome}/greengage_path.sh
      source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
      psql -X -qAt -F '|' -v ON_ERROR_STOP=1 postgres -f '${file}'
    "
}

clickhouse_query() {
  local container="$1"
  shift

  docker exec "${container}" clickhouse-client \
    --multiquery \
    --receive_timeout=600 \
    --send_timeout=600 \
    --query "$*"
}

total_rows=$((ARROWFLIGHT_PERF_SEGMENTS * ARROWFLIGHT_PERF_ROWS_PER_SEGMENT))
node1_rows=$((total_rows / 2))
node2_rows=$((total_rows - node1_rows))
node2_start=$((node1_rows + 1))

clickhouse_local_ddl="
CREATE TABLE default.af_perf_read_local
(
    id Int32,
    segid Int32,
    label String,
    active Bool,
    amount Float64,
    d Date,
    ts DateTime64(6)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE default.af_perf_write_local
AS default.af_perf_read_local
ENGINE = MergeTree
ORDER BY id;
"

clickhouse_query "${clickhouse1_id}" "
DROP VIEW IF EXISTS default.af_perf_read_flightsql;
DROP TABLE IF EXISTS default.af_perf_read;
DROP TABLE IF EXISTS default.af_perf_write;
DROP TABLE IF EXISTS default.af_perf_read_local;
DROP TABLE IF EXISTS default.af_perf_write_local;
${clickhouse_local_ddl}
"
clickhouse_query "${clickhouse2_id}" "
DROP TABLE IF EXISTS default.af_perf_read_local;
DROP TABLE IF EXISTS default.af_perf_write_local;
${clickhouse_local_ddl}
"

clickhouse_insert() {
  local container="$1"
  local start_id="$2"
  local count="$3"

  clickhouse_query "${container}" "
INSERT INTO default.af_perf_read_local
WITH
    toInt64(number + ${start_id}) AS row_id,
    intDiv(row_id - 1, ${ARROWFLIGHT_PERF_ROWS_PER_SEGMENT}) AS segment_id,
    modulo(row_id - 1, ${ARROWFLIGHT_PERF_ROWS_PER_SEGMENT}) + 1 AS segment_row
SELECT
    toInt32(row_id),
    toInt32(segment_id),
    rightPad(
        concat(
            'bench-seg-',
            toString(segment_id),
            '-row-',
            toString(segment_row)),
        ${ARROWFLIGHT_PERF_LABEL_WIDTH},
        'x'),
    segment_row % 2 = 1,
    toFloat64(segment_row) + 0.5,
    toDate('2000-01-01') + toInt32(segment_row % 365 + 1),
    toDateTime64('2000-01-01 00:00:00', 6) + segment_row
FROM numbers(${count});
"
}

clickhouse_insert "${clickhouse1_id}" 1 "${node1_rows}"
clickhouse_insert "${clickhouse2_id}" "${node2_start}" "${node2_rows}"

clickhouse_query "${clickhouse1_id}" "
CREATE TABLE default.af_perf_read
AS default.af_perf_read_local
ENGINE = Distributed(
    flightsql_test_cluster,
    default,
    af_perf_read_local,
    cityHash64(id));

CREATE TABLE default.af_perf_write
AS default.af_perf_write_local
ENGINE = Distributed(
    flightsql_test_cluster,
    default,
    af_perf_write_local,
    cityHash64(id));

CREATE VIEW default.af_perf_read_flightsql
AS
SELECT
    id,
    materialize(segid) AS segid,
    label,
    active,
    amount,
    d,
    ts
FROM default.af_perf_read;

"

gpfdist_locations=""
for ((segment = 0; segment < ARROWFLIGHT_PERF_SEGMENTS; segment++)); do
  if [ -n "${gpfdist_locations}" ]; then
    gpfdist_locations+=", "
  fi
  gpfdist_locations+="'gpfdist://gpfdist:8081/bench_${segment}.csv'"
done

cat >"${sql_dir}/setup.sql" <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;
SET optimizer = off;
SET TIME ZONE 'UTC';

DROP FOREIGN TABLE IF EXISTS af_perf_flightsql_read;
DROP FOREIGN TABLE IF EXISTS af_perf_flightsql_write;
DROP FOREIGN TABLE IF EXISTS af_perf_flightsql_synthetic_read;
DROP FOREIGN TABLE IF EXISTS af_perf_flightsql_synthetic_write;
DROP FOREIGN TABLE IF EXISTS af_perf_flightsql_synthetic_planned_write;
DROP EXTERNAL TABLE IF EXISTS af_perf_gpfdist_read;
DROP EXTERNAL TABLE IF EXISTS af_perf_gpfdist_write;
DROP TABLE IF EXISTS af_perf_source;
DROP SERVER IF EXISTS af_perf_flightsql_server CASCADE;
DROP SERVER IF EXISTS af_perf_flightsql_synthetic_server CASCADE;
DROP SERVER IF EXISTS af_perf_flightsql_synthetic_planned_server CASCADE;

CREATE SERVER af_perf_flightsql_server
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    mpp_execute 'all segments',
    host 'clickhouse1',
    port '9005',
    timeout_ms '60000',
    max_endpoints '1000',
    max_plan_bytes '16777216',
    batch_rows '${ARROWFLIGHT_PERF_BATCH_ROWS}',
    max_batch_bytes '4194304',
    ingest_row_count_check 'off'
);

CREATE FOREIGN TABLE af_perf_flightsql_read
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER af_perf_flightsql_server
OPTIONS (
    schema_name 'default',
    table_name 'af_perf_read_flightsql',
    rows '${total_rows}'
);

CREATE FOREIGN TABLE af_perf_flightsql_write
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER af_perf_flightsql_server
OPTIONS (
    schema_name 'default',
    table_name 'af_perf_write',
    rows '${total_rows}'
);

CREATE SERVER af_perf_flightsql_synthetic_server
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    mpp_execute 'all segments',
    host 'flightsql-synthetic',
    port '9015',
    timeout_ms '60000',
    max_endpoints '1000',
    max_plan_bytes '16777216',
    batch_rows '${ARROWFLIGHT_PERF_BATCH_ROWS}',
    max_batch_bytes '4194304',
    ingest_row_count_check 'off'
);

CREATE FOREIGN TABLE af_perf_flightsql_synthetic_read
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER af_perf_flightsql_synthetic_server
OPTIONS (
    schema_name 'default',
    table_name 'af_perf_read',
    rows '${total_rows}'
);

CREATE FOREIGN TABLE af_perf_flightsql_synthetic_write
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER af_perf_flightsql_synthetic_server
OPTIONS (
    schema_name 'default',
    table_name 'af_perf_write',
    rows '${total_rows}'
);

CREATE SERVER af_perf_flightsql_synthetic_planned_server
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    mpp_execute 'all segments',
    host 'flightsql-mpp-control',
    port '9020',
    timeout_ms '60000',
    max_endpoints '1000',
    max_plan_bytes '16777216',
    batch_rows '${ARROWFLIGHT_PERF_BATCH_ROWS}',
    max_batch_bytes '4194304',
    ingest_row_count_check 'exact',
    write_routing_mode 'planned',
    write_transaction_mode 'auto_commit'
);

CREATE FOREIGN TABLE af_perf_flightsql_synthetic_planned_write
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
SERVER af_perf_flightsql_synthetic_planned_server
OPTIONS (
    schema_name 'default',
    table_name 'af_perf_write',
    rows '${total_rows}'
);

CREATE READABLE EXTERNAL TABLE af_perf_gpfdist_read
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
LOCATION (${gpfdist_locations})
FORMAT 'CSV';

CREATE TABLE af_perf_source
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
DISTRIBUTED BY (id);

INSERT INTO af_perf_source
SELECT
    gp_segment_id * ${ARROWFLIGHT_PERF_ROWS_PER_SEGMENT} + n,
    gp_segment_id,
    rpad(
        'bench-seg-' || gp_segment_id::text || '-row-' || n::text,
        ${ARROWFLIGHT_PERF_LABEL_WIDTH},
        'x'),
    n % 2 = 1,
    n::float8 + 0.5,
    DATE '2000-01-01' + (n % 365 + 1),
    TIMESTAMP '2000-01-01 00:00:00' + n * INTERVAL '1 second'
FROM gp_dist_random('gp_id'),
     generate_series(1, ${ARROWFLIGHT_PERF_ROWS_PER_SEGMENT}) AS generated(n);

ANALYZE af_perf_source;
SQL

cat >"${sql_dir}/checksum.sql.in" <<'SQL'
SELECT
    count(*)::bigint,
    coalesce(sum(id), 0)::bigint,
    coalesce(sum(segid), 0)::bigint,
    coalesce(sum(length(label)), 0)::bigint,
    count(*) FILTER (WHERE active),
    min(amount),
    max(amount),
    min(d),
    max(d),
    min(ts),
    max(ts)
FROM TABLE_NAME;
SQL

make_checksum_sql() {
  local table="$1"
  local output="$2"

  sed "s/TABLE_NAME/${table}/g" \
    "${sql_dir}/checksum.sql.in" >"${output}"
}

make_checksum_sql af_perf_source "${sql_dir}/checksum_source.sql"
make_checksum_sql af_perf_gpfdist_read "${sql_dir}/read_gpfdist.sql"
make_checksum_sql af_perf_flightsql_read \
  "${sql_dir}/read_flightsql_clickhouse.sql"
make_checksum_sql af_perf_flightsql_synthetic_read \
  "${sql_dir}/read_flightsql_synthetic.sql"

cat >"${sql_dir}/write_gpfdist.sql" <<'SQL'
INSERT INTO af_perf_gpfdist_write
SELECT * FROM af_perf_source;
SQL
cat >"${sql_dir}/write_flightsql_clickhouse.sql" <<'SQL'
INSERT INTO af_perf_flightsql_write
SELECT * FROM af_perf_source;
SQL
cat >"${sql_dir}/write_flightsql_synthetic.sql" <<'SQL'
INSERT INTO af_perf_flightsql_synthetic_write
SELECT * FROM af_perf_source;
SQL
cat >"${sql_dir}/write_flightsql_synthetic_planned.sql" <<'SQL'
INSERT INTO af_perf_flightsql_synthetic_planned_write
SELECT * FROM af_perf_source;
SQL
make_checksum_sql af_perf_flightsql_synthetic_write \
  "${sql_dir}/validate_flightsql_synthetic.sql"

gg_psql_file /benchmark/sql/setup.sql >/dev/null
expected_checksum="$(gg_psql_file /benchmark/sql/checksum_source.sql | tr -d '\r')"
expected_clickhouse_checksum="$(
  clickhouse_query "${clickhouse1_id}" "
SELECT
    count(),
    sum(id),
    sum(segid),
    sum(length(label)),
    countIf(active),
    min(amount),
    max(amount),
    min(d),
    max(d),
    min(ts),
    max(ts)
FROM default.af_perf_read
FORMAT TabSeparated
" | tr -d '\r'
)"

printf "workload,method,phase,run,metrics_file\n" >"${manifest}"

run_probe() {
  local workload="$1"
  local method="$2"
  local phase="$3"
  local run_id="$4"
  local sql_file="$5"
  local metrics_file="${raw_dir}/${workload}_${method}_${phase}_${run_id}.json"
  local -a containers

  containers=(
    --container "greengage=${greengage_id}"
  )
  case "${method}" in
    gpfdist)
      containers+=(--container "gpfdist=${gpfdist_id}")
      ;;
    flightsql_clickhouse)
      containers+=(
        --container "clickhouse1=${clickhouse1_id}"
        --container "clickhouse2=${clickhouse2_id}"
      )
      ;;
    flightsql_synthetic)
      containers+=(
        --container "flightsql_synthetic=${flightsql_synthetic_id}"
      )
      ;;
    flightsql_synthetic_planned)
      containers+=(
        --container "flightsql_mpp_control=${flightsql_mpp_control_id}"
        --container "flightsql_mpp_worker_0=${flightsql_mpp_worker_0_id}"
        --container "flightsql_mpp_worker_1=${flightsql_mpp_worker_1_id}"
        --container "flightsql_mpp_worker_2=${flightsql_mpp_worker_2_id}"
      )
      ;;
    *)
      echo "unknown benchmark method ${method}" >&2
      exit 1
      ;;
  esac

  python3 "${script_dir}/arrowflight_resource_probe.py" \
    "${containers[@]}" \
    --output "${metrics_file}" \
    --interval "${ARROWFLIGHT_PERF_SAMPLE_INTERVAL}" \
    -- \
    docker exec "${greengage_id}" \
      runuser -u gpadmin -- bash -lc "
        source ${gg_gphome}/greengage_path.sh
        source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh
        psql -X -qAt -F '|' -v ON_ERROR_STOP=1 postgres \
          -f '/benchmark/sql/${sql_file}'
      "

  printf "%s,%s,%s,%s,%s\n" \
    "${workload}" "${method}" "${phase}" "${run_id}" "${metrics_file}" \
    >>"${manifest}"
}

probe_stdout() {
  python3 - "$1" <<'PY'
import json
import sys

metrics = json.load(open(sys.argv[1], encoding="utf-8"))
print(metrics["command"]["stdout"].strip())
PY
}

validate_read_run() {
  local method="$1"
  local phase="$2"
  local run_id="$3"
  local metrics_file="${raw_dir}/read_${method}_${phase}_${run_id}.json"
  local actual

  actual="$(probe_stdout "${metrics_file}")"
  if [ "${actual}" != "${expected_checksum}" ]; then
    echo "${method} read validation failed: ${actual}" >&2
    echo "expected: ${expected_checksum}" >&2
    exit 1
  fi
}

run_read() {
  local method="$1"
  local phase="$2"
  local run_id="$3"

  run_probe read "${method}" "${phase}" "${run_id}" "read_${method}.sql"
  validate_read_run "${method}" "${phase}" "${run_id}"
}

for method in gpfdist flightsql_clickhouse flightsql_synthetic; do
  for ((run_id = 1; run_id <= ARROWFLIGHT_PERF_WARMUPS; run_id++)); do
    run_read "${method}" warmup "${run_id}"
  done
done

read_orders=(
  "gpfdist flightsql_clickhouse flightsql_synthetic"
  "flightsql_synthetic gpfdist flightsql_clickhouse"
  "flightsql_clickhouse flightsql_synthetic gpfdist"
)
for ((run_id = 1; run_id <= ARROWFLIGHT_PERF_REPEATS; run_id++)); do
  order="${read_orders[$(((run_id - 1) % ${#read_orders[@]}))]}"
  for method in ${order}; do
    run_read "${method}" run "${run_id}"
  done
done

docker restart "${clickhouse1_id}" "${clickhouse2_id}" >/dev/null
wait_healthy "${clickhouse1_id}"
wait_healthy "${clickhouse2_id}"

prepare_gpfdist_write() {
  local phase="$1"
  local run_id="$2"
  local file_name="write_gpfdist_${phase}_${run_id}.csv"

  rm -f "${data_dir}/${file_name}"*
  cat >"${sql_dir}/prepare_gpfdist_write.sql" <<SQL
DROP EXTERNAL TABLE IF EXISTS af_perf_gpfdist_write;
CREATE WRITABLE EXTERNAL TABLE af_perf_gpfdist_write
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
LOCATION ('gpfdist://gpfdist:8081/${file_name}')
FORMAT 'CSV';
SQL
  gg_psql_file /benchmark/sql/prepare_gpfdist_write.sql >/dev/null
}

validate_gpfdist_write() {
  local phase="$1"
  local run_id="$2"
  local prefix="write_gpfdist_${phase}_${run_id}.csv"
  local actual

  cat >"${sql_dir}/validate_gpfdist_write.sql" <<SQL
DROP EXTERNAL TABLE IF EXISTS af_perf_gpfdist_readback;
CREATE READABLE EXTERNAL TABLE af_perf_gpfdist_readback
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamptz
)
LOCATION ('gpfdist://gpfdist:8081/${prefix}')
FORMAT 'CSV';
SQL
  make_checksum_sql af_perf_gpfdist_readback \
    "${sql_dir}/validate_gpfdist_checksum.sql"
  gg_psql_file /benchmark/sql/validate_gpfdist_write.sql >/dev/null
  actual="$(
    gg_psql_file /benchmark/sql/validate_gpfdist_checksum.sql | tr -d '\r'
  )"
  if [ "${actual}" != "${expected_checksum}" ]; then
    echo "gpfdist write validation failed: ${actual}" >&2
    exit 1
  fi
}

prepare_flightsql_write() {
  clickhouse_query "${clickhouse1_id}" \
    "TRUNCATE TABLE default.af_perf_write_local;"
  clickhouse_query "${clickhouse2_id}" \
    "TRUNCATE TABLE default.af_perf_write_local;"
}

validate_flightsql_write() {
  local node1_count
  local node2_count
  local actual

  clickhouse_query "${clickhouse1_id}" \
    "SYSTEM FLUSH DISTRIBUTED default.af_perf_write;"
  node1_count="$(
    clickhouse_query "${clickhouse1_id}" \
      "SELECT count() FROM default.af_perf_write_local" |
      tr -d '[:space:]'
  )"
  node2_count="$(
    clickhouse_query "${clickhouse2_id}" \
      "SELECT count() FROM default.af_perf_write_local" |
      tr -d '[:space:]'
  )"
  if [ "${node1_count}" -le 0 ] || [ "${node2_count}" -le 0 ] ||
     [ "$((node1_count + node2_count))" -ne "${total_rows}" ]; then
    echo "Flight SQL write distribution is ${node1_count},${node2_count}" >&2
    exit 1
  fi

  actual="$(
    clickhouse_query "${clickhouse1_id}" "
SELECT
    count(),
    sum(id),
    sum(segid),
    sum(length(label)),
    countIf(active),
    min(amount),
    max(amount),
    min(d),
    max(d),
    min(ts),
    max(ts)
FROM default.af_perf_write
FORMAT TabSeparated
" | tr -d '\r'
  )"
  if [ "${actual}" != "${expected_clickhouse_checksum}" ]; then
    echo "Flight SQL write validation failed: ${actual}" >&2
    echo "expected: ${expected_clickhouse_checksum}" >&2
    exit 1
  fi
}

prepare_flightsql_synthetic_write() {
  docker restart "${flightsql_synthetic_id}" >/dev/null
  wait_healthy "${flightsql_synthetic_id}"
}

validate_flightsql_synthetic_write() {
  local actual

  actual="$(
    gg_psql_file /benchmark/sql/validate_flightsql_synthetic.sql |
      tr -d '\r'
  )"
  if [ "${actual}" != "${expected_checksum}" ]; then
    echo "synthetic Flight SQL write validation failed: ${actual}" >&2
    echo "expected: ${expected_checksum}" >&2
    exit 1
  fi
}

prepare_flightsql_synthetic_planned_write() {
  compose exec -T flightsql_mpp_control bash -lc "
    set -euo pipefail
    rm -rf /var/lib/flightsql-mpp/plans \
      /var/lib/flightsql-mpp/transactions
    mkdir -p /var/lib/flightsql-mpp/plans \
      /var/lib/flightsql-mpp/transactions
  "
}

validate_flightsql_synthetic_planned_write() {
  local actual

  actual="$(
    compose exec -T flightsql_mpp_control bash -lc "
      set -euo pipefail
      shopt -s nullglob
      plans=(/var/lib/flightsql-mpp/plans/*)
      test \"\${#plans[@]}\" -eq 1
      plan=\"\${plans[0]}\"
      test \"\$(cat \"\${plan}/status\")\" = completed
      routes=(\"\${plan}\"/route_*.done)
      test \"\${#routes[@]}\" -eq ${ARROWFLIGHT_PERF_SEGMENTS}
      for segment in 0 1 2; do
        grep \" worker-\${segment}\$\" \
          \"\${plan}/route_\${segment}.done\" >/dev/null
      done
      awk '{ rows += \$1 } END { print rows + 0 }' \"\${routes[@]}\"
    " | tr -d '[:space:]'
  )"
  if [ "${actual}" != "${total_rows}" ]; then
    echo "planned Flight SQL write validation failed: ${actual}" >&2
    echo "expected rows: ${total_rows}" >&2
    exit 1
  fi
}

run_write() {
  local method="$1"
  local phase="$2"
  local run_id="$3"

  case "${method}" in
    gpfdist)
      prepare_gpfdist_write "${phase}" "${run_id}"
      ;;
    flightsql_clickhouse)
      prepare_flightsql_write
      ;;
    flightsql_synthetic)
      prepare_flightsql_synthetic_write
      ;;
    flightsql_synthetic_planned)
      prepare_flightsql_synthetic_planned_write
      ;;
  esac

  run_probe write "${method}" "${phase}" "${run_id}" \
    "write_${method}.sql"

  case "${method}" in
    gpfdist)
      validate_gpfdist_write "${phase}" "${run_id}"
      ;;
    flightsql_clickhouse)
      validate_flightsql_write
      ;;
    flightsql_synthetic)
      validate_flightsql_synthetic_write
      ;;
    flightsql_synthetic_planned)
      validate_flightsql_synthetic_planned_write
      ;;
  esac
}

for method in \
  gpfdist \
  flightsql_clickhouse \
  flightsql_synthetic \
  flightsql_synthetic_planned; do
  for ((run_id = 1; run_id <= ARROWFLIGHT_PERF_WARMUPS; run_id++)); do
    run_write "${method}" warmup "${run_id}"
  done
done

write_orders=(
  "flightsql_clickhouse gpfdist flightsql_synthetic flightsql_synthetic_planned"
  "gpfdist flightsql_synthetic_planned flightsql_clickhouse flightsql_synthetic"
  "flightsql_synthetic flightsql_clickhouse flightsql_synthetic_planned gpfdist"
  "flightsql_synthetic_planned flightsql_synthetic gpfdist flightsql_clickhouse"
)
for ((run_id = 1; run_id <= ARROWFLIGHT_PERF_REPEATS; run_id++)); do
  order="${write_orders[$(((run_id - 1) % ${#write_orders[@]}))]}"
  for method in ${order}; do
    run_write "${method}" run "${run_id}"
  done
done

if docker logs "${flightsql_mpp_control_id}" 2>&1 |
    grep "flightsql_benchmark_write" >/dev/null; then
  echo "planned Flight SQL control process received Arrow payload" >&2
  exit 1
fi

python3 - "${config_json}" <<PY
import json
import platform
import subprocess
import sys

config = {
    "greengage_base_image": "${ARROWFLIGHT_PERF_GG_BASE_IMAGE}",
    "greengage_image": "${ARROWFLIGHT_PERF_GG_IMAGE}",
    "greengage_image_architecture": subprocess.check_output(
        [
            "docker",
            "image",
            "inspect",
            "${ARROWFLIGHT_PERF_GG_IMAGE}",
            "--format",
            "{{.Architecture}}",
        ],
        text=True,
    ).strip(),
    "flightsql_synthetic_image": "${ARROWFLIGHT_PERF_FLIGHTSQL_SYNTHETIC_IMAGE}",
    "flightsql_synthetic_image_architecture": subprocess.check_output(
        [
            "docker",
            "image",
            "inspect",
            "${ARROWFLIGHT_PERF_FLIGHTSQL_SYNTHETIC_IMAGE}",
            "--format",
            "{{.Architecture}}",
        ],
        text=True,
    ).strip(),
    "gpfdist_image": "${ARROWFLIGHT_PERF_GPFDIST_IMAGE}",
    "gpfdist_image_architecture": subprocess.check_output(
        [
            "docker",
            "image",
            "inspect",
            "${ARROWFLIGHT_PERF_GPFDIST_IMAGE}",
            "--format",
            "{{.Architecture}}",
        ],
        text=True,
    ).strip(),
    "clickhouse_image_architecture": subprocess.check_output(
        [
            "docker",
            "image",
            "inspect",
            "clickhouse/clickhouse-server:26.4.4.38-alpine",
            "--format",
            "{{.Architecture}}",
        ],
        text=True,
    ).strip(),
    "host_machine": platform.machine(),
    "segments": ${ARROWFLIGHT_PERF_SEGMENTS},
    "clickhouse_nodes": 2,
    "planned_control_nodes": 1,
    "planned_worker_nodes": 3,
    "warmups": ${ARROWFLIGHT_PERF_WARMUPS},
    "repeats": ${ARROWFLIGHT_PERF_REPEATS},
    "batch_rows": ${ARROWFLIGHT_PERF_BATCH_ROWS},
    "sample_interval_sec": ${ARROWFLIGHT_PERF_SAMPLE_INTERVAL},
    "origin_remote_cpu_limit": 4,
    "origin_remote_memory_limit_gib": 6,
    "planned_remote_cpu_limit_total": 4,
    "planned_remote_memory_limit_gib_total": 6,
    "greengage_cpu_limit": 8,
    "optimizer": "off",
    "cache_policy": "one warmup followed by interleaved hot-cache runs",
    "workload_phase_isolation": (
        "ClickHouse is restarted between read and write phases"
    ),
    "clickhouse_cancel_ticket_after_do_get": True,
    "clickhouse_materialized_columns": ["segid"],
}
with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(config, output, indent=2)
    output.write("\n")
PY

python3 "${script_dir}/flightsql_benchmark_report.py" \
  --manifest "${manifest}" \
  --metadata "${data_dir}/metadata.json" \
  --config "${config_json}" \
  --output-json "${result_dir}/summary.json" \
  --output-markdown "${result_dir}/summary.md"

echo "flightsql_benchmark=ok"
echo "flightsql_benchmark_results=${result_dir}"
