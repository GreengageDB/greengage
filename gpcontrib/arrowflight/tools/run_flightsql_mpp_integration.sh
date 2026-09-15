#!/usr/bin/env bash
set -euo pipefail

: "${FLIGHTSQL_SYNTHETIC_HOST:=flightsql-synthetic}"
: "${FLIGHTSQL_SYNTHETIC_PORT:=9015}"
: "${FLIGHTSQL_MPP_CONTROL_HOST:=flightsql-mpp-control}"
: "${FLIGHTSQL_MPP_CONTROL_PORT:=9020}"
: "${FLIGHTSQL_MPP_FAIL_CONTROL_HOST:=flightsql-mpp-fail-control}"
: "${FLIGHTSQL_MPP_FAIL_CONTROL_PORT:=9025}"
: "${FLIGHTSQL_MPP_NO_CLUSTER_CONTROL_HOST:=flightsql-mpp-no-cluster-control}"
: "${FLIGHTSQL_MPP_NO_CLUSTER_CONTROL_PORT:=9026}"

suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
source_table="flightsql_mpp_source_${suffix}"
origin_server="flightsql_mpp_origin_${suffix}"
auto_server="flightsql_mpp_auto_${suffix}"
required_server="flightsql_mpp_required_${suffix}"
fail_server="flightsql_mpp_fail_${suffix}"
no_cluster_server="flightsql_mpp_no_cluster_${suffix}"
origin_table="flightsql_mpp_origin_write_${suffix}"
auto_table="flightsql_mpp_auto_write_${suffix}"
required_table="flightsql_mpp_required_write_${suffix}"
fail_table="flightsql_mpp_fail_write_${suffix}"
no_cluster_table="flightsql_mpp_no_cluster_write_${suffix}"

cleanup() {
  psql -X -q -v ON_ERROR_STOP=0 postgres >/dev/null 2>&1 <<SQL || true
DROP FOREIGN TABLE IF EXISTS ${origin_table};
DROP FOREIGN TABLE IF EXISTS ${auto_table};
DROP FOREIGN TABLE IF EXISTS ${required_table};
DROP FOREIGN TABLE IF EXISTS ${fail_table};
DROP FOREIGN TABLE IF EXISTS ${no_cluster_table};
DROP TABLE IF EXISTS ${source_table};
DROP SERVER IF EXISTS ${origin_server} CASCADE;
DROP SERVER IF EXISTS ${auto_server} CASCADE;
DROP SERVER IF EXISTS ${required_server} CASCADE;
DROP SERVER IF EXISTS ${fail_server} CASCADE;
DROP SERVER IF EXISTS ${no_cluster_server} CASCADE;
SQL
}
trap cleanup EXIT

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE EXTENSION IF NOT EXISTS arrowflight;

CREATE SERVER ${origin_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SYNTHETIC_HOST}',
    port '${FLIGHTSQL_SYNTHETIC_PORT}',
    write_routing_mode 'planned'
);

CREATE SERVER ${auto_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_MPP_CONTROL_HOST}',
    port '${FLIGHTSQL_MPP_CONTROL_PORT}',
    write_routing_mode 'planned',
    write_transaction_mode 'auto_commit'
);

CREATE SERVER ${required_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_MPP_CONTROL_HOST}',
    port '${FLIGHTSQL_MPP_CONTROL_PORT}',
    write_routing_mode 'planned',
    write_transaction_mode 'required'
);

CREATE SERVER ${fail_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_MPP_FAIL_CONTROL_HOST}',
    port '${FLIGHTSQL_MPP_FAIL_CONTROL_PORT}',
    write_routing_mode 'planned',
    write_transaction_mode 'required'
);

CREATE SERVER ${no_cluster_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_MPP_NO_CLUSTER_CONTROL_HOST}',
    port '${FLIGHTSQL_MPP_NO_CLUSTER_CONTROL_PORT}',
    write_routing_mode 'planned',
    write_transaction_mode 'required'
);

CREATE TABLE ${source_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
DISTRIBUTED RANDOMLY;

INSERT INTO ${source_table}
SELECT
    i,
    i % 3,
    'mpp-' || i,
    i % 2 = 0,
    i / 10.0,
    DATE '2026-01-01' + i % 30,
    TIMESTAMP '2026-01-01' + i * INTERVAL '1 second'
FROM generate_series(1, 300) AS i;

CREATE FOREIGN TABLE ${origin_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${origin_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${auto_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${auto_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${required_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${required_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${fail_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${fail_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${no_cluster_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${no_cluster_server}
OPTIONS (table_name 'af_perf_write');
SQL

explain="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -qAt -v ON_ERROR_STOP=1 postgres -c "
      EXPLAIN (VERBOSE)
      INSERT INTO ${auto_table}
      SELECT * FROM ${source_table}"
)"
grep -q "Flight SQL Write Routing: planned" <<<"${explain}"

set +e
origin_error="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres \
      -c "INSERT INTO ${origin_table} SELECT * FROM ${source_table}" \
      2>&1
)"
origin_status=$?
set -e
test "${origin_status}" -ne 0
grep -q "does not support planned MPP ingest" <<<"${origin_error}"

set +e
no_cluster_error="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres \
      -c "INSERT INTO ${no_cluster_table} SELECT * FROM ${source_table}" \
      2>&1
)"
no_cluster_status=$?
set -e
test "${no_cluster_status}" -ne 0
grep -q "requires cluster-scoped transactions" <<<"${no_cluster_error}"

PGOPTIONS='-c optimizer=on -c timezone=UTC' \
  psql -X -v ON_ERROR_STOP=1 postgres <<SQL
INSERT INTO ${auto_table}
SELECT * FROM ${source_table};

INSERT INTO ${auto_table}
SELECT * FROM ${source_table} WHERE false;

INSERT INTO ${required_table}
SELECT id + 1000, segid, label, active, amount, d, ts
FROM ${source_table};

BEGIN;
INSERT INTO ${required_table}
SELECT id + 2000, segid, label, active, amount, d, ts
FROM ${source_table};
ROLLBACK;

BEGIN;
SAVEPOINT planned_write;
INSERT INTO ${required_table}
SELECT id + 3000, segid, label, active, amount, d, ts
FROM ${source_table};
ROLLBACK TO SAVEPOINT planned_write;
COMMIT;
SQL

set +e
fail_error="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X -v ON_ERROR_STOP=1 postgres \
      -c "INSERT INTO ${fail_table} SELECT * FROM ${source_table}" \
      2>&1
)"
fail_status=$?
set -e
test "${fail_status}" -ne 0
grep -q "injected benchmark ingest failure" <<<"${fail_error}"

set +e
fail_savepoint_output="$(
  PGOPTIONS='-c optimizer=on -c timezone=UTC' \
    psql -X postgres 2>&1 <<SQL
\set ON_ERROR_STOP on
BEGIN;
SAVEPOINT failed_planned_write;
\set ON_ERROR_STOP off
INSERT INTO ${fail_table}
SELECT id + 4000, segid, label, active, amount, d, ts
FROM ${source_table};
\set ON_ERROR_STOP on
ROLLBACK TO SAVEPOINT failed_planned_write;
COMMIT;
SQL
)"
fail_savepoint_status=$?
set -e
test "${fail_savepoint_status}" -eq 0
grep -q "injected benchmark ingest failure" <<<"${fail_savepoint_output}"

echo "flightsql_mpp_integration=ok"
