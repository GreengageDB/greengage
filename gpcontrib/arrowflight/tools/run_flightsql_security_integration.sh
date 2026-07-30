#!/usr/bin/env bash
set -euo pipefail

: "${FLIGHTSQL_SECURE_HOST:=flightsql-synthetic-secure}"
: "${FLIGHTSQL_SECURE_PORT:=9019}"
: "${FLIGHTSQL_SECURE_ENDPOINT:=grpc+tls://flightsql-synthetic-secure-alias:9019}"
: "${FLIGHTSQL_SECURITY_DIR:=/tmp/flightsql-security}"

suffix="${FLIGHTSQL_TEST_SUFFIX:-$$}"
secure_server="flightsql_secure_${suffix}"
denied_server="flightsql_secure_denied_${suffix}"
wrong_token_server="flightsql_secure_wrong_token_${suffix}"
missing_token_server="flightsql_secure_missing_token_${suffix}"
missing_cert_server="flightsql_secure_missing_cert_${suffix}"
wrong_ca_server="flightsql_secure_wrong_ca_${suffix}"
plaintext_server="flightsql_secure_plaintext_${suffix}"
read_table="flightsql_secure_read_${suffix}"
denied_table="flightsql_secure_denied_read_${suffix}"
wrong_token_table="flightsql_secure_wrong_token_read_${suffix}"
missing_token_table="flightsql_secure_missing_token_read_${suffix}"
missing_cert_table="flightsql_secure_missing_cert_read_${suffix}"
wrong_ca_table="flightsql_secure_wrong_ca_read_${suffix}"
plaintext_table="flightsql_secure_plaintext_read_${suffix}"
write_table="flightsql_secure_write_${suffix}"
readback_table="flightsql_secure_readback_${suffix}"
source_table="flightsql_secure_source_${suffix}"
all_columns="id, segid, label, active, amount, d, ts"
all_counts="count(id), count(segid), count(label), count(active), count(amount), count(d), count(ts)"

cleanup() {
  psql -X -q -v ON_ERROR_STOP=0 postgres >/dev/null 2>&1 <<SQL || true
DROP FOREIGN TABLE IF EXISTS ${read_table};
DROP FOREIGN TABLE IF EXISTS ${denied_table};
DROP FOREIGN TABLE IF EXISTS ${wrong_token_table};
DROP FOREIGN TABLE IF EXISTS ${missing_token_table};
DROP FOREIGN TABLE IF EXISTS ${missing_cert_table};
DROP FOREIGN TABLE IF EXISTS ${wrong_ca_table};
DROP FOREIGN TABLE IF EXISTS ${plaintext_table};
DROP FOREIGN TABLE IF EXISTS ${write_table};
DROP FOREIGN TABLE IF EXISTS ${readback_table};
DROP TABLE IF EXISTS ${source_table};
DROP SERVER IF EXISTS ${secure_server} CASCADE;
DROP SERVER IF EXISTS ${denied_server} CASCADE;
DROP SERVER IF EXISTS ${wrong_token_server} CASCADE;
DROP SERVER IF EXISTS ${missing_token_server} CASCADE;
DROP SERVER IF EXISTS ${missing_cert_server} CASCADE;
DROP SERVER IF EXISTS ${wrong_ca_server} CASCADE;
DROP SERVER IF EXISTS ${plaintext_server} CASCADE;
SQL
}
trap cleanup EXIT

expect_failure() {
  local label="$1"
  local pattern="$2"
  local query="$3"
  local output
  local status

  set +e
  output="$(
    psql -X -v ON_ERROR_STOP=1 postgres -c "${query}" 2>&1
  )"
  status=$?
  set -e

  if [ "${status}" -eq 0 ]; then
    echo "${label}: query unexpectedly succeeded" >&2
    exit 1
  fi
  if ! grep -Eq "${pattern}" <<<"${output}"; then
    echo "${label}: unexpected error" >&2
    echo "${output}" >&2
    exit 1
  fi
  if grep -Fq 'flightsql-test-token' <<<"${output}" ||
     grep -Fq 'wrong-flightsql-test-token' <<<"${output}"; then
    echo "${label}: error output exposed a Bearer token" >&2
    exit 1
  fi
}

create_read_table() {
  local table="$1"
  local server="$2"

  psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE FOREIGN TABLE ${table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${server}
OPTIONS (
    table_name 'af_perf_read',
    rows '3000'
);
SQL
}

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE SERVER ${denied_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SECURE_HOST}',
    port '${FLIGHTSQL_SECURE_PORT}',
    tls 'true',
    tls_ca_file '${FLIGHTSQL_SECURITY_DIR}/ca.pem',
    tls_client_cert_file '${FLIGHTSQL_SECURITY_DIR}/client.pem',
    tls_client_key_file '${FLIGHTSQL_SECURITY_DIR}/client.key',
    auth_token_file '${FLIGHTSQL_SECURITY_DIR}/token'
);
SQL
create_read_table "${denied_table}" "${denied_server}"
expect_failure \
  "endpoint allowlist" \
  "endpoint location is not allowed" \
  "SELECT ${all_columns} FROM ${denied_table} LIMIT 1"

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE SERVER ${secure_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SECURE_HOST}',
    port '${FLIGHTSQL_SECURE_PORT}',
    tls 'true',
    tls_ca_file '${FLIGHTSQL_SECURITY_DIR}/ca.pem',
    tls_client_cert_file '${FLIGHTSQL_SECURITY_DIR}/client.pem',
    tls_client_key_file '${FLIGHTSQL_SECURITY_DIR}/client.key',
    auth_token_file '${FLIGHTSQL_SECURITY_DIR}/token',
    endpoint_location_allowlist '${FLIGHTSQL_SECURE_ENDPOINT}',
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
    'secure-' || i,
    i % 2 = 0,
    i / 10.0,
    DATE '2026-01-01' + i % 30,
    TIMESTAMP '2026-01-01' + i * INTERVAL '1 second'
FROM generate_series(1, 60) AS i;
SQL

create_read_table "${read_table}" "${secure_server}"

secure_rows="$(
  psql -X -qAt -F ':' -v ON_ERROR_STOP=1 postgres \
    -c "SELECT ${all_counts} FROM ${read_table}"
)"
test "${secure_rows}" = "3000:3000:3000:3000:3000:3000:3000"

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE FOREIGN TABLE ${write_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${secure_server}
OPTIONS (table_name 'af_perf_write');

CREATE FOREIGN TABLE ${readback_table}
(
    id int4,
    segid int4,
    label text,
    active bool,
    amount float8,
    d date,
    ts timestamp
)
SERVER ${secure_server}
OPTIONS (table_name 'af_perf_write');

INSERT INTO ${write_table}
SELECT * FROM ${source_table};
SQL

secure_written_rows="$(
  psql -X -qAt -F ':' -v ON_ERROR_STOP=1 postgres \
    -c "SELECT ${all_counts} FROM ${readback_table}"
)"
test "${secure_written_rows}" = "60:60:60:60:60:60:60"

psql -X -v ON_ERROR_STOP=1 postgres <<SQL
CREATE SERVER ${wrong_token_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SECURE_HOST}',
    port '${FLIGHTSQL_SECURE_PORT}',
    tls 'true',
    tls_ca_file '${FLIGHTSQL_SECURITY_DIR}/ca.pem',
    tls_client_cert_file '${FLIGHTSQL_SECURITY_DIR}/client.pem',
    tls_client_key_file '${FLIGHTSQL_SECURITY_DIR}/client.key',
    auth_token_file '${FLIGHTSQL_SECURITY_DIR}/wrong-token',
    endpoint_location_allowlist '${FLIGHTSQL_SECURE_ENDPOINT}'
);

CREATE SERVER ${missing_cert_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SECURE_HOST}',
    port '${FLIGHTSQL_SECURE_PORT}',
    tls 'true',
    tls_ca_file '${FLIGHTSQL_SECURITY_DIR}/ca.pem',
    auth_token_file '${FLIGHTSQL_SECURITY_DIR}/token',
    endpoint_location_allowlist '${FLIGHTSQL_SECURE_ENDPOINT}'
);

CREATE SERVER ${missing_token_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SECURE_HOST}',
    port '${FLIGHTSQL_SECURE_PORT}',
    tls 'true',
    tls_ca_file '${FLIGHTSQL_SECURITY_DIR}/ca.pem',
    tls_client_cert_file '${FLIGHTSQL_SECURITY_DIR}/client.pem',
    tls_client_key_file '${FLIGHTSQL_SECURITY_DIR}/client.key',
    endpoint_location_allowlist '${FLIGHTSQL_SECURE_ENDPOINT}'
);

CREATE SERVER ${wrong_ca_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SECURE_HOST}',
    port '${FLIGHTSQL_SECURE_PORT}',
    tls 'true',
    tls_ca_file '${FLIGHTSQL_SECURITY_DIR}/wrong-ca.pem',
    tls_client_cert_file '${FLIGHTSQL_SECURITY_DIR}/client.pem',
    tls_client_key_file '${FLIGHTSQL_SECURITY_DIR}/client.key',
    auth_token_file '${FLIGHTSQL_SECURITY_DIR}/token',
    endpoint_location_allowlist '${FLIGHTSQL_SECURE_ENDPOINT}'
);

CREATE SERVER ${plaintext_server}
FOREIGN DATA WRAPPER flightsql_fdw
OPTIONS (
    host '${FLIGHTSQL_SECURE_HOST}',
    port '${FLIGHTSQL_SECURE_PORT}'
);
SQL

create_read_table "${wrong_token_table}" "${wrong_token_server}"
create_read_table "${missing_token_table}" "${missing_token_server}"
create_read_table "${missing_cert_table}" "${missing_cert_server}"
create_read_table "${wrong_ca_table}" "${wrong_ca_server}"
create_read_table "${plaintext_table}" "${plaintext_server}"

expect_failure \
  "wrong Bearer token" \
  "Unauthenticated|missing or invalid Bearer token" \
  "SELECT ${all_columns} FROM ${wrong_token_table} LIMIT 1"
expect_failure \
  "missing Bearer token" \
  "Unauthenticated|missing or invalid Bearer token" \
  "SELECT ${all_columns} FROM ${missing_token_table} LIMIT 1"
expect_failure \
  "missing mTLS client certificate" \
  "SSL|TLS|certificate|handshake|unavailable" \
  "SELECT ${all_columns} FROM ${missing_cert_table} LIMIT 1"
expect_failure \
  "untrusted server certificate" \
  "SSL|TLS|certificate|verify|handshake|unavailable" \
  "SELECT ${all_columns} FROM ${wrong_ca_table} LIMIT 1"
expect_failure \
  "plaintext client" \
  "SSL|TLS|socket|connection|unavailable" \
  "SELECT ${all_columns} FROM ${plaintext_table} LIMIT 1"

echo "flightsql_security_integration=ok"
