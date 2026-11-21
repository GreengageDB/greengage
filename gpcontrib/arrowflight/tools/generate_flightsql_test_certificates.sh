#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <output-directory>" >&2
  exit 1
fi

output_dir="$1"
mkdir -p "${output_dir}"
umask 077

openssl genrsa -out "${output_dir}/ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days 2 \
  -key "${output_dir}/ca.key" \
  -subj "/CN=Flight SQL test CA" \
  -out "${output_dir}/ca.pem" >/dev/null 2>&1

openssl genrsa -out "${output_dir}/server.key" 2048 >/dev/null 2>&1
openssl req -new -sha256 \
  -key "${output_dir}/server.key" \
  -subj "/CN=flightsql-synthetic-secure" \
  -out "${output_dir}/server.csr" >/dev/null 2>&1
cat >"${output_dir}/server.ext" <<'EOF'
subjectAltName=DNS:flightsql-synthetic-secure,DNS:flightsql-synthetic-secure-alias
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
EOF
openssl x509 -req -sha256 -days 2 \
  -in "${output_dir}/server.csr" \
  -CA "${output_dir}/ca.pem" \
  -CAkey "${output_dir}/ca.key" \
  -CAcreateserial \
  -extfile "${output_dir}/server.ext" \
  -out "${output_dir}/server.pem" >/dev/null 2>&1

openssl genrsa -out "${output_dir}/client.key" 2048 >/dev/null 2>&1
openssl req -new -sha256 \
  -key "${output_dir}/client.key" \
  -subj "/CN=greengage-flightsql-test" \
  -out "${output_dir}/client.csr" >/dev/null 2>&1
cat >"${output_dir}/client.ext" <<'EOF'
extendedKeyUsage=clientAuth
keyUsage=digitalSignature
EOF
openssl x509 -req -sha256 -days 2 \
  -in "${output_dir}/client.csr" \
  -CA "${output_dir}/ca.pem" \
  -CAkey "${output_dir}/ca.key" \
  -CAcreateserial \
  -extfile "${output_dir}/client.ext" \
  -out "${output_dir}/client.pem" >/dev/null 2>&1

openssl genrsa -out "${output_dir}/wrong-ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days 2 \
  -key "${output_dir}/wrong-ca.key" \
  -subj "/CN=Untrusted Flight SQL test CA" \
  -out "${output_dir}/wrong-ca.pem" >/dev/null 2>&1

printf '%s\n' 'flightsql-test-token' >"${output_dir}/token"
printf '%s\n' 'wrong-flightsql-test-token' >"${output_dir}/wrong-token"

rm -f \
  "${output_dir}/ca.srl" \
  "${output_dir}/server.csr" \
  "${output_dir}/server.ext" \
  "${output_dir}/client.csr" \
  "${output_dir}/client.ext"

