#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/flightsql-read-client-test.XXXXXX")"
trap 'rm -rf "${build_dir}"' EXIT
read -r -a cflags <<< "$(pkg-config --cflags arrow arrow-flight protobuf grpc++)"
read -r -a libs <<< "$(pkg-config --libs arrow arrow-flight protobuf grpc++)"

"${CXX:-c++}" -std=c++20 -O1 -Wall -Wextra -Werror -DUSE_ARROW_FLIGHT \
  -I"${script_dir}/../src/include" "${cflags[@]}" \
  "${script_dir}/flightsql_read_client_test.cpp" "${libs[@]}" \
  -o "${build_dir}/flightsql_read_client_test"
"${build_dir}/flightsql_read_client_test"
