#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
extension_dir="$(dirname "${script_dir}")"
repo_dir="$(cd "${extension_dir}/../.." && pwd)"
# For a VPATH build, use its generated headers and matching portability library.
pg_build_dir="$(cd "${PG_BUILD_DIR:-${repo_dir}}" && pwd)"
pgport_lib="${pg_build_dir}/src/port/libpgport.a"
if [[ ! -r "${pg_build_dir}/src/include/pg_config.h" || ! -r "${pgport_lib}" ]]; then
  printf '%s\n' \
    'Set PG_BUILD_DIR to a configured Greengage build tree with src/port/libpgport.a built.' >&2
  exit 1
fi
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/flightsql-mpp-route-test.XXXXXX")"
trap 'rm -rf "${build_dir}"' EXIT

# Arrow flags can point at an isolated development install such as a PyArrow wheel.
read -r -a arrow_cflags <<< "${ARROW_CFLAGS:-$(pkg-config --cflags arrow arrow-flight)}"
read -r -a arrow_libs <<< "${ARROW_LIBS:-$(pkg-config --libs arrow arrow-flight)}"
read -r -a protobuf_cflags <<< "$(pkg-config --cflags protobuf)"
read -r -a protobuf_libs <<< "$(pkg-config --libs protobuf)"

protoc --proto_path="${extension_dir}/src/proto" \
  --cpp_out="${build_dir}" "${extension_dir}/src/proto/flightsql_mpp.proto"

"${CC:-cc}" -I"${pg_build_dir}/src/include" -I"${repo_dir}/src/include" \
  -c "${repo_dir}/src/common/base64.c" \
  -o "${build_dir}/base64.o"

# Only the tested internal functions are retained; PG backend wrappers are unused.
dead_strip=(-Wl,--gc-sections)
if [[ "$(uname -s)" == Darwin ]]; then
  dead_strip=(-Wl,-dead_strip)
fi

"${CXX:-c++}" -std=c++20 -O1 -Wall -Werror -Wno-deprecated-declarations \
  -ffunction-sections -fdata-sections -DUSE_ARROW_FLIGHT \
  -I"${pg_build_dir}/src/include" -I"${repo_dir}/src/include" \
  -I"${extension_dir}/src/include" -I"${build_dir}" \
  "${arrow_cflags[@]}" "${protobuf_cflags[@]}" \
  "${script_dir}/flightsql_mpp_route_test.cpp" \
  "${build_dir}/flightsql_mpp.pb.cc" "${build_dir}/base64.o" \
  "${pgport_lib}" \
  "${dead_strip[@]}" "${arrow_libs[@]}" "${protobuf_libs[@]}" \
  -o "${build_dir}/flightsql_mpp_route_test"

"${build_dir}/flightsql_mpp_route_test"
