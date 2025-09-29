#!/bin/bash
set -exo pipefail

GPDB_SRC_PATH=${GPDB_SRC_PATH:=gpdb_src}

function test_orca
{
    if [ -n "${SKIP_UNITTESTS}" ]; then
        return
    fi
    OUTPUT_DIR="../../../../gpAux/ext/${BLD_ARCH}"
    pushd ${GPDB_SRC_PATH}/src/backend/gporca
    concourse/build_and_test.py --build_type=RelWithDebInfo --output_dir=${OUTPUT_DIR}
    concourse/build_and_test.py --build_type=Debug --output_dir=${OUTPUT_DIR}
    popd
}

function _main
{
  mkdir gpdb_src/gpAux/ext
  test_orca
}

_main "$@"
