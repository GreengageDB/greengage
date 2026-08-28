#!/bin/bash
# Run the resource group v2 isolation test suite (installcheck-resgroup-v2)
# in a Docker container.
# Required environment:
#   IMAGE           Greengage dev image to run the tests in.
# Optional environment:
#   LOGS            host dir mounted at /logs           (default: $PWD/logs).
#   TEST_OS         centos | ubuntu                     (default: ubuntu).
#   OPTIMIZER       on for ORCA, off for Postgres       (default: off).
#   STATEMENT_MEM   resource group statement_mem        (default: 125MB).
#   CONTAINER_NAME  docker container name               (default: resgroup_v2).
#   DATADIRS        host dir for the demo cluster       (default: $PWD/gpdemo-datadirs).
#   TESTTABLESPACE  host dir for isolation2 tablespace  (default: $PWD/testtablespace).
set -eu

LOGS=${LOGS:-$PWD/logs}
TEST_OS=${TEST_OS:-ubuntu}
OPTIMIZER=${OPTIMIZER:-off}
STATEMENT_MEM=${STATEMENT_MEM:-125MB}
CONTAINER_NAME=${CONTAINER_NAME:-resgroup_v2}
DATADIRS=${DATADIRS:-$PWD/gpdemo-datadirs}
TESTTABLESPACE=${TESTTABLESPACE:-$PWD/testtablespace}

docker run -i --name "$CONTAINER_NAME" --user root:root \
  -v "$LOGS":/logs \
  -v "$DATADIRS":/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs:rw \
  -v "$TESTTABLESPACE":/home/gpadmin/gpdb_src/src/test/isolation2/testtablespace:rw \
  -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
  -e TEST_OS="$TEST_OS" \
  -e OPTIMIZER="$OPTIMIZER" \
  -e STATEMENT_MEM="$STATEMENT_MEM" \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  --privileged \
  --cgroupns=host \
  "$IMAGE" /bin/bash <<'EOF'
set -o pipefail
exitcode=0
/home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb_resgroup_v2.bash || exitcode=$?
echo "$exitcode" > /logs/.exitcode
exit "$exitcode"
EOF
