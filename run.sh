#!/bin/bash
#Usage: ./flake.sh [runs] [file-in-container]

N=${1:-5}
FILE1="/home/gpadmin/gpdb_src/src/test/isolation2/resgroup/regression.diffs"
FILE2="/home/gpadmin/gpdb_src/src/test/isolation2/resgroup/results/resgroup/resgroup_cpu_max_percent.out"

mkdir -p gpdemo-datadirs testtablespace logs
chmod -R 777 gpdemo-datadirs testtablespace logs

docker build -t gpdb7_u22:latest -f ci/Dockerfile.ubuntu .

for i in $(seq 1 "$N"); do
  mkdir -p gpdemo-datadirs testtablespace logs "runs/$i"
  chmod -R 777 gpdemo-datadirs testtablespace logs

  docker run --name gpdb7_resgroup_v2 -e TEST_OS=ubuntu -e OPTIMIZER=on \
    --sysctl "kernel.sem=500 1024000 200 4096" \
    --privileged \
    --cgroupns=host \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    -v "$PWD/gpdemo-datadirs":/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs:rw \
    -v "$PWD/testtablespace":/home/gpadmin/gpdb_src/src/test/isolation2/testtablespace:rw \
    -v "$PWD/logs":/logs:rw \
    gpdb7_u22:latest \
    /home/gpadmin/gpdb_src/concourse/scripts/ic_gpdb_resgroup_v2.bash 2>&1 | tee "runs/$i/output.log"
  rc=${PIPESTATUS[0]}
  echo "run $i: exit $rc" | tee -a runs/summary.txt

  [ -n "$FILE1" ] && docker cp "gpdb7_resgroup_v2:$FILE1" "runs/$i/"
  [ -n "$FILE2" ] && docker cp "gpdb7_resgroup_v2:$FILE2" "runs/$i/"

  rm -rf testtablespace gpdemo-datadirs logs
  docker rm gpdb7_resgroup_v2

done
