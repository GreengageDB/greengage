#!/bin/bash
set -eu
LOGS=${LOGS:-$PWD/logs}
TEST_OS=${TEST_OS:-ubuntu}
OPTIMIZER=${OPTIMIZER:-off}
STATEMENT_MEM=${STATEMENT_MEM:-125MB}

docker run -i --user root:root \
  -v "$LOGS":/logs \
  -v "$PWD/gpdemo-datadirs":/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs:rw \
  -v "$PWD/testtablespace":/home/gpadmin/gpdb_src/src/test/isolation2/testtablespace:rw \
  -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
  -e TEST_OS="$TEST_OS" \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  --privileged \
  --cgroupns=host \
  "$IMAGE" /bin/bash << EOF
set -eox pipefail
exitcode=1

cd /home/gpadmin/
ssh-keygen -A
/usr/sbin/sshd
source gpdb_src/concourse/scripts/common.bash
install_and_configure_gpdb
gpdb_src/concourse/scripts/setup_gpadmin_user.bash
make_cluster
sudo mkdir -p /sys/fs/cgroup/gpdb /sys/fs/cgroup/gpdb.service
sudo chmod -R 777 /sys/fs/cgroup /sys/fs/cgroup/gpdb /sys/fs/cgroup/gpdb.service

dd if=/dev/zero of=/home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/io_limit_fs_1.img bs=1M count=1
mkfs.ext4 /home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/io_limit_fs_1.img
sudo mount -o loop /home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/io_limit_fs_1.img /home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/rg_io_limit_ts_1

dd if=/dev/zero of=/home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/io_limit_fs_2.img bs=1M count=1
mkfs.ext4 /home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/io_limit_fs_2.img
sudo mount -o loop /home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/io_limit_fs_2.img /home/gpadmin/gpdb_src/src/test/isolation2/testtablespace/rg_io_limit_ts_2

sudo -u gpadmin -- bash -c "
  set -ex
  source \$GPHOME/greengage_path.sh
  source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
  make -C /home/gpadmin/gpdb_src/src/test/regress
  make PGOPTIONS='-c optimizer=$OPTIMIZER -c statement_mem=$STATEMENT_MEM' installcheck-resgroup-v2 -C gpdb_src/src/test/isolation2
" && exitcode=0

echo \$exitcode > /logs/.exitcode
exit \$exitcode
EOF
