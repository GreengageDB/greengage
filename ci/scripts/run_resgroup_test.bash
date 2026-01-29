#!/bin/bash
set -eu
LOGS=${LOGS:-$PWD/logs}
TEST_OS=${TEST_OS:-ubuntu}
OPTIMIZER=${OPTIMIZER:-off}
STATEMENT_MEM=${STATEMENT_MEM:-125MB}

docker run -i --user root:root \
  -v "$LOGS":/logs \
  -e TEST_OS="$TEST_OS" \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  --privileged \
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
chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset}
mkdir /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb
chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb
chown -R gpadmin:gpadmin /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb

sudo -u gpadmin -- bash -c "
  set -ex
  source \$GPHOME/greengage_path.sh
  source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
  make -C /home/gpadmin/gpdb_src/src/test/regress
  make PGOPTIONS='-c optimizer=$OPTIMIZER -c statement_mem=$STATEMENT_MEM' installcheck-resgroup -C gpdb_src/
" && exitcode=0

params=(
  "./ d gpAdminLogs"
  "gpdb_src/src/test/ d results"
  "gpdb_src/src/test/ f regression.diffs"
  "gpdb_src/gpAux/gpdemo/datadirs/ d log"
)
for param in "\${params[@]}"; do
  read -r path type name <<< "\$param"
  find \$path -name \$name -type \$type -exec bash -c "tar -rf '/logs/\$name.tar' '{}' ; [ '\$name' == 'regression.diffs' ] && cat '{}' || true" \;
done
chmod -R a+rwX /logs

echo \$exitcode > /logs/.exitcode
exit \$exitcode
EOF
