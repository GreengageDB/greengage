#!/bin/bash
set -eu
TEST_OS=${TEST_OS:-ubuntu}
OPTIMIZER=${OPTIMIZER:-off}
STATEMENT_MEM=${STATEMENT_MEM:-125MB}

echo ">>> DEBUG Free on host <<<"
free -h

# shellcheck disable=SC2086
docker run -i --user root:root \
  -v ./logs:/logs \
  -e TEST_OS=$TEST_OS \
  --sysctl 'kernel.sem=500 1024000 200 4096' \
  --privileged \
  $IMAGE /bin/bash << EOF
set -ex
set -o pipefail

echo ">>> DEBUG Free in Docker <<<"
free -h

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

echo ">>> DEBUG: cgroup files <<<"
cat /proc/1/mounts | grep cgroup || true
cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null || true
cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || true
cat /sys/fs/cgroup/memory.current 2>/dev/null || true
cat /sys/fs/cgroup/memory.max 2>/dev/null || true

EXIT_CODE=0
sudo -u gpadmin -- bash -c "
  set -ex
  source \$GPHOME/greengage_path.sh
  source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
  make -C /home/gpadmin/gpdb_src/src/test/regress
  make PGOPTIONS='-c optimizer=$OPTIMIZER -c statement_mem=$STATEMENT_MEM' installcheck-resgroup -C gpdb_src/
" || EXIT_CODE=1

params=(
  "./ d gpAdminLogs"
  "gpdb_src/src/test/ d results"
  "gpdb_src/src/test/ f regression.diffs"
  "gpdb_src/gpAux/gpdemo/datadirs/ d pg_log"
)
for param in "\${params[@]}"; do
  read -r path type name <<< "\$param"
  find \$path -name \$name -type \$type -exec tar -rf "/logs/\$name.tar" "{}" \;
done
chmod -R a+rwX /logs
ls -lah /logs

exit \$EXIT_CODE
EOF
