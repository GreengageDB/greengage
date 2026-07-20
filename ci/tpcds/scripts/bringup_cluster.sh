#!/bin/bash
# Bring up a Greengage demo cluster INSIDE this container and prepare it as the
# TPC-DS coordinator + segment host. Run as root (docker compose exec).
set -euo pipefail

: "${NUM_PRIMARY_MIRROR_PAIRS:=3}"
: "${WITH_MIRRORS:=false}"
export NUM_PRIMARY_MIRROR_PAIRS WITH_MIRRORS
export WITH_STANDBY=false          # no standby needed for a benchmark

GPHOME=/usr/local/greenplum-db-devel
DEMO=/home/gpadmin/gpdb_src/gpAux/gpdemo

echo "===== [1/5] sshd (harness ssh-to-self for data-gen / gpfdist) ====="
ssh-keygen -A
mkdir -p /run/sshd
pgrep -x sshd >/dev/null || /usr/sbin/sshd

echo "===== [2/5] install binaries + gpadmin user ====="
cd /home/gpadmin
source gpdb_src/concourse/scripts/common.bash
install_and_configure_gpdb
gpdb_src/concourse/scripts/setup_gpadmin_user.bash

echo "===== [3/5] create demo cluster (${NUM_PRIMARY_MIRROR_PAIRS} primaries, mirrors=${WITH_MIRRORS}) ====="
make_cluster

echo "===== [4/5] gpadmin ssh-to-self + environment ====="
su - gpadmin -c '
  set -e
  mkdir -p ~/.ssh && chmod 700 ~/.ssh
  [ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
  grep -qf ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys 2>/dev/null || cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
  for h in localhost tpcds $(hostname) $(hostname -s); do ssh-keyscan "$h"; done >> ~/.ssh/known_hosts 2>/dev/null
  # greenplum_path + demo env must be sourced even by non-interactive ssh shells
  # (sshd runs ~/.bashrc, but the stock guard returns early), so prepend them.
  if ! grep -q greenplum_path ~/.bashrc 2>/dev/null; then
    { echo "source '"$GPHOME"'/greenplum_path.sh";
      echo "source '"$DEMO"'/gpdemo-env.sh 2>/dev/null || true";
      echo "export PGDATABASE=tpcds";
      cat ~/.bashrc 2>/dev/null; } > ~/.bashrc.new && mv ~/.bashrc.new ~/.bashrc
  fi
'

echo "===== [5/5] benchmark database + key GUCs ====="
su - gpadmin -c "
  set -e
  source $GPHOME/greenplum_path.sh
  source $DEMO/gpdemo-env.sh
  createdb tpcds 2>/dev/null || echo 'db tpcds already exists'
  # Consistent optimizer setting for a fair comparison (ORCA on by default).
  gpconfig -c optimizer -v ${OPTIMIZER:-on} >/dev/null 2>&1 || echo 'WARN: could not set optimizer GUC'
  gpstop -u >/dev/null 2>&1 || true
  psql -d tpcds -Atc \"select version();\"
"
echo "CLUSTER_READY"
