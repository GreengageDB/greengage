#!/bin/bash -l

set -exo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GPDB_INSTALL_DIR="/usr/local/greengage-db-devel"

function gen_local_regression_script(){
cat > /home/gpadmin/run_regression_test.sh <<-EOF
set -exo pipefail
INSTALL_DIR=/usr/local/greengage-db-devel
GPDB_SRC_DIR=\${1}/gpdb_src

source \$INSTALL_DIR/greengage_path.sh

cd "\${GPDB_SRC_DIR}/gpAux"
source gpdemo/gpdemo-env.sh

cd "\${GPDB_SRC_DIR}/gpcontrib/gpcloud/regress"
make installcheck pgxs_dir=\$INSTALL_DIR/lib/postgresql/pgxs

[ -s regression.diffs ] && cat regression.diffs && exit 1
exit 0

EOF

  chown gpadmin:gpadmin /home/gpadmin/run_regression_test.sh
  chmod a+x /home/gpadmin/run_regression_test.sh
}

function run_regression_test() {
  su gpadmin -c 'bash /home/gpadmin/run_regression_test.sh $(pwd)'
}

function setup_gpadmin_user() {
  ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash
}

function make_cluster() {
  PYTHONPATH=${SCRIPT_DIR}:${PYTHONPATH} python3 -c "from builds.GpBuild import GpBuild; GpBuild(\"planner\").create_demo_cluster(install_dir='/usr/local/greengage-db-devel')"
}

function configure_with_planner() {
  PYTHONPATH=${SCRIPT_DIR}:${PYTHONPATH} python3 -c "from builds.GpBuild import GpBuild; GpBuild(\"planner\").configure()"
}

function copy_gpdb_bits_to_gphome() {
  mkdir -p ${GPDB_INSTALL_DIR}
  tar -xzf bin_gpdb/*.tar.gz -C ${GPDB_INSTALL_DIR}
  # make sure to correct the path inside of greengage-path.sh
  sed -i "s#GPHOME=.*#GPHOME=$GPDB_INSTALL_DIR#g" ${GPDB_INSTALL_DIR}/greengage_path.sh
}

function _main() {
  if [ -z "$TARGET_OS" ]; then
    echo "FATAL: TARGET_OS is not set"
    exit 1
  fi

  time copy_gpdb_bits_to_gphome
  time setup_gpadmin_user
  time make_cluster
  time gen_local_regression_script
  time run_regression_test
}

_main "$@"
