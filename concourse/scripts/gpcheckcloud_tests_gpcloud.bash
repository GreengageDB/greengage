#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
	cat > /home/gpadmin/run_regression_gpcheckcloud.sh <<-EOF
	set -exo pipefail

	source /usr/local/greengage-db-devel/greengage_path.sh

	cd "\${1}/gpdb_src/gpcontrib/gpcloud/regress"
	bash gpcheckcloud_regress.sh
	EOF

	chown gpadmin:gpadmin /home/gpadmin/run_regression_gpcheckcloud.sh
	chmod a+x /home/gpadmin/run_regression_gpcheckcloud.sh
}

function run_regression_gpcheckcloud() {
	su gpadmin -c "bash /home/gpadmin/run_regression_gpcheckcloud.sh $(pwd)"
}

function setup_gpadmin_user() {
	./gpdb_src/concourse/scripts/setup_gpadmin_user.bash "centos"
}

function _main() {
	time install_and_configure_gpdb
	time setup_gpadmin_user
	time gen_env

	time run_regression_gpcheckcloud
}

_main "$@"
