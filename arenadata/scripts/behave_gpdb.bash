#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../concourse/scripts" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
		cat > /opt/run_test.sh <<-EOF
		set -ex

		source /usr/local/greenplum-db-devel/greenplum_path.sh

		source gpdb_src/gpAux/gpdemo/gpdemo-env.sh

		if [[ ${FEATURE} == "gpexpand" ]]; then
			mkdir -p /home/gpadmin/sqldump
			wget -nv https://rt.adsw.io/artifactory/common/dump.sql.xz -O /home/gpadmin/sqldump/dump.sql.xz

			xz -d /home/gpadmin/sqldump/dump.sql.xz
		fi

		cd "\${1}/gpdb_src/gpMgmt/"
		BEHAVE_TAGS="${BEHAVE_TAGS}"
		BEHAVE_FLAGS="${BEHAVE_FLAGS}"
		if [ ! -z "\${BEHAVE_TAGS}" ]; then
				make -f Makefile.behave behave tags=\${BEHAVE_TAGS}
		else
				flags="\${BEHAVE_FLAGS}" make -f Makefile.behave behave
		fi
	EOF

		chmod a+x /opt/run_test.sh
}

function _main() {

		if [ -z "${BEHAVE_TAGS}" ] && [ -z "${BEHAVE_FLAGS}" ]; then
				echo "FATAL: BEHAVE_TAGS or BEHAVE_FLAGS not set"
				exit 1
		fi

		time install_gpdb
		time ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash


		local hosts="sdw1 sdw2 sdw3 mdw"
		for host in $hosts
		do
			ssh -oStrictHostKeyChecking=no -i /home/gpadmin/.ssh/id_rsa gpadmin@$host /bin/bash -c \
				"getent hosts ${hosts/$host/} | sudo tee -a /etc/hosts &&
				ssh-keyscan ${hosts/$host/} >> /home/gpadmin/.ssh/known_hosts"
		done

		# Run inside a subshell so it does not pollute the environment after
		# sourcing greenplum_path
		time (make_cluster)

		time gen_env

		time run_test
}

_main "$@"
