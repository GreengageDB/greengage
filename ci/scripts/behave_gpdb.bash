#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../concourse/scripts" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
		cat > /opt/run_test.sh <<-EOF
		set -ex

		source /usr/local/greengage-db-devel/greengage_path.sh

		cat > /tmp/sitecustomize.py <<INNER_EOF
		import coverage
		coverage.process_startup()
		INNER_EOF

		# Now copy everything over to the hosts.
		gpscp -f /tmp/hostfile_all -v /tmp/sitecustomize.py =:/usr/local/greengage-db-devel/lib/python
		gpssh -f /tmp/hostfile_all -v -e "echo 'export COVERAGE_PROCESS_START=\${1}/gpdb_src/gpMgmt/test/coveragerc_behave' >> /usr/local/greengage-db-devel/greengage_path.sh"

		source /usr/local/greengage-db-devel/greengage_path.sh

		cd "\${1}/gpdb_src/gpMgmt/"
		BEHAVE_TAGS="${BEHAVE_TAGS}"
		BEHAVE_FLAGS="${BEHAVE_FLAGS}"
		if [ ! -z "\${BEHAVE_TAGS}" ]; then
				make -f Makefile.behave behave tags=\${BEHAVE_TAGS}
		else
				flags="\${BEHAVE_FLAGS}" make -f Makefile.behave behave
		fi

		gpscp -f /tmp/hostfile_all -v =:/tmp/pre-coverage-data/* /tmp/pre-coverage-data/ || true

		if [ ! -z "\${COVERAGE_PROCESS_START}" ] && [ \$(ls /tmp/pre-coverage-data | wc -l) -gt 0 ]; then
			LOCK_FILE=/tmp/coverage-data/coverage.lock
			flock "\$LOCK_FILE" -c "
				mkdir -p /tmp/coverage-data
				cp -r /tmp/pre-coverage-data/* /tmp/coverage-data/
				cd /tmp/coverage-data
				coverage combine --append --rcfile=\${1}/gpdb_src/gpMgmt/test/coveragerc_behave coverage-data*
				coverage html --rcfile=\${1}/gpdb_src/gpMgmt/test/coveragerc_combine_report --show-contexts -d ./coverage-html
				mv /tmp/pre-coverage-data/coverage-data /tmp/coverage-data/
			"
		fi
	EOF

		chmod a+x /opt/run_test.sh
}

function _main() {

		if [ -z "${BEHAVE_TAGS}" ] && [ -z "${BEHAVE_FLAGS}" ]; then
				echo "FATAL: BEHAVE_TAGS or BEHAVE_FLAGS not set"
				exit 1
		fi

		time gen_env

		time run_test
}

_main "$@"
