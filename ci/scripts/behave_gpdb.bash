#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../concourse/scripts" && pwd )"
source "${CWDIR}/common.bash"

CLUSTERS="concourse_cluster"

function gen_env(){
		cat > /opt/run_test.sh <<-EOF
		set -ex

		source /usr/local/greengage-db-devel/greengage_path.sh

		cd "\${1}/gpdb_src/gpMgmt/"
		BEHAVE_TAGS="${BEHAVE_TAGS}"
		BEHAVE_FLAGS="${BEHAVE_FLAGS} --tags=${CLUSTER}"
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

		export BEHAVE_FLAGS="$(echo "$BEHAVE_FLAGS" | sed -e "s| --tags=~concourse_cluster||g")"

		for CLUSTER in $CLUSTERS; do
			time gen_env
			time run_test
		done
}

_main "$@"
