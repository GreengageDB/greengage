#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

OPTIMIZER=${OPTIMIZER:-off}
RESGROUP_STATEMENT_MEM=${STATEMENT_MEM:-125MB}
TEST_OS=${TEST_OS:-ubuntu}

GPDB_DEMO_DATADIRS=/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs
ISOLATION2_TESTTABLESPACE=/home/gpadmin/gpdb_src/src/test/isolation2/testtablespace

fatal() {
    echo "FATAL: $*" >&2
    exit 1
}

record_exitcode() {
    local exitcode=$?

    if [ -d /logs ] && [ -w /logs ]; then
        echo "$exitcode" > /logs/.exitcode || true
    fi

    exit "$exitcode"
}

trap record_exitcode EXIT

assert_root() {
    if [ "$(id -u)" -ne 0 ]; then
        fatal "resource group v2 tests must run as root to configure cgroup v2"
    fi
}

assert_real_filesystem() {
    local path=$1
    local fs_type

    mkdir -p "$path"
    fs_type=$(stat -f -c %T "$path")

    if [ "$fs_type" = "overlayfs" ] || [ "$fs_type" = "tmpfs" ]; then
        fatal "$path is on $fs_type; IO_LIMIT tests require a regular filesystem"
    fi
}

# Create GPDB cgroup, enable parent controllers, and grant gpadmin access.
setup_cgroup_v2() {
    if [ ! -f /sys/fs/cgroup/cgroup.controllers ]; then
        fatal "/sys/fs/cgroup is not a cgroup v2 mount"
    fi

    mkdir -p /sys/fs/cgroup/gpdb

    if [ ! -w /sys/fs/cgroup/cgroup.subtree_control ]; then
        fatal "/sys/fs/cgroup/cgroup.subtree_control is not writable"
    fi

    if ! echo "+cpuset +io +cpu +memory" > /sys/fs/cgroup/cgroup.subtree_control; then
        fatal "failed to enable cgroup v2 controllers"
    fi

    chown -R gpadmin:gpadmin /sys/fs/cgroup/gpdb
    chmod a+w /sys/fs/cgroup/cgroup.procs
}

gen_env() {
    cat > /opt/run_test.sh <<-EOF
		trap look4diffs ERR

		function look4diffs() {
		    diff_files=\`find .. -name regression.diffs\`

		    for diff_file in \${diff_files}; do
		        if [ -f "\${diff_file}" ]; then
		            cat <<-FEOF

					======================================================================
					DIFF FILE: \${diff_file}
					----------------------------------------------------------------------

					\$(cat "\${diff_file}")

				FEOF
		        fi
		    done
		    exit 1
		}

		source /usr/local/greengage-db-devel/greengage_path.sh
		cd "\${1}/gpdb_src"
		source gpAux/gpdemo/gpdemo-env.sh

		make -C src/test/regress
		make PGOPTIONS="-c optimizer=${OPTIMIZER} -c statement_mem=${RESGROUP_STATEMENT_MEM}" \
		    installcheck-resgroup-v2 -C src/test/isolation2
	EOF

    chmod a+x /opt/run_test.sh
}

setup_gpadmin_user() {
    ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash "$TEST_OS"
}

_main() {
    assert_root

    if [ -z "$TEST_OS" ]; then
        fatal "TEST_OS is not set"
    fi

    if [[ "$TEST_OS" != centos* && "$TEST_OS" != ubuntu* ]]; then
        fatal "TEST_OS is set to an invalid value: $TEST_OS; configure TEST_OS to be centos or ubuntu"
    fi

    cd /home/gpadmin
    assert_real_filesystem "$GPDB_DEMO_DATADIRS"
    assert_real_filesystem "$ISOLATION2_TESTTABLESPACE"

    time install_and_configure_gpdb
    time setup_gpadmin_user
    time setup_cgroup_v2
    time make_cluster
    time gen_env
    time run_test
}

_main "$@"
