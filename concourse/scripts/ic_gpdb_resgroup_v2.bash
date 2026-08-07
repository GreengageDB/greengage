#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

CGROUP_BASEDIR=${CGROUP_BASEDIR:-/sys/fs/cgroup}
OPTIMIZER=${OPTIMIZER:-off}
STATEMENT_MEM=${STATEMENT_MEM:-125MB}
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
        fatal "resource group v2 tests must run as root inside a privileged Docker container"
    fi
}

assert_real_filesystem() {
    local path=$1
    local fs_type

    mkdir -p "$path"
    fs_type=$(stat -f -c %T "$path")

    case "$fs_type" in
        overlayfs|tmpfs)
            fatal "$path is on $fs_type; bind mount it from a regular host filesystem for IO_LIMIT tests"
            ;;
    esac
}

enable_cgroup_controller() {
    local controller=$1
    local basedir=$CGROUP_BASEDIR

    if ! grep -qw "$controller" "$basedir/cgroup.controllers"; then
        fatal "cgroup v2 controller '$controller' is not available in $basedir/cgroup.controllers"
    fi

    if ! grep -qw "$controller" "$basedir/cgroup.subtree_control"; then
        if ! echo "+$controller" > "$basedir/cgroup.subtree_control"; then
            fatal "failed to enable cgroup v2 controller '$controller'; run Docker with --privileged, --cgroupns=host and a rw /sys/fs/cgroup mount"
        fi
    fi
}

setup_cgroup_v2() {
    local basedir=$CGROUP_BASEDIR
    local gpdb_cgroup=$basedir/gpdb
    local controller

    if [ ! -f "$basedir/cgroup.controllers" ]; then
        fatal "$basedir is not a cgroup v2 mount"
    fi

    if [ ! -w "$basedir/cgroup.subtree_control" ]; then
        fatal "$basedir/cgroup.subtree_control is not writable; mount /sys/fs/cgroup rw and use --cgroupns=host"
    fi

    chmod a+rw "$basedir/cgroup.procs"

    for controller in cpu cpuset io memory pids; do
        enable_cgroup_controller "$controller"
    done

    mkdir -p "$gpdb_cgroup"
    chmod a+rwx "$basedir" "$gpdb_cgroup"
    chmod -R a+rwX "$gpdb_cgroup"

    if [ ! -w "$basedir/cgroup.procs" ]; then
        fatal "$basedir/cgroup.procs is not writable; cgroup v2 process migration may fail"
    fi

    for controller in cpu cpuset io memory pids; do
        if ! grep -qw "$controller" "$gpdb_cgroup/cgroup.controllers"; then
            fatal "controller '$controller' is not available below $gpdb_cgroup after enabling subtree control"
        fi
    done

    for file in cgroup.procs cpu.max cpu.weight cpuset.cpus cpuset.mems io.max memory.max cgroup.subtree_control; do
        if [ ! -e "$gpdb_cgroup/$file" ]; then
            fatal "required cgroup v2 file $gpdb_cgroup/$file is missing"
        fi
    done
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
		make PGOPTIONS="-c optimizer=${OPTIMIZER} -c statement_mem=${STATEMENT_MEM}" \
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
    setup_cgroup_v2

    time install_and_configure_gpdb
    time setup_gpadmin_user
    time make_cluster
    time gen_env
    time run_test
}

_main "$@"
