#!/bin/bash
# FILE:    README.Rhel-Rocky.bash
# CONTEXT: Called from ci/Dockerfile.rockylinux for Greengage build,
#          or run directly on a bare-metal host
# PURPOSE: Install build dependencies, compile zstd static library,
#          configure system for Greengage (bare-metal only)

set -euxo pipefail

dnf -y install epel-release

# Detect OS version if not already set
export OS_VERSION="${OS_VERSION:-$(grep -oP '(?<= release )\d+' /etc/redhat-release)}"

perl_packages="perl-Env perl-ExtUtils-Embed perl-IPC-Run perl-JSON perl-Test-Base"

case "$OS_VERSION" in
    8)
        dnf config-manager --set-enabled powertools
        python_version='39' # as 3.9; default: 3 is 3.6 as 36; also, available: 38, 3.11, 3.12
        ;;
    9)
        dnf config-manager --set-enabled crb
        python_version='3' # as default 3.9; also, available: 3.11, 3.12, 3.13, 3.14
        perl_packages="$perl_packages  perl-FindBin perl-Opcode perl-Test-Simple perl-Thread-Queue perl-devel"
        ;;
    *)
        echo "Unsupported Rocky Linux version: $OS_VERSION"
        exit 1
        ;;
esac

python_packages="python$python_version python$python_version-devel python$python_version-pip python$python_version-setuptools python$python_version-future"

# shellcheck disable=SC2086 # intentional: word splitting for package lists
dnf -y install \
    apr-devel \
    apr-util-devel \
    autoconf \
    bison \
    bzip2-devel \
    clang-14 \
    cmake \
    expat-devel \
    flex \
    gcc-c++ \
    git \
    glibc-langpack-en \
    gperf \
    indent \
    iproute \
    iputils \
    java-11-openjdk-devel \
    jq \
    krb5-devel \
    krb5-server \
    krb5-workstation \
    libcurl-devel \
    libevent-devel \
    libicu \
    libkadm5 \
    libtool \
    libuuid-devel \
    libuv-devel \
    libxml2-devel \
    libxslt-devel \
    libyaml-devel \
    llvm-devel \
    lsof \
    net-tools \
    openldap-devel \
    openssh-server \
    openssl-devel \
    pam-devel \
    procps-ng \
    readline-devel \
    rpm-build \
    rsync \
    snappy-devel \
    sudo \
    time \
    unzip \
    vim \
    wget \
    xerces-c-devel \
    zlib-devel \
    $python_packages $perl_packages

# Build zstd with static library (not available as a package on Rocky)
curl -Ls https://github.com/facebook/zstd/releases/download/v1.4.4/zstd-1.4.4.tar.gz | tar -xzf -
make -j"$(nproc)" -C zstd-1.4.4
make install PREFIX=/usr/local -C zstd-1.4.4
rm -rf zstd-1.4.4

#---------------------------------------------------------------------
# Bare-metal only configuration
#---------------------------------------------------------------------
if [[ ! -f /.dockerenv && -z "$IS_DOCKER_BUILD" ]]; then

    # Disable SELinux
    setenforce 0 || true
    if [ -f /etc/selinux/config ]; then
        sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
    fi

    # Disable sssd SELinux provider
    if [ -f /etc/sssd/sssd.conf ]; then
        echo 'selinux_provider=none' >> /etc/sssd/sssd.conf
    fi

    # Stop firewall
    systemctl stop firewalld.service || true
    systemctl disable --now firewalld.service || true

    # Configure kernel parameters
    cat >> /etc/sysctl.d/10-gpdb.conf << EOF
kernel.msgmax = 65536
kernel.msgmnb = 65536
kernel.msgmni = 2048
kernel.sem = 500 2048000 200 8192
kernel.shmmni = 1024
kernel.core_uses_pid = 1
kernel.core_pattern=/var/core/core.%h.%t
kernel.sysrq = 1
net.core.netdev_max_backlog = 2000
net.core.rmem_max = 4194304
net.core.wmem_max = 4194304
net.core.rmem_default = 4194304
net.core.wmem_default = 4194304
net.ipv4.tcp_rmem = 4096 4224000 16777216
net.ipv4.tcp_wmem = 4096 4224000 16777216
net.core.optmem_max = 4194304
net.core.somaxconn = 10000
net.ipv4.ip_forward = 0
net.ipv4.tcp_congestion_control = cubic
net.ipv4.tcp_tw_recycle = 0
net.core.default_qdisc = fq_codel
net.ipv4.tcp_mtu_probing = 0
net.ipv4.conf.all.arp_filter = 1
net.ipv4.conf.default.accept_source_route = 0
net.ipv4.ip_local_port_range = 10000 65535
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.tcp_syncookies = 1
net.ipv4.ipfrag_high_thresh = 41943040
net.ipv4.ipfrag_low_thresh = 31457280
net.ipv4.ipfrag_time = 60
net.ipv4.ip_local_reserved_ports=65330
vm.overcommit_memory = 2
vm.overcommit_ratio = 95
vm.swappiness = 10
vm.dirty_expire_centisecs = 500
vm.dirty_writeback_centisecs = 100
vm.zone_reclaim_mode = 0
EOF

    RAM_IN_KB=$(awk '/MemTotal/{print $2}' /proc/meminfo)
    RAM_IN_BYTES=$((RAM_IN_KB * 1024))
    {
        echo "vm.min_free_kbytes = $((RAM_IN_BYTES * 3 / 100 / 1024))"
        echo "kernel.shmall = $((RAM_IN_BYTES / 2 / 4096))"
        echo "kernel.shmmax = $((RAM_IN_BYTES / 2))"
        if [ "$RAM_IN_BYTES" -le $((64 * 1024 * 1024 * 1024)) ]; then
            echo "vm.dirty_background_ratio = 3"
            echo "vm.dirty_ratio = 10"
        else
            echo "vm.dirty_background_ratio = 0"
            echo "vm.dirty_ratio = 0"
            echo "vm.dirty_background_bytes = 1610612736 # 1.5GB"
            echo "vm.dirty_bytes = 4294967296 # 4GB"
        fi
    } >> /etc/sysctl.d/10-gpdb.conf

    sysctl -p /etc/sysctl.d/10-gpdb.conf

    # Configure system limits
    cat >> /etc/security/limits.d/10-nproc.conf << EOF
* soft nofile 524288
* hard nofile 524288
* soft nproc 131072
* hard nproc 131072
* soft core unlimited
EOF

fi
