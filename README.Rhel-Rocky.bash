#!/bin/bash
# FILE:    README.Rhel-Rocky.bash
# CONTEXT: Called from ci/Dockerfile.rockylinux for Greengage build
# PURPOSE: Install build dependencies, compile zstd static library,
#          Install Python based on OS version

set -euxo pipefail

dnf -y install epel-release

# Detect OS version if not already set
export OS_VERSION="${OS_VERSION:-$(grep -oP '(?<= release )\d+' /etc/redhat-release)}"

perl_packages="perl-Env perl-ExtUtils-Embed perl-IPC-Run perl-JSON perl-Test-Base"
python_packages="python3 python3-devel python3-setuptools python3-pip python3-future"

case "$OS_VERSION" in
    8)
        dnf config-manager --set-enabled powertools
        python_packages="python2 python2-devel python2-setuptools python2-pip $python_packages"
        ;;
    9)
        dnf config-manager --set-enabled crb
        perl_packages="$perl_packages perl-Opcode perl-Test-Simple perl-Thread-Queue perl-devel"
        ;;
    *)
        echo "Unsupported Rocky Linux version: $OS_VERSION"
        exit 1
        ;;
esac
# shellcheck disable=SC2086 # intentional: word splitting for package lists
dnf -y install \
    apr-devel \
    apr-util-devel \
    autoconf \
    bison \
    bzip2-devel \
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
