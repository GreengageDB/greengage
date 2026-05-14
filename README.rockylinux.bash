#!/bin/bash
# FILE:    README.rockylinux.bash
# CONTEXT: Called from ci/Dockerfile.rockylinux for Greengage build
# PURPOSE: Install build dependencies, compile zstd static library

set -eux

dnf -y install epel-release
dnf config-manager --set-enabled powertools

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
    net-tools \
    openldap-devel \
    openssh-server \
    openssl-devel \
    pam-devel \
    perl-Env \
    perl-ExtUtils-Embed \
    perl-IPC-Run \
    perl-JSON \
    perl-Test-Base \
    procps-ng \
    python2 \
    python2-devel \
    python2-setuptools \
    readline-devel \
    rsync \
    snappy-devel \
    sudo \
    time \
    unzip \
    vim \
    wget \
    xerces-c-devel \
    zlib-devel

# Build zstd with static library (not available as a package on Rocky)
curl -Ls https://github.com/facebook/zstd/releases/download/v1.4.4/zstd-1.4.4.tar.gz | tar -xzf -
make -j"$(nproc)" -C zstd-1.4.4
make install PREFIX=/usr/local -C zstd-1.4.4
rm -rf zstd-1.4.4
