#!/bin/bash

sudo dnf -y update
sudo dnf -y install epel-release
sudo dnf -y install 'dnf-command(config-manager)'
sudo dnf config-manager --set-enabled devel
sudo dnf makecache

sudo dnf -y install\
    apr-devel \
    bison \
    bzip2-devel \
    cmake3 \
    flex \
    gcc \
    gcc-c++ \
    iproute \
    krb5-devel \
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
    openssl \
    openssl-devel \
    pam-devel \
    perl-Env \
    perl-ExtUtils-Embed \
    perl-IPC-Run \
    perl-JSON \
    perl-Test-Base \
    procps-ng \
    python2-devel \
    python2-pip \
    readline-devel \
    snappy-devel \
    xerces-c-devel \
    zlib-devel
