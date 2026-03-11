#!/bin/bash

# Install needed packages. Please add to this list if you discover additional prerequisites.
sudo yum install -y epel-release
sudo yum install -y \
    apr-devel \
    bison \
    bzip2-devel \
    cmake3 \
    flex \
    gcc \
    gcc-c++ \
    krb5-devel \
    libcurl-devel \
    libevent-devel \
    libkadm5 \
    libtool \
    libuuid-devel \
    libuv-devel \
    libxml2-devel \
    libxslt-devel \
    libyaml-devel \
    libzstd-devel \
    libzstd-static \
    net-tools \
    openldap-devel \
    openssl \
    openssl-devel \
    pam-devel \
    perl-Env \
    perl-ExtUtils-Embed \
    python-devel \
    python-pip \
    readline-devel \
    xerces-c-devel \
    zlib-devel
