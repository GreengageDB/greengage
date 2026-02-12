#!/bin/bash
# Some packages, for example KRB5, not installing properly without this option
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
  bison \
  build-essential \
  cmake \
  curl \
  debhelper \
  devscripts \
  dh-python \
  fakeroot \
  flex \
  g++ \
  gcc \
  git \
  iproute2 \
  iputils-ping \
  krb5-admin-server \
  krb5-kdc \
  libapr1-dev \
  libaprutil1-dev \
  libbz2-dev \
  libcurl4-openssl-dev \
  libevent-dev \
  libipc-run-perl \
  libkrb5-dev \
  libpam-dev \
  libperl-dev \
  libreadline-dev \
  libssl-dev \
  libtool \
  libuv1-dev \
  libxerces-c-dev \
  libxml2-dev \
  libxslt-dev \
  libyaml-dev \
  libzstd-dev \
  locales \
  net-tools \
  openssh-client \
  openssh-server \
  pkg-config \
  protobuf-compiler \
  python3-dev \
  rsync \
  sudo \
  zlib1g-dev

if [ "$(lsb_release -si)" == "Ubuntu" ] && [ "$(lsb_release -sr)" == "22.04" ]; then
  apt-get install -y \
    python-pip \
    python2 \
    python2-dev
  python2 -m pip install future==0.16
else
  apt-get install -y \
    python3-pip \
    python-is-python3;
  python -m pip install future==1.0.0
fi
