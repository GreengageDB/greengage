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

if [ "$(lsb_release -r -s)" = "Ubuntu 24.04.5 LTS"]; then
  USE_PYTHON3_ONLY=1
fi

if [[ -n "$USE_PYTHON3_ONLY" ]]; then
  apt-get install -y \
    python3-pip \
    python-is-python3;
  PIP_FLAGS="--break-system-packages";
else
  apt-get install -y \
    python-pip \
    python2 \
    python2-dev
  if [ -n "$SET_PYTHON2_DEFAULT" ]; then
    ln -s python2 /usr/bin/python;
  fi
  PIP_FLAGS=""
fi

# Install allure-behave for behave tests
if [ -n "$INSTALL_ALLURE_BEHAVE" ]; then
  python -m pip install --no-cache-dir $PIP_FLAGS allure-behave==2.4.0;
fi
