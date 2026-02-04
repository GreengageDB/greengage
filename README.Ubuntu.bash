#!/bin/bash
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
	bison \
	ccache \
	clang \
	cmake \
	curl \
	fakeroot \
	flex \
	g++ \
	gcc \
	git-core \
	inetutils-ping \
	iproute2 \
	krb5-admin-server \
	krb5-kdc \
	libapr1-dev \
	libbz2-dev \
	libcurl4-gnutls-dev \
	libevent-dev \
	libipc-run-perl \
	libkrb5-dev \
	libldap-common \
	libldap-dev \
	libpam-dev \
	libperl-dev \
	libreadline-dev \
	libssl-dev \
	libuv1-dev \
	libxerces-c-dev \
	libxml2-dev \
	libyaml-dev \
	libzstd-dev \
	llvm \
	locales \
	lsof \
	net-tools \
	ninja-build \
	openssh-client \
	openssh-server \
	openssl \
	pkg-config \
	protobuf-compiler \
	python3.11 \
	python3.11-dev \
	python3-dev \
	python3-pip \
	python3-psutil \
	python3-psycopg2 \
	python3-yaml \
	rsync \
	sudo \
	zlib1g-dev

tee -a /etc/sysctl.conf << EOF
kernel.shmmax = 5000000000000
kernel.shmmni = 32768
kernel.shmall = 40000000000
kernel.sem = 1000 32768000 1000 32768
kernel.msgmnb = 1048576
kernel.msgmax = 1048576
kernel.msgmni = 32768

net.core.netdev_max_backlog = 80000
net.core.rmem_default = 2097152
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216

vm.overcommit_memory = 2
vm.overcommit_ratio = 95
EOF

sysctl -p

mkdir -p /etc/security/limits.d
tee -a /etc/security/limits.d/90-greengage.conf << EOF
* soft nofile 1048576
* hard nofile 1048576
* soft nproc 1048576
* hard nproc 1048576
EOF

ulimit -n 65536 65536
