#!/bin/bash
# Some packages, for example KRB5, not installing properly without this option
export DEBIAN_FRONTEND=noninteractive

set -eo pipefail

apt-get update
apt-get install -y \
	bison \
	build-essential \
	ccache \
	clang \
	cmake \
	curl \
	debhelper \
	devscripts \
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
	python3-dev \
	python3-pip \
	python3-psutil \
	python3-psycopg2 \
	python3-yaml \
	rsync \
	sudo \
	zlib1g-dev

curl -fsSL --proto '=https' --proto-redir '=https' \
	https://greengagedb.org/repositories/gpg | \
	gpg --batch --yes --dearmor -o /etc/apt/keyrings/greengagedb.gpg
echo "deb [signed-by=/etc/apt/keyrings/greengagedb.gpg] \
	https://greengagedb.org/repositories/ubuntu/$(lsb_release -sr)/x86_64 \
	greengagedb main" \
	| tee /etc/apt/sources.list.d/greengagedb.list
