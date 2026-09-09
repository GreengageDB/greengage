#!/usr/bin/env bash
set -euo pipefail

arrow_package_version="${ARROW_FLIGHT_PACKAGE_VERSION:-}"

install_ubuntu_deps() {
	local distro_id distro_codename arrow_source_deb

	apt-get update
	apt-get install -y -V \
		ca-certificates \
		libprotobuf-dev \
		lsb-release \
		pkg-config \
		protobuf-compiler \
		wget

	distro_id="$(lsb_release --id --short | tr 'A-Z' 'a-z')"
	distro_codename="$(lsb_release --codename --short)"
	arrow_source_deb="/tmp/apache-arrow-apt-source-latest-${distro_codename}.deb"

	wget -q -O "${arrow_source_deb}" \
		"https://packages.apache.org/artifactory/arrow/${distro_id}/apache-arrow-apt-source-latest-${distro_codename}.deb"
	apt-get install -y -V "${arrow_source_deb}"
	apt-get update

	if [ -n "${arrow_package_version}" ]; then
		apt-get install -y -V \
			"libarrow-dev=${arrow_package_version}" \
			"libarrow-flight-dev=${arrow_package_version}" \
			"libarrow-flight-sql-dev=${arrow_package_version}"
	else
		apt-get install -y -V \
			libarrow-dev \
			libarrow-flight-dev \
			libarrow-flight-sql-dev
	fi

	rm -f "${arrow_source_deb}"
	rm -rf /var/lib/apt/lists/*
}

install_rhel_deps() {
	local major_version

	major_version="$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)"

	dnf install -y \
		'dnf-command(config-manager)' \
		pkgconf-pkg-config \
		protobuf-compiler \
		protobuf-devel
	dnf install -y epel-release || \
		dnf install -y "https://dl.fedoraproject.org/pub/epel/epel-release-latest-${major_version}.noarch.rpm"
	dnf install -y "https://packages.apache.org/artifactory/arrow/almalinux/${major_version}/apache-arrow-release-latest.rpm"
	dnf config-manager --set-enabled epel || :
	dnf config-manager --set-enabled powertools || :
	dnf config-manager --set-enabled crb || :

	if [ -n "${arrow_package_version}" ]; then
		dnf install -y \
			"arrow-devel-${arrow_package_version}" \
			"arrow-flight-devel-${arrow_package_version}" \
			"arrow-flight-sql-devel-${arrow_package_version}"
	else
		dnf install -y \
			arrow-devel \
			arrow-flight-devel \
			arrow-flight-sql-devel
	fi

	dnf clean all
}

if command -v apt-get >/dev/null 2>&1; then
	install_ubuntu_deps
elif command -v dnf >/dev/null 2>&1; then
	install_rhel_deps
else
	echo "unsupported package manager: expected apt-get or dnf" >&2
	exit 1
fi

pkg-config --exists arrow arrow-flight arrow-flight-sql protobuf
