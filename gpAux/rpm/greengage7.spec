# Run command: `make -C ./gpdb_src/gpAux pkg-rpm`

%{!?gproot: %global gproot /opt/greengagedb}
%{!?gpdir:  %global gpdir  greengage7}
%global     prefix %{gproot}/%{gpdir}

# Scripts use `#!/usr/bin/env <interpreter>' for portability
%global __brp_mangle_shebangs_exclude_from ^%{prefix}/.*$

Name:    greengage7
Version: %{gpdb_version}
Release: %{gpdb_release}%{?dist}
Summary: Greengage MPP database engine
Group:   Applications/Databases
License: ASL 2.0
URL:     https://greengagedb.org

Requires: findutils
Requires: glibc-langpack-en
Requires: iproute
Requires: iputils
Requires: less
Requires: net-tools
Requires: openssh-clients
Requires: openssh-server
Requires: openssl
Requires: procps-ng
Requires: rsync
Requires: zip

Requires: python3
Requires: python3-pip
Requires: python3-psutil
Requires: python3-pyyaml
Requires: python3-psycopg2
Requires: python3-setuptools

Conflicts: greengage-loaders

%description
Greengage Database (GPDB) is an advanced, fully featured, open
source data warehouse, based on PostgreSQL. It provides powerful and
rapid analytics on petabyte scale data volumes. Uniquely geared toward
big data analytics, Greengage Database is powered by the world's most
advanced cost-based query optimizer delivering high analytical query
performance on large data volumes.

%install
rm -rf %{buildroot}
env -u CFLAGS -u CPPFLAGS -u CXXFLAGS -u LDFLAGS \
    make dist \
        DESTDIR=%{buildroot} \
        GPROOT=%{gproot} \
        GPDIR=%{gpdir} \
        PARALLEL_MAKE_OPTS=%{?_smp_mflags} \
        -C %{sourcedir}/gpAux

# Remove executable bit from scripts without shebang
find %{buildroot}%{prefix} -type f \
    \( -name "*.py" -o -name "*.pm" -o -name "*.sh" \) \
    -executable | while read f; do \
        head -1 "$f" | grep -q '^#!' || chmod -x "$f"; \
    done
find %{buildroot}%{prefix} -name "*.md" -executable \
    -exec chmod -x {} +

# Remove symbols and debug info from ELF files
find %{buildroot}%{prefix} -type f \( -perm -u+x -o -name "*.so*" \) \
    -exec strip --strip-unneeded {} \; 2>/dev/null

%files
%{prefix}

%changelog
* %(LC_ALL=C date "+%%a %%b %%d %%Y") Greengage Team - %{version}-%{release}
- Build the package from the source tree

