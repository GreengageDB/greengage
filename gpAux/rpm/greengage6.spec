# Run command: `make -C ./gpdb_src/gpAux pkg-rpm`

%{!?gproot: %global gproot /opt/greengagedb}
%{!?gpdir:  %global gpdir  greengage6}
%global     prefix %{gproot}/%{gpdir}

# bin/, sbin/, lib/python/ use bare 'python' interpreter intentionally.
# On Rocky 8, python -> python2; on Rocky 9, python -> python3.11.
# Perl test scripts use /usr/bin/env perl for portability across environments
# where Perl is not at /usr/bin/perl — see commit:eab7246a65e3c7834f8d4763d73972e4a6c0fcbd
%global __brp_mangle_shebangs_exclude_from ^%{prefix}/.*$

Name:    greengage6
Version: %{gpdb_version}
Release: %{gpdb_release}%{?dist}
Summary: Greengage MPP database engine
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

%if 0%{?rhel} >= 9
Requires: python3
Requires: python3-pip
%else
Requires: python2
Requires: python3
Requires: python2-pip
Requires: python3-pip
%endif

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

# Remove debug info from *.so
find %{buildroot}%{prefix} -type f -name "*.so" -exec strip --strip-debug {} \;

%files
%{prefix}

%changelog
