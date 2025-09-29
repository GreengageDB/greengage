FROM rockylinux:8.8 as base

# Install some basic utilities and build tools
RUN dnf -y install 'dnf-command(config-manager)' && dnf config-manager --set-enabled devel && dnf makecache && dnf update -y ca-certificates && \
    rpm --import https://mirror.yandex.ru/rockylinux/RPM-GPG-KEY-Rocky-8 && \
    dnf -y install epel-release java-11-openjdk-devel && \
    dnf -y install git iproute net-tools openssh-server rsync sudo time vim wget unzip \
                   autoconf bison cmake3 flex gperftools indent jq libtool \
                   krb5-server krb5-workstation xerces-c-devel file netcat passwd lsof xz && \
    dnf clean all

# install all software we need
RUN dnf makecache && \
    dnf -y install python3.11-devel && \
    dnf -y install apr-devel bzip2 bzip2-devel expat-devel libcurl-devel && \
    dnf -y install libevent-devel libuuid-devel libxml2-devel libyaml-devel libzstd-devel && \
    dnf -y install openssl-devel pam-devel readline-devel snappy-devel libuv-devel && \
    dnf -y install libicu perl-ExtUtils-Embed perl-Env perl-JSON && \
    dnf -y install perl-IPC-Run perl-Test-Base libxslt-devel openldap-devel && \
    dnf -y install python3.11-psycopg2 python3.11-pyyaml python3.11-lxml python3.11-pip && \
    dnf -y install llvm-devel clang && \
    dnf -y install protobuf-compiler && \
    dnf clean all && \
    #we install pytest from pypi since the version from the repository does not contain an executable file
    #the rest of the packages are not available in the repository.
    pip3 install pytest gsutil behave~=1.2.6 coverage~=4.5 'mock<=5.0.0' allure-behave psutil && \
    pip3 install protobuf==3.20.0

# setup ssh configuration
RUN ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa && \
    ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa && \
    sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers && \
    sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config && \
    sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config

# run environment for gpdb
RUN \
    echo -e '#!/bin/sh' >> /etc/profile.d/jdk_home.sh && \
    echo -e 'export JAVA_HOME=/etc/alternatives/java_sdk' >> /etc/profile.d/jdk_home.sh && \
    echo -e 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile.d/jdk_home.sh

WORKDIR /home/gpadmin

FROM base as build

COPY . gpdb_src

RUN mkdir /home/gpadmin/bin_gpdb

ENV TARGET_OS_VERSION=8
ENV TARGET_OS=rocky8
ENV OUTPUT_ARTIFACT_DIR=bin_gpdb
ENV SKIP_UNITTESTS=
# To set default locale for unittests
ENV LANG=en_US.UTF-8
ENV CONFIGURE_FLAGS="--enable-debug-extensions --with-gssapi --enable-cassert --enable-debug --enable-depend"

# Compile with running mocking tests
RUN bash /home/gpadmin/gpdb_src/concourse/scripts/compile_gpdb.bash
FROM base as code
COPY . gpdb_src
RUN rm -rf gpdb_src/.git/

FROM base as test
COPY --from=code /home/gpadmin/gpdb_src gpdb_src
COPY --from=build /home/gpadmin/bin_gpdb /home/gpadmin/bin_gpdb
