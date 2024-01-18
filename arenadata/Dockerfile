FROM centos:centos7 as base

# Install some basic utilities and build tools
RUN yum makecache && yum update -y ca-certificates && \
    rpm --import https://mirror.yandex.ru/centos/RPM-GPG-KEY-CentOS-7 && \
    yum -y install epel-release java-11-openjdk-devel && \
    yum -y install git iproute net-tools openssh-server rsync sudo time vim wget unzip \
                   autoconf bison cmake3 flex gperf indent jq libtool gcc-c++ \
                   krb5-server krb5-workstation xerces-c-devel file && \
    yum clean all

# install all software we need
RUN yum makecache && \
    yum -y install python3-devel && \
    yum -y install apr-devel bzip2-devel expat-devel libcurl-devel && \
    yum -y install libevent-devel libuuid-devel libxml2-devel libyaml-devel libzstd-devel && \
    yum -y install openssl-devel pam-devel readline-devel snappy-devel libuv-devel && \
    yum -y install libicu perl-ExtUtils-Embed perl-Env perl-JSON && \
    yum -y install perl-IPC-Run perl-Test-Base libxslt-devel openldap-devel && \
    yum clean all

# setup ssh configuration
RUN ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 0600 /root/.ssh/authorized_keys && \
    echo -e "password\npassword" | passwd 2> /dev/null && \
    { ssh-keyscan localhost; ssh-keyscan 0.0.0.0; } >> /root/.ssh/known_hosts && \
    #
    ssh-keygen -f /etc/ssh/ssh_host_key -N '' -t rsa1 && \
    ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa && \
    ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa && \
    sed -i -e 's|Defaults    requiretty|#Defaults    requiretty|' /etc/sudoers && \
    sed -ri 's/UsePAM yes/UsePAM no/g;s/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config && \
    sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@;s@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config

# newer version of gcc and run environment for gpdb
RUN yum -y install centos-release-scl && \
    yum -y install --nogpgcheck devtoolset-7-gcc devtoolset-7-gcc-c++ \
    llvm-toolset-7 llvm-toolset-7-llvm-devel && yum clean all && \
    ln -s /usr/bin/cmake3 /usr/bin/cmake && \
    ln -s /usr/bin/ctest3 /usr/bin/ctest && \
    echo -e 'source /opt/rh/devtoolset-7/enable' >> /opt/gcc_env.sh && \
    echo -e '#!/bin/sh' >> /etc/profile.d/jdk_home.sh && \
    echo -e 'export JAVA_HOME=/etc/alternatives/java_sdk' >> /etc/profile.d/jdk_home.sh && \
    echo -e 'export PATH=$JAVA_HOME/bin:$PATH' >> /etc/profile.d/jdk_home.sh

WORKDIR /home/gpadmin

FROM base as build

COPY . gpdb_src

RUN mkdir /home/gpadmin/bin_gpdb

ENV TARGET_OS_VERSION=7
ENV TARGET_OS=centos
ENV OUTPUT_ARTIFACT_DIR=bin_gpdb
ENV SKIP_UNITTESTS=
# To set default locale for unittests
ENV LANG=en_US.UTF-8
ENV CONFIGURE_FLAGS="--enable-debug-extensions --with-gssapi --enable-cassert --enable-debug --enable-depend"

# Compile with running mocking tests
RUN scl enable devtoolset-7 llvm-toolset-7 'bash /home/gpadmin/gpdb_src/concourse/scripts/compile_gpdb.bash'

FROM base as code
COPY . gpdb_src
RUN rm -rf gpdb_src/.git/

FROM base as test
COPY --from=code /home/gpadmin/gpdb_src gpdb_src
COPY --from=build /home/gpadmin/bin_gpdb /home/gpadmin/bin_gpdb

RUN echo -e '/opt/rh/llvm-toolset-7/root/usr/lib64/\n' >> /etc/ld.so.conf && ldconfig
