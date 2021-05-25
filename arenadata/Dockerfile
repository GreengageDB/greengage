FROM pivotaldata/centos-gpdb-dev:7-gcc6.2-llvm3.7 as base

RUN rm -rf /opt/cmake* \
    && yum remove -y cmake \
    && yum install -y epel-release \
    && yum install -y cmake3 \
                    libzstd-devel \
                    apr-util-devel \
                    libuv-devel \
                    perl-IPC-Run \
                    perl-Test-Base \
                    wget \
    && yum clean all \
    && ln -s /usr/bin/cmake3 /usr/bin/cmake \
    && ln -s /usr/bin/ctest3 /usr/bin/ctest

WORKDIR /home/gpadmin

FROM base as build

ARG sigar=https://ci.arenadata.io/artifactory/ADB/6.7.1_arenadata4/centos/7/community/x86_64/sigar-1.6.5-163.el7.x86_64.rpm
ARG sigar_headers=https://ci.arenadata.io/artifactory/ADB/6.7.1_arenadata4/centos/7/community/x86_64/sigar-headers-1.6.5-163.el7.x86_64.rpm

COPY . gpdb_src

RUN rpm -i $sigar && rpm -i $sigar_headers && mkdir /home/gpadmin/bin_gpdb

ENV TARGET_OS_VERSION=7
ENV TARGET_OS=centos
ENV OUTPUT_ARTIFACT_DIR=bin_gpdb
ENV CONFIGURE_FLAGS="--enable-debug-extensions --with-gssapi --enable-cassert --enable-debug --enable-depend" 

# Compile with running mocking tests
RUN source /opt/gcc_env.sh && \
    bash /home/gpadmin/gpdb_src/concourse/scripts/compile_gpdb.bash

FROM base as code
COPY . gpdb_src
RUN rm -rf gpdb_src/.git/

FROM base as test
COPY --from=code /home/gpadmin/gpdb_src gpdb_src
COPY --from=build /home/gpadmin/bin_gpdb /home/gpadmin/bin_gpdb

# Volume for tests output
VOLUME /home/gpadmin/gpdb_src/src/test

