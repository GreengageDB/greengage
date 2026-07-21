# TPC-DS runner image: the Greengage image under test + the TPC-DS harness.
#
# GREENGAGE_IMAGE is built from ci/Dockerfile.ubuntu on the branch you benchmark,
# e.g.  docker build -t greengage8_u22:claude-merge-7 -f ci/Dockerfile.ubuntu .
# It already contains /home/gpadmin/gpdb_src (source + concourse scripts +
# gpAux/gpdemo) and /home/gpadmin/bin_gpdb (compiled binaries), plus gcc/make.
ARG GREENGAGE_IMAGE
FROM ${GREENGAGE_IMAGE}

# Pin the TPC-DS harness. Override with --build-arg TPCDS_REF=<sha> for repeatability.
ARG TPCDS_REPO=https://github.com/dimoffon/TPC-DS.git
ARG TPCDS_REF=master

USER root

# The harness needs bc (score arithmetic) and openssh-client (ssh-to-self);
# gcc/make (for dsdgen) come from the base image.
RUN set -eux; \
    export DEBIAN_FRONTEND=noninteractive; \
    (command -v bc >/dev/null && command -v ssh >/dev/null) || \
      (apt-get update && apt-get install -y --no-install-recommends bc openssh-client && rm -rf /var/lib/apt/lists/*)

# Clone the harness and apply the GreengageDB-8 compatibility overlay.
RUN git clone "${TPCDS_REPO}" /opt/TPC-DS && \
    git -C /opt/TPC-DS checkout "${TPCDS_REF}"
COPY patches/ /opt/tpcds-patches/
RUN bash /opt/tpcds-patches/apply.sh /opt/TPC-DS

# Driver scripts (bring up cluster, run harness, aggregate results).
COPY scripts/ /opt/tpcds-scripts/

# Production base images (built from ci/Dockerfile.ubuntu with no-cassert flags)
# do not create the gpadmin user — bringup_cluster.sh's setup_gpadmin_user.bash
# creates it at runtime and is idempotent (it skips creation if gpadmin exists).
# Create JUST the user now so the build-time chown below works. Do NOT create the
# `supergroup` group here: the runtime setup runs a bare `groupadd supergroup`
# under `set -e`, which would fail if it already existed. Leaving it to runtime
# keeps that path working; gpadmin doesn't need supergroup for the benchmark.
# No-op if the base already has gpadmin.
RUN id gpadmin >/dev/null 2>&1 || ( useradd -m -u 1000 -s /bin/bash gpadmin && \
      echo "gpadmin ALL = NOPASSWD : ALL" >> /etc/sudoers )

RUN chmod -R +x /opt/tpcds-scripts /opt/tpcds-patches && \
    find /opt/TPC-DS -name '*.sh' -exec chmod +x {} + && \
    chown -R gpadmin:gpadmin /opt/TPC-DS

WORKDIR /home/gpadmin
