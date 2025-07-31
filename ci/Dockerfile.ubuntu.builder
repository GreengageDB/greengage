FROM ubuntu:22.04

COPY README.ubuntu.bash ./
RUN set -eux; \
    ./README.ubuntu.bash; \
    rm README.ubuntu.bash; \
    ln -s python2 /usr/bin/python; \
# The en_US.UTF-8 locale is needed to run GPDB
    locale-gen en_US.UTF-8; \
# To run sshd directly, but not using `/etc/init.d/ssh start`
    mkdir /run/sshd; \
# Alter precedence in favor of IPv4 during resolving
    echo 'precedence ::ffff:0:0/96  100' >> /etc/gai.conf; \
# Install dependencies for deb package building
    DEBIAN_FRONTEND=noninteractive \
        apt install -y build-essential devscripts debhelper dh-python ccache ninja-build python-pip; \
    rm -rf /var/lib/apt/lists/*;

WORKDIR /home/gpadmin
