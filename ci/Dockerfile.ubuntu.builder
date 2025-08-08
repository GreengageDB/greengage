FROM ubuntu:22.04

COPY README.ubuntu.bash ./
RUN set -eux; \
    ./README.ubuntu.bash; \
    rm README.ubuntu.bash; \
    ln -s python2 /usr/bin/python; \
# Install dependencies for deb package building
    DEBIAN_FRONTEND=noninteractive \
        apt install -y build-essential devscripts debhelper dh-python ccache ninja-build python-pip; \
    rm -rf /var/lib/apt/lists/*;

WORKDIR /home/gpadmin
