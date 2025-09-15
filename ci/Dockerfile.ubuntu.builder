FROM ubuntu:22.04

COPY README.ubuntu.bash ./
RUN set -eux; \
    ./README.ubuntu.bash; \
    rm README.ubuntu.bash; \
    ln -s python2 /usr/bin/python;
# Create gpadmin user and add the user to the sudoers
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd -g ${USER_GID} gpadmin && \
    useradd -m -u ${USER_UID} -g gpadmin gpadmin && \
    echo "gpadmin ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
WORKDIR /home/gpadmin
USER gpadmin
