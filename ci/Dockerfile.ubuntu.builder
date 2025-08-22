FROM ubuntu:22.04

COPY README.ubuntu.bash ./
RUN set -eux; \
    ./README.ubuntu.bash; \
    rm README.ubuntu.bash; \
    ln -s python2 /usr/bin/python;

WORKDIR /home/gpadmin
