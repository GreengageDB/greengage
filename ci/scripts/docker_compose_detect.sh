#!/bin/bash
# Docker Compose binary check
flags=$- # seve flags
set +e
docker compose version  >/dev/null 2>&1; exist=$? # Preffered
if [ $exist -eq "0" ]; then
  DOCKER_COMPOSE='docker compose'
else
  docker-compose version  >/dev/null 2>&1; exist=$? # Legacy
  if [ $exist -eq "0" ]; then
    DOCKER_COMPOSE='docker-compose'
  else
    echo No Docker Compose detected. Exiting # Absent
    exit 1
  fi
fi
export DOCKER_COMPOSE
[ -n "$DEBUG" ] && echo Docker Compose binary detected: \"$DOCKER_COMPOSE\"

set -$(sed -E 's/[^abefhkmnptuvxBCHP]//g' <<<$flags) ; unset flags # load saved flags
