#!/bin/bash
[ -z "$DOCKER_COMPOSE" ] && source ${MY_PATH:=$(cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P)}/docker_compose_detect.sh

set -eo pipefail

project="$1"

shift

$DOCKER_COMPOSE -p $project -f ci/docker-compose.yaml --env-file ci/.env up -d $@

if [[ $# -eq 0 ]]; then
  services=$($DOCKER_COMPOSE -p $project -f ci/docker-compose.yaml config --services | tr '\n' ' ')
else
  services="$@"
fi

# Prepare ALL containers first
for service in $services
do
  $DOCKER_COMPOSE -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "mkdir -p /data/gpdata && chmod -R 777 /data &&
      source gpdb_src/concourse/scripts/common.bash && install_gpdb &&
      ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &
done
wait

# Add host keys to known_hosts after containers setup
for service in $services
do
  $DOCKER_COMPOSE -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "ssh-keyscan ${services/$service/} >> /home/gpadmin/.ssh/known_hosts" &
done
wait
