#!/bin/bash
set -eo pipefail

project="$1"

shift

docker compose -p $project -f ci/docker-compose.yaml --env-file ci/.env up -d $@

if [[ $# -eq 0 ]]; then
  services=$(docker compose -p $project -f ci/docker-compose.yaml config --services | tr '\n' ' ')
else
  services="$@"
fi

# Prepare ALL containers first
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "mkdir -p /data/gpdata && chmod -R 777 /data &&
      source gpdb_src/concourse/scripts/common.bash && install_gpdb &&
      ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &
done
wait

# Add host keys to known_hosts after containers setup
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "ssh-keyscan ${services/$service/} >> /home/gpadmin/.ssh/known_hosts" &
done

# Add ip and host to /etc/hosts
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "for HOST in \"cdw sdw1 sdw2 sdw3 sdw4 sdw5 sdw6\"; do echo \"\$(host \"\$HOST\" | grep 'has address' | head -n 1 | cut -d ' ' -f 4) \$HOST\" >>/etc/hosts; done" &
done
wait
