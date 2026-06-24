#!/bin/bash
set -eox pipefail

project="$1"

shift

docker compose -p $project -f ci/docker-compose.yaml --env-file ci/.env up -d $@

if [[ $# -eq 0 ]]; then
  services=$(docker compose -p $project -f ci/docker-compose.yaml config --services | tr '\n' ' ')
else
  services="$@"
fi

docker container ps

# Prepare ALL containers first
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "mkdir -p /data/gpdata && chmod -R 777 /data &&
      # each host should have its own copy of the (initially identical) files in .ssh
      cp -rf .ssh.src .ssh &&
      source gpdb_src/concourse/scripts/common.bash && install_gpdb &&
      ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash"
done
wait

docker container ps

# Add host keys to known_hosts after containers setup
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "
    
    if [[ ${service} == "cdw" ]]; then
      date; 
      ssh-keyscan -v ${services/$service/} 1> >(tee /home/gpadmin/.ssh/known_hosts >/dev/null >> combined.txt) 2> >(tee -a /tmp/allure-results/combined.txt >/dev/null); wait
      date; 
      cat /home/gpadmin/.ssh/known_hosts; 
    else 
      ssh-keyscan ${services/$service/} >> /home/gpadmin/.ssh/known_hosts; 
    
    fi
    
    " &
done
wait

docker container ps

# Add ip and host names of all cluster nodes to /etc/hosts
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "for HOST in $services; do echo \"\$(host \"\$HOST\" | grep 'has address' | head -n 1 | cut -d ' ' -f 4) \$HOST\" >>/etc/hosts; done" &
done
wait
