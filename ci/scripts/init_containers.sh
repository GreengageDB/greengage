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

# Prepare ALL containers first
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "mkdir -p /data/gpdata && chmod -R 777 /data &&
      # each host should have its own copy of the (initially identical) files in .ssh
      cp -rf .ssh.src .ssh &&
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
wait

# Add ip and host names of all cluster nodes to /etc/hosts
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "for HOST in $services; do echo \"\$(host \"\$HOST\" | grep 'has address' | head -n 1 | cut -d ' ' -f 4) \$HOST\" >>/etc/hosts; done" &
done
wait

# Add host names of all cluster nodes to /tmp/hostfile_all for coverage collect
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "for HOST in $services; do echo \"\$HOST\" >>/tmp/hostfile_all; done" &
done
wait

# Setup coverage
#TODO remove hard-coded links
for service in $services; do
  docker compose -p "$project" -f ci/docker-compose.yaml exec -T \
    "$service" bash -c "
      cat > /usr/local/greengage-db-devel/lib/python/sitecustomize.py <<INNER_EOF
import coverage
coverage.process_startup()
INNER_EOF
      echo 'export COVERAGE_PROCESS_START=gpdb_src/gpMgmt/test/coveragerc_behave' >> /usr/local/greengage-db-devel/greengage_path.sh 
    "
done &
wait
