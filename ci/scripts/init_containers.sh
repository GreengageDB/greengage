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
      ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &
done
wait

docker container ps

# Add host keys to known_hosts after containers setup
for service in $services
do
  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    $service bash -c "
    if [[ ${service} == "cdw" || ${service} == "sdw5" || ${service} == "sdw6" ]]; then
      ssh -Q HostKeyAlgorithms; 
    fi

    if [[ ${service} == "cdw" ]]; then
      date; 
      ssh-keyscan -vvv sdw1 1> >(tee -a /home/gpadmin/.ssh/known_hosts >/dev/null >> combined.txt) 2> >(tee -a /tmp/allure-results/combined1.txt >/dev/null);
      echo "Pause"
      ssh-keyscan -vvv sdw2 1> >(tee -a /home/gpadmin/.ssh/known_hosts >/dev/null >> combined.txt) 2> >(tee -a /tmp/allure-results/combined2.txt >/dev/null);
      echo "Pause"
      ssh-keyscan -vvv sdw3 1> >(tee -a /home/gpadmin/.ssh/known_hosts >/dev/null >> combined.txt) 2> >(tee -a /tmp/allure-results/combined3.txt >/dev/null);
      echo "Pause"
      ssh-keyscan -vvv sdw4 1> >(tee -a /home/gpadmin/.ssh/known_hosts >/dev/null >> combined.txt) 2> >(tee -a /tmp/allure-results/combined4.txt >/dev/null);
      echo "Pause"
      ssh-keyscan -vvv sdw5 sdw6 1> >(tee -a /home/gpadmin/.ssh/known_hosts >/dev/null >> combined.txt) 2> >(tee -a /tmp/allure-results/combined5.txt >/dev/null);
      date; 
      cat /home/gpadmin/.ssh/known_hosts; 
    else 
      ssh-keyscan ${services/$service/} >> /home/gpadmin/.ssh/known_hosts; 
    fi
    
    if [[ ${service} == "cdw" || ${service} == "sdw5" || ${service} == "sdw6" ]]; then
      journalctl -u ssh &> /logs/journalssh_${service}.txt
      journalctl -u sshd &> /logs/journalsshd_${service}.txt
      cp /var/log/auth.log /logs/auth_${service}.log
      cp /var/log/secure /logs/secure_${service}
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
