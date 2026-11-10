#!/usr/bin/env bash
set -x -o pipefail

rm -rf ./ssh_keys
mkdir -p ./ssh_keys
if [ ! -e "./ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ./ssh_keys/id_rsa
fi

docker-compose -f ./ci/patroni/docker-compose-patroni.yaml up -d

# Prepare ALL containers first
services=$(docker-compose -f ./ci/patroni/docker-compose-patroni.yaml config --services | grep -vx -e etcd1 -e etcd2 -e etcd3 | tr '\n' ' ')
for service in $services
do
  docker-compose -f ./ci/patroni/docker-compose-patroni.yaml exec -T \
    $service bash -c "mkdir -p /data/gpdata && chmod -R 777 /data &&
      source gpdb_src/concourse/scripts/common.bash && install_gpdb &&
     ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &
done
sleep 5
wait

# Add host keys to known_hosts.
pids=()
for service in $services
do
  peers=$(echo "$services" | tr ' ' '\n' | grep -vx "$service")
  docker-compose -f ./ci/patroni/docker-compose-patroni.yaml exec -T \
    $service bash -c '
      peers="$1"

      # 1. Wait for sshd to accept TCP connections on every peer.
      for h in $peers; do
        ready=0
        for i in $(seq 60); do
          if (exec 3<>/dev/tcp/"$h"/22) 2>/dev/null; then
            ready=1
            break
          fi
          sleep 1
        done
        if [ "$ready" -ne 1 ]; then
          echo "sshd on $h not reachable after 60s" >&2
          exit 1
        fi
      done

      # 2. Fetch host keys with per-host retries.
      for h in $peers; do
        if ssh-keygen -F "$h" -f /home/gpadmin/.ssh/known_hosts >/dev/null 2>&1; then
          continue
        fi
        ok=0
        for attempt in 1 2 3 4 5; do
          if ssh-keyscan -T 10 "$h" >> /home/gpadmin/.ssh/known_hosts 2>/dev/null \
             && ssh-keygen -F "$h" -f /home/gpadmin/.ssh/known_hosts >/dev/null 2>&1; then
            ok=1
            break
          fi
          sleep 2
        done
        if [ "$ok" -ne 1 ]; then
          echo "ERROR: failed to record host key for $h" >&2
          exit 1
        fi
      done
    ' _ "$peers" &
  pids+=($!)
done

# Fail the whole script if any container could not populate its known_hosts.
failed=0
for p in "${pids[@]}"; do
  wait "$p" || failed=1
done
if [ "$failed" -ne 0 ]; then
  echo "ERROR: host key setup failed on one or more containers" >&2
  exit 1
fi

# Run the apt install as root
docker-compose -f ./ci/patroni/docker-compose-patroni.yaml exec -T cdw bash -c "apt update && apt install -y etcd-client"

docker-compose -f ./ci/patroni/docker-compose-patroni.yaml exec -T -u gpadmin cdw bash <<'PATRONI_ETCD3'
set -e
cd /home/gpadmin/patroni
pip install .[etcd3]

ssh scdw 'cd /home/gpadmin/patroni && pip install ".[etcd3]"'
PATRONI_ETCD3

docker-compose -f ./ci/patroni/docker-compose-patroni.yaml exec -u gpadmin \
  -e HOME=/home/gpadmin cdw bash

docker-compose -f ./ci/patroni/docker-compose-patroni.yaml down -v
