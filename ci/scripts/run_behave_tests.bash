#!/usr/bin/env bash
set -x -o pipefail

behave_tests_dir="gpMgmt/test/behave/mgmt_utils"

# TODO concourse_cluster tests are not stable
# clusters="concourse_cluster ~concourse_cluster,demo_cluster"

clusters="~concourse_cluster"

if [ $# -eq 0 ]
then
  # TODO cross_subnet and gpssh tests are excluded
  features=`ls $behave_tests_dir -1 | grep feature | grep -v -E "cross_subnet|gpssh" | sed 's/\.feature$//'`
else
  for feature in $@
  do
    if [ ! -f "$behave_tests_dir/$feature.feature" ]
    then
      echo "Feature '$feature' doesn't exists"
      exit 1
    fi
  done
  features=$@
fi

processes=3

rm -rf allure-results
mkdir allure-results -pm 777
mkdir ssh_keys -p
if [ ! -e "ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ssh_keys/id_rsa
fi
# id_rsa is bind-mounted into every container (ci/docker-compose.yaml). OpenSSH
# ignores a private key readable by group/other, so ensure 0600 on the host copy
# (a git checkout or some runners can leave it 0644) before it is mounted.
chmod 600 ssh_keys/id_rsa

run_feature() {
  local feature=$1
  local cluster=$2
  if [ $cluster = "concourse_cluster" ]; then
    local project="${feature}_concourse"
  else
    local project="${feature}_demo"
  fi
  echo "Started $feature behave tests on cluster $cluster and project $project"
  docker compose -p $project -f ci/docker-compose.yaml --env-file ci/.env up -d
  # Prepare ALL containers first
  local services=$(docker compose -p $project -f ci/docker-compose.yaml config --services | tr '\n' ' ')
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
  wait

  docker compose -p $project -f ci/docker-compose.yaml exec -T \
    -e FEATURE="$feature" -e BEHAVE_FLAGS="--tags $feature --tags=$cluster \
      -f behave_utils.ci.formatter:CustomFormatter \
      -o non-existed-output \
      -f allure_behave.formatter:AllureFormatter \
      -o /tmp/allure-results"  \
    cdw gpdb_src/ci/scripts/behave_gpdb.bash
  status=$?

  # --- DIAGNOSTIC DUMP on behave failure (PG18 io_worker / sshd resource probe) ---
  # gpinitsystem can fail with "Total processes marked as failed" when the parallel
  # segment bring-up exhausts a per-host resource and sshd auth to gpadmin@cdw is
  # rejected ("Permission denied (publickey,password)").  Capture resource state
  # from every still-running container BEFORE teardown; output lands under
  # allure-results (uploaded as a CI artifact).  Never mutates $status.
  if [[ $status -gt 0 ]]; then
    diagdir="allure-results/diag_${feature}"
    mkdir -p "$diagdir"
    echo "behave FAILED status=$status feature=$feature cluster=$cluster; dumping resource state before teardown" | tee "$diagdir/summary.txt"
    for service in $services; do
      timeout 120 docker compose -p "$project" -f ci/docker-compose.yaml exec -T "$service" bash -c '
        echo "===== date / uname ====="; date; uname -a
        echo "===== free -m ====="; free -m
        echo "===== ulimit -a (current) ====="; ulimit -a
        echo "===== ulimit -a (gpadmin) ====="; su - gpadmin -c "ulimit -a" 2>&1
        echo "===== total procs ====="; ps -e --no-headers 2>/dev/null | wc -l
        echo "===== total tasks/LWP ====="; ps -eL --no-headers 2>/dev/null | wc -l
        echo "===== gpadmin procs ====="; ps --no-headers -u gpadmin 2>/dev/null | wc -l
        echo "===== io worker procs (title-anchored) ====="; ps -eo cmd 2>/dev/null | grep -E "^postgres.*io worker" || echo "(none)"
        echo "===== io worker count ====="; ps -eo cmd 2>/dev/null | grep -Ec "^postgres.*io worker"
        echo "===== system open files (file-nr) ====="; cat /proc/sys/fs/file-nr 2>/dev/null
        echo "===== dmesg tail ====="; dmesg 2>/dev/null | tail -n 80
        echo "===== dmesg OOM/fork grep ====="; dmesg 2>/dev/null | grep -iE "out of memory|oom-kill|killed process|cannot fork|fork failed|Cannot allocate" | tail -n 40
        echo "===== effective sshd auth cfg ====="; grep -REi "usepam|passwordauth|pubkeyauth|maxstartups|logingrace|maxsession|strictmodes" /etc/ssh/sshd_config /etc/ssh/sshd_config.d/ 2>/dev/null
        echo "===== sshd / auth log ====="; { cat /var/log/auth.log 2>/dev/null; journalctl -u ssh -u sshd --no-pager 2>/dev/null; } | tail -n 120
        echo "===== gpinitsystem logs ====="; ls -la /home/gpadmin/gpAdminLogs 2>/dev/null; tail -n 200 /home/gpadmin/gpAdminLogs/gpinitsystem_*.log 2>/dev/null
        echo "===== one segment startup log ====="; f=$(find /data /home/gpadmin \( -path "*/log/startup.log" -o -path "*/pg_log/*.csv" \) 2>/dev/null | head -1); echo "seglog=$f"; tail -n 120 "$f" 2>/dev/null
      ' > "$diagdir/${service}.txt" 2>&1 || echo "diag probe for $service failed (container gone?)" | tee -a "$diagdir/summary.txt"
    done
  fi
  # --- END DIAGNOSTIC DUMP ---

  docker compose -p $project -f ci/docker-compose.yaml --env-file ci/.env down -v

  if [[ $status > 0 ]]; then echo "Feature $feature failed with exit code $status"; fi
  exit $status
}

pids=""
exits=0
for feature in $features
do
  for cluster in $clusters
  do
     run_feature $feature $cluster &
     pids+="$! "
     if [[ $(jobs -r -p | wc -l) -ge $processes ]]; then
        wait -n
        ((exits += $?))
     fi
  done
done
for pid in $pids
  do
    wait $pid
    exits=$((exits + $?))
  done
if [[ $exits > 0 ]]; then exit 1; fi
