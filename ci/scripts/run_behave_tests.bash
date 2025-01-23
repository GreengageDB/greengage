#!/usr/bin/env bash
set -x -o pipefail

behave_tests_dir="gpMgmt/test/behave/mgmt_utils"

# TODO concourse_cluster tests are not stable
# clusters="concourse_cluster ~concourse_cluster,demo_cluster"

clusters="~concourse_cluster"

if [ $# -eq 0 ]
then
  # TODO cross_subnet and gpssh tests are excluded
  # FIXME! /home/gpadmin/sqldump/dump.sql is required for gpexpand tests
  features=`ls $behave_tests_dir -1 | grep feature | grep -v -E "cross_subnet|gpssh|gpexpand" | sed 's/\.feature$//'`
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

run_feature() {
  local feature=$1
  local cluster=$2
  if [ $cluster = "concourse_cluster" ]; then
    local project="${feature}_concourse"
  else
    local project="${feature}_demo"
  fi
  echo "Started $feature behave tests on cluster $cluster and project $project"
  docker-compose -p $project -f ci/docker-compose.yaml --env-file ci/.env up -d
  # Prepare ALL containers first
  local services=$(docker-compose -p $project -f ci/docker-compose.yaml config --services | tr '\n' ' ')
  for service in $services
  do
    docker-compose -p $project -f ci/docker-compose.yaml exec -T \
      $service bash -c "mkdir -p /data/gpdata && chmod -R 777 /data &&
        source gpdb_src/concourse/scripts/common.bash && install_gpdb &&
       ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash" &
  done
  wait

  # Add host keys to known_hosts after containers setup
  for service in $services
  do
    docker-compose -p $project -f ci/docker-compose.yaml exec -T \
      $service bash -c "ssh-keyscan ${services/$service/} >> /home/gpadmin/.ssh/known_hosts" &
  done
  wait

  docker-compose -p $project -f ci/docker-compose.yaml exec -T \
    -e FEATURE="$feature" -e BEHAVE_FLAGS="--tags $feature --tags=$cluster \
      -f behave_utils.ci.formatter:CustomFormatter \
      -o non-existed-output \
      -f allure_behave.formatter:AllureFormatter \
      -o /tmp/allure-results"  \
    cdw gpdb_src/ci/scripts/behave_gpdb.bash
  status=$?

  docker-compose -p $project -f ci/docker-compose.yaml --env-file ci/.env down -v

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
