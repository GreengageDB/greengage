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

run_feature() {
  local feature=$1
  local cluster=$2
  if [ $cluster = "concourse_cluster" ]; then
    local project="${feature}_concourse"
  else
    local project="${feature}_demo"
  fi
  echo "Started $feature behave tests on cluster $cluster and project $project"
  docker-compose -p $project -f arenadata/docker-compose.yaml --env-file arenadata/.env up -d
  docker-compose -p $project -f arenadata/docker-compose.yaml exec -T \
    -e BEHAVE_FLAGS="--tags $feature --tags=$cluster \
      -f behave_utils.arenadata.formatter:CustomFormatter \
      -o non-existed-output \
      -f allure_behave.formatter:AllureFormatter \
      -o /tmp/allure-results"  \
    mdw gpdb_src/arenadata/scripts/behave_gpdb.bash
  docker-compose -p $project -f arenadata/docker-compose.yaml --env-file arenadata/.env down -v
}

for feature in $features
do
  for cluster in $clusters
  do
     run_feature $feature $cluster &

     if [[ $(jobs -r -p | wc -l) -ge $processes ]]; then
        wait -n
     fi
  done
done
wait
