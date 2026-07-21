#!/usr/bin/env bash
# Run the TPC-DS benchmark across the three PRODUCTION Greengage images
# (greengage-prod:gg7 / gg8 / gg9) ONE AT A TIME in the same fixed-resource
# container, tearing the container fully down between versions so every run sees
# identical environment + resources. Writes results/tpcds_<label>.md per version
# and a single results/comparison.md at the end.
#
#   SCALE=100 RUN_MULTI_USER=true ./run_all.sh              # full 100 GB run
#   SCALE=1   RUN_MULTI_USER=false VERSIONS="gg9" ./run_all.sh   # quick smoke
#
# Knobs (env): SCALE, RUN_MULTI_USER, MULTI_USER_COUNT, SINGLE_USER_ITERATIONS,
#              NUM_PRIMARY_MIRROR_PAIRS, OPTIMIZER, VERSIONS, TPCDS_REF, KEEP_UP.
set -uo pipefail
cd "$(dirname "$0")"
HERE="$(pwd)"
D="sudo docker"

SCALE="${SCALE:-100}"
RUN_MULTI_USER="${RUN_MULTI_USER:-true}"
MULTI_USER_COUNT="${MULTI_USER_COUNT:-3}"
SINGLE_USER_ITERATIONS="${SINGLE_USER_ITERATIONS:-1}"
NUM_PRIMARY_MIRROR_PAIRS="${NUM_PRIMARY_MIRROR_PAIRS:-3}"
OPTIMIZER="${OPTIMIZER:-on}"
VERSIONS="${VERSIONS:-gg7 gg8 gg9}"
KEEP_UP="${KEEP_UP:-false}"

# Pin the TPC-DS harness to one commit so all three versions use an identical
# harness (fairness). Resolve "master" to a SHA once, up front.
TPCDS_REF="${TPCDS_REF:-master}"
if [ "$TPCDS_REF" = "master" ]; then
  sha=$(git ls-remote https://github.com/dimoffon/TPC-DS.git master 2>/dev/null | awk '{print $1}')
  [ -n "$sha" ] && TPCDS_REF="$sha"
fi
echo "### TPC-DS harness pinned to: $TPCDS_REF"
echo "### scale=${SCALE}GB  multi_user=${RUN_MULTI_USER}  segs=${NUM_PRIMARY_MIRROR_PAIRS}  versions='${VERSIONS}'"

mkdir -p results logs
run_labels=()

for key in $VERSIONS; do
  base="greengage-prod:${key}"
  label="$key"
  runner="tpcds-runner:${label}"
  cname="tpcds_${label}"
  echo
  echo "############################################################"
  echo "# VERSION $label   base=$base   scale=${SCALE}GB   $(date -u '+%F %T UTC')"
  echo "############################################################"
  if ! $D image inspect "$base" >/dev/null 2>&1; then
    echo "SKIP $label: base image $base not found (build it first)"; continue
  fi
  echo "--- disk before ---"; df -h / | tail -1

  echo ">>> [1/5] build runner image $runner"
  if ! $D build --build-arg GREENGAGE_IMAGE="$base" --build-arg TPCDS_REF="$TPCDS_REF" \
        -t "$runner" -f "$HERE/Dockerfile" "$HERE" > "logs/build_runner_${label}.log" 2>&1; then
    echo "FAIL $label: runner image build failed (see logs/build_runner_${label}.log)"; continue
  fi

  echo ">>> [2/5] start container $cname (identical resources)"
  $D rm -f "$cname" >/dev/null 2>&1 || true
  $D run -d --name "$cname" --hostname tpcds --privileged --init \
     --sysctl kernel.sem="500 1024000 200 4096" --ulimit nofile=65535 --shm-size=2gb \
     -v "$HERE/results:/results" -v "$HERE/logs:/logs" \
     "$runner" sleep infinity >/dev/null

  teardown() { [ "$KEEP_UP" = "true" ] || $D rm -f "$cname" >/dev/null 2>&1 || true; }

  echo ">>> [3/5] bring up demo cluster ($NUM_PRIMARY_MIRROR_PAIRS primaries)"
  if ! $D exec -e NUM_PRIMARY_MIRROR_PAIRS="$NUM_PRIMARY_MIRROR_PAIRS" -e WITH_MIRRORS=false \
        -e OPTIMIZER="$OPTIMIZER" "$cname" bash /opt/tpcds-scripts/bringup_cluster.sh \
        > "logs/tpcds_${label}_bringup.log" 2>&1; then
    echo "FAIL $label: cluster bringup failed (see logs/tpcds_${label}_bringup.log)"; teardown; continue
  fi

  echo ">>> [4/5] run TPC-DS (this is the long part)"
  $D exec "$cname" su - gpadmin -c \
    "SCALE='$SCALE' EXPLAIN_ANALYZE=false RANDOM_DISTRIBUTION=false \
     MULTI_USER_COUNT='$MULTI_USER_COUNT' SINGLE_USER_ITERATIONS='$SINGLE_USER_ITERATIONS' \
     RUN_MULTI_USER='$RUN_MULTI_USER' LABEL='$label' bash /opt/tpcds-scripts/run_benchmark.sh" \
    || echo "WARN $label: benchmark exited non-zero — aggregating what completed"

  echo ">>> [5/5] aggregate results"
  $D exec "$cname" su - gpadmin -c \
    "LABEL='$label' SCALE='$SCALE' bash /opt/tpcds-scripts/aggregate_results.sh" \
    || echo "WARN $label: aggregate failed"

  run_labels+=("$label")
  echo "--- report: results/tpcds_${label}.md ---"
  teardown
  echo "--- disk after teardown ---"; df -h / | tail -1
done

echo
echo "### all versions done: ${run_labels[*]:-none}"
if [ "${#run_labels[@]}" -ge 2 ]; then
  ./compare.sh "${run_labels[@]}" && echo "### comparison -> results/comparison.md"
else
  echo "### need >=2 completed versions for a comparison (got: ${run_labels[*]:-none})"
fi
