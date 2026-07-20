#!/bin/bash
# Run the TPC-DS harness against the local demo cluster. Run as gpadmin.
# Bypasses the root/yum tpcds.sh wrapper and calls rollout.sh directly with the
# 15 positional args it expects (same order as tpcds.sh's final line).
set -uo pipefail

GPHOME=/usr/local/greenplum-db-devel
DEMO=/home/gpadmin/gpdb_src/gpAux/gpdemo
source "$GPHOME/greenplum_path.sh"
source "$DEMO/gpdemo-env.sh"
export PGDATABASE=tpcds

SCALE="${SCALE:-1}"
EXPLAIN_ANALYZE="${EXPLAIN_ANALYZE:-false}"
RANDOM_DISTRIBUTION="${RANDOM_DISTRIBUTION:-false}"
MULTI_USER_COUNT="${MULTI_USER_COUNT:-3}"
SINGLE_USER_ITERATIONS="${SINGLE_USER_ITERATIONS:-1}"
RUN_MULTI_USER="${RUN_MULTI_USER:-true}"
LABEL="${LABEL:-local}"
LOG="/logs/tpcds_${LABEL}_run.log"
mkdir -p /logs

# The TPC-DS "Score" needs the concurrent (multi-user) phase, so tie the
# multi-user report and score to whether the multi-user phase runs.
RUN_MULTI_USER_REPORT="$RUN_MULTI_USER"
RUN_SCORE="$RUN_MULTI_USER"

cd /opt/TPC-DS
echo "############################################################"
echo "TPC-DS run: label=$LABEL scale=${SCALE}GB optimizer=$(psql -Atc 'show optimizer')"
echo "  multi_user=$RUN_MULTI_USER count=$MULTI_USER_COUNT iterations=$SINGLE_USER_ITERATIONS"
echo "  db=$PGDATABASE port=$PGPORT host=$(hostname)"
echo "############################################################"

# rollout.sh args:
#  SCALE EXPLAIN RANDOM MU_COUNT  COMPILE GEN INIT DDL LOAD SQL  SINGLE_REP MULTI MULTI_REP SCORE  ITER
./rollout.sh "$SCALE" "$EXPLAIN_ANALYZE" "$RANDOM_DISTRIBUTION" "$MULTI_USER_COUNT" \
  true true true true true true \
  true "$RUN_MULTI_USER" "$RUN_MULTI_USER_REPORT" "$RUN_SCORE" \
  "$SINGLE_USER_ITERATIONS" 2>&1 | tee "$LOG"
rc="${PIPESTATUS[0]}"

echo "ROLLOUT_EXIT=$rc" | tee -a "$LOG"
exit "$rc"
