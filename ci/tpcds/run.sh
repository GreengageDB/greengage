#!/usr/bin/env bash
# Drive one full TPC-DS run for a given Greengage image and write a markdown
# report to ci/tpcds/results/tpcds_<LABEL>.md.
#
#   GREENGAGE_IMAGE=greengage8_u22:claude-merge-7 LABEL=claude-merge-7 ./run.sh
#   GREENGAGE_IMAGE=greengage8_u22:adb-8.x        LABEL=adb-8.x        SCALE=10 ./run.sh
#
# Precedence for every knob: caller env > ci/tpcds/.env > built-in default.
set -euo pipefail
cd "$(dirname "$0")"

getcfg() { local k="$1" d="${2:-}" v="${!1:-}"; if [ -n "$v" ]; then echo "$v"; else
  v=$(grep -E "^${k}=" .env 2>/dev/null | tail -1 | cut -d= -f2-); echo "${v:-$d}"; fi; }

GREENGAGE_IMAGE=$(getcfg GREENGAGE_IMAGE)
[ -n "$GREENGAGE_IMAGE" ] || { echo "ERROR: set GREENGAGE_IMAGE=greengage8_u22:<branch>"; exit 1; }
LABEL=$(getcfg LABEL "${GREENGAGE_IMAGE##*:}")
SCALE=$(getcfg SCALE 1)
TPCDS_REF=$(getcfg TPCDS_REF master)
EXPLAIN_ANALYZE=$(getcfg EXPLAIN_ANALYZE false)
RANDOM_DISTRIBUTION=$(getcfg RANDOM_DISTRIBUTION false)
MULTI_USER_COUNT=$(getcfg MULTI_USER_COUNT 3)
SINGLE_USER_ITERATIONS=$(getcfg SINGLE_USER_ITERATIONS 1)
RUN_MULTI_USER=$(getcfg RUN_MULTI_USER true)
KEEP_UP=$(getcfg KEEP_UP false)     # KEEP_UP=true leaves the container running for debugging
# NUM_PRIMARY_MIRROR_PAIRS / WITH_MIRRORS reach the container via compose's
# environment: block (from .env); they don't need re-exporting here.
export GREENGAGE_IMAGE LABEL SCALE TPCDS_REF

PROJECT="tpcds_$(echo "$LABEL" | tr -cs 'a-zA-Z0-9' '_')"
COMPOSE="docker compose -p $PROJECT -f docker-compose.yaml --env-file .env"
mkdir -p results logs

echo ">>> [1/5] build runner image  (base=$GREENGAGE_IMAGE  label=$LABEL)"
$COMPOSE build
echo ">>> [2/5] start container"
$COMPOSE up -d
if [ "$KEEP_UP" != "true" ]; then trap '$COMPOSE down -v || true' EXIT; fi

echo ">>> [3/5] bring up demo cluster"
$COMPOSE exec -T tpcds bash /opt/tpcds-scripts/bringup_cluster.sh

echo ">>> [4/5] run TPC-DS  (scale=${SCALE}GB  multi_user=${RUN_MULTI_USER})"
$COMPOSE exec -T tpcds su - gpadmin -c \
  "SCALE='$SCALE' EXPLAIN_ANALYZE='$EXPLAIN_ANALYZE' RANDOM_DISTRIBUTION='$RANDOM_DISTRIBUTION' \
   MULTI_USER_COUNT='$MULTI_USER_COUNT' SINGLE_USER_ITERATIONS='$SINGLE_USER_ITERATIONS' \
   RUN_MULTI_USER='$RUN_MULTI_USER' LABEL='$LABEL' bash /opt/tpcds-scripts/run_benchmark.sh" \
  || echo "WARN: benchmark exited non-zero — aggregating whatever completed (see logs/tpcds_${LABEL}_run.log)"

echo ">>> [5/5] aggregate results"
$COMPOSE exec -T tpcds su - gpadmin -c \
  "LABEL='$LABEL' SCALE='$SCALE' bash /opt/tpcds-scripts/aggregate_results.sh"

echo
echo "Done. Report:   ci/tpcds/results/tpcds_${LABEL}.md"
echo "     Raw log:   ci/tpcds/logs/tpcds_${LABEL}_run.log"
echo "     Compare:   ci/tpcds/compare.sh   (after running a second version)"
