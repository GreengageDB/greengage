#!/bin/bash
# Aggregate TPC-DS results from the cluster into a markdown report + a per-query
# TSV (used by compare.sh for cross-version comparison). Run as gpadmin.
set -uo pipefail

# GG7 installs to greengage-db-devel/greengage_path.sh; GG8/9 to
# greenplum-db-devel/greenplum_path.sh. Detect whichever this image has.
for d in /usr/local/greengage-db-devel /usr/local/greenplum-db-devel; do
  [ -d "$d" ] && GPHOME="$d" && break
done
DEMO=/home/gpadmin/gpdb_src/gpAux/gpdemo
for p in "$GPHOME"/greengage_path.sh "$GPHOME"/greenplum_path.sh; do
  [ -f "$p" ] && source "$p" && break
done
source "$DEMO/gpdemo-env.sh"
export PGDATABASE=tpcds

LABEL="${LABEL:-local}"
SCALE="${SCALE:-?}"
LOG="/logs/tpcds_${LABEL}_run.log"
OUT="/results/tpcds_${LABEL}.md"
TSVDIR="/results/${LABEL}"
mkdir -p "$TSVDIR" /results

q() { psql -qtA -F$'\t' -c "$1" 2>/dev/null; }
# to_regclass guard: returns the scalar or 'n/a' if the table is missing.
scalar() { local t="$1" sql="$2"; if [ "$(q "select to_regclass('$t') is not null")" = t ]; then q "$sql"; else echo "n/a"; fi; }

gpver=$(q "select version()")
optimizer=$(q "show optimizer")
segs=$(q "select count(*) from gp_segment_configuration where role='p' and content>=0")

load_s=$(scalar tpcds_reports.load   "select coalesce(round(extract(epoch from sum(duration))::numeric,2),0) from tpcds_reports.load where tuples>0")
anlz_s=$(scalar tpcds_reports.load   "select coalesce(round(extract(epoch from sum(duration))::numeric,2),0) from tpcds_reports.load where tuples=0")
rows=$(scalar   tpcds_reports.load   "select coalesce(sum(tuples),0) from tpcds_reports.load where tuples>0")
su_s=$(scalar   tpcds_reports.sql    "select coalesce(round(sum(s)::numeric,2),0) from (select extract(epoch from min(duration)) s from tpcds_reports.sql group by description) x")
mu_s=$(scalar   tpcds_testing.sql    "select coalesce(round(extract(epoch from sum(duration))::numeric,2),0) from tpcds_testing.sql")

# The harness prints the official Score block from 09_score/rollout.sh; lift it
# from the run log (source of truth for the TPC-DS scoring formula).
score=$(grep -E '^Score' "$LOG" 2>/dev/null | tail -1 | awk '{print $2}')
[ -z "${score:-}" ] && score="n/a"

# Per-query single-user timings (min across iterations) -> TSV + md rows.
q "select description, round(extract(epoch from min(duration))::numeric,2)
     from tpcds_reports.sql group by description order by description" > "$TSVDIR/queries.tsv"

{
  echo "# TPC-DS results — \`${LABEL}\`"
  echo
  echo "- **Generated:** $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo "- **Version:** ${gpver}"
  echo "- **Scale factor:** ${SCALE} GB"
  echo "- **Primary segments:** ${segs}"
  echo "- **Optimizer:** ${optimizer}"
  echo
  echo "## Summary"
  echo
  echo "| Metric | Value |"
  echo "|---|---|"
  echo "| Rows loaded | ${rows} |"
  echo "| Load time (s) | ${load_s} |"
  echo "| Analyze time (s) | ${anlz_s} |"
  echo "| Single-user query total (s) | ${su_s} |"
  echo "| Multi-user query total (s) | ${mu_s} |"
  echo "| **TPC-DS Score** | **${score}** |"
  echo
  echo "## Per-query single-user timings (s)"
  echo
  echo "| Query | Seconds |"
  echo "|---|---:|"
  if [ -s "$TSVDIR/queries.tsv" ]; then
    while IFS=$'\t' read -r desc sec; do echo "| ${desc} | ${sec} |"; done < "$TSVDIR/queries.tsv"
  else
    echo "| _(no single-user query results found)_ | |"
  fi
} > "$OUT"

echo "Wrote $OUT"
echo "Wrote $TSVDIR/queries.tsv ($(wc -l < "$TSVDIR/queries.tsv" 2>/dev/null || echo 0) queries)"
