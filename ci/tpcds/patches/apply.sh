#!/bin/bash
# Apply the GreengageDB-8 compatibility overlay to the dimoffon/TPC-DS harness.
# Idempotent; run at image-build time (see ci/tpcds/Dockerfile).
set -euo pipefail
REPO="${1:?usage: apply.sh <TPC-DS repo dir>}"
fn="$REPO/functions.sh"
init="$REPO/02_init/rollout.sh"

# 1) get_version(): recognise GreengageDB 7 and 8 as the gpdb_6 code path.
#    The stock harness only matches "Greenplum Database 4.3/5/6"; GG8 reports
#    "PostgreSQL 18.4 (Greenplum Database 8.0.0...)" and would fall through to
#    single-node PostgreSQL mode (heap tables, no segment distribution, and the
#    removed pg_filespace_entry). The gpdb_6 path uses
#    gp_segment_configuration.datadir and AO column storage, which GG7/8 support.
# Match BOTH product names: GreengageDB 8 (PG14/PG18) reports "Greenplum
# Database 8.0.0-alpha", but GreengageDB 7 reports "Greengage Database 7.0.0-beta"
# (rebranded), so we map Greenplum 7/8 AND Greengage 7/8 to the gpdb_6 path.
# Anchor on `ELSE 'postgresql' END FROM version()` (always present) and insert the
# four WHEN clauses before it; idempotent via the "Greengage Database 8" guard.
if ! grep -q "Greengage Database 8" "$fn"; then
  sed -i "s/ELSE 'postgresql' END FROM version()/WHEN POSITION ('Greenplum Database 7' IN version) > 0 THEN 'gpdb_6' WHEN POSITION ('Greenplum Database 8' IN version) > 0 THEN 'gpdb_6' WHEN POSITION ('Greengage Database 7' IN version) > 0 THEN 'gpdb_6' WHEN POSITION ('Greengage Database 8' IN version) > 0 THEN 'gpdb_6' ELSE 'postgresql' END FROM version()/" "$fn"
fi
grep -q "Greengage Database 8" "$fn" || { echo "FATAL: get_version patch did not apply (functions.sh layout changed?)"; exit 1; }

# 2) 02_init: GP6-era GUC / resource-group tuning does not always map 1:1 onto
#    GG8 (e.g. gpconfig's --masteronly flag, or admin_group resource-group ALTERs
#    when the demo cluster runs resource *queues*). Make that tuning best-effort
#    so one incompatible knob cannot abort the benchmark. The GUCs that actually
#    matter for the run (optimizer, search_path) are set explicitly in
#    bringup_cluster.sh / by the harness's set_search_path.
for f in check_gucs set_memory_limit set_concurrency set_cpu_rate_limit set_memory_shared_quota set_memory_spill_ratio; do
  sed -i "s/^\([[:space:]]*\)${f}\$/\1${f} || true/" "$init"
done

# 3) The TPC-DS tools (dsdgen/dsqgen) are old C that declares globals in headers
#    included by multiple .c files. GCC 10+ defaults to -fno-common, turning
#    those into "multiple definition" link errors (fails on Ubuntu 22.04 / gcc 11).
#    Build the tools with -fcommon.
mk="$REPO/00_compile_tpcds/tools/makefile"
if [ -f "$mk" ] && ! grep -q 'fcommon' "$mk"; then
  sed -i -E 's/^(LINUX_CFLAGS[[:space:]]*=.*)$/\1 -fcommon/' "$mk"
fi
[ -f "$mk" ] && { grep -q 'fcommon' "$mk" || { echo "FATAL: -fcommon patch did not apply to $mk"; exit 1; }; }

echo "GreengageDB-8 TPC-DS overlay applied to $REPO"
