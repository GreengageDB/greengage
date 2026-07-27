-- start_ignore
! gpconfig -c gp_vmem_limit_per_query -v '20MB' --skipvalidation
! gpconfig -c gp_vmem_protect_limit -v '60'
! gpconfig -c runaway_detector_activation_percent -v 0
! gpstop -rai;
-- Wait for the cluster to fully converge before the OOM test runs.  After
-- gpstop -rai, gpstart briefly serves a master-only (utility) coordinator with
-- all primaries still down; a query dispatched in that window runs
-- coordinator-only and never reaches a QE where gp_vmem_limit_per_query is
-- enforced, so the expected per-query OOM never fires and the query returns a
-- row instead.  Poll with a FRESH connection each iteration (to survive the
-- phase-1 -> phase-2 coordinator restart) until a distributed query dispatches
-- to all 3 primaries (gp_dist_random('gp_id') returns one row per primary).
! for i in $(seq 1 120); do [ "x$(psql -X -A -t -d isolation2test -c "select count(*) from gp_dist_random('gp_id')" 2>/dev/null)" = "x3" ] && break; sleep 1; done;
-- end_ignore
