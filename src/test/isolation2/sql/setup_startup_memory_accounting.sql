-- start_ignore
! gpconfig -c gp_vmem_limit_per_query -v '20MB' --skipvalidation
! gpconfig -c gp_vmem_protect_limit -v '60'
! gpconfig -c runaway_detector_activation_percent -v 0
-- After the immediate shutdown, gpstop -rai's internal gpstart can hit the
-- coordinator mid-startup ("FATAL: the database system is not accepting
-- connections / Hot standby mode is disabled", rc=2) and abort, leaving the
-- cluster DOWN: coordinator stuck in utility/admin mode with all primaries
-- down.  oom_startup_memory then dispatches its join coordinator-only
-- (numsegments=-1), never reaches a QE where gp_vmem_limit_per_query is
-- enforced, and the expected per-query OOM never fires.  Retry the restart
-- until it returns clean, so the cluster is fully converged in dispatch mode
-- (gpstart rc=0 == phase-2 complete, all primaries up) before the OOM test.
-- Same retry-until-clean idiom as pg_rewind_fail_missing_xlog (a326930af4a);
-- it dispatches no query gang, so it cannot perturb a downstream
-- fault-injection test the way the reverted gp_dist_random poll barrier did.
! for i in $(seq 1 10); do gpstop -rai && break; echo "retry gpstop -rai ($i)"; sleep 5; done;
-- end_ignore
