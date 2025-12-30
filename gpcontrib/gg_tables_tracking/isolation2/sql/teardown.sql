--Teardown
!\retcode ORIGINAL_SPL=$(cat ../../../gpcontrib/gg_tables_tracking/isolation2/results/shared_libs); [ -z "$ORIGINAL_SPL" ] && gpconfig -r shared_preload_libraries || gpconfig -c shared_preload_libraries -v "$ORIGINAL_SPL" ;
!\retcode gpconfig -r gg_tables_tracking.tracking_worker_naptime_sec;
!\retcode gpstop -raq -M fast;
