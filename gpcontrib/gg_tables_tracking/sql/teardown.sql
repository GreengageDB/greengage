--Teardown tests
--start_ignore
\! ORIGINAL_SPL=$(cat ./results/shared_libs); [ -z "$ORIGINAL_SPL" ] && gpconfig -r shared_preload_libraries || gpconfig -c shared_preload_libraries -v "$ORIGINAL_SPL"
\! gpconfig -r gg_tables_tracking.tracking_worker_naptime_sec;
\! gpstop -raq -M fast
--end_ignore