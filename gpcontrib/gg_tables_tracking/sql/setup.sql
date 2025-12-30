--Setup shared_preload_libraries
--start_ignore
\! gpconfig -s shared_preload_libraries | grep "value:" | head -1 | awk '{print $3}' > ./results/shared_libs
\! gpconfig -c shared_preload_libraries -v 'gg_tables_tracking'
\! gpstop -raq -M fast
\! gpconfig -c gg_tables_tracking.tracking_worker_naptime_sec -v '5';
--end_ignore