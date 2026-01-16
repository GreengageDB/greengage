--Setup shared_preload_libraries
--start_ignore
\! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'gg_tables_tracking'), ',')" postgres)"
\! gpconfig -c gg_tables_tracking.tracking_worker_naptime_sec -v '5'
\! gpstop -raq -M fast
--end_ignore