--Teardown
!\retcode gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_remove(string_to_array(current_setting('shared_preload_libraries'), ','), 'gg_tables_tracking'), ',')" postgres)";
!\retcode gpconfig -r gg_tables_tracking.tracking_worker_naptime_sec;
!\retcode gpstop -raq -M fast;
