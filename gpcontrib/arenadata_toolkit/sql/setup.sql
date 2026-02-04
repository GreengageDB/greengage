--Setup shared_preload_libraries
--start_ignore
\! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'arenadata_toolkit'), ',')" postgres)"
\! gpstop -raq -M fast
\! gpconfig -c arenadata_toolkit.tracking_worker_naptime_sec -v '5'
\! gpstop -u
--end_ignore
