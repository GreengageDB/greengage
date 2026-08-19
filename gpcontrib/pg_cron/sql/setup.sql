--Setup shared_preload_libraries for tests
--start_ignore
\! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'pg_cron'), ',')" postgres)"
\! gpstop -raq -M fast
\c
--end_ignore
