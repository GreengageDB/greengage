--Setup shared_preload_libraries and create schema for tests
--start_ignore
\! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'credcheck'), ',')" postgres)"
\! gpstop -raq -M fast
--end_ignore