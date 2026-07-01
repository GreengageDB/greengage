--Setup shared_preload_libraries for tests
--start_ignore
\! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'gg_wait_sampling'), ',')" postgres)"
\! gpconfig -c gg_wait_sampling.history_period -v 10000
\! gpconfig -c gg_wait_sampling.profile_period -v 10000
\! gpstop -raq -M fast
--end_ignore
