--Setup
!\retcode gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('shared_preload_libraries'), ','), 'gg_wait_sampling'), ',')" postgres)";
!\retcode gpstop -raq -M fast;
