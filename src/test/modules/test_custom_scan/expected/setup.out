-- Load the provider in every new backend of the cluster. A plan with the
-- custom scan reaches the segments, whose backends read it only knowing
-- the scan's methods, and LOAD reaches only the backends running now.
--start_ignore
\! gpconfig -c session_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(string_to_array(current_setting('session_preload_libraries'), ','), 'test_custom_scan'), ',')" postgres)"
\! gpstop -u
--end_ignore
