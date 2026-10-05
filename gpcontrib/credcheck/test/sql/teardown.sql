-- Restore shared_preload_libraries and pg_hba.conf
--start_ignore
\! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_remove(string_to_array(replace(current_setting('shared_preload_libraries'), ' ', ''), ','), 'credcheck'), ',')" postgres)"
\! sed -i '/ # credcheck test$/d' "$(psql -At -c 'SHOW hba_file' postgres)"
\! gpstop -raq -M fast
--end_ignore
