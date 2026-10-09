-- Setup shared_preload_libraries and allow connections for the role of 08_first_login
--start_ignore
\! gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_append(array_remove(string_to_array(replace(current_setting('shared_preload_libraries'), ' ', ''), ','), 'credcheck'), 'credcheck'), ',')" postgres)"
\! sed -i -e '1i local all forced_login trust # credcheck test' -e '1i host all forced_login samehost trust # credcheck test' -e '/ # credcheck test$/d' "$(psql -At -c 'SHOW hba_file' postgres)"
\! gpstop -raq -M fast
--end_ignore
