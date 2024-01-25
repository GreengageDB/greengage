-- start_ignore
\! gpconfig -c shared_preload_libraries -v 'pg_stat_statements';
\! gpstop -raq -M fast;
-- end_ignore
