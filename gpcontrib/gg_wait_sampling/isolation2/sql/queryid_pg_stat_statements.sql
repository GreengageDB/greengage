-- queryid has the upstream pg_wait_sampling meaning: it is the value
-- pg_stat_statements stored in the query, on every node, and 0 without it.
-- Each half runs in its own session, since the cluster restarts in between.
!\retcode gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_prepend('pg_stat_statements', string_to_array(current_setting('shared_preload_libraries'), ',')), ',')" postgres)";
!\retcode gpstop -raq -M fast;

1: CREATE EXTENSION pg_stat_statements;
1: CREATE EXTENSION gg_wait_sampling;
1: SELECT pg_stat_statements_reset() IS NOT NULL AS reset;
1: SELECT * FROM gg_wait_sampling_reset_profile ORDER BY gp_segment_id;

-- One query sleeping on the coordinator, one on every segment.
1: SELECT pg_sleep(0.5) IS NULL AS slept;
1: SELECT pg_sleep(0.5) IS NULL AS slept FROM gp_dist_random('gp_id');

1: SELECT h.segid, bool_and(h.queryid = p.queryid) AS matches_pg_stat_statements, count(*) > 0 AS sampled FROM gg_wait_sampling_history h, pg_stat_statements p WHERE h.event = 'PgSleep' AND h.segid = -1 AND p.query = 'SELECT pg_sleep($1) IS NULL AS slept' GROUP BY h.segid;
1: SELECT h.segid, bool_and(h.queryid = p.queryid) AS matches_pg_stat_statements, count(*) > 0 AS sampled FROM gg_wait_sampling_history h, pg_stat_statements p WHERE h.event = 'PgSleep' AND h.segid >= 0 AND p.query LIKE 'SELECT pg_sleep($1) IS NULL AS slept FROM gp_dist_random(%' GROUP BY h.segid ORDER BY h.segid;

1: DROP EXTENSION gg_wait_sampling;
1: DROP EXTENSION pg_stat_statements;
1q:

-- Without pg_stat_statements nothing sets the query id.
!\retcode gpconfig -c shared_preload_libraries -v "$(psql -At -c "SELECT array_to_string(array_remove(string_to_array(current_setting('shared_preload_libraries'), ','), 'pg_stat_statements'), ',')" postgres)";
!\retcode gpstop -raq -M fast;

2: CREATE EXTENSION gg_wait_sampling;
2: SELECT * FROM gg_wait_sampling_reset_profile ORDER BY gp_segment_id;
2: SELECT pg_sleep(0.5) IS NULL AS slept;
2: SELECT pg_sleep(0.5) IS NULL AS slept FROM gp_dist_random('gp_id');

2: SELECT h.segid, bool_and(h.queryid = 0) AS queryid_is_zero, count(*) > 0 AS sampled FROM gg_wait_sampling_history h WHERE h.event = 'PgSleep' GROUP BY h.segid ORDER BY h.segid;

2: DROP EXTENSION gg_wait_sampling;
2q:
