-- start_matchsubs
-- m/ERROR:  backend with pid=-?\d+ not found( \([[:alnum:]_]+\.[ch]:\d+\))?/
-- s/ \([[:alnum:]_]+\.[ch]:\d+\)//
-- end_matchsubs

CREATE EXTENSION gg_wait_sampling;

-- Set normal collection periods so the sampler gathers data
--start_ignore
\! gpconfig -c gg_wait_sampling.history_period -v 5
\! gpconfig -c gg_wait_sampling.profile_period -v 5
\! gpstop -u
--end_ignore
-- BASIC QUERYABILITY
-- Row counts are timing-dependent; we only assert they are non-negative
SELECT segid, count(*) > 0 AS cluster_current_queryable
FROM gg_wait_sampling_current GROUP BY segid;

SELECT segid, count(*) > 0 AS cluster_history_queryable
FROM gg_wait_sampling_history GROUP BY segid;

SELECT segid, count(*) > 0 AS cluster_profile_queryable
FROM gg_wait_sampling_profile GROUP BY segid;

-- Own pid must return exactly one row.
SELECT count(*) = 1 AS ok
FROM gg_wait_sampling_get_current_coordinator(pg_backend_pid());
-- A non-existent pid must raise an error.
SELECT gg_wait_sampling_get_current_coordinator(-1);

-- Force sleep on every segment
SELECT * from gg_wait_sampling_reset_profile;
SELECT -1 AS gp_segment_id, pg_sleep(6)
UNION ALL
SELECT gp_segment_id, pg_sleep(6) FROM gp_dist_random('gp_id');
-- Validate
SELECT segid, event_type, event
FROM gg_wait_sampling_profile
WHERE event_type = 'Timeout'
AND event = 'PgSleep';
--start_ignore
\! gpconfig -r gg_wait_sampling.history_period
\! gpconfig -r gg_wait_sampling.profile_period
\! gpstop -u
--end_ignore
DROP EXTENSION gg_wait_sampling;
