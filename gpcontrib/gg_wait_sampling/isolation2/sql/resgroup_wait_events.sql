--
-- Test statistics collection with GGDB specific wait events. Resgroup
--
!\retcode gpconfig -c gp_resource_manager -v group;
!\retcode gpstop -raq -M fast;
CREATE EXTENSION gg_wait_sampling;
CREATE RESOURCE GROUP rg_wait_test WITH(concurrency=0, cpu_max_percent=20);
CREATE ROLE role_wait_test RESOURCE GROUP rg_wait_test;
1:SET ROLE role_wait_test;
1&:BEGIN;

-- gg_wait_sampling_current shows the current wait event
SELECT query, queryid, mppsessionid, command_id, wait_event_type
FROM gg_wait_sampling_current JOIN pg_stat_activity USING(pid)
WHERE wait_event_type='ResourceGroup';

-- gg_wait_sampling_history shows the wait event history
-- gg_wait_sampling_profile shows the wait event per pid profile
SELECT pg_sleep(2);
SELECT event_type, count(event) > 100 as is_history_long, segid FROM gg_wait_sampling_history WHERE event='ResourceGroup'
GROUP BY event_type, segid;
SELECT event, event_type, segid FROM gg_wait_sampling_profile WHERE event='ResourceGroup';

ALTER RESOURCE GROUP rg_wait_test SET CONCURRENCY 1;
1<:
1q:
DROP ROLE role_wait_test;
DROP RESOURCE GROUP rg_wait_test;
DROP EXTENSION gg_wait_sampling;
!\retcode gpconfig -r gp_resource_manager;
!\retcode gpstop -raq -M fast;
