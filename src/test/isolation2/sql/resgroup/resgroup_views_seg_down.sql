-- The test below checks that FTS can break the stuck cancelled/terminated
-- query, if one of the segments has hanged.
-- Original problem was:
-- 1. For some reason (the reason itself is not important) at some point segment hangs.
-- 2. Query to 'gp_resgroup_status_per_segment' also hangs.
-- 3. If 'pg_cancel_backend' or 'pg_terminate_backend' is called before FTS makes
-- promotion, the query stays is stuck condition.

-- Change FTS settings to make FTS probe work faster in the test.
!\retcode gpconfig -c gp_fts_probe_timeout -v 5 --masteronly;
!\retcode gpconfig -c gp_fts_probe_retries -v 1 --masteronly;
!\retcode gpstop -u;

CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

-- Case 1: check the scenario with 'pg_cancel_backend'.

-- Reset FTS probe interval.
SELECT gp_request_fts_probe_scan();

-- Make a successfull query to allocate query executer backends on the segments.
1:SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = 'admin_group';

-- Emulate hanged segment condition:
-- 0. Store the name of datadir for seg0 - we will need it on step 2.
-- 1. Stop the backends from processing requests by injecting a fault.
-- 2. Stop segment postmaster process. This can't be done by the fault injection,
-- as we wouldn't be able to recover the postmaster back to life in this case.
-- Thus do it by sending a STOP signal. We need to do it on all segments (not on
-- the coordinator), as the segment process may be running on a separate machine.
2: @post_run 'get_tuple_cell DATADIR 1 1': SELECT datadir FROM gp_segment_configuration WHERE role = 'p' AND content = 0;

1: SELECT gp_inject_fault('exec_mpp_query_start', 'suspend', dbid, current_setting('gp_session_id')::int)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;

2: @pre_run ' echo "${RAW_STR}" | sed "s#@DATADIR#${DATADIR}#" ': SELECT exec_cmd_on_segments('ps aux | grep ''@DATADIR'' | awk ''FNR == 1 {print $2; exit}'' | xargs kill -STOP');

-- Launch the query again, now it will hang.
1&:SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = 'admin_group';

-- Cancel the hanging query.
SELECT pg_cancel_backend(pid) FROM pg_stat_activity
WHERE query = 'SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = ''admin_group'';';

-- Trigger FTS scan.
SELECT gp_request_fts_probe_scan();

-- Expect to see the content 0, preferred primary is mirror and it is down,
-- the preferred mirror is primary
SELECT content, preferred_role, role, status, mode
FROM gp_segment_configuration WHERE content = 0;

1<:

-- Check the hanged query has complete.
SELECT count(*) = 0 AS query_complete FROM pg_stat_activity
WHERE state = 'active' AND query = 'SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = ''admin_group'';';

-- Recover back the segment
2: @pre_run ' echo "${RAW_STR}" | sed "s#@DATADIR#${DATADIR}#" ': SELECT exec_cmd_on_segments('ps aux | grep ''@DATADIR'' | awk ''FNR == 1 {print $2; exit}'' | xargs kill -CONT');

SELECT gp_inject_fault('exec_mpp_query_start', 'resume', dbid)
FROM gp_segment_configuration WHERE role = 'm' AND content = 0;

SELECT gp_inject_fault('exec_mpp_query_start', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'm' AND content = 0;

-- Fully recover the failed primary as new mirror.
!\retcode gprecoverseg -aF --no-progress;

-- Wait while segments come in sync.
SELECT wait_until_all_segments_synchronized();

-- Expect to see roles flipped and in sync.
SELECT content, preferred_role, role, status, mode
FROM gp_segment_configuration WHERE content = 0;

!\retcode gprecoverseg -ar;

-- Wait while segments come in sync.
SELECT wait_until_all_segments_synchronized();

-- Verify that no segment is down after the recovery.
SELECT count(*) FROM gp_segment_configuration WHERE status = 'd';

-- Case 2: check the scenario with 'pg_terminate_backend'.

-- Reset FTS probe interval.
SELECT gp_request_fts_probe_scan();

-- Make a successfull query to allocate query executer backends on the segments.
1:SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = 'admin_group';

-- Emulate hanged segment condition:
-- 1. Stop the backends from processing requests by injecting a fault.
-- 2. Stop segment postmaster process. This can't be done by the fault injection,
-- as we wouldn't be able to recover the postmaster back to life in this case.
-- Thus do it by sending a STOP signal. We need to do it on all segments (not on
-- the coordinator), as the segment process may be running on a separate machine.
1: SELECT gp_inject_fault('exec_mpp_query_start', 'suspend', dbid, current_setting('gp_session_id')::int)
FROM gp_segment_configuration WHERE role = 'p' AND content = 0;

2: @pre_run ' echo "${RAW_STR}" | sed "s#@DATADIR#${DATADIR}#" ': SELECT exec_cmd_on_segments('ps aux | grep ''@DATADIR'' | awk ''FNR == 1 {print $2; exit}'' | xargs kill -STOP');

-- Launch the query again, now it will hang.
1&:SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = 'admin_group';

-- Terminate the hanging query.
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE query = 'SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = ''admin_group'';';

-- Trigger FTS scan.
SELECT gp_request_fts_probe_scan();

-- Expect to see the content 0, preferred primary is mirror and it is down,
-- the preferred mirror is primary
SELECT content, preferred_role, role, status, mode
FROM gp_segment_configuration WHERE content = 0;

1<:

-- Check the hanged query has complete.
SELECT count(*) = 0 AS query_complete FROM pg_stat_activity
WHERE state = 'active' AND query = 'SELECT count(1) FROM gp_toolkit.gp_resgroup_status_per_segment WHERE groupname = ''admin_group'';';

-- Recover back the segment
2: @pre_run ' echo "${RAW_STR}" | sed "s#@DATADIR#${DATADIR}#" ': SELECT exec_cmd_on_segments('ps aux | grep ''@DATADIR'' | awk ''FNR == 1 {print $2; exit}'' | xargs kill -CONT');

SELECT gp_inject_fault('exec_mpp_query_start', 'resume', dbid)
FROM gp_segment_configuration WHERE role = 'm' AND content = 0;

SELECT gp_inject_fault('exec_mpp_query_start', 'reset', dbid)
FROM gp_segment_configuration WHERE role = 'm' AND content = 0;

-- Fully recover the failed primary as new mirror.
!\retcode gprecoverseg -aF --no-progress;

-- Wait while segments come in sync.
SELECT wait_until_all_segments_synchronized();

-- Expect to see roles flipped and in sync.
SELECT content, preferred_role, role, status, mode
FROM gp_segment_configuration WHERE content = 0;

!\retcode gprecoverseg -ar;

-- Wait while segments come in sync.
SELECT wait_until_all_segments_synchronized();

-- Verify that no segment is down after the recovery.
SELECT count(*) FROM gp_segment_configuration WHERE status = 'd';

1q:

-- Restore parameters.
!\retcode gpconfig -r gp_fts_probe_timeout --masteronly;
!\retcode gpconfig -r gp_fts_probe_retries --masteronly;
!\retcode gpstop -u;

