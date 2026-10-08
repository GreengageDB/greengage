-- A parallel retrieve connection to a segment gets a session id when resource
-- groups are enabled, but it has its own postmaster's start time, not the
-- coordinator's. It must not publish that time as the node's tmid.
!\retcode gpconfig -c gp_resource_manager -v group;
!\retcode gpstop -raq -M fast;

-- Restart the coordinator alone, in a later second, so that its postmaster
-- start time differs from the segments'.
!\retcode sleep 2;
!\retcode pg_ctl -D $COORDINATOR_DATA_DIRECTORY -w -m fast -l $COORDINATOR_DATA_DIRECTORY/log/startup.log -o "-c gp_role=dispatch" restart;

1: CREATE EXTENSION gg_wait_sampling;
1: CREATE TABLE t_retrieve_tmid (i INT) DISTRIBUTED BY (i);
1: INSERT INTO t_retrieve_tmid SELECT generate_series(1, 20);

-- The QEs of this session have published the coordinator's start time on
-- every segment, and segment 0's own postmaster started before it.
1: SELECT c.segid, bool_and(c.tmid = floor(extract(epoch FROM pg_postmaster_start_time()))::int4) AS tmid_is_coordinators FROM gg_wait_sampling_get_current_segments() c WHERE c.mppsessionid > 0 GROUP BY c.segid ORDER BY c.segid;
0U: SELECT floor(extract(epoch FROM pg_postmaster_start_time()))::int4 < (SELECT min(tmid) FROM gg_wait_sampling_get_current() WHERE mppsessionid > 0) AS segment_started_before_coordinator;

1: BEGIN;
1: DECLARE c1 PARALLEL RETRIEVE CURSOR FOR SELECT 1 FROM gp_dist_random('gp_id');
1: @post_run 'parse_endpoint_info 1 1 2 3 4': SELECT endpointname, auth_token, hostname, port, state FROM gp_get_endpoints() WHERE cursorname = 'c1';
0R: @pre_run 'set_endpoint_variable @ENDPOINT1': RETRIEVE ALL FROM ENDPOINT "@ENDPOINT1";

-- The retrieve connection ran a utility statement on segment 0: the node's
-- tmid there is still the coordinator's. This is checked from the utility
-- session only, since a dispatched query would start QEs that publish the
-- coordinator's value again and hide an overwrite.
0U: SELECT floor(extract(epoch FROM pg_postmaster_start_time()))::int4 < (SELECT min(tmid) FROM gg_wait_sampling_get_current() WHERE mppsessionid > 0) AS segment_started_before_coordinator;

0Rq:
1: COMMIT;
1: DROP TABLE t_retrieve_tmid;
1: DROP EXTENSION gg_wait_sampling;
1q:
0Uq:

!\retcode gpconfig -r gp_resource_manager;
!\retcode gpstop -raq -M fast;
