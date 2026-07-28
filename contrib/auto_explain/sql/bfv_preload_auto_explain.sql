-- start_ignore
\! gpconfig -c shared_preload_libraries -v 'auto_explain';
\! gpconfig -c auto_explain.log_min_duration -v 0 --skipvalidation;
\! gpconfig -c auto_explain.log_analyze -v true --skipvalidation;
-- gpstop -rai does an immediate (crash) shutdown; the following gpstart brings the
-- coordinator up in gp_role=utility (admin) mode first and, on a busy cluster, can
-- briefly serve there while its own crash recovery races (gpstart may even fail with
-- rc=2 and be retried).  A session that reconnects during that window runs
-- coordinator-only, so auto_explain (which requires GP_ROLE_DISPATCH) logs nothing and
-- this test fails spuriously.  Restart until gpstop returns clean, then block until a
-- DISPATCHED query reaches every primary and the coordinator is stably in dispatch mode.
\! for t in 1 2 3 4 5; do gpstop -raiq && break; sleep 5; done; segs=$(psql -d postgres -Atc "select count(*) from gp_segment_configuration where role='p' and content>=0" 2>/dev/null); n=0; for i in $(seq 1 120); do [ "$(psql -d postgres -Atc 'show gp_role' 2>/dev/null)" = dispatch ] && [ "$(psql -d postgres -Atc "select count(*) from gp_dist_random('gp_id')" 2>/dev/null)" = "$segs" ] && n=$((n+1)) || n=0; [ "$n" -ge 3 ] && break; sleep 1; done;
\c
-- end_ignore

SET CLIENT_MIN_MESSAGES = LOG;

-- check that auto_explain doesn't work on coordinator with Gp_role is not a GP_ROLE_DISPATCH
-- Query 'SELECT count(1) from (select i from t1 limit 10) t join t2 using (i)' generate executor's slice on coordinator:
--             ->  Redistribute Motion 1:3  (slice2)
--                   Output: t1.i
--                   Hash Key: t1.i
--                   ->  Limit
--                         Output: t1.i
--                         ->  Gather Motion 3:1  (slice1; segments: 3)
-- IMPORTANT: ./configure with --enable-orca

CREATE TABLE t1(i int);
CREATE TABLE t2(i int);
SELECT count(1) from (select i from t1 limit 10) t join t2 using (i);
DROP TABLE t1;
DROP TABLE t2;

-- start_ignore
\! gpconfig -r auto_explain.log_min_duration;
\! gpconfig -r auto_explain.log_analyze;
\! gpconfig -r shared_preload_libraries;
-- Same restart race as above (see the setup block): wait for a stable, fully dispatched
-- cluster before the next test reconnects.
\! for t in 1 2 3 4 5; do gpstop -raiq && break; sleep 5; done; segs=$(psql -d postgres -Atc "select count(*) from gp_segment_configuration where role='p' and content>=0" 2>/dev/null); n=0; for i in $(seq 1 120); do [ "$(psql -d postgres -Atc 'show gp_role' 2>/dev/null)" = dispatch ] && [ "$(psql -d postgres -Atc "select count(*) from gp_dist_random('gp_id')" 2>/dev/null)" = "$segs" ] && n=$((n+1)) || n=0; [ "$n" -ge 3 ] && break; sleep 1; done;
-- end_ignore
