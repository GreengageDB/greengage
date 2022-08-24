
-- start_ignore
\! gpconfig -c shared_preload_libraries -v 'auto_explain';
\! gpstop -raiq;
\c
-- end_ignore

SET auto_explain.log_min_duration = 0;
SET CLIENT_MIN_MESSAGES = LOG;

-- auto_axplain must work only at coordinator with Gp_role is GP_ROLE_DISPATCH
-- check that auto_explain doesn't work on segments which are executors.
-- Query 'CREATE TABLE t1 as select generate_series(1, 1)' and query 'CREATE TABLE t1 as select generate_series(1, 10*1000*1000)'
-- are equally got segfault because it has intermediate executor's slice:
--   Result
--     Output: (generate_series(1, 1))
--       ->  Redistribute Motion 3:3  (slice1; segments: 3)

CREATE TABLE t1 as select generate_series(1, 1);
DROP TABLE t1;

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
\! gpconfig -r shared_preload_libraries;
\! gpstop -raiq;
-- end_ignore
