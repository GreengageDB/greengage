-- start_matchsubs
-- m/ Gather Motion [12]:1  \(slice1; segments: [12]\)/
-- s/ Gather Motion [12]:1  \(slice1; segments: [12]\)/ Gather Motion XXX/
-- m/Memory Usage: \d+\w?B/
-- s/Memory Usage: \d+\w?B/Memory Usage: ###B/
-- m/Buckets: \d+/
-- s/Buckets: \d+/Buckets: ###/
-- m/Batches: \d+/
-- s/Batches: \d+/Batches: ###/
-- m/Planning Time: [0-9.]+ ms/
-- s/Planning Time: [0-9.]+ ms/Planning Time: #.### ms/
-- m/Execution Time: [0-9.]+ ms/
-- s/Execution Time: [0-9.]+ ms/Execution Time: #.### ms/
-- m/Executor memory: \d+\w? bytes \(seg\d+\)/
-- s/Executor memory: \d+\w? bytes \(seg\d+\)/Executor memory: ### bytes (seg#)/
-- m/Executor memory: \d+\w? bytes/
-- s/Executor memory: \d+\w? bytes/Executor memory: ### bytes/
-- m/Memory used:\s+\d+\w?B/
-- s/Memory used:\s+\d+\w?B/Memory used:  ###B/
-- m/\d+\w? bytes max \(seg\d+\)/
-- s/\d+\w? bytes max \(seg\d+\)/### bytes max (seg#)/
-- m/Work_mem: \d+\w? bytes max/
-- s/Work_mem: \d+\w? bytes max/Work_mem: ### bytes max/
-- end_matchsubs

CREATE TEMP TABLE empty_table(a int);
-- We used to incorrectly report "never executed" for a node that returns 0 rows
-- from every segment. This was misleading because "never executed" should
-- indicate that the node was never executed by its parent.
-- explain_processing_off
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, SUMMARY OFF) SELECT a FROM empty_table;
-- explain_processing_on

-- For a node that is truly never executed, we still expect "never executed" to
-- be reported
-- explain_processing_off
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, SUMMARY OFF) SELECT t1.a FROM empty_table t1 join empty_table t2 on t1.a = t2.a;
-- explain_processing_on

-- If all QEs hit errors when executing sort, we might not receive stat data for sort.
-- rethrow error before print explain info.
create extension if not exists gp_inject_fault;
create table sort_error_test1(tc1 int, tc2 int);
create table sort_error_test2(tc1 int, tc2 int);
insert into sort_error_test1 select i,i from generate_series(1,20) i;
select gp_inject_fault('explain_analyze_sort_error', 'error', dbid)
    from gp_segment_configuration where role = 'p' and content > -1;
EXPLAIN analyze insert into sort_error_test2 select * from sort_error_test1 order by 1;
select count(*) from sort_error_test2;
select gp_inject_fault('explain_analyze_sort_error', 'reset', dbid)
    from gp_segment_configuration where role = 'p' and content > -1;
drop table sort_error_test1;
drop table sort_error_test2;

--
-- Test correct slice stats reporting in case of duplicated subplans
--

create table slice_test(i int, j int) distributed by (i);
create table slice_test2(i int, j int) distributed by (i);
insert into slice_test select i, i from generate_series(0, 100) i;
insert into slice_test select i, 2*i from generate_series(0, 100) i;
insert into slice_test2 values (0, 1);

-- explain_processing_off
-- create duplicate subplan in QE slice
explain (analyze, timing off, costs off) select a.i from slice_test a
  where a.j = (select b.i from slice_test2 b where a.i = 0 or b.i = 0)
    and a.i = a.j;

-- checking this didn't break previous behavior
-- create multiple initplans in slice 0 (should be printed as two slices)
explain (analyze, timing off, costs off)
  select a.i from (select x::int as i, x::int / 5 as j from round(random() / 5) as x) a
    where a.j = (select round(random() / 5)::int where a.i = 0)
      and a.i = a.j;
-- explain_processing_on

drop table slice_test;
drop table slice_test2;

-- The statistics for modifying CTEs used to be reported as "never executed",
-- when all plan nodes were executed and some stat information was expected.
-- Test QD recieving the stats from all slices and showing it in explain output.
--start_ignore
DROP TABLE IF EXISTS with_dml;
--end_ignore
CREATE TABLE with_dml (i int, j int) DISTRIBUTED BY (i);
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF)
WITH cte AS (
    INSERT INTO with_dml SELECT i, i * 100 FROM generate_series(1,5) i
    RETURNING i
) SELECT * FROM cte;
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF)
WITH cte AS (
    UPDATE with_dml SET j = j + 1
    RETURNING i
) SELECT * FROM cte;
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF)
WITH cte AS (
    DELETE FROM with_dml WHERE i > 0
    RETURNING i
) SELECT * FROM cte;
DROP TABLE with_dml;

-- Test that EXPLAIN ANALYZE of shared CTE inside InitPlans doesn't segfault
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) 
WITH mat_cte AS MATERIALIZED (SELECT oid FROM pg_class LIMIT 10)
SELECT (SELECT count(*) FROM mat_cte), (SELECT count(*) FROM mat_cte);
