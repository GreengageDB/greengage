--
-- Queries that lead to hanging (not dead lock) when we don't handle synchronization properly in shared scan
-- Queries that lead to wrong result when we don't finish executing the subtree below the shared scan being squelched.
--

CREATE SCHEMA shared_scan;

SET search_path = shared_scan;

CREATE TABLE foo (a int, b int);
CREATE TABLE bar (c int, d int);
CREATE TABLE jazz(e int, f int);

INSERT INTO foo values (1, 2);
INSERT INTO bar SELECT i, i from generate_series(1, 100)i;
INSERT INTO jazz VALUES (2, 2), (3, 3);

ANALYZE foo;
ANALYZE bar;
ANALYZE jazz;

SELECT $query$
SELECT * FROM
        (
        WITH cte AS (SELECT * FROM foo)
        SELECT * FROM (SELECT * FROM cte UNION ALL SELECT * FROM cte)
        AS X
        JOIN bar ON b = c
        ) AS XY
        JOIN jazz on c = e AND b = f;
$query$ AS qry \gset

-- We are very particular about this plan shape and data distribution with ORCA:
-- 1. `jazz` has to be the inner table of the outer HASH JOIN, so that on a
-- segment which has zero tuples in `jazz`, the Sequence node that contains the
-- Shared Scan will be squelched on that segment. If `jazz` is not on the inner
-- side, the above mentioned "hang" scenario will not be covered.
-- 2. The Shared Scan producer has to be on a different slice from consumers,
-- and some tuples coming out of the Share Scan producer on one segments are
-- redistributed to a different segment over Motion. If not, the above mentioned
-- "wrong result" scenario will not be covered.
EXPLAIN (COSTS OFF)
:qry ;

SET statement_timeout = '15s';

:qry ;

RESET statement_timeout;

-- If a Shared Scan is in Subplan, then disuse it and back to normal scan.
SELECT COUNT(*)
FROM (SELECT *,
        (
        WITH cte AS (SELECT * FROM jazz WHERE jazz.e = bar.c)
        SELECT 1 FROM cte c1, cte c2
        )
      FROM bar) as s;

CREATE TABLE t1 (a int, b int);
CREATE TABLE t2 (a int);

-- ORCA plan contains a Shared Scan producer with a unsorted Motion below it
EXPLAIN (COSTS OFF)
WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 LIMIT 10) SELECT a, 1, 1 FROM cte JOIN t2 USING (a);
-- This functions returns one more column than expected.
CREATE OR REPLACE FUNCTION col_mismatch_func1() RETURNS TABLE (field1 int, field2 int)
LANGUAGE 'plpgsql' VOLATILE STRICT AS
$$
DECLARE
   v_qry text;
BEGIN
   v_qry := 'WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 LIMIT 10) SELECT a, 1 , 1 FROM cte JOIN t2 USING (a)';
  RETURN QUERY EXECUTE v_qry;
END
$$;

-- This should only ERROR and should not SIGSEGV
SELECT col_mismatch_func1();

-- ORCA plan contains a Shared Scan producer with a sorted Motion below it
EXPLAIN (COSTS OFF)
WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 ORDER BY b LIMIT 10) SELECT a, 1, 1 FROM cte JOIN t2 USING (a);
--- This functions returns one more column than expected.
CREATE OR REPLACE FUNCTION col_mismatch_func2() RETURNS TABLE (field1 int, field2 int)
    LANGUAGE 'plpgsql' VOLATILE STRICT AS
$$
DECLARE
    v_qry text;
BEGIN
    v_qry := 'WITH cte AS (SELECT * FROM t1 WHERE random() < 0.1 ORDER BY b LIMIT 10) SELECT a, 1 , 1 FROM cte JOIN t2 USING (a)';
    RETURN QUERY EXECUTE v_qry;
END
$$;

-- This should only ERROR and should not SIGSEGV
SELECT col_mismatch_func2();

-- planner didn't support shared scan under subplan
create table sisc1(a int, b int);
create table sisc2(a int, b int);
create table sisc3(a int, b int);

explain (COSTS OFF)
with cte1 as (select * from sisc1),
cte2 as (select * from sisc2),
cte3 as (select * from sisc3)
select * from cte1 where ( EXISTS ( select cte2.a from cte2 left join cte3 on (EXISTS ( select cte1.b from cte2))));

with cte1 as (select * from sisc1),
cte2 as (select * from sisc2),
cte3 as (select * from sisc3)
select * from cte1 where ( EXISTS ( select cte2.a from cte2 left join cte3 on (EXISTS ( select cte1.b from cte2))));

set gp_cte_sharing = on;

with cte1 as (select * from sisc1),
cte2 as (select * from sisc2),
cte3 as (select * from sisc3)
select * from cte1 where ( EXISTS ( select cte2.a from cte2 left join cte3 on (EXISTS ( select cte1.b from cte2))));

reset gp_cte_sharing;
drop table sisc1;
drop table sisc2;
drop table sisc3;

--
-- Check error handling in shared scans
--
-- Helper function to count the number of temporary files in
-- pgsql_tmp.

-- start_ignore
create language plpython3u;
-- end_ignore

create or replace function get_temp_file_num() returns setof int as
$$
import os
fileNum = 0
for root, directories, filenames in os.walk('base/pgsql_tmp'):
  for filename in filenames:
    fileNum += 1
return [fileNum]
$$ language plpython3u execute on all segments;

create table sisc(i int) distributed by (i);
insert into sisc select generate_series(1, 100);

-- Temp file number before running Shared Scan queries
select sum(n) as num_temp_files_before from get_temp_file_num() n
\gset

-- No error, 2 SISC nodes
explain (verbose, costs off)
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2 where t1.i = 1;

with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2 where t1.i = 1;

-- Error, 2 SISC nodes
explain (verbose, costs off)
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2 where t1.i/0 = 1;

with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2 where t1.i/0 = 1;

-- Explicit transaction, 2 SISC nodes
begin;
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2 where t1.i/0 = 1;
rollback;

-- Subtransaction, 2 SISC nodes
begin;
savepoint s1;
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2 where t1.i/0 = 1;
rollback to s1;
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2 where t1.i/0 = 1;
rollback;

-- No error, 3 SISC nodes
explain (verbose, costs off)
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i = 1;

with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i = 1;

-- Error, 3 SISC nodes
explain (verbose, costs off)
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i/0 = 1;

with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i/0 = 1;

-- No error, 3 SISC nodes with 2 in the same slice
explain (verbose, costs off)
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i = 1 and t1.i = t2.i;

with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i = 1 and t1.i = t2.i;

-- Error, 3 SISC nodes with 2 in the same slice
explain (verbose, costs off)
with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i/0 = 1 and t1.i = t2.i;

with cte as materialized (select i from sisc) select count(*) from cte t1, cte t2, cte t3 where t1.i/0 = 1 and t1.i = t2.i;

-- No error, 2 different SISCs
explain (verbose, costs off)
with cte1 as materialized (select i from sisc), cte2 as materialized (select i from sisc) select count(*) from cte1 t1, cte1 t2, cte2 t3, cte2 t4 where t1.i = 1;

with cte1 as materialized (select i from sisc), cte2 as materialized (select i from sisc) select count(*) from cte1 t1, cte1 t2, cte2 t3, cte2 t4 where t1.i = 1;

-- Error, 2 different SISCs
explain (verbose, costs off)
with cte1 as materialized (select i from sisc), cte2 as materialized (select i from sisc) select count(*) from cte1 t1, cte1 t2, cte2 t3, cte2 t4 where t1.i/0 = 1;

with cte1 as materialized (select i from sisc), cte2 as materialized (select i from sisc) select count(*) from cte1 t1, cte1 t2, cte2 t3, cte2 t4 where t1.i/0 = 1;

-- All temporary files should have been cleaned up, so the number of files shouldn't be more than
-- previously. It could be less if some previously existing file has been cleaned up in the meantime.
select sum(n) as num_temp_files_after from get_temp_file_num() n
\gset
select :num_temp_files_before >= :num_temp_files_after;
drop table sisc;
drop function get_temp_file_num();
