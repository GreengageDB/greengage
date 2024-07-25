drop table if exists _tmp_table;
create table _tmp_table (i1 int, i2 int, i3 int, i4 int);
insert into _tmp_table select i, i % 100, i % 10000, i % 75 from generate_series(0,99999) i;

-- make sort to spill
set statement_mem="2MB";
set gp_cte_sharing=on;

select gp_inject_fault('execsort_sort_mergeruns', 'reset', 2);
-- set QueryFinishPending=true in sort mergeruns. This will stop sort and set result_tape to NULL
select gp_inject_fault('execsort_sort_mergeruns', 'finish_pending', 2);

-- return results although sort will be interrupted in one of the segments 
select DISTINCT S from (select row_number() over(partition by i1 order by i2) AS T, count(*) over (partition by i1) AS S from _tmp_table) AS TMP;

select gp_inject_fault('execsort_sort_mergeruns', 'status', 2);

select gp_inject_fault('execsort_dumptuples', 'reset', 2);
-- set QueryFinishPending=true in sort dumptuples. This will stop sort.
select gp_inject_fault('execsort_dumptuples', 'finish_pending', 2);

-- return results although sort will be interrupted in one of the segments 
select DISTINCT S from (select row_number() over(partition by i1 order by i2) AS T, count(*) over (partition by i1) AS S from _tmp_table) AS TMP;

select gp_inject_fault('execsort_dumptuples', 'status', 2);

-- ORCA does not trigger sort_bounded_heap() in following queries
set optimizer=off;

select gp_inject_fault('execsort_sort_bounded_heap', 'reset', 2);
-- set QueryFinishPending=true in sort_bounded_heap. This will stop sort.
select gp_inject_fault('execsort_sort_bounded_heap', 'finish_pending', 2);

-- return results although sort will be interrupted in one of the segments 
select i1 from _tmp_table order by i2 limit 3;

select gp_inject_fault('execsort_sort_bounded_heap', 'status', 2);

-- test if shared input scan deletes memory correctly when QueryFinishPending and its child has been eagerly freed,
-- where the child is a Sort node
drop table if exists testsisc;
create table testsisc (i1 int, i2 int, i3 int, i4 int); 
insert into testsisc select i, i % 1000, i % 100000, i % 75 from
(select generate_series(1, nsegments * 50000) as i from 
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0) foo) bar; 

set gp_resqueue_print_operator_memory_limits=on;
set statement_mem='2MB';
-- ORCA does not generate SharedInputScan with a Sort node underneath it. For
-- the following query, ORCA disregards the order by inside the cte definition;
-- planner on the other hand does not.
set optimizer=off;
select gp_inject_fault('execshare_input_next', 'reset', 2);
-- Set QueryFinishPending to true after SharedInputScan has retrieved the first tuple. 
-- This will eagerly free the memory context of shared input scan's child node.  
select gp_inject_fault('execshare_input_next', 'finish_pending', 2);

with cte as (select i2 from testsisc order by i2)
select * from cte c1, cte c2 limit 2;

select gp_inject_fault('execshare_input_next', 'status', 2);

-- test if shared input scan deletes memory correctly when QueryFinishPending and its child has been eagerly freed,
-- where the child is a Sort node and sort_mk algorithm is used


select gp_inject_fault('execshare_input_next', 'reset', 2);
-- Set QueryFinishPending to true after SharedInputScan has retrieved the first tuple. 
-- This will eagerly free the memory context of shared input scan's child node.  
select gp_inject_fault('execshare_input_next', 'finish_pending', 2);

with cte as (select i2 from testsisc order by i2)
select * from cte c1, cte c2 limit 2;

select gp_inject_fault('execshare_input_next', 'status', 2);

-- Disable faultinjectors
select gp_inject_fault('execsort_sort_mergeruns', 'reset', 2);
select gp_inject_fault('execsort_dumptuples', 'reset', 2);
select gp_inject_fault('execsort_sort_bounded_heap', 'reset', 2);
select gp_inject_fault('execshare_input_next', 'reset', 2);
reset optimizer;

-- test if a query can be canceled when cancel signal arrives fast than the query dispatched.
create table _tmp_table1 as select i as c1, i as c2 from generate_series(1, 10) i;
create table _tmp_table2 as select i as c1, 0 as c2 from generate_series(0, 10) i;

-- Prevent FTS from probing as "before_read_command" fault interfers
-- with FTS probes.  Request a scan so that the skip fault is
-- triggered immediately, rather that waiting until the next probe
-- interval.
select gp_inject_fault_infinite('fts_probe', 'skip', 1);
select gp_request_fts_probe_scan();
select gp_wait_until_triggered_fault('fts_probe', 1, 1);

-- make one QE sleep before reading command
select gp_inject_fault('before_read_command', 'sleep', '', '', '', 1, 1, 50, 2::smallint);

select count(*) from _tmp_table1, _tmp_table2 where 100 / _tmp_table2.c2 > 1;

select gp_inject_fault('before_read_command', 'reset', 2);
-- Resume FTS probes starting from the next probe interval.
select gp_inject_fault('fts_probe', 'reset', 1);

drop table _tmp_table1;
drop table _tmp_table2;
drop table testsisc;

-- test if a query with dynamic bitmapscan plan does not crash when QueryFinishPending set to true
-- Planner doesn't generate dynamic bitmap heap scan plan for below query.
create table t1_bm(a int, b int, c text, d int)
distributed randomly
partition by range(a)
(
   start (1) end (10) every (1),
   default partition extra
);

create table t2_bm(b int, c text);

create index idx1_bm ON t1_bm USING bitmap (b);

set optimizer_enable_hashjoin = off;
set optimizer_enable_materialize = off;

-- set QueryFinishPending to true befor bitmap heap scan retrieve results from bitmap
select gp_inject_fault('before_retrieve_from_bitmap', 'finish_pending', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 0;

select count(distinct a.d) from t1_bm a, t2_bm b
where a.c = b.c and a.b < 10 and b.b < 10;

select gp_inject_fault('before_retrieve_from_bitmap', 'reset', dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = 0;

reset optimizer_enable_hashjoin;
reset optimizer_enable_materialize;
drop table t1_bm;
drop table t2_bm;
