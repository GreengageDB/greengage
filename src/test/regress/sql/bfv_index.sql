-- tests index filter with outer refs
-- start_matchsubs
-- m/\(cost=.*\)/
-- s/\(cost=.*\)//
--
-- m/\(slice\d+; segments: \d+\)/
-- s/\(slice\d+; segments: \d+\)//
-- end_matchsubs
drop table if exists bfv_tab1;

CREATE TABLE bfv_tab1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) distributed by (unique1);

create index bfv_tab1_idx1 on bfv_tab1 using btree(unique1);
explain select * from bfv_tab1, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE bfv_tab1.unique1 = v.i and bfv_tab1.stringu1 = v.j;

set gp_enable_relsize_collection=on;
explain select * from bfv_tab1, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE bfv_tab1.unique1 = v.i and bfv_tab1.stringu1 = v.j;

-- Test that we do not choose to perform an index scan if indisvalid=false.
create table bfv_tab1_with_invalid_index (like bfv_tab1 including indexes);
set allow_system_table_mods=on;
update pg_index set indisvalid=false where indrelid='bfv_tab1_with_invalid_index'::regclass;
reset allow_system_table_mods;
explain select * from bfv_tab1_with_invalid_index where unique1>42;
-- Cannot currently upgrade table with invalid index
-- (see https://github.com/GreengageDB/greengage/issues/10805).
drop table bfv_tab1_with_invalid_index;

reset gp_enable_relsize_collection;

--start_ignore
DROP TABLE IF EXISTS bfv_tab2_facttable1;
DROP TABLE IF EXISTS bfv_tab2_dimdate;
DROP TABLE IF EXISTS bfv_tab2_dimtabl1;
--end_ignore

-- Bug-fix verification for MPP-25537: PANIC when bitmap index used in ORCA select
CREATE TABLE bfv_tab2_facttable1 (
col1 integer,
wk_id smallint,
id integer
)
with (appendonly=true, orientation=column, compresstype=zlib, compresslevel=5)
partition by range (wk_id) (
start (1::smallint) END (20::smallint) inclusive every (1),
default partition dflt
)
;

insert into bfv_tab2_facttable1 select col1, col1, col1 from (select generate_series(1,20) col1)a;

CREATE TABLE bfv_tab2_dimdate (
wk_id smallint,
col2 date
)
;

insert into bfv_tab2_dimdate select col1, current_date - col1 from (select generate_series(1,20,2) col1)a;

CREATE TABLE bfv_tab2_dimtabl1 (
id integer,
col2 integer
)
;

insert into bfv_tab2_dimtabl1 select col1, col1 from (select generate_series(1,20,3) col1)a;

CREATE INDEX idx_bfv_tab2_facttable1 on bfv_tab2_facttable1 (id); 

--start_ignore
set optimizer_analyze_root_partition to on;
--end_ignore

ANALYZE bfv_tab2_facttable1;
ANALYZE bfv_tab2_dimdate;
ANALYZE bfv_tab2_dimtabl1;

SELECT count(*) 
FROM bfv_tab2_facttable1 ft, bfv_tab2_dimdate dt, bfv_tab2_dimtabl1 dt1
WHERE ft.wk_id = dt.wk_id
AND ft.id = dt1.id;

explain SELECT count(*) 
FROM bfv_tab2_facttable1 ft, bfv_tab2_dimdate dt, bfv_tab2_dimtabl1 dt1
WHERE ft.wk_id = dt.wk_id
AND ft.id = dt1.id;

explain SELECT count(*)
FROM bfv_tab2_facttable1 ft, bfv_tab2_dimdate dt, bfv_tab2_dimtabl1 dt1
WHERE ft.wk_id = dt.wk_id
AND ft.id = dt1.id;

-- start_ignore
create language plpython3u;
-- end_ignore

create or replace function count_index_scans(explain_query text) returns int as
$$
rv = plpy.execute(explain_query)
search_text = 'Index Scan'
result = 0
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        result = result+1
return result
$$
language plpython3u;

DROP TABLE bfv_tab1;
DROP TABLE bfv_tab2_facttable1;
DROP TABLE bfv_tab2_dimdate;
DROP TABLE bfv_tab2_dimtabl1;

-- pick index scan when query has a relabel on the index key: non partitioned tables

set enable_seqscan = off;

-- start_ignore
drop table if exists Tab23383;
-- end_ignore

create table Tab23383(a int, b varchar(20));
insert into Tab23383 select g,g from generate_series(1,1000) g;
create index Tab23383_b on Tab23383(b);

-- start_ignore
select disable_xform('CXformGet2TableScan');
-- end_ignore

select count_index_scans('explain select * from Tab23383 where b=''1'';');
select * from Tab23383 where b='1';

select count_index_scans('explain select * from Tab23383 where ''1''=b;');
select * from Tab23383 where '1'=b;

select count_index_scans('explain select * from Tab23383 where ''2''> b order by a limit 10;');
select * from Tab23383 where '2'> b order by a limit 10;

select count_index_scans('explain select * from Tab23383 where b between ''1'' and ''2'' order by a limit 10;');
select * from Tab23383 where b between '1' and '2' order by a limit 10;

-- predicates on both index and non-index key
select count_index_scans('explain select * from Tab23383 where b=''1'' and a=''1'';');
select * from Tab23383 where b='1' and a='1';

--negative tests: no index scan plan possible, fall back to planner
select count_index_scans('explain select * from Tab23383 where b::int=''1'';');

drop table Tab23383;

-- pick index scan when query has a relabel on the index key: partitioned tables
-- start_ignore
drop table if exists Tbl23383_partitioned;
-- end_ignore

create table Tbl23383_partitioned(a int, b varchar(20), c varchar(20), d varchar(20))
partition by range(a)
(partition p1 start(1) end(500),
partition p2 start(500) end(1001));
insert into Tbl23383_partitioned select g,g,g,g from generate_series(1,1000) g;
create index idx23383_b on Tbl23383_partitioned(b);

-- heterogenous indexes
create index idx23383_c on Tbl23383_partitioned_1_prt_p1(c);
create index idx23383_cd on Tbl23383_partitioned_1_prt_p2(c,d);
set optimizer_enable_dynamictablescan = off;
select count_index_scans('explain select * from Tbl23383_partitioned where b=''1''');
select * from Tbl23383_partitioned where b='1';

select count_index_scans('explain select * from Tbl23383_partitioned where ''1''=b');
select * from Tbl23383_partitioned where '1'=b;

select count_index_scans('explain select * from Tbl23383_partitioned where ''2''> b order by a limit 10;');
select * from Tbl23383_partitioned where '2'> b order by a limit 10;

select count_index_scans('explain select * from Tbl23383_partitioned where b between ''1'' and ''2'' order by a limit 10;');
select * from Tbl23383_partitioned where b between '1' and '2' order by a limit 10;

-- predicates on both index and non-index key
select count_index_scans('explain select * from Tbl23383_partitioned where b=''1'' and a=''1'';');
select * from Tbl23383_partitioned where b='1' and a='1';

--negative tests: no index scan plan possible, fall back to planner
select count_index_scans('explain select * from Tbl23383_partitioned where b::int=''1'';');

-- heterogenous indexes
select count_index_scans('explain select * from Tbl23383_partitioned where c=''1'';');
select * from Tbl23383_partitioned where c='1';

-- start_ignore
drop table Tbl23383_partitioned;
-- end_ignore

reset enable_seqscan;

-- negative test: due to non compatible cast and CXformGet2TableScan disabled no index plan possible, fallback to planner

-- start_ignore
drop table if exists tbl_ab;
-- end_ignore

create table tbl_ab(a int, b int);
create index idx_ab_b on tbl_ab(b);

-- start_ignore
select disable_xform('CXformGet2TableScan');
-- end_ignore

explain select * from tbl_ab where b::oid=1;

drop table tbl_ab;
drop function count_index_scans(text);
-- start_ignore
select enable_xform('CXformGet2TableScan');
-- end_ignore

--
-- Check that ORCA can use an index for joins on quals like:
--
-- indexkey CMP expr
-- expr CMP indexkey
--
-- where expr is a scalar expression free of index keys and may have outer
-- references.
--
create table nestloop_x (i int, j int) distributed by (i);
create table nestloop_y (i int, j int) distributed by (i);
insert into nestloop_x select g, g from generate_series(1, 20) g;
insert into nestloop_y select g, g from generate_series(1, 7) g;
create index nestloop_y_idx on nestloop_y (j);

-- Coerce the Postgres planner to produce a similar plan. Nested loop joins
-- are not enabled by default. And to dissuade it from choosing a sequential
-- scan, bump up the cost. enable_seqscan=off  won't help, because there is
-- no other way to scan table 'x', and once the planner chooses a seqscan for
-- one table, it will happily use a seqscan for other tables as well, despite
-- enable_seqscan=off. (On PostgreSQL, enable_seqscan works differently, and
-- just bumps up the cost of a seqscan, so it would work there.)
set seq_page_cost=10000000;
set enable_indexscan=on;
set enable_nestloop=on;

explain select * from nestloop_x as x, nestloop_y as y where x.i + x.j < y.j;
select * from nestloop_x as x, nestloop_y as y where x.i + x.j < y.j;

explain select * from nestloop_x as x, nestloop_y as y where y.j > x.i + x.j + 2;
select * from nestloop_x as x, nestloop_y as y where y.j > x.i + x.j + 2;

drop table nestloop_x, nestloop_y;

SET enable_seqscan = OFF;
SET enable_indexscan = ON;

DROP TABLE IF EXISTS bpchar_ops;
CREATE TABLE bpchar_ops(id INT8, v char(10)) DISTRIBUTED BY(id);
CREATE INDEX bpchar_ops_btree_idx ON bpchar_ops USING btree(v bpchar_pattern_ops);
INSERT INTO bpchar_ops VALUES (0, 'row');
SELECT * FROM bpchar_ops WHERE v = 'row '::char(20);

DROP TABLE bpchar_ops;


--
-- Test index rechecks with AO and AOCS tables (and heaps as well, for good measure)
--
create table shape_heap (c circle) with (appendonly=false);
create table shape_ao (c circle) with (appendonly=true, orientation=row);
create table shape_aocs (c circle) with (appendonly=true, orientation=column);

insert into shape_heap values ('<(0,0), 5>');
insert into shape_ao   values ('<(0,0), 5>');
insert into shape_aocs values ('<(0,0), 5>');

create index shape_heap_bb_idx on shape_heap using gist(c);
create index shape_ao_bb_idx   on shape_ao   using gist(c);
create index shape_aocs_bb_idx on shape_aocs using gist(c);

select c && '<(5,5), 1>'::circle,
       c && '<(5,5), 2>'::circle,
       c && '<(5,5), 3>'::circle
from shape_heap;

-- Test the same values with (bitmap) index scans
--
-- The first two values don't overlap with the value in the tables, <(0,0), 5>,
-- but their bounding boxes do. In a GiST index scan that uses the bounding
-- boxes, these will fetch the row from the index, but filtered out by the
-- recheck using the actual overlap operator. The third entry is sanity check
-- that the index returns any rows.
set enable_seqscan=off;
set enable_indexscan=off;
set enable_bitmapscan=on;

-- Use EXPLAIN to verify that these use a bitmap index scan
explain select * from shape_heap where c && '<(5,5), 1>'::circle;
explain select * from shape_ao   where c && '<(5,5), 1>'::circle;
explain select * from shape_aocs where c && '<(5,5), 1>'::circle;

-- Test that they return correct results.
select * from shape_heap where c && '<(5,5), 1>'::circle;
select * from shape_ao   where c && '<(5,5), 1>'::circle;
select * from shape_aocs where c && '<(5,5), 1>'::circle;

select * from shape_heap where c && '<(5,5), 2>'::circle;
select * from shape_ao   where c && '<(5,5), 2>'::circle;
select * from shape_aocs where c && '<(5,5), 2>'::circle;

select * from shape_heap where c && '<(5,5), 3>'::circle;
select * from shape_ao   where c && '<(5,5), 3>'::circle;
select * from shape_aocs where c && '<(5,5), 3>'::circle;

--
-- Given a table with different column types
--
CREATE TABLE table_with_reversed_index(a int, b bool, c text);

--
-- And it has an index that is ordered differently than columns on the table.
--
CREATE INDEX ON table_with_reversed_index(c, a);
INSERT INTO table_with_reversed_index VALUES (10, true, 'ab');

--
-- Then an index only scan should succeed. (i.e. varattno is set up correctly)
--
SET enable_seqscan=off;
SET enable_bitmapscan=off;
SET optimizer_enable_tablescan=off;
SET optimizer_enable_indexscan=off;
SET optimizer_enable_indexonlyscan=on;
EXPLAIN SELECT c, a FROM table_with_reversed_index WHERE a > 5;
SELECT c, a FROM table_with_reversed_index WHERE a > 5;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET optimizer_enable_tablescan;
RESET optimizer_enable_indexscan;
RESET optimizer_enable_indexonlyscan;

--
-- Test Hash indexes
--

CREATE TABLE hash_tbl (a int, b int) DISTRIBUTED BY(a);
INSERT INTO hash_tbl select i,i FROM generate_series(1, 100)i;
ANALYZE hash_tbl;
CREATE INDEX hash_idx1 ON hash_tbl USING hash(b);

-- Now check the results by turning on indexscan
SET enable_seqscan = ON;
SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;

SET optimizer_enable_tablescan =ON;
SET optimizer_enable_indexscan = ON;
SET optimizer_enable_bitmapscan = OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl WHERE b=3;
SELECT * FROM hash_tbl WHERE b=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl WHERE b=3 and a=3;
SELECT * FROM hash_tbl WHERE b=3 and a=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl WHERE b=3 or b=5;
SELECT * FROM hash_tbl WHERE b=3 or b=5;

-- Now check the results by turning on bitmapscan
SET enable_seqscan = OFF;
SET enable_indexscan = OFF;
SET enable_bitmapscan = ON;

SET optimizer_enable_tablescan =OFF;
SET optimizer_enable_indexscan = OFF;
SET optimizer_enable_bitmapscan = ON;

EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl WHERE b=3;
SELECT * FROM hash_tbl WHERE b=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl WHERE b=3 and a=3;
SELECT * FROM hash_tbl WHERE b=3 and a=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl WHERE b=3 or b=5;
SELECT * FROM hash_tbl WHERE b=3 or b=5;

DROP INDEX hash_idx1;
DROP TABLE hash_tbl;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET optimizer_enable_tablescan;
RESET optimizer_enable_indexscan;
RESET optimizer_enable_bitmapscan;

-- Test Hash indexes with AO tables
CREATE TABLE hash_tbl_ao (a int, b int) WITH (appendonly = true) DISTRIBUTED BY(a);
INSERT INTO hash_tbl_ao select i,i FROM generate_series(1, 100)i;
ANALYZE hash_tbl_ao;
CREATE INDEX hash_idx2 ON hash_tbl_ao USING hash(b);

-- get results for comparison purposes
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl_ao WHERE b=3;
SELECT * FROM hash_tbl_ao WHERE b=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl_ao WHERE b=3 and a=3;
SELECT * FROM hash_tbl_ao WHERE b=3 and a=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl_ao WHERE b=3 or b=5;
SELECT * FROM hash_tbl_ao WHERE b=3 or b=5;

-- Now check the results by turning off seqscan/tablescan
SET enable_seqscan = OFF;
SET optimizer_enable_tablescan =OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl_ao WHERE b=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl_ao WHERE b=3 and a=3;
EXPLAIN (COSTS OFF)
SELECT * FROM hash_tbl_ao WHERE b=3 or b=5;

DROP INDEX hash_idx2;
DROP TABLE hash_tbl_ao;
RESET enable_seqscan;
RESET optimizer_enable_tablescan;
-- Test hash indexes with partition table

CREATE TABLE hash_prt_tbl (a int, b int) DISTRIBUTED BY(a) PARTITION BY RANGE(a)
(PARTITION p1 START (1) END (500) INCLUSIVE,
PARTITION p2 START(501) END (1000) INCLUSIVE);
INSERT INTO hash_prt_tbl select i,i FROM generate_series(1, 1000)i;
ANALYZE hash_prt_tbl;
CREATE INDEX hash_idx3 ON hash_prt_tbl USING hash(b);

-- Now check the results by turning off dynamictablescan/seqscan
SET enable_seqscan = OFF;
SET optimizer_enable_dynamictablescan =OFF;

EXPLAIN (COSTS OFF)
SELECT * FROM hash_prt_tbl WHERE b=3;

EXPLAIN (COSTS OFF)
SELECT * FROM hash_prt_tbl WHERE b=3 and a=3;

EXPLAIN (COSTS OFF)
SELECT * FROM hash_prt_tbl WHERE b=3 or b=5;

DROP INDEX hash_idx3;
DROP TABLE hash_prt_tbl;

RESET enable_seqscan;
RESET optimizer_enable_dynamictablescan;

--
-- Test ORCA generates Bitmap/IndexScan alternative for ScalarArrayOpExpr ANY only
--

CREATE TABLE bitmap_alt (id int, bitmap_idx_col int, btree_idx_col int, hash_idx_col int);
CREATE INDEX bitmap_alt_idx1 on bitmap_alt using bitmap(bitmap_idx_col);
CREATE INDEX bitmap_alt_idx2 on bitmap_alt using btree(btree_idx_col);
CREATE INDEX bitmap_alt_idx3 on bitmap_alt using hash(hash_idx_col);
INSERT INTO bitmap_alt SELECT i, i, i, i from generate_series(1,10)i;
ANALYZE bitmap_alt;

-- ORCA should generate bitmap index scan plans for the following
EXPLAIN (COSTS OFF)
SELECT * FROM bitmap_alt WHERE bitmap_idx_col IN (3, 5);
SELECT * FROM bitmap_alt WHERE bitmap_idx_col IN (3, 5);
EXPLAIN (COSTS OFF)
SELECT * FROM bitmap_alt WHERE btree_idx_col IN (3, 5);
SELECT * FROM bitmap_alt WHERE btree_idx_col IN (3, 5);
EXPLAIN (COSTS OFF)
SELECT * FROM bitmap_alt WHERE hash_idx_col IN (3, 5);
SELECT * FROM bitmap_alt WHERE hash_idx_col IN (3, 5);

-- ORCA should generate seq scan plans for the following
EXPLAIN (COSTS OFF)
SELECT * FROM bitmap_alt WHERE bitmap_idx_col=ALL(ARRAY[3, 5]);
SELECT * FROM bitmap_alt WHERE bitmap_idx_col=ALL(ARRAY[3, 5]);
EXPLAIN (COSTS OFF)
SELECT * FROM bitmap_alt WHERE btree_idx_col=ALL(ARRAY[3, 5]);
SELECT * FROM bitmap_alt WHERE btree_idx_col=ALL(ARRAY[3, 5]);
EXPLAIN (COSTS OFF)
SELECT * FROM bitmap_alt WHERE hash_idx_col=ALL(ARRAY[3, 5]);
SELECT * FROM bitmap_alt WHERE hash_idx_col=ALL(ARRAY[3, 5]);

--
-- Test ORCA considers ScalarArrayOp in indexqual for partitioned table
-- with multikey indexes only when predicate key is the first index key
-- (similar test for non-partitioned tables in create_index)
--
CREATE TABLE pt_with_multikey_index (a int, key1 char(6), key2 char(1))
PARTITION BY list(key2)
(PARTITION p1 VALUES ('R'), PARTITION p2 VALUES ('G'), DEFAULT PARTITION other);

CREATE INDEX multikey_idx on pt_with_multikey_index (key1, key2);
INSERT INTO pt_with_multikey_index SELECT i, 'KEY'||i, 'R' from generate_series(1,500)i;
INSERT INTO pt_with_multikey_index SELECT i, 'KEY'||i, 'G' from generate_series(1,500)i;
INSERT INTO pt_with_multikey_index SELECT i, 'KEY'||i, 'B' from generate_series(1,500)i;

explain (costs off)
SELECT key1 FROM pt_with_multikey_index
WHERE key1 IN ('KEY55', 'KEY65', 'KEY5')
ORDER BY key1;

SELECT key1 FROM pt_with_multikey_index
WHERE key1 IN ('KEY55', 'KEY65', 'KEY5')
ORDER BY key1;

EXPLAIN (costs off)
SELECT * FROM  pt_with_multikey_index
WHERE key1 = 'KEY55' AND key2 IN ('R', 'G')
ORDER BY key2;

SELECT * FROM  pt_with_multikey_index
WHERE key1 = 'KEY55' AND key2 IN ('R', 'G')
ORDER BY key2;

EXPLAIN (costs off)
SELECT * FROM  pt_with_multikey_index
WHERE key1 IN ('KEY55', 'KEY65') AND key2 = 'R'
ORDER BY key1;

SELECT * FROM  pt_with_multikey_index
WHERE key1 IN ('KEY55', 'KEY65') AND key2 = 'R'
ORDER BY key1;

--
-- Enable the index only scan in append only table.
-- Note: expect ORCA to use seq scan rather than index only scan like planner,
-- because ORCA hasn't yet implemented index only scan for AO/CO tables.
--
CREATE TABLE bfv_index_only_ao(a int, b int) WITH (appendonly =true);
CREATE INDEX bfv_index_only_ao_a_b on bfv_index_only_ao(a) include (b);

insert into bfv_index_only_ao select i,i from generate_series(1, 10000) i;

explain select count(*) from bfv_index_only_ao where a < 100;
select count(*) from bfv_index_only_ao where a < 100;
explain select count(*) from bfv_index_only_ao where a < 1000;
select count(*) from bfv_index_only_ao where a < 1000;

CREATE TABLE bfv_index_only_aocs(a int, b int) WITH (appendonly =true, orientation=column);
CREATE INDEX bfv_index_only_aocs_a_b on bfv_index_only_aocs(a) include (b);

insert into bfv_index_only_aocs select i,i from generate_series(1, 10000) i;

explain select count(*) from bfv_index_only_aocs where a < 100;
select count(*) from bfv_index_only_aocs where a < 100;
explain select count(*) from bfv_index_only_aocs where a < 1000;
select count(*) from bfv_index_only_aocs where a < 1000;

-- The following tests are to verify a fix that allows ORCA to
-- choose the bitmap index scan alternative when the predicate
-- is in the form of `value operator cast(column)`. The fix
-- converts the scalar comparison expression to the more common 
-- form of `cast(column) operator value` in the preprocessor.

-- Each test includes two queries. One query's predicate has 
-- the column on the left side, and the other has the column
-- on the right side. We expect the two queries to generate
-- identical plans with bitmap index scan.

-- Index only scan will probably be selected once index only
-- scan in enabled for AO tables in ORCA. To prevent retain
-- the bitmap scan alternative, turn off index only scan.
set optimizer_enable_indexonlyscan=off;
set enable_indexonlyscan=off;
-- Test AO table
-- Index scan is disabled in AO table, so bitmap scan is the
-- most performant
create table ao_tbl (
    path_hash character varying(10)
) with (appendonly='true');
create index ao_idx on ao_tbl using btree (path_hash);
insert into ao_tbl select 'abc' from generate_series(1,20) i;
analyze ao_tbl;
-- identical plans
explain select * from ao_tbl where path_hash = 'ABC'; 
explain select * from ao_tbl where 'ABC' = path_hash;

-- Test AO partition table
-- Dynamic index scan is disabled in AO table, so dynamic bitmap
-- scan is the most performant
create table part_tbl (
    path_hash character varying(10)
) partition by list(path_hash) 
          (partition pics values('a') , 
          default partition other with (appendonly='true'));
create index part_idx on part_tbl using btree (path_hash);
insert into part_tbl select 'abc' from generate_series(1,20) i;
analyze part_tbl;
-- identical plans
explain select * from part_tbl where path_hash = 'ABC'; 
explain select * from part_tbl where 'ABC' = path_hash; 

-- Test table indexed on two columns
-- Two indices allow ORCA to generate the bitmap scan alternative
create table two_idx_tbl (x varchar(10), y varchar(10));
create index x_idx on two_idx_tbl using btree (x);
create index y_idx on two_idx_tbl using btree (y);
insert into two_idx_tbl select 'aa', 'bb' from generate_series(1,10000) i;
analyze two_idx_tbl;
-- encourage bitmap scan by discouraging index scan
set optimizer_enable_indexscan=off;
-- identical plans
explain select * from two_idx_tbl where x = 'cc' or y = 'dd';
explain select * from two_idx_tbl where 'cc' = x or 'dd' = y;

RESET optimizer_enable_indexscan;
RESET optimizer_enable_indexonlyscan;
RESET enable_indexonlyscan;
RESET seq_page_cost;

-- Test IndexNLJoin on IndexOnlyScan in ORCA (both heap and AOCS table)
create table index_only_join_test (a int, b int) distributed by (a);
create table index_only_join_test_aocs (a int, b int) with (appendonly='true') distributed by (a);

create index index_only_join_test_a_idx on index_only_join_test(a);
create index index_only_join_test_b_idx on index_only_join_test(b) include (a);
create index index_only_join_test_aocs_a_idx on index_only_join_test_aocs(a);
create index index_only_join_test_aocs_b_idx on index_only_join_test_aocs(b) include (a);
insert into index_only_join_test select i,i from generate_series(1, 100)i;
insert into index_only_join_test_aocs select i,i from generate_series(1, 100)i;
analyze index_only_join_test;
analyze index_only_join_test_aocs;

set enable_nestloop to on;
set enable_seqscan to off;
set optimizer_enable_indexscan to off;
explain select t1.a from index_only_join_test t1, index_only_join_test t2 where t1.a = t2.a and t1.b < 10;
reset optimizer_enable_indexscan;
explain select t1.a from index_only_join_test_aocs t1, index_only_join_test_aocs t2 where t1.a = t2.a and t1.b < 10;
reset enable_nestloop;
reset enable_seqscan;

drop table index_only_join_test;
drop table index_only_join_test_aocs;
