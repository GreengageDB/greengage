create schema hashagg_spill;
set search_path to hashagg_spill;

-- start_ignore
create language plpythonu;
-- end_ignore

-- force multistage to increase likelihood of spilling
set optimizer_force_multistage_agg = on;

-- set workfile is created to true if all segment did it.
create or replace function hashagg_spill.is_workfile_created(explain_query text)
returns setof int as
$$
import re
query = "select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0;"
rv = plpy.execute(query)
nsegments = int(rv[0]['nsegments'])
rv = plpy.execute(explain_query)
search_text = 'spilling'
result = []
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    if search_text.lower() in cur_line.lower():
        p = re.compile('.+\((segment [\d]+).+ Workfile: \(([\d+]) spilling\)')
        m = p.match(cur_line)
        workfile_created = int(m.group(2))
        cur_row = int(workfile_created == nsegments)
        result.append(cur_row)
return result
$$
language plpythonu;

create table testhagg (i1 int, i2 int, i3 int, i4 int);
insert into testhagg select i,i,i,i from
	(select generate_series(1, nsegments * 15000) as i from
	(select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0) foo) bar;

set statement_mem="1800";
set gp_resqueue_print_operator_memory_limits=on;

-- the number of rows returned by the query varies depending on the number of segments, so
-- only print the first 10
select * from (select max(i1) from testhagg group by i2) foo order by 1 limit 10;
select * from hashagg_spill.is_workfile_created('explain (analyze, verbose) select max(i1) from testhagg group by i2;');
select * from hashagg_spill.is_workfile_created('explain (analyze, verbose) select max(i1) from testhagg group by i2 limit 45000;');


-- Test HashAgg with increasing amount of overflows

reset all;

-- Returns the number of overflows from EXPLAIN ANALYZE output
create or replace function hashagg_spill.num_hashagg_overflows(explain_query text)
returns setof int as
$$
import re
query = "select count(*) as nsegments from gp_segment_configuration where role='p' and content >= 0;"
rv = plpy.execute(query)
rv = plpy.execute(explain_query)
result = []
for i in range(len(rv)):
    cur_line = rv[i]['QUERY PLAN']
    p = re.compile('.+\((seg[\d]+).+ ([\d+]) overflows;')
    m = p.match(cur_line)
    if m:
      overflows = int(m.group(2))
      result.append(overflows)
return result
$$
language plpythonu;

-- Test agg spilling scenarios
drop table if exists aggspill;
create table aggspill (i int, j int, t text) distributed by (i);
insert into aggspill select i, i*2, i::text from generate_series(1, 10000) i;
insert into aggspill select i, i*2, i::text from generate_series(1, 100000) i;
insert into aggspill select i, i*2, i::text from generate_series(1, 1000000) i;

-- No spill with large statement memory 
set statement_mem = '125MB';
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 1) g;

-- Reduce the statement memory to induce spilling
set statement_mem = '10MB';
select overflows >= 1 from hashagg_spill.num_hashagg_overflows('explain analyze
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 2) g') overflows;
select count(*) from (select i, count(*) from aggspill group by i,j having count(*) = 2) g;

-- Reduce the statement memory, nbatches and entrysize even further to cause multiple overflows
set gp_hashagg_default_nbatches = 4;
set statement_mem = '5MB';

select overflows > 1 from hashagg_spill.num_hashagg_overflows('explain analyze
select count(*) from (select i, count(*) from aggspill group by i,j,t having count(*) = 3) g') overflows;

select count(*) from (select i, count(*) from aggspill group by i,j,t having count(*) = 3) g;

reset optimizer_force_multistage_agg;

-- Test the spilling of aggstates
--     with and without serial/deserial functions
--     with and without workfile compression
-- The transition type of numeric is internal, and hence it uses the serial/deserial functions when spilling
-- The transition type value of integer is by Ref, and it does not have any serial/deserial function when spilling
CREATE TABLE hashagg_spill(col1 numeric, col2 int) DISTRIBUTED BY (col1);
INSERT INTO hashagg_spill SELECT id, 1 FROM generate_series(1,20000) id;
ANALYZE hashagg_spill;
SET statement_mem='1000kB';
SET gp_workfile_compression = OFF;
select overflows >= 1 from hashagg_spill.num_hashagg_overflows('explain analyze
SELECT avg(col2) col2 FROM hashagg_spill GROUP BY col1 HAVING(sum(col1)) < 0;') overflows;
SET gp_workfile_compression = ON;
select overflows >= 1 from hashagg_spill.num_hashagg_overflows('explain analyze
SELECT avg(col2) col2 FROM hashagg_spill GROUP BY col1 HAVING(sum(col1)) < 0;') overflows;

-- Test the spilling aggregates for pergroupstate inconsistency
create table spill_pgs1(a bigint, b bigint, c text, d bigint) distributed randomly;
insert into spill_pgs1 select g, null::bigint, 'aaaaaa', g from generate_series(1, 500000) as g;
insert into spill_pgs1 select g, g, 'aaaaaa', g from generate_series(1, 500000) as g;
create index spill_pgs1_idx on spill_pgs1(a) where c = 'aaaaaa' or c = 'bbbbbb';
create table spill_pgs2(a bigint) distributed randomly;
insert into spill_pgs2 select generate_series(1, 500000);
create table spill_pgs3(d bigint, f integer) distributed randomly;
insert into spill_pgs3 select g, 1 from generate_series(300000, 1000000) as g;
set statement_mem='2MB';
set optimizer=off;
select count(q.*) as num from (
    select
        spill_pgs1.a, spill_pgs3.f,
        sum(spill_pgs1.b) filter (where spill_pgs1.c in ('bbbbbb'))
    from spill_pgs1 join spill_pgs2 using(a)
    left join spill_pgs3 using(d)
    where spill_pgs1.c in ('aaaaaa', 'bbbbbb')
    group by 1, 2  
) as q;

-- check spilling to a temp tablespace
CREATE TABLE spill_temptblspace (a numeric) DISTRIBUTED BY (a);
SET temp_tablespaces=pg_default;
INSERT INTO spill_temptblspace SELECT avg(col2) col2 FROM hashagg_spill GROUP BY col1 HAVING(sum(col1)) < 0;
RESET temp_tablespaces;
RESET statement_mem;
RESET gp_workfile_compression;


drop schema hashagg_spill cascade;
