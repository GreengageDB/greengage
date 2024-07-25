-- we ingore this in global init-file, replace words here
-- so that we can let only this case tested in this sql.
-- start_matchsubs
-- m/WARNING:  creating a table with no columns./
-- s/WARNING:  creating a table with no columns./WARNING:  create a table with no columns (misc_jiras)./
-- end_matchsubs

drop schema if exists misc_jiras;
create schema misc_jiras;

--
-- Test backward scanning of tuplestore spill files.
--
-- When tuplestore cannot store all the data in memory it will spill some of
-- the data to temporary files.  In gpdb we used to disable the backward
-- scanning from these spill files because we could not determine the tuple
-- type, memtup or heaptup, correctly.  The issue is fixed, the backward
-- scanning should be supported now.
--

create table misc_jiras.t1 (c1 int, c2 text, c3 smallint) distributed by (c1);
insert into misc_jiras.t1 select i % 13, md5(i::text), i % 3
  from generate_series(1, 60000) i;

-- tuplestore in windowagg uses statement_mem to control the in-memory data size,
-- set a small value to trigger the spilling.
set statement_mem to '1024kB';

set extra_float_digits=0; -- the last decimal digits are somewhat random

-- Inject fault at 'winagg_after_spool_tuples' to show that the tuplestore spills
-- to disk.
SELECT gp_inject_fault('winagg_after_spool_tuples', 'skip', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content>=0;

select sum(cc) from (
    select c1
         , c2
         , case when count(c3) = 0 then -1.0
                else cume_dist() over (partition by c1,
                                       case when count(c3) > 0 then 1 else 0 end
                                       order by count(c3), c2)
           end as cc
      from misc_jiras.t1
     group by 1, 2
) tt;

SELECT gp_inject_fault('winagg_after_spool_tuples', 'reset', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content>=0;

reset statement_mem;

-- non-ASCII multibyte character should show up correctly in error messages.
select '溋' || (B'1');

-- Github Issue 17271
-- test create zero-column table will throw warning only on QD
-- test policy on each segment (including coordinator)
create table t_17271();
-- coordinator policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_distribution_policy where localoid = 't_17271'::regclass::oid;
-- segment policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_dist_random('gp_distribution_policy') where localoid = 't_17271'::regclass::oid;

create table t1_17271(a int , b int);
create table t2_17271 as select from t1_17271 group by a;

-- coordinator policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_distribution_policy where localoid = 't2_17271'::regclass::oid;
-- segment policy
select localoid::regclass::text, policytype,numsegments,distkey,distclass
from gp_dist_random('gp_distribution_policy') where localoid = 't2_17271'::regclass::oid;

drop table t_17271;
drop table t1_17271;
drop table t2_17271;
