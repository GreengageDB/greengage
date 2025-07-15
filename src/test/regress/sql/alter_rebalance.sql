-- Check 'ALTER TABLE ... REBALANCE' command and 'gp_target_numsegments' GUC

-- Create hashed distributed tables
create table table_distr_hashed(a int) distributed by (a);
insert into table_distr_hashed select generate_series(1, 20);

create table table_distr_hashed_ao_row(a int) with (appendonly=true, orientation=row) distributed by (a);
insert into table_distr_hashed_ao_row select generate_series(1, 20);

create table table_distr_hashed_ao_col(a int) with (appendonly=true, orientation=column) distributed by (a);
insert into table_distr_hashed_ao_col select generate_series(1, 20);

-- Create randomly distributed tables
create table table_distr_random(a int) distributed randomly;
insert into table_distr_random select generate_series(1, 20);

create table table_distr_random_ao_row(a int) with (appendonly=true, orientation=row) distributed randomly;
insert into table_distr_random_ao_row select generate_series(1, 20);

create table table_distr_random_ao_col(a int) with (appendonly=true, orientation=column) distributed randomly;
insert into table_distr_random_ao_col select generate_series(1, 20);

-- Create replicated distributed tables
create table table_distr_replicated(a int) distributed replicated;
insert into table_distr_replicated select generate_series(1, 20);

create table table_distr_replicated_ao_row(a int) with (appendonly=true, orientation=row) distributed replicated;
insert into table_distr_replicated_ao_row select generate_series(1, 20);

create table table_distr_replicated_ao_col(a int) with (appendonly=true, orientation=column) distributed replicated;
insert into table_distr_replicated_ao_col select generate_series(1, 20);

-- Create part tables
create table part_range_table_distr_hashed (a int, b date) distributed by (a)
partition by range (b) (
    start (date '2023-01-01') inclusive
    end (date '2024-01-01') exclusive
    every (interval '1 month'),
    default partition other_vals
);
insert into part_range_table_distr_hashed select i, '2023-01-02' from generate_series(1, 20)i;
insert into part_range_table_distr_hashed select i, '2023-05-02' from generate_series(1, 20)i;
insert into part_range_table_distr_hashed select i, '2020-05-02' from generate_series(1, 20)i;

create table part_range_table_distr_random (a int, b date) distributed randomly
partition by range (b) (
    start (date '2023-01-01') inclusive
    end (date '2024-01-01') exclusive
    every (interval '1 month'),
    default partition other_vals
);
insert into part_range_table_distr_random select i, '2023-01-02' from generate_series(1, 20)i;
insert into part_range_table_distr_random select i, '2023-05-02' from generate_series(1, 20)i;
insert into part_range_table_distr_random select i, '2020-05-02' from generate_series(1, 20)i;

create table part_list_table_distr_hashed(a int, b text) distributed by (a)
partition by list (b) (
    partition part1 values ('test1'),
    partition part2 values ('test2'),
    default partition other_vals
);
insert into part_list_table_distr_hashed select i, 'test1' from generate_series(1, 20)i;
insert into part_list_table_distr_hashed select i, 'test2' from generate_series(1, 20)i;
insert into part_list_table_distr_hashed select i, 'test3' from generate_series(1, 20)i;

create table part_list_table_distr_random(a int, b text) distributed randomly
partition by list (b) (
    partition part1 values ('test1'),
    partition part2 values ('test2'),
    default partition other_vals
);
insert into part_list_table_distr_random select i, 'test1' from generate_series(1, 20)i;
insert into part_list_table_distr_random select i, 'test2' from generate_series(1, 20)i;
insert into part_list_table_distr_random select i, 'test3' from generate_series(1, 20)i;

create table multi_part_table_distr_hashed(a int, b date, c text) distributed by (a)
partition by range (b)
subpartition by list (c) subpartition template
(
    subpartition subpart1 values ('test1'),
    subpartition subpart2 values ('test2')
)
(
    partition part1 start (date '2023-01-01'),
    partition part2 start (date '2023-02-01'),
	partition part3 start (date '2023-03-01') end (date '2024-01-01')
);
insert into multi_part_table_distr_hashed select i, '2023-01-05', 'test1' from generate_series(1, 20)i;
insert into multi_part_table_distr_hashed select i, '2023-02-05', 'test2' from generate_series(1, 20)i;
insert into multi_part_table_distr_hashed select i, '2023-03-05', 'test1' from generate_series(1, 20)i;

-- Now check shrink of the created tables into 2 segments
set gp_target_numsegments = 2;

alter table table_distr_hashed rebalance;
alter table table_distr_hashed_ao_row rebalance;
alter table table_distr_hashed_ao_col rebalance;

alter table table_distr_random rebalance;
alter table table_distr_random_ao_row rebalance;
alter table table_distr_random_ao_col rebalance;

alter table table_distr_replicated rebalance;
alter table table_distr_replicated_ao_row rebalance;
alter table table_distr_replicated_ao_col rebalance;

alter table part_range_table_distr_hashed rebalance;
alter table part_range_table_distr_random rebalance;

alter table part_list_table_distr_hashed rebalance;
alter table part_list_table_distr_random rebalance;

alter table multi_part_table_distr_hashed rebalance; 

-- Verify that data is presented only on segments #0 and #1
select a, gp_segment_id from table_distr_hashed order by a;
select a, gp_segment_id from table_distr_hashed_ao_row order by a;
select a, gp_segment_id from table_distr_hashed_ao_col order by a;

select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random order by a;
select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random_ao_row order by a;
select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random_ao_col order by a;

select a from table_distr_replicated order by a;
select a from table_distr_replicated_ao_row order by a;
select a from table_distr_replicated_ao_col order by a;

select *, gp_segment_id from part_range_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from part_range_table_distr_random order by a, b;

select *, gp_segment_id from part_list_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from part_list_table_distr_random order by a, b;

select *, gp_segment_id from multi_part_table_distr_hashed order by a, b, c;

-- Check that new data is added only to reduced set of segments
reset gp_target_numsegments;

insert into table_distr_hashed select generate_series(21, 40);
insert into table_distr_hashed_ao_row select generate_series(21, 40);
insert into table_distr_hashed_ao_col select generate_series(21, 40);

insert into table_distr_random select generate_series(21, 40);
insert into table_distr_random_ao_row select generate_series(21, 40);
insert into table_distr_random_ao_col select generate_series(21, 40);

insert into table_distr_replicated select generate_series(21, 40);
insert into table_distr_replicated_ao_row select generate_series(21, 40);
insert into table_distr_replicated_ao_col select generate_series(21, 40);

insert into part_range_table_distr_hashed select i, '2023-01-02' from generate_series(21, 40)i;
insert into part_range_table_distr_hashed select i, '2023-05-02' from generate_series(21, 40)i;
insert into part_range_table_distr_hashed select i, '2020-05-02' from generate_series(21, 40)i;

insert into part_range_table_distr_random select i, '2023-01-02' from generate_series(21, 40)i;
insert into part_range_table_distr_random select i, '2023-05-02' from generate_series(21, 40)i;
insert into part_range_table_distr_random select i, '2020-05-02' from generate_series(21, 40)i;

insert into part_list_table_distr_hashed select i, 'test1' from generate_series(21, 40)i;
insert into part_list_table_distr_hashed select i, 'test2' from generate_series(21, 40)i;
insert into part_list_table_distr_hashed select i, 'test3' from generate_series(21, 40)i;

insert into part_list_table_distr_random select i, 'test1' from generate_series(21, 40)i;
insert into part_list_table_distr_random select i, 'test2' from generate_series(21, 40)i;
insert into part_list_table_distr_random select i, 'test3' from generate_series(21, 40)i;

insert into multi_part_table_distr_hashed select i, '2023-01-05', 'test1' from generate_series(21, 40)i;
insert into multi_part_table_distr_hashed select i, '2023-02-05', 'test2' from generate_series(21, 40)i;
insert into multi_part_table_distr_hashed select i, '2023-03-05', 'test1' from generate_series(21, 40)i;

select a, gp_segment_id from table_distr_hashed order by a;
select a, gp_segment_id from table_distr_hashed_ao_row order by a;
select a, gp_segment_id from table_distr_hashed_ao_col order by a;

select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random order by a;
select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random_ao_row order by a;
select a, (gp_segment_id < 2) as correct_segment_id from table_distr_random_ao_col order by a;

select a from table_distr_replicated order by a;
select a from table_distr_replicated_ao_row order by a;
select a from table_distr_replicated_ao_col order by a;

select *, gp_segment_id from part_range_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from part_range_table_distr_random order by a, b;

select *, gp_segment_id from part_list_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from part_list_table_distr_random order by a, b;

select *, gp_segment_id from multi_part_table_distr_hashed order by a, b, c;

-- And do some cleanup
drop table table_distr_hashed;
drop table table_distr_hashed_ao_row;
drop table table_distr_hashed_ao_col;

drop table table_distr_random;
drop table table_distr_random_ao_row;
drop table table_distr_random_ao_col;

drop table table_distr_replicated;
drop table table_distr_replicated_ao_row;
drop table table_distr_replicated_ao_col;

drop table part_range_table_distr_hashed;
drop table part_range_table_distr_random;

drop table part_list_table_distr_hashed;
drop table part_list_table_distr_random;

drop table multi_part_table_distr_hashed;

-- Check that all newly created tables have data only on segments #0 and #1
set gp_target_numsegments = 2;
create table new_table_distr_hashed(a int) distributed by (a);
insert into new_table_distr_hashed select generate_series(1, 20);

create table new_table_distr_hashed_ao_row(a int) with (appendonly=true, orientation=row) distributed by (a);
insert into new_table_distr_hashed_ao_row select generate_series(1, 20);

create table new_table_distr_hashed_ao_col(a int) with (appendonly=true, orientation=column) distributed by (a);
insert into new_table_distr_hashed_ao_col select generate_series(1, 20);

create table new_table_distr_random(a int) distributed randomly;
insert into new_table_distr_random select generate_series(1, 20);

create table new_table_distr_random_ao_row(a int) with (appendonly=true, orientation=row) distributed randomly;
insert into new_table_distr_random_ao_row select generate_series(1, 20);

create table new_table_distr_random_ao_col(a int) with (appendonly=true, orientation=column) distributed randomly;
insert into new_table_distr_random_ao_col select generate_series(1, 20);

create table new_table_distr_replicated(a int) distributed replicated;
insert into new_table_distr_replicated select generate_series(1, 20);

create table new_table_distr_replicated_ao_row(a int) with (appendonly=true, orientation=row) distributed replicated;
insert into new_table_distr_replicated_ao_row select generate_series(1, 20);

create table new_table_distr_replicated_ao_col(a int) with (appendonly=true, orientation=column) distributed replicated;
insert into new_table_distr_replicated_ao_col select generate_series(1, 20);

create table new_part_range_table_distr_hashed (a int, b date) distributed by (a)
partition by range (b) (
    start (date '2023-01-01') inclusive
    end (date '2024-01-01') exclusive
    every (interval '1 month'),
    default partition other_vals
);
insert into new_part_range_table_distr_hashed select i, '2023-01-02' from generate_series(1, 20)i;
insert into new_part_range_table_distr_hashed select i, '2023-05-02' from generate_series(1, 20)i;
insert into new_part_range_table_distr_hashed select i, '2020-05-02' from generate_series(1, 20)i;

create table new_part_range_table_distr_random (a int, b date) distributed randomly
partition by range (b) (
    start (date '2023-01-01') inclusive
    end (date '2024-01-01') exclusive
    every (interval '1 month'),
    default partition other_vals
);
insert into new_part_range_table_distr_random select i, '2023-01-02' from generate_series(1, 20)i;
insert into new_part_range_table_distr_random select i, '2023-05-02' from generate_series(1, 20)i;
insert into new_part_range_table_distr_random select i, '2020-05-02' from generate_series(1, 20)i;

create table new_part_list_table_distr_hashed(a int, b text) distributed by (a)
partition by list (b) (
    partition part1 values ('test1'),
    partition part2 values ('test2'),
    default partition other_vals
);
insert into new_part_list_table_distr_hashed select i, 'test1' from generate_series(1, 20)i;
insert into new_part_list_table_distr_hashed select i, 'test2' from generate_series(1, 20)i;
insert into new_part_list_table_distr_hashed select i, 'test3' from generate_series(1, 20)i;

create table new_part_list_table_distr_random(a int, b text) distributed randomly
partition by list (b) (
    partition part1 values ('test1'),
    partition part2 values ('test2'),
    default partition other_vals
);
insert into new_part_list_table_distr_random select i, 'test1' from generate_series(1, 20)i;
insert into new_part_list_table_distr_random select i, 'test2' from generate_series(1, 20)i;
insert into new_part_list_table_distr_random select i, 'test3' from generate_series(1, 20)i;

create table new_multi_part_table_distr_hashed(a int, b date, c text) distributed by (a)
partition by range (b)
subpartition by list (c) subpartition template
(
    subpartition subpart1 values ('test1'),
    subpartition subpart2 values ('test2')
)
(
    partition part1 start (date '2023-01-01'),
    partition part2 start (date '2023-02-01'),
	partition part3 start (date '2023-03-01') end (date '2024-01-01')
);
insert into new_multi_part_table_distr_hashed select i, '2023-01-05', 'test1' from generate_series(1, 20)i;
insert into new_multi_part_table_distr_hashed select i, '2023-02-05', 'test2' from generate_series(1, 20)i;
insert into new_multi_part_table_distr_hashed select i, '2023-03-05', 'test1' from generate_series(1, 20)i;

-- Also check CTAS statement
create table new_table_ctas as select a from generate_series(1, 20)a distributed by(a);
select * into new_table_into from generate_series(1, 20)a;

select a, gp_segment_id from new_table_distr_hashed order by a;
select a, gp_segment_id from new_table_distr_hashed_ao_row order by a;
select a, gp_segment_id from new_table_distr_hashed_ao_col order by a;

select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random order by a;
select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random_ao_row order by a;
select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random_ao_col order by a;

select a from new_table_distr_replicated order by a;
select a from new_table_distr_replicated_ao_row order by a;
select a from new_table_distr_replicated_ao_col order by a;

select *, gp_segment_id from new_part_range_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from new_part_range_table_distr_random order by a, b;

select *, gp_segment_id from new_part_list_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from new_part_list_table_distr_random order by a, b;

select *, gp_segment_id from new_multi_part_table_distr_hashed order by a, b, c;

select a, gp_segment_id from new_table_ctas order by a;
select *, (gp_segment_id < 2) as correct_segment_id from new_table_into order by a;

-- Validate the insertion works fine with the new tables
-- after 'gp_target_numsegments' reset
reset gp_target_numsegments;

insert into new_table_ctas select generate_series(21, 40);
insert into new_table_into select generate_series(21, 40);

insert into new_table_distr_hashed select generate_series(21, 40);
insert into new_table_distr_hashed_ao_row select generate_series(21, 40);
insert into new_table_distr_hashed_ao_col select generate_series(21, 40);

insert into new_table_distr_random select generate_series(21, 40);
insert into new_table_distr_random_ao_row select generate_series(21, 40);
insert into new_table_distr_random_ao_col select generate_series(21, 40);

insert into new_part_range_table_distr_hashed select i, '2023-01-02' from generate_series(21, 40)i;
insert into new_part_range_table_distr_hashed select i, '2023-05-02' from generate_series(21, 40)i;
insert into new_part_range_table_distr_hashed select i, '2020-05-02' from generate_series(21, 40)i;

insert into new_part_range_table_distr_random select i, '2023-01-02' from generate_series(21, 40)i;
insert into new_part_range_table_distr_random select i, '2023-05-02' from generate_series(21, 40)i;
insert into new_part_range_table_distr_random select i, '2020-05-02' from generate_series(21, 40)i;

insert into new_part_list_table_distr_hashed select i, 'test1' from generate_series(21, 40)i;
insert into new_part_list_table_distr_hashed select i, 'test2' from generate_series(21, 40)i;
insert into new_part_list_table_distr_hashed select i, 'test3' from generate_series(21, 40)i;

insert into new_part_list_table_distr_random select i, 'test1' from generate_series(21, 40)i;
insert into new_part_list_table_distr_random select i, 'test2' from generate_series(21, 40)i;
insert into new_part_list_table_distr_random select i, 'test3' from generate_series(21, 40)i;

insert into new_multi_part_table_distr_hashed select i, '2023-01-05', 'test1' from generate_series(21, 40)i;
insert into new_multi_part_table_distr_hashed select i, '2023-02-05', 'test2' from generate_series(21, 40)i;
insert into new_multi_part_table_distr_hashed select i, '2023-03-05', 'test1' from generate_series(21, 40)i;

select a, gp_segment_id from new_table_ctas order by a;
select *, (gp_segment_id < 2) as correct_segment_id from new_table_into order by a;

select a, gp_segment_id from new_table_distr_hashed order by a;
select a, gp_segment_id from new_table_distr_hashed_ao_row order by a;
select a, gp_segment_id from new_table_distr_hashed_ao_col order by a;

select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random order by a;
select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random_ao_row order by a;
select a, (gp_segment_id < 2) as correct_segment_id from new_table_distr_random_ao_col order by a;

select *, gp_segment_id from new_part_range_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from new_part_range_table_distr_random order by a, b;

select *, gp_segment_id from new_part_list_table_distr_hashed order by a, b;
select *, (gp_segment_id < 2) as correct_segment_id from new_part_list_table_distr_random order by a, b;

select *, gp_segment_id from new_multi_part_table_distr_hashed order by a, b, c;

-- And do some cleanup
drop table new_table_distr_hashed;
drop table new_table_distr_hashed_ao_row;
drop table new_table_distr_hashed_ao_col;

drop table new_table_distr_random;
drop table new_table_distr_random_ao_row;
drop table new_table_distr_random_ao_col;

drop table new_table_distr_replicated;
drop table new_table_distr_replicated_ao_row;
drop table new_table_distr_replicated_ao_col;

drop table new_part_range_table_distr_hashed;
drop table new_part_range_table_distr_random;

drop table new_part_list_table_distr_hashed;
drop table new_part_list_table_distr_random;

drop table new_multi_part_table_distr_hashed;

drop table new_table_ctas;
drop table new_table_into;

-- Check rollback of alter rebalance operation
create table table_distr_hashed(a int) distributed by (a);
insert into table_distr_hashed select generate_series(1, 20);

select count(1), gp_segment_id from table_distr_hashed group by gp_segment_id order by gp_segment_id;

begin;
set gp_target_numsegments = 2;
alter table table_distr_hashed rebalance;
select count(1), gp_segment_id from table_distr_hashed group by gp_segment_id order by gp_segment_id;
rollback;
select count(1), gp_segment_id from table_distr_hashed group by gp_segment_id order by gp_segment_id;

reset gp_target_numsegments;
drop table table_distr_hashed;

-- Check rebalance with parameter
create table table_distr_hashed(a int) distributed by (a);
insert into table_distr_hashed select generate_series(1, 20);
select count(1), gp_segment_id from table_distr_hashed group by gp_segment_id order by gp_segment_id;

-- Shrink to 2 segments
alter table table_distr_hashed rebalance 2;
select count(1), gp_segment_id from table_distr_hashed group by gp_segment_id order by gp_segment_id;

-- Shrink to 1 segment
alter table table_distr_hashed rebalance 1;
select count(1), gp_segment_id from table_distr_hashed group by gp_segment_id order by gp_segment_id;

-- Expand back to 3 segments
alter table table_distr_hashed rebalance 3;
select count(1), gp_segment_id from table_distr_hashed group by gp_segment_id order by gp_segment_id;

-- Try to expand to 4 segments - should do nothing
alter table table_distr_hashed rebalance 3;

drop table table_distr_hashed;
