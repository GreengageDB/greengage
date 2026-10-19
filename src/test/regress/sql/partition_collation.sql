--
-- Range partition bounds must be compared using the partition key's
-- collation, not the database default collation.
--
-- To catch that whatever the database default is, every scenario runs twice:
-- once with a "C" key and once with an en_US.utf8 key. At least one of the
-- two differs from the default. On the en_US key only distinct letters are
-- compared, because glibc versions disagree on how 'a' and 'A' are ordered.
--
-- en_US.utf8 orders 'a' < 'B' < 'c', while "C" orders 'B' < 'a' < 'c'.
--
create schema partition_collation;
set search_path = partition_collation;

create collation coll_en (locale = 'en_US.utf8');

--
-- Routing.
--
-- "C" key: 'B' sorts before 'a', so it belongs to the default partition.
-- With an en_US default, inserting it used to fail.
create table rp_c (id int, k text collate "C")
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('c'),
 default partition other);

insert into rp_c values (1, 'B'), (2, 'a'), (3, 'b'), (4, 'd');
-- The tableoid queries show which partition each row was stored in. A row in
-- the wrong partition is still returned by queries on the parent table, so
-- checking query results alone would not catch it.
select tableoid::regclass, * from rp_c order by id;

-- en_US key: 'B' sorts between 'a' and 'c', so it belongs to p1. With a "C"
-- default it used to be put in the default partition without any error.
create table rp_en (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('c'),
 default partition other);

insert into rp_en values (1, 'B'), (2, 'a'), (3, 'b'), (4, 'd');
select tableoid::regclass, * from rp_en order by id;

--
-- CREATE with bounds that are only ordered correctly under the key's
-- collation. These used to fail with "START greater than END".
--
create table rp_c_bounds (id int, k text collate "C")
distributed by (id)
partition by range (k)
(partition p1 start ('B') end ('a'),
 partition p2 start ('a') end ('z'));

insert into rp_c_bounds values (1, 'B'), (2, 'b'), (3, 'Zz'), (4, 'a');
select tableoid::regclass, * from rp_c_bounds order by id;

create table rp_en_bounds (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('B'),
 partition p2 start ('B') end ('z'));

insert into rp_en_bounds values (1, 'aZ'), (2, 'Bb'), (3, 'c');
select tableoid::regclass, * from rp_en_bounds order by id;

--
-- ADD PARTITION must check the new bounds with the key's collation too.
-- These used to fail with "new partition overlaps existing partition".
--
create table rp_c_add (id int, k text collate "C")
distributed by (id)
partition by range (k)
(partition p2 start ('a') end ('z'));

alter table rp_c_add add partition p1 start ('B') end ('a');
insert into rp_c_add values (1, 'B'), (2, 'b');
select tableoid::regclass, * from rp_c_add order by id;

create table rp_en_add (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p2 start ('B') end ('z'));

alter table rp_en_add add partition p1 start ('a') end ('B');
insert into rp_en_add values (1, 'aZ'), (2, 'c');
select tableoid::regclass, * from rp_en_add order by id;

--
-- SPLIT PARTITION AT must check that the AT value is inside the partition
-- with the key's collation. These used to fail with "AT clause parameter is
-- not a member of the target partition specification".
--
create table rp_c_split (id int, k text collate "C")
distributed by (id)
partition by range (k)
(partition p1 start ('B') end ('a'),
 partition p2 start ('a') end ('z'));

alter table rp_c_split split partition p1 at ('Q') into (partition p1a, partition p1b);
insert into rp_c_split values (1, 'C'), (2, 'R'), (3, 'b');
select tableoid::regclass, * from rp_c_split order by id;

create table rp_en_split (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('z'));

alter table rp_en_split split partition p1 at ('C') into (partition p1a, partition p1b);
insert into rp_en_split values (1, 'b'), (2, 'D');
select tableoid::regclass, * from rp_en_split order by id;

reset search_path;
drop schema partition_collation cascade;
