--
-- Range partition bounds must be compared using the partition key's
-- collation, not the database default collation.
--
-- The tests run in a database with a C default, and the partitioning keys
-- have an en_US.utf8 collation, so comparing with the default collation, or
-- comparing bytes, gives different results. Only distinct letters are
-- compared, because glibc versions disagree on how 'a' and 'A' are ordered.
--
-- en_US.utf8 orders 'a' < 'B' < 'c', while "C" orders 'B' < 'a' < 'c'.
--
set client_min_messages = warning;
drop database if exists partition_collation;
create database partition_collation with encoding = 'UTF8'
  lc_collate = 'C' lc_ctype = 'C' template = template0;
\c partition_collation
set client_min_messages = warning;

create collation coll_en (locale = 'en_US.utf8');

--
-- Routing. 'B' sorts between 'a' and 'c', so it belongs to p1. It used to be
-- put in the default partition without any error.
--
create table rp_en (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('c'),
 default partition other);

insert into rp_en values (1, 'B'), (2, 'a'), (3, 'b'), (4, 'd');
-- The tableoid queries show which partition each row was stored in. A row in
-- the wrong partition is still returned by queries on the parent table, so
-- checking query results alone would not catch it.
select tableoid::regclass, * from rp_en order by id;

--
-- COPY, and UPDATE of the partitioning key, must route rows the same way.
--
create table rp_en_dml (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('c'),
 default partition other);

copy rp_en_dml from stdin;
1	B
2	d
\.
select tableoid::regclass, * from rp_en_dml order by id;

-- Row 1 moves into the default partition and row 2 moves into p1. Moving a
-- row to another partition with UPDATE is only supported by GPORCA.
set optimizer = on;
update rp_en_dml set k = 'd' where id = 1;
update rp_en_dml set k = 'B' where id = 2;
reset optimizer;
select tableoid::regclass, * from rp_en_dml order by id;

--
-- CREATE with bounds that are only ordered correctly under the key's
-- collation. This used to fail with "START greater than END".
--
create table rp_en_bounds (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('B'),
 partition p2 start ('B') end ('z'));

insert into rp_en_bounds values (1, 'aZ'), (2, 'Bb'), (3, 'c');
select tableoid::regclass, * from rp_en_bounds order by id;

--
-- ADD PARTITION must check the new bounds with the key's collation too. This
-- used to fail with "new partition overlaps existing partition".
--
create table rp_en_add (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p2 start ('B') end ('z'));

alter table rp_en_add add partition p1 start ('a') end ('B');
insert into rp_en_add values (1, 'aZ'), (2, 'c');
select tableoid::regclass, * from rp_en_add order by id;

--
-- SPLIT PARTITION AT must check that the AT value is inside the partition
-- with the key's collation. This used to fail with "AT clause parameter is
-- not a member of the target partition specification".
--
create table rp_en_split (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('z'));

alter table rp_en_split split partition p1 at ('C') into (partition p1a, partition p1b);
insert into rp_en_split values (1, 'b'), (2, 'D');
select tableoid::regclass, * from rp_en_split order by id;

--
-- SPLIT DEFAULT PARTITION must check the new bounds with the key's collation
-- and move the matching rows out of the default partition.
--
create table rp_en_split_def (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p2 start ('B') end ('z'),
 default partition other);

insert into rp_en_split_def values (1, 'aZ'), (2, 'zz');
alter table rp_en_split_def split default partition start ('a') end ('B')
into (partition other, partition p1);
select tableoid::regclass, * from rp_en_split_def order by id;

--
-- PARTITION FOR (value) must find the partition with the key's collation.
-- Both collations order these bounds the same way, but 'C' sorts after 'b'
-- in en_US.utf8 and before 'b' in "C".
--
create table rp_en_for (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('0') end ('b'),
 partition p2 start ('b') end ('z'));

insert into rp_en_for values (1, 'a'), (2, 'x');
-- 'C' is in p2, so only row 2 is removed.
alter table rp_en_for truncate partition for ('C');
select tableoid::regclass, * from rp_en_for order by id;

--
-- Each partitioning level must use the collation of its own key column. Here
-- the first level has a "C" key and the second level an en_US.utf8 key, with
-- the same bounds on both levels: 'C' sorts before 'b' in "C" and after 'b'
-- in en_US.utf8.
--
create table rp_sub (id int, k1 text collate "C", k2 text collate coll_en)
distributed by (id)
partition by range (k1)
subpartition by range (k2)
subpartition template
(subpartition s1 start ('0') end ('b'),
 subpartition s2 start ('b') end ('z'))
(partition p1 start ('0') end ('b'),
 partition p2 start ('b') end ('z'));

-- Row 1 belongs to p1 and s2, row 2 to p2 and s1.
insert into rp_sub values (1, 'C', 'C'), (2, 'x', 'a');
select tableoid::regclass, * from rp_sub order by id;

--
-- Partition boundaries carry the collation of the partitioning key.
--
select tablename, partitionname, partitionrangestart, partitionrangeend
from pg_partitions
where tablename = 'rp_en' and not partitionisdefault;

-- Each subpartition level, and the subpartition template, carries the
-- collation of its own key.
select partitionlevel, partitiontablename, partitionrangestart, partitionrangeend
from pg_partitions
where tablename = 'rp_sub'
order by partitionlevel, partitiontablename;

select partitionlevel, partitionname, partitionrangestart, partitionrangeend
from pg_partition_templates
where tablename = 'rp_sub'
order by partitionname;

-- LIST values carry the collation of the partitioning key too, including
-- values added by ADD PARTITION and SPLIT PARTITION.
create table lp_en (id int, k text collate coll_en)
distributed by (id)
partition by list (k)
(partition p1 values ('a', 'b'),
 partition p2 values ('c', 'd'));

alter table lp_en add partition p3 values ('e');
alter table lp_en split partition p2 at ('c') into (partition p2a, partition p2b);

select partitionname, partitionlistvalues
from pg_partitions
where tablename = 'lp_en'
order by partitionname;

--
-- The partition definition is printed in the CREATE TABLE syntax, which has
-- no COLLATE clause for boundaries, and pg_dump relies on it. Check the
-- output, then create a copy of each table from it and insert the same rows.
--
select pg_get_partition_def('rp_en'::regclass, true);
select pg_get_partition_def('rp_sub'::regclass, true);
select pg_get_partition_template_def('rp_sub'::regclass, true, false);
select pg_get_partition_def('lp_en'::regclass, true);

do $$
begin
	execute 'create table rp_en_copy (id int, k text collate coll_en) distributed by (id) '
		|| pg_get_partition_def('rp_en'::regclass);
	execute 'create table rp_sub_copy (id int, k1 text collate "C", k2 text collate coll_en) distributed by (id) '
		|| pg_get_partition_def('rp_sub'::regclass);
end
$$;

insert into rp_en_copy values (1, 'B'), (2, 'a'), (3, 'b'), (4, 'd');
select tableoid::regclass, * from rp_en_copy order by id;

insert into rp_sub_copy values (1, 'C', 'C'), (2, 'x', 'a');
select tableoid::regclass, * from rp_sub_copy order by id;

--
-- Tables created by earlier versions have the database default collation
-- stored in their boundaries, and keep comparing the boundaries using it. To
-- get such tables, copy the boundaries and their order from a table with the
-- same partitions and a default-collation key.
--
set allow_system_table_mods = on;
create function copy_bounds(target regclass, source regclass) returns void
language sql as $$
	update pg_partition_rule r
	set parrangestart = s.parrangestart, parrangeend = s.parrangeend,
	    parlistvalues = s.parlistvalues, parruleord = s.parruleord
	from pg_partition p, pg_partition ps, pg_partition_rule s
	where r.paroid = p.oid and p.parrelid = target
	  and s.paroid = ps.oid and ps.parrelid = source
	  and s.parname = r.parname;
$$;

create table rp_def_old (id int, k text)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('m'),
 partition p2 start ('m') end ('t'),
 default partition other);
create table rp_en_old (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('a') end ('m'),
 partition p2 start ('m') end ('t'),
 default partition other);

create table rp_def_old_nodef (id int, k text)
distributed by (id)
partition by range (k)
(partition p1 start ('m') end ('p'));
create table rp_en_old_nodef (id int, k text collate coll_en)
distributed by (id)
partition by range (k)
(partition p1 start ('m') end ('p'));

create table lp_def_old (id int, k text)
distributed by (id)
partition by list (k)
(partition p1 values ('a', 'b'),
 default partition other);
create table lp_en_old (id int, k text collate coll_en)
distributed by (id)
partition by list (k)
(partition p1 values ('a', 'b'),
 default partition other);

select copy_bounds('rp_en_old', 'rp_def_old');
select copy_bounds('rp_en_old_nodef', 'rp_def_old_nodef');
select copy_bounds('lp_en_old', 'lp_def_old');
reset allow_system_table_mods;

-- 'B' sorts before 'a' in "C", so it is still put in the default partition.
insert into rp_en_old values (1, 'B');
select tableoid::regclass, * from rp_en_old order by id;

-- A new range must be ordered under the stored collation and under the
-- key's collation. ['a', 'B') is ordered in en_US.utf8 but not in "C", and
-- ['B', 'a') is ordered in "C" but not in en_US.utf8, so both are rejected.
alter table rp_en_old_nodef add partition p2 start ('a') end ('B');
alter table rp_en_old_nodef add partition p2 start ('B') end ('a');
alter table rp_en_old split default partition start ('B') end ('a')
  into (partition p0, default partition);

-- Partitions added to such a table get the collation of its existing
-- boundaries, so no boundary is shown with COLLATE.
alter table rp_en_old_nodef add partition p2 start ('q') end ('s');

alter table rp_en_old split partition p1 at ('g') into (partition p1a, partition p1b);
alter table rp_en_old split default partition start ('t') end ('w')
  into (partition p3, default partition);
alter table rp_en_old drop default partition;
alter table rp_en_old add partition p4 start ('w') end ('y');

alter table lp_en_old split default partition at ('c')
  into (partition p2, default partition);
alter table lp_en_old drop default partition;
alter table lp_en_old add partition p3 values ('d');

select tablename, partitionname, partitionrangestart, partitionrangeend,
       partitionlistvalues
from pg_partitions
where tablename in ('rp_en_old', 'rp_en_old_nodef', 'lp_en_old')
order by tablename, partitionposition;

insert into rp_en_old values (1, 'c'), (2, 'h'), (3, 'n'), (4, 'u'), (5, 'x');
select tableoid::regclass, * from rp_en_old order by id;
insert into rp_en_old_nodef values (1, 'n'), (2, 'r');
select tableoid::regclass, * from rp_en_old_nodef order by id;
insert into lp_en_old values (1, 'a'), (2, 'c'), (3, 'd');
select tableoid::regclass, * from lp_en_old order by id;

\c regression
drop database partition_collation;
