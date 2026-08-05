-- PG13-18 feature combinations on append-optimized tables that previously
-- had no coverage: UNIQUE NULLS NOT DISTINCT, COPY ON_ERROR / REJECT_LIMIT,
-- virtual generated columns, temporal (WITHOUT OVERLAPS) and exclusion
-- constraints.
create schema ao_pg18_features;
set search_path = ao_pg18_features;

-- ============================================================
-- PG15 UNIQUE NULLS NOT DISTINCT on AO: enforced through the same
-- blockdir/visimap unique machinery as ordinary AO unique indexes.
-- ============================================================
create table unnd_ao (a int, b int, unique nulls not distinct (a, b)) with (appendonly=true) distributed by (a);
insert into unnd_ao values (1, null);
insert into unnd_ao values (1, null);
insert into unnd_ao values (2, null);
select count(*) from unnd_ao;
create table unnd_aoco (a int, b int, unique nulls not distinct (a, b)) using ao_column distributed by (a);
insert into unnd_aoco values (1, null);
insert into unnd_aoco values (1, null);
-- default NULLS DISTINCT still admits duplicate NULLs on AO
create table und_ao (a int, b int, unique (a, b)) with (appendonly=true) distributed by (a);
insert into und_ao values (1, null), (1, null);
select count(*) from und_ao;

-- ============================================================
-- PG17 COPY ON_ERROR IGNORE / PG18 REJECT_LIMIT into AO and AOCS
-- (the skipped rows never reach the AO insert path)
-- ============================================================
create table copy_ao (a int, b int) with (appendonly=true) distributed by (a);
copy copy_ao from stdin with (on_error ignore);
1	1
2	bad
3	3
\.
select * from copy_ao order by a;
create table copy_aoco (a int, b int) using ao_column distributed by (a);
copy copy_aoco from stdin with (on_error ignore, reject_limit 2);
1	1
2	bad
3	oops
4	4
\.
select * from copy_aoco order by a;
-- exceeding REJECT_LIMIT aborts the COPY.  Note: in Greengage the limit is
-- enforced per segment, so use the same distribution key for both bad rows
-- to make the excess deterministic.
copy copy_aoco from stdin with (on_error ignore, reject_limit 1);
5	5
6	bad
6	oops
8	8
\.
select count(*) from copy_aoco;

-- ============================================================
-- PG18 virtual generated columns on AO and AOCS
-- ============================================================
create table vgen_ao (a int, b int generated always as (a * 2) virtual) with (appendonly=true) distributed by (a);
insert into vgen_ao values (1), (2);
select a, b from vgen_ao order by a;
alter table vgen_ao add column c int generated always as (a + 100) virtual;
select a, b, c from vgen_ao order by a;
create table vgen_aoco (a int, b int generated always as (a * 3) virtual, c int) using ao_column distributed by (a);
insert into vgen_aoco values (1, default, 10), (2, default, 20);
select a, b, c from vgen_aoco order by a;
update vgen_aoco set c = c + 1 where a = 1;
select a, b, c from vgen_aoco order by a;

-- ============================================================
-- Logical replication: AO tables cannot be published -- the Appendonly
-- resource manager has no decode routine, so a publication would accept
-- them and then silently replicate nothing.
-- ============================================================
create table pub_ao (a int) with (appendonly=true) distributed by (a);
create publication pub_ao_fail for table pub_ao;

-- ============================================================
-- PG18 temporal constraints (WITHOUT OVERLAPS) and exclusion constraints
-- are rejected on AO: their recheck cannot see rows inserted by the same
-- command, so conflicting rows in one statement would both be accepted.
-- ============================================================
create table two_ao (id int, valid_at daterange, constraint two_ao_pk primary key (id, valid_at without overlaps)) with (appendonly=true) distributed by (id);
create table excl_ao (id int, r daterange, exclude using gist (id with =, r with &&)) with (appendonly=true) distributed by (id);
-- ... and on heap they work (QD->QE dispatch of IndexStmt.iswithoutoverlaps):
-- range keys only, to stay within core GiST opclasses
create table two_heap (id int4range, valid_at daterange, constraint two_heap_pk primary key (id, valid_at without overlaps)) distributed by (id);
insert into two_heap values (int4range(1,2), daterange('2020-01-01','2020-06-01')),
                            (int4range(1,2), daterange('2020-06-01','2021-01-01'));
insert into two_heap values (int4range(1,2), daterange('2020-03-01','2020-04-01'));
select count(*) from two_heap;

set search_path = public;
drop schema ao_pg18_features cascade;
