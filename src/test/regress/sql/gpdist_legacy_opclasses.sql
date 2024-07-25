--
-- Tests for legacy cdbhash opclasses
--

drop schema if exists gpdist_legacy_opclasses;
create schema gpdist_legacy_opclasses;
set search_path to gpdist_legacy_opclasses;

-- Basic sanity check of all the legacy hash opclasses. Create a table that
-- uses all of them in the distribution key, and insert a value.
set gp_use_legacy_hashops=on;
create table all_legacy_types(
  int2_col int2,
  int4_col int4,
  int8_col int8,
  float4_col float4,
  float8_col float8,
  numeric_col numeric,
  char_col "char",
  bpchar_col char(20),
  text_col text,
  varchar_col varchar(20),
  bytea_col bytea,
  name_col name,
  oid_col oid,
  tid_col tid,
  timestamp_col timestamp,
  timestamptz_col timestamptz,
  date_col date,
  time_col time,
  timetz_col timetz,
  interval_col interval,
  inet_col inet,
  cidr_col cidr,
  macaddr_col macaddr,
  bit_col bit(11),
  varbit_col varbit(20),
  bool_col bool,
  oidvector_col oidvector,
  money_col money,
  --uuid_col uuid,
  complex_col complex
) distributed by (
  int2_col,
  int4_col,
  int8_col,
  float4_col,
  float8_col,
  numeric_col,
  char_col,
  bpchar_col,
  text_col,
  varchar_col,
  bytea_col,
  name_col,
  oid_col,
  tid_col,
  timestamp_col,
  timestamptz_col,
  date_col,
  time_col,
  timetz_col,
  interval_col,
  inet_col,
  cidr_col,
  macaddr_col,
  bit_col,
  varbit_col,
  bool_col,
  oidvector_col,
  money_col,
  --uuid_col,
  complex_col
);

-- Verify that all columns are using the legacy hashops
select attno, opc.opcname from
  (select unnest(distkey) as attno, unnest(distclass) as distclass from gp_distribution_policy
   where localoid='all_legacy_types'::regclass) as d,
  pg_opclass opc
where opc.oid=distclass
order by attno;

insert into all_legacy_types values (
  '12345',          -- int2
  '12345678',       -- int4
  '1234567890123',  -- int8
  '1.2345',         -- float4
  '1.23456789',     -- float8
  '1.23456789',     -- numeric
  'x',              -- "char"
  'bpchar foobar',  -- bpchar
  'text foobar',     -- text
  'varchar foobar', -- varchar
  '\xdeadbeef',     -- bytea
  'name foobar',    -- name
  '12345',          -- oid
  '(1,1)',          -- tid
  '2018-11-25 01:23:45+02',   -- timestamp
  '2018-11-25 01:23:45+02',   -- timestamptz
  '2018-11-25',     -- date,
  '01:23:45',       -- time,
  '01:23:45+02',    -- timetz,
  '6 years',        -- interval,
  '192.168.1.255/25',   -- inet,
  '10.1.2',         -- cidr,
  '08:00:2b:01:02:03',         -- macaddr,
  B'11011000000',   -- bit,
  B'01010101010',   -- varbit,
  'true',        -- bool,
  '1 2 3 4',        -- oidvector,
  '123.45',        -- money,
  --uuid_col         -- uuid,
  '5 + 3i'         -- complex
);

-- Test that CTAS honors the gp_use_legacy_hashops GUC
-- Note: When ORCA is on, the distribution is RANDOM.
create table legacy_hashops_ctas as select 1;
select gpdp.distkey, pgopc.opcname
  from gp_distribution_policy gpdp, pg_opclass pgopc
  where gpdp.localoid='legacy_hashops_ctas'::regclass and pgopc.oid::text = gpdp.distclass::text;

set gp_use_legacy_hashops=off;


--
-- Test joins between tables using a mix of default and legacy opclasses.
--
create table legacy_int (id int4) distributed by (id cdbhash_int4_ops);
insert into legacy_int values (1), (2), (3);

create table modern_int (id int4) distributed by (id);
insert into modern_int values (2), (3), (4);
analyze modern_int;

create table modern_text (t text) distributed by (t);
insert into modern_text values ('foo'), ('1');

-- Only legacy opclasses used. ORCA can deal with this
explain (costs off) select * from legacy_int a inner join legacy_int b on a.id = b.id;
select * from legacy_int a inner join legacy_int b on a.id = b.id;

-- A mixture of legacy and modern opclasses. Needs a Redistribute motion. ORCA
-- can't handle this, and falls back.
explain (costs off) select * from legacy_int a inner join modern_int b on a.id = b.id;
select * from legacy_int a inner join modern_int b on a.id = b.id;

-- for the sake of completeness, modern opclasses only. No problem for ORCA.
explain (costs off) select * from modern_int a inner join modern_int b on a.id = b.id;
select * from modern_int a inner join modern_int b on a.id = b.id;

-- In principle, ORCA would create a correct plan for this, because even though it
-- mixes the legacy and other opclasses, they're not used in join conditions. But
-- the ORCA translator code is conservative, and falls back if there are any mixed
-- use, even if it would be safe.
explain (costs off) select * from legacy_int a inner join modern_text b on a.id::text = b.t;
select * from legacy_int a inner join modern_text b on a.id::text = b.t;


-- Also test with a domain over a base type, and enums.
create domain intdom as integer;
create table legacy_domain_over_int(id intdom) distributed by(id cdbhash_int4_ops);
insert into legacy_domain_over_int values (1), (2), (3);
analyze legacy_domain_over_int;

explain (costs off) select * from legacy_domain_over_int a inner join legacy_domain_over_int b on a.id = b.id;
explain (costs off) select * from legacy_int a inner join legacy_domain_over_int b on a.id = b.id;
explain (costs off) select * from modern_int a inner join legacy_domain_over_int b on a.id = b.id;

create type colors as enum ('red', 'green', 'blue');
create table legacy_enum(color colors) distributed by(color cdbhash_enum_ops);
insert into legacy_enum values ('red'), ('green'), ('blue');

explain (costs off) select * from legacy_enum a inner join legacy_enum b on a.color = b.color;
select * from legacy_enum a inner join legacy_enum b on a.color = b.color;

--
-- A regression issue that the data is reorganized incorrectly when
-- gp_use_legacy_hashops has non-default value.
--
-- The ALTER TABLE command reorganizes the data by using a temporary table, if
-- a "distributed by" clause is specified without the opclasses, the default
-- opclasses will be chosen.  There was a bug that the non-legacy opclasses are
-- always chosen, regarding the setting of gp_use_legacy_hashops.  However the
-- table's new opclasses are determined with gp_use_legacy_hashops, so when
-- gp_use_legacy_hashops is true the data will be incorrectly redistributed.
--

-- set the guc to the non-default value
set gp_use_legacy_hashops to on;

create table legacy_data_reorg (c1 int) distributed by (c1);
insert into legacy_data_reorg select i from generate_series(1, 10) i;

-- verify the opclass and data distribution
select gp_segment_id, c1 from legacy_data_reorg order by 1, 2;
select dp.localoid::regclass::name as name, oc.opcname
  from gp_distribution_policy dp
  join pg_opclass oc
    on oc.oid::text = dp.distclass::text
 where dp.localoid = 'legacy_data_reorg'::regclass::oid;

-- when reorganizing the table we set the distributed-by without an explicit
-- opclass, so the default one should be chosen according to
-- gp_use_legacy_hashops.
alter table legacy_data_reorg set with (reorganize) distributed by (c1);

-- double-check the opclass and data distribution
select gp_segment_id, c1 from legacy_data_reorg order by 1, 2;
select dp.localoid::regclass::name as name, oc.opcname
  from gp_distribution_policy dp
  join pg_opclass oc
    on oc.oid::text = dp.distclass::text
 where dp.localoid = 'legacy_data_reorg'::regclass::oid;

--
-- A regression issue similar to previous one, with CTAS.
--
-- The default opclasses in CTAS should also be determined with
-- gp_use_legacy_hashops.
--

set gp_use_legacy_hashops=off;
create table ctastest_off as select 123 as col distributed by (col);

set gp_use_legacy_hashops=on;
create table ctastest_on as select 123 as col distributed by (col);

select dp.localoid::regclass::name as name, oc.opcname
  from gp_distribution_policy dp
  join pg_opclass oc
    on oc.oid::text = dp.distclass::text
 where dp.localoid in ('ctastest_on'::regclass::oid,
                       'ctastest_off'::regclass::oid);
set gp_use_legacy_hashops=on;
create table try_distinct_array (test_char varchar,test_array integer[]);
insert into try_distinct_array select 'y',string_to_array('1~1','~')::int[];
analyze try_distinct_array;
insert into try_distinct_array select 'n',string_to_array('1~1','~')::int[];
-- Aggregate with grouping column that does not have legacy hashop
explain (costs off) select distinct test_array from try_distinct_array;
select distinct test_array from try_distinct_array;
-- Hash join on column that does not have legacy hashop
explain (costs off) select * from try_distinct_array a, try_distinct_array b where a.test_array=b.test_array;
select * from try_distinct_array a, try_distinct_array b where a.test_array=b.test_array;

-- CTAS should use value of gp_use_legacy_hashops when setting the distribution policy based on an existing table
set gp_use_legacy_hashops=on;
create table ctas_base_legacy as select unnest(array[1,2,3]) as col distributed by (col);
set gp_use_legacy_hashops=off;
create table ctas_from_legacy as select * from ctas_base_legacy distributed by (col);
create table ctas_explicit_legacy as select * from ctas_base_legacy distributed by (col cdbhash_int4_ops);

create table ctas_base_nonlegacy as select unnest(array[1,2,3]) as col distributed by (col);
set gp_use_legacy_hashops=on;
create table ctas_from_nonlegacy as select * from ctas_base_nonlegacy distributed by (col);
create table ctas_explicit_nonlegacy as select * from ctas_base_nonlegacy distributed by (col int4_ops);

select dp.localoid::regclass as name, opc.opcname
  from gp_distribution_policy dp
  join pg_opclass opc
    on ARRAY[opc.oid]::oidvector = dp.distclass
 where dp.localoid in ('ctas_base_legacy'::regclass,
                       'ctas_from_legacy'::regclass,
                       'ctas_base_nonlegacy'::regclass,
                       'ctas_from_nonlegacy'::regclass,
                       'ctas_explicit_legacy'::regclass,
                       'ctas_explicit_nonlegacy'::regclass);
select * from ctas_from_legacy where col=1;
select * from ctas_explicit_legacy where col=1;
select * from ctas_from_nonlegacy where col=1;
select * from ctas_explicit_nonlegacy where col=1;
