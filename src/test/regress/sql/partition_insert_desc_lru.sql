--
-- Memory budget of the open per-partition AO/AOCS insert descriptors while
-- inserting through a partition root (GUC gp_partition_insert_desc_budget).
--
-- Exercises the "flush + close the least-recently-used insert descriptor, then
-- transparently re-open it when that partition is written to again" path, which
-- is not reachable by any other feature. The budget is the statement's memory
-- (query_mem, or statement_mem for COPY); a 1MB statement_mem is smaller than
-- the descriptors of the tables below, so they evict on almost every switch of
-- partition. This test pins the functional correctness of eviction + re-open.
--

set client_min_messages to warning;
create schema pdlru;
set search_path = pdlru, public;

-- GUC surface: USERSET, default off (unbounded / historical)
show gp_partition_insert_desc_budget;
set gp_partition_insert_desc_budget = on;
show gp_partition_insert_desc_budget;
reset gp_partition_insert_desc_budget;
show gp_partition_insert_desc_budget;

-- Highest pg_ao(cs)seg.modcount over the leaves of a partitioned table, read on
-- the segments. Every close of an insert descriptor bumps it, so after a
-- single load into an empty table it is 1 unless a leaf was evicted and
-- re-opened.
create function pdlru.max_modcount(root regclass) returns bigint
language plpgsql as $$
declare
  r record;
  m bigint := 0;
  v bigint;
begin
  for r in select a.segrelid::regclass as seg
           from pg_partitions p
           join pg_appendonly a
             on a.relid = (quote_ident(p.partitionschemaname) || '.' ||
                           quote_ident(p.partitiontablename))::regclass
           where (quote_ident(p.schemaname) || '.' ||
                  quote_ident(p.tablename))::regclass = root
  loop
    execute format('select coalesce(max(modcount), 0) from gp_dist_random(%L)',
                   r.seg::text) into v;
    m := greatest(m, v);
  end loop;
  return m;
end $$;

----------------------------------------------------------------------
-- 1. AO row: INSERT ... SELECT and COPY through the root, round-robin
--    partition access, budget smaller than the touched descriptors.
----------------------------------------------------------------------

create table pdlru.ao_ref (id int, part int, payload text)
  with (appendonly=true, orientation=row, blocksize=262144)
  distributed by (id)
  partition by range (part) (start (0) end (8) every (1));
create table pdlru.ao_rr (like pdlru.ao_ref)
  with (appendonly=true, orientation=row, blocksize=262144)
  partition by range (part) (start (0) end (8) every (1));

-- unbounded reference load
set gp_partition_insert_desc_budget = off;
insert into pdlru.ao_ref select g, g % 8, 'v' || g from generate_series(1, 8000) g;

-- same data, tight budget: evict + re-open on almost every row
set gp_partition_insert_desc_budget = on;
set statement_mem = '1MB';
insert into pdlru.ao_rr select g, g % 8, 'v' || g from generate_series(1, 8000) g;
reset statement_mem;

-- identical contents
select count(*), sum(id), sum(length(payload)) from pdlru.ao_ref;
select count(*), sum(id), sum(length(payload)) from pdlru.ao_rr;
-- row numbering resumed on re-open, not restarted -> no duplicate AO tuple ids
select count(*) as dup_tids from (
  select gp_segment_id, part, ctid from pdlru.ao_rr group by 1, 2, 3 having count(*) > 1
) d;
-- eviction + re-open really happened, and only with the budget
select pdlru.max_modcount('pdlru.ao_ref') as ref_modcount,
       pdlru.max_modcount('pdlru.ao_rr') > 1 as evicted;

-- COPY path (what gprestore of a non-leaf-partition backup runs)
copy (select g, g % 8, 'v' || g from generate_series(1, 8000) g) to '/tmp/pdlru_ao.csv' csv;
truncate pdlru.ao_rr;
set statement_mem = '1MB';
copy pdlru.ao_rr from '/tmp/pdlru_ao.csv' csv;
reset statement_mem;
select count(*), sum(id), sum(length(payload)) from pdlru.ao_rr;
select count(*) as dup_tids from (
  select gp_segment_id, part, ctid from pdlru.ao_rr group by 1, 2, 3 having count(*) > 1
) d;
select pdlru.max_modcount('pdlru.ao_rr') > 1 as evicted;

----------------------------------------------------------------------
-- 2. AOCS: bounded result must equal the unbounded result exactly.
----------------------------------------------------------------------

create table pdlru.src (id int, part int, a int, b text, c numeric)
  distributed by (id);
insert into pdlru.src
  select g, (g * 7) % 10, g, 'b' || g, g / 3.0 from generate_series(1, 20000) g;

create table pdlru.aocs_ref (like pdlru.src)
  with (appendonly=true, orientation=column)
  distributed by (id)
  partition by range (part) (start (0) end (10) every (1));
create table pdlru.aocs (like pdlru.src)
  with (appendonly=true, orientation=column)
  distributed by (id)
  partition by range (part) (start (0) end (10) every (1));

set gp_partition_insert_desc_budget = off;
insert into pdlru.aocs_ref select * from pdlru.src;
set gp_partition_insert_desc_budget = on;
set statement_mem = '1MB';
insert into pdlru.aocs select * from pdlru.src;
reset statement_mem;

select (select row(count(*), sum(id), sum(a), sum(c), sum(hashtext(b)::bigint))
        from pdlru.aocs_ref)
     = (select row(count(*), sum(id), sum(a), sum(c), sum(hashtext(b)::bigint))
        from pdlru.aocs)
       as bounded_equals_unbounded;
select count(*) as dup_tids from (
  select gp_segment_id, part, ctid from pdlru.aocs group by 1, 2, 3 having count(*) > 1
) d;
select pdlru.max_modcount('pdlru.aocs') > 1 as evicted;

-- the default statement_mem holds every touched descriptor -> no eviction
create table pdlru.aocs_ample (like pdlru.src)
  with (appendonly=true, orientation=column)
  distributed by (id)
  partition by range (part) (start (0) end (10) every (1));
insert into pdlru.aocs_ample select * from pdlru.src;
select pdlru.max_modcount('pdlru.aocs_ample') as ample_budget_modcount;

----------------------------------------------------------------------
-- 3. Index / block directory correctness across an eviction (AO row).
----------------------------------------------------------------------

create table pdlru.aoi (id int, part int, k int)
  with (appendonly=true, blocksize=262144)
  distributed by (id)
  partition by range (part) (start (0) end (6) every (1));
create index aoi_k on pdlru.aoi (k);

set statement_mem = '1MB';
insert into pdlru.aoi select g, (g * 3) % 6, g % 100 from generate_series(1, 12000) g;
reset statement_mem;
select pdlru.max_modcount('pdlru.aoi') > 1 as evicted;

set enable_seqscan = off;
select count(*) as idx_count from pdlru.aoi where k = 42;
set enable_seqscan = on;
set enable_indexscan = off;
set enable_bitmapscan = off;
select count(*) as seq_count from pdlru.aoi where k = 42;
-- index agrees with seqscan for every key value
set enable_seqscan = off;
create temp table idxcnt as select k, count(*) c from pdlru.aoi group by k;
set enable_seqscan = on;
select count(*) as key_count_mismatches from (
  select i.k from idxcnt i
  join (select k, count(*) c from pdlru.aoi group by k) s on i.k = s.k
  where i.c <> s.c
) d;
reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;

----------------------------------------------------------------------
-- 4. UPDATE / DELETE on partitions that were evicted and re-opened.
----------------------------------------------------------------------

update pdlru.aocs set b = b || '!' where id % 5 = 0;
delete from pdlru.aocs where id % 13 = 0;
select count(*) as rows_after,
       count(*) filter (where b like '%!') as updated_rows
from pdlru.aocs;
select count(*) as dup_tids from (
  select gp_segment_id, part, ctid from pdlru.aocs group by 1, 2, 3 having count(*) > 1
) d;

----------------------------------------------------------------------
-- 5. Multi-level partitioning + default partitions.
----------------------------------------------------------------------

create table pdlru.ml_ref (id int, r int, s int, v text)
  with (appendonly=true, orientation=column)
  distributed by (id)
  partition by range (r)
  subpartition by list (s)
    subpartition template (
      subpartition s0 values (0),
      subpartition s1 values (1),
      default subpartition sdef )
  (start (0) end (4) every (1), default partition rdef);
create table pdlru.ml (like pdlru.ml_ref)
  with (appendonly=true, orientation=column)
  partition by range (r)
  subpartition by list (s)
    subpartition template (
      subpartition s0 values (0),
      subpartition s1 values (1),
      default subpartition sdef )
  (start (0) end (4) every (1), default partition rdef);

set gp_partition_insert_desc_budget = off;
insert into pdlru.ml_ref select g, g % 6, g % 4, 'v' || g from generate_series(1, 8000) g;
set gp_partition_insert_desc_budget = on;
set statement_mem = '1MB';
insert into pdlru.ml     select g, g % 6, g % 4, 'v' || g from generate_series(1, 8000) g;
reset statement_mem;
select (select row(count(*), sum(id)) from pdlru.ml_ref)
     = (select row(count(*), sum(id)) from pdlru.ml)
       as bounded_equals_unbounded;
select pdlru.max_modcount('pdlru.ml') > 1 as evicted;

----------------------------------------------------------------------
-- 6. Transaction abort after evictions: flushed segfiles roll back.
----------------------------------------------------------------------

create table pdlru.ao_abort (id int, part int)
  with (appendonly=true, blocksize=262144)
  distributed by (id)
  partition by range (part) (start (0) end (6) every (1));

begin;
set statement_mem = '1MB';
insert into pdlru.ao_abort select g, g % 6 from generate_series(1, 6000) g;
reset statement_mem;
select count(*) as in_txn from pdlru.ao_abort;
rollback;
select count(*) as after_rollback from pdlru.ao_abort;

-- the segfiles are usable again after the aborted write
set statement_mem = '1MB';
insert into pdlru.ao_abort select g, g % 6 from generate_series(1, 600) g;
reset statement_mem;
select count(*) as reused_after_abort from pdlru.ao_abort;

----------------------------------------------------------------------
-- 7. The setting reaches QEs started after it was SET (it is a synced
--    GUC): let the idle gangs go, then load again on fresh ones.
----------------------------------------------------------------------

truncate pdlru.ao_rr;
set gp_vmem_idle_resource_timeout = 100;
select pg_sleep(1);
set statement_mem = '1MB';
insert into pdlru.ao_rr select g, g % 8, 'v' || g from generate_series(1, 8000) g;
reset statement_mem;
reset gp_vmem_idle_resource_timeout;
select pdlru.max_modcount('pdlru.ao_rr') > 1 as evicted_on_fresh_gangs;

reset search_path;
set client_min_messages to warning;
drop schema pdlru cascade;
