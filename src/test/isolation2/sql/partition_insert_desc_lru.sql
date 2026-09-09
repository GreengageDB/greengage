-- Concurrency properties of gp_max_partition_open_insert_descs (bounded set of
-- open per-partition AO/AOCS insert descriptors).
--
-- When the LRU evicts a leaf partition's insert descriptor mid-statement it
-- flushes that leaf's buffered rows to the segment file. This must NOT make the
-- rows visible to other sessions before commit, and the transaction-scoped
-- 'append-only segment file' lock taken when the descriptor was opened must
-- stay held across the eviction (released only at commit/abort).

create table pdlru_iso (id int, part int, v text)
  with (appendonly=true, orientation=column)
  distributed by (id)
  partition by range (part) (start (0) end (20) every (1));

-- ============ commit path ============

1: begin;
1: set gp_max_partition_open_insert_descs = 1;
-- open + lock leaf 0's segfile on every segment
1: insert into pdlru_iso select g, 0 from generate_series(1, 2000) g;
-- touch 18 other leaves; with limit 1 this evicts (flushes + closes) leaf 0
1: insert into pdlru_iso select g, (g % 18) + 1 from generate_series(1, 6000) g;

-- concurrent reader: leaf 0's flushed-but-uncommitted rows must be invisible
2: select count(*) from pdlru_iso where part = 0;

-- the 'append-only segment file' locks are still held despite the eviction
2: select locktype, mode, granted from gp_dist_random('pg_locks')
     where locktype = 'append-only segment file' group by 1, 2, 3;

-- session 1 writes leaf 0 again (transparent re-open) and commits
1: insert into pdlru_iso select g, 0 from generate_series(2001, 2100) g;
1: end;

2: select count(*) from pdlru_iso where part = 0;
2: select count(*) as dup_tids from (
     select gp_segment_id, ctid from pdlru_iso where part = 0
     group by 1, 2 having count(*) > 1) d;
-- locks released after commit
2: select count(*) from gp_dist_random('pg_locks')
     where locktype = 'append-only segment file';

-- ============ abort path ============

-- leaf 19 is untouched by everything above
1: begin;
1: set gp_max_partition_open_insert_descs = 1;
1: insert into pdlru_iso select g, 19 from generate_series(1, 2000) g;
1: insert into pdlru_iso select g, (g % 17) + 1 from generate_series(1, 6000) g;
1: abort;

-- nothing from the aborted transaction survived
2: select count(*) from pdlru_iso where part = 19;
2: select count(*) from pdlru_iso;

-- the leaf whose descriptor was evicted inside the aborted txn still works
3: set gp_max_partition_open_insert_descs = 2;
3: insert into pdlru_iso select g, 19 from generate_series(1, 500) g;
3: select count(*) from pdlru_iso where part = 19;

1q:
2q:
3q:

drop table pdlru_iso;
