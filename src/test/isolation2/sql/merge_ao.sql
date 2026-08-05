-- MERGE with MATCHED actions on an append-optimized table.
--
-- AO DML takes an ExclusiveLock upgrade on the QD (CdbTryOpenTable), so
-- concurrent writers are serialized: a second MERGE blocks until the first
-- one commits and then sees its result.  This also exercises the wholerow
-- old-tuple path (AO cannot fetch a row by TID) under concurrency.
1: drop table if exists merge_ao_tgt;
1: drop table if exists merge_ao_src;
1: create table merge_ao_tgt (k int, v int) with (appendonly = true) distributed by (k);
1: create table merge_ao_src (k int, v int) distributed by (k);
1: insert into merge_ao_tgt select i, 0 from generate_series(1, 5) i;
1: insert into merge_ao_src select i, 1 from generate_series(1, 8) i;

-- concurrent MERGE vs MERGE: the second blocks, then applies on top of the
-- first one's committed result
1: begin;
1: merge into merge_ao_tgt t using merge_ao_src s on t.k = s.k when matched then update set v = t.v + s.v;
2&: merge into merge_ao_tgt t using merge_ao_src s on t.k = s.k when matched then update set v = t.v + 10 when not matched then insert values (s.k, 100);
1: commit;
2<:
3: select k, v from merge_ao_tgt order by k;

-- concurrent plain UPDATE vs MERGE DELETE action: also serialized
1: begin;
1: update merge_ao_tgt set v = v + 1 where k = 1;
2&: merge into merge_ao_tgt t using merge_ao_src s on t.k = s.k when matched and t.k = 1 then delete;
1: commit;
2<:
3: select k, v from merge_ao_tgt order by k;

1: drop table merge_ao_tgt;
1: drop table merge_ao_src;
