-- INSERT ... ON CONFLICT on an append-optimized table under concurrency.
--
-- The parser upgrades any ON CONFLICT on an AO target to ExclusiveLock, so
-- concurrent sessions are serialized: the second blocks and then sees the
-- first one's committed result -- there is no window in which both could
-- pass the arbiter pre-check and insert the same key.
1: drop table if exists oc_ao;
1: create table oc_ao (k int unique, v int) with (appendonly = true) distributed by (k);
1: insert into oc_ao values (1, 0);

-- concurrent DO NOTHING with the same new key: exactly one row survives
1: begin;
1: insert into oc_ao values (2, 0) on conflict (k) do nothing;
2&: insert into oc_ao values (2, 99) on conflict (k) do nothing;
1: commit;
2<:
3: select k, v from oc_ao order by k;

-- concurrent DO UPDATE on the same key: applied on top of the first result
1: begin;
1: insert into oc_ao values (1, 1) on conflict (k) do update set v = 1;
2&: insert into oc_ao values (1, 2) on conflict (k) do update set v = oc_ao.v + 10;
1: commit;
2<:
3: select k, v from oc_ao order by k;

1: drop table oc_ao;
