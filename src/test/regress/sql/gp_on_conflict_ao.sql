-- INSERT ... ON CONFLICT on append-optimized tables.
--
-- The arbiter unique index provides the block directory needed to fetch the
-- conflicting row by TID, and the ExclusiveLock upgrade the parser takes for
-- any ON CONFLICT on an AO target serializes concurrent writers, so the
-- speculative-insertion protocol degenerates to check-then-insert.
create schema on_conflict_ao;
set search_path = on_conflict_ao;

create table oca (k int unique, v text) with (appendonly = true) distributed by (k);
insert into oca values (1, 'one'), (2, 'two');

-- DO NOTHING: conflicting rows are skipped, the rest inserted
insert into oca values (1, 'dup'), (3, 'three') on conflict (k) do nothing;
select k, v from oca order by k;

-- DO NOTHING without an arbiter specification
insert into oca values (2, 'dup') on conflict do nothing;
select count(*) from oca;

-- DO UPDATE: the conflicting row is updated, the other inserted
insert into oca values (2, 'TWO'), (4, 'four') on conflict (k) do update set v = excluded.v || '!';
select k, v from oca order by k;

-- DO UPDATE ... WHERE that filters the conflict away
insert into oca values (3, 'x') on conflict (k) do update set v = excluded.v where oca.v = 'nope';
select k, v from oca order by k;

-- referencing both the existing row and EXCLUDED in the SET clause
insert into oca values (3, 'THREE') on conflict (k) do update set v = oca.v || '+' || excluded.v;
select k, v from oca order by k;

-- RETURNING, including PG18 OLD/NEW
insert into oca values (4, 'FOUR'), (5, 'five') on conflict (k) do update set v = excluded.v
  returning old.v as old_v, new.v as new_v, k;
select k, v from oca order by k;

-- conflict against a row inserted earlier in the same transaction
begin;
insert into oca values (6, 'six');
insert into oca values (6, 'SIX') on conflict (k) do update set v = excluded.v;
commit;
select k, v from oca where k = 6;

-- same-command duplicate keys: DO NOTHING inserts the first, skips the second
insert into oca values (10, 'a'), (10, 'b') on conflict (k) do nothing;
select k, v from oca where k = 10;

-- same-command duplicate keys: DO UPDATE raises the cardinality violation
insert into oca values (11, 'a'), (11, 'b') on conflict (k) do update set v = excluded.v;

-- updating a pre-existing row twice in one command is also rejected
insert into oca values (2, 'a'), (2, 'b') on conflict (k) do update set v = excluded.v;

-- a plain unique violation without ON CONFLICT still errors
insert into oca values (1, 'boom');

-- ON CONFLICT on AO is not allowed in serializable transactions
begin transaction isolation level serializable;
insert into oca values (12, 'x') on conflict (k) do nothing;
rollback;

-- column-oriented AO behaves identically
create table ocaco (k int unique, v text) using ao_column distributed by (k);
insert into ocaco values (1, 'one');
insert into ocaco values (1, 'dup'), (2, 'two') on conflict (k) do nothing;
insert into ocaco values (1, 'ONE') on conflict (k) do update set v = excluded.v;
select k, v from ocaco order by k;
insert into ocaco values (2, 'a'), (2, 'b') on conflict (k) do update set v = excluded.v;

set search_path = public;
drop schema on_conflict_ao cascade;
