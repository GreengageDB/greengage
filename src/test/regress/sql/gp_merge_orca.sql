--
-- MERGE under ORCA (optimizer=on).  ORCA has no MERGE (CMD_MERGE) support and
-- must fall back to the Postgres planner (CTranslatorQueryToDXL raises
-- ExmiQuery2DXLUnsupportedFeature).  A production (non-cassert) build without
-- that fallback returns a NULL DXL and segfaults the coordinator, so this
-- exercises MERGE / MERGE-variants under optimizer=on on distributed and
-- partitioned targets.
--
set optimizer = on;
-- 1) basic MERGE into a distributed target
create table gp_merge_orca_t (id int, val int, txt text) distributed by (id);
create table gp_merge_orca_s (id int, val int, txt text) distributed by (id);
insert into gp_merge_orca_t select g, g, 'old' from generate_series(1, 6) g;
insert into gp_merge_orca_s select g, g*100, 'new' from generate_series(4, 9) g;
merge into gp_merge_orca_t t using gp_merge_orca_s s on t.id = s.id
  when matched then update set val = s.val, txt = s.txt
  when not matched then insert values (s.id, s.val, s.txt);
select id, val, txt from gp_merge_orca_t order by id;
-- 2) PG17: WHEN NOT MATCHED BY SOURCE
merge into gp_merge_orca_t t using gp_merge_orca_s s on t.id = s.id
  when matched then update set txt = 'kept'
  when not matched by source then delete;
select id, val, txt from gp_merge_orca_t order by id;
-- 3) PG17: MERGE ... RETURNING merge_action()
merge into gp_merge_orca_t t using gp_merge_orca_s s on t.id = s.id
  when matched then update set val = t.val + 1
  when not matched then insert values (s.id, s.val, s.txt)
  returning merge_action(), t.id, t.val;
-- 4) partitioned + distributed target (the originally-reported shape)
create table gp_merge_orca_p (a int, b int, c int)
  distributed by (a) partition by range (b) (start (1) end (11) every (1));
create table gp_merge_orca_ps (a int, b int, c int) distributed by (a);
insert into gp_merge_orca_ps select g, g%10 + 1, g from generate_series(1, 20) g;
merge into gp_merge_orca_p d using gp_merge_orca_ps s on s.a = d.a
  when matched then update set b = s.b, c = s.c
  when not matched then insert (a, b, c) values (s.a, s.b, s.c);
select count(*) as merged_rows from gp_merge_orca_p;
drop table gp_merge_orca_t, gp_merge_orca_s, gp_merge_orca_p, gp_merge_orca_ps;
reset optimizer;
