include: helpers/server_helpers.sql;

-- table to just store the master's data directory path on segment.
CREATE TABLE lockmodes_datadir(a int, dir text);
INSERT INTO lockmodes_datadir select 1,datadir from gp_segment_configuration where role='p' and content=-1;

1: set optimizer = off;

create or replace view show_locks_lockmodes as
  select locktype, mode, granted, relation::regclass
  from pg_locks
  where
    gp_segment_id = -1 and
    locktype = 'relation' and
    relation::regclass::text like 't_lockmods%';

show gp_enable_global_deadlock_detector;


-- 1. The firs part of test is with
--    gp_enable_global_deadlock_detector off

-- 1.1 test for heap tables
create table t_lockmods (c int) distributed randomly;
insert into t_lockmods select * from generate_series(1, 5);

create table t_lockmods1 (c int) distributed randomly;

create table t_lockmods_rep(c int) distributed replicated;

-- 1.1.2 update | delete should hold ExclusiveLock on result relations
1: begin;
1: update t_lockmods set c = c + 0;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: delete from t_lockmods;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.1.3 insert should hold RowExclusiveLock on result relations
1: begin;
1: insert into t_lockmods select * from generate_series(1, 5);
2: select * from show_locks_lockmodes;
1: abort;

-- 1.1.5 use cached plan should be consistent with no cached plan
1: prepare update_tlockmods as update t_lockmods set c = c + 0;
1: prepare delete_tlockmods as delete from t_lockmods;
1: prepare insert_tlockmods as insert into t_lockmods select * from generate_series(1, 5);

1: begin;
1: execute update_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute delete_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute insert_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.2 test for AO table
create table t_lockmods_ao (c int) with (appendonly=true) distributed randomly;
insert into t_lockmods_ao select * from generate_series(1, 8);
create table t_lockmods_ao1 (c int) with (appendonly=true) distributed randomly;

-- 1.2.2 update | delete should hold ExclusiveLock on result relations
1: begin;
1: update t_lockmods_ao set c = c + 0;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: delete from t_lockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

-- 1.2.3 insert should hold RowExclusiveLock on result relations
1: begin;
1: insert into t_lockmods_ao select * from generate_series(1, 5);
2: select * from show_locks_lockmodes;
1: abort;

-- 1.2.4 use cached plan should be consistent with no cached plan
1: prepare update_tlockmods_ao as update t_lockmods_ao set c = c + 0;
1: prepare delete_tlockmods_ao as delete from t_lockmods_ao;
1: prepare insert_tlockmods_ao as insert into t_lockmods_ao select * from generate_series(1, 5);

1: begin;
1: execute update_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute delete_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute insert_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1q:
2q:

-- enable gdd
7: ALTER SYSTEM SET gp_enable_global_deadlock_detector TO on;
-- Use utility session on seg 0 to restart master. This way avoids the
-- situation where session issuing the restart doesn't disappear
-- itself.
1U:SELECT pg_ctl(dir, 'restart') from lockmodes_datadir;

1: show gp_enable_global_deadlock_detector;

1: set optimizer = off;

-- 2. The firs part of test is with
--    gp_enable_global_deadlock_detector on

-- 2.1.2 update | delete should hold RowExclusiveLock on result relations
1: begin;
1: update t_lockmods set c = c + 0;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: delete from t_lockmods;
2: select * from show_locks_lockmodes;
1: abort;

-- 2.1.3 insert should hold RowExclusiveLock on result relations
1: begin;
1: insert into t_lockmods select * from generate_series(1, 5);
2: select * from show_locks_lockmodes;
1: abort;

-- 2.1.5 use cached plan should be consistent with no cached plan
1: prepare update_tlockmods as update t_lockmods set c = c + 0;
1: prepare delete_tlockmods as delete from t_lockmods;
1: prepare insert_tlockmods as insert into t_lockmods select * from generate_series(1, 5);

1: begin;
1: execute update_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute delete_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute insert_tlockmods;
2: select * from show_locks_lockmodes;
1: abort;

-- 2.2.2 update | delete should hold ExclusiveLock on result relations
1: begin;
1: update t_lockmods_ao set c = c + 0;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: delete from t_lockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

-- 2.2.3 insert should hold RowExclusiveLock on result relations
1: begin;
1: insert into t_lockmods_ao select * from generate_series(1, 5);
2: select * from show_locks_lockmodes;
1: abort;

-- 2.2.4 use cached plan should be consistent with no cached plan
1: prepare update_tlockmods_ao as update t_lockmods_ao set c = c + 0;
1: prepare delete_tlockmods_ao as delete from t_lockmods_ao;
1: prepare insert_tlockmods_ao as insert into t_lockmods_ao select * from generate_series(1, 5);

1: begin;
1: execute update_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute delete_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1: begin;
1: execute insert_tlockmods_ao;
2: select * from show_locks_lockmodes;
1: abort;

1q:
2q:

-- 2.8 Verify behaviors of select with locking clause (i.e. select for update)
-- when running concurrently with index creation, for Heap tables.
-- For AO/CO tables, refer to create_index_allows_readonly.source.

1: CREATE TABLE create_index_select_for_update_tbl(a int, b int);
1: INSERT INTO create_index_select_for_update_tbl SELECT i,i FROM generate_series(1,10)i;
1: set optimizer = off;

-- 2.8.1 with GDD enabled, expect blocking as is.
-- This behavior will be changed to no-blocking on GPDB7 and later.
1: show gp_enable_global_deadlock_detector;

1: BEGIN;
1: SELECT * FROM create_index_select_for_update_tbl WHERE a = 2 FOR UPDATE;

2: set optimizer = off;

2: BEGIN;
-- expect blocking as is
-- This behavior will be changed to no-blocking on GPDB7 and later.
2&: CREATE INDEX create_index_select_for_update_idx ON create_index_select_for_update_tbl(a);

1: COMMIT;

2<:
2: COMMIT;

2: DROP INDEX create_index_select_for_update_idx;

2: BEGIN;
2: CREATE INDEX create_index_select_for_update_idx ON create_index_select_for_update_tbl(a);

1: BEGIN;
-- expect blocking as is
-- This behavior will be changed to no-blocking on GPDB7 and later.
1&: SELECT * FROM create_index_select_for_update_tbl WHERE a = 2 FOR UPDATE;

2: COMMIT;

1<:
1: COMMIT;
-- close session to avoid renew session failure after restart
1q:

2: DROP INDEX create_index_select_for_update_idx;

-- 2.8.2 with GDD disabled, expect blocking
-- reset gdd
2: ALTER SYSTEM RESET gp_enable_global_deadlock_detector;
-- close session to avoid renew session failure after restart
2q:
1U:SELECT pg_ctl(dir, 'restart') from lockmodes_datadir;

1: set optimizer = off;
1: show gp_enable_global_deadlock_detector;

1: BEGIN;
1: SELECT * FROM create_index_select_for_update_tbl WHERE a = 2 FOR UPDATE;

2: set optimizer = off;

2: BEGIN;
-- expect blocking
2&: CREATE INDEX create_index_select_for_update_idx ON create_index_select_for_update_tbl(a);

1: COMMIT;

2<:
2: COMMIT;

2: DROP INDEX create_index_select_for_update_idx;

2: BEGIN;
2: CREATE INDEX create_index_select_for_update_idx ON create_index_select_for_update_tbl(a);

1: BEGIN;
-- expect blocking
1&: SELECT * FROM create_index_select_for_update_tbl WHERE a = 2 FOR UPDATE;

2: COMMIT;

1<:
1: COMMIT;

1: drop table lockmodes_datadir;
1q:
2q:
