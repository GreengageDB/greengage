-- Check that table expand doesn't lead to invalid data distribution
-- if WAL wasn't replicated to a mirror in time, the respective Primary failed,
-- and the mirror with obsolete data is promoted. That could happen if the user
-- disabled 'synchronous_commit'. In this case we still use synchronous commit
-- internally for the expand operation, so it should either wait till the
-- replication is complete, or fail if the primary has gone down.

include: helpers/server_helpers.sql;

create extension if not exists gp_debug_numsegments;
create extension if not exists gp_inject_fault;

select gp_debug_set_create_table_default_numsegments(2);
drop table if exists test;
create table test(a int) distributed by (a);
insert into test select generate_series(1, 100);

1:set synchronous_commit = off;

2:select gp_inject_fault('wal_sender_loop', 'suspend', dbid) from gp_segment_configuration where content=0 and role='p';

1&:alter table test expand table;

2:select gp_wait_until_triggered_fault('wal_sender_loop', 1, dbid) from gp_segment_configuration where content=0 and role='p';
2:select pg_sleep(10);
2:select pg_ctl(datadir, 'stop', 'immediate') from gp_segment_configuration where content = 0 and role = 'p';
2:select gp_request_fts_probe_scan();
2:select role, preferred_role, status from gp_segment_configuration where content = 0 order by dbid;

1<:

select count(*) from test;

select count(*), gp_segment_id from test group by gp_segment_id order by gp_segment_id;

1:reset synchronous_commit;
1q:

!\retcode gprecoverseg -aF --no-progress;

!\retcode gprecoverseg -ar;

drop table test;

-- Check the same for SET WITH (REORGANIZE=TRUE), which is used for partitioned table.
select gp_debug_set_create_table_default_numsegments(2);
create table test (a int) distributed by (a)
partition by range (a)
(
    start (0) end (1000) every (1000)
);
insert into test select generate_series(1, 100);

alter table test expand partition prepare;

1:set synchronous_commit = off;

2:select gp_inject_fault('wal_sender_loop', 'suspend', dbid) from gp_segment_configuration where content=0 and role='p';

1&:alter table test_1_prt_1 set with (reorganize=true) distributed by (a);

2:select gp_wait_until_triggered_fault('wal_sender_loop', 1, dbid) from gp_segment_configuration where content=0 and role='p';
2:select pg_sleep(10);
2:select pg_ctl(datadir, 'stop', 'immediate') from gp_segment_configuration where content = 0 and role = 'p';
2:select gp_request_fts_probe_scan();
2:select role, preferred_role, status from gp_segment_configuration where content = 0 order by dbid;

1<:

select count(*) from test;

select count(*), gp_segment_id from test group by gp_segment_id order by gp_segment_id;

1:reset synchronous_commit;
1q:

!\retcode gprecoverseg -aF --no-progress;

!\retcode gprecoverseg -ar;

drop table test;

select gp_debug_reset_create_table_default_numsegments();
