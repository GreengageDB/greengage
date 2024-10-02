-- This test checks for correct output of gp_toolkit.gp_workfile_entries view
-- It is placed in integration tests because testing for this requires checking
-- for workfiles while a query is still running.

-- check there are no workfiles
1: select segid, prefix, slice from gp_toolkit.gp_workfile_entries order by segid;

1: create table workfile_test(id serial, s text) distributed by (id);
1: insert into workfile_test(s) select v::text from generate_series(1, 2000) v;

1: select gp_inject_fault('after_workfile_mgr_create_set', 'suspend', '', '', '', 2, 2, 0, dbid) from gp_segment_configuration where content > -1 and role = 'p';

1&: select * from
    (select * from workfile_test t1, workfile_test t2 order by t1.id+t2.id limit 10000000) x,
    (select * from workfile_test t3, workfile_test t4 order by t3.id+t4.id limit 10000000) y,
    generate_series(1, 10) z;
-- wait until 2 workfile is created on each segment
2: select gp_wait_until_triggered_fault('after_workfile_mgr_create_set', 1, dbid) from gp_segment_configuration where content > -1 and role = 'p';
-- there should be exactly 6 workfiles, two for each segment (no duplication)
2: select count(*) from gp_toolkit.gp_workfile_entries group by segid;

-- interrupt the query
-- start_ignore
2: select pg_cancel_backend(pid) from pg_stat_activity where query like '%workfile_test%' and pid != pg_backend_pid();
-- end_ignore
1<:
1q:

2: select gp_inject_fault('after_workfile_mgr_create_set', 'reset', dbid) from gp_segment_configuration where content > -1 and role = 'p';

-- check there are no workfiles left
2: select segid, prefix, slice from gp_toolkit.gp_workfile_entries order by segid;
2: drop table workfile_test;
2q:
