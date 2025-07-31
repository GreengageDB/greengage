-- Test for correct processing AbortTransaction after getting OOM at
-- cdbcomponent_recycleIdleQE and re-entering here from AbortTransaction.

--start_ignore
create extension if not exists gp_inject_fault;
drop view if exists view;
drop function if exists test_function();
drop table if exists test_table;
--end_ignore

create table test_table (f1 text, f2 text, f3 text, f4 text)
distributed by (f1);

create function test_function() returns setof test_table as
$$
declare
	rec1 record;
begin
	for rec1 in select * from test_table
	loop
		return next rec1;
	end loop;
end;
$$
language plpgsql;

create view test_view as select t.* from test_function() t;

select gp_inject_fault('cdb_freelist_append_oom', 'skip', dbid)
from gp_segment_configuration
where role = 'p' and content = -1;

select * from test_view;

select gp_inject_fault('cdb_freelist_append_oom', 'reset', dbid)
from gp_segment_configuration
where role = 'p' and content = -1;

select * from test_view;

-- Cleanup
drop view test_view;
drop function test_function();
drop table test_table;
