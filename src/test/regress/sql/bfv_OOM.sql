-- Test for correct processing AbortTransaction after getting OOM at
-- cdbcomponent_recycleIdleQE and re-entering here from AbortTransaction.

-- start_ignore
create extension if not exists gp_inject_fault;
-- end_ignore

-- start_matchsubs
-- m/WARNING:.*Any temporary tables for this session have been dropped because the gang was disconnected/
-- s/session id \=\s*\d+/session id \= DUMMY/gm
-- end_matchsubs

create temp table test_table (f1 int) distributed by (f1);

select gp_inject_fault('cdb_freelist_append_oom', 'skip', dbid)
from gp_segment_configuration
where role = 'p' and content = -1;

do $$
declare
  rec1 record;
begin
  for rec1 in select * from test_table
  loop
    null;
  end loop;
end; $$;

select gp_inject_fault('cdb_freelist_append_oom', 'reset', dbid)
from gp_segment_configuration
where role = 'p' and content = -1;
