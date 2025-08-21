-- start_ignore
create extension if not exists gp_inject_fault;
-- end_ignore

-- start_matchsubs
-- m/WARNING:.*Any temporary tables for this session have been dropped because the gang was disconnected/
-- s/session id \=\s*\d+/session id \= DUMMY/gm
-- end_matchsubs

CREATE TEMP TABLE t1();

SELECT gp_inject_fault('cdb_freelist_append_oom', 'skip', dbid)
  FROM gp_segment_configuration
  WHERE role = 'p' AND content = -1;

-- Emulate an OOM inside cdbdisp_destroyDispatcherState(). We should gracefully
-- recover instead of trying entering recursion or getting SIGSEGV.
DO $$
  DECLARE
    rec1 RECORD;
  BEGIN
    FOR rec1 IN SELECT * FROM t1
    LOOP
      NULL;
    END LOOP;
  END;
$$;

SELECT gp_inject_fault('all', 'reset', dbid)
  FROM gp_segment_configuration
  WHERE role = 'p' AND content = -1;
