-- when the WAL replication lag exceeds 'max_slot_wal_keep_size', the extra WAL
-- log will be removed on the primary and the replication slot will be marked as
-- obsoleted. In this case, the mirror will be marked down as well and need full
-- recovery to brought it back. 

include: helpers/server_helpers.sql;

CREATE OR REPLACE FUNCTION advance_xlog(num int) RETURNS void AS
$$
DECLARE
	i int; 
BEGIN 
    i := 0; 
	CREATE TABLE t_dummy_switch(i int) DISTRIBUTED BY (i); 
	LOOP 
		IF i >= num THEN 
			DROP TABLE t_dummy_switch; 
			RETURN; 
		END IF; 
		PERFORM pg_switch_xlog() FROM gp_dist_random('gp_id') WHERE gp_segment_id=0; 
		INSERT INTO t_dummy_switch SELECT generate_series(1,10); 
		i := i + 1; 
	END LOOP; 
	DROP TABLE t_dummy_switch; 
END; 
$$ language plpgsql;

CREATE OR REPLACE FUNCTION check_wal_file_count(content int)
RETURNS text AS $$
    import subprocess
    rv = plpy.execute("select datadir from gp_segment_configuration where content = 0 and role = 'p'", 2)
    datadir = rv[0]['datadir']
    cmd = 'ls %s/pg_xlog/ | grep "^[0-9A-F]*$" | wc -l' % datadir
    remove_output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    return remove_output.strip()
$$ LANGUAGE PLPYTHONU;

-- max_slot_wal_keep_size is 64MB * 4
!\retcode gpconfig -c max_slot_wal_keep_size -v 256;
!\retcode gpconfig -c checkpoint_segments -v 1 --skipvalidation;
!\retcode gpconfig -c wal_keep_segments -v 0 --skipvalidation;
!\retcode gpconfig -c gp_fts_probe_retries -v 2 --masteronly;
!\retcode gpconfig -c gp_fts_mark_mirror_down_grace_period -v 0;
!\retcode gpstop -u;

-- walsender skip sending WAL to the mirror
1: SELECT gp_inject_fault_infinite('walsnd_skip_send', 'skip', dbid) FROM gp_segment_configuration WHERE content=0 AND role='p';

2: BEGIN;
2: DROP TABLE IF EXISTS t_slot_size_limit;
2: CREATE TABLE t_slot_size_limit(a int);
2: INSERT INTO t_slot_size_limit SELECT generate_series(1,1000);

-- generate 2 more WAL files, which exceeds 'max_slot_wal_keep_size'
2: SELECT advance_xlog(12);

-- checkpoint will trigger the check of obsolete replication slot, it will stop the walsender.
2: CHECKPOINT;

1: SELECT gp_inject_fault_infinite('walsnd_skip_send', 'reset', dbid) FROM gp_segment_configuration WHERE content=0 AND role='p';
1: SELECT gp_request_fts_probe_scan();
2: END;

-- check the mirror is down and the sync_error is set.
1: SELECT role, preferred_role, status FROM gp_segment_configuration WHERE content = 0;
1: SELECT sync_error FROM gp_stat_replication WHERE gp_segment_id = 0;
-- the number of WAL file is approximate to 1 + XLOGfileslop(checkpoint_segments * 2 + 1) + max_slot_wal_keep_size / 64 / 1024 
1: SELECT check_wal_file_count(0)::int <= 8;

-- do full recovery
!\retcode gprecoverseg -aF;
select wait_until_segment_synchronized(0);

-- the mirror is up and the replication is back
1: SELECT role, preferred_role, status FROM gp_segment_configuration WHERE content = 0;
1: SELECT state, sync_error FROM gp_stat_replication WHERE gp_segment_id = 0;

-- failover to the mirror and check the data on the primary is replicated to the mirror
1: select pg_ctl((select datadir from gp_segment_configuration c where c.role='p' and c.content=0), 'stop');
1: select gp_request_fts_probe_scan();
1: select content, preferred_role, role, status, mode from gp_segment_configuration where content = 0;
1: SELECT count(*) FROM t_slot_size_limit;

!\retcode gprecoverseg -a;
!\retcode gprecoverseg -ar;

!\retcode gpconfig -r wal_keep_segments --skipvalidation;
!\retcode gpconfig -r checkpoint_segments --skipvalidation;
!\retcode gpconfig -r max_slot_wal_keep_size;
!\retcode gpconfig -r gp_fts_probe_retries --masteronly;
!\retcode gpconfig -r gp_fts_mark_mirror_down_grace_period;
!\retcode gpstop -u;

-- no limit on the wal keep size
1: SELECT advance_xlog(12);
1: SELECT check_wal_file_count(0)::int > 12;
