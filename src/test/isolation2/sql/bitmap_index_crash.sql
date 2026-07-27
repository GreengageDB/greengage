-- Test to validate bitmap index is fine after crash recovery in-case
-- meta-page buffer eviction happens. There used to be bug if the
-- metapage is not present in shared buffers, it will not be fetched
-- from disk. Instead, a zeroed out page will be returned. A
-- subsequent flush of the metapage will lead to an inadvertent
-- overwrite.
1:CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

-- connectSeg(): non-dispatching utility-mode readiness probe (psql select 1 by
-- port/hostname); used to wait for a just-restarted primary to actually serve a
-- query before the next direct connection to it.
CREATE OR REPLACE FUNCTION connectSeg(n int, port int, hostname text) RETURNS bool AS $$
import os
import subprocess
import time
for i in range(n):
    try:
        subprocess.run(["psql", "-h", str(hostname), "-p", str(port), "postgres", "-Xc", "select 1;"],
                       env={"PGOPTIONS": "-c gp_role=utility", "PATH": os.getenv("PATH")},
                       check=True)
        return True
    except Exception as e:
        time.sleep(1)
raise Exception("wait connection timeout")
$$
LANGUAGE plpython3u;
-- skip FTS probes for this test to avoid segment being marked down on restart
1:SELECT gp_inject_fault_infinite('fts_probe', 'skip', dbid)
    FROM gp_segment_configuration WHERE role='p' AND content=-1;
1:SELECT gp_request_fts_probe_scan();
1:SELECT gp_wait_until_triggered_fault('fts_probe', 1, dbid)
    FROM gp_segment_configuration WHERE role='p' AND content=-1;

-- test setup
1:CREATE TABLE bm(a int);
1:CREATE INDEX ON bm USING bitmap (a);
-- pause checkpoint to make sure CRASH RECOVERY happens for bitmap index replay
1:SELECT gp_inject_fault_infinite('checkpoint', 'skip', dbid) FROM gp_segment_configuration WHERE role='p';
1:CHECKPOINT;

-- this insert's WAL we wish to replay
1:insert into bm select generate_series(1, 5000);
1U:select pg_relation_size(oid)/current_setting('block_size')::bigint from pg_class where relname = 'bm_a_idx';

-- set small shared_buffers to make sure META_PAGE of bitmap index evicts out
1U: ALTER SYSTEM set shared_buffers to 20;
1:SELECT pg_ctl(datadir, 'restart') from gp_segment_configuration where role = 'p' and content = 1;
-- pg_ctl -w returns while the restarted primary is still in crash recovery; wait
-- until content=1 serves a real query before the next access to it.
1:SELECT connectSeg(600, port, hostname) FROM gp_segment_configuration WHERE role='p' AND content=1;

-- force index scan and make sure the index is fine
2:SET enable_seqscan to off;
2:SELECT * FROM bm WHERE a < 10;

-- teardown cleanup for the test
1Uq:
1U:ALTER SYSTEM reset shared_buffers;
2:SELECT pg_ctl(datadir, 'restart') from gp_segment_configuration where role = 'p' and content = 1;
2:SELECT connectSeg(600, port, hostname) FROM gp_segment_configuration WHERE role='p' AND content=1;
3:SELECT gp_inject_fault_infinite('checkpoint', 'reset', dbid) FROM gp_segment_configuration WHERE role='p';

3:SELECT gp_inject_fault('fts_probe', 'reset', dbid) FROM gp_segment_configuration WHERE role='p' AND content=-1;
