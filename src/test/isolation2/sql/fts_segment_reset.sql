-- This test verifies that FTS shouldn't issue failover to mirror when
-- the primary is taking long in the RESET state.

-- start_matchsubs
-- m/seg0 [0-9.]+:\d+/
-- s/seg0 [0-9.]+:\d+/seg0 IP:PORT/
-- end_matchsubs

-- Helper for the CPU check at the end of this test.  The ftsprobe worker is
-- a bgworker child of the QD postmaster.  A SELECT without a FROM always
-- runs on the QD, so the worker is visible to this function (any query over
-- a table, temp tables included, may get dispatched to a QE, where no
-- ftsprobe worker exists).  The 'start' call remembers the worker's pid and
-- CPU time in SD (per-session data); the 'end' call verifies the very same
-- worker is still running and that it burned at most 30% of one CPU in
-- between.  Anything unexpected errors out loudly, so a broken check can
-- never pass silently.
CREATE OR REPLACE LANGUAGE plpython3u;
CREATE OR REPLACE FUNCTION fts_cpu_check(action text)
RETURNS bool AS $$
    import os, time

    def sample(expect_pid):
        # Backend processes inherit the postmaster's working directory, which
        # is the QD data directory.  Getting it without any query matters:
        # even SHOW is rewritten into a SELECT from pg_settings and gets
        # dispatched to the QEs, where it can fail while they recover.
        datadir = os.getcwd()
        master_pid = int(open(os.path.join(datadir, 'postmaster.pid')).readline())
        for entry in os.listdir('/proc'):
            if not entry.isdigit():
                continue
            try:
                stat = open('/proc/%s/stat' % entry).read()
                fields = stat[stat.rfind(')') + 2:].split()
                if int(fields[1]) != master_pid:
                    continue
                cmd = open('/proc/%s/cmdline' % entry).read()
                if 'ftsprobe process' not in cmd:
                    continue
                if expect_pid is not None and int(entry) != expect_pid:
                    plpy.error('ftsprobe process restarted (%d -> %d)'
                               % (expect_pid, int(entry)))
                return int(entry), (int(fields[11]) + int(fields[12])) \
                       / float(os.sysconf('SC_CLK_TCK'))
            except (OSError, ValueError, IndexError):
                continue        # process went away meanwhile
        plpy.error('ftsprobe process not found under postmaster pid %d'
                   % master_pid)

    if action == 'start':
        pid, secs = sample(None)
        SD['start'] = (pid, secs, time.time())
        return True
    elif action == 'end':
        try:
            pid0, secs0, t0 = SD['start']
        except KeyError:
            plpy.error("fts_cpu_check('start') was not called")
        pid, secs = sample(pid0)
        return secs - secs0 <= 0.30 * (time.time() - t0)
    else:
        plpy.error("action must be 'start' or 'end'")
$$ LANGUAGE plpython3u;

-- Let FTS detect/declare failure sooner
!\retcode gpconfig -c gp_fts_probe_interval -v 10 --masteronly;
-- Because after RESET, it still takes a little while for the primary
-- to restart, and potentially makes FTS think it's in "recovery not
-- in progress" stage and promote the mirror, we would need the FTS
-- to make that decision a bit less frequently.
!\retcode gpconfig -c gp_fts_probe_retries -v 15;
!\retcode gpstop -u;

-- Let the background writer sleep 17 seconds to delay the resetting.
-- This number is selected to be larger than the 15-second retry window
-- which makes a meaningful test, meanwhile reduce the chance that FTS sees
-- a "recovery not in progress" primary as much as possible.
-- It also will not let the postmaster send SIGKILL to the bgwriter.
select gp_inject_fault_infinite('postmaster_delay_termination_bg_writer', 'skip', dbid)
from gp_segment_configuration where role = 'p' and content = 0;

-- We need to set fault at the handler of abort signal from backend
-- so then we can wait until it is triggered. We need to do this because
-- sometimes process of calling abort() (which is issued by the injection
-- of 'panic' fault in the next lines of this test) and abortion itself can
-- take a long time to complete. So, sometimes this test can give unexpected
-- results, when you think that commands sended after issuing panic fault
-- should fail, but they are being executed without any errors.
select gp_inject_fault('backend_abort_handling', 'skip', dbid)
from gp_segment_configuration where role = 'p' and content = 0;

-- While seg0's primary is down and its postmaster sits in the reset state
-- (waiting for the delayed bgwriter), FTS keeps retrying to probe it (see
-- processRetry() in ftsprobe.c) and used to busy-spin that whole period
-- away at 100% CPU.  Open the CPU sampling window now, until the joins
-- below complete, i.e. across seg0's whole reset period.
select fts_cpu_check('start');

-- Now bring down primary of seg0. There're a lot of ways to do that, in order
-- to better emulate a real-world scnarios we're injecting a PANIC to do that.
1:select gp_inject_fault('start_prepare', 'panic', dbid) 
from gp_segment_configuration where role = 'p' AND content = 0;
1&:create table fts_reset_t(a int);

-- We now wait till our seg0 will start reaping it's childs and resetting.
-- We can ignore the results of this command, because it can throw 3 things:
-- error because we couldn't send command because seg0 already restarting;
-- error because reaper killed backend on seg0 because it started reaping
-- backends (previous panic fault injection)
-- no error, because we managed to overrun reaper and sendeed results
-- of wait_until_triggered back
-- start_ignore
select gp_wait_until_triggered_fault('backend_abort_handling', 1, dbid)
from gp_segment_configuration where role = 'p' and content = 0;
-- end_ignore

-- This should fail due to the seg0 in reset mode
2&:create table fts_reset_t2(a int);

-- Kick a probe round so that the FTS retry backoff for seg0 starts right
-- away, inside the sampling window opened above.
select gp_request_fts_probe_scan();

-- Try another one but let the gang creation retry for longer.
-- Default is 2000ms (gp_gang_creation_retry_timer) * 5 (gp_gang_creation_retry_count) = 10s.
-- Now make it 50s which is well longer than the delay we inserted before, so it can succeed.
3:set gp_gang_creation_retry_timer = 10000;
3:create table fts_reset_t3(a int);

1<:
2<:

-- By now seg0 is back up, so the sampling window above covered its whole
-- reset period, which the ftsprobe worker must idle through: the second
-- sample must land on the same worker (fts_cpu_check() checks the pid) and
-- it must not have burned more than 30% of one CPU over the window.
select fts_cpu_check('end');

-- We shouldn't see failover to mirror
select gp_request_fts_probe_scan();
select dbid, role, preferred_role, status from gp_segment_configuration where content = 0;

select gp_inject_fault('postmaster_delay_termination_bg_writer', 'reset', dbid) from gp_segment_configuration where role = 'p' and content = 0;

-- The only table that should have been created successfully
drop table fts_reset_t3;

-- In case anything goes wrong, we don't want to affect other tests. So rebalance the cluster anyway.
!\retcode gprecoverseg -aF
!\retcode gprecoverseg -ar

-- restore parameters
!\retcode gpconfig -r gp_fts_probe_interval --masteronly;
!\retcode gpconfig -r gp_fts_probe_retries --masteronly;
!\retcode gpstop -u;
