-- Verify the allocator crash diagnostics (pddiag): a backend that dies with
-- SIGABRT (here via a panic fault -> abort()) must leave a complete pddiag
-- report in the server log: every report line is prefixed "pddiag: ", the
-- header carries signo/pid, and the block ends with "pddiag: end.".  The
-- victim emits a marker WARNING into the server log before dying and saves
-- its pid in a heap table, so a fresh session can locate the exact report
-- block among everything else in the shared log.
--
-- The report is written to stderr, which the logging collector routes into
-- the current server log file (the syslogger copies non-protocol pipe data
-- verbatim); the test therefore requires logging_collector=on and a
-- per-datadir log_directory (as in gpdemo).  The marker is located by its
-- csvlog WARNING field (,"WARNING","01000","<marker>",) rather than by the
-- bare marker text: statement logging (log_statement=all is a demo default)
-- would otherwise log the marker string inside the arming/check statements
-- themselves, after the real marker line.  gp_log_format=csv is the server
-- default; a text-format log would break the pattern on purpose (loudly).
--
-- The PANIC text reaching the client is part of the expected output: unlike
-- the regress driver, isolation2 does not process -- start_matchsubs blocks
-- from test files (only @post_run-generated rules), so nothing masks it.

CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

-- give the event ring some content so the report has events to dump
1: SELECT count(*) FROM generate_series(1,100000) g;
-- guarantee: parse/analysis runs entirely on the QD, and this VALUES
-- list forces several thousand allocations through one allocator
-- funnel site, crossing the 1:2048 sampling no matter where the
-- executor dispatches the scans above -- the trace entries need at
-- least one recorded ring event for a nonzero seq (the dump skips
-- seq-0 trace entries)
1: SELECT count(*) FROM (VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) v(x);
-- marker into the server log: the report block must appear AFTER this line;
-- locating by marker (not by a report-pid file name) also makes reruns exact
1: DO $$ BEGIN RAISE WARNING 'pddiag_test_phase1_marker'; END $$;
-- remember the victim pid, then arm a panic on the next query of session 1;
-- the panic abort()s the backend and pddiag's SIGABRT handler dumps the report
1: CREATE TABLE pddiag_victim_pid(victim_pid int);
1: INSERT INTO pddiag_victim_pid SELECT pg_backend_pid();
1: select gp_inject_fault('exec_simple_query_start', 'panic', current_setting('gp_dbid')::smallint);
1: select 1;
-- session 1 is dead now; verify the report from a fresh session: take the
-- newest recently-written server log that contains the marker and check the
-- text after the LAST marker occurrence (the reverse/position arithmetic
-- finds the last occurrence; substring() from that point is the tail)
2: SELECT position('pddiag: crash report' in tail) > 0 AS has_report, position('pddiag: signo=6' in tail) > 0 AS has_signo, position('pddiag: pid=' || (SELECT victim_pid FROM pddiag_victim_pid) || E'\n' in tail) > 0 AS has_pid, position('pddiag: end.' in tail) > 0 AS is_complete FROM (SELECT substring(content from length(content) - position(',"rekram_1esahp_tset_gaiddp","00010","GNINRAW",' in reverse(content)) - length(',"WARNING","01000","pddiag_test_phase1_marker",') + 2) AS tail FROM (SELECT pg_read_file(current_setting('log_directory') || '/' || f) AS content, (SELECT modification FROM pg_stat_file(current_setting('log_directory') || '/' || f)) AS mtime FROM pg_ls_dir(current_setting('log_directory')) AS f WHERE NOT (SELECT isdir FROM pg_stat_file(current_setting('log_directory') || '/' || f)) AND (SELECT modification FROM pg_stat_file(current_setting('log_directory') || '/' || f)) > now() - interval '10 minutes' ORDER BY mtime DESC LIMIT 5) recent WHERE position(',"WARNING","01000","pddiag_test_phase1_marker",' in content) > 0 ORDER BY mtime DESC LIMIT 1) r;
2: DROP TABLE pddiag_victim_pid;

-- phase 2: cover the SIGSEGV slot itself (the panic case above exercised
-- only SIGABRT) and the si_code-acceptance path in the SA_SIGINFO handler.
-- The stock 'segv' fault writes through NULL, so the recorded fault address
-- is 0 and the report again falls back to the unfiltered ring tail; a
-- nonzero-address probe of the per-pointer ring history would need a
-- wild-pointer fault the stock injector cannot emit.  The report is located
-- by the marker, not by state written by the victim: killing a QD backend
-- takes every other backend down with it (shared-memory reinit) and on the
-- SIGSEGV path a just-committed distributed transaction of the dead session
-- is not reliably visible to a fresh session.
3: DO $$ BEGIN RAISE WARNING 'pddiag_test_phase2_marker'; END $$;
3: select gp_inject_fault('exec_simple_query_start', 'segv', current_setting('gp_dbid')::smallint);
-- the error text a client sees for a segv is not stable: blank it
3: @post_run 'echo ""' : select 1;

-- session 3 is dead; the report after the LAST phase-2 marker must carry
-- signo=11, the si_code-accepted (but NULL) fault address, and the
-- completion marker
4: SELECT position('pddiag: crash report' in tail) > 0 AS has_report, position('pddiag: signo=11' in tail) > 0 AS has_segv_signo, position(E'pddiag: fault=0\n' in tail) > 0 AS has_null_fault, position('pddiag: end.' in tail) > 0 AS is_complete FROM (SELECT substring(content from length(content) - position(',"rekram_2esahp_tset_gaiddp","00010","GNINRAW",' in reverse(content)) - length(',"WARNING","01000","pddiag_test_phase2_marker",') + 2) AS tail FROM (SELECT pg_read_file(current_setting('log_directory') || '/' || f) AS content, (SELECT modification FROM pg_stat_file(current_setting('log_directory') || '/' || f)) AS mtime FROM pg_ls_dir(current_setting('log_directory')) AS f WHERE NOT (SELECT isdir FROM pg_stat_file(current_setting('log_directory') || '/' || f)) AND (SELECT modification FROM pg_stat_file(current_setting('log_directory') || '/' || f)) > now() - interval '10 minutes' ORDER BY mtime DESC LIMIT 5) recent WHERE position(',"WARNING","01000","pddiag_test_phase2_marker",' in content) > 0 ORDER BY mtime DESC LIMIT 1) r;
