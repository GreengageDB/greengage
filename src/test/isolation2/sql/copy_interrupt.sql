!\retcode gpconfig -c client_connection_check_interval -v 20s;
!\retcode gpstop -u;

2: SELECT gp_inject_fault_infinite('proc_kill', 'skip', dbid)
    FROM gp_segment_configuration 
    WHERE role = 'p' AND content = -1;

0: CREATE TABLE copy_interrupt_table(a int);
0: CREATE TABLE copy_interrupt_table_seg(a int) DISTRIBUTED REPLICATED;
0&: COPY copy_interrupt_table FROM PROGRAM 'while true; do echo 1; sleep 1; done | cat -';
1&: COPY copy_interrupt_table_seg FROM PROGRAM 'while true; do echo <SEGID>; sleep 1; done | cat -' ON SEGMENT;

0t:
1t:

2: SELECT gp_wait_until_triggered_fault('proc_kill', 2, dbid)
    FROM gp_segment_configuration
    WHERE role = 'p' AND content = -1;
2: SELECT gp_inject_fault('proc_kill', 'reset', dbid)
    FROM gp_segment_configuration 
    WHERE role = 'p' AND content = -1;
-- There shouldn't be any backend to terminate by this point
-- Still, terminate it to not leave hanging processes even in case test fails
-- For the test to pass, query should return zero rows
SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity 
    WHERE query LIKE 'COPY copy_interrupt_table FROM PROGRAM%'
    OR query LIKE 'COPY copy_interrupt_table_seg FROM PROGRAM%';

DROP TABLE copy_interrupt_table;
DROP TABLE copy_interrupt_table_seg;
!\retcode gpconfig -r client_connection_check_interval;
!\retcode gpstop -u;
