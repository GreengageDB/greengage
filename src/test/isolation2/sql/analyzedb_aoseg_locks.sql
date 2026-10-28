CREATE TABLE analyzedb_aoseg_locks (a int, b int) WITH (appendonly=true) DISTRIBUTED BY (a)
PARTITION BY RANGE (b) (START (0) END (2) EVERY (1));
INSERT INTO analyzedb_aoseg_locks SELECT i, i % 2 FROM generate_series(1, 10) i;

1: BEGIN;
1: TRUNCATE analyzedb_aoseg_locks_1_prt_1;

!&PGOPTIONS="$PGOPTIONS -c deadlock_timeout=600s" analyzedb -a -d isolation2test -t public.analyzedb_aoseg_locks > /dev/null 2>&1;
2: SELECT wait_until_waiting_for_required_lock((SELECT segrelid::regclass::text FROM pg_appendonly WHERE relid = 'analyzedb_aoseg_locks_1_prt_1'::regclass), 'AccessShareLock', -1);

1: TRUNCATE analyzedb_aoseg_locks;
1: COMMIT;

!\retcode while pgrep -f '[a]nalyzedb -a -d isolation2test -t public.analyzedb_aoseg_locks' > /dev/null; do sleep 0.1; done;

1: BEGIN;
1: TRUNCATE analyzedb_aoseg_locks_1_prt_2;

!&PGOPTIONS="$PGOPTIONS -c deadlock_timeout=600s" analyzedb -a -d isolation2test -t public.analyzedb_aoseg_locks > /dev/null 2>&1;
2: SELECT wait_until_waiting_for_required_lock((SELECT segrelid::regclass::text FROM pg_appendonly WHERE relid = 'analyzedb_aoseg_locks_1_prt_2'::regclass), 'AccessShareLock', -1);

1: TRUNCATE analyzedb_aoseg_locks;
1: COMMIT;

!\retcode while pgrep -f '[a]nalyzedb -a -d isolation2test -t public.analyzedb_aoseg_locks' > /dev/null; do sleep 0.1; done;

DROP TABLE analyzedb_aoseg_locks;
