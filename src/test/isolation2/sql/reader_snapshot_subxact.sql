-- Rows that a transaction in progress inserted in a subtransaction it
-- released must not be taken for aborted by another session's reader gang:
-- its snapshot comes from its writer's without the subtransaction XIDs, so
-- a subtransaction XID that it cannot find among those in progress has to
-- be looked up in pg_subtrans.  Taken for finished, but not committed in
-- clog, such rows were hinted aborted for good and lost at the commit.

CREATE TABLE rs_sub (a int, b int) DISTRIBUTED BY (a);
CREATE TABLE rs_other (a int) DISTRIBUTED BY (a);

-- a reader gang (the join redistributes on b)
1: BEGIN;
1: SAVEPOINT s;
1: INSERT INTO rs_sub SELECT g, g FROM generate_series(1, 300) g;
1: RELEASE s;
-- (a transaction that finishes after the subtransaction's XID was assigned)
2: INSERT INTO rs_other SELECT generate_series(1, 30);
3: SET optimizer = off;
3: SELECT count(*) FROM rs_sub x JOIN rs_sub y ON x.b = y.b;
1: COMMIT;
1: SELECT count(*) FROM rs_sub;

-- a cursor's reader gang, which gets its snapshot from its writer otherwise
TRUNCATE rs_sub;
1: BEGIN;
1: SAVEPOINT s;
1: INSERT INTO rs_sub SELECT g, g FROM generate_series(1, 300) g;
1: RELEASE s;
2: INSERT INTO rs_other SELECT generate_series(1, 30);
3: BEGIN;
3: DECLARE c CURSOR FOR SELECT count(*) FROM rs_sub x JOIN rs_sub y ON x.b = y.b;
3: FETCH ALL FROM c;
3: COMMIT;
1: COMMIT;
1: SELECT count(*) FROM rs_sub;

-- The same with more subtransaction XIDs in progress than a snapshot holds
-- (here another transaction's, over 64 per segment): the readers look the
-- XIDs up in pg_subtrans.
TRUNCATE rs_sub;
4: BEGIN;
4: DO $$ BEGIN FOR i IN 1..600 LOOP BEGIN INSERT INTO rs_other VALUES (i); EXCEPTION WHEN others THEN NULL; END; END LOOP; END $$;
1: BEGIN;
1: SAVEPOINT s;
1: INSERT INTO rs_sub SELECT g, g FROM generate_series(1, 300) g;
1: RELEASE s;
2: INSERT INTO rs_other SELECT generate_series(1, 30);
3: SELECT count(*) FROM rs_sub x JOIN rs_sub y ON x.b = y.b;
1: COMMIT;
1: SELECT count(*) FROM rs_sub;

-- A cursor's readers, which may start long after their snapshot was taken
-- (here in a REPEATABLE READ transaction, with the subtransaction XIDs
-- overflowed), look XIDs as old as the snapshot up in pg_subtrans.
TRUNCATE rs_sub;
1: BEGIN;
1: INSERT INTO rs_sub SELECT g, g FROM generate_series(1, 300) g;
3: BEGIN ISOLATION LEVEL REPEATABLE READ;
3: SELECT count(*) FROM rs_other;
1: COMMIT;
4: COMMIT;
2: INSERT INTO rs_other SELECT generate_series(1, 30);
2: INSERT INTO rs_other SELECT generate_series(1, 30);
3: DECLARE c CURSOR FOR SELECT count(*) FROM rs_sub x JOIN rs_sub y ON x.b = y.b;
3: FETCH ALL FROM c;
3: COMMIT;
3: SELECT count(*) FROM rs_sub x JOIN rs_sub y ON x.b = y.b;
1: SELECT count(*) FROM rs_sub;

1q:
2q:
3q:
4q:
DROP TABLE rs_sub, rs_other;
