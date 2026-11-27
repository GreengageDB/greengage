-- The hooks of EXPLAIN for extensions: an option, its validation, a line
-- on each plan node and after the plan, and the data that each qExec
-- sends to the qDisp. The plans are those of the Postgres-based planner.
SET optimizer = off;
CREATE TABLE teh (a int, b int) DISTRIBUTED BY (a);
INSERT INTO teh SELECT i, i % 10 FROM generate_series(1, 100) i;
ANALYZE teh;

-- An option that no extension registered is still refused.
EXPLAIN (COSTS OFF, NO_SUCH_OPTION) SELECT 1;

-- The module refuses its option without ANALYZE in the validation hook.
EXPLAIN (COSTS OFF, TEST_HOOKS) SELECT 1;

-- Without the option the hooks print nothing.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT count(*) FROM teh;

-- One slice on the segments: each of its three qExecs reports, and sends
-- an empty piece under another name besides.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS)
SELECT count(*) FROM teh;

-- A statement that writes runs its root slice on the segments too: the
-- qExecs of both slices report.
CREATE TABLE teh2 (a int, b int) DISTRIBUTED BY (b);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS)
INSERT INTO teh2 SELECT * FROM teh;

-- An initplan is dispatched on its own, and its qExecs report too.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS)
SELECT count(*) FROM teh WHERE a > (SELECT avg(a) FROM teh);

-- A slice that reads a catalog runs on the coordinator: its qExec
-- reports as seg-1.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS)
INSERT INTO teh2 SELECT 0, 0 FROM pg_am LIMIT 1;

-- A rule gives one EXPLAIN two plans: each shows the reports of its own
-- statement only.
CREATE TABLE teh3 (a int, b int) DISTRIBUTED BY (a);
CREATE RULE teh_also AS ON INSERT TO teh3
  DO ALSO INSERT INTO teh2 VALUES (new.a, new.b);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS)
INSERT INTO teh3 SELECT * FROM teh;
DROP TABLE teh3;

-- The option set to false prints nothing either.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS false)
SELECT count(*) FROM teh;

-- The lines in another format.
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS,
         FORMAT YAML)
SELECT count(*) FROM teh;

-- A qExec that sends no piece still has its message passed on.
SET test_explain_hooks.send = off;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS)
SELECT count(*) FROM teh;
RESET test_explain_hooks.send;

-- A plain EXPLAIN that a function runs on the coordinator, after the
-- initplan reported, keeps the reports of the explained statement.
CREATE FUNCTION teh_nested() RETURNS int
LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE 'EXPLAIN SELECT 1';
  RETURN 1;
END
$$;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, TEST_HOOKS)
SELECT count(*), teh_nested() FROM teh WHERE a > (SELECT avg(a) FROM teh);
DROP FUNCTION teh_nested();

-- The qDisp refuses a piece of data that does not lie within the
-- message, whether or not the EXPLAIN asks for TEST_HOOKS: one too short
-- for the length of its name, one whose name is empty, one whose name
-- is longer than the message, one whose name has no zero, one without
-- the length of its data, one whose length is below zero, and one whose
-- data is missing.
SET test_explain_hooks.bad_piece = 'short';
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM teh;
SET test_explain_hooks.bad_piece = 'empty_name';
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM teh;
SET test_explain_hooks.bad_piece = 'long_name';
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM teh;
SET test_explain_hooks.bad_piece = 'unterminated';
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM teh;
SET test_explain_hooks.bad_piece = 'no_length';
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM teh;
SET test_explain_hooks.bad_piece = 'negative';
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM teh;
SET test_explain_hooks.bad_piece = 'truncated';
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM teh;
RESET test_explain_hooks.bad_piece;

DROP TABLE teh, teh2;
