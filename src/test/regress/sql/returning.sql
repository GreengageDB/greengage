--
-- Test INSERT/UPDATE/DELETE RETURNING
--

-- Simple cases

CREATE TEMP TABLE foo (f1 serial, f2 text, f3 int default 42);

-- GPDB: otherwise the UPDATE tests fail with:
-- ERROR:  Cannot parallelize an UPDATE statement that updates the distribution columns
ALTER TABLE foo SET DISTRIBUTED RANDOMLY;

INSERT INTO foo (f2,f3)
  VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9)
  RETURNING *, f1+f3 AS sum;

SELECT * FROM foo;

UPDATE foo SET f2 = lower(f2), f3 = DEFAULT RETURNING foo.*, f1+f3 AS sum13;

SELECT * FROM foo;

DELETE FROM foo WHERE f1 > 2 RETURNING f3, f2, f1, least(f1,f3);

SELECT * FROM foo;

-- Subplans and initplans in the RETURNING list

INSERT INTO foo SELECT f1+10, f2, f3+99 FROM foo
  RETURNING *, f1+112 IN (SELECT q1 FROM int8_tbl) AS subplan,
    EXISTS(SELECT * FROM int4_tbl) AS initplan;

UPDATE foo SET f3 = f3 * 2
  WHERE f1 > 10
  RETURNING *, f1+112 IN (SELECT q1 FROM int8_tbl) AS subplan,
    EXISTS(SELECT * FROM int4_tbl) AS initplan;

DELETE FROM foo
  WHERE f1 > 10
  RETURNING *, f1+112 IN (SELECT q1 FROM int8_tbl) AS subplan,
    EXISTS(SELECT * FROM int4_tbl) AS initplan;

-- Joins

UPDATE foo SET f3 = f3*2
  FROM int4_tbl i
  WHERE foo.f1 + 123455 = i.f1
  RETURNING foo.*, i.f1 as "i.f1";

SELECT * FROM foo;

DELETE FROM foo
  USING int4_tbl i
  WHERE foo.f1 + 123455 = i.f1
  RETURNING foo.*, i.f1 as "i.f1";

SELECT * FROM foo;

-- Check inheritance cases

CREATE TEMP TABLE foochild (fc int) INHERITS (foo);

INSERT INTO foochild VALUES(123,'child',999,-123);

ALTER TABLE foo ADD COLUMN f4 int8 DEFAULT 99;

SELECT * FROM foo;
SELECT * FROM foochild;

UPDATE foo SET f4 = f4 + f3 WHERE f4 = 99 RETURNING *;

SELECT * FROM foo;
SELECT * FROM foochild;

UPDATE foo SET f3 = f3*2
  FROM int8_tbl i
  WHERE foo.f1 = i.q2
  RETURNING *;

SELECT * FROM foo;
SELECT * FROM foochild;

DELETE FROM foo
  USING int8_tbl i
  WHERE foo.f1 = i.q2
  RETURNING *;

SELECT * FROM foo;
SELECT * FROM foochild;

DROP TABLE foochild;

-- Rules and views

CREATE TEMP VIEW voo AS SELECT f1, f2 FROM foo;

CREATE RULE voo_i AS ON INSERT TO voo DO INSTEAD
  INSERT INTO foo VALUES(new.*, 57);

INSERT INTO voo VALUES(11,'zit');
-- fails:
INSERT INTO voo VALUES(12,'zoo') RETURNING *, f1*2;

-- fails, incompatible list:
CREATE OR REPLACE RULE voo_i AS ON INSERT TO voo DO INSTEAD
  INSERT INTO foo VALUES(new.*, 57) RETURNING *;

CREATE OR REPLACE RULE voo_i AS ON INSERT TO voo DO INSTEAD
  INSERT INTO foo VALUES(new.*, 57) RETURNING f1, f2;

-- should still work
INSERT INTO voo VALUES(13,'zit2');
-- works now
INSERT INTO voo VALUES(14,'zoo2') RETURNING *;

SELECT * FROM foo;
SELECT * FROM voo;

CREATE OR REPLACE RULE voo_u AS ON UPDATE TO voo DO INSTEAD
  UPDATE foo SET f1 = new.f1, f2 = new.f2 WHERE f1 = old.f1
  RETURNING f1, f2;

update voo set f1 = f1 + 1 where f2 = 'zoo2';
update voo set f1 = f1 + 1 where f2 = 'zoo2' RETURNING *, f1*2;

SELECT * FROM foo;
SELECT * FROM voo;

CREATE OR REPLACE RULE voo_d AS ON DELETE TO voo DO INSTEAD
  DELETE FROM foo WHERE f1 = old.f1
  RETURNING f1, f2;

DELETE FROM foo WHERE f1 = 13;
DELETE FROM foo WHERE f2 = 'zit' RETURNING *;

SELECT * FROM foo;
SELECT * FROM voo;

-- Check use of a whole-row variable for an un-flattenable view
CREATE TEMP VIEW foo_v AS SELECT * FROM foo OFFSET 0;
UPDATE foo SET f2 = foo_v.f2 FROM foo_v WHERE foo_v.f1 = foo.f1
  RETURNING foo_v;
SELECT * FROM foo;

-- Check use of a whole-row variable for an inlined set-returning function
CREATE FUNCTION foo_f() RETURNS SETOF foo AS
  $$ SELECT * FROM foo OFFSET 0 $$ LANGUAGE sql STABLE;
UPDATE foo SET f2 = foo_f.f2 FROM foo_f() WHERE foo_f.f1 = foo.f1
  RETURNING foo_f;
SELECT * FROM foo;
DROP FUNCTION foo_f();

-- As above, but SRF is defined to return a composite type
CREATE TYPE foo_t AS (f1 int, f2 text, f3 int, f4 int8);
CREATE FUNCTION foo_f() RETURNS SETOF foo_t AS
  $$ SELECT * FROM foo OFFSET 0 $$ LANGUAGE sql STABLE;
UPDATE foo SET f2 = foo_f.f2 FROM foo_f() WHERE foo_f.f1 = foo.f1
  RETURNING foo_f;
SELECT * FROM foo;
DROP FUNCTION foo_f();
DROP TYPE foo_t;

-- Try a join case

CREATE TEMP TABLE joinme (f2j text, other int);
INSERT INTO joinme VALUES('more', 12345);
INSERT INTO joinme VALUES('zoo2', 54321);
INSERT INTO joinme VALUES('other', 0);

CREATE TEMP VIEW joinview AS
  SELECT foo.*, other FROM foo JOIN joinme ON (f2 = f2j);

SELECT * FROM joinview;

CREATE RULE joinview_u AS ON UPDATE TO joinview DO INSTEAD
  UPDATE foo SET f1 = new.f1, f3 = new.f3
    FROM joinme WHERE f2 = f2j AND f2 = old.f2
    RETURNING foo.*, other;

UPDATE joinview SET f1 = f1 + 1 WHERE f3 = 57 RETURNING *, other + 1;

SELECT * FROM joinview;
SELECT * FROM foo;
SELECT * FROM voo;

-- Check aliased target relation
INSERT INTO foo AS bar DEFAULT VALUES RETURNING *; -- ok
INSERT INTO foo AS bar DEFAULT VALUES RETURNING foo.*; -- fails, wrong name
INSERT INTO foo AS bar DEFAULT VALUES RETURNING bar.*; -- ok
INSERT INTO foo AS bar DEFAULT VALUES RETURNING bar.f3; -- ok

--
-- Test RETURNING OLD/NEW.
--
-- Start with new data, to ensure predictable TIDs.
--
TRUNCATE foo;
INSERT INTO foo VALUES (1, 'xxx', 10, 20), (2, 'more', 42, 141), (3, 'zoo2', 57, 99);

-- Error cases
INSERT INTO foo DEFAULT VALUES RETURNING WITH (nonsuch AS something) *;
INSERT INTO foo DEFAULT VALUES RETURNING WITH (new AS foo) *;
INSERT INTO foo DEFAULT VALUES RETURNING WITH (old AS o, new AS n, old AS o) *;
INSERT INTO foo DEFAULT VALUES RETURNING WITH (old AS o, new AS n, new AS n) *;
INSERT INTO foo DEFAULT VALUES RETURNING WITH (old AS x, new AS x) *;

-- INSERT has NEW, but not OLD
EXPLAIN (verbose, costs off)
INSERT INTO foo VALUES (4)
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;
INSERT INTO foo VALUES (4)
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

-- GPDB: foo is DISTRIBUTED RANDOMLY, so it cannot carry a UNIQUE index on f1,
-- and therefore ON CONFLICT (f1) has no arbiter index.  Just insert the row the
-- following UPDATE/DELETE OLD/NEW tests expect.
INSERT INTO foo VALUES (5, 'ok');

-- UPDATE has OLD and NEW
EXPLAIN (verbose, costs off)
UPDATE foo SET f4 = 100 WHERE f1 = 5
  RETURNING old.tableoid::regclass, old.ctid, old.*, old,
            new.tableoid::regclass, new.ctid, new.*, new,
            old.f4::text||'->'||new.f4::text AS change;
UPDATE foo SET f4 = 100 WHERE f1 = 5
  RETURNING old.tableoid::regclass, old.ctid, old.*, old,
            new.tableoid::regclass, new.ctid, new.*, new,
            old.f4::text||'->'||new.f4::text AS change;

-- DELETE has OLD, but not NEW
EXPLAIN (verbose, costs off)
DELETE FROM foo WHERE f1 = 5
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;
DELETE FROM foo WHERE f1 = 5
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

-- RETURNING OLD and NEW from subquery
EXPLAIN (verbose, costs off)
INSERT INTO foo VALUES (5, 'subquery test')
  RETURNING (SELECT max(old.f4 + x) FROM generate_series(1, 10) x) old_max,
            (SELECT max(new.f4 + x) FROM generate_series(1, 10) x) new_max;
INSERT INTO foo VALUES (5, 'subquery test')
  RETURNING (SELECT max(old.f4 + x) FROM generate_series(1, 10) x) old_max,
            (SELECT max(new.f4 + x) FROM generate_series(1, 10) x) new_max;

EXPLAIN (verbose, costs off)
UPDATE foo SET f4 = 100 WHERE f1 = 5
  RETURNING (SELECT old.f4 = new.f4),
            (SELECT max(old.f4 + x) FROM generate_series(1, 10) x) old_max,
            (SELECT max(new.f4 + x) FROM generate_series(1, 10) x) new_max;
UPDATE foo SET f4 = 100 WHERE f1 = 5
  RETURNING (SELECT old.f4 = new.f4),
            (SELECT max(old.f4 + x) FROM generate_series(1, 10) x) old_max,
            (SELECT max(new.f4 + x) FROM generate_series(1, 10) x) new_max;

EXPLAIN (verbose, costs off)
DELETE FROM foo WHERE f1 = 5
  RETURNING (SELECT max(old.f4 + x) FROM generate_series(1, 10) x) old_max,
            (SELECT max(new.f4 + x) FROM generate_series(1, 10) x) new_max;
DELETE FROM foo WHERE f1 = 5
  RETURNING (SELECT max(old.f4 + x) FROM generate_series(1, 10) x) old_max,
            (SELECT max(new.f4 + x) FROM generate_series(1, 10) x) new_max;

-- DELETE turned into UPDATE by a rule has OLD and NEW
CREATE RULE foo_del_rule AS ON DELETE TO foo DO INSTEAD
  UPDATE foo SET f2 = f2||' (deleted)', f3 = -1, f4 = -1 WHERE f1 = OLD.f1
  RETURNING *;
EXPLAIN (verbose, costs off)
DELETE FROM foo WHERE f1 = 4 RETURNING old.*,new.*, *;
DELETE FROM foo WHERE f1 = 4 RETURNING old.*,new.*, *;

-- UPDATE on view with rule
EXPLAIN (verbose, costs off)
UPDATE joinview SET f3 = f3 + 1 WHERE f3 = 57
  RETURNING old.*, new.*, *, new.f3 - old.f3 AS delta_f3;
UPDATE joinview SET f3 = f3 + 1 WHERE f3 = 57
  RETURNING old.*, new.*, *, new.f3 - old.f3 AS delta_f3;

-- GPDB: INSTEAD OF triggers are not supported in Greengage, so the upstream
-- "UPDATE on view with INSTEAD OF trigger" sub-block (CREATE TRIGGER and the
-- two joinview UPDATEs that rely on it) is dropped here.

-- Test wholerow & dropped column handling
ALTER TABLE foo DROP COLUMN f3 CASCADE;
UPDATE foo SET f4 = f4 + 1 RETURNING old.f3;  -- should fail
UPDATE foo SET f4 = f4 + 1 RETURNING old, new;

-- INSERT/DELETE on zero column table
CREATE TABLE zerocol();
INSERT INTO zerocol SELECT RETURNING old.*, new.*, *;
INSERT INTO zerocol SELECT
  RETURNING old.tableoid::regclass, old.ctid,
            new.tableoid::regclass, new.ctid, ctid, *;
DELETE FROM zerocol
  RETURNING old.tableoid::regclass, old.ctid,
            new.tableoid::regclass, new.ctid, ctid, *;
DROP TABLE zerocol;

-- Test schema-qualified table name in RETURNING list
CREATE TABLE public.tt(a int, b int);
INSERT INTO public.tt VALUES (1, 10);
UPDATE public.tt SET b = b * 2 RETURNING a, b, old.b, new.b, tt.b, public.tt.b;
DROP TABLE public.tt;

-- Test cross-partition updates and attribute mapping
-- GPDB: foo_parted is DISTRIBUTED RANDOMLY so the cross-partition UPDATEs below
-- (which change the partition/attribute a) do not update a distribution column;
-- the standalone children keep their upstream (differing) column order but must
-- share the same DISTRIBUTED RANDOMLY policy for ATTACH PARTITION to succeed.
CREATE TABLE foo_parted (a int, b float8, c text) PARTITION BY LIST (a) DISTRIBUTED RANDOMLY;
CREATE TABLE foo_part_s1 PARTITION OF foo_parted FOR VALUES IN (1);
CREATE TABLE foo_part_s2 PARTITION OF foo_parted FOR VALUES IN (2);
CREATE TABLE foo_part_d1 (c text, a int, b float8) DISTRIBUTED RANDOMLY;
ALTER TABLE foo_parted ATTACH PARTITION foo_part_d1 FOR VALUES IN (3);
CREATE TABLE foo_part_d2 (b float8, c text, a int) DISTRIBUTED RANDOMLY;
ALTER TABLE foo_parted ATTACH PARTITION foo_part_d2 FOR VALUES IN (4);

INSERT INTO foo_parted
  VALUES (1, 17.1, 'P1'), (2, 17.2, 'P2'), (3, 17.3, 'P3'), (4, 17.4, 'P4')
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

UPDATE foo_parted SET a = 2, b = b + 1, c = c || '->P2' WHERE a = 1
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

UPDATE foo_parted SET a = 1, b = b + 1, c = c || '->P1' WHERE a = 3
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

UPDATE foo_parted SET a = 3, b = b + 1, c = c || '->P3' WHERE a = 1
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

UPDATE foo_parted SET a = 4, b = b + 1, c = c || '->P4' WHERE a = 3
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

-- cross-partition update that uses ReturningExpr nodes, without returning
-- old/new table values
CREATE VIEW foo_parted_v AS SELECT *, 'xxx' AS dummy FROM foo_parted;
UPDATE foo_parted_v SET a = 1, c = c || '->P1' WHERE a = 2 AND c = 'P2'
  RETURNING 'P2:'||old.dummy, 'P1:'||new.dummy;

DELETE FROM foo_parted
  RETURNING old.tableoid::regclass, old.ctid, old.*,
            new.tableoid::regclass, new.ctid, new.*, *;

DROP TABLE foo_parted CASCADE;

-- GPDB: the upstream "Test deparsing" case uses multiple writable CTEs in a
-- single query, which Greengage does not support (only one modifying WITH clause
-- is allowed per query), so that deparse test is dropped here.
