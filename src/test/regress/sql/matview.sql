-- create a table to use as a basis for views and materialized views in various combinations
CREATE TABLE t (id int NOT NULL PRIMARY KEY, type text NOT NULL, amt numeric NOT NULL);
INSERT INTO t VALUES
  (1, 'x', 2),
  (2, 'x', 3),
  (3, 'y', 5),
  (4, 'y', 7),
  (5, 'z', 11);

-- we want a view based on the table, too, since views present additional challenges
CREATE VIEW tv AS SELECT type, sum(amt) AS totamt FROM t GROUP BY type;
SELECT * FROM tv ORDER BY type;

-- create a materialized view with no data, and confirm correct behavior
EXPLAIN (costs off)
  CREATE MATERIALIZED VIEW tm AS SELECT type, sum(amt) AS totamt FROM t GROUP BY type WITH NO DATA distributed by(type);
CREATE MATERIALIZED VIEW tm AS SELECT type, sum(amt) AS totamt FROM t GROUP BY type WITH NO DATA distributed by(type);
SELECT relispopulated FROM pg_class WHERE oid = 'tm'::regclass;
SELECT * FROM tm;
REFRESH MATERIALIZED VIEW tm;
SELECT relispopulated FROM pg_class WHERE oid = 'tm'::regclass;
CREATE UNIQUE INDEX tm_type ON tm (type);
SELECT * FROM tm;

-- create various views
EXPLAIN (costs off)
  CREATE MATERIALIZED VIEW tvm AS SELECT * FROM tv ORDER BY type;
CREATE MATERIALIZED VIEW tvm AS SELECT * FROM tv ORDER BY type;
SELECT * FROM tvm;
CREATE MATERIALIZED VIEW tmm AS SELECT sum(totamt) AS grandtot FROM tm;
CREATE MATERIALIZED VIEW tvmm AS SELECT sum(totamt) AS grandtot FROM tvm distributed by(grandtot);
CREATE UNIQUE INDEX tvmm_expr ON tvmm (grandtot);
CREATE VIEW tvv AS SELECT sum(totamt) AS grandtot FROM tv;
EXPLAIN (costs off)
  CREATE MATERIALIZED VIEW tvvm AS SELECT * FROM tvv;
CREATE MATERIALIZED VIEW tvvm AS SELECT * FROM tvv;
CREATE VIEW tvvmv AS SELECT * FROM tvvm;
CREATE MATERIALIZED VIEW bb AS SELECT * FROM tvvmv;
CREATE INDEX aa ON bb (grandtot);

-- check that plans seem reasonable
\d+ tvm
\d+ tvm
\d+ tvvm
\d+ bb

-- test schema behavior
CREATE SCHEMA mvschema;
ALTER MATERIALIZED VIEW tvm SET SCHEMA mvschema;
\d+ tvm
\d+ tvmm
SET search_path = mvschema, public;
\d+ tvm

-- modify the underlying table data
INSERT INTO t VALUES (6, 'z', 13);

-- confirm pre- and post-refresh contents of fairly simple materialized views
SELECT * FROM tm ORDER BY type;
SELECT * FROM tvm ORDER BY type;
REFRESH MATERIALIZED VIEW CONCURRENTLY tm;
REFRESH MATERIALIZED VIEW tvm;
SELECT * FROM tm ORDER BY type;
SELECT * FROM tvm ORDER BY type;
RESET search_path;

-- confirm pre- and post-refresh contents of nested materialized views
EXPLAIN (costs off)
  SELECT * FROM tmm;
EXPLAIN (costs off)
  SELECT * FROM tvmm;
EXPLAIN (costs off)
  SELECT * FROM tvvm;
SELECT * FROM tmm;
SELECT * FROM tvmm;
SELECT * FROM tvvm;
REFRESH MATERIALIZED VIEW tmm;
REFRESH MATERIALIZED VIEW CONCURRENTLY tvmm;
REFRESH MATERIALIZED VIEW tvmm;
REFRESH MATERIALIZED VIEW tvvm;
EXPLAIN (costs off)
  SELECT * FROM tmm;
EXPLAIN (costs off)
  SELECT * FROM tvmm;
EXPLAIN (costs off)
  SELECT * FROM tvvm;
SELECT * FROM tmm;
SELECT * FROM tvmm;
SELECT * FROM tvvm;

-- test diemv when the mv does not exist
DROP MATERIALIZED VIEW IF EXISTS no_such_mv;

-- make sure invalid combination of options is prohibited
REFRESH MATERIALIZED VIEW CONCURRENTLY tvmm WITH NO DATA;

-- no tuple locks on materialized views
-- start_ignore
SELECT * FROM tvvm FOR SHARE;
-- end_ignore

-- test join of mv and view
SELECT type, m.totamt AS mtot, v.totamt AS vtot FROM tm m LEFT JOIN tv v USING (type) ORDER BY type;

-- make sure that dependencies are reported properly when they block the drop
DROP TABLE t;

-- make sure dependencies are dropped and reported
-- and make sure that transactional behavior is correct on rollback
-- incidentally leaving some interesting materialized views for pg_dump testing
BEGIN;
DROP TABLE t CASCADE;
ROLLBACK;

-- some additional tests not using base tables
CREATE VIEW v_test1 AS SELECT 1 moo;
CREATE VIEW v_test2 AS SELECT moo, 2*moo FROM v_test1 UNION ALL SELECT moo, 3*moo FROM v_test1;
\d+ v_test2
CREATE MATERIALIZED VIEW mv_test2 AS SELECT moo, 2*moo FROM v_test2 UNION ALL SELECT moo, 3*moo FROM v_test2;
\d+ mv_test2
CREATE MATERIALIZED VIEW mv_test3 AS SELECT * FROM mv_test2 WHERE moo = 12345;
SELECT relispopulated FROM pg_class WHERE oid = 'mv_test3'::regclass;

DROP VIEW v_test1 CASCADE;

-- test that duplicate values on unique index prevent refresh
CREATE TABLE foo(a, b) AS VALUES(1, 10);
CREATE MATERIALIZED VIEW mv AS SELECT * FROM foo distributed by(a);
CREATE UNIQUE INDEX ON mv(a);
INSERT INTO foo SELECT * FROM foo;
REFRESH MATERIALIZED VIEW mv;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv;
DROP TABLE foo CASCADE;

-- make sure that all columns covered by unique indexes works
CREATE TABLE foo(a, b, c) AS VALUES(1, 2, 3);
CREATE MATERIALIZED VIEW mv AS SELECT * FROM foo distributed by(a);
CREATE UNIQUE INDEX ON mv (a);
INSERT INTO foo VALUES(2, 3, 4);
INSERT INTO foo VALUES(3, 4, 5);
REFRESH MATERIALIZED VIEW mv;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv;
DROP TABLE foo CASCADE;

-- allow subquery to reference unpopulated matview if WITH NO DATA is specified
CREATE MATERIALIZED VIEW mv1 AS SELECT 1 AS col1 WITH NO DATA;
CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM mv1
  WHERE col1 = (SELECT LEAST(col1) FROM mv1) WITH NO DATA;
DROP MATERIALIZED VIEW mv1 CASCADE;

-- make sure that types with unusual equality tests work
CREATE TABLE boxes (id serial primary key, b box);
INSERT INTO boxes (b) VALUES
  ('(32,32),(31,31)'),
  ('(2.0000004,2.0000004),(1,1)'),
  ('(1.9999996,1.9999996),(1,1)');
CREATE MATERIALIZED VIEW boxmv AS SELECT * FROM boxes distributed by(id);
CREATE UNIQUE INDEX boxmv_id ON boxmv (id);
UPDATE boxes SET b = '(2,2),(1,1)' WHERE id = 2;
REFRESH MATERIALIZED VIEW CONCURRENTLY boxmv;
SELECT * FROM boxmv ORDER BY id;
DROP TABLE boxes CASCADE;

-- make sure that column names are handled correctly
CREATE TABLE mvtest_v (i int, j int);
CREATE MATERIALIZED VIEW mvtest_mv_v (ii, jj, kk) AS SELECT i, j FROM mvtest_v; -- error
CREATE MATERIALIZED VIEW mvtest_mv_v (ii, jj) AS SELECT i, j FROM mvtest_v distributed by(ii); -- ok
CREATE MATERIALIZED VIEW mvtest_mv_v_2 (ii) AS SELECT i, j FROM mvtest_v; -- ok
CREATE MATERIALIZED VIEW mvtest_mv_v_3 (ii, jj, kk) AS SELECT i, j FROM mvtest_v WITH NO DATA; -- error
CREATE MATERIALIZED VIEW mvtest_mv_v_3 (ii, jj) AS SELECT i, j FROM mvtest_v WITH NO DATA; -- ok
CREATE MATERIALIZED VIEW mvtest_mv_v_4 (ii) AS SELECT i, j FROM mvtest_v WITH NO DATA; -- ok
ALTER TABLE mvtest_v RENAME COLUMN i TO x;
INSERT INTO mvtest_v values (1, 2);
CREATE UNIQUE INDEX mvtest_mv_v_ii ON mvtest_mv_v (ii);
REFRESH MATERIALIZED VIEW mvtest_mv_v;
UPDATE mvtest_v SET j = 3 WHERE x = 1;
REFRESH MATERIALIZED VIEW CONCURRENTLY mvtest_mv_v;
REFRESH MATERIALIZED VIEW mvtest_mv_v_2;
REFRESH MATERIALIZED VIEW mvtest_mv_v_3;
REFRESH MATERIALIZED VIEW mvtest_mv_v_4;
SELECT * FROM mvtest_v;
SELECT * FROM mvtest_mv_v;
SELECT * FROM mvtest_mv_v_2;
SELECT * FROM mvtest_mv_v_3;
SELECT * FROM mvtest_mv_v_4;
DROP TABLE mvtest_v CASCADE;

-- Check that unknown literals are converted to "text" in CREATE MATVIEW,
-- so that we don't end up with unknown-type columns.
CREATE MATERIALIZED VIEW mv_unspecified_types AS
  SELECT 42 as i, 42.5 as num, 'foo' as u, 'foo'::unknown as u2, null as n;
\d+ mv_unspecified_types
SELECT * FROM mv_unspecified_types;

-- make sure that create WITH NO DATA does not plan the query (bug #13907)
create materialized view mvtest_error as select 1/0 as x;  -- fail
create materialized view mvtest_error as select 1/0 as x with no data;

-- make sure that matview rows can be referenced as source rows (bug #9398)
CREATE TABLE v AS SELECT generate_series(1,10) AS a;
CREATE MATERIALIZED VIEW mv_v AS SELECT a FROM v WHERE a <= 5;
DELETE FROM v WHERE EXISTS ( SELECT * FROM mv_v WHERE mv_v.a = v.a );
SELECT * FROM v;
SELECT * FROM mv_v;
DROP TABLE v CASCADE;

-- make sure running as superuser works when MV owned by another role (bug #11208)
CREATE ROLE user_dw;
SET ROLE user_dw;
CREATE TABLE foo_data AS SELECT i, md5(random()::text)
  FROM generate_series(1, 10) i;
CREATE MATERIALIZED VIEW mv_foo AS SELECT * FROM foo_data distributed by(i);
CREATE UNIQUE INDEX ON mv_foo (i);
RESET ROLE;
REFRESH MATERIALIZED VIEW mv_foo;
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_foo;
DROP OWNED BY user_dw CASCADE;
DROP ROLE user_dw;

-- make sure that create WITH NO DATA works via SPI
BEGIN;
CREATE FUNCTION mvtest_func()
  RETURNS void AS $$
BEGIN
  CREATE MATERIALIZED VIEW mvtest1 AS SELECT 1 AS x;
  CREATE MATERIALIZED VIEW mvtest2 AS SELECT 1 AS x WITH NO DATA;
END;
$$ LANGUAGE plpgsql;
SELECT mvtest_func();
SELECT * FROM mvtest1;
SELECT * FROM mvtest2;
ROLLBACK;

-- make sure refresh mat view will dispatch oid at the final
-- execution of the mat view's body query. See Github Issue
-- https://github.com/GreengageDB/greengage/issues/11956 for details.

create table t_github_issue_11956(a int, b int) distributed randomly;
insert into t_github_issue_11956 values (1, 1);

create function f_github_issue_11956() returns int as
$$
select sum(a+b)::int from t_github_issue_11956
$$
language sql stable;

create materialized view mat_view_github_issue_11956
as
select * from t_github_issue_11956 where a > f_github_issue_11956()
distributed randomly;

refresh materialized view mat_view_github_issue_11956;

drop materialized view mat_view_github_issue_11956;
drop table t_github_issue_11956;

-- test REFRESH MATERIALIZED VIEW on AO table with index
-- more details could be found at https://github.com/GreengageDB/greengage/issues/16447
CREATE TABLE base_table (idn character varying(10) NOT NULL);
INSERT INTO base_table select i from generate_series(1, 5000) i;
CREATE MATERIALIZED VIEW base_view WITH (APPENDONLY=true) AS SELECT tt1.idn AS idn_ban FROM base_table tt1;
CREATE INDEX test_id1 on base_view using btree(idn_ban);
REFRESH MATERIALIZED VIEW base_view ;
SELECT * FROM base_view where idn_ban = '10';

DROP MATERIALIZED VIEW base_view;
DROP TABLE base_table;
