-- start_ignore
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
DROP TABLE IF EXISTS t;
-- end_ignore
CREATE TABLE t(a int, b text) DISTRIBUTED BY (a);

-- Known issue: query is not added to pg_stat_statements statistics in
-- case it is planned by GPORCA. So disable GPORCA during tests.
SET optimizer=off;

SELECT pg_stat_statements_reset();

SELECT GROUPING (a) FROM t GROUP BY ROLLUP(a, b);
-- launch not equivalent query
SELECT GROUPING (b) FROM t GROUP BY ROLLUP(a, b);
-- check group_id() in a query
SELECT group_id() FROM t GROUP BY ROLLUP(a, b);

-- check that queries have separate entries
SELECT query, calls FROM pg_stat_statements ORDER BY query;

SELECT pg_stat_statements_reset();

-- check that different grouping options result in separate entries
SELECT COUNT (*) FROM t GROUP BY ROLLUP(a, b);
SELECT COUNT (*) FROM t GROUP BY CUBE(a, b);
SELECT COUNT (*) FROM t GROUP BY GROUPING SETS(a, b);
SELECT COUNT (*) FROM t GROUP BY GROUPING SETS((a), (a, b));
SELECT COUNT (*) FROM t GROUP BY a, b;

SELECT query, calls FROM pg_stat_statements ORDER BY query;

-- check several parameters options in ROLLUP
-- all should result in separate entries
SELECT pg_stat_statements_reset();

SELECT COUNT (*) FROM t GROUP BY ROLLUP(a, b);
SELECT COUNT (*) FROM t GROUP BY ROLLUP(b);

SELECT query, calls FROM pg_stat_statements ORDER BY query;

--- check anytable parameter for a function
SELECT pg_stat_statements_reset();

-- call of anytable_out will cause an error,
-- thus prevent actual call by adding FALSE condition
SELECT * FROM anytable_out(TABLE(SELECT * FROM t)) WHERE 1 = 0;
SELECT * FROM anytable_out(TABLE(SELECT * FROM t WHERE a=0)) WHERE 1 = 0;

SELECT query, calls FROM pg_stat_statements ORDER BY query;

DROP TABLE t;

RESET optimizer;
