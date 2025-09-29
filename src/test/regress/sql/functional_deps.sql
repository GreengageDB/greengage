-- from http://www.depesz.com/index.php/2010/04/19/getting-unique-elements/

-- Enable logs that allow to show GPORCA failed to produce a plan
SET optimizer_trace_fallback = on;

CREATE TEMP TABLE articles (
    id int CONSTRAINT articles_pkey PRIMARY KEY,
    keywords text,
    -- GPDB doesn't support having a PRIMARY KEY and UNIQUE constraints on the
    -- same table. Hence leave out the UNIQUE constraints.
    title text /* UNIQUE */ NOT NULL,
    body text /* UNIQUE */,
    created date
);

CREATE TEMP TABLE articles_in_category (
    article_id int,
    category_id int,
    changed date,
    PRIMARY KEY (article_id, category_id)
);

-- test functional dependencies based on primary keys/unique constraints

-- base tables

-- group by primary key (OK)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id;

-- group by unique not null (fail/todo)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY title;

-- group by unique nullable (fail)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY body;

-- group by something else (fail)
SELECT id, keywords, title, body, created
FROM articles
GROUP BY keywords;

-- multiple tables

-- group by primary key (OK)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a, articles_in_category AS aic
WHERE a.id = aic.article_id AND aic.category_id in (14,62,70,53,138)
GROUP BY a.id;

-- group by something else (fail)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a, articles_in_category AS aic
WHERE a.id = aic.article_id AND aic.category_id in (14,62,70,53,138)
GROUP BY aic.article_id, aic.category_id;

-- JOIN syntax

-- group by left table's primary key (OK)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY a.id;

-- group by something else (fail)
SELECT a.id, a.keywords, a.title, a.body, a.created
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY aic.article_id, aic.category_id;

-- group by right table's (composite) primary key (OK)
SELECT aic.changed
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY aic.category_id, aic.article_id;

-- group by right table's partial primary key (fail)
SELECT aic.changed
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY aic.article_id;


-- example from documentation

CREATE TEMP TABLE products (product_id int, name text, price numeric);
CREATE TEMP TABLE sales (product_id int, units int);

-- OK
SELECT product_id, p.name, (sum(s.units) * p.price) AS sales
    FROM products p LEFT JOIN sales s USING (product_id)
    GROUP BY product_id, p.name, p.price;

-- fail
SELECT product_id, p.name, (sum(s.units) * p.price) AS sales
    FROM products p LEFT JOIN sales s USING (product_id)
    GROUP BY product_id;

ALTER TABLE products ADD PRIMARY KEY (product_id);

-- OK now
SELECT product_id, p.name, (sum(s.units) * p.price) AS sales
    FROM products p LEFT JOIN sales s USING (product_id)
    GROUP BY product_id;


-- Drupal example, http://drupal.org/node/555530

CREATE TEMP TABLE node (
    nid SERIAL,
    vid integer NOT NULL default '0',
    type varchar(32) NOT NULL default '',
    title varchar(128) NOT NULL default '',
    uid integer NOT NULL default '0',
    status integer NOT NULL default '1',
    created integer NOT NULL default '0',
    -- snip
    PRIMARY KEY (nid, vid)
);

CREATE TEMP TABLE users (
    uid integer NOT NULL default '0',
    name varchar(60) NOT NULL default '',
    pass varchar(32) NOT NULL default '',
    -- snip
    PRIMARY KEY (uid)
    /* , UNIQUE (name) */
);

-- OK
SELECT u.uid, u.name FROM node n
INNER JOIN users u ON u.uid = n.uid
WHERE n.type = 'blog' AND n.status = 1
GROUP BY u.uid, u.name;

-- OK
SELECT u.uid, u.name FROM node n
INNER JOIN users u ON u.uid = n.uid
WHERE n.type = 'blog' AND n.status = 1
GROUP BY u.uid;


-- Check views and dependencies

-- fail
CREATE TEMP VIEW fdv1 AS
SELECT id, keywords, title, body, created
FROM articles
GROUP BY body;

-- OK
CREATE TEMP VIEW fdv1 AS
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id;

-- fail
ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT;

DROP VIEW fdv1;


-- multiple dependencies
CREATE TEMP VIEW fdv2 AS
SELECT a.id, a.keywords, a.title, aic.category_id, aic.changed
FROM articles AS a JOIN articles_in_category AS aic ON a.id = aic.article_id
WHERE aic.category_id in (14,62,70,53,138)
GROUP BY a.id, aic.category_id, aic.article_id;

ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT; -- fail
ALTER TABLE articles_in_category DROP CONSTRAINT articles_in_category_pkey RESTRICT; --fail

DROP VIEW fdv2;


-- nested queries

CREATE TEMP VIEW fdv3 AS
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id
UNION
SELECT id, keywords, title, body, created
FROM articles
GROUP BY id;

ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT; -- fail

DROP VIEW fdv3;


CREATE TEMP VIEW fdv4 AS
SELECT * FROM articles WHERE title IN (SELECT title FROM articles GROUP BY id);

ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT; -- fail

DROP VIEW fdv4;


-- prepared query plans: this results in failure on reuse

PREPARE foo AS
  SELECT id, keywords, title, body, created
  FROM articles
  GROUP BY id;

EXECUTE foo;

ALTER TABLE articles DROP CONSTRAINT articles_pkey RESTRICT;

EXECUTE foo;  -- fail

-- start_ignore
DROP TABLE IF EXISTS test_table1;
DROP TABLE IF EXISTS test_table2;
DROP TABLE IF EXISTS test_table3;
-- end_ignore

CREATE TABLE test_table1 (a int PRIMARY KEY, b text, c int);
INSERT INTO test_table1 VALUES
(1, 'text1', 0),
(2, 'text2', 1),
(3, 'text3', 0),
(4, 'text4', 1),
(5, 'text0', 0),
(6, 'text1', 1),
(7, 'text2', 0),
(8, 'text3', 1),
(9, 'text4', 0),
(10, 'text0', 1),
(11, 'text1', 0),
(12, 'text2', 1),
(13, 'text3', 0),
(14, 'text4', 1),
(15, 'text0', 0),
(16, 'text1', 1),
(17, 'text2', 0),
(18, 'text3', 1),
(19, 'text4', 0),
(20, 'text0', 1),
(21, 'text1', 0),
(22, 'text2', 1),
(23, 'text3', 0),
(24, 'text4', 1),
(25, 'text0', 0);

CREATE TABLE test_table2 (a int, b text, c int, CONSTRAINT id_t2 PRIMARY KEY(a,c));
INSERT INTO test_table2 VALUES
(1, 'text|1|-1|', -1),
(1, 'text|1|1|',   1),
(2, 'text|2|-1|', -1),
(2, 'text|2|1|',   1),
(3, 'text|3|-1|', -1),
(3, 'text|3|1|',   1),
(4, 'text|4|-1|', -1),
(4, 'text|4|1|',   1),
(5, 'text|5|-1|', -1),
(5, 'text|5|1|',   1);

CREATE TABLE test_table3 (a int PRIMARY KEY, b text, c int);
INSERT INTO test_table3 VALUES
(1, 't3-text1', 0),
(2, 't3-text2', 1),
(3, 't3-text3', 0),
(4, 't3-text4', 1),
(5, 't3-text0', 0),
(6, 't3-text1', 1),
(7, 't3-text2', 0),
(8, 't3-text3', 1),
(9, 't3-text4', 0),
(10, 't3-text0', 1);

-- Check simple group by clause
SELECT a, b FROM test_table1 GROUP BY a ORDER BY a;
SELECT a, b, c FROM test_table1 GROUP BY a, a > 1 ORDER BY a;
SELECT a, b, c FROM test_table1 GROUP BY a, b ORDER BY a;
-- Check grouping sets
SELECT a, b FROM test_table1 GROUP BY GROUPING SETS ((a)) ORDER BY a;
SELECT a, b FROM test_table1 GROUP BY GROUPING SETS ((a), (a)) ORDER BY a;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a)) ORDER BY a, b;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a, b)) ORDER BY a, b;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a, b), (a), (a,c)) ORDER BY a, b, c;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a, a), ()) ORDER BY a, b, c; -- fail
-- Check rollup
SELECT a, b, c FROM test_table1 GROUP BY ROLLUP (a,b,c) ORDER BY a, b, c;
-- Check expressions in target list
SELECT a, b, c, 1+(a+c)*2 AS exp_ac FROM test_table1 GROUP BY a ORDER BY a, b, c, exp_ac;
SELECT 1+(a+c)*2 AS exp_ac, a FROM test_table1 GROUP BY a ORDER BY a;
SELECT 1+(a+c)*2 AS exp_ac_1, 100+(a+c)*2 AS exp_ac_2 FROM test_table1 GROUP BY a ORDER BY exp_ac_1;
SELECT 1+(a+c)*2 AS exp_ac, a FROM test_table1 GROUP BY GROUPING SETS ((a), (a,b)) ORDER BY a;
-- Check together with aggregate functions in target list
SELECT count(*) AS cnt, 1+(a+c)*2 AS exp_ac, a FROM test_table1 GROUP BY GROUPING SETS ((a), (a,b)) ORDER BY a;
-- Check with grouping function in target list
SELECT grouping(a) AS g_a, a, b FROM test_table1 GROUP BY GROUPING SETS ((a)) ORDER BY a;
SELECT grouping(a) AS g_a, grouping(b) AS g_b, avg(c) AS avg_c FROM test_table1 GROUP BY GROUPING SETS ((a), (b), ()) ORDER BY avg_c;
-- Check aggregate functions in ORDER BY clause
SELECT a, b FROM test_table1 GROUP BY GROUPING SETS ((a), (a,b)) ORDER BY avg(c), a, b;
-- Check aggregate functions in HAVING clause
SELECT a, b FROM test_table1 GROUP BY GROUPING SETS ((a), (a,b)) HAVING avg(c) > 0 ORDER BY avg(c), a, b;
-- Check grouping functions in HAVING clause
SELECT grouping(a) AS g_a, a, avg(c) as avg_c FROM test_table1 GROUP BY ROLLUP (a) HAVING grouping(a) = 0 ORDER BY a, avg_c;
SELECT 1+(a+c)*2 AS exp_ac_1, b, grouping(b) AS g_b FROM test_table1 GROUP BY a, b HAVING grouping(b) = 0 ORDER BY exp_ac_1, b;
SELECT grouping(a) AS g_a, grouping(b) AS g_b, avg(c) AS avg_c FROM test_table1 GROUP BY GROUPING SETS ((a), (b), ())
	HAVING  grouping(a) = 1 AND grouping(b) = 1 ORDER BY avg_c;
-- Check sub-query
SELECT * FROM (SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a), (a,c)) ORDER BY a, c) AS sub_t;
SELECT sub_t.a, sub_t.b, sub_t.c FROM (SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a), (a,c)) ORDER BY a, c) AS sub_t 
GROUP BY GROUPING SETS ((sub_t.a, sub_t.b, sub_t.c), ()) ORDER BY sub_t.a, sub_t.b, sub_t.c; -- falls back to Postgres-based planner yet
SELECT (SELECT c) FROM test_table1 GROUP BY a ORDER BY a;
SELECT b, (SELECT c FROM (SELECT c) AS alias_test_table1) FROM test_table1 GROUP BY a ORDER BY a;
-- Check cases with primary key consisting of more than 1 column
SELECT a, b, c FROM test_table2 GROUP BY a, c ORDER BY a, c;
SELECT a, b, c FROM test_table2 GROUP BY GROUPING SETS ((a, c), (a)) ORDER BY a, c; -- fail
SELECT grouping(a) AS g_a, grouping(c) AS g_c, count(*) AS cnt, a, avg(c) FROM test_table2 GROUP BY GROUPING SETS ((a, c), () ) ORDER BY a, c;
SELECT grouping(a) AS g_a, grouping(c) AS g_c, a, b, c FROM test_table2 GROUP BY GROUPING SETS (a, c, (a, c) ) ORDER BY g_a, g_c, a, c; -- fail
SELECT grouping(a) AS g_a, grouping(c) AS g_c, a, b, c FROM test_table2 GROUP BY GROUPING SETS ((a, c) ) ORDER BY g_a, g_c, a, c;
-- Check cases with join and grouping by primary key of one of the tables
SELECT l.a, l.b, count(r.b) AS cnt FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
-- Plus check expressions with aggregate functions 
SELECT l.a, l.b, count(r.b) + avg(r.a) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
SELECT l.a, l.b, count(r.b) + avg(l.a) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
SELECT l.a, l.b, count(r.b) + avg(l.c) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
SELECT l.a, l.b, count(r.b) + avg(r.a) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
-- Plus check aggregate functions with expression as parameters
SELECT l.a, l.b, avg(r.a + l.c) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
SELECT l.a, l.b, avg(r.a + r.c) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
SELECT l.a, l.b, avg(l.a + l.c) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
-- Plus check both cases above together
SELECT l.a, l.b, avg(r.a + l.c) + count(r.b) AS agg_expr FROM test_table1 AS l JOIN test_table2 AS r ON l.a=r.a GROUP BY l.a ORDER BY l.a;
-- Check cases with join of 2 tables on their primary key
SELECT l.a, l.b, r.a, r.b FROM test_table1 AS l JOIN test_table3 AS r ON l.a=r.a GROUP BY l.a, r.a ORDER BY l.a;
SELECT grouping(l.a) AS g_l_a, grouping(r.a) AS g_r_a, grouping(r.c) AS g_r_c, l.a, r.a, r.c
	FROM test_table1 AS l JOIN test_table3 AS r ON l.a=r.a GROUP BY GROUPING SETS ((l.a, r.a), (r.c), ()) ORDER BY l.a, r.c;

-- Checking for ungrouped columns in TargetList

-- grouping expression and GROUP BY
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a),(b),()), a ORDER BY a, b, c;
SELECT a, b, c FROM test_table1 GROUP BY a, GROUPING SETS ((a),(b),()) ORDER BY a, b, c;
SELECT a, b, c FROM test_table1 GROUP BY b, GROUPING SETS ((a),(b),()); -- fail
SELECT a, b, c FROM test_table1 GROUP BY b, GROUPING SETS ((a),()); -- fail
SELECT a, b, c FROM test_table1 GROUP BY a, GROUPING SETS ((a),(b), ROLLUP (a)) ORDER BY a, b, c; -- falls back to Postgres-based planner yet
SELECT a, b, c FROM test_table1 GROUP BY b, GROUPING SETS ((a),(b), ROLLUP (a)); -- fail
SELECT a, b, c FROM test_table1 GROUP BY a, GROUPING SETS ((a),(b), CUBE (a)) ORDER BY a, b, c; -- falls back to Postgres-based planner yet
SELECT a, b, c FROM test_table1 GROUP BY b, GROUPING SETS ((a),(b), CUBE (a)); -- fail

-- only grouping expression
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a),(b),()); -- fail
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS (a), GROUPING SETS (a,()) ORDER BY a, b, c;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS (a), GROUPING SETS (b,()) ORDER BY a, b, c;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS (b), GROUPING SETS (a,()) ORDER BY a, b, c; -- fail
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS (a,()), GROUPING SETS (a,()) ORDER BY a, b, c; -- fail
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS (a,()), GROUPING SETS (a,()) ORDER BY a, b, c; -- fail
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS (GROUPING SETS (GROUPING SETS (a),a)),
		                                 GROUPING SETS (b,()) ORDER BY a, b, c;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS (GROUPING SETS (GROUPING SETS (b),a)),
		                                 GROUPING SETS (b,()); -- fail
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a),(b), GROUPING SETS ((a),(c))) ORDER BY a, b, c;
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a),(b), GROUPING SETS ((a),())); --fail
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a),(b), GROUPING SETS ((a),(b)), GROUPING SETS ((a),())); --fail
SELECT a, b, c FROM test_table1 GROUP BY GROUPING SETS ((a),(b), ROLLUP(a)); -- fail

-- composite Primary Key
SELECT a, b, c FROM test_table2 GROUP BY GROUPING SETS ((a,c),()), a; -- fail
SELECT a, b, c FROM test_table2 GROUP BY GROUPING SETS ((a,c),()), a, c ORDER BY a, b, c;
SELECT a, b, c FROM test_table2 GROUP BY GROUPING SETS ((a,c), GROUPING SETS (a,c)); -- fail
SELECT a, b, c FROM test_table2 GROUP BY GROUPING SETS (a,c), GROUPING SETS (a,()) ORDER BY a, b, c; -- fail
SELECT a, b, c FROM test_table2 GROUP BY ROLLUP (a,c), a; -- fail
SELECT a, b, c FROM test_table2 GROUP BY ROLLUP (a,c), a, c ORDER BY a, b, c;

DROP TABLE test_table1;
DROP TABLE test_table2;
DROP TABLE test_table3;

RESET optimizer_trace_fallback;
RESET optimizer;
