CREATE EXTENSION tablefunc;

--
-- normal_rand()
-- no easy way to do this for regression testing
--
SELECT avg(normal_rand)::int FROM normal_rand(100, 250, 0.2);

--
-- crosstab()
--
CREATE TABLE ct(id int, rowclass text, rowid text, attribute text, val text);
\copy ct from 'data/ct.data'

SELECT * FROM crosstab2('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' and (attribute = ''att2'' or attribute = ''att3'') ORDER BY 1,2;');
SELECT * FROM crosstab3('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' and (attribute = ''att2'' or attribute = ''att3'') ORDER BY 1,2;');
SELECT * FROM crosstab4('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' and (attribute = ''att2'' or attribute = ''att3'') ORDER BY 1,2;');

SELECT * FROM crosstab2('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' ORDER BY 1,2;');
SELECT * FROM crosstab3('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' ORDER BY 1,2;');
SELECT * FROM crosstab4('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' ORDER BY 1,2;');

SELECT * FROM crosstab2('SELECT rowid, attribute, val FROM ct where rowclass = ''group2'' and (attribute = ''att1'' or attribute = ''att2'') ORDER BY 1,2;');
SELECT * FROM crosstab3('SELECT rowid, attribute, val FROM ct where rowclass = ''group2'' and (attribute = ''att1'' or attribute = ''att2'') ORDER BY 1,2;');
SELECT * FROM crosstab4('SELECT rowid, attribute, val FROM ct where rowclass = ''group2'' and (attribute = ''att1'' or attribute = ''att2'') ORDER BY 1,2;');

SELECT * FROM crosstab2('SELECT rowid, attribute, val FROM ct where rowclass = ''group2'' ORDER BY 1,2;');
SELECT * FROM crosstab3('SELECT rowid, attribute, val FROM ct where rowclass = ''group2'' ORDER BY 1,2;');
SELECT * FROM crosstab4('SELECT rowid, attribute, val FROM ct where rowclass = ''group2'' ORDER BY 1,2;');

SELECT * FROM crosstab('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' ORDER BY 1,2;') AS c(rowid text, att1 text, att2 text);
SELECT * FROM crosstab('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' ORDER BY 1,2;') AS c(rowid text, att1 text, att2 text, att3 text);
SELECT * FROM crosstab('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' ORDER BY 1,2;') AS c(rowid text, att1 text, att2 text, att3 text, att4 text);

-- check it works with OUT parameters, too

CREATE FUNCTION crosstab_out(text,
	OUT rowid text, OUT att1 text, OUT att2 text, OUT att3 text)
RETURNS setof record
AS '$libdir/tablefunc','crosstab'
LANGUAGE C STABLE STRICT;

SELECT * FROM crosstab_out('SELECT rowid, attribute, val FROM ct where rowclass = ''group1'' ORDER BY 1,2;');

--
-- hash based crosstab
--
create table cth(id serial, rowid text, rowdt timestamp, attribute text, val text);
insert into cth values(DEFAULT,'test1','01 March 2003','temperature','42');
insert into cth values(DEFAULT,'test1','01 March 2003','test_result','PASS');
-- the next line is intentionally left commented and is therefore a "missing" attribute
-- insert into cth values(DEFAULT,'test1','01 March 2003','test_startdate','28 February 2003');
insert into cth values(DEFAULT,'test1','01 March 2003','volts','2.6987');
insert into cth values(DEFAULT,'test2','02 March 2003','temperature','53');
insert into cth values(DEFAULT,'test2','02 March 2003','test_result','FAIL');
insert into cth values(DEFAULT,'test2','02 March 2003','test_startdate','01 March 2003');
insert into cth values(DEFAULT,'test2','02 March 2003','volts','3.1234');
-- next group tests for NULL rowids
insert into cth values(DEFAULT,NULL,'25 October 2007','temperature','57');
insert into cth values(DEFAULT,NULL,'25 October 2007','test_result','PASS');
insert into cth values(DEFAULT,NULL,'25 October 2007','test_startdate','24 October 2007');
insert into cth values(DEFAULT,NULL,'25 October 2007','volts','1.41234');

-- return attributes as plain text
SELECT * FROM crosstab(
  'SELECT rowid, rowdt, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth ORDER BY 1')
AS c(rowid text, rowdt timestamp, temperature text, test_result text, test_startdate text, volts text);

-- this time without rowdt
SELECT * FROM crosstab(
  'SELECT rowid, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth ORDER BY 1')
AS c(rowid text, temperature text, test_result text, test_startdate text, volts text);

-- convert attributes to specific datatypes
SELECT * FROM crosstab(
  'SELECT rowid, rowdt, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth ORDER BY 1')
AS c(rowid text, rowdt timestamp, temperature int4, test_result text, test_startdate timestamp, volts float8);

-- source query and category query out of sync
SELECT * FROM crosstab(
  'SELECT rowid, rowdt, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth WHERE attribute IN (''temperature'',''test_result'',''test_startdate'') ORDER BY 1')
AS c(rowid text, rowdt timestamp, temperature int4, test_result text, test_startdate timestamp);

-- if category query generates no rows, get expected error
SELECT * FROM crosstab(
  'SELECT rowid, rowdt, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth WHERE attribute = ''a'' ORDER BY 1')
AS c(rowid text, rowdt timestamp, temperature int4, test_result text, test_startdate timestamp, volts float8);

-- if category query generates more than one column, get expected error
SELECT * FROM crosstab(
  'SELECT rowid, rowdt, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT rowdt, attribute FROM cth ORDER BY 2')
AS c(rowid text, rowdt timestamp, temperature int4, test_result text, test_startdate timestamp, volts float8);

-- if source query returns zero rows, get zero rows returned
SELECT * FROM crosstab(
  'SELECT rowid, rowdt, attribute, val FROM cth WHERE false ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth ORDER BY 1')
AS c(rowid text, rowdt timestamp, temperature text, test_result text, test_startdate text, volts text);

-- if source query returns zero rows, get zero rows returned even if category query generates no rows
SELECT * FROM crosstab(
  'SELECT rowid, rowdt, attribute, val FROM cth WHERE false ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth WHERE false ORDER BY 1')
AS c(rowid text, rowdt timestamp, temperature text, test_result text, test_startdate text, volts text);

-- check it works with a named result rowtype

create type my_crosstab_result as (
  rowid text, rowdt timestamp,
  temperature int4, test_result text, test_startdate timestamp, volts float8);

CREATE FUNCTION crosstab_named(text, text)
RETURNS setof my_crosstab_result
AS '$libdir/tablefunc','crosstab_hash'
LANGUAGE C STABLE STRICT;

SELECT * FROM crosstab_named(
  'SELECT rowid, rowdt, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth ORDER BY 1');

-- check it works with OUT parameters

CREATE FUNCTION crosstab_out(text, text,
  OUT rowid text, OUT rowdt timestamp,
  OUT temperature int4, OUT test_result text,
  OUT test_startdate timestamp, OUT volts float8)
RETURNS setof record
AS '$libdir/tablefunc','crosstab_hash'
LANGUAGE C STABLE STRICT;

SELECT * FROM crosstab_out(
  'SELECT rowid, rowdt, attribute, val FROM cth ORDER BY 1',
  'SELECT DISTINCT attribute FROM cth ORDER BY 1');

--
-- connectby
--

-- test connectby with text based hierarchy
CREATE TABLE connectby_text(keyid text, parent_keyid text, pos int);
\copy connectby_text from 'data/connectby_text.data'

-- with branch, without orderby
SELECT * FROM connectby('connectby_text', 'keyid', 'parent_keyid', 'row2', 0, '~') AS t(keyid text, parent_keyid text, level int, branch text);

-- without branch, without orderby
SELECT * FROM connectby('connectby_text', 'keyid', 'parent_keyid', 'row2', 0) AS t(keyid text, parent_keyid text, level int);

-- with branch, with orderby
SELECT * FROM connectby('connectby_text', 'keyid', 'parent_keyid', 'pos', 'row2', 0, '~') AS t(keyid text, parent_keyid text, level int, branch text, pos int) ORDER BY t.pos;

-- without branch, with orderby
SELECT * FROM connectby('connectby_text', 'keyid', 'parent_keyid', 'pos', 'row2', 0) AS t(keyid text, parent_keyid text, level int, pos int) ORDER BY t.pos;

-- test connectby with int based hierarchy
CREATE TABLE connectby_int(keyid int, parent_keyid int);
\copy connectby_int from 'data/connectby_int.data'

-- with branch
SELECT * FROM connectby('connectby_int', 'keyid', 'parent_keyid', '2', 0, '~') AS t(keyid int, parent_keyid int, level int, branch text);

-- without branch
SELECT * FROM connectby('connectby_int', 'keyid', 'parent_keyid', '2', 0) AS t(keyid int, parent_keyid int, level int);

-- recursion detection
INSERT INTO connectby_int VALUES(10,9);
INSERT INTO connectby_int VALUES(11,10);
INSERT INTO connectby_int VALUES(9,11);

-- should fail due to infinite recursion
SELECT * FROM connectby('connectby_int', 'keyid', 'parent_keyid', '2', 0, '~') AS t(keyid int, parent_keyid int, level int, branch text);

-- infinite recursion failure avoided by depth limit
SELECT * FROM connectby('connectby_int', 'keyid', 'parent_keyid', '2', 4, '~') AS t(keyid int, parent_keyid int, level int, branch text);

-- should fail as first two columns must have the same type
SELECT * FROM connectby('connectby_int', 'keyid', 'parent_keyid', '2', 0, '~') AS t(keyid text, parent_keyid int, level int, branch text);

-- should fail as key field datatype should match return datatype
SELECT * FROM connectby('connectby_int', 'keyid', 'parent_keyid', '2', 0, '~') AS t(keyid float8, parent_keyid float8, level int, branch text);

-- tests for values using custom queries
-- query with one column - failed
SELECT * FROM connectby('connectby_int', '1; --', 'parent_keyid', '2', 0) AS t(keyid int, parent_keyid int, level int);
-- query with two columns first value as NULL
SELECT * FROM connectby('connectby_int', 'NULL::int, 1::int; --', 'parent_keyid', '2', 0) AS t(keyid int, parent_keyid int, level int);
-- query with two columns second value as NULL
SELECT * FROM connectby('connectby_int', '1::int, NULL::int; --', 'parent_keyid', '2', 0) AS t(keyid int, parent_keyid int, level int);
-- query with two columns, both values as NULL
SELECT * FROM connectby('connectby_int', 'NULL::int, NULL::int; --', 'parent_keyid', '2', 0) AS t(keyid int, parent_keyid int, level int);

-- test for falsely detected recursion
DROP TABLE connectby_int;
CREATE TABLE connectby_int(keyid int, parent_keyid int);
INSERT INTO connectby_int VALUES(11,NULL);
INSERT INTO connectby_int VALUES(10,11);
INSERT INTO connectby_int VALUES(111,11);
INSERT INTO connectby_int VALUES(1,111);
-- this should not fail due to recursion detection
SELECT * FROM connectby('connectby_int', 'keyid', 'parent_keyid', '11', 0, '-') AS t(keyid int, parent_keyid int, level int, branch text);

-- test for CTAS and DML operations
-- from crosstab(text,text)
CREATE TABLE ct1(
    row_name varchar(32),
    extra_col varchar(32),
    attr varchar(32),
    value varchar(32)
)
DISTRIBUTED BY (row_name);
\copy ct1 from 'data/ctas.data'

CREATE TABLE ct2
(
    row_name varchar(32),
    extra    varchar(32),
    cat1     varchar(32),
    cat2     varchar(32),
    cat3     varchar(32),
    cat4     varchar(32)
) DISTRIBUTED BY (row_name);

INSERT INTO ct2
SELECT ct.*
FROM crosstab(
  'select row_name, extra_col, attr, value
  from ct1
  order by 1',
  'select ''att'' || seq from generate_series(1, 4) seq order by 1')
AS ct(row_name varchar, extra varchar, att1 varchar, att2 varchar, att3 varchar, att4 varchar);

SELECT * FROM ct2;

CREATE TABLE ct2_2 AS SELECT ct.*
FROM crosstab(
  'select row_name, extra_col, attr, value
  from ct1
  order by 1',
  'select ''att'' || seq from generate_series(1, 4) seq order by 1')
AS ct(row_name varchar, extra varchar, att1 varchar, att2 varchar, att3 varchar, att4 varchar);

SELECT * FROM ct2_2;

-- from crosstab(text)
CREATE TABLE ct3 (
    row_name varchar(32),
    cat1     varchar(32),
    cat2     varchar(32),
    cat3     varchar(32)
) DISTRIBUTED BY (row_name);

INSERT INTO ct3
SELECT *
FROM crosstab(
  'select row_name, attr, value
   from ct1
   where attr = ''att2'' or attr = ''att3''
   order by 1,2')
AS ct(row_name varchar, att1 varchar, att2 varchar, att3 varchar);

SELECT * FROM ct3;

CREATE TABLE ct3_2 AS SELECT *
FROM crosstab(
  'select row_name, attr, value
   from ct1
   where attr = ''att2'' or attr = ''att3''
   order by 1,2')
AS ct(row_name varchar, att1 varchar, att2 varchar, att3 varchar);

SELECT * FROM ct3_2;

-- from crosstab2(text)
CREATE TABLE ct4(
    row_name text,
    extra_col text,
    attr text,
    value text
)
DISTRIBUTED BY (row_name);
\copy ct4 from 'data/ctas.data'

CREATE TABLE ct5 (
    row_name   text,
    category_1 text,
    category_2 text
) DISTRIBUTED BY (row_name);

INSERT INTO ct5
SELECT *
FROM crosstab2(
  'select row_name, attr, value
   from ct4
   where attr = ''att2'' or attr = ''att3''
   order by 1,2');

SELECT * FROM ct5;

CREATE TABLE ct5_2 AS SELECT *
FROM crosstab2(
  'select row_name, attr, value
   from ct4
   where attr = ''att2'' or attr = ''att3''
   order by 1,2') AS ct ;

SELECT * FROM ct5_2;

-- from crosstab3(text)
CREATE TABLE ct6 (
    row_name   text,
    category_1 text,
    category_2 text,
    category_3 text
) DISTRIBUTED BY (row_name);

INSERT INTO ct6
SELECT *
FROM crosstab3(
  'select row_name, attr, value
   from ct4
   where attr = ''att2'' or attr = ''att3''
   order by 1,2');
SELECT * FROM ct6;

CREATE TABLE ct6_2 AS SELECT *
FROM crosstab3(
  'select row_name, attr, value
   from ct4
   where attr = ''att2'' or attr = ''att3''
   order by 1,2') AS ct ;

SELECT * FROM ct6_2;

-- from crosstab4(text)
CREATE TABLE ct7 (
    row_name   text,
    category_1 text,
    category_2 text,
    category_3 text,
    category_4 text
) DISTRIBUTED BY (row_name);

INSERT INTO ct7
SELECT *
FROM crosstab4(
  'select row_name, attr, value
   from ct4
   where attr = ''att2'' or attr = ''att3''
   order by 1,2');

SELECT * FROM ct7;

CREATE TABLE ct7_2 AS SELECT *
FROM crosstab4(
  'select row_name, attr, value
   from ct4
   where attr = ''att2'' or attr = ''att3''
   order by 1,2') AS ct ;

SELECT * FROM ct7_2;

-- from connectby() without branch, with orderby
CREATE TABLE connby(
  keyid text,
  parent_keyid text,
  level int,
  pos int
) DISTRIBUTED BY (keyid);

INSERT INTO connby
SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'pos', 'row2', 0
) AS t(keyid text, parent_keyid text, level int, pos int) ORDER BY t.pos;

SELECT * FROM connby;

CREATE TABLE connby_2 AS SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'pos', 'row2', 0
) AS t(keyid text, parent_keyid text, level int, pos int) ORDER BY t.pos;

SELECT * FROM connby_2;

-- from connectby() without branch, without orderby
CREATE TABLE connby2(
  keyid text,
  parent_keyid text,
  level int
) DISTRIBUTED BY (keyid);

INSERT INTO connby2
SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'row2', 0
) AS t(keyid text, parent_keyid text, level int);

SELECT * FROM connby2;

CREATE TABLE connby2_2 AS SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'row2', 0
) AS t(keyid text, parent_keyid text, level int);

SELECT * FROM connby2_2;

-- from connectby() with branch, with orderby
CREATE TABLE connby3(
  keyid text,
  parent_keyid text,
  level int,
  branch text,
  pos int
) DISTRIBUTED BY (keyid);

INSERT INTO connby3
SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'pos', 'row2', 0, '~'
) AS t(keyid text, parent_keyid text, level int, branch text, pos int) ORDER BY t.pos;

SELECT * FROM connby3;

CREATE TABLE connby3_2 AS SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'pos', 'row2', 0, '~'
) AS t(keyid text, parent_keyid text, level int, branch text, pos int) ORDER BY t.pos;

SELECT * FROM connby3_2;

-- from connectby() with branch, without orderby
CREATE TABLE connby4(
  keyid text,
  parent_keyid text,
  level int,
  branch text
) DISTRIBUTED BY (keyid);

INSERT INTO connby4
SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'row2', 0, '~'
) AS t(keyid text, parent_keyid text, level int, branch text);

SELECT * FROM connby4;

CREATE TABLE connby4_2 AS SELECT * FROM connectby(
  'connectby_text', 'keyid', 'parent_keyid', 'row2', 0, '~'
) AS t(keyid text, parent_keyid text, level int, branch text);

SELECT * FROM connby4_2;

-- from normal_dist
CREATE TABLE t_normal(normal_rand float8);

INSERT INTO t_normal SELECT * FROM normal_rand(5, 250, 0);

SELECT * FROM t_normal;

CREATE TABLE t_normal_2 AS SELECT * FROM normal_rand(5, 250, 0);

SELECT * FROM t_normal_2;
