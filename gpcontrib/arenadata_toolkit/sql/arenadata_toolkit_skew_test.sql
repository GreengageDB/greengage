CREATE EXTENSION arenadata_toolkit;

----------------------------------------------------------------------------------------------------------
-- test helpers
----------------------------------------------------------------------------------------------------------

SET search_path = arenadata_toolkit;

-- start_matchsubs
-- m/(.*)size=\d+/
-- s/(.*)size=\d+/$1size/
-- end_matchsubs

-- function compares behaviour of pg_relation_size and adb_relation_storage_size functions for
-- table and it's forks in case of AO/CO tables (and also external) there are no other forks except main
CREATE FUNCTION compare_table_and_forks_size_calculation(tbl_oid OID)
RETURNS TABLE(fork TEXT, result_equals BOOLEAN, is_empty BOOLEAN, tbl_size TEXT) AS $$
BEGIN
	RETURN QUERY
		SELECT 'main', pg_relation_size(tbl_oid) = adb_relation_storage_size(tbl_oid),
			pg_relation_size(tbl_oid) = 0, 'size=' || pg_relation_size(tbl_oid)::TEXT
		UNION
		SELECT 'fsm', pg_relation_size(tbl_oid, 'fsm') = adb_relation_storage_size(tbl_oid, 'fsm'),
			pg_relation_size(tbl_oid, 'fsm') = 0, 'size=' || pg_relation_size(tbl_oid, 'fsm')::TEXT
		UNION
		SELECT 'vm', pg_relation_size(tbl_oid, 'vm') = adb_relation_storage_size(tbl_oid, 'vm'),
			pg_relation_size(tbl_oid, 'vm') = 0, 'size=' || pg_relation_size(tbl_oid, 'vm')::TEXT
		ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION check_size_diff(tbl_oid OID) RETURNS TABLE(size_equals BOOLEAN, mul_factor INT) AS $$
BEGIN
	RETURN QUERY SELECT adb_relation_storage_size(tbl_oid) = pg_relation_size(tbl_oid),
		round(adb_relation_storage_size(tbl_oid)::DECIMAL / pg_relation_size(tbl_oid))::INT;
END;
$$ LANGUAGE plpgsql;

----------------------------------------------------------------------------------------------------------
-- test adb_relation_storage_size function (in compare with pg_relation_size)
----------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------
-- test case 1: check that behaviour for not existing table the same
----------------------------------------------------------------------
SELECT (pg_relation_size(100000::OID) IS NULL) = (adb_relation_storage_size(100000::OID) IS NULL) AS equals;

----------------------------------------------------------------------
-- test case 2: check behaviour equality for heap tables
----------------------------------------------------------------------
CREATE TABLE heap(i INT, j INT) WITH (appendonly = false) DISTRIBUTED BY (i);

-- check emptiness of heap table
SELECT adb_relation_storage_size('heap'::regclass::oid) = 0 AS is_empty;

-- compare size calculation on filled heap table (vm and fsm table sizes must
-- be > 0 - 'is_empty' column must be false)
INSERT INTO heap SELECT i, i AS j FROM generate_series(1, 100000) i;
VACUUM heap;
SELECT * FROM compare_table_and_forks_size_calculation('heap'::regclass::OID);

DROP TABLE heap;

----------------------------------------------------------------------
-- test case 3: check behaviour equality for AO tables
----------------------------------------------------------------------
CREATE TABLE empty_ao(i INT, j INT) WITH (appendonly=true) DISTRIBUTED BY (i);

-- check emptiness of AO table
SELECT adb_relation_storage_size('empty_ao'::regclass::oid) = 0 AS is_empty;

-- compare size calculation of AO table (vm and fsm tables size must be empty - 'is_empty' column true).
-- CTAS (utility) statement is used to check that zero (0) segment is taken into account
-- (from src/backend/access/appendonly/README.md: utility mode inserts data to zero segment)
CREATE TABLE ao WITH (appendonly=true) AS SELECT i, i AS j FROM generate_series(1, 10000) i DISTRIBUTED BY (i);
INSERT INTO ao SELECT i, i AS j FROM generate_series(1, 100000) i;
VACUUM ao;
SELECT * FROM compare_table_and_forks_size_calculation('ao'::regclass::OID);

DROP TABLE empty_ao, ao;

----------------------------------------------------------------------
-- test case 4: check behaviour equality for CO tables
----------------------------------------------------------------------
CREATE TABLE empty_co(i INT, j INT) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);

-- check emptiness of CO table
SELECT adb_relation_storage_size('empty_co'::regclass::oid) = 0 AS is_empty;

-- compare size calculation of CO table (vm and fsm tables size must be empty - 'is_empty' column true).
-- CTAS (utility) statement is used to check that zero (0) segment is taken into account
-- (from src/backend/access/appendonly/README.md: utility mode inserts data to zero segment)
CREATE TABLE co WITH (appendonly=true, orientation=column) AS
	SELECT i, i AS j FROM generate_series(1, 10000) i DISTRIBUTED BY (i);
INSERT INTO co SELECT i, i AS j FROM generate_series(1, 100000) i;
VACUUM co;
SELECT * FROM compare_table_and_forks_size_calculation('co'::regclass::OID);

DROP TABLE empty_co, co;

------------------------------------------------ ----------------------
-- test case 4: check behaviour equality for external tables
-----------------------------------------------------------------------
CREATE EXTERNAL WEB TABLE external_tbl(field TEXT) EXECUTE 'echo 1' FORMAT 'TEXT';

-- size of table (and it's fork) must be 0
SELECT * FROM compare_table_and_forks_size_calculation('external_tbl'::regclass::OID);

DROP EXTERNAL TABLE external_tbl;

------------------------------------------------ ----------------------
-- test case 5: check behaviour difference (pg_relation_size and
-- adb_relation_storage_size) for AO/CO table size (when insert transaction
-- failed the physical size of table is returned by adb_relation_storage_size
-- unlike 'virtual' size (as it's done at pg_relation_size)
-----------------------------------------------------------------------
CREATE TABLE ao(i INT, j INT) WITH (appendonly=true) DISTRIBUTED BY (i);
INSERT INTO ao SELECT i, i AS j from generate_series(1, 1000) i;
CREATE TABLE co(i INT, j INT) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);
INSERT INTO co SELECT i, i AS j from generate_series(1, 1000) i;

SELECT * FROM check_size_diff('ao'::regclass::OID);
SELECT * FROM check_size_diff('co'::regclass::OID);

-- insert data and rollback it, to check that physical size was changed while 'virtual' not
BEGIN;
INSERT INTO co SELECT i, i AS j FROM generate_series(1, 1000) i;
INSERT INTO ao SELECT i, i AS j FROM generate_series(1, 1000) i;
ABORT;

SELECT * FROM check_size_diff('ao'::regclass::OID);
SELECT * FROM check_size_diff('co'::regclass::OID);

DROP TABLE ao, co;

----------------------------------------------------------------------------------------------------------
-- test adb_skew_coefficients view
----------------------------------------------------------------------------------------------------------

CREATE TABLE heap(i INT, j INT) WITH (appendonly = false) DISTRIBUTED BY(i);
CREATE TABLE ao  (i INT, j INT) WITH (appendonly = true) DISTRIBUTED BY(i);
CREATE TABLE co  (i INT, j INT) WITH (appendonly = true, orientation = column) DISTRIBUTED BY(i);

insert into heap SELECT i, i AS j FROM generate_series(1, 100000) i;
insert into ao   SELECT i, i AS j FROM generate_series(1, 100000) i;
insert into co   SELECT i, i AS j FROM generate_series(1, 100000) i;

-- round is used to prevent test flakiness. Original values of coefficient for
-- AO, CO, heap tables  differs (but for AO/CO them are close enough), because
-- of different storage models. (Also zero value of skew coefficient for heap
-- table is a kind of luck conditioned by NUM_SEG=3 and inserted 100000 tuples)
SELECT skcnamespace, skcrelname, round(skccoeff, 2) as skccoeff_round FROM adb_skew_coefficients order by skcrelname;

-- produce the skew
insert into heap SELECT 1, i AS j FROM generate_series(1, 10000) i;
insert into ao   SELECT 1, i AS j FROM generate_series(1, 10000) i;
insert into co   SELECT 1, i AS j FROM generate_series(1, 10000) i;

-- check that skewcoeff was increased
SELECT skcnamespace, skcrelname, round(skccoeff, 2) as skccoeff_round FROM adb_skew_coefficients order by skcrelname;

DROP TABLE heap, ao, co;

----------------------------------------------------------------------------------------------------------
-- test adb_skew_coefficients view on partitioned table
----------------------------------------------------------------------------------------------------------

CREATE TABLE part_table (id INT, a INT, b INT, c INT, d INT, str TEXT)
DISTRIBUTED BY (id)
PARTITION BY RANGE (a)
	SUBPARTITION BY RANGE (b)
		SUBPARTITION TEMPLATE (START (1) END (3) EVERY (1))
	SUBPARTITION BY RANGE (c)
		SUBPARTITION TEMPLATE (START (1) END (3) EVERY (1))
	SUBPARTITION BY RANGE (d)
		SUBPARTITION TEMPLATE (START (1) END (3) EVERY (1))
	SUBPARTITION BY LIST (str)
		SUBPARTITION TEMPLATE (
			SUBPARTITION sub_prt1 VALUES ('sub_prt1'),
			SUBPARTITION sub_prt2 VALUES ('sub_prt2'))
	(START (1) END (3) EVERY (1));

-- check that adb_skew_coefficients works on empty table
SELECT skcnamespace, skcrelname, round(skccoeff, 2) AS skccoeff_round
	FROM adb_skew_coefficients
	WHERE skcrelname LIKE 'part_table%'
	ORDER BY skcrelname;

-- add small data to all parts of table
INSERT INTO part_table
	SELECT i+1, mod(i/16,2)+1, mod(i/8,2)+1, mod(i/4,2)+1, mod(i/2,2)+1, 'sub_prt' || mod(i,2)+1
	FROM generate_series(0,399) as i;

SELECT skcnamespace, skcrelname, round(skccoeff, 2) AS skccoeff_round
	FROM adb_skew_coefficients
	WHERE skcrelname LIKE 'part_table%'
	ORDER BY skcrelname;

-- add a lot of data for one part to generate big skew coefficient
-- distributing by first columnt is helps to us to put all data to one segment
INSERT INTO part_table SELECT 1,1,1,1,1,'sub_prt1' FROM generate_series(1,10000) AS i;

SELECT skcnamespace, skcrelname, round(skccoeff, 2) AS skccoeff_round
	FROM adb_skew_coefficients
	WHERE skcrelname LIKE 'part_table%' AND skccoeff != 0
	ORDER BY skcrelname;

DROP TABLE part_table;

DROP FUNCTION compare_table_and_forks_size_calculation(OID);
DROP FUNCTION check_size_diff(OID);

RESET search_path;
DROP EXTENSION arenadata_toolkit;
DROP SCHEMA arenadata_toolkit CASCADE;
