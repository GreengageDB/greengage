CREATE EXTENSION arenadata_toolkit;
-- Change log level to disable notice messages from PL/pgSQL
SET client_min_messages=WARNING;
SELECT arenadata_toolkit.adb_create_tables();
RESET client_min_messages;

CREATE SCHEMA test_vacuum;

CREATE TABLE test_vacuum.vacuumed (a int) DISTRIBUTED BY (a);
CREATE TABLE test_vacuum.not_vacuumed (a int) DISTRIBUTED BY (a);
-- Disable multiple notifications about the creation of multiple subpartitions.
SET client_min_messages=WARNING;
CREATE TABLE test_vacuum.part_table (id INT, a INT, b INT, c INT, d INT, str TEXT)
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
RESET client_min_messages;

INSERT INTO test_vacuum.vacuumed SELECT generate_series(1, 10);
INSERT INTO test_vacuum.not_vacuumed SELECT generate_series(1, 10);

DELETE FROM test_vacuum.vacuumed WHERE a >= 5;
DELETE FROM test_vacuum.not_vacuumed WHERE a >= 5;

VACUUM test_vacuum.vacuumed;

-- default strategy
SELECT * FROM arenadata_toolkit.adb_vacuum_strategy_newest_first('VACUUM') WHERE table_schema = 'test_vacuum';
-- reversed strategy
SELECT * FROM arenadata_toolkit.adb_vacuum_strategy_newest_last('VACUUM') WHERE table_schema = 'test_vacuum';

DROP SCHEMA test_vacuum CASCADE;
DROP EXTENSION arenadata_toolkit;

-- Change log level to disable notice messages of dropped objects from
-- "DROP SCHEMA arenadata_toolkit CASCADE;"
SET client_min_messages=WARNING;
DROP SCHEMA arenadata_toolkit CASCADE;
RESET client_min_messages;
