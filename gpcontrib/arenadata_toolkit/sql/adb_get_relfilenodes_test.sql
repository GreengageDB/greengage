SET client_min_messages=WARNING;

\! mkdir -p /tmp/arenadata_toolkit_test
CREATE TABLESPACE arenadata_test location '/tmp/arenadata_toolkit_test/';

CREATE EXTENSION arenadata_toolkit;
SELECT arenadata_toolkit.adb_create_tables();

-- Test work with empty tablespace
SELECT table_schema, table_tablespace, content,
		regexp_replace(table_name, '\d+', 'tbloid') AS table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;

-- Test work with non-empty tablespace

-- Simple table
CREATE TABLE arenadata_toolkit_table(a int, b int)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content,
		regexp_replace(table_name, '\d+', 'tbloid') AS table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- Table with toasts
CREATE TABLE arenadata_toolkit_table(a int, b text)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content,
		regexp_replace(table_name, '\d+', 'tbloid') AS table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- AO table
CREATE TABLE arenadata_toolkit_table(a int, b int)
	WITH (APPENDONLY=true)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content,
		regexp_replace(table_name, '\d+', 'tbloid') AS table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- Work with Temp table should be as at empty tablespace
CREATE TEMP TABLE arenadata_toolkit_table(a int, b int)
	TABLESPACE arenadata_test
	DISTRIBUTED BY (a);
SELECT table_schema, table_tablespace, content,
		regexp_replace(table_name, '\d+', 'tbloid') AS table_name
	FROM arenadata_toolkit.__db_files_current
	WHERE table_tablespace = 'arenadata_test' AND table_name != ''
	ORDER BY
		table_name ASC,
		content ASC;
DROP TABLE arenadata_toolkit_table;

-- Cleanup
DROP TABLESPACE arenadata_test;
\! rm -rf /tmp/arenadata_toolkit_test

DROP EXTENSION arenadata_toolkit;
DROP SCHEMA arenadata_toolkit cascade;

RESET client_min_messages;
