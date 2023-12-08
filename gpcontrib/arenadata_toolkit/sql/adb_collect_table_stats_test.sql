CREATE EXTENSION arenadata_toolkit;
SET search_path = arenadata_toolkit;

-- Need to disable notice messages from plpgsql functions
SET client_min_messages=WARNING;

CREATE FUNCTION remove_partition_from_db_files_history()
RETURNS VOID
AS $$
BEGIN
	EXECUTE FORMAT($fmt$ALTER TABLE db_files_history DROP PARTITION %1$I$fmt$,
				'p'||to_char(now(), 'YYYYMM'));
END$$
LANGUAGE plpgsql VOLATILE
EXECUTE ON MASTER;

-- There are not "db_files_history" and partitions
SELECT count(inhrelid)
FROM pg_inherits
LEFT JOIN pg_class ON oid = inhparent
WHERE relname = 'db_files_history';

SELECT adb_create_tables();

-- There are "db_files_history" and two partitions (default and for current month)
SELECT count(inhrelid)
FROM pg_inherits
LEFT JOIN pg_class ON oid = inhparent
WHERE relname = 'db_files_history';

-- Remove partition from "db_files_history" for current month
SELECT remove_partition_from_db_files_history();

-- There is only default partition for "db_files_history"
SELECT count(inhrelid)
FROM pg_inherits
LEFT JOIN pg_class ON oid = inhparent
WHERE relname = 'db_files_history';

SELECT adb_collect_table_stats();

-- There are "db_files_history" and two partitions (default and for current month)
SELECT count(inhrelid)
FROM pg_inherits
LEFT JOIN pg_class ON oid = inhparent
WHERE relname = 'db_files_history';

SELECT adb_collect_table_stats();

-- There is not any new partitions for "db_files_history"
SELECT count(inhrelid)
FROM pg_inherits
LEFT JOIN pg_class ON oid = inhparent
WHERE relname = 'db_files_history';

-- Create table with partitions for test "INSERT INTO arenadata_toolkit.db_files_current"
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

-- add small data to all parts of table
INSERT INTO part_table
	SELECT i+1, mod(i/16,2)+1, mod(i/8,2)+1, mod(i/4,2)+1, mod(i/2,2)+1, 'sub_prt' || mod(i,2)+1
	FROM generate_series(0,399) as i;

-- add a lot of data for one part
-- distributing by first columnt is helps to us to put all data to one segment
INSERT INTO part_table SELECT 1,1,1,1,1,'sub_prt1' FROM generate_series(1,10000) AS i;

-- update arenadata_toolkit.db_files_current
select arenadata_toolkit.adb_collect_table_stats();

select table_name, table_schema, table_parent_table, table_parent_schema, file_size
FROM arenadata_toolkit.db_files_current
where table_name LIKE 'part_table%'
ORDER BY oid;

-- Cleanup
DROP TABLE part_table;
DROP FUNCTION remove_partition_from_db_files_history();
DROP EXTENSION arenadata_toolkit;
