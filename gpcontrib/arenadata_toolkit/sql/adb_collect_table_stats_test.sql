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

CREATE FUNCTION add_extra_partition_to_db_files_history()
RETURNS VOID
AS $$
BEGIN
	EXECUTE FORMAT($fmt$ALTER TABLE arenadata_toolkit.db_files_history SPLIT DEFAULT PARTITION
		START (date %1$L) INCLUSIVE
		END (date %2$L) EXCLUSIVE
		INTO (PARTITION %3$I, default partition);$fmt$,
			to_char(now() - interval '1 month', 'YYYY-MM-01'),
			to_char(now(), 'YYYY-MM-01'),
			'p'||to_char(now() - interval '1 month', 'YYYYMM'));
END$$
LANGUAGE plpgsql VOLATILE
EXECUTE ON MASTER;

-- Create a table with partitioning not by a column of the timestamp type.
-- It is required to verify that adb_collect_table_stats() doesn't fail
-- when there are tables with pertitioning not by timestamp presented.
CREATE TABLE part_table_bigint (a int, b bigint)
DISTRIBUTED BY (a)
PARTITION BY RANGE(b)
(
	PARTITION part_table_bigint_t1 START ('10'::bigint) END ('1000000'::bigint),
	PARTITION part_table_bigint_t2 START ('1000001'::bigint) END ('2000000'::bigint)
);

-- Create a table with partitioning not by a column of the timestamp type
-- and with the same name as 'db_files_history', but in a different schema.
-- It is required to verify that adb_collect_table_stats() doesn't fail
-- when there are tables with pertitioning not by timestamp presented.
CREATE TABLE public.db_files_history (a int, b bigint)
DISTRIBUTED BY (a)
PARTITION BY RANGE(b)
(
	PARTITION pub_db_files_history_t1 START ('10'::bigint) END ('1000000'::bigint),
	PARTITION pub_db_files_history_t2 START ('1000001'::bigint) END ('2000000'::bigint)
);

-- There are not "db_files_history" and partitions
-- Note: we can't use 'arenadata_toolkit.db_files_history'::regclass at this
-- moment as it doesn't exist yet (before call to adb_create_tables())
-- and will cause an error.
SELECT count(1)
FROM pg_inherits pi
JOIN pg_class pc ON pc.oid = pi.inhparent
JOIN pg_namespace pn ON pc.relnamespace = pn.oid
WHERE pc.relname = 'db_files_history' AND pn.nspname = 'arenadata_toolkit';

SELECT adb_create_tables();

-- There are "db_files_history" and two partitions (default and for current month)
SELECT count(1)
FROM pg_inherits
WHERE inhparent = 'arenadata_toolkit.db_files_history'::regclass;

-- Remove partition from "db_files_history" for current month
SELECT remove_partition_from_db_files_history();

-- There is only default partition for "db_files_history"
SELECT count(1)
FROM pg_inherits
WHERE inhparent = 'arenadata_toolkit.db_files_history'::regclass;

SELECT adb_collect_table_stats();

-- There are "db_files_history" and two partitions (default and for current month)
SELECT count(1)
FROM pg_inherits
WHERE inhparent = 'arenadata_toolkit.db_files_history'::regclass;

SELECT adb_collect_table_stats();

-- There is not any new partitions for "db_files_history"
SELECT count(1)
FROM pg_inherits
WHERE inhparent = 'arenadata_toolkit.db_files_history'::regclass;

-- Create a partition for the month before the current one
SELECT add_extra_partition_to_db_files_history();

-- There are "db_files_history" and three partitions
-- (default, for the current month and for the month before the current one)
SELECT count(1)
FROM pg_inherits
WHERE inhparent = 'arenadata_toolkit.db_files_history'::regclass;

-- Remove the partition from "db_files_history" for the current month
SELECT remove_partition_from_db_files_history();

-- There are "db_files_history" and two partitions (default and for the month before the current one)
SELECT count(1)
FROM pg_inherits
WHERE inhparent = 'arenadata_toolkit.db_files_history'::regclass;

SELECT adb_collect_table_stats();

-- There are "db_files_history" and three partitions
-- (default, for the current month and for the month before the current one)
SELECT count(1)
FROM pg_inherits
WHERE inhparent = 'arenadata_toolkit.db_files_history'::regclass;

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
DROP TABLE part_table_bigint;
DROP TABLE public.db_files_history;
DROP FUNCTION remove_partition_from_db_files_history();
DROP EXTENSION arenadata_toolkit;
DROP SCHEMA arenadata_toolkit CASCADE;
RESET client_min_messages;
