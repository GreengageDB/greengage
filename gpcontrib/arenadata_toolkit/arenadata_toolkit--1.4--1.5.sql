/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.4--1.5.sql */

/*
 This is part of arenadata_toolkit API for ADB Bundle.
 This function collects information in db_files_current and db_files_history tables.
 */
CREATE OR REPLACE FUNCTION arenadata_toolkit.adb_collect_table_stats()
RETURNS VOID
AS $$
BEGIN
	IF NOT EXISTS (
		SELECT
		FROM pg_partition pp
		JOIN pg_partition_rule pr ON pr.paroid = pp.oid
		WHERE
			-- The filter on the line below, which duplicates condition in the 'case',
			-- is an optimization to fetch less tuples from the pg_partition.
			pp.parrelid = 'arenadata_toolkit.db_files_history'::regclass
			AND
			CASE
				WHEN pp.parrelid = 'arenadata_toolkit.db_files_history'::regclass
				THEN
					now() >= CAST(substring(pg_get_expr(pr.parrangestart, pr.parchildrelid) FROM '#"%#"::%' FOR '#')
						AS TIMESTAMP WITHOUT TIME ZONE)
					AND
					now() < CAST(substring(pg_get_expr(pr.parrangeend, pr.parchildrelid) FROM '#"%#"::%' FOR '#')
						AS TIMESTAMP WITHOUT TIME ZONE)
				ELSE FALSE
			END)
	THEN
		EXECUTE FORMAT($fmt$ALTER TABLE arenadata_toolkit.db_files_history SPLIT DEFAULT PARTITION
			START (date %1$L) INCLUSIVE
			END (date %2$L) EXCLUSIVE
			INTO (PARTITION %3$I, default partition);$fmt$,
				to_char(now(), 'YYYY-MM-01'),
				to_char(now() + interval '1 month','YYYY-MM-01'),
				'p'||to_char(now(), 'YYYYMM'));
	END IF;

	CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.db_files_current
	(
		oid BIGINT,
		table_name TEXT,
		table_schema TEXT,
		type CHAR(1),
		storage CHAR(1),
		table_parent_table TEXT,
		table_parent_schema TEXT,
		table_database TEXT,
		table_tablespace TEXT,
		"content" INTEGER,
		segment_preferred_role CHAR(1),
		hostname TEXT,
		address TEXT,
		file TEXT,
		modifiedtime TIMESTAMP WITHOUT TIME ZONE,
		file_size BIGINT,
		tablespace_location TEXT
	)
	DISTRIBUTED RANDOMLY;

	/*
		Since this table is temporary and user can call this several times in a session
		then we need to truncate it (an alternative will be to drop it).
	 */
	TRUNCATE TABLE pg_temp.db_files_current;

	/* We use temporary table in this case, because we don't want to add own own unmapped
	   relfilenode oids in a db_files_current and db_files_history tables. The downside of
	   this approach that if time between calls to adb_collect_table_stats will be less
	   then checkpointing interval (there will be no unlink peformed), than these unmapped
	   files will still go to db_files_current and db_files_history as unmapped.
	*/
	INSERT INTO pg_temp.db_files_current
	SELECT
		f.oid,
		f.table_name,
		f.table_schema,
		f.type,
		f.storage,
		cl.relname AS table_parent_table,
		n.nspname AS table_parent_schema,
		f.table_database,
		f.table_tablespace,
		f."content",
		f.segment_preferred_role,
		f.hostname,
		f.address,
		f.file,
		CURRENT_TIMESTAMP AS modifiedtime,
		f.file_size,
		f.tablespace_location
	FROM arenadata_toolkit.__db_files_current AS f
	LEFT JOIN pg_partition_rule pr ON pr.parchildrelid = f.oid
	LEFT JOIN pg_partition pp ON pr.paroid = pp.oid
	LEFT JOIN pg_class cl ON cl.oid = pp.parrelid
	LEFT JOIN pg_namespace n ON cl.relnamespace = n.oid;

	/*
	 Here we may truncate it, it's relfilenodes aren't in db_files_current temporary table.
	 */
	TRUNCATE TABLE arenadata_toolkit.db_files_current;

	INSERT INTO arenadata_toolkit.db_files_current
	SELECT *
	FROM pg_temp.db_files_current;

	INSERT INTO arenadata_toolkit.db_files_history
	SELECT *, now() AS collecttime
	FROM arenadata_toolkit.db_files_current;
END$$
LANGUAGE plpgsql VOLATILE
EXECUTE ON MASTER;
