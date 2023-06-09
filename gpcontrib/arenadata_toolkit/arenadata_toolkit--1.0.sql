/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION arenadata_toolkit" to load this file. \quit

DO $$
BEGIN
	/*
	 For new deployments create arenadata_toolkit schema, but disconnect it from extension,
	 since user's tables like arenadata_toolkit.db_files_history need to be out of extension.
	 */
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'arenadata_toolkit')
	THEN
		CREATE SCHEMA arenadata_toolkit;
		ALTER EXTENSION arenadata_toolkit DROP SCHEMA arenadata_toolkit;
	END IF;
END$$;

GRANT USAGE ON SCHEMA arenadata_toolkit TO public;

CREATE FUNCTION arenadata_toolkit.adb_relation_storage_size(reloid OID, forkName TEXT default 'main')
RETURNS BIGINT
AS '$libdir/arenadata_toolkit', 'adb_relation_storage_size'
LANGUAGE C VOLATILE STRICT;

GRANT EXECUTE ON FUNCTION arenadata_toolkit.adb_relation_storage_size(OID, TEXT) TO public;
COMMENT ON FUNCTION arenadata_toolkit.adb_relation_storage_size(OID, TEXT) IS 'Provides relation storage size details';

CREATE VIEW arenadata_toolkit.adb_skew_coefficients
AS
WITH storage AS (
    SELECT
        autoid,
        arenadata_toolkit.adb_relation_storage_size(autoid) AS size
    FROM gp_dist_random('gp_toolkit.__gp_user_tables')
    WHERE autrelstorage != 'x'
), skew AS (
    SELECT
        autoid AS skewoid,
        stddev(size) AS skewdev,
        avg(size) AS skewmean
    FROM storage GROUP BY autoid
)
SELECT
    skew.skewoid AS skcoid,
    pgn.nspname  AS skcnamespace,
    pgc.relname  AS skcrelname,
    CASE WHEN skewdev > 0 THEN skewdev/skewmean * 100.0 ELSE 0 END AS skccoeff
FROM skew
JOIN pg_catalog.pg_class pgc
    ON (skew.skewoid = pgc.oid)
JOIN pg_catalog.pg_namespace pgn
    ON (pgc.relnamespace = pgn.oid);

GRANT SELECT ON arenadata_toolkit.adb_skew_coefficients TO public;

/*
 This is part of arenadata_toolkit API for ADB Bundle.
 This function creates tables for performing adb_collect_table_stats.
 */
CREATE FUNCTION arenadata_toolkit.adb_create_tables()
RETURNS VOID
AS $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tables
					WHERE schemaname = 'arenadata_toolkit' AND tablename  = 'db_files_history')
	THEN
		EXECUTE FORMAT($fmt$CREATE TABLE arenadata_toolkit.db_files_history(
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
			collecttime TIMESTAMP WITHOUT TIME ZONE
		)
		WITH (appendonly=true, compresstype=zlib, compresslevel=9)
		DISTRIBUTED RANDOMLY
		PARTITION BY RANGE (collecttime)
		(
			PARTITION %1$I START (date %2$L) INCLUSIVE
			END (date %3$L) EXCLUSIVE
			EVERY (INTERVAL '1 month'),
			DEFAULT PARTITION default_part
		);$fmt$,
		'p' || to_char(NOW(), 'YYYYMM'),
		to_char(NOW(), 'YYYY-MM-01'),
		to_char(NOW() + interval '1 month','YYYY-MM-01'));

		/*
		 Only for admin usage.
		 */
		REVOKE ALL ON TABLE arenadata_toolkit.db_files_history FROM public;
	END IF;

	/*
	 The reason of not using CREATE TABLE IF NOT EXISTS statement, is that these tables
	 may exist from previous ADB installations. In theory there might be some custom user's grants
	 for these tables, so revoking them here may lead to a problems with an access.
	 For fresh new deployments we may set as we want.
	 */
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tables
					WHERE schemaname = 'arenadata_toolkit' AND tablename  = 'daily_operation')
	THEN
		CREATE TABLE arenadata_toolkit.daily_operation
		(
			schema_name TEXT,
			table_name TEXT,
			action TEXT,
			status TEXT,
			time BIGINT,
			processed_dttm TIMESTAMP
		)
		WITH (appendonly=true, compresstype=zlib, compresslevel=1)
		DISTRIBUTED RANDOMLY;

		/*
		 Only for admin usage.
		 */
		REVOKE ALL ON TABLE arenadata_toolkit.daily_operation FROM public;
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tables
					WHERE schemaname = 'arenadata_toolkit' AND tablename  = 'operation_exclude')
	THEN

		CREATE TABLE arenadata_toolkit.operation_exclude (schema_name TEXT)
		WITH (appendonly=true, compresstype=zlib, compresslevel=1)
		DISTRIBUTED RANDOMLY;

		/*
		 Only for admin usage.
		 */
		REVOKE ALL ON TABLE arenadata_toolkit.operation_exclude FROM public;

		INSERT INTO arenadata_toolkit.operation_exclude (schema_name)
		VALUES ('gp_toolkit'),
				('information_schema'),
				('pg_aoseg'),
				('pg_bitmapindex'),
				('pg_catalog'),
				('pg_toast');
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_tables
					WHERE schemaname = 'arenadata_toolkit' AND tablename = 'db_files_current')
	THEN
		CREATE TABLE arenadata_toolkit.db_files_current
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
			file_size BIGINT
		)
		WITH (appendonly = false) DISTRIBUTED RANDOMLY;
	END IF;

	GRANT SELECT ON TABLE arenadata_toolkit.db_files_current TO public;

	/*
	 We don't use this external web table in arenadata_toolkit extension.
	 collect_table_stats.sql deletes it in normal conditions,
	 but we drop it here as a double check.
	 */
	IF EXISTS (SELECT 1 FROM pg_tables
				WHERE schemaname = 'arenadata_toolkit' AND tablename = 'db_files')
	THEN
		DROP EXTERNAL TABLE arenadata_toolkit.db_files;
	END IF;
END$$
LANGUAGE plpgsql VOLATILE
EXECUTE ON MASTER;

/*
 Only for admin usage.
 */
REVOKE ALL ON FUNCTION arenadata_toolkit.adb_create_tables() FROM public;
