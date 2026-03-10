/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.7--1.8.sql */

CREATE OR REPLACE VIEW arenadata_toolkit.__db_files_current AS
SELECT
	c.oid AS oid,
	c.relname AS table_name,
	n.nspname AS table_schema,
	c.relkind AS type,
	c.relstorage AS storage,
	d.datname AS table_database,
	t.spcname AS table_tablespace,
	dbf.segindex AS content,
	dbf.segment_preferred_role AS segment_preferred_role,
	dbf.hostname AS hostname,
	dbf.address AS address,
	dbf.full_path AS file,
	dbf.size AS file_size,
	dbf.modified_dttm AS modifiedtime,
	dbf.changed_dttm AS changedtime,
	CASE
		WHEN 'pg_default' = t.spcname THEN gpconf.datadir || '/base'
		WHEN 'pg_global' = t.spcname THEN gpconf.datadir || '/global'
		ELSE (SELECT pg_tablespace_location(oid)
			  FROM gp_dist_random('pg_catalog.pg_tablespace')
			  WHERE oid = t.oid and gp_segment_id = dbf.segindex)
		END AS tablespace_location
FROM arenadata_toolkit.__db_segment_files dbf
LEFT JOIN pg_class c ON c.oid = dbf.reloid
LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_tablespace t ON dbf.tablespace_oid = t.oid
LEFT JOIN pg_database d ON dbf.datoid = d.oid
LEFT JOIN gp_segment_configuration gpconf ON dbf.dbid = gpconf.dbid;
