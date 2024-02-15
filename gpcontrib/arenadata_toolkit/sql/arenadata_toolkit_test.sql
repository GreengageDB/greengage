----------------------------------------------------------------------------------------------------------
-- test helpers
----------------------------------------------------------------------------------------------------------
SET client_min_messages=WARNING;
-- start_matchsubs
-- m/(.*)prt_p\d{6}/
-- s/(.*)prt_p\d{6}/$1prt_pYYYYMM/
-- m/(.*)backup_\d{8}t\d{6}/
-- s/(.*)backup_\d{8}t\d{6}/$1backup_YYYYMMDDtHHMMSS/
-- end_matchsubs
-- function that mocks manual installation of arenadata_toolkit from bundle
CREATE FUNCTION mock_manual_installation() RETURNS VOID AS $$
BEGIN
	CREATE SCHEMA arenadata_toolkit;
	EXECUTE 'GRANT ALL ON SCHEMA arenadata_toolkit to ' || CURRENT_USER;
	EXECUTE FORMAT($fmt$CREATE TABLE arenadata_toolkit.db_files_history(collecttime TIMESTAMP WITHOUT TIME ZONE)
			WITH (appendonly=true, compresstype=zlib, compresslevel=9) DISTRIBUTED RANDOMLY
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
	CREATE TABLE arenadata_toolkit.daily_operation(field INT) WITH (appendonly=true) DISTRIBUTED RANDOMLY;
	CREATE TABLE arenadata_toolkit.operation_exclude(schema_name TEXT) WITH (appendonly=true) DISTRIBUTED RANDOMLY;
	CREATE EXTERNAL WEB TABLE arenadata_toolkit.db_files (field TEXT) EXECUTE 'echo 1' FORMAT 'TEXT';
END;
$$ LANGUAGE plpgsql;

-- view that returns information about tables that belongs to the arenadata_toolkit schema (and schema itself)
CREATE VIEW toolkit_objects_info AS
	SELECT oid as objid, 'schema'  as objtype, nspname as objname,
		'-'::"char" as objstorage,
		replace(nspacl::text, CURRENT_USER, 'owner') AS objacl -- replace current_user to static string, to prevent test flakiness
		FROM pg_namespace WHERE nspname = 'arenadata_toolkit'
	UNION
	SELECT oid as objid, 'table' as objtype, relname as objname,
		relstorage as objstorage,
		replace(relacl::text, CURRENT_USER, 'owner') AS objacl -- replace current_user to static string, to prevent test flakiness
		FROM pg_class WHERE relname IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'arenadata_toolkit')
	UNION
	SELECT oid as objid, 'proc' as objtype, proname as objname,
		'-'::"char" as objstorage,
		replace(proacl::text, CURRENT_USER, 'owner') AS objacl -- replace current_user to static string, to prevent test flakiness
		FROM pg_proc WHERE pronamespace = (SELECT oid from pg_namespace WHERE nspname = 'arenadata_toolkit')
	;
----------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
-- test case 1: arenadata_toolkit was installed
----------------------------------------------------------------------------------------------------------

-- setup
SELECT mock_manual_installation();

-- check that created toolkit objects doesn't depend on extension
SELECT * FROM pg_depend d JOIN toolkit_objects_info objs ON d.objid = objs.objid AND d.deptype='e';

-- show toolkit objects (and their grants) that belongs to arenadata_toolkit schema
SELECT objname, objtype, objstorage, objacl FROM toolkit_objects_info ORDER BY objname;

CREATE EXTENSION arenadata_toolkit;
SELECT arenadata_toolkit.adb_create_tables();

-- show toolkit objects (and their grants) that belongs to arenadata_toolkit schema after creating
-- extension and calling adb_create_tables
SELECT objname, objtype, objstorage, objacl FROM toolkit_objects_info ORDER BY objname;

-- check that toolkit objects now depends on extension
SELECT objname, objtype, extname, deptype FROM pg_depend d JOIN
	toolkit_objects_info objs ON d.objid = objs.objid JOIN
	pg_extension e ON d.refobjid = e.oid
WHERE d.deptype = 'e' AND e.extname = 'arenadata_toolkit' ORDER BY objname;

DROP EXTENSION arenadata_toolkit;
DROP SCHEMA arenadata_toolkit cascade;

----------------------------------------------------------------------------------------------------------
-- test case 2: arenadata_toolkit wasn't installed
----------------------------------------------------------------------------------------------------------

CREATE EXTENSION arenadata_toolkit;
SELECT arenadata_toolkit.adb_create_tables();

-- show toolkit objects (and their grants) that belongs to arenadata_toolkit schema after creating
-- extension and calling adb_create_tables
SELECT objname, objtype, objstorage, objacl FROM toolkit_objects_info ORDER BY objname;

-- check that toolkit objects now depends on extension
SELECT objname, objtype, extname, deptype FROM pg_depend d JOIN
	toolkit_objects_info objs ON d.objid = objs.objid JOIN
	pg_extension e ON d.refobjid = e.oid
WHERE d.deptype = 'e' AND e.extname = 'arenadata_toolkit' ORDER BY objname;

DROP EXTENSION arenadata_toolkit;
DROP SCHEMA arenadata_toolkit cascade;

DROP FUNCTION mock_manual_installation();
DROP VIEW toolkit_objects_info;
RESET client_min_messages;
