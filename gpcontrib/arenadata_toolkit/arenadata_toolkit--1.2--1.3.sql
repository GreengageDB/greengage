/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.2--1.3.sql */

/*
 * Returns columns (table_schema, table_name) ordered by increasing vacuum time. In this
 * list, if newest_first is true, then tables that are not yet vacuumed are located first,
 * and already vacuumed - at the end, else (newest_first is false) tables that are already
 * vacuumed are located first, and tables that are not yet vacuumed are located at the end.
 */
CREATE FUNCTION arenadata_toolkit.adb_vacuum_strategy(actionname TEXT, newest_first BOOLEAN)
RETURNS TABLE (table_schema NAME, table_name NAME) AS
$func$
BEGIN
	RETURN query EXECUTE format($$
	SELECT nspname, relname
	FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON relnamespace = n.oid
		LEFT JOIN pg_catalog.pg_partition_rule ON parchildrelid = c.oid
		LEFT JOIN pg_catalog.pg_stat_last_operation ON staactionname = UPPER(%L)
			AND objid = c.oid AND classid = 'pg_catalog.pg_class'::pg_catalog.regclass
	WHERE relkind = 'r' AND relstorage != 'x' AND parchildrelid IS NULL
		AND nspname NOT IN (SELECT schema_name FROM arenadata_toolkit.operation_exclude)
	ORDER BY statime ASC NULLS %s
	$$, actionname, CASE WHEN newest_first THEN 'FIRST' ELSE 'LAST' END);
END;
$func$ LANGUAGE plpgsql STABLE EXECUTE ON MASTER;

/*
 * Only for admin usage.
 */
REVOKE ALL ON FUNCTION arenadata_toolkit.adb_vacuum_strategy(TEXT, BOOLEAN) FROM public;

/*
 * Returns columns (table_schema, table_name) ordered by increasing vacuum time.
 * In this list, tables that are not yet vacuumed are located first,
 * and already vacuumed - at the end (default strategy).
 */
CREATE FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_first(actionname TEXT)
RETURNS TABLE (table_schema NAME, table_name NAME) AS
$$
	SELECT arenadata_toolkit.adb_vacuum_strategy(actionname, true);
$$ LANGUAGE sql STABLE EXECUTE ON MASTER;

/*
 * Only for admin usage.
 */
REVOKE ALL ON FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_first(TEXT) FROM public;

/*
 * Returns columns (table_schema, table_name) ordered by increasing vacuum time.
 * In this list, tables that are already vacuumed are located first,
 * and tables that are not yet vacuumed are located at the end.
 */
CREATE FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_last(actionname TEXT)
RETURNS TABLE (table_schema NAME, table_name NAME) AS
$$
	SELECT arenadata_toolkit.adb_vacuum_strategy(actionname, false);
$$ LANGUAGE sql STABLE EXECUTE ON MASTER;

/*
 * Only for admin usage.
 */
REVOKE ALL ON FUNCTION arenadata_toolkit.adb_vacuum_strategy_newest_last(TEXT) FROM public;
