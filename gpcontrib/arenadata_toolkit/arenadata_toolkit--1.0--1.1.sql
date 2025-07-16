/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.0--1.1.sql */

ALTER FUNCTION arenadata_toolkit.adb_get_relfilenodes(tablespace_oid OID) ROWS 30000000;

CREATE OR REPLACE FUNCTION arenadata_toolkit.adb_relation_storage_size_on_segments(
	IN reloid OID, IN forkName TEXT default 'main',
	OUT gp_segment_id INT, OUT size BIGINT)
AS 'SELECT pg_catalog.gp_execution_segment() AS gp_segment_id, *
	FROM arenadata_toolkit.adb_relation_storage_size($1, $2)'
LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION arenadata_toolkit.adb_relation_storage_size_on_segments(OID, TEXT) TO public;

CREATE OR REPLACE VIEW arenadata_toolkit.adb_skew_coefficients
AS
WITH recursive cte AS (
	SELECT
		oid AS id,
		tables_size.gp_segment_id AS seg_id,
		tables_size.size AS size
	FROM pg_catalog.pg_class,
		 arenadata_toolkit.adb_relation_storage_size_on_segments(oid) AS tables_size
	WHERE relkind = 'r' AND relstorage != 'x' AND
		relnamespace IN (SELECT aunoid FROM gp_toolkit.__gp_user_namespaces)
	UNION ALL
	SELECT inhparent AS id, seg_id, size
	FROM cte
	LEFT JOIN pg_catalog.pg_inherits ON inhrelid = id
	WHERE inhparent != 0
), tables_size_by_segments AS (
	SELECT id, sum(size) AS size
	FROM cte
	GROUP BY id, seg_id
), skew AS (
	SELECT
		id AS skewoid,
		stddev(size) AS skewdev,
		avg(size) AS skewmean
	FROM tables_size_by_segments
	GROUP BY id
)
SELECT
	skew.skewoid AS skcoid,
	pgn.nspname  AS skcnamespace,
	pgc.relname  AS skcrelname,
	CASE WHEN skewdev > 0 THEN skewdev/skewmean * 100.0 ELSE 0 END AS skccoeff
FROM skew
JOIN pg_catalog.pg_class pgc ON (skew.skewoid = pgc.oid)
JOIN pg_catalog.pg_namespace pgn ON (pgc.relnamespace = pgn.oid);

GRANT SELECT ON arenadata_toolkit.adb_skew_coefficients TO public;
