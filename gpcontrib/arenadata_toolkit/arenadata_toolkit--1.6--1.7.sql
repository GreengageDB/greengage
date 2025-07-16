/* gpcontrib/arenadata_toolkit/arenadata_toolkit--1.6--1.7.sql */

CREATE FUNCTION arenadata_toolkit.tracking_register_db(dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_register_db' LANGUAGE C EXECUTE ON MASTER;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_register_db(dbid OID) FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_unregister_db(dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_unregister_db' LANGUAGE C EXECUTE ON MASTER;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_unregister_db(dbid OID) FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_register_schema(schemaname NAME, dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_register_schema' LANGUAGE C EXECUTE ON master;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_register_schema(schema NAME, dbid OID) FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_unregister_schema(schema NAME, dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_unregister_schema' LANGUAGE C EXECUTE ON master;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_unregister_schema(schema NAME, dbid OID) FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_set_relkinds(relkinds NAME, dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_set_relkinds' LANGUAGE C EXECUTE ON master;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_set_relkinds(relkinds NAME, dbid OID) FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_set_relstorages(relstorages NAME, dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_set_relstorages' LANGUAGE C EXECUTE ON master;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_set_relstorages(relstorages NAME, dbid OID) FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_set_snapshot_on_recovery(val BOOL, dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_set_snapshot_on_recovery' LANGUAGE C EXECUTE ON master;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_set_snapshot_on_recovery(val BOOL, dbid OID) FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_trigger_initial_snapshot(dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_trigger_initial_snapshot' LANGUAGE C;

GRANT EXECUTE ON FUNCTION arenadata_toolkit.tracking_trigger_initial_snapshot(dbid OID) TO public;

CREATE FUNCTION arenadata_toolkit.tracking_is_initial_snapshot_triggered_master(dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_is_initial_snapshot_triggered' LANGUAGE C EXECUTE ON master;

GRANT EXECUTE ON FUNCTION arenadata_toolkit.tracking_is_initial_snapshot_triggered_master(dbid OID) TO public;

CREATE FUNCTION arenadata_toolkit.tracking_is_initial_snapshot_triggered_segments(dbid OID DEFAULT 0)
returns BOOL AS '$libdir/arenadata_toolkit',
'tracking_is_initial_snapshot_triggered' LANGUAGE C EXECUTE ON ALL segments;

GRANT EXECUTE ON FUNCTION arenadata_toolkit.tracking_is_initial_snapshot_triggered_segments(dbid OID) TO public;

CREATE FUNCTION arenadata_toolkit.tracking_is_segment_initialized()
returns TABLE(segindex INT, is_initialized BOOL) AS '$libdir/arenadata_toolkit',
'tracking_is_segment_initialized' LANGUAGE C;

REVOKE ALL ON FUNCTION arenadata_toolkit.tracking_is_segment_initialized() FROM public;

CREATE FUNCTION arenadata_toolkit.tracking_track_version()
returns BIGINT AS '$libdir/arenadata_toolkit',
'tracking_track_version' LANGUAGE C STABLE EXECUTE ON MASTER;

-- Shouldn't be called explicitly
GRANT EXECUTE ON FUNCTION arenadata_toolkit.tracking_track_version() TO public;

CREATE FUNCTION arenadata_toolkit.tracking_get_track_master(version BIGINT)
RETURNS TABLE(relid OID, relname NAME, relfilenode OID, size BIGINT, state "char", segid INT,
relnamespace OID, relkind "char", relstorage "char") AS '$libdir/arenadata_toolkit',
'tracking_get_track' LANGUAGE C EXECUTE ON MASTER;

GRANT EXECUTE ON FUNCTION arenadata_toolkit.tracking_get_track_master(version BIGINT) TO public;

CREATE FUNCTION arenadata_toolkit.tracking_get_track_segments(version BIGINT)
RETURNS TABLE(relid OID, relname NAME, relfilenode OID, size BIGINT, state "char", segid INT,
relnamespace OID, relkind "char", relstorage "char") AS '$libdir/arenadata_toolkit',
'tracking_get_track' LANGUAGE C EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION arenadata_toolkit.tracking_get_track_segments(version BIGINT) TO public;

CREATE VIEW arenadata_toolkit.tables_track AS
SELECT t.*, coalesce(c.oid, i.indrelid, vm.relid, blk.relid, seg.relid) AS parent_relid
FROM arenadata_toolkit.tracking_get_track_master(arenadata_toolkit.tracking_track_version()) AS t
LEFT JOIN pg_class AS c
    ON c.reltoastrelid = t.relid AND t.relkind = 't'
LEFT JOIN pg_index AS i
    ON i.indexrelid = t.relid AND t.relkind = 'i'
LEFT JOIN pg_catalog.pg_appendonly AS vm
    ON vm.visimaprelid = t.relid AND t.relkind = 'M'
LEFT JOIN pg_catalog.pg_appendonly AS blk
    ON blk.blkdirrelid = t.relid AND t.relkind = 'b'
LEFT JOIN pg_catalog.pg_appendonly AS seg
    ON seg.segrelid = t.relid AND t.relkind = 'o'
UNION ALL
SELECT t.*, coalesce(c.oid, i.indrelid, vm.relid, blk.relid, seg.relid) AS parent_relid
FROM arenadata_toolkit.tracking_get_track_segments(arenadata_toolkit.tracking_track_version()) AS t
LEFT JOIN pg_class AS c
    ON c.reltoastrelid = t.relid AND t.relkind = 't'
LEFT JOIN pg_index AS i
    ON i.indexrelid = t.relid AND t.relkind = 'i'
LEFT JOIN pg_catalog.pg_appendonly AS vm
    ON vm.visimaprelid = t.relid AND t.relkind = 'M'
LEFT JOIN pg_catalog.pg_appendonly AS blk
    ON blk.blkdirrelid = t.relid AND t.relkind = 'b'
LEFT JOIN pg_catalog.pg_appendonly AS seg
    ON seg.segrelid = t.relid AND t.relkind = 'o';

GRANT SELECT ON arenadata_toolkit.tables_track TO public;

CREATE VIEW arenadata_toolkit.is_initial_snapshot_triggered AS
SELECT CASE
WHEN TRUE = ALL(select arenadata_toolkit.tracking_is_initial_snapshot_triggered_segments())
AND arenadata_toolkit.tracking_is_initial_snapshot_triggered_master() 
THEN 1 ELSE NULL END AS is_triggered;

GRANT SELECT ON arenadata_toolkit.is_initial_snapshot_triggered TO public;
