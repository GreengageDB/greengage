/* gpcontrib/gg_tables_tracking/gg_tables_tracking--1.0.sql */

DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'gg_tables_tracking')
	THEN
		CREATE SCHEMA gg_tables_tracking;
	END IF;
END$$;

GRANT USAGE ON SCHEMA gg_tables_tracking TO public;

CREATE FUNCTION gg_tables_tracking.tracking_register_db(dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_register_db' LANGUAGE C EXECUTE ON COORDINATOR;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_register_db(dbid OID) FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_unregister_db(dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_unregister_db' LANGUAGE C EXECUTE ON COORDINATOR;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_unregister_db(dbid OID) FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_register_schema(schemaname NAME, dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_register_schema' LANGUAGE C EXECUTE ON COORDINATOR;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_register_schema(schema NAME, dbid OID) FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_unregister_schema(schema NAME, dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_unregister_schema' LANGUAGE C EXECUTE ON COORDINATOR;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_unregister_schema(schema NAME, dbid OID) FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_set_relkinds(relkinds NAME, dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_set_relkinds' LANGUAGE C EXECUTE ON COORDINATOR;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_set_relkinds(relkinds NAME, dbid OID) FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_set_relams(relams NAME, dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_set_relams' LANGUAGE C EXECUTE ON COORDINATOR;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_set_relams(relams NAME, dbid OID) FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_set_snapshot_on_recovery(val BOOL, dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_set_snapshot_on_recovery' LANGUAGE C EXECUTE ON COORDINATOR;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_set_snapshot_on_recovery(val BOOL, dbid OID) FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_trigger_initial_snapshot(dbid OID DEFAULT 0)
returns TABLE(success BOOL) AS '$libdir/gg_tables_tracking',
'tracking_trigger_initial_snapshot' LANGUAGE C;

GRANT EXECUTE ON FUNCTION gg_tables_tracking.tracking_trigger_initial_snapshot(dbid OID) TO public;

CREATE FUNCTION gg_tables_tracking.tracking_is_initial_snapshot_triggered_master(dbid OID DEFAULT 0)
returns TABLE(is_triggered BOOL) AS '$libdir/gg_tables_tracking',
'tracking_is_initial_snapshot_triggered' LANGUAGE C EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION gg_tables_tracking.tracking_is_initial_snapshot_triggered_master(dbid OID) TO public;

CREATE FUNCTION gg_tables_tracking.tracking_is_initial_snapshot_triggered_segments(dbid OID DEFAULT 0)
returns TABLE(is_triggered BOOL) AS '$libdir/gg_tables_tracking',
'tracking_is_initial_snapshot_triggered' LANGUAGE C EXECUTE ON ALL segments;

GRANT EXECUTE ON FUNCTION gg_tables_tracking.tracking_is_initial_snapshot_triggered_segments(dbid OID) TO public;

CREATE FUNCTION gg_tables_tracking.tracking_is_segment_initialized()
returns TABLE(segindex INT, is_initialized BOOL) AS '$libdir/gg_tables_tracking',
'tracking_is_segment_initialized' LANGUAGE C;

REVOKE ALL ON FUNCTION gg_tables_tracking.tracking_is_segment_initialized() FROM public;

CREATE FUNCTION gg_tables_tracking.tracking_track_version()
returns BIGINT AS '$libdir/gg_tables_tracking',
'tracking_track_version' LANGUAGE C STABLE;

-- Shouldn't be called explicitly
GRANT EXECUTE ON FUNCTION gg_tables_tracking.tracking_track_version() TO public;

CREATE FUNCTION gg_tables_tracking.tracking_get_track_master(version BIGINT)
RETURNS TABLE(relid OID, relname NAME, relfilenode OID, size BIGINT, state "char", segid INT,
relnamespace OID, relkind "char", relam OID) AS '$libdir/gg_tables_tracking',
'tracking_get_track' LANGUAGE C EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION gg_tables_tracking.tracking_get_track_master(version BIGINT) TO public;

CREATE FUNCTION gg_tables_tracking.tracking_get_track_segments(version BIGINT)
RETURNS TABLE(relid OID, relname NAME, relfilenode OID, size BIGINT, state "char", segid INT,
relnamespace OID, relkind "char", relam OID) AS '$libdir/gg_tables_tracking',
'tracking_get_track' LANGUAGE C EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION gg_tables_tracking.tracking_get_track_segments(version BIGINT) TO public;

CREATE VIEW gg_tables_tracking.tables_track AS
SELECT t.*, 
    coalesce(
        c.oid,           -- toast table parent
        i.indrelid,      -- index parent
        vm.relid,        -- visimap parent
        blk.relid,       -- block directory parent
        seg.relid,       -- segment parent
        inh.inhparent    -- partition parent
    ) AS parent_relid
FROM gg_tables_tracking.tracking_get_track_master(gg_tables_tracking.tracking_track_version()
) AS t
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
LEFT JOIN pg_catalog.pg_inherits AS inh
    ON inh.inhrelid = t.relid AND t.relkind = 'r'
UNION ALL
SELECT t.*, coalesce(
        c.oid,
        i.indrelid,
        vm.relid,
        blk.relid,
        seg.relid,
        inh.inhparent
    ) AS parent_relid
FROM gg_tables_tracking.tracking_get_track_segments(gg_tables_tracking.tracking_track_version()
) AS t
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
LEFT JOIN pg_catalog.pg_inherits AS inh
    ON inh.inhrelid = t.relid AND t.relkind = 'r';

GRANT SELECT ON gg_tables_tracking.tables_track TO public;

CREATE VIEW gg_tables_tracking.is_initial_snapshot_triggered AS
SELECT CASE
    WHEN bool_and(is_triggered) THEN 1 
    ELSE NULL 
END AS is_triggered
FROM (
    SELECT is_triggered FROM gg_tables_tracking.tracking_is_initial_snapshot_triggered_master()
    UNION ALL
    SELECT is_triggered FROM gg_tables_tracking.tracking_is_initial_snapshot_triggered_segments()
) t;

GRANT SELECT ON gg_tables_tracking.is_initial_snapshot_triggered TO public;

CREATE FUNCTION gg_tables_tracking.wait_for_worker_initialize()
RETURNS SETOF BOOLEAN AS '$libdir/gg_tables_tracking.so', 'wait_for_worker_initialize'
LANGUAGE C EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION gg_tables_tracking.wait_for_worker_initialize() TO public;
