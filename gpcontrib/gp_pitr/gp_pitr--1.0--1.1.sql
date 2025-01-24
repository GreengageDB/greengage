/* gpcontrib/gp_pitr/gp_pitr--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_pitr UPDATE TO '1.1'" to load this file. \quit

DROP FUNCTION gp_create_restore_point(text);
CREATE FUNCTION gp_create_restore_point(
    IN restore_point_name text,
    OUT gp_segment_id smallint, OUT restore_lsn pg_lsn
)
    RETURNS SETOF record
AS '$libdir/gp_pitr', 'gp_create_restore_point'
LANGUAGE C VOLATILE STRICT EXECUTE ON MASTER;

COMMENT ON FUNCTION gp_create_restore_point(text) IS 'Create a named restore point on all segments';

REVOKE EXECUTE ON FUNCTION gp_create_restore_point(text) FROM public;

-- pg_switch_xlog wrapper functions to switch WAL segment files on Greengage cluster-wide
CREATE FUNCTION gp_switch_wal(
    OUT gp_segment_id smallint, OUT pg_switch_wal pg_lsn, OUT pg_walfile_name text
)
    RETURNS SETOF record
AS '$libdir/gp_pitr', 'gp_switch_wal'
LANGUAGE C VOLATILE EXECUTE ON MASTER;

COMMENT ON FUNCTION gp_switch_wal() IS 'Switch WAL segment files on all segments';

REVOKE EXECUTE ON FUNCTION gp_switch_wal() FROM public;

CREATE VIEW gp_stat_archiver AS
SELECT -1 AS gp_segment_id, * FROM pg_stat_archiver
UNION
SELECT gp_execution_segment() AS gp_segment_id, * FROM gp_dist_random('pg_stat_archiver');

GRANT SELECT ON gp_stat_archiver TO PUBLIC;
