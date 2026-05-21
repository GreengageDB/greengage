/* gpcontrib/gg_wait_sampling/gg_wait_sampling--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gg_wait_sampling" to load this file. \quit

CREATE FUNCTION gg_wait_sampling_get_current_segments (
	pid int4 DEFAULT NULL,
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_current'
LANGUAGE C VOLATILE CALLED ON NULL INPUT EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_current_segments TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_get_current_coordinator (
	pid int4 DEFAULT NULL,
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_current'
LANGUAGE C VOLATILE CALLED ON NULL INPUT EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_current_coordinator TO PUBLIC;

CREATE VIEW gg_wait_sampling_current AS
	SELECT * FROM gg_wait_sampling_get_current_coordinator()
	UNION ALL
	SELECT * FROM gg_wait_sampling_get_current_segments();

GRANT SELECT ON gg_wait_sampling_current TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_get_history_segments (
	OUT pid int4,
	OUT ts timestamptz,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_history'
LANGUAGE C VOLATILE STRICT EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_history_segments TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_get_history_coordinator (
	OUT pid int4,
	OUT ts timestamptz,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_history'
LANGUAGE C VOLATILE STRICT EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_history_coordinator TO PUBLIC;

CREATE VIEW gg_wait_sampling_history AS
	SELECT * FROM gg_wait_sampling_get_history_coordinator()
	UNION ALL
	SELECT * FROM gg_wait_sampling_get_history_segments();

GRANT SELECT ON gg_wait_sampling_history TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_get_profile_segments (
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT count int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_profile'
LANGUAGE C VOLATILE STRICT EXECUTE ON ALL SEGMENTS;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_profile_segments TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_get_profile_coordinator (
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT count int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_profile'
LANGUAGE C VOLATILE STRICT EXECUTE ON COORDINATOR;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_profile_coordinator TO PUBLIC;

CREATE VIEW gg_wait_sampling_profile AS
	SELECT * FROM gg_wait_sampling_get_profile_coordinator()
	UNION ALL
	SELECT * FROM gg_wait_sampling_get_profile_segments();

GRANT SELECT ON gg_wait_sampling_profile TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_reset_profile_segments()
RETURNS SETOF BOOL
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_reset_profile'
LANGUAGE C VOLATILE STRICT EXECUTE ON ALL SEGMENTS;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION gg_wait_sampling_reset_profile_segments() FROM PUBLIC;

CREATE FUNCTION gg_wait_sampling_reset_profile_coordinator()
RETURNS SETOF BOOL
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_reset_profile'
LANGUAGE C VOLATILE STRICT EXECUTE ON COORDINATOR;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION gg_wait_sampling_reset_profile_coordinator() FROM PUBLIC;

CREATE VIEW gg_wait_sampling_reset_profile AS
	SELECT -1 AS gp_segment_id, gg_wait_sampling_reset_profile_coordinator
	AS done FROM gg_wait_sampling_reset_profile_coordinator()
	UNION ALL
	SELECT gp_execution_segment(), gg_wait_sampling_reset_profile_segments
	AS done FROM gg_wait_sampling_reset_profile_segments();

REVOKE ALL ON gg_wait_sampling_reset_profile FROM PUBLIC;

-- Duplicates for ease calling from other extensions
CREATE FUNCTION gg_wait_sampling_get_current (
	pid int4 DEFAULT NULL,
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_current'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_current TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_get_history (
	OUT pid int4,
	OUT ts timestamptz,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_history'
LANGUAGE C VOLATILE STRICT;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_history TO PUBLIC;

CREATE FUNCTION gg_wait_sampling_get_profile (
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT count int8,
	OUT mppsessionid int4,
	OUT command_id int4,
	OUT tmid int4,
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_profile'
LANGUAGE C VOLATILE STRICT;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_profile TO PUBLIC;
