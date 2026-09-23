/* gpcontrib/gg_wait_sampling/gg_wait_sampling--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gg_wait_sampling UPDATE TO '1.2'" to load this file. \quit

-- Version 1.2 drops the tmid column. The functions have OUT parameters, so
-- they must be recreated rather than replaced; the views depend on them.

DROP VIEW gg_wait_sampling_current;
DROP VIEW gg_wait_sampling_history;
DROP VIEW gg_wait_sampling_profile;

DROP FUNCTION gg_wait_sampling_get_current_segments(int4);
DROP FUNCTION gg_wait_sampling_get_current_coordinator(int4);
DROP FUNCTION gg_wait_sampling_get_current(int4);
DROP FUNCTION gg_wait_sampling_get_history_segments();
DROP FUNCTION gg_wait_sampling_get_history_coordinator();
DROP FUNCTION gg_wait_sampling_get_history();
DROP FUNCTION gg_wait_sampling_get_profile_segments();
DROP FUNCTION gg_wait_sampling_get_profile_coordinator();
DROP FUNCTION gg_wait_sampling_get_profile();

CREATE FUNCTION gg_wait_sampling_get_current_segments (
	pid int4 DEFAULT NULL,
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
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

-- Duplicates for ease calling from other extensions
CREATE FUNCTION gg_wait_sampling_get_current (
	pid int4 DEFAULT NULL,
	OUT pid int4,
	OUT event_type text,
	OUT event text,
	OUT queryid int8,
	OUT mppsessionid int4,
	OUT command_id int4,
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
	OUT segid int4
)
RETURNS SETOF record
AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_profile'
LANGUAGE C VOLATILE STRICT;

GRANT EXECUTE ON FUNCTION gg_wait_sampling_get_profile TO PUBLIC;
