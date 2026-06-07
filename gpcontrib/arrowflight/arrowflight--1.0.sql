/* gpcontrib/arrowflight/arrowflight--1.0.sql
 *
 * FDW-only Arrow Flight extension objects.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION arrowflight" to load this file. \quit

CREATE FUNCTION arrowflight_build_info()
RETURNS text
AS 'MODULE_PATHNAME', 'arrowflight_build_info'
LANGUAGE C STABLE;

GRANT EXECUTE ON FUNCTION arrowflight_build_info() TO PUBLIC;

CREATE FUNCTION arrowflight_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'arrowflight_fdw_handler'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION arrowflight_fdw_handler() FROM PUBLIC;

CREATE FUNCTION arrowflight_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'arrowflight_fdw_validator'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION arrowflight_fdw_validator(text[], oid) FROM PUBLIC;

CREATE FOREIGN DATA WRAPPER arrowflight_fdw
    HANDLER arrowflight_fdw_handler
    VALIDATOR arrowflight_fdw_validator
    OPTIONS (mpp_execute 'all segments');
