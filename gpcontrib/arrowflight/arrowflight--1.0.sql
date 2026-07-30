/* gpcontrib/arrowflight/arrowflight--1.0.sql
 *
 * Arrow Flight SQL foreign data wrapper objects.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION arrowflight" to load this file. \quit

CREATE FUNCTION arrowflight_build_info()
RETURNS text
AS 'MODULE_PATHNAME', 'arrowflight_build_info'
LANGUAGE C STABLE;

GRANT EXECUTE ON FUNCTION arrowflight_build_info() TO PUBLIC;

CREATE FUNCTION flightsql_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'flightsql_fdw_handler'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION flightsql_fdw_handler() FROM PUBLIC;

CREATE FUNCTION flightsql_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'flightsql_fdw_validator'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION flightsql_fdw_validator(text[], oid) FROM PUBLIC;

CREATE FOREIGN DATA WRAPPER flightsql_fdw
    HANDLER flightsql_fdw_handler
    VALIDATOR flightsql_fdw_validator
    OPTIONS (mpp_execute 'all segments');
