-- credcheck extension for PostgreSQL
-- Copyright (c) 2021-2023 MigOps Inc - All rights reserved.
-- Copyright (c) 2023 Gilles Darold - All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION credcheck" to load this file. \quit

----
-- Remove all entries from authent failure cache.
-- Returns the number of entries removed.
----
CREATE FUNCTION pg_banned_role_reset( )
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

----
-- Remove entries of the specified user from authent failure cache.
-- Returns the number of entries removed.
----
CREATE FUNCTION pg_banned_role_reset( IN username name )
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

----
-- Look at authent failure cache entries
----
CREATE FUNCTION pg_banned_role (
	OUT roleid Oid,
	OUT failure_count integer,
	OUT banned_date timestamp
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

-- Register a view on the function for ease of use.
CREATE VIEW pg_banned_role AS
  SELECT * FROM pg_banned_role();

GRANT SELECT ON pg_banned_role TO PUBLIC;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_banned_role_reset() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_banned_role_reset(name) FROM PUBLIC;

