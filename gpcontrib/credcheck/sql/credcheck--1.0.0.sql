-- credcheck extension for PostgreSQL
-- Copyright (c) 2021-2023 MigOps Inc - All rights reserved.

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION credcheck" to load this file. \quit

CREATE SCHEMA credcheck;

----
-- Remove all entries from password history.
-- Returns the number of entries removed.
----
CREATE FUNCTION pg_password_history_reset( )
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE;

----
-- Remove entries of the specified user from password history.
-- Returns the number of entries removed.
----
CREATE FUNCTION pg_password_history_reset( IN username name )
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

----
-- Look at password history entries
----
CREATE FUNCTION pg_password_history (
	OUT rolename name,
	OUT password_date timestamp with time zone,
	OUT password_hash text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

-- Register a view on the function for ease of use.
CREATE VIEW pg_password_history AS
  SELECT * FROM pg_password_history();

----
-- Change password creation timestamp for all entries of the specified
-- user in the password history. Proposed for testing purpose only.
-- Returns the number of entries changed.
----
CREATE FUNCTION pg_password_history_timestamp( IN username name, IN new_timestamp timestamp with time zone)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

GRANT SELECT ON pg_password_history TO PUBLIC;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_password_history_reset() FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_password_history_reset(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_password_history_timestamp(name, timestamp with time zone) FROM PUBLIC;

