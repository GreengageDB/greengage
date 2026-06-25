-- credcheck extension for PostgreSQL
-- Copyright (c) 2024-2026 HexaCluster Corp - All rights reserved.

----
-- Check a candidate password (and the associated username) against the
-- configured credcheck username/password policy without creating or
-- altering a role. Raises the same error as CREATE/ALTER ROLE would when
-- the policy is violated, and returns true when the password is accepted.
-- Note: the password reuse policy is not evaluated here
----
CREATE FUNCTION pg_check_password( IN username name, IN password text )
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;
