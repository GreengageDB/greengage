LOAD 'credcheck';

SET credcheck.password_change_first_login = true;
CREATE USER aaa PASSWORD 'DummY';
-- verify that credcheck_internal.force_change_password is present after user creation
SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole='aaa'::regrole AND 'credcheck_internal.force_change_password=true'=ANY(setconfig);
DROP USER aaa;

-- Enforcement when a forced role actually logs in.
-- Create a forced role and reconnect as it so that
-- credcheck_internal.force_change_password is applied at login, exactly as in
-- production (the flag comes from pg_db_role_setting, not from a manual SET).
SET credcheck.password_change_first_login = true;
CREATE USER forced_login PASSWORD 'DummY1passWORD!';
\c - forced_login;

-- Session/transaction-control statements that drivers and connection poolers
-- must be able to issue while establishing a session (pgbouncer varcache_apply,
-- pgbouncer server_reset_query = DISCARD ALL, JDBC/libpq startup SETs). These
-- must be allowed even though the role has not changed its password yet.
SET client_encoding = 'UTF8';
SET application_name = 'x';
SHOW password_encryption;
BEGIN;
COMMIT;
DISCARD ALL;

-- The flag stays armed after DISCARD ALL (its default comes from the role
-- settings), so real data access is still blocked -- and, unlike the swallowed
-- pooler-setup error, this one reaches the client.
SELECT 1;

-- DDL and other utility statements remain blocked too.
CREATE TABLE t_force (i int);

-- A forced session must not be able to clear the flag itself.
SET credcheck_internal.force_change_password = false;

-- The password change itself is allowed and clears the forced state.
ALTER USER forced_login PASSWORD 'BrandNew3passWORD!';
SELECT 1 AS after_password_change;

-- Reconnect as the superuser to clean up.
\c - postgres
DROP USER forced_login;
