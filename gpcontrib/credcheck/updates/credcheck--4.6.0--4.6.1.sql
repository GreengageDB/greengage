-- credcheck extension for PostgreSQL

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION credcheck UPDATE TO '4.6.1'" to load this file. \quit

-- The password history is visible to superusers only
REVOKE SELECT ON pg_password_history FROM PUBLIC;
