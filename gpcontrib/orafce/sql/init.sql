\set ECHO none
set client_min_messages TO error;
CREATE EXTENSION IF NOT EXISTS orafce;
-- PG15 revoked the default CREATE privilege on schema public from PUBLIC.
-- The dbms_pipe session tests create helper functions in public under a
-- non-superuser role (pipe_test_owner); without this grant those CREATEs fail
-- with "permission denied for schema public", session A never sends, and
-- session B blocks forever in receive_message (the suite hangs).
GRANT ALL ON SCHEMA public TO public;
set client_min_messages TO default;