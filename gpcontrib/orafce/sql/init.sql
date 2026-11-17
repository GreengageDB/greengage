\set ECHO none
set client_min_messages TO error;
CREATE EXTENSION IF NOT EXISTS orafce;
-- GPDB: the public schema no longer grants CREATE to PUBLIC (upstream
-- b073c3ccd06), and dbms_pipe_session_A/B create their functions there as
-- the non-owner pipe_test_owner.
GRANT ALL ON SCHEMA public TO public;
set client_min_messages TO default;