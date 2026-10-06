-- start_matchsubs
-- m/ \(credcheck.c:\d+\)/
-- s/ \(credcheck.c:\d+\)//
-- end_matchsubs
-- start_ignore
DROP USER IF EXISTS credtest;
DROP USER IF EXISTS credtest_reader;
DROP EXTENSION IF EXISTS credcheck CASCADE;
-- end_ignore
-- Version 4.6.0 granted the password history view to PUBLIC
CREATE EXTENSION credcheck VERSION '4.6.0';
SELECT pg_password_history_reset();
SET credcheck.password_reuse_history = 2;
CREATE USER credtest WITH PASSWORD 'H8Hdre=S2';
CREATE USER credtest_reader;
-- success, a superuser can see the password history
SELECT rolename FROM pg_password_history WHERE rolename = 'credtest';
SET ROLE credtest_reader;
-- fail, the password history is not visible to other users, even if granted
SELECT rolename FROM pg_password_history WHERE rolename = 'credtest';
SELECT rolename FROM pg_password_history() WHERE rolename = 'credtest';
RESET ROLE;
-- The update revokes the view from PUBLIC
ALTER EXTENSION credcheck UPDATE TO '4.6.1';
SET ROLE credtest_reader;
-- fail, the view is not granted anymore
SELECT rolename FROM pg_password_history WHERE rolename = 'credtest';
SELECT rolename FROM pg_password_history() WHERE rolename = 'credtest';
RESET ROLE;
-- success, a superuser can still see the password history
SELECT rolename FROM pg_password_history WHERE rolename = 'credtest';
SELECT pg_password_history_reset();
DROP USER credtest_reader;
DROP USER credtest;
DROP EXTENSION credcheck CASCADE;
