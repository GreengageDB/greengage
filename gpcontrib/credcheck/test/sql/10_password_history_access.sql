-- start_matchsubs
-- m/ \(credcheck.c:\d+\)/
-- s/ \(credcheck.c:\d+\)//
-- end_matchsubs
-- start_ignore
DROP USER IF EXISTS credtest;
DROP USER IF EXISTS credtest_reader;
DROP EXTENSION IF EXISTS credcheck CASCADE;
-- end_ignore
CREATE EXTENSION credcheck;
SELECT pg_password_history_reset();
SET credcheck.password_reuse_history = 2;
CREATE USER credtest WITH PASSWORD 'H8Hdre=S2';
CREATE USER credtest_reader;
-- success, a superuser can see the password history
SELECT rolename FROM pg_password_history WHERE rolename = 'credtest';
SET ROLE credtest_reader;
-- fail, only a superuser can see the password history, even if the view is granted
SELECT rolename FROM pg_password_history WHERE rolename = 'credtest';
SELECT rolename FROM pg_password_history() WHERE rolename = 'credtest';
RESET ROLE;
SELECT pg_password_history_reset();
DROP USER credtest_reader;
DROP USER credtest;
DROP EXTENSION credcheck CASCADE;
