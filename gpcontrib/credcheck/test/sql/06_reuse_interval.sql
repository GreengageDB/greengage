DROP USER IF EXISTS credtest;
DROP EXTENSION credcheck CASCADE;
CREATE EXTENSION credcheck;
-- no password in the history, the extension has not been loaded
CREATE USER credtest WITH PASSWORD 'AJ8YuRe=6O0';
LOAD 'credcheck';
SET credcheck.password_reuse_history = 1;
SET credcheck.password_reuse_interval = 365;
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- Add a new password in the history and set its age to 100 days
ALTER USER credtest PASSWORD 'J8YuRe=6O';
UPDATE credcheck.pg_auth_history SET password_date = now() - '100 days'::interval;
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- fail, the password is in the history for less than 1 year
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- success, but the old password must be kept in the history (interval not reached)
ALTER USER credtest PASSWORD 'AJ8YuRe=6O0';
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- fail, the password is still present in the history
ALTER USER credtest PASSWORD 'J8YuRe=6O';
-- Change the age of the password to exceed the 1 year interval
UPDATE credcheck.pg_auth_history SET password_date = now() - '380 days'::interval;
-- success, the old password present in the history has expired
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- Rename user, all entries in the history table must follow the change
ALTER USER credtest RENAME TO credtest2;
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- Dropping the user must empty the record in history table
DROP USER credtest2;
SELECT * FROM credcheck.pg_auth_history;
