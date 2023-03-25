DROP USER IF EXISTS credtest;
DROP EXTENSION credcheck CASCADE;
CREATE EXTENSION credcheck;
SELECT pg_password_history_reset();
SELECT * FROM pg_password_history WHERE rolename = 'credtest';
-- no password in the history, settings password_reuse_history
-- or password_reuse_interval are not set yet
CREATE USER credtest WITH PASSWORD 'AJ8YuRe=6O0';
SET credcheck.password_reuse_history = 1;
SET credcheck.password_reuse_interval = 365;
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest' ORDER BY password_date ;
-- Add a new password in the history and set its age to 100 days
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT pg_password_history_timestamp('credtest', now()::timestamp - '100 days'::interval);
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest' ORDER BY password_date ;
-- fail, the password is in the history for less than 1 year
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest' ORDER BY password_date ;
-- success, but the old password must be kept in the history (interval not reached)
ALTER USER credtest PASSWORD 'AJ8YuRe=6O0';
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest' ORDER BY password_date ;
-- fail, the password is still present in the history
ALTER USER credtest PASSWORD 'J8YuRe=6O';
-- Change the age of the password to exceed the 1 year interval
SELECT pg_password_history_timestamp('credtest', now()::timestamp - '380 days'::interval);
-- success, the old password present in the history has expired
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest' ORDER BY password_date ;
-- Rename user, all entries in the history table must follow the change
ALTER USER credtest RENAME TO credtest2;
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest2' ORDER BY password_date ;
-- Dropping the user must empty the record in history table
DROP USER credtest2;
SELECT * FROM pg_password_history WHERE rolename = 'credtest2';
-- Reset the password history
SELECT pg_password_history_reset();
