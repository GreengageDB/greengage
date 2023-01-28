DROP USER IF EXISTS credtest;
DROP EXTENSION credcheck CASCADE;
CREATE EXTENSION credcheck;
LOAD 'credcheck';
SET credcheck.password_reuse_history = 2;
-- When creating user the password must be stored in the history
CREATE USER credtest WITH PASSWORD 'H8Hdre=S2';
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- fail, the credential is still in the history
ALTER USER credtest PASSWORD 'J8YuRe=6O';
-- eject the first credential from the history and add a new one
ALTER USER credtest PASSWORD 'AJ8YuRe=6O0';
SELECT rolename, password_text FROM credcheck.pg_auth_history ;
-- fail, the credential is still in the history
ALTER USER credtest PASSWORD 'J8YuRe=6O';
-- success, eject the second credential from the history and reuse the first one
ALTER USER credtest PASSWORD 'H8Hdre=S2';
-- success, the second credential has been removed from the history
ALTER USER credtest PASSWORD 'J8YuRe=6O';
-- Dropping the user must empty the record in history table
DROP USER credtest;
SELECT * FROM credcheck.pg_auth_history;
