-- The statements of a query string holding several statements must be checked
-- as well as a single statement
-- start_ignore
DROP USER IF EXISTS credtest;
DROP USER IF EXISTS shrt;
DROP USER IF EXISTS credtest_until;
DROP USER IF EXISTS credtest_first;
DROP USER IF EXISTS credtest_until_do;
DROP USER IF EXISTS credtest_first_do;
DROP EXTENSION IF EXISTS credcheck CASCADE;
-- end_ignore
CREATE EXTENSION credcheck;
SELECT pg_password_history_reset();
SET credcheck.password_reuse_history = 2;
CREATE USER credtest WITH PASSWORD 'H8Hdre=S2';
RESET credcheck.password_reuse_history;
-- fail, the credential is still in the history
\! PGOPTIONS='-c credcheck.password_reuse_history=2' psql -X -d postgres -c "ALTER USER credtest PASSWORD 'H8Hdre=S2'; SELECT 1"
\! PGOPTIONS='-c credcheck.password_reuse_history=2' psql -X -d postgres -c "SELECT 1; ALTER USER credtest PASSWORD 'H8Hdre=S2'"
-- fail, the password change is not allowed
\! PGOPTIONS='-c credcheck.disallow_change_password=on' psql -X -d postgres -c "ALTER USER credtest PASSWORD 'J8YuRe=6O'; SELECT 1"
-- fail, the username is too short
\! PGOPTIONS='-c credcheck.username_min_length=10' psql -X -d postgres -c "CREATE USER shrt; SELECT 1"
SELECT count(*) FROM pg_roles WHERE rolname = 'shrt';
-- the expiration date must be set
\! PGOPTIONS='-c credcheck.password_valid_until=30' psql -X -d postgres -c "CREATE USER credtest_until PASSWORD 'J8YuRe=6O'; SELECT 1"
SELECT rolvaliduntil IS NOT NULL FROM pg_roles WHERE rolname = 'credtest_until';
-- the password change at first login must be forced
\! PGOPTIONS='-c credcheck.password_change_first_login=on' psql -X -d postgres -c "CREATE USER credtest_first PASSWORD 'J8YuRe=6O'; SELECT 1"
SELECT 1 FROM pg_catalog.pg_db_role_setting JOIN pg_catalog.pg_roles ON oid = setrole WHERE rolname='credtest_first' AND 'credcheck_internal.force_change_password=true'=ANY(setconfig);
-- The statements run by a DO block must be checked as well
SET credcheck.password_reuse_history = 2;
-- fail, the credential is still in the history
DO $$ BEGIN ALTER USER credtest PASSWORD 'H8Hdre=S2'; END $$;
RESET credcheck.password_reuse_history;
SET credcheck.username_min_length = 10;
-- fail, the username is too short
DO $$ BEGIN CREATE USER shrt; END $$;
RESET credcheck.username_min_length;
SELECT count(*) FROM pg_roles WHERE rolname = 'shrt';
SET credcheck.password_valid_until = 30;
-- the expiration date must be set
DO $$ BEGIN CREATE USER credtest_until_do PASSWORD 'J8YuRe=6O'; END $$;
RESET credcheck.password_valid_until;
SELECT rolvaliduntil IS NOT NULL FROM pg_roles WHERE rolname = 'credtest_until_do';
SET credcheck.password_change_first_login = on;
-- the password change at first login must be forced
DO $$ BEGIN CREATE USER credtest_first_do PASSWORD 'J8YuRe=6O'; END $$;
RESET credcheck.password_change_first_login;
SELECT 1 FROM pg_catalog.pg_db_role_setting JOIN pg_catalog.pg_roles ON oid = setrole WHERE rolname='credtest_first_do' AND 'credcheck_internal.force_change_password=true'=ANY(setconfig);
DROP USER credtest;
DROP USER IF EXISTS shrt;
DROP USER credtest_until;
DROP USER credtest_first;
DROP USER credtest_until_do;
DROP USER credtest_first_do;
SELECT pg_password_history_reset();
DROP EXTENSION credcheck CASCADE;
