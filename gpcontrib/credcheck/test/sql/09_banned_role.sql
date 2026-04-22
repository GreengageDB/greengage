-- start_matchsubs
-- m/ \(credcheck.c:\d+\)/
-- s/ \(credcheck.c:\d+\)//
-- end_matchsubs
-- start_ignore
DROP USER IF EXISTS credtest;
DROP EXTENSION IF EXISTS credcheck CASCADE;
-- end_ignore
CREATE EXTENSION credcheck;
SELECT pg_banned_role_reset();
CREATE USER credtest WITH PASSWORD 'H8Hdre=S2';
-- start_ignore
\! sed -i '/credtest/d' $MASTER_DATA_DIRECTORY/pg_hba.conf;
\! echo 'host	all	credtest	samehost	md5' | tee -a $MASTER_DATA_DIRECTORY/pg_hba.conf;
\! gpconfig -c credcheck.max_auth_failure -v 3;
\! gpstop -u;
-- end_ignore
\! PGPASSWORD='J8YuRe=6O' psql -h localhost -U credtest -d regression -w
\! PGPASSWORD='J8YuRe=6O' psql -h localhost -U credtest -d regression -w
\! PGPASSWORD='J8YuRe=6O' psql -h localhost -U credtest -d regression -w
SELECT rolname, failure_count FROM pg_banned_role;
SELECT pg_banned_role_reset('credtest');
SELECT rolname, failure_count FROM pg_banned_role;
\! PGPASSWORD='J8YuRe=6O' psql -h localhost -U credtest -d regression -w
-- start_ignore
\! gpconfig -r credcheck.max_auth_failure;
\! gpstop -u;
-- end_ignore
SELECT pg_banned_role_reset();
DROP USER credtest;
DROP EXTENSION credcheck CASCADE;
