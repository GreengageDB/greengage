LOAD 'credcheck';

SET credcheck.password_change_first_login = true;
CREATE USER aaa PASSWORD 'DummY';
-- verify that credcheck_internal.force_change_password is present after user creation
SELECT 1 FROM pg_catalog.pg_db_role_setting WHERE setrole='aaa'::regrole AND 'credcheck_internal.force_change_password=true'=ANY(setconfig);
DROP USER aaa;
