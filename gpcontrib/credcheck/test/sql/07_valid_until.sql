LOAD 'credcheck';
--
--reset all settings
--
SET credcheck.username_min_length TO DEFAULT;
SET credcheck.username_min_special TO DEFAULT;
SET credcheck.username_min_upper TO DEFAULT;
SET credcheck.username_min_upper TO DEFAULT;
SET credcheck.username_min_digit TO DEFAULT;
SET credcheck.username_contain_password TO DEFAULT;
SET credcheck.username_ignore_case TO DEFAULT;
SET credcheck.username_contain TO DEFAULT;
SET credcheck.username_not_contain TO DEFAULT;
SET credcheck.username_min_repeat TO DEFAULT;
SET credcheck.password_min_length TO DEFAULT;
SET credcheck.password_min_special TO DEFAULT;
SET credcheck.password_min_upper TO DEFAULT;
SET credcheck.password_min_upper TO DEFAULT;
SET credcheck.password_min_digit TO DEFAULT;
SET credcheck.password_contain_username TO DEFAULT;
SET credcheck.password_ignore_case TO DEFAULT;
SET credcheck.password_contain TO DEFAULT;
SET credcheck.password_not_contain TO DEFAULT;
SET credcheck.password_min_repeat TO DEFAULT;
SET credcheck.password_reuse_history = 0;
SET credcheck.password_reuse_interval = 0;

-- VALID UNTIL clause checks
SET credcheck.password_valid_until TO 4;
SET credcheck.password_valid_max TO 0;
-- the VALID UNTIL clause must be present, if not it will be added automaticaly
CREATE USER aaa PASSWORD 'DummY';
select count(*) from pg_shadow where usename = 'aaa' AND valuntil::date = (now()+'5 days'::interval)::date;
DROP USER aaa;
-- Success, the VALID UNTIL clause is present and respect the delay
CREATE USER aaa PASSWORD 'DummY' VALID UNTIL '2050-01-01 00:00:00';
-- fail, the VALID UNTIL clause does not respect the delay
ALTER USER aaa PASSWORD 'DummY2' VALID UNTIL '2022-01-01 00:00:00';
SET credcheck.password_valid_max TO 180;
-- fail, the VALID UNTIL clause can not exceed a maximum of 180 days
ALTER USER aaa PASSWORD 'DummY2' VALID UNTIL '2050-01-01 00:00:00';
-- Clear the user
DROP USER aaa;
-- fail, the VALID UNTIL clause can not exceed a maximum of 180 days
CREATE USER aaa PASSWORD 'DummY2' VALID UNTIL '2050-01-01 00:00:00';
SET credcheck.password_valid_until to 60;
SET credcheck.password_reuse_interval to 15;
SET credcheck.password_reuse_history to 4;
CREATE role aaa with login password 'password'; 
select rolname, rolvaliduntil between now() + '59 days'::interval and now() + '61 days'::interval from pg_roles WHERE rolname='aaa';
-- History must have one entry
SELECT count(*), '1' AS "expected" FROM pg_password_history ;
DROP USER aaa;

--
-- Privilege escalation regression: a non-superuser altering their own role must
-- never be elevated to superuser just because credcheck auto-injects a VALID
-- UNTIL clause. Only a pure password change on the caller's own role may be run
-- with elevated privileges.
--
CREATE USER low_priv;
SET ROLE low_priv;
-- Attempt to smuggle privileged options into a self ALTER ROLE; must not elevate
DO $$
DECLARE
	escalated boolean;
BEGIN
	BEGIN
		ALTER ROLE low_priv CREATEDB;
	EXCEPTION WHEN OTHERS THEN NULL;
	END;
    BEGIN
		ALTER ROLE low_priv CREATEROLE;
	EXCEPTION WHEN OTHERS THEN NULL;
	END;
	BEGIN
		ALTER ROLE low_priv BYPASSRLS;
	EXCEPTION WHEN OTHERS THEN NULL;
	END;
	BEGIN
		ALTER ROLE low_priv SUPERUSER;
	EXCEPTION WHEN OTHERS THEN NULL;
	END;
	BEGIN
		ALTER ROLE low_priv PASSWORD 'SomePass2' SUPERUSER;
	EXCEPTION WHEN OTHERS THEN NULL;
	END;
	SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls
		INTO escalated FROM pg_roles WHERE rolname = current_role;
	IF escalated THEN
		RAISE NOTICE 'FAIL: low_priv gained privileges';
	ELSE
		RAISE NOTICE 'PASS: no privilege escalation';
	END IF;
END $$;
-- A plain self password change must still be accepted (elevation still works)
ALTER ROLE low_priv PASSWORD 'SomePass3';
RESET ROLE;
DROP ROLE low_priv;

--
-- issue #77: credcheck.password_valid_min gives newly created roles a short
-- VALID UNTIL window (so users are forced to change their password quickly)
-- while existing roles that change their password keep the longer
-- password_valid_until window.
--
SET credcheck.password_reuse_history TO 0;
SET credcheck.password_reuse_interval TO 0;
SET credcheck.password_valid_until TO 60;
SET credcheck.password_valid_min TO 5;
SET credcheck.password_valid_max TO 90;
-- new role without VALID UNTIL: auto-set to now()+5 days (password_valid_min), not 60
CREATE USER remi PASSWORD 'DummY1';
SELECT rolname, rolvaliduntil between now() + '4 days'::interval and now() + '6 days'::interval AS min_window FROM pg_roles WHERE rolname='remi';
-- existing role changing its password without VALID UNTIL: auto-set to now()+60 days (password_valid_until)
ALTER USER remi PASSWORD 'DummY2';
SELECT rolname, rolvaliduntil between now() + '59 days'::interval and now() + '61 days'::interval AS until_window FROM pg_roles WHERE rolname='remi';
DROP USER remi;
-- fail: a new role requesting a VALID UNTIL beyond password_valid_max (90 days)
CREATE USER remi PASSWORD 'DummY1' VALID UNTIL '2999-01-01';
-- fail: a new role requesting a VALID UNTIL below password_valid_min (5 days)
CREATE USER remi PASSWORD 'DummY1' VALID UNTIL '2020-01-01';
-- password_valid_min works even when password_valid_until is disabled (0)
SET credcheck.password_valid_until TO 0;
SET credcheck.password_valid_max TO 0;
SET credcheck.password_valid_min TO 7;
CREATE USER remi PASSWORD 'DummY1';
SELECT rolname, rolvaliduntil between now() + '6 days'::interval and now() + '8 days'::interval AS min_only_window FROM pg_roles WHERE rolname='remi';
DROP USER remi;
-- non-regression: with password_valid_min explicitly disabled (0), CREATE ROLE
-- must behave exactly as before issue #77 and fall back to password_valid_until.
SET credcheck.password_valid_min TO 0;
SET credcheck.password_valid_until TO 30;
SET credcheck.password_valid_max TO 0;
-- new role without VALID UNTIL: auto-set to now()+30 days (password_valid_until), the pre-#77 behaviour
CREATE USER remi PASSWORD 'DummY1';
SELECT rolname, rolvaliduntil between now() + '29 days'::interval and now() + '31 days'::interval AS fallback_window FROM pg_roles WHERE rolname='remi';
DROP USER remi;
-- and the password_valid_until floor still applies at CREATE when min is 0
CREATE USER remi PASSWORD 'DummY1' VALID UNTIL '2020-01-01';
SET credcheck.password_valid_min TO DEFAULT;
SET credcheck.password_valid_until TO DEFAULT;
SET credcheck.password_valid_max TO DEFAULT;
