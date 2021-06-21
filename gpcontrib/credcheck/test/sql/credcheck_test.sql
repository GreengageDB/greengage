LOAD 'credcheck';
ALTER SYSTEM SET credcheck.username_min_length TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_special TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_digit TO DEFAULT;
ALTER SYSTEM SET credcheck.username_contain_password TO DEFAULT;
ALTER SYSTEM SET credcheck.username_ignore_case TO DEFAULT;
ALTER SYSTEM SET credcheck.username_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.username_not_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_repeat TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_length TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_special TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_digit TO DEFAULT;
ALTER SYSTEM SET credcheck.password_contain_username TO DEFAULT;
ALTER SYSTEM SET credcheck.password_ignore_case TO DEFAULT;
ALTER SYSTEM SET credcheck.password_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.password_not_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_repeat TO DEFAULT;
--XXX
--to avoid the race condition among the reload_conf and next ALTER SYSTEM
--for now, we are adding some pg_sleep and hoping that
--postgresql SIGHUP will complete in the next 100ms
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
--username checks
--
--length must be >=2
--
ALTER SYSTEM SET credcheck.username_min_length TO 2;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS aa;
CREATE USER aa WITH PASSWORD 'dummy';
DROP USER IF EXISTS aa;
--
--min user repeat
--
ALTER SYSTEM SET credcheck.username_min_repeat TO 5;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS abbbaaaaaa;
CREATE USER abbbaaaaaa WITH PASSWORD 'dummy';
DROP USER IF EXISTS abbbaaaaaa;
--
--min special >= 1
--
ALTER SYSTEM SET credcheck.username_min_special TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS a$;
CREATE USER a$ WITH PASSWORD 'dummy';
DROP USER IF EXISTS a$;
--
--min upper >=1
--
ALTER SYSTEM SET credcheck.username_min_upper TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aA$";
CREATE USER "aA$" WITH PASSWORD 'dummy';
DROP USER IF EXISTS "aA$";
--
--min lower >=2
--
ALTER SYSTEM SET credcheck.username_min_lower TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aaA$";
CREATE USER "aaA$" WITH PASSWORD 'dummy';
DROP USER IF EXISTS "aaA$";
--
--must contain one of the characters 'a','b','c'
--
ALTER SYSTEM SET credcheck.username_contain TO 'a,b,c';
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aA$user";
CREATE USER "aA$user" WITH PASSWORD 'dummy';
DROP USER IF EXISTS auser;
--
--must not contain one of the characters 'x','z'
--
ALTER SYSTEM SET credcheck.username_not_contain TO 'x,z';
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aaA$user";
CREATE USER "aaA$user" WITH PASSWORD 'dummy';
DROP USER IF EXISTS "aaA$user";
--
--username contain password
--
ALTER SYSTEM SET credcheck.username_contain_password TO on;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aaA$usernopass";
CREATE USER "aaA$usernopass" WITH PASSWORD 'dummy';
DROP USER IF EXISTS "aaA$usernopass";
--
--ignore case while performing checks
--
ALTER SYSTEM SET credcheck.username_ignore_case TO on;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aa$user_dummy";
CREATE USER "aa$user_dummy" WITH PASSWORD 'DUMMY';
DROP USER IF EXISTS "aa$user_dummy";
--
--min digit >=1
--
ALTER SYSTEM SET credcheck.username_min_digit TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS aa;
CREATE USER aa WITH PASSWORD 'dummy';
DROP USER IF EXISTS aa;
--
--reset all settings
--
ALTER SYSTEM SET credcheck.username_min_length TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_special TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_digit TO DEFAULT;
ALTER SYSTEM SET credcheck.username_contain_password TO DEFAULT;
ALTER SYSTEM SET credcheck.username_ignore_case TO DEFAULT;
ALTER SYSTEM SET credcheck.username_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.username_not_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_repeat TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_length TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_special TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_digit TO DEFAULT;
ALTER SYSTEM SET credcheck.password_contain_username TO DEFAULT;
ALTER SYSTEM SET credcheck.password_ignore_case TO DEFAULT;
ALTER SYSTEM SET credcheck.password_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.password_not_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_repeat TO DEFAULT;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
--password checks
--
--length must be >=2
--
ALTER SYSTEM SET credcheck.password_min_length TO 2;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS aa;
CREATE USER aa WITH PASSWORD 'd';
DROP USER IF EXISTS aa;

--
--min special >= 1
--
ALTER SYSTEM SET credcheck.password_min_special TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS aa;
CREATE USER aa WITH PASSWORD 'a$';
DROP USER IF EXISTS aa;
--
--min upper >=1
--
ALTER SYSTEM SET credcheck.password_min_upper TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aa";
CREATE USER "aa" WITH PASSWORD 'aA$';
DROP USER IF EXISTS "aa";
--
--min lower >=2
--
ALTER SYSTEM SET credcheck.password_min_lower TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aa";
CREATE USER "aa" WITH PASSWORD 'aA$';
DROP USER IF EXISTS "aa";
--
--must contain one of the characters 'a','b','c'
--
ALTER SYSTEM SET credcheck.password_contain TO 'a,b,c';
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aa";
CREATE USER "aa" WITH PASSWORD 'ddaU$';
DROP USER IF EXISTS "aa";
--
--must not contain one of the characters 'x','z'
--
ALTER SYSTEM SET credcheck.password_not_contain TO 'x,z';
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aa";
CREATE USER "aa" WITH PASSWORD 'Ax$';
DROP USER IF EXISTS "aa";
--
--username contain password
--
ALTER SYSTEM SET credcheck.password_contain_username TO on;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aa";
CREATE USER "aa" WITH PASSWORD 'Ab$';
DROP USER IF EXISTS "aa";
--
--ignore case while performing checks
--
ALTER SYSTEM SET credcheck.password_ignore_case TO on;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS "aa";
CREATE USER "aa" WITH PASSWORD 'random_AA$';
DROP USER IF EXISTS "aa";
--
--min digit >=1
--
ALTER SYSTEM SET credcheck.password_min_digit TO 1;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS aa;
CREATE USER aa WITH PASSWORD 'a@1';
DROP USER IF EXISTS aa;
--
--min password repeat 2
--
ALTER SYSTEM SET credcheck.password_min_repeat TO 2;
SELECT pg_reload_conf(), pg_sleep_for('100 milliseconds'::interval);
DROP USER IF EXISTS aa;
CREATE USER aa WITH PASSWORD 'a@bbb';
DROP USER IF EXISTS aa;
--
--reset all settings
--
ALTER SYSTEM SET credcheck.username_min_length TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_special TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_digit TO DEFAULT;
ALTER SYSTEM SET credcheck.username_contain_password TO DEFAULT;
ALTER SYSTEM SET credcheck.username_ignore_case TO DEFAULT;
ALTER SYSTEM SET credcheck.username_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.username_not_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.username_min_repeat TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_length TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_special TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_upper TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_digit TO DEFAULT;
ALTER SYSTEM SET credcheck.password_contain_username TO DEFAULT;
ALTER SYSTEM SET credcheck.password_ignore_case TO DEFAULT;
ALTER SYSTEM SET credcheck.password_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.password_not_contain TO DEFAULT;
ALTER SYSTEM SET credcheck.password_min_repeat TO DEFAULT;
