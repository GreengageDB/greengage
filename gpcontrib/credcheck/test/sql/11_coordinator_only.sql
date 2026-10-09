-- In Greengage, the password history and the banned roles are kept in the
-- shared memory of the coordinator only.
SET credcheck.password_reuse_history = 2;
CREATE USER cc_coord1 WITH PASSWORD 'H8Hdre=S2';
CREATE USER cc_coord2 WITH PASSWORD 'J8YuRe=6O';
CREATE TABLE cc_roles (u name) DISTRIBUTED BY (u);
INSERT INTO cc_roles VALUES ('cc_coord1'), ('cc_coord2'), ('cc_coord3');
-- The history is read on the coordinator, even when joined with a
-- distributed table
SELECT r.u, count(h.rolename) FROM cc_roles r
  LEFT JOIN pg_password_history h ON h.rolename = r.u
  GROUP BY r.u ORDER BY r.u;
-- The history and the banned roles can't be changed on segments
SELECT pg_password_history_reset(u) FROM cc_roles;
SELECT pg_password_history_timestamp(u, now()) FROM cc_roles;
SELECT pg_banned_role_reset(u) FROM cc_roles;
-- The password policy is checked on the coordinator only too
SELECT pg_check_password(u, 'H8Hdre=S2') FROM cc_roles;
-- and not by entry db QEs, which don't see the settings of the session
INSERT INTO cc_roles SELECT rolname FROM pg_authid
  WHERE rolname = 'cc_coord1' AND pg_check_password(rolname, 'H8Hdre=S2');
-- They can be executed on the coordinator
SELECT pg_password_history_reset('cc_coord1');
SELECT pg_check_password('cc_coord3', 'H8Hdre=S2');
SELECT r.u, count(h.rolename) FROM cc_roles r
  LEFT JOIN pg_password_history h ON h.rolename = r.u
  GROUP BY r.u ORDER BY r.u;
-- The password hashes are visible by superusers only, the banned roles by
-- pg_read_all_stats too
SET ROLE cc_coord1;
SELECT count(*) FROM pg_password_history;
SELECT count(*) FROM pg_password_history();
SELECT count(*) FROM pg_banned_role;
SELECT count(*) FROM pg_banned_role();
RESET ROLE;
GRANT pg_read_all_stats TO cc_coord1;
SET ROLE cc_coord1;
SELECT count(*) FROM pg_password_history;
SELECT count(*) >= 0 FROM pg_banned_role;
RESET ROLE;
REVOKE pg_read_all_stats FROM cc_coord1;
-- DROP ROLE with a special role specifier is rejected
DROP ROLE CURRENT_USER;
DROP ROLE SESSION_USER, public;
-- A failed DROP ROLE keeps the history of the role
CREATE TABLE cc_owned (i int) DISTRIBUTED BY (i);
ALTER TABLE cc_owned OWNER TO cc_coord2;
DROP USER cc_coord2;
SELECT count(*) FROM pg_password_history WHERE rolename = 'cc_coord2';
DROP TABLE cc_owned;
DROP TABLE cc_roles;
DROP USER cc_coord1;
DROP USER cc_coord2;
-- A successful one removes it
SELECT count(*) FROM pg_password_history WHERE rolename = 'cc_coord2';
RESET credcheck.password_reuse_history;
