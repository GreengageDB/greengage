-- start_ignore
\! gpconfig -c shared_preload_libraries -v 'arenadata_toolkit'
\! gpstop -raq -M fast
\c
-- end_ignore
-- start_matchsubs
--
-- m/ERROR:  \[arenadata_toolkit\] exceeded maximum number of tracked databases \(track_files\.c:\d+\)/
-- s/\d+/XXX/g
--
-- end_matchsubs
--start_ignore
DROP DATABASE IF EXISTS tracking1;
DROP DATABASE IF EXISTS tracking2;
DROP DATABASE IF EXISTS tracking3;
DROP DATABASE IF EXISTS tracking4;
DROP DATABASE IF EXISTS tracking5;
DROP DATABASE IF EXISTS tracking6;
--end_ignore

-- Test database registering GUC.
CREATE DATABASE tracking1;
\c tracking1;
CREATE EXTENSION arenadata_toolkit;

SHOW arenadata_toolkit.tracking_is_db_tracked;

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_register_db();

SHOW arenadata_toolkit.tracking_is_db_tracked;

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_unregister_db();

SHOW arenadata_toolkit.tracking_is_db_tracked;

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET arenadata_toolkit.tracking_is_db_tracked = true;

ALTER DATABASE tracking1 SET arenadata_toolkit.tracking_is_db_tracked = true;

-- Test limit of tracking databases.
SHOW arenadata_toolkit.tracking_db_track_count;

CREATE DATABASE tracking2;
CREATE DATABASE tracking3;
CREATE DATABASE tracking4;
CREATE DATABASE tracking5;
CREATE DATABASE tracking6;

DO $$
DECLARE
    db_oid oid;
BEGIN
    FOR db_oid IN 
        SELECT oid 
        FROM pg_database 
        WHERE datname IN ('tracking1', 'tracking2', 'tracking3',
        'tracking4', 'tracking5', 'tracking6')
    LOOP
        PERFORM arenadata_toolkit.tracking_register_db(db_oid);
    END LOOP;
END;
$$;


DO $$
DECLARE
    db_oid oid;
BEGIN
    FOR db_oid IN 
        SELECT oid 
        FROM pg_database 
        WHERE datname IN ('tracking1', 'tracking2', 'tracking3',
        'tracking4', 'tracking5', 'tracking6')
    LOOP
        PERFORM arenadata_toolkit.tracking_unregister_db(db_oid);
    END LOOP;
END;
$$;

DROP DATABASE IF EXISTS tracking2;
DROP DATABASE IF EXISTS tracking3;
DROP DATABASE IF EXISTS tracking4;
DROP DATABASE IF EXISTS tracking5;
DROP DATABASE IF EXISTS tracking6;

-- Test arenadata_toolkit.tracking_snapshot_on_recovery GUC
SELECT arenadata_toolkit.tracking_set_snapshot_on_recovery(true);

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET arenadata_toolkit.tracking_snapshot_on_recovery = false;

ALTER DATABASE tracking1 SET arenadata_toolkit.tracking_snapshot_on_recovery = false;

-- Test arenadata_toolkit.tracking_relstorages GUC
SELECT arenadata_toolkit.tracking_set_relstorages('f,a,x');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_set_relstorages('v,v,v,,,');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_set_relstorages('d,b,c');

SELECT arenadata_toolkit.tracking_set_relstorages('');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET arenadata_toolkit.tracking_relstorages = "h, a, x";

ALTER DATABASE tracking1 SET arenadata_toolkit.tracking_relstorages = "h, a, x";

-- Resetting case is allowed.
ALTER DATABASE tracking1 RESET arenadata_toolkit.tracking_relstorages;

-- Test arenadata_toolkit.tracking_relkinds GUC
SELECT arenadata_toolkit.tracking_set_relkinds('r,t,o,S');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_set_relkinds('m,M,o,,,');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_set_relkinds('d,b,c');

SELECT arenadata_toolkit.tracking_set_relkinds('');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET arenadata_toolkit.tracking_relkinds = "h, a, x";

ALTER DATABASE tracking1 SET arenadata_toolkit.tracking_relkinds = "h, a, x";

-- Resetting case is allowed.
ALTER DATABASE tracking1 RESET arenadata_toolkit.tracking_relkinds;

-- Test arenadata_toolkit.tracking_schemas GUC
SELECT arenadata_toolkit.tracking_unregister_schema('public');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_register_schema('arenadata_toolkit');

SELECT arenadata_toolkit.tracking_register_schema('public');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_unregister_schema('public');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT arenadata_toolkit.tracking_register_schema('pg_pg');

-- Prohibit manual GUC setting.
SET arenadata_toolkit.tracking_schemas = "pg_catalog, mychema";

ALTER DATABASE tracking1 SET arenadata_toolkit.tracking_schemas =  "pg_catalog, mychema";

-- Resetting case is allowed.
ALTER DATABASE tracking1 RESET arenadata_toolkit.tracking_schemas;

-- Test GUCs are set in the caller's session.
SELECT arenadata_toolkit.tracking_register_db();
SHOW arenadata_toolkit.tracking_is_db_tracked;

SELECT arenadata_toolkit.tracking_unregister_db();
SHOW arenadata_toolkit.tracking_is_db_tracked;

SELECT arenadata_toolkit.tracking_set_snapshot_on_recovery(true);
SHOW arenadata_toolkit.tracking_snapshot_on_recovery;

SELECT arenadata_toolkit.tracking_set_snapshot_on_recovery(false);
SHOW arenadata_toolkit.tracking_snapshot_on_recovery;

SHOW arenadata_toolkit.tracking_schemas;
SELECT arenadata_toolkit.tracking_register_schema('arenadata_toolkit');
SHOW arenadata_toolkit.tracking_schemas;

SELECT arenadata_toolkit.tracking_unregister_schema('arenadata_toolkit');
SHOW arenadata_toolkit.tracking_schemas;

SHOW arenadata_toolkit.tracking_relkinds;
SELECT arenadata_toolkit.tracking_set_relkinds('r,t');
SHOW arenadata_toolkit.tracking_relkinds;

SHOW arenadata_toolkit.tracking_relstorages;
SELECT arenadata_toolkit.tracking_set_relstorages('a');
SHOW arenadata_toolkit.tracking_relstorages;

\c contrib_regression;

DROP DATABASE tracking1;
