-- start_matchsubs
--
-- m/ERROR:  \[gg_tables_tracking\] exceeded maximum number of tracked databases \(track_files\.c:\d+\)/
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
CREATE EXTENSION gg_tables_tracking;

SHOW gg_tables_tracking.tracking_is_db_tracked;

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.wait_for_worker_initialize();

SELECT gg_tables_tracking.tracking_register_db();

SHOW gg_tables_tracking.tracking_is_db_tracked;

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_unregister_db();

SHOW gg_tables_tracking.tracking_is_db_tracked;

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET gg_tables_tracking.tracking_is_db_tracked = true;

ALTER DATABASE tracking1 SET gg_tables_tracking.tracking_is_db_tracked = true;

-- Test limit of tracking databases.
SHOW gg_tables_tracking.tracking_db_track_count;

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
        PERFORM gg_tables_tracking.tracking_register_db(db_oid);
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
        PERFORM gg_tables_tracking.tracking_unregister_db(db_oid);
    END LOOP;
END;
$$;

DROP DATABASE IF EXISTS tracking2;
DROP DATABASE IF EXISTS tracking3;
DROP DATABASE IF EXISTS tracking4;
DROP DATABASE IF EXISTS tracking5;
DROP DATABASE IF EXISTS tracking6;

-- Test gg_tables_tracking.tracking_snapshot_on_recovery GUC
SELECT gg_tables_tracking.tracking_set_snapshot_on_recovery(true);

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET gg_tables_tracking.tracking_snapshot_on_recovery = false;

ALTER DATABASE tracking1 SET gg_tables_tracking.tracking_snapshot_on_recovery = false;

-- Test gg_tables_tracking.tracking_relams GUC
SELECT gg_tables_tracking.tracking_set_relams('heap, ao_column, brin');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_set_relams('v,v,v,,,');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_set_relams('d,b,c');

SELECT gg_tables_tracking.tracking_set_relams('');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET gg_tables_tracking.tracking_relams = "heap, ao_column, brin";

ALTER DATABASE tracking1 SET gg_tables_tracking.tracking_relams = "heap, ao_column, brin";

-- Resetting case is allowed.
ALTER DATABASE tracking1 RESET gg_tables_tracking.tracking_relams;

-- Test gg_tables_tracking.tracking_relkinds GUC
SELECT gg_tables_tracking.tracking_set_relkinds('r,t,o,S');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_set_relkinds('m,M,o,,,');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_set_relkinds('d,b,c');

SELECT gg_tables_tracking.tracking_set_relkinds('');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

-- Prohibit manual GUC setting.
SET gg_tables_tracking.tracking_relkinds = "h, a, x";

ALTER DATABASE tracking1 SET gg_tables_tracking.tracking_relkinds = "h, a, x";

-- Resetting case is allowed.
ALTER DATABASE tracking1 RESET gg_tables_tracking.tracking_relkinds;

-- Test gg_tables_tracking.tracking_schemas GUC
SELECT gg_tables_tracking.tracking_unregister_schema('public');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_register_schema('gg_tables_tracking');

SELECT gg_tables_tracking.tracking_register_schema('public');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_unregister_schema('public');

SELECT datname, setconfig FROM pg_db_role_setting JOIN pg_database ON
setdatabase=oid WHERE datname=current_database();

SELECT gg_tables_tracking.tracking_register_schema('pg_pg');

-- Prohibit manual GUC setting.
SET gg_tables_tracking.tracking_schemas = "pg_catalog, mychema";

ALTER DATABASE tracking1 SET gg_tables_tracking.tracking_schemas =  "pg_catalog, mychema";

-- Resetting case is allowed.
ALTER DATABASE tracking1 RESET gg_tables_tracking.tracking_schemas;

-- Test GUCs are set in the caller's session.
SELECT gg_tables_tracking.tracking_register_db();
SHOW gg_tables_tracking.tracking_is_db_tracked;

SELECT gg_tables_tracking.tracking_unregister_db();
SHOW gg_tables_tracking.tracking_is_db_tracked;

SELECT gg_tables_tracking.tracking_set_snapshot_on_recovery(true);
SHOW gg_tables_tracking.tracking_snapshot_on_recovery;

SELECT gg_tables_tracking.tracking_set_snapshot_on_recovery(false);
SHOW gg_tables_tracking.tracking_snapshot_on_recovery;

SHOW gg_tables_tracking.tracking_schemas;
SELECT gg_tables_tracking.tracking_register_schema('gg_tables_tracking');
SHOW gg_tables_tracking.tracking_schemas;

SELECT gg_tables_tracking.tracking_unregister_schema('gg_tables_tracking');
SHOW gg_tables_tracking.tracking_schemas;

SHOW gg_tables_tracking.tracking_relkinds;
SELECT gg_tables_tracking.tracking_set_relkinds('r,t');
SHOW gg_tables_tracking.tracking_relkinds;

SHOW gg_tables_tracking.tracking_relams;
SELECT gg_tables_tracking.tracking_set_relams('ao_row');
SHOW gg_tables_tracking.tracking_relams;

\c contrib_regression;

DROP DATABASE tracking1;
