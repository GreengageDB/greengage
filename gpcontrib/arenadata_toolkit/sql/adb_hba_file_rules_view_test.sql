-- start_ignore
-- Prepare DB for the test
DROP EXTENSION IF EXISTS arenadata_toolkit;
DROP SCHEMA IF EXISTS arenadata_toolkit CASCADE;
--end_ignore

CREATE EXTENSION arenadata_toolkit;
SET search_path = arenadata_toolkit;

-- save pg_hba.conf
\! mv -f $MASTER_DATA_DIRECTORY/pg_hba.conf $MASTER_DATA_DIRECTORY/pg_hba.conf.back
-- fill pg_hba.conf with test entry
\! echo "local    all    all    trust" >> $MASTER_DATA_DIRECTORY/pg_hba.conf
-- test pg_hba.conf filling
SELECT * FROM adb_hba_file_rules_view;
-- restore original pg_hba.conf
\! mv -f $MASTER_DATA_DIRECTORY/pg_hba.conf.back $MASTER_DATA_DIRECTORY/pg_hba.conf

DROP EXTENSION arenadata_toolkit;
DROP SCHEMA arenadata_toolkit CASCADE;
