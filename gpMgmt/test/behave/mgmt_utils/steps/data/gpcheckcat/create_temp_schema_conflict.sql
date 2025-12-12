\t
create temp table temp_table(i int);
create schema good_schema;
create table good_schema.good_table(i int);
-- rename the temp schema to correspond active bgworker session
set allow_system_table_mods=1;

update pg_namespace ns set nspname='pg_temp_0'||x.sess_id
from pg_stat_activity x where x.backend_type like 'logical%'
and ns.nspname like
'pg_temp_'||(SELECT sess_id FROM pg_stat_activity WHERE pid = pg_backend_pid());

\o test/behave/mgmt_utils/steps/data/gpcheckcat/pid_leak
select pg_backend_pid();
-- sleep for 5 seconds so as to be able to kill the process while the session is still running
select pg_sleep(5)
