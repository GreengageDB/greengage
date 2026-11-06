@auto_failover @skip
Feature: auto failover behave tests

    Scenario: Start and stop a cluster in HA mode
        Given the environment variable "USER" is set to "gpadmin"
         And the environment variable "LOGNAME" is set to "gpadmin"
         And the database is not running
         And a working directory of the test as '/home/gpadmin/.data'
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/.data/primary /home/gpadmin/.data/mirror'"
         And the user runs command "gpssh -h scdw -e 'rm -rf /home/gpadmin/.data/standby'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'mkdir -p /home/gpadmin/.data/primary /home/gpadmin/.data/mirror'"
         And the user runs command "gpssh -h scdw -e 'mkdir -p /home/gpadmin/.data /home/gpadmin/gpAdminLogs'"
         And the temporary file "/tmp/auto_failover_cluster_cfg" is created with content
         """
         TRUSTED_SHELL=ssh
         ENCODING=UNICODE
         SEG_PREFIX=gpseg
         HEAP_CHECKSUM=on
         HBA_HOSTNAMES=0
         QD_PRIMARY_ARRAY=cdw~cdw~7000~/home/gpadmin/.data/gpseg-1~1~-1~0
         declare -a PRIMARY_ARRAY=(
             sdw1~sdw1~7100~/home/gpadmin/.data/primary/gpseg0~2~0
             sdw2~sdw2~7101~/home/gpadmin/.data/primary/gpseg1~3~1
             sdw3~sdw3~7102~/home/gpadmin/.data/primary/gpseg2~4~2
         )
         declare -a MIRROR_ARRAY=(
             sdw2~sdw2~7200~/home/gpadmin/.data/mirror/gpseg0~5~0
             sdw3~sdw3~7201~/home/gpadmin/.data/mirror/gpseg1~6~1
             sdw1~sdw1~7202~/home/gpadmin/.data/mirror/gpseg2~7~2
         )
         """
        When initialize a cluster using "/tmp/auto_failover_cluster_cfg" with standby on scdw, port 7001, data directory '/home/gpadmin/.data/standby'
        Then the temporary file "/tmp/auto_failover_cluster_cfg" is removed
        Given the environment variable "COORDINATOR_DATA_DIRECTORY" is set to "/home/gpadmin/.data/gpseg-1"
         And the environment variable "PGPORT" is set to "7000"
         And coordinator data directory is updated
         And below sql is executed in "postgres" db
         """
         CREATE USER patroni WITH SUPERUSER ENCRYPTED PASSWORD 'patroni_pass';
         CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';
         GRANT pg_read_all_settings TO replicator;
         CREATE USER rewind_user WITH ENCRYPTED PASSWORD 'rewind_pass';
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_dir(text, boolean, boolean) TO rewind_user;
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_stat_file(text, boolean) TO rewind_user;
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text) TO rewind_user;
         GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint, boolean) TO rewind_user;
         GRANT pg_read_all_settings TO rewind_user;
         """
         And Patroni access is enabled on both coordinators
        When the user runs "gpconfig -c hot_standby -v on"
        Then gpconfig should return a return code of 0
        When the user runs "gpstop -a"
        Then gpstop should return a return code of 0
        When Patroni test cluster state is cleared
         And the user runs "gpstart --HA --config-file-coordinator /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml --config-file-standby /home/gpadmin/gpdb_src/ci/patroni/configs/smdw.yml"
        Then gpstart should return a return code of 0
         And Patroni has leader "mdw" and streaming replica "smdw"
        When the user runs psql with "-c 'SELECT 1'" against database "postgres"
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader mdw --candidate smdw --force"
        Then Patroni has leader "smdw" and streaming replica "mdw"
        When the user runs psql with "-h scdw -p 7001 -c 'SELECT 1'" against database "postgres"
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader smdw --candidate mdw --force"
        Then Patroni has leader "mdw" and streaming replica "smdw"
        When the user runs psql with "-c 'SELECT 1'" against database "postgres"
        When the user runs "gpstop --HA"
        Then gpstop should return a return code of 0
         And the user runs command "ssh cdw 'pgrep -x postgres >/dev/null; test $? -eq 1'"
         And the user runs command "ssh scdw 'pgrep -x postgres >/dev/null; test $? -eq 1'"
         And the user runs command "ssh sdw1 'pgrep -x postgres >/dev/null; test $? -eq 1'"
         And the user runs command "ssh sdw2 'pgrep -x postgres >/dev/null; test $? -eq 1'"
         And the user runs command "ssh sdw3 'pgrep -x postgres >/dev/null; test $? -eq 1'"
         And the user runs command "test ! -f /home/gpadmin/.data/gpseg-1/postmaster.pid"
         And the user runs command "ssh scdw test ! -f /home/gpadmin/.data/standby/postmaster.pid"
         And "PGPORT" environment variable should be restored
         And "COORDINATOR_DATA_DIRECTORY" environment variable should be restored
         And "USER" environment variable should be restored
         And "LOGNAME" environment variable should be restored
         And coordinator data directory is updated
