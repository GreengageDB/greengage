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
        #########
        # Test 1 - check start cluster in HA mode + manual promotion
        #########
        When Patroni test cluster state is cleared
         And the user runs "gpstart --HA --config-file-coordinator /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml --config-file-standby /home/gpadmin/gpdb_src/ci/patroni/configs/smdw.yml"
        Then gpstart should return a return code of 0
         And Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And ETCD leader ticket at endpoint="http://etcd1:2379" belongs to "mdw"
         And Patroni has streaming replica "smdw" on host "scdw"
        When the user runs psql with "-c 'SELECT 1'" against database "postgres"
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader mdw --candidate smdw --force"
        Then Patroni has running leader "smdw" on host "scdw"
         And the coordinator on host "scdw" port "7001" is ready for queries
         And ETCD leader ticket at endpoint="http://etcd1:2379" belongs to "smdw"
         And Patroni has streaming replica "mdw" on host "cdw"
        When the user runs psql with "-h scdw -p 7001 -c 'SELECT 1'" against database "postgres"
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader smdw --candidate mdw --force"
        Then Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And ETCD leader ticket at endpoint="http://etcd1:2379" belongs to "mdw"
         And Patroni has streaming replica "smdw" on host "scdw"
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
        #########
        # Test 2 - check Coordinator auto-recovery if Coordinator was stopped
        #########
        When Patroni test cluster state is cleared
         And the user runs "gpstart --HA --config-file-coordinator /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml --config-file-standby /home/gpadmin/gpdb_src/ci/patroni/configs/smdw.yml"
        Then gpstart should return a return code of 0
         And Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And Patroni has streaming replica "smdw" on host "scdw"
        When the current coordinator process is saved
         And the coordinator goes down
        Then the saved coordinator process has stopped
         # wait for loop_wait seconds
        When waiting "10" seconds
         # verify the Coordinator is up again
        Then Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And the user runs command "ssh cdw 'pgrep -x postgres >/dev/null; test $? -eq 0'"
         And Patroni has streaming replica "smdw" on host "scdw"
        #########
        # Test 3 - check Coordinator auto-failover if Coordinator and its HA-agent were stopped
        #########
        When the user runs command "ssh cdw 'pkill -TERM -f "python3 -m [p]atroni"'"
        When the coordinator goes down
         And the user runs command "ssh cdw 'pgrep -x postgres >/dev/null; test $? -eq 1'"
         # wait for lease to expire
         And waiting "31" seconds
        Then Patroni has no running leader "mdw" on host "cdw"
         And the user runs command "ssh cdw 'pgrep -x postgres >/dev/null; test $? -eq 1'"
         And the user runs command "test ! -f /home/gpadmin/.data/gpseg-1/postmaster.pid"
         And Patroni has running leader "smdw" on host "scdw"
         And the coordinator on host "scdw" port "7001" is ready for queries
        # The two manual switchovers above advance the standby dbid from 8 to 10.
        When execute following sql on host "scdw" port "7001" in db "postgres" and store result in the context
            """
            SELECT * FROM gp_segment_configuration ORDER BY dbid;
            """
        Then validate that following rows are exactly the stored rows
          | dbid | content | role | preferred_role | mode | status | port | hostname | address | datadir                            |
          | 10   | -1      | p    | p              | s    | u      | 7001 | scdw     | scdw    | /home/gpadmin/.data/standby        |
          | 2    | 0       | p    | p              | s    | u      | 7100 | sdw1     | sdw1    | /home/gpadmin/.data/primary/gpseg0 |
          | 3    | 1       | p    | p              | s    | u      | 7101 | sdw2     | sdw2    | /home/gpadmin/.data/primary/gpseg1 |
          | 4    | 2       | p    | p              | s    | u      | 7102 | sdw3     | sdw3    | /home/gpadmin/.data/primary/gpseg2 |
          | 5    | 0       | m    | m              | s    | u      | 7200 | sdw2     | sdw2    | /home/gpadmin/.data/mirror/gpseg0  |
          | 6    | 1       | m    | m              | s    | u      | 7201 | sdw3     | sdw3    | /home/gpadmin/.data/mirror/gpseg1  |
          | 7    | 2       | m    | m              | s    | u      | 7202 | sdw1     | sdw1    | /home/gpadmin/.data/mirror/gpseg2  |
        # Start again mdv HA-agent manually, as current `gpstop --HA` will error out
        # if no Leader or no Replica is presented (it is accepted MVP behavior).
        When the user runs command "python3 -m patroni /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml > ~/gpAdminLogs/patroni_agent.log 2>&1 &"
        Then Patroni has streaming replica "mdw" on host "cdw"
        When execute following sql on host "scdw" port "7001" in db "postgres" and store result in the context
            """
            SELECT * FROM gp_segment_configuration ORDER BY dbid;
            """
        Then validate that following rows are exactly the stored rows
          | dbid | content | role | preferred_role | mode | status | port | hostname | address | datadir                            |
          | 10   | -1      | p    | p              | s    | u      | 7001 | scdw     | scdw    | /home/gpadmin/.data/standby        |
          | 11   | -1      | m    | m              | s    | u      | 7000 | cdw      | cdw     | /home/gpadmin/.data/gpseg-1        |
          | 2    | 0       | p    | p              | s    | u      | 7100 | sdw1     | sdw1    | /home/gpadmin/.data/primary/gpseg0 |
          | 3    | 1       | p    | p              | s    | u      | 7101 | sdw2     | sdw2    | /home/gpadmin/.data/primary/gpseg1 |
          | 4    | 2       | p    | p              | s    | u      | 7102 | sdw3     | sdw3    | /home/gpadmin/.data/primary/gpseg2 |
          | 5    | 0       | m    | m              | s    | u      | 7200 | sdw2     | sdw2    | /home/gpadmin/.data/mirror/gpseg0  |
          | 6    | 1       | m    | m              | s    | u      | 7201 | sdw3     | sdw3    | /home/gpadmin/.data/mirror/gpseg1  |
          | 7    | 2       | m    | m              | s    | u      | 7202 | sdw1     | sdw1    | /home/gpadmin/.data/mirror/gpseg2  |
         # Now perform manual switchover to get all things back in place
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader smdw --candidate mdw --force"
        Then Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And Patroni has streaming replica "smdw" on host "scdw"
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM gp_segment_configuration ORDER BY dbid;
            """
        Then validate that following rows are exactly the stored rows
          | dbid | content | role | preferred_role | mode | status | port | hostname | address | datadir                            |
          | 11   | -1      | p    | p              | s    | u      | 7000 | cdw      | cdw     | /home/gpadmin/.data/gpseg-1        |
          | 12   | -1      | m    | m              | s    | u      | 7001 | scdw     | scdw    | /home/gpadmin/.data/standby        |
          | 2    | 0       | p    | p              | s    | u      | 7100 | sdw1     | sdw1    | /home/gpadmin/.data/primary/gpseg0 |
          | 3    | 1       | p    | p              | s    | u      | 7101 | sdw2     | sdw2    | /home/gpadmin/.data/primary/gpseg1 |
          | 4    | 2       | p    | p              | s    | u      | 7102 | sdw3     | sdw3    | /home/gpadmin/.data/primary/gpseg2 |
          | 5    | 0       | m    | m              | s    | u      | 7200 | sdw2     | sdw2    | /home/gpadmin/.data/mirror/gpseg0  |
          | 6    | 1       | m    | m              | s    | u      | 7201 | sdw3     | sdw3    | /home/gpadmin/.data/mirror/gpseg1  |
          | 7    | 2       | m    | m              | s    | u      | 7202 | sdw1     | sdw1    | /home/gpadmin/.data/mirror/gpseg2  |
        # Test 4. - Check we can change lease ticket TTL.
        # Change default 30 seconds TTL to 35 seconds.
        When Patroni lease ticket TTL is set to "35" seconds
         And waiting "35" seconds
        Then verify ETCD (at endpoint="http://etcd1:2379") lease TTL is equal to "35" seconds
        # Test 5 - reject manual switchover to a standby isolated from ETCD.
        When ETCD hosts "etcd1 etcd2 etcd3" are blocked from host "scdw"
         # Allow the 35-second member lease and a 10-second HA loop to expire.
         And waiting "46" seconds
        Then Patroni has no streaming replica "smdw" on host "scdw"
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader mdw --candidate smdw --force" eok
        Then patronictl should return a return code of 1
         And patronictl should print "No candidates found to switchover to" error message
         And Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And Patroni has no running leader "smdw" on host "scdw"
        When ETCD hosts are unblocked from host "scdw"
        Then Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And Patroni has streaming replica "smdw" on host "scdw"
        # Test 6 - stop the primary coordinator when it loses ETCD access.
        When the current coordinator process is saved
         And ETCD hosts "etcd1 etcd2 etcd3" are blocked from host "cdw"
        Then the saved coordinator process has stopped
         # Observe the new leader from the host that can still reach ETCD.
         And from host "scdw", Patroni has running leader "smdw" on host "scdw"
         And the coordinator on host "scdw" port "7001" is ready for queries
         And the coordinator on host "cdw" port "7000" is no longer primary
        When execute following sql on host "scdw" port "7001" in db "postgres" and store result in the context
            """
            SELECT * FROM gp_segment_configuration ORDER BY dbid;
            """
        Then validate that following rows are exactly the stored rows
          | dbid | content | role | preferred_role | mode | status | port | hostname | address | datadir                            |
          | 12   | -1      | p    | p              | s    | u      | 7001 | scdw     | scdw    | /home/gpadmin/.data/standby        |
          | 2    | 0       | p    | p              | s    | u      | 7100 | sdw1     | sdw1    | /home/gpadmin/.data/primary/gpseg0 |
          | 3    | 1       | p    | p              | s    | u      | 7101 | sdw2     | sdw2    | /home/gpadmin/.data/primary/gpseg1 |
          | 4    | 2       | p    | p              | s    | u      | 7102 | sdw3     | sdw3    | /home/gpadmin/.data/primary/gpseg2 |
          | 5    | 0       | m    | m              | s    | u      | 7200 | sdw2     | sdw2    | /home/gpadmin/.data/mirror/gpseg0  |
          | 6    | 1       | m    | m              | s    | u      | 7201 | sdw3     | sdw3    | /home/gpadmin/.data/mirror/gpseg1  |
          | 7    | 2       | m    | m              | s    | u      | 7202 | sdw1     | sdw1    | /home/gpadmin/.data/mirror/gpseg2  |
        When ETCD hosts are unblocked from host "cdw"
        Then Patroni has running leader "smdw" on host "scdw"
         And the coordinator on host "scdw" port "7001" is ready for queries
         And Patroni has streaming replica "mdw" on host "cdw"
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader smdw --candidate mdw --force"
        Then Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And Patroni has streaming replica "smdw" on host "scdw"
        When execute following sql on host "cdw" port "7000" in db "postgres" and store result in the context
            """
            SELECT * FROM gp_segment_configuration ORDER BY dbid;
            """
        Then validate that following rows are exactly the stored rows
          | dbid | content | role | preferred_role | mode | status | port | hostname | address | datadir                            |
          | 13   | -1      | p    | p              | s    | u      | 7000 | cdw      | cdw     | /home/gpadmin/.data/gpseg-1        |
          | 14   | -1      | m    | m              | s    | u      | 7001 | scdw     | scdw    | /home/gpadmin/.data/standby        |
          | 2    | 0       | p    | p              | s    | u      | 7100 | sdw1     | sdw1    | /home/gpadmin/.data/primary/gpseg0 |
          | 3    | 1       | p    | p              | s    | u      | 7101 | sdw2     | sdw2    | /home/gpadmin/.data/primary/gpseg1 |
          | 4    | 2       | p    | p              | s    | u      | 7102 | sdw3     | sdw3    | /home/gpadmin/.data/primary/gpseg2 |
          | 5    | 0       | m    | m              | s    | u      | 7200 | sdw2     | sdw2    | /home/gpadmin/.data/mirror/gpseg0  |
          | 6    | 1       | m    | m              | s    | u      | 7201 | sdw3     | sdw3    | /home/gpadmin/.data/mirror/gpseg1  |
          | 7    | 2       | m    | m              | s    | u      | 7202 | sdw1     | sdw1    | /home/gpadmin/.data/mirror/gpseg2  |
        # Test 7 - stop the primary when another HA agent owns the leader ticket.
        # Suspend the standby agent so promotion happens only after the old primary stops.
        When the current coordinator process is saved
         And Patroni HA agent on host "scdw" is suspended
         And ETCD leader ticket at endpoint="http://etcd1:2379" is transferred from "mdw" to "smdw"
        Then the saved coordinator process has stopped
         And the coordinator on host "cdw" port "7000" is no longer primary
         And ETCD leader ticket at endpoint="http://etcd1:2379" belongs to "smdw"
        When Patroni HA agent on host "scdw" is resumed
        Then Patroni has running leader "smdw" on host "scdw"
         And the coordinator on host "scdw" port "7001" is ready for queries
         And Patroni has streaming replica "mdw" on host "cdw"
        When execute following sql on host "scdw" port "7001" in db "postgres" and store result in the context
            """
            SELECT * FROM gp_segment_configuration ORDER BY dbid;
            """
        Then validate that following rows are exactly the stored rows
          | dbid | content | role | preferred_role | mode | status | port | hostname | address | datadir                            |
          | 14   | -1      | p    | p              | s    | u      | 7001 | scdw     | scdw    | /home/gpadmin/.data/standby        |
          | 15   | -1      | m    | m              | s    | u      | 7000 | cdw      | cdw     | /home/gpadmin/.data/gpseg-1        |
          | 2    | 0       | p    | p              | s    | u      | 7100 | sdw1     | sdw1    | /home/gpadmin/.data/primary/gpseg0 |
          | 3    | 1       | p    | p              | s    | u      | 7101 | sdw2     | sdw2    | /home/gpadmin/.data/primary/gpseg1 |
          | 4    | 2       | p    | p              | s    | u      | 7102 | sdw3     | sdw3    | /home/gpadmin/.data/primary/gpseg2 |
          | 5    | 0       | m    | m              | s    | u      | 7200 | sdw2     | sdw2    | /home/gpadmin/.data/mirror/gpseg0  |
          | 6    | 1       | m    | m              | s    | u      | 7201 | sdw3     | sdw3    | /home/gpadmin/.data/mirror/gpseg1  |
          | 7    | 2       | m    | m              | s    | u      | 7202 | sdw1     | sdw1    | /home/gpadmin/.data/mirror/gpseg2  |
        When the user runs command "patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml switchover test-cluster --leader smdw --candidate mdw --force"
        Then Patroni has running leader "mdw" on host "cdw"
         And the coordinator on host "cdw" port "7000" is ready for queries
         And Patroni has streaming replica "smdw" on host "scdw"
        When execute following sql on host "cdw" port "7000" in db "postgres" and store result in the context
            """
            SELECT * FROM gp_segment_configuration ORDER BY dbid;
            """
        Then validate that following rows are exactly the stored rows
          | dbid | content | role | preferred_role | mode | status | port | hostname | address | datadir                            |
          | 15   | -1      | p    | p              | s    | u      | 7000 | cdw      | cdw     | /home/gpadmin/.data/gpseg-1        |
          | 16   | -1      | m    | m              | s    | u      | 7001 | scdw     | scdw    | /home/gpadmin/.data/standby        |
          | 2    | 0       | p    | p              | s    | u      | 7100 | sdw1     | sdw1    | /home/gpadmin/.data/primary/gpseg0 |
          | 3    | 1       | p    | p              | s    | u      | 7101 | sdw2     | sdw2    | /home/gpadmin/.data/primary/gpseg1 |
          | 4    | 2       | p    | p              | s    | u      | 7102 | sdw3     | sdw3    | /home/gpadmin/.data/primary/gpseg2 |
          | 5    | 0       | m    | m              | s    | u      | 7200 | sdw2     | sdw2    | /home/gpadmin/.data/mirror/gpseg0  |
          | 6    | 1       | m    | m              | s    | u      | 7201 | sdw3     | sdw3    | /home/gpadmin/.data/mirror/gpseg1  |
          | 7    | 2       | m    | m              | s    | u      | 7202 | sdw1     | sdw1    | /home/gpadmin/.data/mirror/gpseg2  |
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
