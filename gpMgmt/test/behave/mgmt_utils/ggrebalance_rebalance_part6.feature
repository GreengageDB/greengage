@ggrebalance_rebalance @ggrebalance_rebalance_part6
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario: test 9.3.1. rebalance - interrupt during shrink, and full rollback.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_SHRINK_TABLES_DONE_begin"
         And the gp_segment_configuration have been saved
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance --non-interactive-mode -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 0
         And verify the gp_segment_configuration has been restored
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100

    Scenario: test 9.3.2. rebalance - interrupt during shrink, and full rollback, interrupt during shrink rollback, and continue.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_SHRINK_CATALOG_STARTED_begin"
         And the gp_segment_configuration have been saved
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance --non-interactive-mode -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START_end"
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is already in progress, and was interrupted. Execute 'ggrebalance' without '-r' flag." to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And verify the gp_segment_configuration has been restored
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100

    Scenario: test 9.4. rebalance - interrupt after shrink, but before rebalance start, and full rollback.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_REBALANCE_STARTED_begin"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance --non-interactive-mode -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 4, row count = 100

    Scenario: test 9.5. rebalance - shrink, rebalance (and interrupt during it) and full rollback.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_REBALANCE_DONE_begin"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance --non-interactive-mode -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 4, row count = 100

    Scenario: test 10. rebalance - rebalance and full rollback.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And the gp_segment_configuration have been saved
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And ggrebalance should not print "Tables shrunk:" to logfile with latest timestamp
         And ggrebalance should not print "Bytes processed:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink rate:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink total time:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rolled back:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rollback rate:" to logfile with latest timestamp
         And ggrebalance should not print "Rollback total time:" to logfile with latest timestamp
         And ggrebalance should print "Segments moved:\s*0" regex to logfile
         And ggrebalance should print "Rolled back steps:\s*\d+" regex to logfile
         And ggrebalance should print "Cancelled steps:\s*0" regex to logfile
         And ggrebalance should not print " WARNINGS " to logfile with latest timestamp
         And verify the gp_segment_configuration has been restored
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100

    Scenario: test 11. rebalance - planner should detect primary-mirror conflicts when mirror's dst host= primary's src host.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'mkdir -p /data/gpdata/ggrebalance/primary'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'mkdir -p /data/gpdata/ggrebalance/mirror'"
         And the temporary file "/tmp/rebalance_s6" is created with content
         """
         TRUSTED_SHELL=ssh
         ENCODING=UNICODE
         SEG_PREFIX=gpseg
         HEAP_CHECKSUM=on
         HBA_HOSTNAMES=0
         QD_PRIMARY_ARRAY=cdw~cdw~7000~/data/gpdata/ggrebalance/gpseg-1~1~-1~0
         declare -a PRIMARY_ARRAY=(
            sdw1~sdw1~7002~/data/gpdata/ggrebalance/primary/gpseg0~2~0~11100
            sdw1~sdw1~7003~/data/gpdata/ggrebalance/primary/gpseg1~3~1~11110
            sdw1~sdw1~7004~/data/gpdata/ggrebalance/primary/gpseg2~4~2~11220
            sdw2~sdw2~7005~/data/gpdata/ggrebalance/primary/gpseg3~5~3~11350
            sdw2~sdw2~7006~/data/gpdata/ggrebalance/primary/gpseg4~6~4~11360
            sdw2~sdw2~7007~/data/gpdata/ggrebalance/primary/gpseg5~7~5~11370
            )
            declare -a MIRROR_ARRAY=(
            sdw2~sdw2~7050~/data/gpdata/ggrebalance/mirror/gpseg0~8~0~51130
            sdw2~sdw2~7051~/data/gpdata/ggrebalance/mirror/gpseg1~9~1~51140
            sdw2~sdw2~7052~/data/gpdata/ggrebalance/mirror/gpseg2~10~2~51160
            sdw3~sdw3~7053~/data/gpdata/ggrebalance/mirror/gpseg3~11~3~51160
            sdw3~sdw3~7054~/data/gpdata/ggrebalance/mirror/gpseg4~12~4~51200
            sdw2~sdw2~7055~/data/gpdata/ggrebalance/mirror/gpseg5~13~5~51136
            )
         """
        When initialize a cluster using "/tmp/rebalance_s6"
        Then the temporary file "/tmp/rebalance_s6" is removed
        Given the environment variable "COORDINATOR_DATA_DIRECTORY" is set to "/data/gpdata/ggrebalance/gpseg-1"
         And coordinator data directory is updated
         And all files in gpAdminLogs directory are deleted
        When set fault inject "on_enter_STATE_REBALANCE_PREPARE_MOVES_DONE_end"
         And the user runs "ggrebalance --non-interactive-mode --skip-resource-estimation -x 6 -d '/data/gpdata/ggrebalance/primary, /data/gpdata/ggrebalance/mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "psql -d postgres -c 'SELECT move_order, status FROM ggrebalance.segment_move_steps ORDER BY 1' -o /tmp/move_order.out"
        Then psql should return a return code of 0
         And verify that the file "/tmp/move_order.out" contains text
"""
 move_order |      status      
------------+------------------
          0 | PLANNED
          1 | APPROVE_REQUIRED
          2 | APPROVE_REQUIRED
          3 | PLANNED
          4 | PLANNED
          5 | APPROVE_REQUIRED
          6 | APPROVE_REQUIRED
          7 | PLANNED
(8 rows)


"""     
         And the temporary file "/tmp/move_order.out" is removed
        Given the database is not running
        Given the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /data/gpdata/ggrebalance/primary'"
        And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /data/gpdata/ggrebalance/mirror'"
        Then "COORDINATOR_DATA_DIRECTORY" environment variable should be restored
         And coordinator data directory is updated
