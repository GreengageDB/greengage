# Keep this suite's CI runtime within the 100-120 minute budget.
# If it exceeds 120 minutes, move some scenarios or example cases into a new feature file.

@ggrebalance_rebalance @ggrebalance_rebalance_part2
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario: 4. rebalance - check rebalance after interrupted shrink.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "fault_rebalance_table_test_db_2.test_schema_2.test_table_1"
        When the user runs "ggrebalance --non-interactive-mode -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Continue interrupted shrink operation..." to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 4, row count = 100

    Scenario: 5. rebalance - check interrupted rebalance after shrink.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_REBALANCE_EXECUTION_STARTED_begin"
        When the user runs "ggrebalance --non-interactive-mode -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Continue interrupted rebalance operation..." to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 4, row count = 100

    Scenario Outline: 6. rebalance - case when mirror and primary swap their hosts.
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
            sdw2~sdw2~7003~/data/gpdata/ggrebalance/primary/gpseg3~5~3~11350
            sdw3~sdw3~7004~/data/gpdata/ggrebalance/primary/gpseg4~6~4~11360
            sdw3~sdw3~7005~/data/gpdata/ggrebalance/primary/gpseg5~7~5~11370
            )
            declare -a MIRROR_ARRAY=(
            sdw3~sdw3~7050~/data/gpdata/ggrebalance/mirror/gpseg0~8~0~51130
            sdw3~sdw3~7051~/data/gpdata/ggrebalance/mirror/gpseg1~9~1~51140
            sdw2~sdw2~7052~/data/gpdata/ggrebalance/mirror/gpseg2~10~2~51160
            sdw1~sdw1~7053~/data/gpdata/ggrebalance/mirror/gpseg3~11~3~51160
            sdw2~sdw2~7054~/data/gpdata/ggrebalance/mirror/gpseg4~12~4~51200
            sdw2~sdw2~7055~/data/gpdata/ggrebalance/mirror/gpseg5~13~5~51136
            )
         """
        When initialize a cluster using "/tmp/rebalance_s6"
        Then the temporary file "/tmp/rebalance_s6" is removed
        Given the environment variable "COORDINATOR_DATA_DIRECTORY" is set to "/data/gpdata/ggrebalance/gpseg-1"
         And coordinator data directory is updated
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When the user runs <command>
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 1 segments where "hostname='sdw1' and content = 2 and role = 'm' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw2' and content = 2 and role = 'p' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        Given the database is not running
        Given the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /data/gpdata/ggrebalance/primary'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /data/gpdata/ggrebalance/mirror'"
        Then <restore_env>
         And coordinator data directory is updated 

        Examples: inplace swap and using 3rd host
        | command    | restore_env|
        |"ggrebalance --non-interactive-mode -x 6 -d '/data/gpdata/ggrebalance/primary, /data/gpdata/ggrebalance/mirror'"|stub|
        |"ggrebalance --non-interactive-mode -x 6 -d '/data/gpdata/ggrebalance/primary, /data/gpdata/ggrebalance/mirror'  --inplace-swap-roles"|"COORDINATOR_DATA_DIRECTORY" environment variable should be restored|
    
    Scenario: 7. rebalance - case with multiple swaps.
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
            sdw1~sdw1~7005~/data/gpdata/ggrebalance/primary/gpseg3~5~3~11350
            sdw2~sdw2~7006~/data/gpdata/ggrebalance/primary/gpseg4~6~4~11360
            sdw2~sdw2~7007~/data/gpdata/ggrebalance/primary/gpseg5~7~5~11370
            )
            declare -a MIRROR_ARRAY=(
            sdw2~sdw2~7050~/data/gpdata/ggrebalance/mirror/gpseg0~8~0~51130
            sdw2~sdw2~7051~/data/gpdata/ggrebalance/mirror/gpseg1~9~1~51140
            sdw3~sdw3~7052~/data/gpdata/ggrebalance/mirror/gpseg2~10~2~51160
            sdw3~sdw3~7053~/data/gpdata/ggrebalance/mirror/gpseg3~11~3~51160
            sdw3~sdw3~7054~/data/gpdata/ggrebalance/mirror/gpseg4~12~4~51200
            sdw3~sdw3~7055~/data/gpdata/ggrebalance/mirror/gpseg5~13~5~51136
            )
         """
        When initialize a cluster using "/tmp/rebalance_s6"
        Then the temporary file "/tmp/rebalance_s6" is removed
        Given the environment variable "COORDINATOR_DATA_DIRECTORY" is set to "/data/gpdata/ggrebalance/gpseg-1"
         And coordinator data directory is updated
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 6 -d '/data/gpdata/ggrebalance/primary, /data/gpdata/ggrebalance/mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 1 segments where "hostname='sdw1' and content = 2 and role = 'm' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw3' and content = 2 and role = 'p' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw1' and content = 3 and role = 'm' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw3' and content = 3 and role = 'p' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        Given the database is not running
        Given the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /data/gpdata/ggrebalance/primary'"
        And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /data/gpdata/ggrebalance/mirror'"
        Then "COORDINATOR_DATA_DIRECTORY" environment variable should be restored
         And coordinator data directory is updated

    Scenario Outline: 8.1.1 rebalance - interrupt during mirror move before gp_segment_configuration update, continue and retry failed step.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "yes" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Plan to retry step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When execute following sql in db "postgres" and store result in the context
            """
            select false as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where not is_rollback union select true as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where is_rollback;
            """
        Then validate that following rows are in the stored rows
          |  is_rollback | value_exists  |
          |  f           | t             |
          |  t           | f             |

    Examples:
        | fault_name                                                                    |
        | _update_config_begin                                                          |

    Scenario Outline: 8.1.2 rebalance - interrupt during mirror move before gp_segment_configuration update, continue and rollback failed step.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Plan to rollback step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should print "Segments moved:\s*\d+" regex to logfile
         And ggrebalance should print "Rolled back steps:\s*\d+" regex to logfile
         And ggrebalance should print "Cancelled steps:\s*0" regex to logfile
         And ggrebalance should print " WARNINGS " to logfile with latest timestamp
         And ggrebalance should not print " Cancelled steps " to logfile with latest timestamp
         And ggrebalance should not print "Cluster might be not in fault tolerance mode!" to logfile with latest timestamp
         And ggrebalance should print "Cluster is left in unbalanced state" to logfile with latest timestamp
         And ggrebalance should print " Rolled back steps " to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When execute following sql in db "postgres" and store result in the context
            """
            select false as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where not is_rollback union select true as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where is_rollback;
            """
        Then validate that following rows are in the stored rows
          |  is_rollback | value_exists  |
          |  f           | t             |
          |  t           | t             |


    Examples:
        | fault_name                                                                    |
        | _update_config_begin                                                          |

    Scenario Outline: 8.1.3 rebalance - interrupt during mirror move before gp_segment_configuration update, continue and cancel failed step.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Retry step?"
         And user will answer "no" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Cancel step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should print "Segments moved:\s*\d+" regex to logfile
         And ggrebalance should print "Rolled back steps:\s*0" regex to logfile
         And ggrebalance should print "Cancelled steps:\s*\d+" regex to logfile
         And ggrebalance should print " WARNINGS " to logfile with latest timestamp
         And ggrebalance should print " Cancelled steps " to logfile with latest timestamp
         And ggrebalance should print "Cluster might be not in fault tolerance mode!" to logfile with latest timestamp
         And ggrebalance should print "These segments should be started manually in order cluster to become fault tolerant:" to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'd'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'd'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When execute following sql in db "postgres" and store result in the context
            """
            select false as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where not is_rollback union select true as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where is_rollback;
            """
        Then validate that following rows are in the stored rows
          |  is_rollback | value_exists  |
          |  f           | t             |
          |  t           | f             |

    Examples:
        | fault_name                                                                    |
        | _update_config_begin                                                          |

    Scenario Outline: 8.2.1. rebalance - interrupt during switchover step (before invocation of 'gprecoverseg'), continue and retry failed step.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "yes" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Processing error status for switchover step" to logfile with latest timestamp
         And ggrebalance should print "Plan to retry step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When execute following sql in db "postgres" and store result in the context
            """
            select false as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where not is_rollback union select true as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where is_rollback;
            """
        Then validate that following rows are in the stored rows
          |  is_rollback | value_exists  |
          |  f           | t             |
          |  t           | f             |

    Examples:
        | fault_name                                                                    |
        | FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR                                   |
        | FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY                                   |
        | GpSegmentRebalanceOperation_rebalance_at_seg_stop                             |

    Scenario Outline: 8.2.2. rebalance - interrupt during switchover P->M step (before invocation of 'gprecoverseg'), continue and rollback failed step.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --no-progress --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Processing error status for switchover step" to logfile with latest timestamp
         And ggrebalance should print "Plan to rollback step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should not print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should not print "Rolled back steps:" to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:" to logfile with latest timestamp
         And ggrebalance should print " WARNINGS " to logfile with latest timestamp
         And ggrebalance should not print " Cancelled steps " to logfile with latest timestamp
         And ggrebalance should print "Cluster is left in unbalanced state" to logfile with latest timestamp
         And ggrebalance should print " Rolled back steps " to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When execute following sql in db "postgres" and store result in the context
            """
            select false as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where not is_rollback union select true as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where is_rollback;
            """
        Then validate that following rows are in the stored rows
          |  is_rollback | value_exists  |
          |  f           | t             |
          |  t           | t             |

    Examples:
        | fault_name                                                                    |
        | FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR                                   |

    Scenario: 8.2.2.1 rebalance - interrupt during switchover P->M , rollback failed step throws exception.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "GpSegmentRebalanceOperation_rebalance_at_seg_stop"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And user will answer "yes" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And the user runs psql with "-c 'SELECT gp_request_fts_probe_scan()'" against database "postgres"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Processing error status for switchover step" to logfile with latest timestamp
         And ggrebalance should print "Cannot perform rollback of this step. Some segment is down" to logfile with latest timestamp
         And ggrebalance should not print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should not print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should not print "Rolled back steps:		6" to logfile with latest timestamp
         And ggrebalance should not print " WARNINGS " to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:		6" to logfile with latest timestamp
         And ggrebalance should not print "Cluster might be not in fault tolerance mode!" to logfile with latest timestamp
         And ggrebalance should not print "Cluster is left in unbalanced state" to logfile with latest timestamp
         And ggrebalance should not print " Rolled back steps " to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'd'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When the user runs "gprecoverseg -a"
        Then gprecoverseg should return a return code of 0
        When the user runs "ggrebalance -r --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
    
     Scenario: 8.2.2.2 rebalance - interrupt during switchover P->M, rollback failed step after manual recovery proceeds.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "GpSegmentRebalanceOperation_rebalance_at_seg_stop"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And user will answer "yes" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And the user runs psql with "-c 'SELECT gp_request_fts_probe_scan()'" against database "postgres"
         And the user runs "gprecoverseg -a"
         And gprecoverseg should return a return code of 0
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Processing error status for switchover step" to logfile with latest timestamp
         And ggrebalance should not print "Cannot perform rollback of this step. Some segment is down" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should print "Rolled back steps:		6" to logfile with latest timestamp
         And ggrebalance should print " WARNINGS " to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:		6" to logfile with latest timestamp
         And ggrebalance should not print "Cluster might be not in fault tolerance mode!" to logfile with latest timestamp
         And ggrebalance should print "Cluster is left in unbalanced state" to logfile with latest timestamp
         And ggrebalance should print " Rolled back steps " to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When the user runs "ggrebalance -r --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"

    Scenario Outline: 8.2.3. rebalance - interrupt during switchover M->P step (before invocation of 'gprecoverseg'), continue and rollback failed step.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Processing error status for switchover step" to logfile with latest timestamp
         And ggrebalance should print "Plan to rollback step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When execute following sql in db "postgres" and store result in the context
            """
            select false as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where not is_rollback union select true as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where is_rollback;
            """
        Then validate that following rows are in the stored rows
          |  is_rollback | value_exists  |
          |  f           | t             |
          |  t           | t             |

    Examples:
        | fault_name                                                                    |
        | FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY                                   |

    Scenario Outline: 8.2.4. rebalance - interrupt during switchover P->M step (before invocation of 'gprecoverseg'), continue and cancel failed step.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --no-progress --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Retry step?"
         And user will answer "no" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Processing error status for switchover step" to logfile with latest timestamp
         And ggrebalance should print "Cancel step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should not print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should not print "Rolled back steps:" to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:" to logfile with latest timestamp
         And ggrebalance should print " WARNINGS " to logfile with latest timestamp
         And ggrebalance should print " Cancelled steps " to logfile with latest timestamp
         And ggrebalance should not print "Cluster might be not in fault tolerance mode!" to logfile with latest timestamp
         And ggrebalance should print "Cluster is left in unbalanced state" to logfile with latest timestamp
         And ggrebalance should not print " Rolled back steps " to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
        When execute following sql in db "postgres" and store result in the context
            """
            select false as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where not is_rollback union select true as is_rollback, count(1) > 0 as value_exists from ggrebalance.segment_move_steps where is_rollback;
            """
        Then validate that following rows are in the stored rows
          |  is_rollback | value_exists  |
          |  f           | t             |
          |  t           | f             |

    Examples:
        | fault_name                                                                    |
        | FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR                                   |
