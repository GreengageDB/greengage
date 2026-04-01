@ggrebalance_basics @skip
Feature: ggrebalance behave tests

    Scenario Outline: test 1. validate incompatible option combinations
        Given a standard local demo cluster is running
         And the environment variable "COORDINATOR_DATA_DIRECTORY" is set from output of "echo $(dirname $(pwd))/gpAux/gpdemo/datadirs/qddir/demoDataDir-1"
         And coordinator data directory is updated
        When the user runs "ggrebalance <options>"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "<error_message>" to stdout
        Given <run_status>
        Then <restore_env>
         And coordinator data directory is updated
        
        Examples: Mutually exclusive options
          | options                                                     | error_message                                                         | run_status | restore_env|
          | --target-hosts sdw1,sdw2 --target-hosts-file /tmp/hosts.txt | Can't use together options '--target-hosts' and '--target-hosts-file' |  stub |stub|
          | --target-hosts sdw1,sdw2 --add-hosts sdw3                   | Can't use together options '--target-hosts' and '--add-hosts'         | stub       |stub|
          | --target-hosts sdw1,sdw2 --remove-hosts sdw3                | Can't use together options '--target-hosts' and '--remove-hosts'      | stub       |stub|
          | --target-hosts-file /tmp/hosts.txt --add-hosts sdw3         | Can't use together options '--target-hosts-file' and '--add-hosts'    |  stub      |stub|
          | --target-hosts-file /tmp/hosts.txt --remove-hosts sdw3      | Can't use together options '--target-hosts-file' and '--remove-hosts' |  stub      |stub|
          | --target-hosts-file /tmp/hosts.txt --add-hosts-file /tmp/add.txt    | Can't use together options '--target-hosts-file' and '--add-hosts-file'    |  stub      |stub|
          | --target-hosts-file /tmp/hosts.txt --remove-hosts-file /tmp/rm.txt  | Can't use together options '--target-hosts-file' and '--remove-hosts-file' |  stub      |stub|
          | --add-hosts sdw3 --add-hosts-file /tmp/add.txt              | Can't use together options '--add-hosts' and '--add-hosts-file'       | stub       |stub|
          | --remove-hosts sdw3 --remove-hosts-file /tmp/rm.txt         | Can't use together options '--remove-hosts' and '--remove-hosts-file' | stub       |stub|
          | --target-datadirs '/data/p/gpseg{content},/data/m/gpseg{content}' --target-datadirs-file /tmp/datadirs.txt | Can't use together options '--target-datadirs' and '--target-datadirs-file' | stub |stub|
          | --mirror-mode grouped --skip-rebalance                      | Can't use together options '--skip-rebalance' and '--mirror-mode'     | stub       |stub|
          | -m spread --skip-rebalance                                  | Can't use together options '--skip-rebalance' and '--mirror-mode'     | stub       |stub|
          | --skip-rebalance --inplace-swap-roles                       | Can't use together options '--skip-rebalance' and '--inplace-swap-roles'     | stub     |stub|
          | -c --target-hosts sdw1,sdw2                                 | Can't use together options '--clean-required' and '--target-hosts'    | stub       |stub|
          | -c --add-hosts sdw3                                         | Can't use together options '--clean-required' and '--add-hosts'       | stub       |stub|
          | -c --add-hosts-file /tmp/add.txt                            | Can't use together options '--clean-required' and '--add-hosts-file'  | stub       |stub|
          | -c --remove-hosts sdw3                                      | Can't use together options '--clean-required' and '--remove-hosts'       | stub       |stub|
          | -c --remove-hosts-file /tmp/rm.txt                          | Can't use together options '--clean-required' and '--remove-hosts-file'  | stub       |stub|
          | -c --target-datadirs '/data/p/gpseg{content},/data/m/gpseg{content}' | Can't use together options '--clean-required' and '--target-datadirs'   | stub    |stub|
          | -c -x 2                                                     | Can't use together options '--clean-required' and '--target-segment-count' | stub    |stub|
          | -c --mirror-mode grouped                                    | Can't use together options '--clean-required' and '--mirror-mode'       | stub     |stub|
          | -c --skip-rebalance                                         | Can't use together options '--clean-required' and '--skip-rebalance'    | stub     |stub|
          | -c --show-plan                                              | Can't use together options '--clean-required' and '--show-plan'         | stub     |stub|
          | -c --analyze                                                | Can't use together options '--clean-required' and '--analyze'           | stub     |stub|
          | -c --replay-lag 1                                           | Can't use together options '--clean-required' and '--replay-lag'        | stub     |stub|
          | -c --hba-hostnames                                          | Can't use together options '--clean-required' and '--hba-hostnames'     | stub     |stub|
          | -c --inplace-swap-roles                                         | Can't use together options '--clean-required' and '--inplace-swap-roles'     | stub     |stub|
          | -c --skip-resource-estimation                               | Can't use together options '--clean-required' and '--skip-resource-estimation' | stub |stub|
          | -r --target-hosts sdw1,sdw2                                 | Can't use together options '--rollback-required' and '--target-hosts'    | stub       |stub|
          | -r --add-hosts sdw3                                         | Can't use together options '--rollback-required' and '--add-hosts'       | stub       |stub|
          | -r --add-hosts-file /tmp/add.txt                            | Can't use together options '--rollback-required' and '--add-hosts-file'  | stub       |stub|
          | -r --remove-hosts sdw3                                      | Can't use together options '--rollback-required' and '--remove-hosts'       | stub       |stub|
          | -r --remove-hosts-file /tmp/rm.txt                          | Can't use together options '--rollback-required' and '--remove-hosts-file'  | stub       |stub|
          | -r --target-datadirs '/data/p/gpseg{content},/data/m/gpseg{content}' | Can't use together options '--rollback-required' and '--target-datadirs'   | stub    |stub|
          | -r -x 2                                                     | Can't use together options '--rollback-required' and '--target-segment-count' | stub    |stub|
          | -r --mirror-mode grouped                                    | Can't use together options '--rollback-required' and '--mirror-mode'       | stub     |stub|
          | -r --skip-rebalance                                         | Can't use together options '--rollback-required' and '--skip-rebalance'    | stub     |stub|
          | -r --show-plan                                              | Can't use together options '--rollback-required' and '--show-plan'         | stub     |stub|
          | -r --inplace-swap-roles                                     | Can't use together options '--rollback-required' and '--inplace-swap-roles'     | stub     |stub|
          | -r --skip-resource-estimation                               | Can't use together options '--rollback-required' and '--skip-resource-estimation' |the database is not running |"COORDINATOR_DATA_DIRECTORY" environment variable should be restored|

    Scenario: test 2. ggrebalance simple scenarios
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2"
         And segment information for content 1 is saved in context
         And segment information for content 2 is saved in context
         And segment information for content 3 is saved in context
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 4 --skip-rebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Skipping rebalance" to logfile with latest timestamp
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Cleanup is not required." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -x 2 --skip-rebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And verify no segment running for saved segment information
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Previous run was completed successfully. Please execute cleanup before a new run." to logfile with latest timestamp
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "No steps to rollback found for rebalance" to logfile with latest timestamp

    Scenario: test 3. check cleanup after the target segment count was updated
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_end"
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance -c -y"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Reset numsegments to default is done." to logfile with latest timestamp
         And ggrebalance should print "Cleanup is complete" to logfile with latest timestamp

    Scenario: test 4. check cleanup after shrink is complete, and rebalance was interrupted
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
        When the user runs "ggrebalance -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cleanup is complete" to logfile with latest timestamp
         # some mirrors are definitely down, so do not check them
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 4, row count = 100
