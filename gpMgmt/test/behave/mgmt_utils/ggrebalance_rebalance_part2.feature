@ggrebalance_rebalance @ggrebalance_rebalance_part2
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario Outline: 3. rebalance - interrupt and continue.
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
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When the user runs "ggrebalance --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should not print "Tables shrunk:" to logfile with latest timestamp
         And ggrebalance should not print "Bytes processed:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink rate:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink total time:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rolled back:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rollback rate:" to logfile with latest timestamp
         And ggrebalance should not print "Rollback total time:" to logfile with latest timestamp
         And ggrebalance should print "Segments moved:\s*\d+" regex to logfile
         And ggrebalance should print "Rolled back steps:\s*0" regex to logfile
         And ggrebalance should print "Cancelled steps:\s*0" regex to logfile
         And ggrebalance should not print " WARNINGS " to logfile with latest timestamp
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

    Examples:
        | fault_name                                                                    |
        | on_enter_STATE_REBALANCE_STARTED_begin                                        |
        | on_enter_STATE_REBALANCE_STARTED_end                                          |
        | on_enter_STATE_REBALANCE_PREPARE_MOVES_STARTED_begin                          |
        | on_enter_STATE_REBALANCE_PREPARE_MOVES_STARTED_end                            |
        | on_enter_STATE_REBALANCE_PREPARE_MOVES_DONE_begin                             |
        | on_enter_STATE_REBALANCE_PREPARE_MOVES_DONE_end                               |
        | on_enter_STATE_REBALANCE_EXECUTION_STARTED_begin                              |
        | on_enter_STATE_REBALANCE_EXECUTION_STARTED_end                                |
        | on_enter_STATE_REBALANCE_MOVES_SUCCEEDED_begin                                |
        | on_enter_STATE_REBALANCE_MOVES_SUCCEEDED_end                                  |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED_begin  |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED_end    |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE_begin     |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE_end       |
        | on_enter_STATE_REBALANCE_EXECUTION_DONE_begin                                 |
        | on_enter_STATE_REBALANCE_EXECUTION_DONE_end                                   |
        | on_enter_STATE_REBALANCE_DONE_begin                                           |
        | on_enter_STATE_REBALANCE_DONE_end                                             |

