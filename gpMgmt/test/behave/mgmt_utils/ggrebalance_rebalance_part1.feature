# Keep this suite's CI runtime within the 100-120 minute budget.
# If it exceeds 120 minutes, move some scenarios or example cases into a new feature file.

@ggrebalance_rebalance @ggrebalance_rebalance_part1
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario Outline: test 1. rebalance - check scenario, when we remove/add a host and rebalance the cluster (with different parallel and batch size).
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
        When the user runs "ggrebalance --non-interactive-mode --parallel <parallel_size> --batch-size <batch_size> -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
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
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 6"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Previous run was completed successfully. Please execute cleanup before a new run" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
        And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 6"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cluster is already balanced, no segment moves will be held." to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
        And all files in gpAdminLogs directory are deleted
        When set fault inject "on_enter_STATE_REBALANCE_PREPARE_MOVES_DONE_end"
         And the user runs "ggrebalance --non-interactive-mode --parallel <parallel_size> --batch-size <batch_size> -x 6 --add-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And the user runs "psql -d postgres -c 'SELECT move_order, status FROM ggrebalance.segment_move_steps ORDER BY 1' -o /tmp/move_order.out"
        Then verify that the file "/tmp/move_order.out" contains text
"""
 move_order |      status      
------------+------------------
          0 | PLANNED
          1 | PLANNED
          2 | APPROVE_REQUIRED
          3 | APPROVE_REQUIRED
          4 | PLANNED
          5 | PLANNED
          6 | APPROVE_REQUIRED
          7 | APPROVE_REQUIRED
          8 | PLANNED
(9 rows)


"""
         And unset fault inject
         And the temporary file "/tmp/move_order.out" is removed
        When the user runs "ggrebalance --non-interactive-mode --parallel <parallel_size> --batch-size <batch_size>"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 6"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Previous run was completed successfully. Please execute cleanup before a new run" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 6"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cluster is already balanced, no segment moves will be held." to logfile with latest timestamp

    Examples:
        | parallel_size | batch_size |
        | 1             | 1          |
        | 64            | 64         |
        | 96            | 128        |

    Scenario: test 2. rebalance - check rebalance after shrink.
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
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -x 3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should print "Tables shrunk:\s*4" regex to logfile
         And ggrebalance should not print "Bytes processed:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink rate:" to logfile with latest timestamp
         And ggrebalance should print "Shrink total time:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rolled back:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rollback rate:" to logfile with latest timestamp
         And ggrebalance should not print "Rollback total time:" to logfile with latest timestamp
         And ggrebalance should print "Segments moved:\s*\d+" regex to logfile
         And ggrebalance should print "Rolled back steps:\s*0" regex to logfile
         And ggrebalance should print "Cancelled steps:\s*0" regex to logfile
         And ggrebalance should not print " WARNINGS " to logfile with latest timestamp
         And the cluster configuration has 1 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 3, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 3, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 3, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 3, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 3, row count = 100
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 3"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Cluster is already balanced, no segment moves will be held." to logfile with latest timestamp

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
