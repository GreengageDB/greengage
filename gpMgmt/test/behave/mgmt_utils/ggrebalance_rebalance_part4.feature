# Keep this suite's CI runtime within the 100-120 minute budget.
# If it exceeds 120 minutes, move some scenarios or example cases into a new feature file.

@ggrebalance_rebalance @ggrebalance_rebalance_part4
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario Outline: 9.2. rebalance - rebalance interrupt, rollback (and interrupt again) and continue.
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
         And set fault inject "on_enter_STATE_REBALANCE_DONE_begin"
        When the user runs "ggrebalance --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "yes" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And verify the gp_segment_configuration has been restored
         And clear user's answers
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100

    Examples:
        | fault_name                                                                    |
        | on_enter_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED_begin                 |
        | on_enter_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED_end                   |
        | on_enter_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE_begin                    |
        | on_enter_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE_end                      |
        | on_enter_STATE_REBALANCE_EXECUTION_STARTED_begin                              |
        | on_enter_STATE_REBALANCE_EXECUTION_STARTED_end                                |
        | on_enter_STATE_REBALANCE_MOVES_SUCCEEDED_begin                                |
        | on_enter_STATE_REBALANCE_MOVES_SUCCEEDED_end                                  |
