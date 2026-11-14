@ggrebalance_rollback_mirror_move
Feature: ggrebalance behave tests (rollback of mirror move steps)

    Scenario: rebalance - interrupt rebalance after mirror moves, interrupt rollback during mirror move before gp_segment_configuration update, and continue rollback.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And the gp_segment_configuration have been saved
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And all files in gpAdminLogs directory are deleted
        # Interrupt the rebalance right after the first batch (mirror moves) is done, before the first
        # switchover. This way the rollback consists of mirror moves only.
         And set fault inject "on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED_begin"
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        # Fail the rollback mirror moves inside gpmovemirrors, before it runs gprecoverseg (the mirrors are still up).
        # The catalog still points to the forward target of the moves.
        When set fault inject "FAULT_GPMOVEMIRRORS_BEFORE_GPRECOVERSEG"
         And the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        # Continue the rollback. The failed rollback step must be detected as not done and retried.
        When the user runs "ggrebalance --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And verify the gp_segment_configuration has been restored
         And ggrebalance should print "gp_segment_configuration is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Plan to retry rollback step" to logfile with latest timestamp
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
