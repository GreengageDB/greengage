@ggrebalance_switchover_retry_limit @skip
Feature: ggrebalance behave tests (retry limit of a failed switchover)

    Scenario: rebalance - a switchover that keeps failing must hit the retry limit, like a mirror move does.
        Given the database is not running
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast'"
         And the user runs command "gpssh -h sdw1 -h sdw2 -h sdw3 -e 'rm -rf /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR"
        When the user runs "ggrebalance --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        # 2nd attempt (1st retry), the fault is still set.
        When the user runs "ggrebalance --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Plan to retry step" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        # 3rd run: the step has failed twice, the retry limit must be hit.
        When the user runs "ggrebalance --non-interactive-mode"
        Then ggrebalance should print "We've run out of retry attempts" to logfile with latest timestamp
         And ggrebalance should not print "Plan to retry step" to logfile with latest timestamp
         And unset fault inject
