@ggrebalance_rebalance
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario Outline: test 1. rebalance - check scenario, when we remove/add a host and rebalance the cluster (with different batch size).
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
        When the user runs "ggrebalance -B <batch_size> -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
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
        When the user runs "ggrebalance -B <batch_size> -x 6 --add-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
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
        | batch_size |
        | 1          |
        | 16         |
        | 64         |
        | 128        |

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
        When the user runs "ggrebalance -x 3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
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
         And set fault inject delay <fault_delay_ms> ms
        When the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And unset fault inject delay
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When the user runs "ggrebalance"
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

    Examples:
        | fault_name                                                                    | fault_delay_ms |
        | on_enter_STATE_REBALANCE_STARTED_begin                                        | 0              |
        | on_enter_STATE_REBALANCE_STARTED_end                                          | 0              |
        | on_enter_STATE_REBALANCE_MOVE_MIRRORS_STARTED_begin                           | 0              |
        | on_enter_STATE_REBALANCE_MOVE_MIRRORS_STARTED_end                             | 0              |
        | on_enter_STATE_REBALANCE_MOVE_MIRRORS_DONE_begin                              | 0              |
        | on_enter_STATE_REBALANCE_MOVE_MIRRORS_DONE_end                                | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED_begin | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED_end   | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE_begin    | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE_end      | 0              |
        | on_enter_STATE_REBALANCE_MOVE_PRIMARIES_STARTED_begin                         | 0              |
        | on_enter_STATE_REBALANCE_MOVE_PRIMARIES_STARTED_end                           | 0              |
        | on_enter_STATE_REBALANCE_MOVE_PRIMARIES_DONE_begin                            | 0              |
        | on_enter_STATE_REBALANCE_MOVE_PRIMARIES_DONE_end                              | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED_begin | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED_end   | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE_begin    | 0              |
        | on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE_end      | 0              |
        | FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR                                   | 0              |
        | FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY                                   | 0              |
        | on_enter_STATE_REBALANCE_DONE_begin                                           | 0              |
        | on_enter_STATE_REBALANCE_DONE_end                                             | 0              |
        | FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR                                   | 1500           |
        | FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR                                   | 3000           |
        | FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY                                   | 1500           |
        | FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY                                   | 3000           |
        | on_enter_STATE_REBALANCE_MOVE_MIRRORS_STARTED_begin                           | 3000           |
        | on_enter_STATE_REBALANCE_MOVE_PRIMARIES_STARTED_begin                         | 3000           |

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
        When the user runs "ggrebalance -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance"
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
         And set fault inject "on_enter_STATE_REBALANCE_MOVE_MIRRORS_STARTED_end"
        When the user runs "ggrebalance -x 4 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance"
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
