# Keep this suite's CI runtime within the 100-120 minute budget.
# If it exceeds 120 minutes, move some scenarios or example cases into a new feature file.

@ggrebalance_rebalance @ggrebalance_rebalance_part3
Feature: ggrebalance behave tests (rebalance scenarios)

    Scenario Outline: 8.3.1. rebalance - interrupt during mirror move after gp_segment_configuration update, but before port update, continue and cancel failed step.
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
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Cancel step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should not print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should not print "Rolled back steps:" to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:" to logfile with latest timestamp
         And ggrebalance should print " WARNINGS " to logfile with latest timestamp
         And ggrebalance should print " Cancelled steps " to logfile with latest timestamp
         And ggrebalance should print "Cluster might be not in fault tolerance mode!" to logfile with latest timestamp
         And ggrebalance should print "These segments should be started manually in order cluster to become fault tolerant:" to logfile with latest timestamp
         And ggrebalance should print "Cluster is left in unbalanced state" to logfile with latest timestamp
         And ggrebalance should not print " Rolled back steps " to logfile with latest timestamp
         And clear user's answers
         # some mirrors are definitely down, so do not check them
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
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
        | _update_config_end                                                            |

    Scenario Outline: 8.3.2. rebalance - interrupt during mirror move after gp_segment_configuration update, but before port update, continue and rollback failed step.
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
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: False" to logfile with latest timestamp
         And ggrebalance should print "Plan to rollback step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
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
        | _update_config_end                                                            |

    Scenario Outline: 8.4.1. rebalance - interrupt during mirror move after port update (when the mirror is actually started), and continue.
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
        When the user runs "ggrebalance --non-interactive-mode -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "Start checking if segment is up with timeout" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: True" to logfile with latest timestamp
         And ggrebalance should print "The step is complete, mark it as done" to logfile with latest timestamp
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
        | _do_recovery_end                                                              |

    Scenario Outline: 8.4.2.1 rebalance - interrupt during mirror move after port update (but before the mirror is actually started), and continue (with retry).
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
         And on host "sdw1" set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And on host "sdw1" unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Timeout waiting for segment start, wait again?"
         And user will answer "yes" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "Start checking if segment is up with timeout" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: True" to logfile with latest timestamp
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
        | start_segment_begin                                                           |

    Scenario Outline: 8.4.2.2 rebalance - interrupt during mirror move after port update (but before the mirror is actually started), and continue (with step rollback).
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
         And on host "sdw1" set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And on host "sdw1" unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Timeout waiting for segment start, wait again?"
         And user will answer "no" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "Start checking if segment is up with timeout" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Plan to rollback step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
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
        | start_segment_begin                                                           |

    Scenario Outline: 8.4.2.3. rebalance - interrupt during mirror move after port update (but before the mirror is actually started), and continue (with step cancel).
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
         And on host "sdw1" set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And on host "sdw1" unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Timeout waiting for segment start, wait again?"
         And user will answer "no" to the prompt "Retry step?"
         And user will answer "no" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "Start checking if segment is up with timeout" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Cancel step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And clear user's answers
         # some mirrors are definitely down, so do not check them
         And the cluster configuration has 2 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
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
        | start_segment_begin                                                           |

    Scenario Outline: 8.4.2.4. rebalance - interrupt during mirror move after port update (but before the mirror is actually started), and continue (with retry) and double fault with retry max count hit.
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
         And on host "sdw1" set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -n 1 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When user will answer "no" to the prompt "Timeout waiting for segment start, wait again?"
         And user will answer "yes" to the prompt "Retry step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "Start checking if segment is up with timeout" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Plan to retry step" to logfile with latest timestamp
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
         And on host "sdw1" unset fault inject
         And user will answer "yes" to the prompt "Rollback step?"
         And user will answer "yes" to the prompt "Approve switchovers?"
         And user will answer "yes" to the prompt "Proceed with continue?"
        When the user runs "ggrebalance -n 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Checking error status for step" to logfile with latest timestamp
         And ggrebalance should print "Start checking if segment is up with timeout" to logfile with latest timestamp
         And ggrebalance should print "gp_segment_configuration is updated: True" to logfile with latest timestamp
         And ggrebalance should print "Port is updated: True" to logfile with latest timestamp
         And ggrebalance should print "We've run out of retry attempts" to logfile with latest timestamp
         And ggrebalance should print "Plan to rollback step" to logfile with latest timestamp
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And clear user's answers
         And the cluster configuration has 3 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 1 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 3 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
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
        | start_segment_begin                                                           |

    Scenario Outline: 9.1. rebalance - rebalance interrupt and full rollback.
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
         And set fault inject "<fault_name>"
        When the user runs "ggrebalance --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And all files in gpAdminLogs directory are deleted
         And the gprecoverseg lock directory is removed
        When the user runs "ggrebalance --non-interactive-mode -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance rollback is complete" to logfile with latest timestamp
         And verify the gp_segment_configuration has been restored
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 6, row count = 100

    Examples:
        | fault_name                                                                    |
        | on_enter_STATE_REBALANCE_PREPARE_MOVES_STARTED_begin                          |
        | on_enter_STATE_REBALANCE_EXECUTION_STARTED_begin                              |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED_begin  |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED_end    |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE_begin     |
        | on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE_end       |
        | FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR                                   |
        | FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY                                   |
        | on_enter_STATE_REBALANCE_DONE_begin                                           |
