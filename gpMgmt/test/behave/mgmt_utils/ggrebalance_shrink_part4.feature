@ggrebalance_shrink @ggrebalance_shrink_part4
Feature: ggrebalance behave tests

    Scenario Outline: test 3.1. shrink - check continue after interrupted rollback state. In this case we fail in rollback too early, and normal shrink will be complete.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name_shrink>"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And there is a "heap" partition table "test_schema_1.part_test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" partition table "test_schema_1.part_test_table_2" in "test_db_1" with "100" rows
         And there is an unlogged "heap" table "test_schema_1.unlogged_test_table_1" in "test_db_1" with "100" rows
         And a materialized view "test_schema_1.mv_test_table_1" exists on table "test_schema_1.test_table_1"
         And database "gptest" exists
         And the user create a writable external table with name "ext_test"
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And set fault inject "<fault_name_rollback>"
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And clear user's answers
         And verify no segment running for saved segment information
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And the numsegments of table "ext_test" is 1
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 1, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 1, row count = 100

    Examples:
        | fault_name_shrink                                          | fault_name_rollback                                                     |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START_begin |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START_begin |

    Scenario Outline: test 3.2. shrink - check continue after interrupted rollback state
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name_shrink>"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And there is a "heap" partition table "test_schema_1.part_test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" partition table "test_schema_1.part_test_table_2" in "test_db_1" with "100" rows
         And there is an unlogged "heap" table "test_schema_1.unlogged_test_table_1" in "test_db_1" with "100" rows
         And a materialized view "test_schema_1.mv_test_table_1" exists on table "test_schema_1.test_table_1"
         And database "gptest" exists
         And the user create a writable external table with name "ext_test"
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And set fault inject "<fault_name_rollback>"
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When user will answer "yes" to the prompt "Proceed with continue?"
         And the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And clear user's answers
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And the numsegments of table "ext_test" is 2
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 2, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 2, row count = 100

    Examples:
        | fault_name_shrink                                          | fault_name_rollback                                                     |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START_end   |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE_begin  |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE_end    |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START_begin               |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START_end                 |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE_begin                |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE_end                  |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START_begin              |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START_end                |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE_begin               |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE_end                 |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START_begin                  |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START_end   |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE_begin  |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE_end    |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START_begin               |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START_end                 |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE_begin                |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE_end                  |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START_begin              |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START_end                |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE_begin               |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE_end                 |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START_begin                  |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                    | fault_rebalance_table_test_db_2.test_schema_2.test_table_1              |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                    | before_set_status_test_db_2.test_schema_2.test_table_1                  |

    Scenario Outline: test 3.3. shrink - check continue after interrupted rollback state (interruption is done after rebalance schema is dropped)
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name_shrink>"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And there is a "heap" partition table "test_schema_1.part_test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" partition table "test_schema_1.part_test_table_2" in "test_db_1" with "100" rows
         And there is an unlogged "heap" table "test_schema_1.unlogged_test_table_1" in "test_db_1" with "100" rows
         And a materialized view "test_schema_1.mv_test_table_1" exists on table "test_schema_1.test_table_1"
         And database "gptest" exists
         And the user create a writable external table with name "ext_test"
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And set fault inject "<fault_name_rollback>"
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Rebalance schema doesn't exists and no shrink plan is supplied. Please specify shrink plan." to logfile with latest timestamp
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And the numsegments of table "ext_test" is 2
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 2, row count = 100
        When there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "100" rows
        Then distribution information from table "test_schema_1.test_table_3" with data in "test_db_1" is equal to segment count = 2, row count = 100

    Examples:
        | fault_name_shrink                                          | fault_name_rollback                                                     |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START_end                    |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE_begin                   |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end           | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE_end                     |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START_end                    |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE_begin                   |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1 | on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE_end                     |

