@ggrebalance_shrink
Feature: ggrebalance behave tests

    Scenario: test 1.1. shrink - check continue after interrupted state, if interruption is done before the rebalance schema creation
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_SETUP_SCHEMA_STARTED_begin"
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
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Rebalance schema doesn't exists and no shrink plan is supplied. Please specify shrink plan." to logfile with latest timestamp
        When the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
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

    Scenario Outline: test 1.2. shrink - check continue after interrupted state
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And there is a "heap" partition table "test_schema_1.part_test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" partition table "test_schema_1.part_test_table_2" in "test_db_1" with "100" rows
         And there is an unlogged "heap" table "test_schema_1.unlogged_test_table_1" in "test_db_1" with "100" rows
         And a materialized view "test_schema_1.mv_test_table_1" exists on table "test_schema_1.test_table_1"
         And the user connects to "gptest" with named connection "test_connection"
         And the user executes "CREATE TEMP TABLE temp_table(a int);" with named connection "test_connection"
         And database "gptest" exists
         And the user create a writable external table with name "ext_test"
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
        When the user runs "ggrebalance -x 1 --parallel 1 --batch-size 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And the user drops the named connection "test_connection"
        When execute following sql in db "postgres" and store result in the context
            """
            select count(1) as temp_tables_for_redistribute from ggrebalance.table_rebalance_status_detail where schema_name LIKE 'pg\_temp\_%';
            """
        Then validate that following rows are in the stored rows
          |  temp_tables_for_redistribute  |
          |  0                             |
        When the user runs "ggrebalance -x 1 --parallel 1 --batch-size 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Can't start a new operation, because the previous one was interrupted" to logfile with latest timestamp
        When the user runs "ggrebalance -x 1 --parallel 1 --batch-size 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Can't start a new operation, because the previous one was interrupted" to logfile with latest timestamp
        When the user runs "ggrebalance --parallel 1 --batch-size 1"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
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
        | fault_name                                                                  |
        | on_enter_STATE_SETUP_SCHEMA_STARTED_end                                     |
        | on_enter_STATE_SETUP_SCHEMA_DONE_begin                                      |
        | on_enter_STATE_SETUP_SCHEMA_DONE_end                                        |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_begin |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_end   |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_begin    |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_end      |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_begin                          |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end                            |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin                             |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_end                               |
        | on_enter_STATE_SHRINK_TABLES_STARTED_begin                                  |
        | on_enter_STATE_SHRINK_TABLES_STARTED_end                                    |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                                     |
        | on_enter_STATE_SHRINK_TABLES_DONE_end                                       |
        | on_enter_STATE_SHRINK_CATALOG_STARTED_begin                                 |
        | on_enter_STATE_SHRINK_CATALOG_STARTED_end                                   |
        | on_enter_STATE_SHRINK_CATALOG_DONE_begin                                    |
        | on_enter_STATE_SHRINK_CATALOG_DONE_end                                      |
        | on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED_begin                           |
        | on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED_end                             |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1                  |
        | fault_segment_stop_dbid_3                                                   |

    Scenario Outline: test 1.3. test shrink continue after cluster restart
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
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
        When set fault inject "<fault_name>"
         And the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "gpstop -arf"
        Then gpstart should return a return code of 0
        When there is a "heap" table "test_schema_2.test_table_3" in "test_db_2" with data
         And the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And verify no segment running for saved segment information
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And the numsegments of table "ext_test" is 1
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_2.test_table_3" with data in "test_db_2" is equal to segment count = 1, row count = 1094

    Examples:
        | fault_name                                                                  |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_begin    |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin                             |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                                     |
        | on_enter_STATE_SHRINK_TABLES_DONE_end                                       |
        | on_enter_STATE_SHRINK_CATALOG_STARTED_begin                                 |
        | on_enter_STATE_SHRINK_CATALOG_STARTED_end                                   |

    Scenario: test 2.1. shrink - check rollback after interrupted state, if interruption is done before the rebalance schema creation
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_SETUP_SCHEMA_STARTED_begin"
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
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance schema doesn't exist. Can't perform rollback." to logfile with latest timestamp
        When the user runs "ggrebalance -c"
        Then ggrebalance should return a return code of 0
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

    Scenario Outline: test 2.2. shrink - check rollback after interrupted state
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
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
        When the user runs "ggrebalance -x 1 --parallel 1 --batch-size 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
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
        | fault_name                                                                  |
        | on_enter_STATE_SETUP_SCHEMA_STARTED_end                                     |
        | on_enter_STATE_SETUP_SCHEMA_DONE_begin                                      |
        | on_enter_STATE_SETUP_SCHEMA_DONE_end                                        |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_begin |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_end   |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_begin    |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_end      |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_begin                          |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end                            |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin                             |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_end                               |
        | on_enter_STATE_SHRINK_TABLES_STARTED_begin                                  |
        | on_enter_STATE_SHRINK_TABLES_STARTED_end                                    |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                                     |
        | on_enter_STATE_SHRINK_TABLES_DONE_end                                       |
        | on_enter_STATE_SHRINK_CATALOG_STARTED_begin                                 |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1                  |

    Scenario Outline: test 2.3. shrink - check rollback after interrupted state (interruption is done after the point of no return). Rollback fails. So just continue shrink.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
         And set fault inject "<fault_name>"
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
        When the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Can't perform rollback as the catalog is already updated" to logfile with latest timestamp
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
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
        | fault_name                                                                  |
        | on_enter_STATE_SHRINK_CATALOG_STARTED_end                                   |
        | on_enter_STATE_SHRINK_CATALOG_DONE_begin                                    |
        | on_enter_STATE_SHRINK_CATALOG_DONE_end                                      |
        | on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED_begin                           |
        | on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED_end                             |
        | fault_segment_stop_dbid_3                                                   |

    Scenario Outline: test 2.4. shrink - check rollback after interrupted state and cluster restart
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
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
        When set fault inject "<fault_name>"
         And the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And there is a "heap" table "test_schema_2.test_table_3" in "test_db_2" with "200" rows
        When the user runs "gpstop -arf"
        Then gpstart should return a return code of 0
        When there is a "heap" table "test_schema_2.test_table_4" in "test_db_2" with "300" rows
         And the user runs "ggrebalance -r"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And the numsegments of table "ext_test" is 2
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_3" with data in "test_db_2" is equal to segment count = 2, row count = 200
         And distribution information from table "test_schema_2.test_table_4" with data in "test_db_2" is equal to segment count = 2, row count = 300
    Examples:
        | fault_name                                                                  |
        | on_enter_STATE_SETUP_SCHEMA_STARTED_end                                     |
        | on_enter_STATE_SETUP_SCHEMA_DONE_begin                                      |
        | on_enter_STATE_SETUP_SCHEMA_DONE_end                                        |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_begin |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED_end   |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_begin    |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_end      |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_begin                          |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_end                            |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin                             |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_end                               |
        | on_enter_STATE_SHRINK_TABLES_STARTED_begin                                  |
        | on_enter_STATE_SHRINK_TABLES_STARTED_end                                    |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                                     |
        | on_enter_STATE_SHRINK_TABLES_DONE_end                                       |
        | on_enter_STATE_SHRINK_CATALOG_STARTED_begin                                 |
        | fault_rebalance_table_test_db_2.test_schema_2.test_table_1                  |

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
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
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
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
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

    Scenario Outline: test 3.4. shrink - check continue after interrupted rollback state and cluster restart
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
         And there is a "heap" table "test_schema_2.test_table_3" in "test_db_2" with "200" rows
        When the user runs "gpstop -arf"
        Then gpstart should return a return code of 0
        When there is a "heap" table "test_schema_2.test_table_4" in "test_db_2" with "300" rows
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 2, row count = 100
         And the numsegments of table "ext_test" is 2
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 2, row count = 100
         And distribution information from table "test_schema_2.test_table_3" with data in "test_db_2" is equal to segment count = 2, row count = 200
         And distribution information from table "test_schema_2.test_table_4" with data in "test_db_2" is equal to segment count = 2, row count = 300
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

    Scenario: test 4. test shrink continue, when a table planned for rebalance was dropped
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
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
        When set fault inject "fault_rebalance_table_test_db_2.test_schema_2.test_table_1"
         And the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And table "test_schema_2.test_table_1" is dropped in "test_db_2"
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And verify no segment running for saved segment information
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And the numsegments of table "ext_test" is 1
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 1, row count = 100

    Scenario: test 4.1. test shrink continue, when a mat view planned for rebalance was dropped
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
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
        When set fault inject "fault_rebalance_table_test_db_1.test_schema_1.mv_test_table_1"
         And the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And materialized view "test_schema_1.mv_test_table_1" is dropped in "test_db_1"
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And verify no segment running for saved segment information
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And the numsegments of table "ext_test" is 1
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 1, row count = 100

    Scenario: test 5. test shrink continue, when a db with the table planned for rebalance was dropped
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
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
        When set fault inject "fault_rebalance_table_test_db_2.test_schema_2.test_table_1"
         And the user runs "ggrebalance -x 1 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
         And unset fault inject
         And the database "test_db_2" does not exist
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And verify no segment running for saved segment information
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And the numsegments of table "ext_test" is 1

    Scenario: test 6. test shrink, when a table, planned for rebalance, is dropped in a parallel transaction, committed after the start of table redistribution
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And segment information for content 1 is saved in context
         And all files in gpAdminLogs directory are deleted
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And there is a "heap" partition table "test_schema_1.part_test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" partition table "test_schema_1.part_test_table_2" in "test_db_1" with "100" rows
         And there is an unlogged "heap" table "test_schema_1.unlogged_test_table_1" in "test_db_1" with "100" rows
         And a materialized view "test_schema_1.mv_test_table_1" exists on table "test_schema_1.test_table_1"
         And database "gptest" exists
         And there is a "heap" table "test_table_1" in "gptest" with "100" rows
         And the user create a writable external table with name "ext_test"
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And set fault inject "on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_begin"
         And set fault inject type to suspend
        When the user asynchronously runs "ggrebalance -x 1 --skip-rebalance" and the process is saved
         And the user waits till ggrebalance prints "Updated target segment count to 1" in the logs (with timeout of "60" sec)
         And the user connects to "gptest" with named connection "test_connection"
         And the user executes "BEGIN; DROP TABLE test_table_1;" with named connection "test_connection"
         And unset fault inject
         And the user waits till ggrebalance prints "Start table rebalance for \"gptest\".\"public\".\"test_table_1\" to 1 segments" in the logs (with timeout of "60" sec)
         And waiting "5" seconds
         And the user executes "COMMIT;" with named connection "test_connection"
         And the user drops the named connection "test_connection"
        Then the async process finished with a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And verify no segment running for saved segment information
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.part_test_table_2" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.unlogged_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_1.mv_test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 1, row count = 100
         And the numsegments of table "ext_test" is 1
