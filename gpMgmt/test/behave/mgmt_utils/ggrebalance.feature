@ggrebalance
Feature: ggrebalance behave tests

    Scenario Outline: test shrink continue after cluster restart
    Given the database is not running
    And a working directory of the test as '/data/gpdata/ggrebalance'
    And a cluster is created with mirrors on "cdw" and "sdw1"
    And all files in gpAdminLogs directory are deleted
    And database "test_db_1" exists
    And schema "test_schema_1" exists in "test_db_1"
    And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
    And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
    And database "test_db_2" exists
    And schema "test_schema_2" exists in "test_db_2"
    And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
    And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
    When set fault inject "<fault_name>"
    And the user runs "ggrebalance -x 1"
    Then ggrebalance should return a return code of 1
    And ggrebalance should print "ggrebalance failed" to logfile with latest timestamp
    And unset fault inject
    When the user runs "gpstop -arf"
    Then gpstart should return a return code of 0
    When there is a "heap" table "test_schema_2.test_table_3" in "test_db_2" with data
    And the user runs "ggrebalance -x 1"
    Then ggrebalance should print "Cluster restarted after previous run, trying to repopulate the relation queue" to logfile
    And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
    And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 1, row count = 100
    And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 1, row count = 100
    And distribution information from table "test_schema_2.test_table_3" with data in "test_db_2" is equal to segment count = 1, row count = 1094
    And ggrebalance should return a return code of 0
    Examples:
        | fault_name                                                                  |
        | on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE_begin    |
        | on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin                             |
        | on_enter_STATE_SHRINK_TABLES_DONE_begin                                     |
