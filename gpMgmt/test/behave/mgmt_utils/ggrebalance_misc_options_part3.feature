@ggrebalance_misc_options @ggrebalance_misc_options_part3
Feature: ggrebalance behave tests (misc options scenarios)

    Scenario: test 21. Check '--no-progress', '--simple-progress' and '--detailed-progress' options.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_0" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_2" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_4" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_5" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_6" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_7" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_8" in "test_db_1" with "50000" rows
         And there is a "heap" table "test_schema_1.test_table_9" in "test_db_1" with "50000" rows
         And all files in gpAdminLogs directory are deleted

         # Check for '--no-progress'
         And set fault inject "on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin"
        When the user runs "ggrebalance --no-progress --non-interactive-mode -n 1 -x 3 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT to_regclass('ggrebalance.rebalance_progress') IS NOT NULL AS progress_view_exists;
            """
        Then validate that following rows are in the stored rows
          |  progress_view_exists  |
          |  f                     |
        When the user runs "ggrebalance -r -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And ggrebalance should print "Skip final shrink summary report" to logfile with latest timestamp
         And ggrebalance should not print "Tables shrunk:" to logfile with latest timestamp
         And ggrebalance should not print "Bytes processed:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink rate:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink total time:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rolled back:" to logfile with latest timestamp
         And ggrebalance should not print "Tables rollback rate:" to logfile with latest timestamp
         And ggrebalance should not print "Rollback total time:" to logfile with latest timestamp
         And ggrebalance should not print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should not print "Rolled back steps:" to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:" to logfile with latest timestamp
         And ggrebalance should not print " WARNINGS " to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted

         # Checks for '--simple-progress'
         # Check progress report before any table is redistributed
         And set fault inject "on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin"
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -n 1 -x 3 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value |
          |  1.1. Tables shrunk              | 0          |
          |  1.2. Tables shrink in progress  | 0          |
          |  1.3. Tables left to shrink      | 10         |
         And all files in gpAdminLogs directory are deleted

         # Check progress report before finish of first table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_0"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value |
          |  1.1. Tables shrunk              | 0          |
          |  1.2. Tables shrink in progress  | 1          |
          |  1.3. Tables left to shrink      | 9          |
         And all files in gpAdminLogs directory are deleted

         # Check progress report when some tables have been redistributed
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_3"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value |
          |  1.1. Tables shrunk              | 3          |
          |  1.2. Tables shrink in progress  | 1          |
          |  1.3. Tables left to shrink      | 6          |
         And all files in gpAdminLogs directory are deleted

         # Check progress report before finish of last table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_9"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value |
          |  1.1. Tables shrunk              | 9          |
          |  1.2. Tables shrink in progress  | 1          |
          |  1.3. Tables left to shrink      | 0          |
         And all files in gpAdminLogs directory are deleted

         # Check progress report when all tables have been redistributed
         And set fault inject "on_enter_STATE_SHRINK_TABLES_DONE_end"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value |
          |  1.1. Tables shrunk              | 10         |
          |  1.2. Tables shrink in progress  | 0          |
          |  1.3. Tables left to shrink      | 0          |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback before any table is rolled back
         And set fault inject "on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE_end"
        When the user runs "ggrebalance -r -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value |
          |  1.1. Tables rolled back          | 0          |
          |  1.2. Tables rollback in progress | 0          |
          |  1.3. Tables left to rollback     | 10         |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback before finish of first table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_0"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value |
          |  1.1. Tables rolled back          | 0          |
          |  1.2. Tables rollback in progress | 1          |
          |  1.3. Tables left to rollback     | 9          |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback when some tables have been redistributed
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_3"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value |
          |  1.1. Tables rolled back          | 3          |
          |  1.2. Tables rollback in progress | 1          |
          |  1.3. Tables left to rollback     | 6          |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback before finish of last table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_9"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value |
          |  1.1. Tables rolled back          | 9          |
          |  1.2. Tables rollback in progress | 1          |
          |  1.3. Tables left to rollback     | 0          |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback when all tables have been redistributed
         And set fault inject "on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE_begin"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value |
          |  1.1. Tables rolled back          | 10         |
          |  1.2. Tables rollback in progress | 0          |
          |  1.3. Tables left to rollback     | 0          |
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And ggrebalance should print "Tables shrunk:\s*10" regex to logfile
         And ggrebalance should not print "Bytes processed:" to logfile with latest timestamp
         And ggrebalance should not print "Shrink rate:" to logfile with latest timestamp
         And ggrebalance should print "Shrink total time:" to logfile with latest timestamp
         And ggrebalance should print "Tables rolled back:\s*10" regex to logfile
         And ggrebalance should not print "Tables rollback rate:" to logfile with latest timestamp
         And ggrebalance should print "Rollback total time:" to logfile with latest timestamp
         And ggrebalance should not print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should not print "Rolled back steps:" to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:" to logfile with latest timestamp
         And ggrebalance should not print " WARNINGS " to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted

         # Checks for '--detailed-progress'
         # Check progress report before any table is redistributed
         And set fault inject "on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin"
        When the user runs "ggrebalance --detailed-progress --non-interactive-mode -n 1 -x 3 --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value  |
          |  1.1. Tables shrunk              | 0           |
          |  1.2. Tables shrink in progress  | 0           |
          |  1.3. Tables left to shrink      | 10          |
          |  2.1. Bytes processed            | 0           |
          |  2.2. Bytes left to process      | > 0         |
          |  2.3. Bytes in progress          | 0           |
          |  3.1. Estimated shrink rate      | 0 MB/s      |
          |  3.2. Estimated time             | not defined |
         And all files in gpAdminLogs directory are deleted

         # Check progress report before finish of first table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_0"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value  |
          |  1.1. Tables shrunk              | 0           |
          |  1.2. Tables shrink in progress  | 1           |
          |  1.3. Tables left to shrink      | 9           |
          |  2.1. Bytes processed            | 0           |
          |  2.2. Bytes left to process      | > 0         |
          |  2.3. Bytes in progress          | > 0         |
          |  3.1. Estimated shrink rate      | 0 MB/s      |
          |  3.2. Estimated time             | not defined |
         And all files in gpAdminLogs directory are deleted

         # Check progress report when some tables have been redistributed
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_3"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value  |
          |  1.1. Tables shrunk              | 3           |
          |  1.2. Tables shrink in progress  | 1           |
          |  1.3. Tables left to shrink      | 6           |
          |  2.1. Bytes processed            | > 0         |
          |  2.2. Bytes left to process      | > 0         |
          |  2.3. Bytes in progress          | > 0         |
          |  3.1. Estimated shrink rate      | > 0 MB/s    |
          |  3.2. Estimated time             | > 0 s       |
         And all files in gpAdminLogs directory are deleted

         # Check progress report before finish of last table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_9"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value  |
          |  1.1. Tables shrunk              | 9           |
          |  1.2. Tables shrink in progress  | 1           |
          |  1.3. Tables left to shrink      | 0           |
          |  2.1. Bytes processed            | > 0         |
          |  2.2. Bytes left to process      | > 0         |
          |  2.3. Bytes in progress          | > 0         |
          |  3.1. Estimated shrink rate      | > 0 MB/s    |
          |  3.2. Estimated time             | > 0 s       |
         And all files in gpAdminLogs directory are deleted

         # Check progress report when all tables have been redistributed
         And set fault inject "on_enter_STATE_SHRINK_TABLES_DONE_end"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value  |
          |  1.1. Tables shrunk              | 10          |
          |  1.2. Tables shrink in progress  | 0           |
          |  1.3. Tables left to shrink      | 0           |
          |  2.1. Bytes processed            | > 0         |
          |  2.2. Bytes left to process      | 0           |
          |  2.3. Bytes in progress          | 0           |
          |  3.1. Estimated shrink rate      | > 0 MB/s    |
          |  3.2. Estimated time             | 0 s         |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback before any table is rolled back
         And set fault inject "on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE_end"
        When the user runs "ggrebalance -r -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value  |
          |  1.1. Tables rolled back          | 0           |
          |  1.2. Tables rollback in progress | 0           |
          |  1.3. Tables left to rollback     | 10          |
          |  2.1. Bytes processed             | 0           |
          |  2.2. Bytes left to process       | > 0         |
          |  2.3. Bytes in progress           | 0           |
          |  3.1. Estimated shrink rate       | 0 MB/s      |
          |  3.2. Estimated time              | not defined |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback before finish of first table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_0"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value  |
          |  1.1. Tables rolled back          | 0           |
          |  1.2. Tables rollback in progress | 1           |
          |  1.3. Tables left to rollback     | 9           |
          |  2.1. Bytes processed             | 0           |
          |  2.2. Bytes left to process       | > 0         |
          |  2.3. Bytes in progress           | > 0         |
          |  3.1. Estimated shrink rate       | 0 MB/s      |
          |  3.2. Estimated time              | not defined |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback when some tables have been redistributed
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_3"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value  |
          |  1.1. Tables rolled back          | 3           |
          |  1.2. Tables rollback in progress | 1           |
          |  1.3. Tables left to rollback     | 6           |
          |  2.1. Bytes processed             | > 0         |
          |  2.2. Bytes left to process       | > 0         |
          |  2.3. Bytes in progress           | > 0         |
          |  3.1. Estimated shrink rate       | > 0 MB/s    |
          |  3.2. Estimated time              | > 0 s       |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback before finish of last table redistribution
         And set fault inject "before_set_status_test_db_1.test_schema_1.test_table_9"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "failed to redistribute some tables during shrink" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value  |
          |  1.1. Tables rolled back          | 9           |
          |  1.2. Tables rollback in progress | 1           |
          |  1.3. Tables left to rollback     | 0           |
          |  2.1. Bytes processed             | > 0         |
          |  2.2. Bytes left to process       | > 0         |
          |  2.3. Bytes in progress           | > 0         |
          |  3.1. Estimated shrink rate       | > 0 MB/s    |
          |  3.2. Estimated time              | > 0 s       |
         And all files in gpAdminLogs directory are deleted

         # Check progress report at rollback when all tables have been redistributed
         And set fault inject "on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE_begin"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT
                stat_name,
                CASE
                    WHEN stat_name = '2.1. Bytes processed' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.2. Bytes left to process' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '2.3. Bytes in progress' AND stat_value != '0' THEN '> 0'
                    WHEN stat_name = '3.1. Estimated shrink rate' AND stat_value != '0 MB/s' THEN '> 0 MB/s'
                    WHEN stat_name = '3.2. Estimated time' AND stat_value != 'not defined' AND stat_value != '0 s' THEN '> 0 s'
                    ELSE stat_value
                END AS stat_value
            FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                        | stat_value  |
          |  1.1. Tables rolled back          | 10          |
          |  1.2. Tables rollback in progress | 0           |
          |  1.3. Tables left to rollback     | 0           |
          |  2.1. Bytes processed             | > 0         |
          |  2.2. Bytes left to process       | 0           |
          |  2.3. Bytes in progress           | 0           |
          |  3.1. Estimated shrink rate       | > 0 MB/s    |
          |  3.2. Estimated time              | 0 s         |
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rollback is complete" to logfile with latest timestamp
         And ggrebalance should print "Tables shrunk:\s*10" regex to logfile
         And ggrebalance should print "Bytes processed:\s*\d+" regex to logfile
         And ggrebalance should print "Shrink rate:" to logfile with latest timestamp
         And ggrebalance should print "Shrink total time:" to logfile with latest timestamp
         And ggrebalance should print "Tables rolled back:\s*10" regex to logfile
         And ggrebalance should print "Tables rollback rate:" to logfile with latest timestamp
         And ggrebalance should print "Rollback total time:" to logfile with latest timestamp
         And ggrebalance should not print "Segments moved:" to logfile with latest timestamp
         And ggrebalance should not print "Rolled back steps:" to logfile with latest timestamp
         And ggrebalance should not print "Cancelled steps:" to logfile with latest timestamp
         And ggrebalance should not print " WARNINGS " to logfile with latest timestamp

    Scenario: test 22. Check ggrebalance interruption with '--detailed-progress' options during shrink and re-enter.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table" in "test_db_1" with "100" rows
         And the user connects to "test_db_1" with named connection "test_connection"
         And the user executes "BEGIN; LOCK TABLE test_schema_1.test_table;" with named connection "test_connection"
        When the user asynchronously runs "ggrebalance -x 1 --skip-rebalance --non-interactive-mode --detailed-progress" and the process is saved
         And the user waits till ggrebalance prints "Updated target segment count to 1" in the logs (with timeout of "60" sec)
         And waiting "5" seconds
         And the user asynchronously sets up to end ggrebalance process with SIGINT
        Then the user waits till ggrebalance prints "Shrink was interrupted" in the logs (with timeout of "60" sec)
         And the user executes "ROLLBACK;" with named connection "test_connection"
         And the user drops the named connection "test_connection"
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
