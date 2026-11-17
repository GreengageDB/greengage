@ggrebalance_misc_options @ggrebalance_misc_options_part2
Feature: ggrebalance behave tests (misc options scenarios)

    Scenario: test 13. Check ggrebalance launch when pid file (or any other mark) for other tool exists.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When we run a sample background script to generate a pid on "coordinator" segment
         And a sample ggrebalance.pid file is created using the background pid in the coordinator_data_directory
         And the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance is already running." to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "ggrebalance is already running." to logfile with latest timestamp
         And the background pid is killed on "coordinator" segment
         And a sample ggrebalance.pid file is removed from the coordinator_data_directory
         And all files in gpAdminLogs directory are deleted
        When we run a sample background script to generate a pid on "coordinator" segment
         And a sample gpexpand.pid file is created using the background pid in the coordinator_data_directory
         And the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "gpexpand is already running." to logfile with latest timestamp
         And the background pid is killed on "coordinator" segment
         And a sample gpexpand.pid file is removed from the coordinator_data_directory
         And all files in gpAdminLogs directory are deleted
        When schema "gpexpand" exists in "postgres"
         And the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "gpexpand schema exists. Assuming gpexpand is already running." to logfile with latest timestamp
         And schema "gpexpand" is removed in "postgres"
         And all files in gpAdminLogs directory are deleted
        When we run a sample background script to generate a pid on "coordinator" segment
         And a sample gpexpand.status file is created using the background pid in the coordinator_data_directory
         And the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "gpexpand.status file exists. Assuming gpexpand is already running." to logfile with latest timestamp
         And the background pid is killed on "coordinator" segment
         And a sample gpexpand.status file is removed from the coordinator_data_directory
         And all files in gpAdminLogs directory are deleted
        When we run a sample background script to generate a pid on "coordinator" segment
         And a sample gprecoverseg.lock directory is created using the background pid in coordinator_data_directory
         And the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "gprecoverseg is already running." to logfile with latest timestamp
         And the background pid is killed on "coordinator" segment
         And the gprecoverseg lock directory is removed
         And all files in gpAdminLogs directory are deleted
        When the user connects to "postgres" with named connection "test_connection"
         And the user executes "SET application_name TO 'gpbackup_20260424085425'" with named connection "test_connection"
         And the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "gpbackup/gprestore utility is already running." to logfile with latest timestamp
         And the user drops the named connection "test_connection"
         And all files in gpAdminLogs directory are deleted
        When the user connects to "postgres" with named connection "test_connection"
         And the user executes "SET application_name TO 'gprestore_20260424085425'" with named connection "test_connection"
         And the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "gpbackup/gprestore utility is already running." to logfile with latest timestamp
         And the user drops the named connection "test_connection"

    Scenario: test 13.2. Check other tools launch when ggrebalance operation hasn't finished.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "10000" rows
         And there is a "heap" table "test_schema_1.test_table_2" in "test_db_1" with "10000" rows
         And there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "10000" rows
         And there is a "heap" table "test_schema_1.test_table_4" in "test_db_1" with "10000" rows
         And there is a "heap" table "test_schema_1.test_table_5" in "test_db_1" with "10000" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED_begin"
         And set fault inject type to suspend
        When the user asynchronously runs "ggrebalance -n 1 -x 2 --skip-rebalance" and the process is saved
         And the user waits till ggrebalance prints "Updated target segment count to 2" in the logs (with timeout of "60" sec)
        When the user runs "gpexpand"
        Then gpexpand should return a return code of 1
         And gpexpand should print "ggrebalance is already running. gpexpand is not allowed to run in parallel" to logfile with latest timestamp
        When the user runs "gpcheckcat"
        Then gpcheckcat should return a return code of 1
         And gpcheckcat should print "ERROR: Using gpcheckcat is not supported during cluster rebalancing" to logfile with latest timestamp
        When the user runs "gpconfig -c application_name -v 'gpconfig_test'"
        Then gpconfig should return a return code of 1
         And gpconfig should print "ERROR: Using gpconfig is not supported during cluster rebalancing" to logfile with latest timestamp
         And unset fault inject
         And the user asynchronously sets up to end ggrebalance process with SIGINT
        Then the async process finished with a return code of 1
         And ggrebalance should print "Shrink was interrupted" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "gpexpand"
        Then gpexpand should return a return code of 1
         And gpexpand should print "ggrebalance is already running. gpexpand is not allowed to run in parallel" to logfile with latest timestamp
        When the user runs "gpcheckcat"
        Then gpcheckcat should return a return code of 1
         And gpcheckcat should print "ERROR: Using gpcheckcat is not supported during cluster rebalancing" to logfile with latest timestamp
        When the user runs "gpconfig -c application_name -v 'gpconfig_test'"
        Then gpconfig should return a return code of 1
         And gpconfig should print "ERROR: Using gpconfig is not supported during cluster rebalancing" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
        When the user runs "gpexpand"
         Then gpexpand should not print "ggrebalance is already running. gpexpand is not allowed to run in parallel" to logfile with latest timestamp
        When the user runs "gpcheckcat"
         Then gpcheckcat should not print "ERROR: Usage of gpcheckcat is not supported while the cluster is being rebalanced" to logfile with latest timestamp
        When the user runs "gpconfig -c application_name -v 'gpconfig_test'"
         Then gpconfig should not print "ERROR: Usage of gpconfig is not supported while the cluster is being rebalanced" to logfile with latest timestamp

    Scenario: test 14.  Check ggrebalance if pg_basebackup is already running
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3", with 1 segments on each
         And all the segments are running
         And the segments are synchronized
         And all files in gpAdminLogs directory are deleted
         And the information of contents 0,1,2 is saved
         And user immediately stops all mirror processes for content 0,1,2
         And user can start transactions
         And the user suspend the walsender on the primary on content 0
         And the user asynchronously runs "gprecoverseg -aF" and the process is saved
         And the user just waits until recovery_progress.file is created in gpAdminLogs
         And user waits until gp_stat_replication table has no pg_basebackup entries for content 1,2
         And an FTS probe is triggered
         And the user waits until mirror on content 1,2 is up
         And verify that mirror on content 0 is down
         And the gprecoverseg lock directory is removed
         And user immediately stops all mirror processes for content 1,2
         And the user waits until mirror on content 1,2 is down
        When the user runs "ggrebalance"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Segments {0} have running pg_basebackup." to logfile with latest timestamp
         And the user reset the walsender on the primary on content 0
         And the user waits until saved async process is completed
         And verify that mirror on content 0 is up

    Scenario: test 15. Check '--log-dir' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -l "/tmp/ggrebalance_logs" -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And gpAdminLogs directory has no "ggrebalance*" files
         And gpAdminLogs directory has no "gpmovemirrors*" files
         And gpAdminLogs directory has no "gprecoverseg*" files
         And "/tmp/ggrebalance_logs" directory has "ggrebalance*" files
         And "/tmp/ggrebalance_logs" directory has "gpmovemirrors*" files
         And "/tmp/ggrebalance_logs" directory has "gprecoverseg*" files
         And all files in "/tmp/ggrebalance_logs" directory are deleted
        When the user runs "ggrebalance -c"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode --log-dir "/tmp/ggrebalance logs" -x 6 --add-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And gpAdminLogs directory has no "ggrebalance*" files
         And gpAdminLogs directory has no "gpmovemirrors*" files
         And gpAdminLogs directory has no "gprecoverseg*" files
         And "/tmp/ggrebalance logs" directory has "ggrebalance*" files
         And "/tmp/ggrebalance logs" directory has "gpmovemirrors*" files
         And "/tmp/ggrebalance logs" directory has "gprecoverseg*" files
         And all files in "/tmp/ggrebalance logs" directory are deleted

    Scenario: test 16.1. Check without '--analyze' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS not_analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  not_analyzed_tables_cnt  |
          |  2                        |
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NOT NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  analyzed_tables_cnt  |
          |  0                    |
        When the user runs "ggrebalance --non-interactive-mode -x 3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS not_analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  not_analyzed_tables_cnt  |
          |  2                        |
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NOT NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  analyzed_tables_cnt  |
          |  0                    |

    Scenario: test 16.2. Check with '--analyze' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS not_analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  not_analyzed_tables_cnt  |
          |  2                        |
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NOT NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  analyzed_tables_cnt  |
          |  0                    |
        When the user runs "ggrebalance --non-interactive-mode --analyze -x 3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS not_analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  not_analyzed_tables_cnt  |
          |  0                        |
        When execute following sql in db "test_db_1" and store result in the context
            """
            SELECT COUNT(1) AS analyzed_tables_cnt FROM pg_stat_all_tables WHERE last_analyze IS NOT NULL AND relname IN ('test_table_1', 'test_table_2');
            """
        Then validate that following rows are in the stored rows
          |  analyzed_tables_cnt  |
          |  2                    |

    Scenario: test 17. Check '--replay-lag' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode --replay-lag 0 -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And gprecoverseg should print "0 bytes of wal is still to be replayed on mirror with dbid.*, let mirror catchup on replay then trigger rebalance" regex to logfile
         And all files in gpAdminLogs directory are deleted

    Scenario: test 18-1. Check '--hba-hostnames' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode --hba-hostnames -x 2 --add-hosts sdw2 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And pg_hba file "/data/gpdata/ggrebalance/data/primary/gpseg0/pg_hba.conf" on host "sdw1" contains entries for "sdw2"

    Scenario: test 18-2. Check without '--hba-hostnames' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 2 --add-hosts sdw2 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And pg_hba file "/data/gpdata/ggrebalance/data/primary/gpseg0/pg_hba.conf" on host "sdw1" contains only cidr addresses

    Scenario: test 19. Check '--duration' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --duration 00:00:20 --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "soft shutdown, waiting for critical operations to finish..." to logfile with latest timestamp
         And ggrebalance should print "Rebalance was interrupted" to logfile with latest timestamp

    Scenario: test 20. Check '--end' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --end '2000-01-01 01:01:00' --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "End time occurs in the past" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --end $(date -d '+20 seconds' '+%Y%m%dT%H%M%S') --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "soft shutdown, waiting for critical operations to finish..." to logfile with latest timestamp
         And ggrebalance should print "Rebalance was interrupted" to logfile with latest timestamp

