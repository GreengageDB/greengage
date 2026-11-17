@ggrebalance_misc_options @ggrebalance_misc_options_part1
Feature: ggrebalance behave tests (misc options scenarios)

    Scenario: test 1. Check if cluster has no mirroring.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with no mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 6 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Cluster has mirroring disabled. Can't proceed with rebalance" to logfile with latest timestamp

    Scenario: test 2. Check if cluster can't be rebalanced to a balanced state with given parameters.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 5 --remove-hosts sdw3 -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Cannot evenly distribute 5 segments across 2 hosts." to logfile with latest timestamp
        When the user runs "ggrebalance -x 6 --remove-hosts 'sdw2, sdw3' -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Cannot perform rebalance at 1 host" to logfile with latest timestamp

    Scenario: test 3. Check rebalance with 'grouped' mirror configuration.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3", with 3 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 6 --mirror-mode grouped -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And verify that mirror segments are in "group" configuration
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100

    Scenario: test 4. Check rebalance with 'spread' mirror configuration (plus check '--approve-swap-roles').
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3", with 3 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --approve-swap-roles -x 6 --mirror-mode spread -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And verify that mirror segments are in "spread" configuration
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100

    Scenario: test 5. Check rebalance against coordinator-only mode.
        Given the database is not running
         And the user runs "gpstart -ma"
         And "gpstart -ma" should return a return code of 0
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 1"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "System was started in single node mode - only utility mode connections are allowed" to logfile with latest timestamp

    Scenario: test 6. Check rebalance with '--target-hosts'.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1", with 4 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        # make a dry run without '-d' option to check default datadirs
        When the user runs "ggrebalance --non-interactive-mode -x 4 --target-hosts 'sdw2, sdw3' -p --skip-resource-estimation"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "To:   sdw2:20500:/data1/primary/gpseg0" to logfile with latest timestamp
         And ggrebalance should print "To:   sdw3:21500:/data1/mirror/gpseg0" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 4 --target-hosts 'sdw2, sdw3' -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100

    Scenario: test 6.1 Check rebalance with both '--add-hosts' and '--remove-hosts' (result should be the same as with the usage of '--target-hosts' in the previous test).
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1", with 4 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        # make a dry run without '-d' option to check default datadirs
        When the user runs "ggrebalance --non-interactive-mode -x 4 --add-hosts 'sdw2, sdw3' --remove-hosts 'sdw1' -p --skip-resource-estimation"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "To:   sdw2:20500:/data1/primary/gpseg0" to logfile with latest timestamp
         And ggrebalance should print "To:   sdw3:21500:/data1/mirror/gpseg0" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 4 --add-hosts 'sdw2, sdw3' --remove-hosts 'sdw1' -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100

    Scenario: test 7. Check rebalance with '--target-hosts-file'.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1", with 4 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When there is a file "/tmp/ggrebalance_target_hosts" with hosts " "
         And the user runs "ggrebalance -x 4 --target-hosts-file /tmp/ggrebalance_target_hosts -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "Empty '/tmp/ggrebalance_target_hosts' file" to logfile with latest timestamp
         And the temporary file "/tmp/ggrebalance_target_hosts" is removed
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -x 4 --target-hosts-file /tmp/ggrebalance_non_existing_file -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 1
         And ggrebalance should print "No such file or directory: '/tmp/ggrebalance_non_existing_file'" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When there is a file "/tmp/ggrebalance_target_hosts" with hosts "sdw2|sdw3"
         And the user runs "ggrebalance --non-interactive-mode -x 4 --target-hosts-file /tmp/ggrebalance_target_hosts -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 0 segments where "hostname='sdw1' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw2' and content > -1 and role = 'm' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'p' and status = 'u'"
         And the cluster configuration has 2 segments where "hostname='sdw3' and content > -1 and role = 'm' and status = 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 4, row count = 100
         And the temporary file "/tmp/ggrebalance_target_hosts" is removed

    Scenario: test 8. Check rebalance with '--add-hosts-file'.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1", with 3 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And there is a file "/tmp/ggrebalance_add_hosts" with hosts "sdw2|sdw3"
        When the user runs "ggrebalance --non-interactive-mode -x 3 --add-hosts-file /tmp/ggrebalance_add_hosts -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
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
         And the temporary file "/tmp/ggrebalance_add_hosts" is removed

    Scenario: test 9. Check rebalance with '--remove-hosts-file' (plus check '--show-plan').
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3", with 2 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And there is a file "/tmp/ggrebalance_remove_hosts" with hosts "sdw3"
        When the user runs "ggrebalance --show-plan --non-interactive-mode -x 4 --remove-hosts-file /tmp/ggrebalance_remove_hosts -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
         Then ggrebalance should return a return code of 0
         And ggrebalance should not print "Rebalance is complete" to logfile with latest timestamp
         And ggrebalance should not print "Shrink is complete" to logfile with latest timestamp
         And ggrebalance should print "SHRINK PLAN" to logfile with latest timestamp
         And ggrebalance should print "-SEGMENTS TO REMOVE-" to logfile with latest timestamp
         And ggrebalance should print "-BALANCE MOVES-" to logfile with latest timestamp
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 4 --remove-hosts-file /tmp/ggrebalance_remove_hosts -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
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
         And the temporary file "/tmp/ggrebalance_remove_hosts" is removed

    Scenario: test 10. Check rebalance with '--target-datadirs' plain paths (without template substitution).
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3", with 2 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 6 --remove-hosts sdw3 --target-datadirs '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_new/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_new/dbfast_mirror'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_new/dbfast/gpseg_'"
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_new/dbfast_mirror/gpseg_'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100

    Scenario: test 11. Check rebalance with '--target-datadirs' {content} and {hostname} substitution.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3", with 2 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance --non-interactive-mode -x 6 --remove-hosts sdw3 --target-datadirs '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_new/dbfast/new_seg{content}_on_host_{hostname}, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_new/dbfast_mirror/new_seg{content}_on_host_{hostname}'"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_new/dbfast/new\_seg_\_on\_host\_sdw_'"
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_new/dbfast\_mirror/new\_seg_\_on\_host\_sdw_'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100

    Scenario: test 12. Check rebalance with '--target-datadirs-file' {content} and {hostname} substitution.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3", with 2 segments on each
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And database "test_db_2" exists
         And schema "test_schema_2" exists in "test_db_2"
         And there is a "heap" table "test_schema_2.test_table_1" in "test_db_2" with "100" rows
         And there is a "ao" table "test_schema_2.test_table_2" in "test_db_2" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And there is a file "/tmp/ggrebalance_target_datadirs" with hosts "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_{hostname}/dbfast/new_seg{content}|/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_{hostname}/dbfast_mirror/new_seg{content}"
        When the user runs "ggrebalance --non-interactive-mode -x 6 --remove-hosts sdw3 --target-datadirs-file /tmp/ggrebalance_target_datadirs"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_sdw_/dbfast/new\_seg_'"
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_sdw_/dbfast\_mirror/new\_seg_'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And the temporary file "/tmp/ggrebalance_target_datadirs" is removed
        When the user runs "ggrebalance -c"
         And all files in gpAdminLogs directory are deleted
         And there is a file "/tmp/ggrebalance_target_datadirs" with hosts "/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_{hostname}/new_dbfast/new_seg{content}|/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs_{hostname}/new_dbfast_mirror/new_seg{content}"
        When the user runs "ggrebalance --non-interactive-mode -x 6 --add-hosts sdw3 --target-datadirs-file /tmp/ggrebalance_target_datadirs"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_sdw_/new\_dbfast/new\_seg_'"
         And the cluster configuration has some segments where "datadir like '/home/gpadmin/gpdb\_src/gpAux/gpdemo/datadirs\_sdw_/new\_dbfast\_mirror/new\_seg_'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_1" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_2.test_table_2" with data in "test_db_2" is equal to segment count = 6, row count = 100
         And the temporary file "/tmp/ggrebalance_target_datadirs" is removed

