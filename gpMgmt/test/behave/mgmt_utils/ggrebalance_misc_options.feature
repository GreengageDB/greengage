@ggrebalance_misc_options @skip
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

    Scenario: test 4. Check rebalance with 'spread' mirror configuration.
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
        When the user runs "ggrebalance --non-interactive-mode -x 6 --mirror-mode spread -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
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
        When the user runs "ggrebalance -x 4 --add-hosts 'sdw2, sdw3' --remove-hosts 'sdw1' -d '/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast, /home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror'"
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

    Scenario: test 9. Check rebalance with '--remove-hosts-file'.
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

    Scenario: test 21. Check '--simple-progress' option.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_0" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_2" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_3" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_4" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_5" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_6" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_7" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_8" in "test_db_1" with "500" rows
         And there is a "heap" table "test_schema_1.test_table_9" in "test_db_1" with "500" rows
         And all files in gpAdminLogs directory are deleted
         And set fault inject "on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE_begin"
        When the user runs "ggrebalance --simple-progress --non-interactive-mode -n 1 -x 3  --skip-rebalance"
        Then ggrebalance should return a return code of 1
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value |
          |  1.1. Tables shrinked            | 0          |
          |  1.2. Tables shrink in progress  | 0          |
          |  1.3. Tables left to shrink      | 10         |
         And all files in gpAdminLogs directory are deleted
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
          |  1.1. Tables shrinked            | 0          |
          |  1.2. Tables shrink in progress  | 1          |
          |  1.3. Tables left to shrink      | 9          |
         And all files in gpAdminLogs directory are deleted
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
          |  1.1. Tables shrinked            | 3          |
          |  1.2. Tables shrink in progress  | 1          |
          |  1.3. Tables left to shrink      | 6          |
         And all files in gpAdminLogs directory are deleted
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
          |  1.1. Tables shrinked            | 9          |
          |  1.2. Tables shrink in progress  | 1          |
          |  1.3. Tables left to shrink      | 0          |
         And all files in gpAdminLogs directory are deleted
        When the user runs "ggrebalance -n 1 --non-interactive-mode"
        Then ggrebalance should return a return code of 0
         And ggrebalance should print "Shrink is complete" to logfile with latest timestamp
         And unset fault inject
        When execute following sql in db "postgres" and store result in the context
            """
            SELECT * FROM ggrebalance.rebalance_progress;
            """
        Then validate that following rows are in the stored rows
          |  stat_name                       | stat_value |
          |  1.1. Tables shrinked            | 10         |
          |  1.2. Tables shrink in progress  | 0          |
          |  1.3. Tables left to shrink      | 0          |
