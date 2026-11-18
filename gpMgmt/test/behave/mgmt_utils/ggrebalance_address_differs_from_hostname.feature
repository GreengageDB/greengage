@ggrebalance_rebalance
Feature: ggrebalance preserves segment addresses and hostnames

    @ggrebalance_address_differs_from_hostname
    Scenario: rebalance - cluster where gp_segment_configuration.address is an IP address, not the hostname.
        Given the database is not running
         And a working directory of the test as '/data/gpdata/ggrebalance_addresses'
         And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
         And database "test_db_1" exists
         And schema "test_schema_1" exists in "test_db_1"
         And there is a "heap" table "test_schema_1.test_table_1" in "test_db_1" with "100" rows
         And there is a "ao" table "test_schema_1.test_table_2" in "test_db_1" with "100" rows
         And all files in gpAdminLogs directory are deleted
         And the user runs command "set -e; for h in sdw1 sdw2 sdw3; do ip=$(getent ahostsv4 $h | awk 'NR==1{print $1}'); PGOPTIONS='-c allow_system_table_mods=true' psql -v ON_ERROR_STOP=1 -d postgres -c "update gp_segment_configuration set address = '$ip' where hostname = '$h'"; done"
         And the user runs command "gpstop -ar"
         And the cluster configuration has 0 segments where "content > -1 and address = hostname"
         And the cluster configuration has 0 segments where "content > -1 and status <> 'u'"
         And all files in gpAdminLogs directory are deleted
         And the cluster configuration is saved for "before rebalance"
        When the user runs "ggrebalance --non-interactive-mode -x 6 --remove-hosts sdw3 -d '/data/gpdata/ggrebalance_addresses/data/primary, /data/gpdata/ggrebalance_addresses/data/mirror'"
        Then ggrebalance should not print "does not exist" to logfile with latest timestamp
         And ggrebalance should return a return code of 0
         And ggrebalance should print "Rebalance is complete" to logfile with latest timestamp
         And the cluster configuration has 0 segments where "hostname = 'sdw3' and content > -1"
         And ggrebalance should not print "Cluster is left in unbalanced state" to logfile with latest timestamp
         And the cluster configuration has 0 segments where "content > -1 and address = hostname"
         And the cluster configuration has 6 segments where "hostname = 'sdw1' and content > -1"
         And the cluster configuration has 6 segments where "hostname = 'sdw2' and content > -1"
         And each segment has a hostname and address pair from "before rebalance"
         And the cluster configuration has 12 segments where "content > -1 and address::inet IS NOT NULL"
         And the cluster configuration has 0 segments where "content > -1 and status <> 'u'"
         And distribution information from table "test_schema_1.test_table_1" with data in "test_db_1" is equal to segment count = 6, row count = 100
         And distribution information from table "test_schema_1.test_table_2" with data in "test_db_1" is equal to segment count = 6, row count = 100
