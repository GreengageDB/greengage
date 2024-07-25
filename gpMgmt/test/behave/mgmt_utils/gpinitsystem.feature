@gpinitsystem
Feature: gpinitsystem tests

    Scenario: gpinitsystem creates a cluster with data_checksums on
        Given the database is initialized with checksum "on"
        When the user runs "gpconfig -s data_checksums"
        Then gpconfig should return a return code of 0
        And gpconfig should print "Values on all segments are consistent" to stdout
        And gpconfig should print "Coordinator value: on" to stdout
        And gpconfig should print "Segment     value: on" to stdout

    Scenario: gpinitsystem creates a cluster with data_checksums off
        Given the database is initialized with checksum "off"
        When the user runs "gpconfig -s data_checksums"
        Then gpconfig should return a return code of 0
        And gpconfig should print "Values on all segments are consistent" to stdout
        And gpconfig should print "Coordinator value: off" to stdout
        And gpconfig should print "Segment     value: off" to stdout

    Scenario: gpinitsystem should import the system collations
        Given the database is not running
        And create demo cluster config
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        Then gpinitsystem should return a return code of 0
        And the user runs "psql postgres -c "create table collationimport1 as select * from pg_collation where collnamespace = 'pg_catalog'::regnamespace""
        # no more collation is imported
        When the user runs "psql postgres -c "select pg_import_system_collations('pg_catalog')""
        Then psql should return a return code of 0
        And psql should print "0" to stdout
        And psql should print "(1 row)" to stdout
        And the user runs "psql postgres -c "create table collationimport2 as select * from pg_collation where collnamespace = 'pg_catalog'::regnamespace""
        # no difference is before import and after import
        When the user runs "psql postgres -c "select * from collationimport1 except select * from collationimport2""
        Then psql should return a return code of 0
        And psql should print "(0 rows)" to stdout

    Scenario: gpinitsystem creates a cluster when the user set LC_ALL env variable
        Given create demo cluster config
        And the environment variable "LC_ALL" is set to "en_US.UTF-8"
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        Then gpinitsystem should return a return code of 0
        Given the user runs "gpstate"
        Then gpstate should return a return code of 0
        And "LC_ALL" environment variable should be restored

    Scenario: gpinitsystem creates a cluster when the user set -n or --locale parameter
        Given create demo cluster config
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -n en_US.UTF-8"
        Then gpinitsystem should return a return code of 0
        Given the user runs "gpstate"
        Then gpstate should return a return code of 0

	Scenario: gpinitsystem exits with status 0 when the user set locale variables
        Given create demo cluster config
        And the environment variable "LC_MONETARY" is set to "en_US.UTF-8"
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        Then gpinitsystem should return a return code of 0
        Given the user runs "gpstate"
        Then gpstate should return a return code of 0
        And "LC_MONETARY" environment variable should be restored

    Scenario: gpinitsystem exits with status 0 when the user set locale parameters
        Given create demo cluster config
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile --lc-monetary=en_US.UTF-8"
        Then gpinitsystem should return a return code of 0
        Given the user runs "gpstate"
        Then gpstate should return a return code of 0

    Scenario: gpinitsystem exits with status 1 when the user enters nothing for the confirmation
        Given create demo cluster config
         When the user runs command "echo '' | gpinitsystem -c ../gpAux/gpdemo/clusterConfigFile" eok
         Then gpinitsystem should return a return code of 1
        Given the user runs "gpstate"
         Then gpstate should return a return code of 2

    Scenario: gpinitsystem exits with status 1 when the user enters no for the confirmation
        Given create demo cluster config
         When the user runs command "echo no | gpinitsystem -c ../gpAux/gpdemo/clusterConfigFile" eok
         Then gpinitsystem should return a return code of 1
        Given the user runs "gpstate"
         Then gpstate should return a return code of 2

    Scenario: gpinitsystem creates a cluster when the user confirms the dialog
        Given create demo cluster config
         When the user runs command "echo y | gpinitsystem -c ../gpAux/gpdemo/clusterConfigFile"
         Then gpinitsystem should return a return code of 0
        Given the user runs "gpstate"
         Then gpstate should return a return code of 0

    Scenario: gpinitsystem creates a backout file when gpinitsystem process terminated
        Given create demo cluster config
        And all files in gpAdminLogs directory are deleted
        When the user asynchronously runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile" and the process is saved
        And the user asynchronously sets up to end gpinitsystem process when "Waiting for parallel processes" is printed in the logs
        And the user waits until saved async process is completed
        And the user waits until gpcreateseg process is completed
        Then gpintsystem logs should contain lines about running backout script
        And the user runs the gpinitsystem backout script
        And all files in gpAdminLogs directory are deleted
        And the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        And gpinitsystem should return a return code of 0
        And gpintsystem logs should not contain lines about running backout script

    Scenario: gpinitsystem creates a backout file when gpcreateseg process terminated
        Given create demo cluster config
        And all files in gpAdminLogs directory are deleted
        When the user asynchronously runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile" and the process is saved
        And the user asynchronously sets up to end gpcreateseg process when it starts
        And the user waits until saved async process is completed
        Then gpintsystem logs should contain lines about running backout script
        And the user runs the gpinitsystem backout script
        And all files in gpAdminLogs directory are deleted
        And the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        And gpinitsystem should return a return code of 0
        And gpintsystem logs should not contain lines about running backout script

    Scenario: gpinitsystem does not create or need backout file when user terminated very early
        Given create demo cluster config
        And all files in gpAdminLogs directory are deleted
        When the user asynchronously runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile" and the process is saved
        And the user asynchronously sets up to end bin/gpinitsystem process in 0 seconds
        And the user waits until saved async process is completed
        Then gpintsystem logs should not contain lines about running backout script
        And all files in gpAdminLogs directory are deleted
        And the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        Then gpinitsystem should return a return code of 0

    Scenario: gpinitsystem fails with exit code 1 when the functions file is not found
       Given create demo cluster config
           # force a load error when trying to source gp_bash_functions.sh
        When the user runs command "ln -s -f `which gpinitsystem` /tmp/gpinitsystem-link; . /tmp/gpinitsystem-link" eok
        Then gpinitsystem should return a return code of 1
        When the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        Then gpinitsystem should return a return code of 0

    Scenario: gpinitsystem continues running when gpinitstandby fails
       Given create demo cluster config
           # force gpinitstandby to fail by specifying a directory that does not exist (gpinitsystem continues successfully)
        When the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -s localhost -S not-a-real-directory -P 21100 -h ../gpAux/gpdemo/hostfile"
        Then gpinitsystem should return a return code of 0

    Scenario: after a failed run of gpinitsystem, a re-run should return exit status 0
       Given create demo cluster config
           # force a failure by passing no args
        When the user runs "gpinitsystem"
        Then gpinitsystem should return a return code of 1
        When the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        Then gpinitsystem should return a return code of 0

    Scenario: after gpinitsystem logs a warning, a re-run should return exit status 0
      Given create demo cluster config
          # log a warning
        And the user runs command "sed -i.bak 's/^ENCODING.*//g' ../gpAux/gpdemo/clusterConfigFile"
       When the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
       Then gpinitsystem should return a return code of 0
      Given create demo cluster config
       When the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
       Then gpinitsystem should return a return code of 0

    Scenario: gpinitsystem should warn but not fail when standby cannot be instantiated
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the standby is not initialized
        And the user runs command "rm -rf /tmp/gpinitsystemtest && mkdir /tmp/gpinitsystemtest"
        # stop db and make sure cluster config exists so that we can manually initialize standby
        And the cluster config is generated with data_checksums "1"
        When the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -s localhost -P 21100 -S /wrong/path -h ../gpAux/gpdemo/hostfile"
        Then gpinitsystem should return a return code of 0
        And gpinitsystem should not print "To activate the Standby Coordinator Segment in the event of Coordinator" to stdout
        And gpinitsystem should print "Cluster setup finished, but Standby Coordinator failed to initialize. Review contents of log files for errors." to stdout
        And sql "select * from gp_toolkit.__gp_user_namespaces" is executed in "postgres" db

    Scenario: gpinitsystem generates an output configuration file and then starts cluster with data_checksums on
        Given the cluster config is generated with data_checksums "on"
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -O /tmp/output_config_file"
        And gpinitsystem should return a return code of 0
        Then verify that file "output_config_file" exists under "/tmp"
        And verify that the file "/tmp/output_config_file" contains "HEAP_CHECKSUM=on"
        And the user runs "gpinitsystem -a -I /tmp/output_config_file -l /tmp/"
        Then gpinitsystem should return a return code of 0
        When the user runs "gpconfig -s data_checksums"
        Then gpconfig should return a return code of 0
        And gpconfig should print "Values on all segments are consistent" to stdout
        And gpconfig should print "Coordinator value: on" to stdout
        And gpconfig should print "Segment     value: on" to stdout

    Scenario: gpinitsystem generates an output configuration file and then starts cluster with data_checksums off
        Given the cluster config is generated with data_checksums "off"
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -O /tmp/output_config_file"
        And gpinitsystem should return a return code of 0
        Then verify that file "output_config_file" exists under "/tmp"
        And verify that the file "/tmp/output_config_file" contains "HEAP_CHECKSUM=off"
        And the user runs "gpinitsystem -a -I /tmp/output_config_file -l /tmp/"
        Then gpinitsystem should return a return code of 0
        When the user runs "gpconfig -s data_checksums"
        Then gpconfig should return a return code of 0
        And gpconfig should print "Values on all segments are consistent" to stdout
        And gpconfig should print "Coordinator value: off" to stdout
        And gpconfig should print "Segment     value: off" to stdout

    Scenario: gpinitsystem should warn but not fail when standby cannot be instantiated
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the standby is not initialized
        And the user runs command "rm -rf $COORDINATOR_DATA_DIRECTORY/newstandby"
        And the user runs command "rm -rf /tmp/gpinitsystemtest && mkdir /tmp/gpinitsystemtest"
        And the cluster config is generated with data_checksums "1"
        When the user runs "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -l /tmp/gpinitsystemtest -s localhost -P 21100 -S $COORDINATOR_DATA_DIRECTORY/newstandby -h ../gpAux/gpdemo/hostfile"
        Then gpinitsystem should return a return code of 0
        And gpinitsystem should print "Log file scan check passed" to stdout
        And sql "select * from gp_toolkit.__gp_user_namespaces" is executed in "postgres" db

    Scenario: gpinitsystem creates a cluster in default timezone
        Given the database is not running
        And "TZ" environment variable is not set
        And the system timezone is saved
        When a demo cluster is created using gpinitsystem args " "
        And gpinitsystem should return a return code of 0
        Then the database timezone is saved
        And the database timezone matches the system timezone
        And the startup timezone is saved
        And the startup timezone matches the system timezone

    Scenario: gpinitsystem creates a cluster using TZ
        Given the database is not running
        And the environment variable "TZ" is set to "US/Hawaii"
        When a demo cluster is created using gpinitsystem args " "
        And gpinitsystem should return a return code of 0
        Then the database timezone is saved
        And the database timezone matches "HST"
        And the startup timezone is saved
        And the startup timezone matches "HST"
        And "TZ" environment variable should be restored

    Scenario: gpinitsystem should print FQDN in pg_hba.conf when HBA_HOSTNAMES=1
        Given the cluster config is generated with HBA_HOSTNAMES "1"
        When generate cluster config file "/tmp/output_config_file"
        Then verify that the file "/tmp/output_config_file" contains "HBA_HOSTNAMES=1"
        When initialize a cluster using "/tmp/output_config_file"
        Then verify that the file "../gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_hba.conf" contains FQDN only for trusted host
        And verify that the file "../gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/pg_hba.conf" contains FQDN only for trusted host

    Scenario: gpinitsystem should print CIDR in pg_hba.conf when HBA_HOSTNAMES=0
        Given the cluster config is generated with HBA_HOSTNAMES "0"
        When generate cluster config file "/tmp/output_config_file"
        Then verify that the file "/tmp/output_config_file" contains "HBA_HOSTNAMES=0"
        When initialize a cluster using "/tmp/output_config_file"
        Then verify that the file "../gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_hba.conf" contains CIDR only for trusted host
        And verify that the file "../gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/pg_hba.conf" contains CIDR only for trusted host

    Scenario: gpinitsystem should print FQDN in pg_hba.conf for standby when HBA_HOSTNAMES=1
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the standby is not initialized
        And ensure the standby directory does not exist
        And the cluster config is generated with HBA_HOSTNAMES "1"
        When generate cluster config file "/tmp/output_config_file"
        Then verify that the file "/tmp/output_config_file" contains "HBA_HOSTNAMES=1"
        When initialize a cluster with standby using "/tmp/output_config_file"
        Then verify that the file "../gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_hba.conf" contains FQDN only for trusted host
        And verify that the file "../gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/pg_hba.conf" contains FQDN only for trusted host
        And verify that the file "../gpAux/gpdemo/datadirs/qddir/demoDataDir-1/newstandby/pg_hba.conf" contains FQDN only for trusted host

    Scenario: gpinitsystem on a DCA system is able to set the DCA specific GUCs
	Given create demo cluster config
        And the user runs command "rm -r ~/gpAdminLogs/gpinitsystem*"
        And a working directory of the test as '/tmp/gpinitsystem'
        # create a dummy dca version file so that DCA specific parameters are set
        And the user runs command "touch /tmp/gpinitsystem/gpdb-appliance-version"
        When the user runs command "source $GPHOME/greenplum_path.sh; __DCA_VERSION_FILE__=/tmp/gpinitsystem/gpdb-appliance-version $GPHOME/bin/gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile"
        Then gpinitsystem should return a return code of 0
        # the log file must have the entry indicating that DCA specific configuration has been set
        And the user runs command "grep -E 'Setting DCA specific configuration values' ~/gpAdminLogs/gpinitsystem*log"

    Scenario: gpinitsystem uses the system locale if no locale is specified
        Given the database is not running
        And "LC_COLLATE" environment variable is not set
        And "LC_CTYPE" environment variable is not set
        And the system locale is saved
        When a demo cluster is created using gpinitsystem args " "
        And gpinitsystem should return a return code of 0
        Then the database locales are saved
        And the database locales "lc_collate,lc_ctype,lc_messages,lc_monetary,lc_numeric,lc_time" match the system locale

    Scenario: gpinitsystem uses a single locale if one is specified
        Given the database is not running
        And the system locale is saved
        When a demo cluster is created using gpinitsystem args "--locale=C"
        And gpinitsystem should return a return code of 0
        Then the database locales are saved
        And the database locales "lc_collate,lc_ctype,lc_messages,lc_monetary,lc_numeric,lc_time" match the locale "C"

    Scenario: gpinitsystem uses multiple locales if multiple are specified
        Given the database is not running
        And the environment variable "LC_COLLATE" is set to "C"
        And the environment variable "LC_CTYPE" is set to "C"
        And the system locale is saved
        When a demo cluster is created using the installed UTF locale
        And gpinitsystem should return a return code of 0
        Then the database locales are saved
        And the database locales "lc_collate" match the locale "C"
        And the database locales "lc_ctype" match the installed UTF locale
        And the database locales "lc_messages,lc_monetary,lc_numeric,lc_time" match the system locale
        And "LC_COLLATE" environment variable should be restored
        And "LC_CTYPE" environment variable should be restored

    @backup_restore_bashrc
    Scenario: gpinitsystem succeeds if there is banner on host
        Given the database is not running
        When the user sets banner on host
        And a demo cluster is created using gpinitsystem args " "
        And gpinitsystem should return a return code of 0

    @backup_restore_bashrc
    Scenario: gpinitsystem succeeds if there is multi-line banner on host
        Given the database is not running
        When the user sets multi-line banner on host
        And a demo cluster is created using gpinitsystem args " "
        And gpinitsystem should return a return code of 0

    Scenario: gpinitsystem should create consistent port entry on segments postgresql.conf file
        Given the database is not running
        When a demo cluster is created using gpinitsystem args " "
        And gpinitsystem should return a return code of 0
        Then gpstate should return a return code of 0
        And check segment conf: postgresql.conf

    Scenario: gpinitsystem creates a cluster successfully when run with -D option
        Given create demo cluster config
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -D"
        Then gpinitsystem should return a return code of 0
        And gpinitsystem should not print "Start Function REMOTE_EXECUTE_AND_GET_OUTPUT" to stdout
        And gpinitsystem should not print "End Function REMOTE_EXECUTE_AND_GET_OUTPUT" to stdout

     Scenario: gpinitsystem creates a cluster with for multi-nic setup and populates table entries correctly
         Given the database is not running
         And create demo cluster config
         #Create hosts file with new host-name
         And backup /etc/hosts file and update hostname entry for localhost
         And update hostlist file with updated host-address
         And update clusterConfig file with new port and host-address
         And update the private keys for the new host address
         When the user runs "gpinitsystem -a -c /tmp/clusterConfigFile-1 -h /tmp/hostfile--1"
         Then gpinitsystem should return a return code of 0
         #Verify entries in the gp_segment_configuration as expected
         And verify that cluster config has host-name populated correctly
         #restore hosts file, cleanup of hostlist, config file
         And restore /etc/hosts file and cleanup hostlist file

    Scenario: gpinitsystem should pass the default value of trusted_shell properly to gpcreateseg
        Given create demo cluster config
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -h ../gpAux/gpdemo/hostfile"
        Then gpinitsystem should return a return code of 0
        When the user runs command "grep -q '.*gpcreateseg\.sh.*Completed .*lalshell.*' ~/gpAdminLogs/gpinitsystem*log"
        Then grep should return a return code of 0

    Scenario: gpinitsystem should pass the changed value of trusted_shell properly to gpcreateseg
        Given create demo cluster config
        And the user runs command "sed -i.bak -E 's/TRUSTED_SHELL=.*/TRUSTED_SHELL=ssh/g' ../gpAux/gpdemo/clusterConfigFile"
        When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile -h ../gpAux/gpdemo/hostfile"
        Then gpinitsystem should return a return code of 0
        When the user runs command "grep -q '.*gpcreateseg\.sh.*Completed ssh.*' ~/gpAdminLogs/gpinitsystem*log"
        Then grep should return a return code of 0
        And the user runs command "mv ../gpAux/gpdemo/clusterConfigFile.bak ../gpAux/gpdemo/clusterConfigFile"

    @postmaster
    Scenario: gpinitsystem creates a cluster with GUC gp_postmaster_address_family
        Given the database is initialized with checksum "off"
        When the user runs "gpconfig -s gp_postmaster_address_family"
        Then gpconfig should return a return code of 0
        And gpconfig should print "Coordinator value: auto" to stdout
        And gpconfig should print "Segment     value: auto" to stdout
        When the user runs "gpconfig --skipvalidation -c gp_postmaster_address_family -v ipv4"
        Then gpconfig should return a return code of 0
        When the user runs "gpstop -ar"
        Then gpstop should return a return code of 0
        When the user runs "gpconfig -s gp_postmaster_address_family"
        Then gpconfig should return a return code of 0
        And gpconfig should print "Coordinator value: ipv4" to stdout
        And gpconfig should print "Segment     value: ipv4" to stdout
        When the user runs "gpconfig --skipvalidation -c gp_postmaster_address_family -v auto"
        Then gpconfig should return a return code of 0
        When the user runs "gpstop -ar"
        Then gpstop should return a return code of 0

    Scenario: gpinitsystem exits with status 1 when the user provides invalid option
        Given create demo cluster config
         When the user runs command "gpinitsystem -a -c ../gpAux/gpdemo/clusterConfigFile --ignore-warnings" eok
         Then gpinitsystem should return a return code of 1
          And gpinitsystem should print "Unknown option --ignore-warnings" to stdout
        Given the user runs "gpstate"
         Then gpstate should return a return code of 2
