@gprecoverseg
Feature: gprecoverseg tests

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg recovery with a recovery configuration file and differential flag
      Given the database is running
        And all the segments are running
        And the segments are synchronized
        And user immediately stops all mirror processes for content 0,1,2
        And the user waits until mirror on content 0,1,2 is down
        And user can start transactions
        And the gprecoverseg input file "recover_config_file" is cleaned up
       When a gprecoverseg input file "recover_config_file" is created with all the failed segments and valid recovery type
        And the user runs "gprecoverseg -i /tmp/recover_config_file -a --differential"
       Then gprecoverseg should return a return code of 0
        And verify that mirror on content 0,1,2 is up
        And gprecoverseg should print "Synchronization mode.* = Differential" to stdout 2 times
        And gprecoverseg should print "Synchronization mode.* = Full" to stdout 1 times
        And all the segments are running
        And the segments are synchronized


  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg" with a recovery configuration file specifying the recovery type
      Given the database is running
        And all the segments are running
        And the segments are synchronized
        And user immediately stops all mirror processes for content 0,1,2
        And the user waits until mirror on content 0,1,2 is down
        And user can start transactions
        And the gprecoverseg input file "recover_config_file" is cleaned up
       When a gprecoverseg input file "recover_config_file" is created with all the failed segments and invalid recovery type
        And the user runs "gprecoverseg -i /tmp/recover_config_file -a"
       Then gprecoverseg should return a return code of 2
        And gprecoverseg should print "Invalid recovery type provided, please provide any of I,D,F,i,d,f as recovery_type" to stdout
        And verify that mirror on content 0,1,2 is down
       When a gprecoverseg input file "recover_config_file" is created with all the failed segments and valid recovery type
        And the user runs "gprecoverseg -i /tmp/recover_config_file -a"
       Then gprecoverseg should return a return code of 0
        And verify that mirror on content 0,1,2 is up
        And gprecoverseg should print "Synchronization mode.*= Incremental" to stdout 1 times
        And gprecoverseg should print "Synchronization mode.* = Differential" to stdout 1 times
        And gprecoverseg should print "Synchronization mode.* = Full" to stdout 1 times
        And all the segments are running
        And the segments are synchronized

    Scenario Outline: <scenario> recovery works with tablespaces
        Given the database is running
          And user stops all primary processes
          And user can start transactions
          And a tablespace is created with data
         When the user runs "gprecoverseg <args>"
         Then gprecoverseg should return a return code of 0
          And gprecoverseg should print "Future gprecoverseg executions might remove the currently created pg_basebackup/pg_rewind/rsync progress files, please save these files if needed." to stdout
          And the segments are synchronized
          And verify replication slot internal_wal_replication_slot is available on all the segments
          And the tablespace is valid
          And the tablespace has valid symlink
          And the database segments are in execute mode

        Given another tablespace is created with data
         When the user runs "gprecoverseg -ra"
         Then gprecoverseg should return a return code of 0
          And the segments are synchronized
          And verify replication slot internal_wal_replication_slot is available on all the segments
          And the tablespace is valid
          And the tablespace has valid symlink
          And the other tablespace is valid
          And the database segments are in execute mode
      Examples:
        | scenario     | args               |
        | incremental  | -a                 |
        | differential | -a --differential  |
        | full         | -aF                |

    @demo_cluster
    @concourse_cluster
    Scenario: differential recovery runs successfully
        Given the database is running
          And the segments are synchronized
          And verify replication slot internal_wal_replication_slot is available on all the segments
          And user stops all primary processes
          And user can start transactions
         When the user runs "gprecoverseg -av --differential"
         Then gprecoverseg should return a return code of 0
          And gprecoverseg should print "Successfully dropped replication slot internal_wal_replication_slot" to stdout
          And gprecoverseg should print "Successfully created replication slot internal_wal_replication_slot" to stdout
          And gprecoverseg should print "Segments successfully recovered" to stdout
          And the user waits until mirror on content 0,1,2 is up
          And verify replication slot internal_wal_replication_slot is available on all the segments
          And the segments are synchronized
          And the cluster is rebalanced

    Scenario: differential recovery shows error message if run with the wrong argument
      Given the database is running
        And user stops all primary processes
        And user can start transactions
        And a gprecoverseg directory under '/tmp' with mode '0700' is created
        And a gprecoverseg input file is created
       When the user runs "gprecoverseg -a --differential -F"
       Then gprecoverseg should return a return code of 2
        And gprecoverseg should print "Only one of -F and --differential may be specified" to stdout
       When the user runs "gprecoverseg -a --differential -p localhost"
       Then gprecoverseg should return a return code of 2
        And gprecoverseg should print "Only one of -p, -r and --differential may be specified" to stdout
       When the user runs "gprecoverseg -a --differential -o outputConfigFile"
       Then gprecoverseg should return a return code of 2
        And gprecoverseg should print "Invalid -o provided with --differential argument" to stdout
       When the user runs "gprecoverseg -a --differential"
       Then gprecoverseg should return a return code of 0
        And the segments are synchronized
        And the cluster is rebalanced


    @demo_cluster
    @concourse_cluster
    Scenario: Differential recovery succeeds if previous incremental recovery failed
        Given the database is running
          And user stops all primary processes
          And user can start transactions
          And all files in pg_xlog directory are deleted from data directory of preferred primary of content 0,1,2
         When the user runs "gprecoverseg -a"
         Then gprecoverseg should return a return code of 1
          And user can start transactions
          And verify that mirror on content 0,1,2 is down
         When the user runs "gprecoverseg -a --differential"
         Then gprecoverseg should return a return code of 0
          And verify that mirror on content 0,1,2 is up
          And verify replication slot internal_wal_replication_slot is available on all the segments
          And the cluster is rebalanced

    @demo_cluster
    @concourse_cluster
    Scenario: Differential recovery succeeds if previous full recovery failed
        Given the database is running
          And user stops all primary processes
          And user can start transactions
          And a gprecoverseg directory under '/tmp' with mode '0700' is created
          And a gprecoverseg input file is created
          And edit the input file to recover mirror with content 0 incremental
          And edit the input file to recover mirror with content 1 full inplace
          And edit the input file to recover mirror with content 2 to a new directory on remote host with mode 0000
         When the user runs gprecoverseg with input file and additional args "-a"
         Then gprecoverseg should return a return code of 1
          And user can start transactions
          And verify that mirror on content 0,1 is up
          And verify that mirror on content 2 is down
         When the user runs "gprecoverseg -a --differential"
         Then gprecoverseg should return a return code of 0
          And verify that mirror on content 0,1,2 is up
          And verify replication slot internal_wal_replication_slot is available on all the segments
          And the cluster is rebalanced

    @concourse_cluster
    Scenario: gpstate track of differential recovery for single host
      Given the database is running
      And all files in gpAdminLogs directory are deleted on all hosts in the cluster
      And user immediately stops all mirror processes for content 0
      And the user waits until mirror on content 0 is down
      And user can start transactions
      And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
      And sql "DROP TABLE IF EXISTS test_recoverseg_1; CREATE TABLE test_recoverseg_1 AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
      When the user asynchronously runs "gprecoverseg -a --differential" and the process is saved
      Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies that all dbids progress with pg_data are present
      When the user runs "gpstate -e"
      Then gpstate should print "Segments in recovery" to stdout
      And gpstate output contains "differential" entries for mirrors of content 0
          And gpstate output looks like
              | Segment | Port   | Recovery type  | Stage                                      | Completed bytes \(kB\) | Percentage completed |
              | \S+     | [0-9]+ | differential   | Syncing pg_data of dbid 6                  | ([\d,]+)[ \t]          | \d+%                 |
      And the user waits until saved async process is completed
      And all files in gpAdminLogs directory are deleted on all hosts in the cluster
      And sql "DROP TABLE IF EXISTS test_recoverseg;" is executed in "postgres" db
      And sql "DROP TABLE IF EXISTS test_recoverseg_1;" is executed in "postgres" db
      And the cluster is rebalanced


    @concourse_cluster
    Scenario: check Tablespace Recovery Progress with gpstate
       Given the database is running
      And all files in gpAdminLogs directory are deleted on all hosts in the cluster
      And user immediately stops all mirror processes for content 0
      And user can start transactions
      And a tablespace is created with data
      And insert additional data into the tablespace
      When the user asynchronously runs "gprecoverseg -a --differential" and the process is saved
      Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies that all dbids progress with tablespace are present
      When the user runs "gpstate -e"
      Then gpstate should print "Segments in recovery" to stdout
      And gpstate output contains "differential" entries for mirrors of content 0
          And gpstate output looks like
              | Segment | Port   | Recovery type  | Stage                                      | Completed bytes \(kB\) | Percentage completed |
              | \S+     | [0-9]+ | differential   | Syncing tablespace of dbid 6 for oid \d+   | ([\d,]+)[ \t]          | \d+%                 |
      And the user waits until saved async process is completed
      And all files in gpAdminLogs directory are deleted on all hosts in the cluster
      And the cluster is rebalanced


    Scenario: full recovery works with tablespaces
        Given the database is running
          And a tablespace is created with data
          And user stops all primary processes
          And user can start transactions
         When the user runs "gprecoverseg -a -F"
         Then gprecoverseg should return a return code of 0
          And the segments are synchronized
          And the tablespace is valid

        Given another tablespace is created with data
         When the user runs "gprecoverseg -ra"
         Then gprecoverseg should return a return code of 0
          And the segments are synchronized
          And the tablespace is valid
          And the other tablespace is valid

    Scenario Outline: full recovery limits number of parallel processes correctly
        Given the database is running
        And 2 gprecoverseg directory under '/tmp/recoverseg' with mode '0700' is created
        And a good gprecoverseg input file is created for moving 2 mirrors
        When the user runs gprecoverseg with input file and additional args "-a -v <args>"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should only spawn up to <master_workers> workers in WorkerPool
        And check if gprecoverseg ran "$GPHOME/sbin/gpsegsetuprecovery.py" 1 times with args "-b <segHost_workers>"
        And check if gprecoverseg ran "$GPHOME/sbin/gpsegrecovery.py" 1 times with args "-b <segHost_workers>"
        And gpsegsetuprecovery should only spawn up to <segHost_workers> workers in WorkerPool
        And gpsegrecovery should only spawn up to <segHost_workers> workers in WorkerPool
        And check if gprecoverseg ran "$GPHOME/sbin/gpsegstop.py" 1 times with args "-b <segHost_workers>"
        And the segments are synchronized
        And check segment conf: postgresql.conf

      Examples:
        | args      | master_workers | segHost_workers |
        | -B 1 -b 1 |  1             |  1              |
        | -B 2 -b 1 |  2             |  1              |
        | -B 1 -b 2 |  1             |  2              |

    Scenario: Differential recovery limits number of parallel processes correctly
        Given the database is running
        And user immediately stops all primary processes for content 0,1,2
        And user can start transactions
        When the user runs "gprecoverseg -av --differential -B 1 -b 2"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should only spawn up to 1 workers in WorkerPool
        And check if gprecoverseg ran "$GPHOME/sbin/gpsegsetuprecovery.py" 1 times with args "-b 2"
        And check if gprecoverseg ran "$GPHOME/sbin/gpsegrecovery.py" 1 times with args "-b 2"
        And gpsegsetuprecovery should only spawn up to 2 workers in WorkerPool
        And gpsegrecovery should only spawn up to 2 workers in WorkerPool
        And the segments are synchronized
        And check segment conf: postgresql.conf
        And the cluster is rebalanced

    Scenario Outline: Rebalance correctly limits the number of concurrent processes
      Given the database is running
      And user stops all primary processes
      And user can start transactions
      And all files in gpAdminLogs directory are deleted on all hosts in the cluster
      And the user runs "gprecoverseg -a -v <args>"
      And gprecoverseg should return a return code of 0
      And the segments are synchronized
      When the user runs "gprecoverseg -ra -v <args>"
      Then gprecoverseg should return a return code of 0
      And gprecoverseg should only spawn up to <master_workers> workers in WorkerPool
      And gpsegsetuprecovery should only spawn up to <segHost_workers> workers in WorkerPool
      And gpsegrecovery should only spawn up to <segHost_workers> workers in WorkerPool
      And check if gprecoverseg ran "$GPHOME/sbin/gpsegsetuprecovery.py" 1 times with args "-b <segHost_workers>"
      And check if gprecoverseg ran "$GPHOME/sbin/gpsegrecovery.py" 1 times with args "-b <segHost_workers>"
      And check if gprecoverseg ran "$GPHOME/sbin/gpsegstop.py" 1 times with args "-b <segHost_workers>"
      And the segments are synchronized
      And check segment conf: postgresql.conf

    Examples:
      | args      | master_workers | segHost_workers |
      | -B 1 -b 1 |  1             |  1              |
      | -B 2 -b 1 |  2             |  1              |
      | -B 1 -b 2 |  1             |  2              |

  Scenario: gprecoverseg should not output bootstrap error on success
        Given the database is running
        And user immediately stops all primary processes
        And user can start transactions
        When the user runs "gprecoverseg -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Initiating segment recovery. Upon completion, will start the successfully recovered segments" to stdout
        And gprecoverseg should print "Done!" to stdout for each mirror
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
        And the segments are synchronized
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
	    And the segments are synchronized

  Scenario: gprecoverseg full recovery displays pg_controldata success info
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And user stops all mirror processes
        When user can start transactions
        And the user runs "gprecoverseg -F -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Successfully finished pg_controldata.* for dbid.*" to stdout
        And the segments are synchronized
        And check segment conf: postgresql.conf

  Scenario Outline: gprecoverseg <scenario> recovery displays pg_controldata success info
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And user stops all mirror processes
        When user can start transactions
        And the user runs "gprecoverseg <args>"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Successfully finished pg_controldata.* for dbid.*" to stdout
        And the segments are synchronized
        And verify replication slot internal_wal_replication_slot is available on all the segments
        And check segment conf: postgresql.conf
     Examples:
        | scenario     | args               |
        | incremental  | -a                 |
        | differential | -a --differential  |
        | full         | -aF                |

  Scenario: gprecoverseg mixed recovery displays pg_basebackup and rewind progress to the user
      Given the database is running
      And all the segments are running
      And the segments are synchronized
      And all files in gpAdminLogs directory are deleted
      And user immediately stops all primary processes
      And user can start transactions
      And sql "DROP TABLE if exists test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,10000) AS i" is executed in "postgres" db
      And the "test_recoverseg" table row count in "postgres" is saved
      And a gprecoverseg directory under '/tmp' with mode '0700' is created
      And a gprecoverseg input file is created
      And edit the input file to recover mirror with content 0 to a new directory with mode 0700
      And edit the input file to recover mirror with content 1 full inplace
      And edit the input file to recover mirror with content 2 incremental
      When the user runs gprecoverseg with input file and additional args "-av"
      Then gprecoverseg should return a return code of 0
      And gprecoverseg should print "pg_basebackup: base backup completed" to stdout for mirrors with content 0,1
      And gprecoverseg should print "Done!" to stdout for mirrors with content 2
      And gprecoverseg should print "Segments successfully recovered" to stdout
      And check if gprecoverseg ran gpsegsetuprecovery.py 1 times with the expected args
      And check if gprecoverseg ran gpsegrecovery.py 1 times with the expected args
      And gpAdminLogs directory has "pg_basebackup*" files
      And gpAdminLogs directory has "pg_rewind*" files
      And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
      And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts
      And the old data directories are cleaned up for content 0

      And all the segments are running
      And the segments are synchronized
      And the user runs "gprecoverseg -ar"
      And gprecoverseg should return a return code of 0
      And the row count from table "test_recoverseg" in "postgres" is verified against the saved data

    Scenario: gprecoverseg incremental recovery displays pg_rewind progress to the user
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And all files in gpAdminLogs directory are deleted
        And user immediately stops all primary processes
        And user can start transactions
        When the user runs "gprecoverseg -a -s"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Done!" to stdout for each mirror
        And gpAdminLogs directory has "pg_rewind*" files
        And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
        And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts

        And all the segments are running
        And the segments are synchronized
        And the cluster is rebalanced

  Scenario: gprecoverseg does not display pg_basebackup progress to the user when --no-progress option is specified
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And user stops all mirror processes
        When user can start transactions
        And the user runs "gprecoverseg -F -a --no-progress"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Initiating segment recovery. Upon completion, will start the successfully recovered segments" to stdout
        And gprecoverseg should not print "pg_basebackup: base backup completed" to stdout
        And gpAdminLogs directory has "pg_basebackup*" files
        And all the segments are running
        And the segments are synchronized

    Scenario: gprecoverseg runs with given master data directory option
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And user stops all mirror processes
          And user can start transactions
          And "MASTER_DATA_DIRECTORY" environment variable is not set
         Then the user runs utility "gprecoverseg" with master data directory and "-F -a"
          And gprecoverseg should return a return code of 0
          And "MASTER_DATA_DIRECTORY" environment variable should be restored
          And all the segments are running
          And the segments are synchronized

    Scenario: gprecoverseg priorities given master data directory over env option
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And user stops all mirror processes
          And user can start transactions
          And the environment variable "MASTER_DATA_DIRECTORY" is set to "/tmp/"
         Then the user runs utility "gprecoverseg" with master data directory and "-F -a"
          And gprecoverseg should return a return code of 0
          And "MASTER_DATA_DIRECTORY" environment variable should be restored
          And all the segments are running
          And the segments are synchronized

    Scenario: gprecoverseg differential recovery displays rsync progress to the user
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And all files in gpAdminLogs directory are deleted on all hosts in the cluster
        And user stops all mirror processes
        When user can start transactions
        And the user runs "gprecoverseg --differential -a -s"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Initiating segment recovery. Upon completion, will start the successfully recovered segments" to stdout
        And gprecoverseg should print "total size" to stdout for each mirror
        And gprecoverseg should print "Segments successfully recovered" to stdout
        And gpAdminLogs directory has "rsync*" files on all segment hosts
        And gpAdminLogs directory has "gpsegrecovery*" files
        And gpAdminLogs directory has "gpsegsetuprecovery*" files
        And all the segments are running
        And the segments are synchronized
        And verify replication slot internal_wal_replication_slot is available on all the segments
        And check segment conf: postgresql.conf

  Scenario: gprecoverseg does not display rsync progress to the user when --no-progress option is specified
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And all files in gpAdminLogs directory are deleted on all hosts in the cluster
        And user stops all mirror processes
        When user can start transactions
        And the user runs "gprecoverseg --differential -a -s --no-progress"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Initiating segment recovery. Upon completion, will start the successfully recovered segments" to stdout
        And gprecoverseg should not print "total size is .*  speedup is .*" to stdout
        And gprecoverseg should print "Segments successfully recovered" to stdout
        And gpAdminLogs directory has "rsync*" files on all segment hosts
        And gpAdminLogs directory has "gpsegrecovery*" files
        And gpAdminLogs directory has "gpsegsetuprecovery*" files
        And all the segments are running
        And the segments are synchronized
        And check segment conf: postgresql.conf


  Scenario: When gprecoverseg incremental recovery uses pg_rewind to recover and an existing postmaster.pid on the killed primary segment corresponds to a non postgres process
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the "primary" segment information is saved
        When the postmaster.pid file on "primary" segment is saved
        And user stops all primary processes
        When user can start transactions
        And the background pid is killed on "primary" segment
        And we run a sample background script to generate a pid on "primary" segment
        And we generate the postmaster.pid file with the background pid on "primary" segment
        And the user runs "gprecoverseg -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Initiating segment recovery. Upon completion, will start the successfully recovered segments" to stdout
        And gprecoverseg should print "no rewind required" to stdout for each mirror
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
        And all the segments are running
        And the segments are synchronized
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
        And the segments are synchronized
        And the backup pid file is deleted on "primary" segment
        And the background pid is killed on "primary" segment

  Scenario: Pid does not correspond to any running process
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the "primary" segment information is saved
        When the postmaster.pid file on "primary" segment is saved
        And user stops all primary processes
        When user can start transactions
        And we generate the postmaster.pid file with a non running pid on the same "primary" segment
        And the user runs "gprecoverseg -a"
        Then gprecoverseg should print "Initiating segment recovery. Upon completion, will start the successfully recovered segments" to stdout
        And gprecoverseg should print "no rewind required" to stdout for each mirror
        And gprecoverseg should return a return code of 0
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
        And all the segments are running
        And the segments are synchronized
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
        And the segments are synchronized
        And the backup pid file is deleted on "primary" segment

    Scenario: pg_isready functions on recovered segments
        Given the database is running
          And all the segments are running
          And the segments are synchronized
         When user stops all primary processes
          And user can start transactions

         When the user runs "gprecoverseg -a"
         Then gprecoverseg should return a return code of 0
          And the segments are synchronized

         When the user runs "gprecoverseg -ar"
         Then gprecoverseg should return a return code of 0
          And all the segments are running
          And the segments are synchronized
          And pg_isready reports all primaries are accepting connections

  Scenario: gprecoverseg incremental recovery displays status for mirrors after pg_rewind call
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And user stops all mirror processes
        And user can start transactions
        When the user runs "gprecoverseg -a -s"
        And gprecoverseg should print "skipping pg_rewind on mirror as recovery.conf is present" to stdout
        Then gprecoverseg should return a return code of 0
        And gpAdminLogs directory has "pg_rewind*" files
        And all the segments are running
        And the segments are synchronized
        And the cluster is rebalanced

    @backup_restore_bashrc
    Scenario: gprecoverseg should not return error when banner configured on host
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        When the user sets banner on host
        And user stops all mirror processes
        And user can start transactions
        When the user runs "gprecoverseg -a"
        Then gprecoverseg should return a return code of 0
        And all the segments are running
        And the segments are synchronized
        And the cluster is rebalanced

  @demo_cluster
  Scenario: gprecoverseg should not give warning if pg_basebackup is running for the up segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1
    And user can start transactions
    And an FTS probe is triggered
    And the user suspend the walsender on the primary on content 2
    And the user asynchronously runs pg_basebackup with primary of content 2 as source and the process is saved
    And an FTS probe is triggered
    And gp_stat_replication table has pg_basebackup entry for content 2
    When the user runs "gprecoverseg -avF"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should not print "No basebackup running" to stdout
    And verify that mirror on content 0,1 is up
    And an FTS probe is triggered
    And gp_stat_replication table has pg_basebackup entry for content 2
    And the user reset the walsender on the primary on content 2
    And the user waits until saved async process is completed
    And the user waits until mirror on content 2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg gives warning if pg_basebackup already running for one of the failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user suspend the walsender on the primary on content 0
    And the user asynchronously runs "gprecoverseg -aF" and the process is saved
    And the user just waits until recovery_progress.file is created in gpAdminLogs
    And user waits until gp_stat_replication table has no pg_basebackup entries for content 1,2
    And an FTS probe is triggered
    And the user waits until mirror on content 1,2 is up
    And verify that mirror on content 0 is down
    And the gprecoverseg lock directory is removed
    And user immediately stops all primary processes for content 1,2
    And the user waits until mirror on content 1,2 is down
    When the user runs "gprecoverseg -avF"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_basebackup running for segments with contentIds [0], skipping recovery of these segments" to logfile
    And verify that mirror on content 1,2 is up
    And verify that mirror on content 0 is down
    And an FTS probe is triggered
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0 is up
    And the user runs "gprecoverseg -avF"
    Then gprecoverseg should print "No basebackup running" to stdout
    And gprecoverseg should return a return code of 0
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg gives warning if pg_basebackup already running for some of the failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user suspend the walsender on the primary on content 0
    And the user suspend the walsender on the primary on content 1
    And the user asynchronously runs "gprecoverseg -aF" and the process is saved
    And the user just waits until recovery_progress.file is created in gpAdminLogs
    And user waits until gp_stat_replication table has no pg_basebackup entries for content 2
    And an FTS probe is triggered
    And the user waits until mirror on content 2 is up
    And verify that mirror on content 0,1 is down
    And the gprecoverseg lock directory is removed
    And user immediately stops all primary processes for content 2
    And the user waits until mirror on content 2 is down
    When the user runs "gprecoverseg -avF"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_basebackup running for segments with contentIds [0, 1], skipping recovery of these segments" to logfile
    And verify that mirror on content 2 is up
    And verify that mirror on content 0,1 is down
    And an FTS probe is triggered
    And the user reset the walsender on the primary on content 0
    And the user reset the walsender on the primary on content 1
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1 is up
    And the user runs "gprecoverseg -avF"
    Then gprecoverseg should print "No basebackup running" to stdout
    And gprecoverseg should return a return code of 0
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg -aF gives warning if pg_basebackup already running for all of the failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user suspend the walsender on the primary on content 0
    And the user suspend the walsender on the primary on content 1
    And the user suspend the walsender on the primary on content 2
    And the user asynchronously runs "gprecoverseg -aF" and the process is saved
    And the user just waits until recovery_progress.file is created in gpAdminLogs
    And verify that mirror on content 0,1,2 is down
    And the gprecoverseg lock directory is removed
    When the user runs "gprecoverseg -aF"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_basebackup running for segments with contentIds [0, 1, 2], skipping recovery of these segments" to logfile
    And gprecoverseg should print "No segments to recover" to stdout
    And verify that mirror on content 0,1,2 is down
    And an FTS probe is triggered
    And the user reset the walsender on the primary on content 0
    And the user reset the walsender on the primary on content 1
    And the user reset the walsender on the primary on content 2
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1,2 is up
    And the user runs "gprecoverseg -avF"
    Then gprecoverseg should print "No basebackup running" to stdout
    And gprecoverseg should return a return code of 0
    And verify that mirror on content 0,1,2 is up
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg -i gives warning if pg_basebackup already running for all failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user suspend the walsender on the primary on content 0
    And the user suspend the walsender on the primary on content 1
    And the user suspend the walsender on the primary on content 2
    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 full inplace
    And edit the input file to recover mirror with content 1 full inplace
    And edit the input file to recover mirror with content 2 full inplace
    When the user asynchronously runs gprecoverseg with input file and additional args "-a" and the process is saved
    Then the user just waits until recovery_progress.file is created in gpAdminLogs
    And verify that mirror on content 0,1,2 is down
    And the gprecoverseg lock directory is removed
    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should print "Found pg_basebackup running for segments with contentIds [0, 1, 2], skipping recovery of these segments" to logfile
    And gprecoverseg should print "No segments to recover" to stdout
    And gprecoverseg should return a return code of 0
    And verify that mirror on content 0,1,2 is down
    And an FTS probe is triggered
    And the user reset the walsender on the primary on content 0
    And the user reset the walsender on the primary on content 1
    And the user reset the walsender on the primary on content 2
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And verify that mirror on content 0,1,2 is up
    And user can start transactions
    When the user runs gprecoverseg with input file and additional args "-av"
    Then gprecoverseg should print "No basebackup running" to stdout
    And gprecoverseg should return a return code of 0
    Then the cluster is rebalanced

  Scenario: gprecoverseg errors out with restricted options
    Given the database is running
    And user stops all primary processes
    And user can start transactions
    When the user runs "gprecoverseg xyz"
    Then gprecoverseg should return a return code of 2
    And gprecoverseg should print "Recovers a primary or mirror segment instance" to stdout
    And gprecoverseg should print "too many arguments: only options may be specified" to stdout
    When the user runs "gprecoverseg -a"
    Then gprecoverseg should return a return code of 0
    And the segments are synchronized
    And the cluster is rebalanced

    Scenario: gprecoverseg keeps segment logs
      Given the database is running
      And all the segments are running
      And the segments are synchronized
      And the "primary" segment information is saved
      And the "primary" segment pg_log dir content saved
      When user kills "primary" segment process with signal "SIGKILL"
      And user can start transactions
      And the user runs "gprecoverseg -a"
      Then gprecoverseg should return a return code of 0
      And the "primary" segment pg_log directory content preserved


########################### @concourse_cluster tests ###########################
# The @concourse_cluster tag denotes the scenario that requires a remote cluster
    @demo_cluster
    @concourse_cluster
    Scenario Outline: <scenario> recovery skips unreachable segments
      Given the database is running
      And all the segments are running
      And the segments are synchronized

      And the primary on content 0 is stopped
      And user can start transactions
      And the primary on content 1 is stopped
      And user can start transactions
      And the status of the primary on content 0 should be "d"
      And the status of the primary on content 1 should be "d"

      And the host for the primary on content 1 is made unreachable

      And the user runs psql with "-c 'CREATE TABLE IF NOT EXISTS test_recoverseg (i int)'" against database "postgres"
      And the user runs psql with "-c 'INSERT INTO test_recoverseg SELECT generate_series(1, 10000)'" against database "postgres"

      When the user runs "gprecoverseg <args>"
      Then gprecoverseg should print "Not recovering segment \d because invalid_host is unreachable" to stdout
      And the user runs psql with "-c 'SELECT gp_request_fts_probe_scan()'" against database "postgres"
      And the status of the primary on content 0 should be "u"
      And the status of the primary on content 1 should be "d"

      # Rebalance all possible segments and skip unreachable segment pairs.
      When the user runs "gprecoverseg -ar"
      Then gprecoverseg should return a return code of 0
      And gprecoverseg should print "Not rebalancing primary segment dbid \d with its mirror dbid \d because one is either down, unreachable, or not synchronized" to stdout
      And content 0 is balanced
      And content 1 is unbalanced

      And the user runs psql with "-c 'DROP TABLE test_recoverseg'" against database "postgres"
      And the cluster is returned to a good state

      Examples:
        | scenario     | args               |
        | incremental  | -a                 |
        | differential | -a --differential  |
        | full         | -aF                |

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates output sample config file correctly when failed segment hosts are unreachable
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And the primary on content 1 is stopped
    And user can start transactions
    And the primary on content 2 is stopped
    And user can start transactions
    And the status of the primary on content 1 should be "d"
    And the status of the primary on content 2 should be "d"
    And the host for the primary on content 1 is made unreachable
    When the user runs "gprecoverseg -o /tmp/output_config"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "One or more hosts are not reachable via SSH." to stdout
    And gprecoverseg should print "Host invalid_host is unreachable" to stdout
    And the created config file /tmp/output_config contains the commented row for unreachable failed segment
    And the cluster is returned to a good state

  @concourse_cluster
  Scenario Outline: <scenario> incremental recovery works with tablespaces on a multi-host environment
    Given the database is running
    And user stops all primary processes
    And user can start transactions
    And a tablespace is created with data
    When the user runs "gprecoverseg <args>"
    Then gprecoverseg should return a return code of 0
    And the segments are synchronized
    And the tablespace is valid
    And the tablespace has valid symlink
    And the database segments are in execute mode

    Given another tablespace is created with data
    When the user runs "gprecoverseg -ra"
    Then gprecoverseg should return a return code of 0
    And the segments are synchronized
    And verify replication slot internal_wal_replication_slot is available on all the segments
    And the tablespace is valid
    And the tablespace has valid symlink
    And the other tablespace is valid
    And the database segments are in execute mode
    Examples:
        | scenario     | args               |
        | incremental  | -a                 |
        | differential | -a --differential  |
        | full         | -aF                |

  @concourse_cluster
  Scenario: recovering a host with tablespaces succeeds
    Given the database is running

        # Add data including tablespaces
    And a tablespace is created with data
    And database "gptest" exists
    And the user connects to "gptest" with named connection "default"
    And the user runs psql with "-c 'CREATE TABLE public.before_host_is_down (i int) DISTRIBUTED BY (i)'" against database "gptest"
    And the user runs psql with "-c 'INSERT INTO public.before_host_is_down SELECT generate_series(1, 10000)'" against database "gptest"
    And the "public.before_host_is_down" table row count in "gptest" is saved

        # Stop one of the nodes as if for hardware replacement and remove any traces as if it was a new node.
        # Recoverseg requires the host being restored have the same hostname.
    And the user runs "gpstop -a --host sdw1"
    And gpstop should return a return code of 0
    And the user runs remote command "rm -rf /data/gpdata/*" on host "sdw1"
    And user can start transactions

        # Add data after one of the nodes is down for maintenance
    And database "gptest" exists
    And the user connects to "gptest" with named connection "default"
    And the user runs psql with "-c 'CREATE TABLE public.after_host_is_down (i int) DISTRIBUTED BY (i)'" against database "gptest"
    And the user runs psql with "-c 'INSERT INTO public.after_host_is_down SELECT generate_series(1, 10000)'" against database "gptest"
    And the "public.after_host_is_down" table row count in "gptest" is saved

        # restore the down node onto a node with the same hostname
    When the user runs "gprecoverseg -a -p sdw1"
    Then gprecoverseg should return a return code of 0
    And all the segments are running
    And user can start transactions
    And the user runs "gprecoverseg -ra"
    And gprecoverseg should return a return code of 0
    And all the segments are running
    And the segments are synchronized
    And user can start transactions

        # verify the data
    And the tablespace is valid
    And the tablespace has valid symlink
    And the row count from table "public.before_host_is_down" in "gptest" is verified against the saved data
    And the row count from table "public.after_host_is_down" in "gptest" is verified against the saved data

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in gpAdminLogs
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    When the user asynchronously runs "gprecoverseg -a" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And a sample recovery_progress.file is created from saved lines
    And we run a sample background script to generate a pid on "master" segment
    Then a sample gprecoverseg.lock directory is created using the background pid in master_data_directory
    When the user runs "gpstate -e"
    Then gpstate should print "Segments in recovery" to stdout
#    And gpstate output contains "incremental,incremental,incremental" entries for mirrors of content 0,1,2
#    And gpstate output looks like
#      | Segment | Port   | Recovery type  | Completed bytes \(kB\) | Total bytes \(kB\) | Percentage completed |
#      | \S+     | [0-9]+ | incremental    | [0-9]+                 | [0-9]+             | [0-9]+\%             |
#      | \S+     | [0-9]+ | incremental    | [0-9]+                 | [0-9]+             | [0-9]+\%             |
#      | \S+     | [0-9]+ | incremental    | [0-9]+                 | [0-9]+             | [0-9]+\%             |
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And the background pid is killed on "master" segment
    Then the gprecoverseg lock directory is removed

    And the cluster is rebalanced
    And user immediately stops all primary processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    When the user runs "gpstate -e"
    Then gpstate should print "Segments in recovery" to stdout
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in gpAdminLogs for full recovery of mirrors
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all mirror processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And the user waits until mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in custom logdir for full recovery of mirrors
    Given the database is running
    And all files in "/tmp/custom_logdir" directory are deleted on all hosts in the cluster
    And user immediately stops all mirror processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -aF -l /tmp/custom_logdir" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in /tmp/custom_logdir and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in /tmp/custom_logdir
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in /tmp/custom_logdir
    And the user waits until mirror on content 0,1,2 is up
    And user can start transactions
    And all files in "/tmp/custom_logdir" directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg creates recovery_progress.file in gpAdminLogs for differential recovery of mirrors
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all mirror processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -a --differential" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And the user waits until mirror on content 0,1,2 is up
    And user can start transactions
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster


  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg -i creates recovery_progress.file in gpAdminLogs for mixed recovery of mirrors
    Given the database is running
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And the user waits until mirror on content 0,1,2 is down
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 to a new directory on remote host with mode 0700
    And edit the input file to recover mirror with content 1 full inplace
    And edit the input file to recover mirror with content 2 incremental
    When the user asynchronously runs gprecoverseg with input file and additional args "-a" and the process is saved
    And the user suspend the walsender on the primary on content 0
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And recovery_progress.file should not exist in gpAdminLogs
    And the user waits until mirror on content 0,1,2 is up
    And the old data directories are cleaned up for content 0
    And user can start transactions
    And check segment conf: postgresql.conf
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario:  SIGINT on gprecoverseg should delete the progress file
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    And the user suspend the walsender on the primary on content 0
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    When the user asynchronously sets up to end gprecoverseg process with SIGINT
    And the user waits until saved async process is completed
    Then recovery_progress.file should not exist in gpAdminLogs
    Then the user reset the walsender on the primary on content 0
    And the gprecoverseg lock directory is removed
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2 is up
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario:  SIGINT on gprecoverseg differential recovery should delete the progress file
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    When the user asynchronously runs "gprecoverseg -a --differential" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    Then verify if the gprecoverseg.lock directory is present in master_data_directory
    When the user asynchronously sets up to end gprecoverseg process with SIGINT
    And the user waits until saved async process is completed
    Then recovery_progress.file should not exist in gpAdminLogs
    Then the gprecoverseg lock directory is removed
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2 is up
    And the cluster is rebalanced



  @demo_cluster
  @concourse_cluster
  Scenario:  SIGHUP on gprecoverseg should not display progress in gpstate -e
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS a;" is executed in "postgres" db
    And the user suspend the walsender on the primary on content 0
    When the user asynchronously runs "gprecoverseg -aF" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    Then verify if the gprecoverseg.lock directory is present in master_data_directory
    When the user runs "gpstate -e"
    Then gpstate should print "Segments in recovery" to stdout
    When the user asynchronously sets up to end gprecoverseg process with SIGHUP
    And the user waits until saved async process is completed
    When the user runs "gpstate -e"
    Then gpstate should not print "Segments in recovery" to stdout
    Then the user reset the walsender on the primary on content 0
    And the user waits until mirror on content 0,1,2 is up
    And the gprecoverseg lock directory is removed
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg mixed recovery segments come up even if one basebackup takes longer
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user suspend the walsender on the primary on content 0
    And sql "DROP TABLE if exists test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,100000000) AS i" is executed in "postgres" db
    And the "test_recoverseg" table row count in "postgres" is saved
    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 full inplace
    And edit the input file to recover mirror with content 1 full inplace
    And edit the input file to recover mirror with content 2 incremental
    When the user asynchronously runs gprecoverseg with input file and additional args "-a" and the process is saved
    Then the user waits until recovery_progress.file is created in gpAdminLogs and verifies its format
    And user waits until gp_stat_replication table has no pg_basebackup entries for content 1
    And an FTS probe is triggered
    And the user waits until mirror on content 1,2 is up
    And verify that mirror on content 0 is down
    And user can start transactions
    And verify that lines from recovery_progress.file are present in segment progress files in gpAdminLogs
    And the user reset the walsender on the primary on content 0
    And the user waits until saved async process is completed
    And gpAdminLogs directory has "pg_basebackup*" files on respective hosts only for content 0,1
    And gpAdminLogs directory has "pg_rewind*" files on respective hosts only for content 2
    And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
    And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts
    And the cluster is recovered in full and rebalanced
    And the row count from table "test_recoverseg" in "postgres" is verified against the saved data

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg incremental recovery segments come up even if one rewind fails
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And sql "DROP TABLE if exists test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,10000) AS i" is executed in "postgres" db
    And the "test_recoverseg" table row count in "postgres" is saved
    And all files in pg_xlog directory are deleted from data directory of preferred primary of content 0
    When the user runs "gprecoverseg -a"
    Then gprecoverseg should return a return code of 1
    And user can start transactions

    And check if incremental recovery failed for mirrors with content 0 for gprecoverseg
    And gprecoverseg should print "Failed to recover the following segments. You must run either gprecoverseg --differential or gprecoverseg -F for all incremental failures" to stdout
    And check if incremental recovery was successful for mirrors with content 1,2
    And gpAdminLogs directory has "pg_rewind*" files on all segment hosts
    And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
    And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts

    And the cluster is recovered in full and rebalanced
    And the row count from table "test_recoverseg" in "postgres" is verified against the saved data

  @demo_cluster
  Scenario Outline: gprecoverseg differential recovery segments come up even if recovery for one segment fails
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And sql "DROP TABLE if exists test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,10000) AS i" is executed in "postgres" db
    And the "test_recoverseg" table row count in "postgres" is saved
    And a temporary directory with mode '0000' is created under data_dir of primary with content 0
    When the user runs "gprecoverseg -av --differential"
    Then gprecoverseg should return a return code of 1
    And user can start transactions
    And check if differential recovery failed for mirrors with content 0 for gprecoverseg
    And gprecoverseg should print "Failed to recover the following segments. You must run either gprecoverseg --differential or gprecoverseg -F for all differential failures" to stdout
    And verify that mirror on content 1,2 is up
    And the segments are synchronized for content 1,2
    And gpAdminLogs directory has "rsync*" files on all segment hosts
    And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
    And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts
    And the temporary directory is removed
    And the cluster is recovered <args> and rebalanced
    And the row count from table "test_recoverseg" in "postgres" is verified against the saved data

    Examples:
      | scenario     | args               |
      | differential | using differential |
      | full         | in full            |

   @concourse_cluster
    Scenario: Propagating env var
    Given the database is running
    And An entry to send SUSPEND_PG_REWIND env var is added on all hosts of cluster
    And An entry to accept SUSPEND_PG_REWIND env var is added on all hosts of cluster

  @concourse_cluster
  Scenario: gprecoverseg gives warning if pg_rewind already running for one failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 2
    And user can start transactions
    And the environment variable "SUSPEND_PG_REWIND" is set to "600"
    And the user asynchronously runs "gprecoverseg -a" and the process is saved
    Then the user just waits until recovery_progress.file is created in gpAdminLogs
    And verify that mirror on content 2 is down
    And the gprecoverseg lock directory is removed
    And user immediately stops all primary processes for content 0,1
    And the user waits until mirror on content 0,1 is down
    And an FTS probe is triggered
    And user can start transactions
    And "SUSPEND_PG_REWIND" environment variable should be restored
    When the user runs "gprecoverseg -a"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_rewind running for segments with contentIds [2], skipping recovery of these segments" to logfile
    And verify that mirror on content 2 is down
    And verify that mirror on content 0,1 is up
    And pg_rewind is killed on mirror with content 2
    And the user asynchronously sets up to end gprecoverseg process with SIGHUP
    And the gprecoverseg lock directory is removed
    And verify that mirror on content 2 is down
    And the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2 is up
    And the cluster is rebalanced

  @concourse_cluster
  Scenario: gprecoverseg gives warning if pg_rewind already running for some failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 2,3
    And user can start transactions
    And the environment variable "SUSPEND_PG_REWIND" is set to "600"
    And the user asynchronously runs "gprecoverseg -a" and the process is saved
    Then the user just waits until recovery_progress.file is created in gpAdminLogs
    And verify that mirror on content 2,3 is down
    And the gprecoverseg lock directory is removed
    And user immediately stops all primary processes for content 0,1
    And the user waits until mirror on content 0,1 is down
    And an FTS probe is triggered
    And user can start transactions
    And "SUSPEND_PG_REWIND" environment variable should be restored
    When the user runs "gprecoverseg -a"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_rewind running for segments with contentIds [2, 3], skipping recovery of these segments" to logfile
    And verify that mirror on content 2,3 is down
    And verify that mirror on content 0,1 is up
    And pg_rewind is killed on mirror with content 2,3
    And the user asynchronously sets up to end gprecoverseg process with SIGHUP
    And the gprecoverseg lock directory is removed
    And verify that mirror on content 2,3 is down
    And the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2,3 is up
    And the cluster is rebalanced

  @concourse_cluster
  Scenario: gprecoverseg gives warning if pg_rewind already running for all failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2,3
    And user can start transactions
    And the environment variable "SUSPEND_PG_REWIND" is set to "600"
    And the user asynchronously runs "gprecoverseg -a" and the process is saved
    Then the user just waits until recovery_progress.file is created in gpAdminLogs
    And verify that mirror on content 0,1,2,3 is down
    And the gprecoverseg lock directory is removed
    And an FTS probe is triggered
    And user can start transactions
    When the user runs "gprecoverseg -aF"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_rewind running for segments with contentIds [0, 1, 2, 3], skipping recovery of these segments" to logfile
    And verify that mirror on content 0,1,2,3 is down
    And pg_rewind is killed on mirror with content 0,1,2,3
    And the user asynchronously sets up to end gprecoverseg process with SIGHUP
    And the gprecoverseg lock directory is removed
    And verify that mirror on content 0,1,2,3 is down
    And "SUSPEND_PG_REWIND" environment variable should be restored
    And the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2,3 is up
    And the cluster is rebalanced

  @concourse_cluster
  Scenario: gprecoverseg -i gives warning if pg_rewind already running for some of the failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1
    And user can start transactions
    And the environment variable "SUSPEND_PG_REWIND" is set to "600"
    And the user asynchronously runs "gprecoverseg -a" and the process is saved
    And the user just waits until recovery_progress.file is created in gpAdminLogs
    And verify that mirror on content 0,1 is down
    And the gprecoverseg lock directory is removed
    And user immediately stops all primary processes for content 2,3
    And the user waits until mirror on content 2,3 is down
    And an FTS probe is triggered
    And user can start transactions
    Then "SUSPEND_PG_REWIND" environment variable should be restored
    Given a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 full inplace
    And edit the input file to recover mirror with content 1 incremental
    And edit the input file to recover mirror with content 2 incremental
    And edit the input file to recover mirror with content 3 incremental
    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_rewind running for segments with contentIds [0, 1], skipping recovery of these segments" to logfile
    And verify that mirror on content 2,3 is up
    And verify that mirror on content 0,1 is down
    And pg_rewind is killed on mirror with content 0,1
    And the user asynchronously sets up to end gprecoverseg process with SIGHUP
    And the gprecoverseg lock directory is removed
    And verify that mirror on content 0,1 is down
    And the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2 is up
    And the cluster is rebalanced

  @concourse_cluster
  Scenario: gprecoverseg -i gives warning if pg_rewind already running for all of the failed segments
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2,3
    And user can start transactions
    And the environment variable "SUSPEND_PG_REWIND" is set to "600"
    And the user asynchronously runs "gprecoverseg -a" and the process is saved
    And the user just waits until recovery_progress.file is created in gpAdminLogs
    And verify that mirror on content 0,1,2,3 is down
    And the gprecoverseg lock directory is removed
    And an FTS probe is triggered
    And user can start transactions
    Given a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 full inplace
    And edit the input file to recover mirror with content 1 full inplace
    And edit the input file to recover mirror with content 2 incremental
    And edit the input file to recover mirror with content 3 incremental
    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should return a return code of 0
    And gprecoverseg should print "Found pg_rewind running for segments with contentIds [0, 1, 2, 3], skipping recovery of these segments" to logfile
    And verify that mirror on content 0,1,2,3 is down
    And pg_rewind is killed on mirror with content 0,1,2,3
    And the user asynchronously sets up to end gprecoverseg process with SIGHUP
    And the gprecoverseg lock directory is removed
    And verify that mirror on content 0,1,2,3 is down
    Then "SUSPEND_PG_REWIND" environment variable should be restored
    And the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And an FTS probe is triggered
    And the user waits until mirror on content 0,1,2,3 is up
    And the cluster is rebalanced
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg mixed recovery one basebackup fails and one rewind fails while others succeed
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And the information of contents 0,1,2 is saved
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions

    And sql "DROP TABLE if exists test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,10000) AS i" is executed in "postgres" db
    And the "test_recoverseg" table row count in "postgres" is saved
    And all files in pg_xlog directory are deleted from data directory of preferred primary of content 0

    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 incremental
    And edit the input file to recover mirror with content 1 full inplace
    And edit the input file to recover mirror with content 2 to a new directory on remote host with mode 0000
    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should return a return code of 1
    And user can start transactions
    And check segment conf: postgresql.conf


    And check if incremental recovery failed for mirrors with content 0 for gprecoverseg
    And check if full recovery was successful for mirrors with content 1
    And check if full recovery failed for mirrors with content 2 for gprecoverseg
    And gprecoverseg should not print "Segments successfully recovered" to stdout
    And check if mirrors on content 0,1,2 are in their original configuration
    And the gp_configuration_history table should contain a backout entry for the primary segment for contents 2

    And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
    And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts
    And check segment conf: postgresql.conf

    And the mode of all the created data directories is changed to 0700
    And the cluster is recovered in full and rebalanced
    And check segment conf: postgresql.conf
    And the row count from table "test_recoverseg" in "postgres" is verified against the saved data

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg mixed recovery segments come up even if one pg_ctl_start fails
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And the information of contents 0,1,2 is saved
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions

    And sql "DROP TABLE if exists test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,10000) AS i" is executed in "postgres" db
    And the "test_recoverseg" table row count in "postgres" is saved

    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 to a new directory on remote host with mode 0755
    And edit the input file to recover mirror with content 1 full inplace
    And edit the input file to recover mirror with content 2 incremental

    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should return a return code of 1
    And user can start transactions
    And verify that mirror on content 0 is down
    And verify that mirror on content 1,2 is up

    And check if start failed for contents 0 during full recovery for gprecoverseg
    And check if full recovery was successful for mirrors with content 1
    And check if incremental recovery was successful for mirrors with content 2
    And check if mirrors on content 0 are moved to new location on input file
    And check if mirrors on content 1,2 are in their original configuration
    And gpAdminLogs directory has "pg_basebackup*" files on respective hosts only for content 0,1
    And gpAdminLogs directory has "pg_rewind*" files on respective hosts only for content 2
    And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
    And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts
    And verify there are no recovery backout files
    And the old data directories are cleaned up for content 0

    And the mode of all the created data directories is changed to 0700
    Then the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And all previous progress files are removed from gpAdminLogs directory on respective hosts only for content 0
    And user can start transactions
    And the segments are synchronized
    And the cluster is rebalanced
    And check segment conf: postgresql.conf
    And the row count from table "test_recoverseg" in "postgres" is verified against the saved data

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg mixed recovery segments come up even if all pg_ctl_start fails
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And the information of contents 0,1,2 is saved
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions

    And sql "DROP TABLE if exists test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,10000) AS i" is executed in "postgres" db
    And the "test_recoverseg" table row count in "postgres" is saved

    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 to a new directory on remote host with mode 0755
    And edit the input file to recover mirror with content 1 to a new directory on remote host with mode 0755
    And edit the input file to recover mirror with content 2 to a new directory on remote host with mode 0755

    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should return a return code of 1
    And user can start transactions

    And check if start failed for contents 0 during full recovery for gprecoverseg
    Then gprecoverseg should print "pg_basebackup: base backup completed" to stdout for mirrors with content 0,1,2
    And gprecoverseg should print "Initiating segment recovery." to stdout
    And verify that mirror on content 0,1,2 is down

    And check if mirrors on content 0,1,2 are moved to new location on input file
    And gpAdminLogs directory has "pg_basebackup*" files on all segment hosts
    And gpAdminLogs directory has "gpsegsetuprecovery*" files on all segment hosts
    And gpAdminLogs directory has "gpsegrecovery*" files on all segment hosts
    And verify there are no recovery backout files
    And the old data directories are cleaned up for content 0,1,2
    And the mode of all the created data directories is changed to 0700
    Then the user runs "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
    And user can start transactions
    And the segments are synchronized
    And the cluster is rebalanced
    And check segment conf: postgresql.conf
    And the row count from table "test_recoverseg" in "postgres" is verified against the saved data

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg differential recovery gives warning if any of the failed segment's source is in backup already
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user runs sql "select pg_start_backup('test')" in "postgres" on primary segment with content 0
    When the user runs "gprecoverseg -a --differential"
    Then gprecoverseg should return a return code of 0
    And the user waits until mirror on content 1,2 is up
    And verify that mirror on content 0 is down
    Then gprecoverseg should print "Found differential recovery running for segments with contentIds [0], skipping recovery of these segments" to logfile
    And the user runs sql "select pg_stop_backup()" in "postgres" on primary segment with content 0
    When the user runs "gprecoverseg -av --differential"
    Then gprecoverseg should return a return code of 0
    And the user waits until mirror on content 0,1,2 is up
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg differential recovery gives warning if some of the failed segment's source is in backup already
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user runs sql "select pg_start_backup('test')" in "postgres" on primary segment with content 0,1
    When the user runs "gprecoverseg -a --differential"
    Then gprecoverseg should return a return code of 0
    And the user waits until mirror on content 2 is up
    And verify that mirror on content 0,1 is down
    Then gprecoverseg should print "Found differential recovery running for segments with contentIds [0, 1], skipping recovery of these segments" to logfile
    And the user runs sql "select pg_stop_backup()" in "postgres" on primary segment with content 0,1
    When the user runs "gprecoverseg -av --differential"
    Then gprecoverseg should return a return code of 0
    And the user waits until mirror on content 0,1,2 is up
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg differential recovery gives warning if all of the failed segment's source is in backup already
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0,1,2
    And user can start transactions
    And the user runs sql "select pg_start_backup('test')" in "postgres" on primary segment with content 0,1,2
    When the user runs "gprecoverseg -a --differential"
    Then gprecoverseg should return a return code of 0
    And verify that mirror on content 0,1,2 is down
    Then gprecoverseg should print "Found differential recovery running for segments with contentIds [0, 1, 2], skipping recovery of these segments" to logfile
    And the user runs sql "select pg_stop_backup()" in "postgres" on primary segment with content 0,1,2
    When the user runs "gprecoverseg -av --differential"
    Then gprecoverseg should return a return code of 0
    And the user waits until mirror on content 0,1,2 is up
    And the cluster is rebalanced

    @concourse_cluster
    Scenario: gprecoverseg behave test requires a cluster with at least 2 hosts
        Given the database is running
        Given database "gptest" exists
        And the information of a "mirror" segment on a remote host is saved

    @concourse_cluster
    Scenario Outline: When gprecoverseg <scenario> recovery is executed and an existing postmaster.pid on the killed primary segment corresponds to a non postgres process
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the "primary" segment information is saved
        When the postmaster.pid file on "primary" segment is saved
        And user stops all primary processes
        When user can start transactions
        And the background pid is killed on "primary" segment
        And we run a sample background script to generate a pid on "primary" segment
        And we generate the postmaster.pid file with the background pid on "primary" segment
        And the user runs "gprecoverseg <args>"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
        And gprecoverseg should print "Skipping to stop segment.* on host.* since it is not a postgres process" to stdout
        And all the segments are running
        And the segments are synchronized
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print "Unhandled exception in thread started by <bound method Worker.__bootstrap" to stdout
        And the segments are synchronized
        And the backup pid file is deleted on "primary" segment
        And the background pid is killed on "primary" segment

      Examples:
        | scenario     | args               |
        | differential | -a --differential  |
        | full         | -aF                |


    @concourse_cluster
    Scenario: gprecoverseg full recovery testing
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the information of a "mirror" segment on a remote host is saved
        When user kills a "mirror" process with the saved information
        And user can start transactions
        Then the saved "mirror" segment is marked down in config
        When the user runs "gprecoverseg -F -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Initiating segment recovery. Upon completion, will start the successfully recovered segments" to stdout
        And gprecoverseg should print "pg_basebackup: base backup completed" to stdout
        And gprecoverseg should print "Segments successfully recovered" to stdout
        And all the segments are running
        And the segments are synchronized
        And check segment conf: postgresql.conf

    @concourse_cluster
    Scenario: gprecoverseg with -i and -o option
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the information of a "mirror" segment on a remote host is saved
        When user kills a "mirror" process with the saved information
        And user can start transactions
        Then the saved "mirror" segment is marked down in config
        When the user runs "gprecoverseg -o failedSegmentFile"
        Then gprecoverseg should return a return code of 0
        Then gprecoverseg should print "Configuration file output to failedSegmentFile successfully" to stdout
        When the user runs "gprecoverseg -i failedSegmentFile -a"
        Then gprecoverseg should return a return code of 0
        Then gprecoverseg should print "1 segment\(s\) to recover" to stdout
        And all the segments are running
        And the segments are synchronized

    @concourse_cluster
    Scenario: gprecoverseg should not throw exception for empty input file
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the information of a "mirror" segment on a remote host is saved
        When user kills a "mirror" process with the saved information
        And user can start transactions
        Then the saved "mirror" segment is marked down in config
        When the user runs command "touch /tmp/empty_file"
        When the user runs "gprecoverseg -i /tmp/empty_file -a"
        Then gprecoverseg should return a return code of 0
        Then gprecoverseg should print "No segments to recover" to stdout
        When the user runs "gprecoverseg -a -F"
        Then all the segments are running
        And the segments are synchronized

    @concourse_cluster
    Scenario: gprecoverseg should use the same setting for data_checksums for a full recovery
        Given the database is running
        And results of the sql "show data_checksums" db "template1" are stored in the context
        # cause a full recovery AFTER a failure on a remote primary
        And all the segments are running
        And the segments are synchronized
        And the information of a "mirror" segment on a remote host is saved
        And the information of the corresponding primary segment on a remote host is saved
        When user kills a "primary" process with the saved information
        And user can start transactions
        Then the saved "primary" segment is marked down in config
        When the user runs "gprecoverseg -F -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Heap checksum setting is consistent between master and the segments that are candidates for recoverseg" to stdout
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Heap checksum setting is consistent between master and the segments that are candidates for recoverseg" to stdout
        And all the segments are running
        And the segments are synchronized
        # validate the new segment has the correct setting by getting admin connection to that segment
        Then the saved primary segment reports the same value for sql "show data_checksums" db "template1" as was saved

  @concourse_cluster
  Scenario: gprecoverseg should use the same setting for data_checksums for a differential recovery
        Given the database is running
        And results of the sql "show data_checksums" db "template1" are stored in the context
        And all the segments are running
        And the segments are synchronized
        And the information of a "mirror" segment on a remote host is saved
        And the information of the corresponding primary segment on a remote host is saved
        When user kills a "primary" process with the saved information
        And user can start transactions
        Then the saved "primary" segment is marked down in config
        When the user runs "gprecoverseg --differential -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Heap checksum setting is consistent between master and the segments that are candidates for recoverseg" to stdout
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should print "Heap checksum setting is consistent between master and the segments that are candidates for recoverseg" to stdout
        And all the segments are running
        And the segments are synchronized
        And verify replication slot internal_wal_replication_slot is available on all the segments
        Then the saved primary segment reports the same value for sql "show data_checksums" db "template1" as was saved


  @concourse_cluster
    Scenario: moving mirror to a different host must work
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "mirror" segment on a remote host is saved
          And the information of the corresponding primary segment on a remote host is saved
         When user kills a "mirror" process with the saved information
          And user can start transactions
         Then the saved "mirror" segment is marked down in config
         When the user runs "gprecoverseg -a -p cdw"
         Then gprecoverseg should return a return code of 0
         When user kills a "primary" process with the saved information
          And user can start transactions
         Then the saved "primary" segment is marked down in config
         When the user runs "gprecoverseg -a"
         Then gprecoverseg should return a return code of 0
          And all the segments are running
          And the segments are synchronized
         When the user runs "gprecoverseg -ra"
         Then gprecoverseg should return a return code of 0
          And all the segments are running
          And the segments are synchronized
          And check segment conf: postgresql.conf




    @concourse_cluster
    Scenario: gprecoverseg does not create backout scripts if a segment recovery fails before the catalog is changed
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
         When user kills a "primary" process with the saved information
          And user can start transactions
         Then the saved "primary" segment is marked down in config

         When all files in gpAdminLogs directory are deleted on all hosts in the cluster
          And the user asynchronously sets up to end gprecoverseg process when "Recovery type" is printed in the logs
          And the user runs "gprecoverseg -a"
         Then gprecoverseg should return a return code of -15
          And the gprecoverseg lock directory is removed

         When the user runs "gprecoverseg -a"
         Then gprecoverseg should return a return code of 0
         When the user runs "gprecoverseg -r -a"
         Then gprecoverseg should return a return code of 0
          And all the segments are running
          And the segments are synchronized
          And check segment conf: postgresql.conf


    @concourse_cluster
      #TODO do we need to add a test for old way of testing this scenario(by killing gprecoverseg)
    Scenario: gprecoverseg can revert catalog changes after a failed segment recovery
      Given the database is running
      And all the segments are running
      And the segments are synchronized
      And the information of contents 0,1,2 is saved
      And all files in gpAdminLogs directory are deleted on hosts cdw,sdw1,sdw2
      And the "primary" segment information is saved

      And the primary on content 0 is stopped
      And user can start transactions
      And the status of the primary on content 0 should be "d"
      And user can start transactions

      And a gprecoverseg directory under '/tmp' with mode '0700' is created
      And a gprecoverseg input file is created
      And edit the input file to recover mirror with content 0 to a new directory on remote host with mode 0000
      When the user runs gprecoverseg with input file and additional args "-a"
      Then gprecoverseg should return a return code of 1
      And check if full recovery failed for mirrors with content 0 for gprecoverseg

      Then the contents 0,1,2 should have their original data directory in the system configuration
      And the gp_configuration_history table should contain a backout entry for the primary segment for contents 0
      And verify that mirror on content 0 is down

      And the mode of all the created data directories is changed to 0700
      When the user runs gprecoverseg with input file and additional args "-a"
      And gprecoverseg should return a return code of 0
      And user can start transactions
      And all the segments are running
      And the segments are synchronized
      Then the cluster is rebalanced
      And check segment conf: postgresql.conf

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg can revert catalog changes even if all segments failed during recovery
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And the information of contents 0,1,2 is saved
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And the "primary" segment information is saved

    And the primary on content 0 is stopped
    And user can start transactions
    And the primary on content 1 is stopped
    And user can start transactions
    And the primary on content 2 is stopped
    And user can start transactions
    And an FTS probe is triggered
    And the status of the primary on content 0 should be "d"
    And the status of the primary on content 1 should be "d"
    And the status of the primary on content 2 should be "d"

    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 to a new directory on remote host with mode 0000
    And edit the input file to recover mirror with content 1 to a new directory on remote host with mode 0000
    And edit the input file to recover mirror with content 2 to a new directory on remote host with mode 0000
    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should return a return code of 1
    And user can start transactions
    And check if full recovery failed for mirrors with content 0,1,2 for gprecoverseg
    And verify that mirror on content 0,1,2 is down

    Then check if mirrors on content 0,1,2 are in their original configuration
    And the gp_configuration_history table should contain a backout entry for the primary segment for contents 0,1,2

    And the mode of all the created data directories is changed to 0700
    When the user runs gprecoverseg with input file and additional args "-a"
    Then gprecoverseg should return a return code of 0
    And check if mirrors on content 0,1,2 are moved to new location on input file
    And user can start transactions
    And all the segments are running
    And the segments are synchronized
    And the cluster is rebalanced
    And check segment conf: postgresql.conf

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg can revert catalog changes even if some segments failed during recovery
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And the information of contents 0,1,2 is saved
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster

    And the primary on content 0 is stopped
    And user can start transactions
    And the primary on content 1 is stopped
    And user can start transactions
    And the primary on content 2 is stopped
    And user can start transactions
    And the status of the primary on content 0 should be "d"
    And the status of the primary on content 1 should be "d"
    And the status of the primary on content 2 should be "d"
    And user can start transactions

    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the input file to recover mirror with content 0 to a new directory on remote host with mode 0000
    And edit the input file to recover mirror with content 1 to a new directory on remote host with mode 0000
    And edit the input file to recover mirror with content 2 to a new directory on remote host with mode 0700
    When the user runs gprecoverseg with input file and additional args "-av"
    Then gprecoverseg should return a return code of 1
    And check if full recovery failed for mirrors with content 0,1 for gprecoverseg
    And verify there are no recovery backout files
    And gprecoverseg should print "Some mirrors failed during basebackup. Reverting the gp_segment_configuration updates for these mirrors" to stdout
    And gprecoverseg should print "Successfully reverted the gp_segment_configuration updates for the failed mirrors" to stdout

    And verify that mirror on content 0,1 is down
    And verify that mirror on content 2 is up
    And check if mirrors on content 0,1 are in their original configuration
    And check if mirrors on content 2 are moved to new location on input file
    And the old data directories are cleaned up for content 2
    And the gp_configuration_history table should contain a backout entry for the primary segment for contents 0,1

    And the mode of all the created data directories is changed to 0700
    When the user runs "gprecoverseg -a"
    Then gprecoverseg should return a return code of 0
    And user can start transactions
    And all the segments are running
    And the segments are synchronized
    And the cluster is rebalanced
    And check segment conf: postgresql.conf

    @concourse_cluster
    Scenario: gprecoverseg cleans up backout scripts upon successful segment recovery
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
          And the gprecoverseg input file "newDirectoryFile" is cleaned up
         When user kills a "primary" process with the saved information
          And user can start transactions
         Then the saved "primary" segment is marked down in config

         When a gprecoverseg input file "newDirectoryFile" is created with a different data directory for content 0
          And the user runs "gprecoverseg -i /tmp/newDirectoryFile -a -v"
         Then gprecoverseg should return a return code of 0
         Then gprecoverseg should print "Recovery Target instance directory   = /tmp/newdir" to stdout
          And the user runs "gprecoverseg -r -a"
         Then gprecoverseg should return a return code of 0
          And all the segments are running
          And the segments are synchronized



  @concourse_cluster
    Scenario: gprecoverseg recovery to new host populates hostname and address from the config file correctly
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
          And the gprecoverseg input file "recover-config.conf" is cleaned up
         When user kills a "primary" process with the saved information
          And user can start transactions
         Then the saved "primary" segment is marked down in config
          When a gprecoverseg input file "recover-config.conf" is created with added parameter hostname to recover the failed segment on new host
          And the user runs "gprecoverseg -i /tmp/recover-config.conf -a -v"
         Then gprecoverseg should return a return code of 0
         When check hostname and address updated on segment configuration with the saved information
          And all the segments are running
          And the segments are synchronized

    @concourse_cluster
    Scenario: gprecoverseg recovery to same host (full inplace) populates hostname and address from the config file correctly
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
          And the gprecoverseg input file "recover-config.conf" is cleaned up
          When user kills a "primary" process with the saved information
          And user can start transactions
          Then the saved "primary" segment is marked down in config
          When a gprecoverseg input file "recover-config.conf" is created with hostname parameter to recover the failed segment on same host
          And the user runs "gprecoverseg -i /tmp/recover-config.conf -a -v"
          Then gprecoverseg should return a return code of 0
          When check hostname and address updated on segment configuration with the saved information
          And all the segments are running
          And the segments are synchronized


  @concourse_cluster
    Scenario: gprecoverseg recovery with invalid format with hostname in config file
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
          And the gprecoverseg input file "recover-config-invalid.conf" is cleaned up
          When user kills a "primary" process with the saved information
          And user can start transactions
          Then the saved "primary" segment is marked down in config
          When a gprecoverseg input file "recover-config-invalid.conf" is created with invalid format for inplace full recovery of failed segment
          And the user runs "gprecoverseg -i /tmp/recover-config-invalid.conf -a -v"
          Then gprecoverseg should return a return code of 2
          And gprecoverseg should print "line 1 of file /tmp/recover-config-invalid.conf: expected equal parts, either 3 or 4 on both segment group, obtained 4 on group1 and 3 on group2" to stdout
          Then the user runs "gprecoverseg -a"
          Then gprecoverseg should return a return code of 0
          And the cluster is rebalanced


  @concourse_cluster
    Scenario: gprecoverseg incremental recovery populates hostname and address from the config file correctly
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
          And the gprecoverseg input file "recover-config.conf" is cleaned up
          When user kills a "primary" process with the saved information
          And user can start transactions
          Then the saved "primary" segment is marked down in config
          When a gprecoverseg input file "recover-config.conf" is created with hostname parameter matches with segment configuration table for incremental recovery of failed segment
          And the user runs "gprecoverseg -i /tmp/recover-config.conf -a -v"
          Then gprecoverseg should return a return code of 0
          When check hostname and address updated on segment configuration with the saved information
          And all the segments are running
          And the segments are synchronized

    @concourse_cluster
    Scenario: gprecoverseg recovery with and without hostname parameter in config file
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And user stops all primary processes
          And user can start transactions
          And the gprecoverseg input file "recover-config.conf" is cleaned up
          When a gprecoverseg input file "recover-config.conf" is created with and without parameter hostname to recover all the failed segments
          And the user runs "gprecoverseg -i /tmp/recover-config.conf -a -F -v"
          Then gprecoverseg should return a return code of 0
          And all the segments are running
          And the segments are synchronized

    @concourse_cluster
    Scenario: gprecoverseg throws warning and skips recovery if provided hostname and address can not be resolved to same host
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
          And user stops all primary processes
          And user can start transactions
          And the gprecoverseg input file "recover-config.conf" is cleaned up
          When a gprecoverseg input file "recover-config.conf" created with invalid failover hostname for full recovery of failed segment
          And the user runs "gprecoverseg -i /tmp/recover-config.conf -a -F -v"
          Then gprecoverseg should return a return code of 0
          And gprecoverseg should print a "Not able to co-relate hostname:.* with address.*Skipping recovery for segments with contentId" warning
          Then the user runs "gprecoverseg -a"
          Then gprecoverseg should return a return code of 0
          And all the segments are running
          And the segments are synchronized

    @concourse_cluster
    Scenario: gprecoverseg incremental recovery with config file and wrong hostname
        Given the database is running
          And all the segments are running
          And the segments are synchronized
          And the information of a "primary" segment on a remote host is saved
          And the gprecoverseg input file "recover-config.conf" is cleaned up
          When user kills a "primary" process with the saved information
          And user can start transactions
          Then the saved "primary" segment is marked down in config
          When a gprecoverseg input file "recover-config.conf" is created with invalid hostname parameter that does not matches with the segment configuration table hostname
          And the user runs "gprecoverseg -i /tmp/recover-config.conf -a -v"
          Then gprecoverseg should return a return code of 2
          And gprecoverseg should print "A segment to recover was not found in configuration.  This segment is described by hostname|address|port|directory .*'" to stdout
          Then the user runs "gprecoverseg -a"
          And gprecoverseg should return a return code of 0
          And the cluster is rebalanced

  @demo_cluster
  Scenario: gprecoverseg recovers segment when config file contains hostname on demo cluster
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0
    And user can start transactions
    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the hostsname input file to recover segment with content 0 full inplace
    And update /etc/hosts file with address for the localhost
    When the user runs gprecoverseg with input file and additional args "-a"
    And gprecoverseg should return a return code of 0
    And restore /etc/hosts file and cleanup hostlist file
    And the cluster configuration has no segments where "content=0 and status='d'"
    Then the cluster is rebalanced

  @demo_cluster
  Scenario: gprecoverseg skips recovery when config file contains invalid hostname on demo cluster
    Given the database is running
    And all the segments are running
    And the segments are synchronized
    And all files in gpAdminLogs directory are deleted on all hosts in the cluster
    And user immediately stops all primary processes for content 0
    And user can start transactions
    And a gprecoverseg directory under '/tmp' with mode '0700' is created
    And a gprecoverseg input file is created
    And edit the hostsname input file to recover segment with content 0 with invalid hostname
    When the user runs gprecoverseg with input file and additional args "-a"
    And gprecoverseg should print a "Could not resolve hostname:invalid_host" warning
    And gprecoverseg should print a "Not able to co-relate hostname:invalid_host with address:.*Skipping recovery for segments with contentId" warning
    And gprecoverseg should print "No segments to recover" to stdout
    And gprecoverseg should return a return code of 0
    And the user runs "gprecoverseg -a -v"
    Then gprecoverseg should return a return code of 0
    And the cluster is rebalanced

  @demo_cluster
  @concourse_cluster
  Scenario: gprecoverseg rebalance aborts and throws exception if replay lag on mirror is more than or equal to the allowed limit
      Given the database is running
        And all the segments are running
        And the segments are synchronized
        And all files in gpAdminLogs directory are deleted on all hosts in the cluster
        And user immediately stops all primary processes for content 0
        And user can start transactions
       When the user runs "gprecoverseg -av --replay-lag 10"
       Then gprecoverseg should return a return code of 2
        And gprecoverseg should print "--replay-lag should be used only with -r" to stdout
        When the user runs "gprecoverseg -av"
        Then gprecoverseg should return a return code of 0
        When the user runs "gprecoverseg -ar --replay-lag 0"
        Then gprecoverseg should return a return code of 2
         And gprecoverseg should print "Allowed replay lag during rebalance is 0.0 GB" to stdout
         And gprecoverseg should print ".* bytes of xlog is still to be replayed on mirror with dbid.*, let mirror catchup on replay then trigger rebalance" regex to logfile
        When the user runs "gprecoverseg -ar"
        Then gprecoverseg should return a return code of 0
         And all the segments are running
         And user can start transactions


    @remove_rsync_bash
    @concourse_cluster
    Scenario: None of the accumulated wal (after running pg_start_backup and before copying the pg_control file) is lost during differential
      Given the database is running
        And all the segments are running
        And the segments are synchronized
        And all files in gpAdminLogs directory are deleted on all hosts in the cluster
        And sql "DROP TABLE IF EXISTS test_recoverseg; CREATE TABLE test_recoverseg AS SELECT generate_series(1,1000) AS a;" is executed in "postgres" db
        And user immediately stops all mirror processes for content 0
        And the user waits until mirror on content 0 is down
        And user can start transactions
        And user creates a new executable rsync script which inserts data into table and runs checkpoint along with doing rsync
       When the user runs "gprecoverseg -av --differential"
       Then gprecoverseg should return a return code of 0
        And verify that mirror on content 0 is up
       Then the row count of table test_recoverseg in "postgres" should be 2000
      Given user immediately stops all primary processes for content 0
        And user can start transactions
       Then the row count of table test_recoverseg in "postgres" should be 2000
       And the cluster is recovered in full and rebalanced


  @demo_cluster
  @concourse_cluster
  Scenario: Cleanup orphaned directory of dropped database after differential recovery
      Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the user runs psql with "-c 'CREATE DATABASE test_orphan_dir'" against database "template1"
        And save the information of the database "test_orphan_dir"
        And the "primary" segment information is saved
        And the primary on content 0 is stopped
        And user can start transactions
        And the user runs psql with "-c 'DROP DATABASE test_orphan_dir'" against database "template1"
       When the user runs "gprecoverseg -a --differential"
       Then gprecoverseg should return a return code of 0
        And the user runs psql with "-c 'SELECT gp_request_fts_probe_scan()'" against database "template1"
        And the status of the primary on content 0 should be "u"
       Then verify deletion of orphaned directory of the dropped database
        And the cluster is rebalanced

