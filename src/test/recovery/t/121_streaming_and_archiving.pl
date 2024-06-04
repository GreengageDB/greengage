use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 17;
use File::Copy;

my $node_master;
my $node_standby;

##################### Test with wal_sender_archiving_status_interval disabled #####################

# Initialize master node with WAL archiving setup and wal_sender_archiving_status_interval as disabled
$node_master = get_new_node('master');
$node_master->init(
    has_archiving    => 1,
    allows_streaming => 1);

$node_master->append_conf('postgresql.conf', 'wal_sender_archiving_status_interval = 0');
$node_master->start;
my $master_data = $node_master->data_dir;

# Set an incorrect archive_command so that archiving fails
my $incorrect_command = "exit 1";
$node_master->safe_psql(
'postgres', qq{
ALTER SYSTEM SET archive_command TO '$incorrect_command';
SELECT pg_reload_conf();
});

# Take backup
$node_master->backup('my_backup');

# Initialize the standby node. It will inherit wal_sender_archiving_status_interval from the master
$node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, 'my_backup', has_streaming => 1);
my $standby_data = $node_standby->data_dir;
$node_standby->start;

$node_master->safe_psql(
	'postgres', q{
	CREATE TABLE test1 AS SELECT generate_series(1,10) AS x;
});

my $current_walfile = $node_master->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_current_xlog_location());");
$node_master->safe_psql(
	'postgres', q{
	SELECT pg_switch_xlog();
	CHECKPOINT;
});

# After switching wal, the current wal file will be marked as ready to be archived on the master. But this wal file
# won't get archived because of the incorrect archive_command
my $walfile_ready = "pg_xlog/archive_status/$current_walfile.ready";
my $walfile_done = "pg_xlog/archive_status/$current_walfile.done";

# Wait for the standby to catch up
$node_master->wait_for_catchup($node_standby, 'write',
	$node_master->lsn('insert'));

# Wait for archive failure
$node_master->poll_query_until('postgres',
q{SELECT failed_count > 0 FROM pg_stat_archiver}, 't')
or die "Timed out while waiting for archiving to fail";

# Due to archival failure, check if master has only .ready wal file and not .done
# Check if standby has .done created as wal_sender_archiving_status_interval is disabled.
ok( -f "$master_data/$walfile_ready",
	".ready file exists on master for WAL segment $current_walfile"
);
ok( !-f "$master_data/$walfile_done",
	".done file does not exist on master for WAL segment $current_walfile"
);

ok( !-f "$standby_data/$walfile_ready",
	".ready file does not exist on standby for WAL segment $current_walfile when wal_sender_archiving_status_interval=0"
);
ok( -f "$standby_data/$walfile_done",
	".done file exists on standby for WAL segment $current_walfile when wal_sender_archiving_status_interval=0"
);

##################### Test with wal_sender_archiving_status_interval enabled #####################
$node_master->append_conf('postgresql.conf', 'wal_sender_archiving_status_interval = 50ms');
$node_master->reload;
# We need to enable wal_sender_archiving_status_interval on the standby as well. Technically the value doesn't matter
# since the wal_receiver process only cares about this guc being set and not the actual value.
$node_standby->append_conf('postgresql.conf', 'wal_sender_archiving_status_interval = 50ms');
$node_standby->reload;

$node_master->safe_psql(
	'postgres', q{
	CREATE TABLE test2 AS SELECT generate_series(1,10) AS x;
});

my $current_walfile2 = $node_master->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_current_xlog_location())");
my $walfile_ready2 = "pg_xlog/archive_status/$current_walfile2.ready";
my $walfile_done2 = "pg_xlog/archive_status/$current_walfile2.done";

$node_master->safe_psql(
	'postgres', q{
	SELECT pg_switch_xlog();
	CHECKPOINT;
});

# Wait for standby to catch up
$node_master->wait_for_catchup($node_standby, 'write',
	$node_master->lsn('insert'));

# Check if master and standby have created .ready file for the wal that failed to archive
ok( -f "$master_data/$walfile_ready2",
	".ready file exists on master for WAL segment $current_walfile2 when wal_sender_archiving_status_interval=50ms"
);
ok( -f "$standby_data/$walfile_ready2",
	".ready file exists on standby for WAL segment $current_walfile2 when wal_sender_archiving_status_interval=50ms"
);

ok( !-f "$master_data/$walfile_done2",
	".done file does not exist on master for WAL segment $current_walfile2 when wal_sender_archiving_status_interval=50ms"
);
ok( !-f "$standby_data/$walfile_done2",
	".done file does not exist on standby for WAL segment $current_walfile2 when wal_sender_archiving_status_interval=50ms"
);

# Make WAL archiving work again for master by resetting the archive_command
$node_master->safe_psql(
	'postgres', q{
	ALTER SYSTEM RESET archive_command;
	SELECT pg_reload_conf();
});

# Force the archiver process to wake up and start archiving
$node_master->safe_psql(
   'postgres', q{
   SELECT pg_switch_xlog();
});

# Check if master has .done file created for the archived segment and also that the file gets uploaded to the archive
wait_until_file_exists("$master_data/$walfile_done2", ".done file to exist on master for WAL segment $current_walfile2");

ok( !-f "$master_data/$walfile_ready2",
	".ready file does not exist for WAL segment $current_walfile2");

wait_until_file_exists($node_master->archive_dir . "/$current_walfile2",
	"$current_walfile2 to be archived by the master");

# Check if standby has .done file created for the archived segment
wait_until_file_exists("$standby_data/$walfile_done2",
	".done file to exist on standby for WAL segment $current_walfile2"
);
ok( !-f "$standby_data/$walfile_ready2",
	".ready file does not exist on standby for WAL segment $current_walfile2");

ok( !-f $node_standby->archive_dir . "/$current_walfile2",
	"$current_walfile2 does not exist in standby's archive");

################### Now again make archiving fail but this time promote the standby and let the standby archive the wal
$node_master->safe_psql(
	'postgres', qq{
	ALTER SYSTEM SET archive_command TO '$incorrect_command';
	SELECT pg_reload_conf();
});
# make archiving work for the standby node
$node_standby->safe_psql(
	'postgres', q{
	ALTER SYSTEM RESET archive_command;
	SELECT pg_reload_conf();
});

$node_master->safe_psql(
	'postgres', q{
	CREATE TABLE test3 AS SELECT generate_series(1,10) AS x;
});
my $current_walfile3 = $node_master->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_current_xlog_location())");
$node_master->safe_psql(
	'postgres', q{
	SELECT pg_switch_xlog();
	CHECKPOINT;
});
my $walfile_ready3 = "pg_xlog/archive_status/$current_walfile3.ready";
my $walfile_done3 = "pg_xlog/archive_status/$current_walfile3.done";

# Wait for the standby to catch up
$node_master->wait_for_catchup($node_standby, 'write',
	$node_master->lsn('insert'));

ok( -f "$standby_data/$walfile_ready3",
	".ready file exists on standby for WAL segment $current_walfile3 when wal_sender_archiving_status_interval=50ms"
);
ok( !-f "$standby_data/$walfile_done3",
	".done file does not exist on standby for WAL segment $current_walfile3 when wal_sender_archiving_status_interval=50ms"
);
ok( !-f $node_master->archive_dir . "/$current_walfile3",
	"$current_walfile3 does not exist in the archive");

# Now promote the standby
$node_standby->promote;

# Wait for promotion to complete
$node_standby->poll_query_until('postgres', "SELECT NOT pg_is_in_recovery();")
or die "Timed out while waiting for promotion";

# Wait until the wal file gets archived by the standby, Note that the archive dir is master's archive dir and not standby's.
# This is because archive_command (which has the cp command) gets copied over from the master node when we initialize the standby
wait_until_file_exists($node_master->archive_dir . "/$current_walfile3",
	"$current_walfile3 to be archived by the standby");

ok( -f "$master_data/$walfile_ready3",
	".ready file exists on master for WAL segment $current_walfile3 when wal_sender_archiving_status_interval=50ms"
);
# master won't be able to archive and hence won't have the .done file
ok( !-f "$master_data/$walfile_done3",
	".done file does not exist on master for WAL segment $current_walfile3 when wal_sender_archiving_status_interval=50ms"
);
wait_until_file_exists("$standby_data/$walfile_done3",
	".done file to exist on standby for WAL segment $current_walfile3 when wal_sender_archiving_status_interval=50ms"
);
ok( !-f "$standby_data/$walfile_ready3",
	".ready file does not exist on standby for WAL segment $current_walfile3 when wal_sender_archiving_status_interval=50ms"
);

done_testing();
