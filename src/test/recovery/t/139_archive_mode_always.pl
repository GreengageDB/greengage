# test wal persistance for archive_mode 'always'
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;
use File::Copy;

# The idea of the test is to check that not archived WAL files are not removed
# by creation of checkpoint (on master) or restartpoint (on standby).

# Initialize master node, doing archives
my $node_master = get_new_node('master');
$node_master->init(
	has_archiving    => 1,
	allows_streaming => 1);
my $backup_name = 'my_backup';

# Append necessary config. Some of params override already initialized ones.
# archive_mode 'always' is a subject of the test.
# archive_command 'false' is used to prevent archiving. The idea is to check
#   WAL file is not removed by checkpointer. Successfull archiving (with file
#   removing) should not affect the test.
# wal_keep_segments '0' is used to prevent keeping old WAL files. As test
#   checks that WAL file still exist, it should not be kept by any other
#   mechanics.
# create_restartpoint_on_ckpt_record_replay 'on' makes restartpoint (and
#   waiting for completion) while replaying checkpoint record on standby.
$node_master->append_conf(
		'postgresql.conf', qq{
archive_mode = 'always'
archive_command = '/usr/bin/false'
wal_keep_segments = 0
create_restartpoint_on_ckpt_record_replay = 'on'
});

# Start it
$node_master->start;

# Take backup for standby
$node_master->backup($backup_name);

# Initialize standby node from backup, fetching WAL from archives
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby->start;

# Create some content on master
$node_master->safe_psql('postgres',
	"CREATE TABLE t AS SELECT generate_series(1,1000) i;");

# Find the next WAL segment to be archived
my $walfile_to_be_kept = $node_master->safe_psql('postgres',
	"SELECT pg_xlogfile_name(pg_current_xlog_location());");

# Switch WAL and do checkpoint. Before version 11, Postgres needs files for
# two points, so do checkpoint twice (see CreateCheckPoint() for more).
# Checkpoint is requested with CHECKPOINT_WAIT, so it's enough to wait for
# command end.
$node_master->safe_psql('postgres', "
SELECT pg_switch_xlog();
CHECKPOINT;
CHECKPOINT;
");

my $current_lsn =
  $node_master->safe_psql('postgres', "SELECT pg_current_xlog_location();");

# Wait until necessary replay has been done on standby
my $caughtup_query =
  "SELECT '$current_lsn'::pg_lsn <= pg_last_xlog_replay_location()";
$node_standby->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for standby to catch up";

my $wal_filepath_on_master = $node_master->data_dir . '/pg_xlog/' . $walfile_to_be_kept;
my $wal_filepath_on_standby = $node_standby->data_dir . '/pg_xlog/' . $walfile_to_be_kept;

# Old wal file should exist on both, master and standby
ok(-f "$wal_filepath_on_master", 'latest wal file from the old timeline exists on master');
ok(-f "$wal_filepath_on_standby", 'latest wal file from the old timeline exists on standby');
