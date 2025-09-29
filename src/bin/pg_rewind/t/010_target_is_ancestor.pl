
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Test cases where the target is a direct ancestor of the source.
#
# We set up two timelines like this:
#
#            -------------- TLI 2 (source)
#           /
#  ---A----B-------C------- TLI 1
#
#
# On TLI 1, we take a backup on points A, B and C. We use a server
# restored from each of those points as the target, and a primary
# server running on TLI 2 as the source.
#
# - A is a direct ancestor of the source, so no rewind is required
# - B is also a direct ancestor of the source, at exactly the point
#     where the timelines diverged. No rewind is required.
# - C is not a direct ancestor, so rewind is required. (This case is
#   not the focus of these tests, it's here just for completeness.)

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use File::Copy;
use File::Path qw(rmtree);

my $tmp_folder = PostgreSQL::Test::Utils::tempdir;

my $orig_node = PostgreSQL::Test::Cluster->new('orig_node');
$orig_node->init(allows_streaming => 1);
$orig_node->append_conf(
	'postgresql.conf', qq(
wal_log_hints = on
));
$orig_node->start;
$orig_node->safe_psql('postgres', 'CREATE TABLE test_tab (t TEXT)');
$orig_node->safe_psql('postgres',
	"INSERT INTO test_tab VALUES ('at the beginning')");
$orig_node->stop;
$orig_node->backup_fs_cold('backup_A');

$orig_node->start;
$orig_node->safe_psql('postgres',
	"INSERT INTO test_tab VALUES ('after backup A')");
$orig_node->stop;
$orig_node->backup_fs_cold('backup_B');

# Restore backup B to create Timeline 2
my $node_2 = PostgreSQL::Test::Cluster->new('node_2');
$node_2->init_from_backup($orig_node, 'backup_B', has_streaming => 1);
$node_2->start;
$node_2->promote;
$node_2->safe_psql('postgres', "INSERT INTO test_tab VALUES ('in node')");

# Restore backup B in recovery mode to create Timeline 2
my $node_3 = PostgreSQL::Test::Cluster->new('node_3');
$node_3->init_from_backup($orig_node, 'backup_B', has_restoring => 1, standby => 0);
$node_3->start;
$node_3->safe_psql('postgres', "INSERT INTO test_tab VALUES ('in node')");

$orig_node->start;
$orig_node->safe_psql('postgres',
	"INSERT INTO test_tab VALUES ('this will be lost on rewind')");
$orig_node->stop;
$orig_node->backup_fs_cold('backup_C');

# Create a new node from a backup, and run pg_rewind to rewind it as a
# follower of $source_node
sub rewind_from_backup
{
	my ($backup, $expected_stderr, $source_node) = @_;

	my $source_pgdata = $source_node->data_dir;
	my $source_connstr = $source_node->connstr();

	my $target_node = PostgreSQL::Test::Cluster->new('restored_' . $backup . '_' . $source_node->name);
	$target_node->init_from_backup($orig_node, $backup, has_streaming => 1);
	my $target_pgdata = $target_node->data_dir;

	# Keep a temporary postgresql.conf or it would be overwritten during the rewind.
	copy("$target_pgdata/postgresql.conf", "$tmp_folder/postgresql.conf.tmp");

	command_checks_all(
		[
			'pg_rewind',
			"--debug",
			"--no-sync",
			"--source-server=$source_connstr",
			"--target-pgdata=$target_pgdata"
		],
		0,
		[],
		$expected_stderr,
		"pg_rewind on node restored from $backup");

	# Now move back postgresql.conf with old settings
	move("$tmp_folder/postgresql.conf.tmp", "$target_pgdata/postgresql.conf");

	# Make the more recent WAL from timeline 1 available to the
	# restored server.  It doesn't need it, the point of this is to
	# test that the server ignores this extra WAL.
	copy($orig_node->data_dir . "/pg_wal/000000010000000000000001",
	  $target_node->data_dir . "/pg_wal/000000010000000000000001")
	  || die "copying 000000010000000000000001: $!";

	# Configure it to connect to the new primary
	$target_node->append_conf(
		'postgresql.conf', qq(
primary_conninfo = '$source_connstr'
));
	$target_node->set_standby_mode();
	$target_node->start;

	$source_node->wait_for_catchup($target_node->name);
	my $result =
	  $target_node->safe_psql('postgres', "SELECT * FROM test_tab");
	is( $result, qq(at the beginning
after backup A
in node),
		"check query after rewind from $backup");
	$target_node->stop;
}

rewind_from_backup('backup_C',
	[qr/pg_rewind: rewinding from last common checkpoint at .* on timeline 1/],
	$node_2);
rewind_from_backup('backup_B', [qr/pg_rewind: no rewind required/], $node_2);
rewind_from_backup('backup_A', [qr/pg_rewind: no rewind required/], $node_2);

rewind_from_backup('backup_C',
	[qr/pg_rewind: rewinding from last common checkpoint at .* on timeline 1/],
	$node_3);
rewind_from_backup('backup_B', [qr/pg_rewind: no rewind required/], $node_3);
rewind_from_backup('backup_A', [qr/pg_rewind: no rewind required/], $node_3);

$node_2->stop;
$node_3->stop;

done_testing();
