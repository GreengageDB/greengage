
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Test that recovery server follows the right path through the WAL
# files towards the recovery target timeline. Consider the following
# timeline history:
#
#    ---S---------------------->P   TLI 1
#                     \
#                      -------->    TLI 2
#
# Standby has replayed the WALL on TLI up to point S, and is currently
# stopped. However, it already has all the WAL from TLI 1 locally in
# pg_wal, because it was restored from the archive or streamed from the
# primary on TLI 1 earlier. If you now set recovery target timeline to
# TLI 2, the standby must not replay the WAL it already has on TLI 1,
# beyond the point where they diverged.
#
# This test sets up that scenario, by pausing the replay on the
# standby.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use File::Copy;

my $primary1 = PostgreSQL::Test::Cluster->new('primary1');
$primary1->init(allows_streaming => 1);
$primary1->start;
$primary1->backup("bkp");

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary1, "bkp", has_streaming => 1);
$standby->start;

# Pause the standby
# After bump to PostgreSQL 14 or newest we should add check by executing
# pg_get_wal_replay_pause_state(), because now there is guarantee, that recovery
# is actually paused, but after bump to PostgreSQL 14 we should check state.
$standby->safe_psql('postgres', "SELECT pg_wal_replay_pause()");

# Make some changes in the primary. They will be streamed to the
# standby, but not yet applied.
$primary1->safe_psql('postgres', 'CREATE TABLE test_tab1 (t TEXT)');
$primary1->safe_psql('postgres', 'CREATE TABLE test_tab2 (t TEXT)');
$primary1->safe_psql('postgres',
	"INSERT INTO test_tab1 VALUES ('both timelines')");
$primary1->safe_psql('postgres',
	"INSERT INTO test_tab2 VALUES ('both timelines')");

# Create timeline 2 at this point
my $primary2 = PostgreSQL::Test::Cluster->new('primary2');
$primary2->init_from_backup($primary1, "bkp", has_streaming => 1);
$primary2->start;
$primary1->wait_for_catchup($primary2, 'replay', $primary1->lsn('flush'));
$primary2->promote;

# Make some changes on both timelines
$primary1->safe_psql('postgres',
	"INSERT INTO test_tab1 VALUES ('TLI 1 only')");

$primary2->safe_psql('postgres',
	"INSERT INTO test_tab2 VALUES ('TLI 2 only')");

# Stop the standby, and re-point it to TLI 2 (primary2).
$standby->stop;
my $primary2_connstr = $primary2->connstr;
$standby->append_conf('postgresql.conf',
	"primary_conninfo = '$primary2_connstr'");
$standby->append_conf('postgresql.conf',
	"recovery_target_timeline = 2");

# Copy the history file. Otherwise, the standby will just error out
# with "FATAL: recovery target timeline 2 does not exist"
#
# XXX: Perhaps it should try to connect to the primary and fetch the
# history file from there instead of erroring out. But it doesn't do
# that today.
copy($primary2->data_dir . '/pg_wal/00000002.history',
	$standby->data_dir . '/pg_wal/00000002.history')
  or BAIL_OUT("could not copy 00000002.history");

# Check that it recovers correctly to TLI 2 when started up.
$standby->start;
$primary2->wait_for_catchup($standby, 'replay', $primary2->lsn('flush'));

my $result =
  $standby->safe_psql('postgres', "SELECT * FROM test_tab1");
is( $result, qq(both timelines),
		"changes on TLI 1 are *not* on standby");

$result =
  $standby->safe_psql('postgres', "SELECT * FROM test_tab2");
is( $result, qq(both timelines
TLI 2 only),
		"all changes on TLI 2 are on the standby");

done_testing();
