use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More tests => 3;

use FindBin;
use lib $FindBin::RealBin;
use File::Copy;

use RewindTest;

sub run_test
{
	my $test_mode     = shift;

	RewindTest::setup_cluster($test_mode);
	RewindTest::start_primary();
	RewindTest::create_standby($test_mode);
	RewindTest::promote_standby(1);

	my $master_pgdata = $node_primary->data_dir();
	my $tmp_folder    = PostgreSQL::Test::Utils::tempdir;

	# pg_rewind should handle empty (or even removed) postgres.conf
	# gp_dbid is taken from internal.auto.conf
	copy(
		"$master_pgdata/postgresql.conf",
		"$tmp_folder/master-postgresql-full.conf.tmp");

	open my $file, '>', "$master_pgdata/postgresql.conf";

	RewindTest::run_pg_rewind($test_mode, do_not_start_primary => 1);

	# Append new file to the old one to preserve primary_conninfo
	my $new_file_data = slurp_file("$master_pgdata/postgresql.conf");
	append_to_file("$tmp_folder/master-postgresql-full.conf.tmp", $new_file_data);

	copy(
		"$tmp_folder/master-postgresql-full.conf.tmp",
		"$master_pgdata/postgresql.conf");

	$node_primary->start;
	RewindTest::promote_primary();

	RewindTest::clean_rewind_test();
}

# Run the test in both modes
run_test('local');
run_test('remote');

exit(0);
