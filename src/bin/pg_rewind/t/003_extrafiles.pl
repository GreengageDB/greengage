# Test how pg_rewind reacts to extra files and directories in the data dirs.

use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More;

use File::Find;

use FindBin;
use lib $FindBin::RealBin;

use RewindTest;


sub run_test
{
	my $test_mode = shift;

	RewindTest::setup_cluster($test_mode);
	RewindTest::start_primary();
	primary_psql("ALTER SYSTEM SET log_directory TO 'relative_log_dir'");
	primary_psql("SELECT pg_reload_conf()");

	my $test_primary_datadir = $node_primary->data_dir;

	# Create a subdir and files that will be present in both
	mkdir "$test_primary_datadir/tst_both_dir";
	append_to_file "$test_primary_datadir/tst_both_dir/both_file1", "in both1";
	append_to_file "$test_primary_datadir/tst_both_dir/both_file2", "in both2";
	mkdir "$test_primary_datadir/tst_both_dir/both_subdir/";
	append_to_file "$test_primary_datadir/tst_both_dir/both_subdir/both_file3",
	  "in both3";

	RewindTest::create_standby($test_mode);
	standby_psql("ALTER SYSTEM SET log_directory TO 'relative_log_dir'");
	standby_psql("SELECT pg_reload_conf()");

	# Create different subdirs and files in primary and standby
	my $test_standby_datadir = $node_standby->data_dir;

	mkdir "$test_standby_datadir/tst_standby_dir";
	append_to_file "$test_standby_datadir/tst_standby_dir/standby_file1",
	  "in standby1";
	append_to_file "$test_standby_datadir/tst_standby_dir/standby_file2",
	  "in standby2";
	mkdir "$test_standby_datadir/tst_standby_dir/standby_subdir/";
	append_to_file
	  "$test_standby_datadir/tst_standby_dir/standby_subdir/standby_file3",
	  "in standby3";

	mkdir "$test_primary_datadir/tst_primary_dir";
	append_to_file "$test_primary_datadir/tst_primary_dir/primary_file1",
	  "in primary1";
	append_to_file "$test_primary_datadir/tst_primary_dir/primary_file2",
	  "in primary2";
	mkdir "$test_primary_datadir/tst_primary_dir/primary_subdir/";
	append_to_file
	  "$test_primary_datadir/tst_primary_dir/primary_subdir/primary_file3",
	  "in primary3";

	# GPDB: gpbackup default directory should be ignored
	mkdir "$test_primary_datadir/backups";
	append_to_file "$test_primary_datadir/backups/primary_backup_file",
	  "backup data in primary1";

	# GPDB: default log directory 'log' should be ignored
	mkdir "$test_primary_datadir/log";
	append_to_file "$test_primary_datadir/log/primary_log_file.csv",
	  "random log in coordinator";

	my @list_of_expected_files = (
			"$test_primary_datadir/backups",
			"$test_primary_datadir/backups/primary_backup_file",
			"$test_primary_datadir/log",
			"$test_primary_datadir/log/primary_log_file.csv",
			"$test_primary_datadir/tst_both_dir",
			"$test_primary_datadir/tst_both_dir/both_file1",
			"$test_primary_datadir/tst_both_dir/both_file2",
			"$test_primary_datadir/tst_both_dir/both_subdir",
			"$test_primary_datadir/tst_both_dir/both_subdir/both_file3",
			"$test_primary_datadir/tst_standby_dir",
			"$test_primary_datadir/tst_standby_dir/standby_file1",
			"$test_primary_datadir/tst_standby_dir/standby_file2",
			"$test_primary_datadir/tst_standby_dir/standby_subdir",
			"$test_primary_datadir/tst_standby_dir/standby_subdir/standby_file3",
	);

	# GPDB: log directory files, based on the log_directory GUC of
	# --source-server (AKA standby) should be ignored
	if ($test_mode eq "remote")
	{
		mkdir "$test_standby_datadir/relative_log_dir";
		append_to_file "$test_standby_datadir/relative_log_dir/standby_relative_log_file.csv",
		  "random relative_log_dir in standby";
		# we don't include the csv file as it should be ignored by pg_rewind
		push(@list_of_expected_files, "$test_primary_datadir/relative_log_dir");
	}

	RewindTest::promote_standby();
	RewindTest::run_pg_rewind($test_mode);

	# List files in the data directory after rewind.
	my @paths;
	find(
		sub {
			push @paths, $File::Find::name
			  if $File::Find::name =~ m/.*(\/log|\/relative_log_dir|tst_|backups).*/;
		},
		$test_primary_datadir);
	@paths = sort @paths;
	@list_of_expected_files = sort @list_of_expected_files;
	is_deeply(
		\@paths,
		\@list_of_expected_files,
		"file lists match");

	RewindTest::clean_rewind_test();
	return;
}

# Run the test in both modes.
run_test('local');
run_test('remote');

done_testing();
