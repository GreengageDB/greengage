use strict;
use warnings;
use File::Basename qw(basename dirname);
use File::Compare;
use File::Path qw(rmtree);

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

program_help_ok('pg_basebackup');
program_version_ok('pg_basebackup');
program_options_handling_ok('pg_basebackup');

my $tempdir = PostgreSQL::Test::Utils::tempdir;

my $node = PostgreSQL::Test::Cluster->new('main');

# Set umask so test directories and files are created with default permissions
umask(0077);

# Initialize node without replication settings
$node->init(extra => ['--data-checksums']);
$node->start;
my $pgdata = $node->data_dir;

$node->command_fails(['pg_basebackup', '--target-gp-dbid', '123'],
	'pg_basebackup needs target directory specified');

# Some Windows ANSI code pages may reject this filename, in which case we
# quietly proceed without this bit of test coverage.
if (open my $badchars, '>>', "$tempdir/pgdata/FOO\xe0\xe0\xe0BAR")
{
	print $badchars "test backup of file with non-UTF8 name\n";
	close $badchars;
}

$node->set_replication_conf();
$node->reload;

command_fails(['pg_basebackup', '-D', "$tempdir/backup" ],
	'pg_basebackup fails without specifiying the target greenplum db id');

$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123' ],
	'pg_basebackup fails because of WAL configuration');

ok(!-d "$tempdir/backup", 'backup directory was cleaned up');

# Create a backup directory that is not empty so the next command will fail
# but leave the data directory behind
mkdir("$tempdir/backup")
  or BAIL_OUT("unable to create $tempdir/backup");
append_to_file("$tempdir/backup/dir-not-empty.txt", "Some data");

$node->command_fails([ 'pg_basebackup', '-D', "$tempdir/backup", '-n' ],
	'failing run with no-clean option');

ok(-d "$tempdir/backup", 'backup directory was created and left behind');
rmtree("$tempdir/backup");

open my $conf, '>>', "$pgdata/postgresql.conf";
print $conf "max_replication_slots = 10\n";
print $conf "max_wal_senders = 10\n";
print $conf "wal_level = replica\n";
close $conf;
$node->restart;

# Write some files to test that they are not copied.
foreach my $filename (
	qw(backup_label tablespace_map postgresql.auto.conf.tmp
	current_logfiles.tmp global/pg_internal.init.123))
{
	open my $file, '>>', "$pgdata/$filename";
	print $file "DONOTCOPY";
	close $file;
}

# Connect to a database to create global/pg_internal.init.  If this is removed
# the test to ensure global/pg_internal.init is not copied will return a false
# positive.
$node->safe_psql('postgres', 'SELECT 1;');

# Create an unlogged table to test that forks other than init are not copied.
$node->safe_psql('postgres', 'CREATE UNLOGGED TABLE base_unlogged (id int)');

my $baseUnloggedPath = $node->safe_psql('postgres',
	q{select pg_relation_filepath('base_unlogged')});

# Make sure main and init forks exist
ok(-f "$pgdata/${baseUnloggedPath}_init", 'unlogged init fork in base');
ok(-f "$pgdata/$baseUnloggedPath",        'unlogged main fork in base');

# Create files that look like temporary relations to ensure they are ignored.
my $postgresOid = $node->safe_psql('postgres',
	q{select oid from pg_database where datname = 'postgres'});

my @tempRelationFiles =
  qw(t_999 t_999.1 t_9999_vm t_99999_vm.1);

foreach my $filename (@tempRelationFiles)
{
	append_to_file("$pgdata/base/$postgresOid/$filename", 'TEMP_RELATION');
}

# Run base backup.
$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/backup", '-X', 'none', '--target-gp-dbid', '123', '--no-verify-checksums' ],
	'pg_basebackup runs');
ok(-f "$tempdir/backup/PG_VERSION", 'backup was created');

# Permissions on backup should be default
SKIP:
{
	skip "unix-style permissions not supported on Windows", 1
	  if ($windows_os);

	ok(check_mode_recursive("$tempdir/backup", 0700, 0600),
		"check backup dir permissions");
}

# Only archive_status directory should be copied in pg_wal/.
is_deeply(
	[ sort(slurp_dir("$tempdir/backup/pg_wal/")) ],
	[ sort qw(. .. archive_status) ],
	'no WAL files copied');

# Contents of these directories should not be copied.
foreach my $dirname (
	qw(pg_dynshmem pg_notify pg_replslot pg_serial pg_snapshots pg_stat_tmp pg_subtrans)
  )
{
	is_deeply(
		[ sort(slurp_dir("$tempdir/backup/$dirname/")) ],
		[ sort qw(. ..) ],
		"contents of $dirname/ not copied");
}

# These files should not be copied.
foreach my $filename (
	qw(postgresql.auto.conf.tmp postmaster.opts postmaster.pid tablespace_map current_logfiles.tmp
	global/pg_internal.init global/pg_internal.init.123))
{
	ok(!-f "$tempdir/backup/$filename", "$filename not copied");
}

# Unlogged relation forks other than init should not be copied
ok(-f "$tempdir/backup/${baseUnloggedPath}_init",
	'unlogged init fork in backup');
ok( !-f "$tempdir/backup/$baseUnloggedPath",
	'unlogged main fork not in backup');

# Temp relations should not be copied.
foreach my $filename (@tempRelationFiles)
{
	ok( !-f "$tempdir/backup/base/$postgresOid/$filename",
		"base/$postgresOid/$filename not copied");
}

# Make sure existing backup_label was ignored.
isnt(slurp_file("$tempdir/backup/backup_label"),
	'DONOTCOPY', 'existing backup_label not copied');
rmtree("$tempdir/backup");

$node->command_ok(
	[
		'pg_basebackup', '-D', "$tempdir/backup2", '--waldir',
		"$tempdir/xlog2", '--target-gp-dbid', '123'
	],
	'separate xlog directory');
ok(-f "$tempdir/backup2/PG_VERSION", 'backup was created');
ok(-d "$tempdir/xlog2/",             'xlog directory was created');
rmtree("$tempdir/backup2");
rmtree("$tempdir/xlog2");

$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/tarbackup", '--target-gp-dbid', '123', , '-Ft' ],
	'tar format');
ok(-f "$tempdir/tarbackup/base.tar", 'backup tar was created');
rmtree("$tempdir/tarbackup");

########################## Test that the headers are zeroed out in both the primary and mirror WAL files
my $node_wal_compare_primary = PostgreSQL::Test::Cluster->new('wal_compare_primary');
# We need to enable archiving for this test because we depend on the backup history
# file created by pg_basebackup to retrieve the "STOP WAL LOCATION". This file only
# gets persisted if archiving is turned on.
$node_wal_compare_primary->init(
	has_archiving    => 1,
	allows_streaming => 1);
$node_wal_compare_primary->start;

my $node_wal_compare_primary_datadir = $node_wal_compare_primary->data_dir;
my $node_wal_compare_standby_datadir = "$tempdir/wal_compare_standby";

# Ensure that when pg_basebackup is run, the last WAL segment file
# containing the BACKUP_END and wal SWITCH records match on both
# the primary and mirror segment. We want to ensure that all pages after
# the wal SWITCH record are all zeroed out. Previously, the primary
# segment's WAL segment file would have interleaved page headers instead
# of all zeros. Although the WAL segment files from the primary and
# mirror segments were logically the same, they were different physically
# and would lead to checksum mismatches for external tools that checked
# for that.

## Insert data and then run pg_basebackup
$node_wal_compare_primary->safe_psql('postgres',  'CREATE TABLE zero_header_test as SELECT generate_series(1,1000);');
$node_wal_compare_primary->command_ok([ 'pg_basebackup', '-D', $node_wal_compare_standby_datadir, '--target-gp-dbid', '123' , '-X', 'stream'],
	'pg_basebackup wal file comparison test');
ok( -f "$node_wal_compare_standby_datadir/PG_VERSION", 'pg_basebackup ran successfully');

# We can't rely on `pg_current_wal_lsn()` to get the last WAL filename that was
# copied over to the standby. This is because it's possible for newer WAL files
# to get created after pg_basebackup is run. More specifically, insertion of
# "Standby" records can create new WAL files quickly enough for
# `pg_current_wal_lsn()` to not return the WAL file where pg_basebackup had stopped.
# So instead, we rely on the backup history file created by pg_basebackup to get
# this information. We can safely assume that there's only one backup history
# file in the primary's wal dir
my $backup_history_file = "$node_wal_compare_primary_datadir/pg_wal/*.backup";
my $stop_wal_file_cmd = 'sed -n "s/STOP WAL LOCATION.*(file //p" ' . $backup_history_file . ' | sed "s/)//g"';
my $stop_wal_file = `$stop_wal_file_cmd`;
chomp($stop_wal_file);

my $primary_wal_file_path = "$node_wal_compare_primary_datadir/pg_wal/$stop_wal_file";
my $mirror_wal_file_path = "$node_wal_compare_standby_datadir/pg_wal/$stop_wal_file";

# Test that primary and mirror WAL file is the same
ok(compare($primary_wal_file_path, $mirror_wal_file_path) eq 0, "wal file comparison");

# Test that all the bytes after the last written record in the WAL file are zeroed out
my $total_bytes_cmd = 'pg_controldata ' . $node_wal_compare_standby_datadir .  ' | grep "Bytes per WAL segment:" |  awk \'{print $5}\'';
my $total_allocated_bytes = `$total_bytes_cmd`;

my $current_lsn_cmd = 'pg_waldump ' . $primary_wal_file_path . ' | grep "SWITCH" | awk \'{print $10}\' | sed "s/,//"';
my $current_lsn = `$current_lsn_cmd`;
chomp($current_lsn);
my $current_byte_offset = $node_wal_compare_primary->safe_psql('postgres', "SELECT file_offset FROM pg_walfile_name_offset('$current_lsn');");

# Get offset of last written record
open my $fh, '<:raw', $primary_wal_file_path;
# Since pg_walfile_name_offset does not account for the wal switch record, we need to add it ourselves
my $wal_switch_record_len = 32;
seek $fh, $current_byte_offset + $wal_switch_record_len, 0;
my $bytes_read = "";
my $len_bytes_to_validate = $total_allocated_bytes - $current_byte_offset;
read($fh, $bytes_read, $len_bytes_to_validate);
close $fh;
ok($bytes_read =~ /\A\x00*+\z/, 'make sure wal segment is zeroed');

############################## End header test #####################################

$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-T=/foo" ],
	'-T with empty old directory fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-T/foo=" ],
	'-T with empty new directory fails');
$node->command_fails(
	[
		'pg_basebackup', '-D', "$tempdir/backup_foo", '-Fp',
		"-T/foo=/bar=/baz", '--target-gp-dbid', '123'
	],
	'-T with multiple = fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-Tfoo=/bar" ],
	'-T with old directory not absolute fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-T/foo=bar" ],
	'-T with new directory not absolute fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-Tfoo" ],
	'-T with invalid format fails');

# Tar format doesn't support filenames longer than 100 bytes.
my $superlongname = "superlongname_" . ("x" x 100);
my $superlongpath = "$pgdata/$superlongname";

open my $file, '>', "$superlongpath"
  or die "unable to create file $superlongpath";
close $file;
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/tarbackup_l1", '--target-gp-dbid', '123', '-Ft' ],
	'pg_basebackup tar with long name fails');
unlink "$pgdata/$superlongname";

# The following tests test symlinks. Windows doesn't have symlinks, so
# skip on Windows.
SKIP:
{
	skip "symlinks not supported on Windows", 18 if ($windows_os);

	# Move pg_replslot out of $pgdata and create a symlink to it.
	$node->stop;

	# Set umask so test directories and files are created with group permissions
	umask(0027);

	# Enable group permissions on PGDATA
	chmod_recursive("$pgdata", 0750, 0640);

	rename("$pgdata/pg_replslot", "$tempdir/pg_replslot")
	  or BAIL_OUT "could not move $pgdata/pg_replslot";
	symlink("$tempdir/pg_replslot", "$pgdata/pg_replslot")
	  or BAIL_OUT "could not symlink to $pgdata/pg_replslot";

	$node->start;

	# Create a temporary directory in the system location and symlink it
	# to our physical temp location.  That way we can use shorter names
	# for the tablespace directories, which hopefully won't run afoul of
	# the 99 character length limit.
	my $shorter_tempdir = PostgreSQL::Test::Utils::tempdir_short . "/tempdir";
	symlink "$tempdir", $shorter_tempdir;

	mkdir "$tempdir/tblspc1";
	$node->safe_psql('postgres',
		"CREATE TABLESPACE tblspc1 LOCATION '$shorter_tempdir/tblspc1';");
	$node->safe_psql('postgres',
		"CREATE TABLE test1 (a int) TABLESPACE tblspc1;");
	$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/tarbackup2", '-Ft',
					  '--target-gp-dbid', '123'],
		'tar format with tablespaces');
	ok(-f "$tempdir/tarbackup2/base.tar", 'backup tar was created');
	my @tblspc_tars = glob "$tempdir/tarbackup2/[0-9]*.tar";
	is(scalar(@tblspc_tars), 1, 'one tablespace tar was created');
	rmtree("$tempdir/tarbackup2");

	# Create an unlogged table to test that forks other than init are not copied.
	$node->safe_psql('postgres',
		'CREATE UNLOGGED TABLE tblspc1_unlogged (id int) TABLESPACE tblspc1;'
	);

	my $tblspc1UnloggedPath = $node->safe_psql('postgres',
		q{select pg_relation_filepath('tblspc1_unlogged')});

	my $node_dbid = $node->dbid;
	# Make sure main and init forks exist
	ok( -f "$pgdata/${tblspc1UnloggedPath}_init",
		'unlogged init fork in tablespace');
	ok(-f "$pgdata/$tblspc1UnloggedPath", 'unlogged main fork in tablespace');

	# Create files that look like temporary relations to ensure they are ignored
	# in a tablespace.
	my @tempRelationFiles = qw(t_888 t_888888_vm.1);
	my $tblSpc1Id         = basename(
		dirname(
			dirname(
				$node->safe_psql(
					'postgres', q{select pg_relation_filepath('test1')}))));

	foreach my $filename (@tempRelationFiles)
	{
		append_to_file(
			"$shorter_tempdir/tblspc1/$node_dbid/$tblSpc1Id/$postgresOid/$filename",
			'TEMP_RELATION');
	}

	$node->command_fails(
		[ 'pg_basebackup', '-D', "$tempdir/backup1", '-Fp',
		  '--target-gp-dbid', '-1'
		],
		'plain format with tablespaces fails without tablespace mapping and target-gp-dbid as the test server dbid');

	$node->command_ok(
		[
			'pg_basebackup', '-D', "$tempdir/backup1", '-Fp',
		 	'--target-gp-dbid', '1',
			"-T$shorter_tempdir/tblspc1=$tempdir/tbackup/tblspc1"
		],
		'plain format with tablespaces succeeds with tablespace mapping');
	ok(-d "$tempdir/tbackup/tblspc1/1", 'tablespace was relocated');
	opendir(my $dh, "$pgdata/pg_tblspc") or die;
	ok( (   grep {
				-l "$tempdir/backup1/pg_tblspc/$_"
				  and readlink "$tempdir/backup1/pg_tblspc/$_" eq
				  "$tempdir/tbackup/tblspc1/1"
			} readdir($dh)),
		"tablespace symlink was updated");
	closedir $dh;

	# Group access should be enabled on all backup files
	ok(check_mode_recursive("$tempdir/backup1", 0750, 0640),
		"check backup dir permissions");

	# Unlogged relation forks other than init should not be copied
	my ($tblspc1UnloggedBackupPath) =
	  $tblspc1UnloggedPath =~ /[^\/]*\/[^\/]*\/[^\/]*$/g;

	ok(-f "$tempdir/tbackup/tblspc1/1/${tblspc1UnloggedBackupPath}_init",
		'unlogged init fork in tablespace backup');
	ok(!-f "$tempdir/tbackup/tblspc1/1/$tblspc1UnloggedBackupPath",
		'unlogged main fork not in tablespace backup');

	# Temp relations should not be copied.
	foreach my $filename (@tempRelationFiles)
	{
		ok( !-f "$tempdir/tbackup/tblspc1/1/$tblSpc1Id/$postgresOid/$filename",
			"[tblspc1]/$postgresOid/$filename not copied");

		# Also remove temp relation files or tablespace drop will fail.
		my $filepath =
		  "$shorter_tempdir/tblspc1/$node_dbid/$tblSpc1Id/$postgresOid/$filename";

		unlink($filepath)
		  or BAIL_OUT("unable to unlink $filepath");
	}

	ok( -d "$tempdir/backup1/pg_replslot",
		'pg_replslot symlink copied as directory');
	rmtree("$tempdir/backup1");

	mkdir "$tempdir/tbl=spc2";
	$node->safe_psql('postgres', "DROP TABLE test1;");
	$node->safe_psql('postgres', "DROP TABLE tblspc1_unlogged;");
	$node->safe_psql('postgres', "DROP TABLESPACE tblspc1;");
	$node->safe_psql('postgres',
		"CREATE TABLESPACE tblspc2 LOCATION '$shorter_tempdir/tbl=spc2';");
	$node->command_ok(
		[
			'pg_basebackup', '-D', "$tempdir/backup3", '--target-gp-dbid', '123', '-Fp',
			"-T$shorter_tempdir/tbl\\=spc2=$tempdir/tbackup/tbl\\=spc2"
		],
		'mapping tablespace with = sign in path');
	ok(-d "$tempdir/tbackup/tbl=spc2",
		'tablespace with = sign was relocated');
	$node->safe_psql('postgres', "DROP TABLESPACE tblspc2;");
	rmtree("$tempdir/backup3");

	mkdir "$tempdir/$superlongname";
	$node->safe_psql('postgres',
		"CREATE TABLESPACE tblspc3 LOCATION '$tempdir/$superlongname';");
	$node->command_ok(
		[ 'pg_basebackup', '-D', "$tempdir/tarbackup_l3", '--target-gp-dbid', '123', '-Ft' ],
		'pg_basebackup tar with long symlink target');
	$node->safe_psql('postgres', "DROP TABLESPACE tblspc3;");
	rmtree("$tempdir/tarbackup_l3");
}

$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/backupR", '--target-gp-dbid', '123', '-R' ],
	'pg_basebackup -R runs');
ok(-f "$tempdir/backupR/postgresql.auto.conf", 'postgresql.auto.conf exists');
ok(-f "$tempdir/backupR/standby.signal",       'standby.signal was created');
my $recovery_conf = slurp_file "$tempdir/backupR/postgresql.auto.conf";
rmtree("$tempdir/backupR");

my $port = $node->port;
like(
	$recovery_conf,
	qr/^primary_conninfo = '.*port=$port.*'\n/m,
	'postgresql.auto.conf sets primary_conninfo');

$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxd", '--target-gp-dbid', '123' ],
	'pg_basebackup runs in default xlog mode');
ok(grep(/^[0-9A-F]{24}$/, slurp_dir("$tempdir/backupxd/pg_wal")),
	'WAL files copied');
rmtree("$tempdir/backupxd");

$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxf", '--target-gp-dbid', '123', '-X', 'fetch' ],
	'pg_basebackup -X fetch runs');
ok(grep(/^[0-9A-F]{24}$/, slurp_dir("$tempdir/backupxf/pg_wal")),
	'WAL files copied');
rmtree("$tempdir/backupxf");
$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxs", '--target-gp-dbid', '123', '-X', 'stream' ],
	'pg_basebackup -X stream runs');
ok(grep(/^[0-9A-F]{24}$/, slurp_dir("$tempdir/backupxs/pg_wal")),
	'WAL files copied');
rmtree("$tempdir/backupxs");
$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxst", '--target-gp-dbid', '123', '-X', 'stream', '-Ft' ],
	'pg_basebackup -X stream runs in tar mode');
ok(-f "$tempdir/backupxst/pg_wal.tar", "tar file was created");
rmtree("$tempdir/backupxst");
$node->command_ok(
	[
		'pg_basebackup', '--target-gp-dbid', '123',
        '-D',
		"$tempdir/backupnoslot", '-X',
		'stream',                '--no-slot'
	],
	'pg_basebackup -X stream runs with --no-slot');
rmtree("$tempdir/backupnoslot");

$node->command_fails(
	[
		'pg_basebackup', '--target-gp-dbid', '123',
        '-D',
		"$tempdir/backupxs_sl_fail", '-X',
		'stream',                    '-S',
		'slot0'
	],
	'pg_basebackup fails with nonexistent replication slot');

$node->command_fails(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backupxs_slot", '-C' ],
	'pg_basebackup -C fails without slot name');

$node->command_fails(
	[
		'pg_basebackup', '--target-gp-dbid', '123',
        '-D',
		"$tempdir/backupxs_slot", '-C',
		'-S',                     'slot0',
		'--no-slot'
	],
	'pg_basebackup fails with -C -S --no-slot');

$node->command_ok(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backupxs_slot", '-C', '-S', 'slot0' ],
	'pg_basebackup -C runs');
rmtree("$tempdir/backupxs_slot");

is( $node->safe_psql(
		'postgres',
		q{SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'slot0'}
	),
	'slot0',
	'replication slot was created');
isnt(
	$node->safe_psql(
		'postgres',
		q{SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot0'}
	),
	'',
	'restart LSN of new slot is not null');

$node->command_fails(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backupxs_slot1", '-v', '-C', '-S', 'slot0' ],
	'pg_basebackup fails with -C -S and a previously existing slot');

$node->safe_psql('postgres',
	q{SELECT * FROM pg_create_physical_replication_slot('slot1')});
my $lsn = $node->safe_psql('postgres',
	q{SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot1'}
);
is($lsn, '', 'restart LSN of new slot is null');
$node->command_fails(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/fail", '-S', 'slot1', '-X', 'none' ],
	'pg_basebackup with replication slot fails without WAL streaming');
$node->command_ok(
	[
		'pg_basebackup', '-D', "$tempdir/backupxs_sl", '--target-gp-dbid', '123', '-X',
		'stream',        '-S', 'slot1'
	],
	'pg_basebackup -X stream with replication slot runs');
$lsn = $node->safe_psql('postgres',
	q{SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot1'}
);
like($lsn, qr!^0/[0-9A-Z]{7,8}$!, 'restart LSN of slot has advanced');
rmtree("$tempdir/backupxs_sl");

$node->command_ok(
	[
		'pg_basebackup', '--target-gp-dbid', '123',
        '-D', "$tempdir/backupxs_sl_R", '-X',
		'stream',        '-S', 'slot1',                  '-R'
	],
	'pg_basebackup with replication slot and -R runs');
like(
	slurp_file("$tempdir/backupxs_sl_R/postgresql.auto.conf"),
	qr/^primary_slot_name = 'slot1'\n/m,
	'recovery conf file sets primary_slot_name');

my $checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'on', 'checksums are enabled');
rmtree("$tempdir/backupxs_sl_R");

# create tables to corrupt and get their relfilenodes
my $file_corrupt1 = $node->safe_psql('postgres',
	q{SELECT a INTO corrupt1 FROM generate_series(1,10000) AS a; ALTER TABLE corrupt1 SET (autovacuum_enabled=false); SELECT pg_relation_filepath('corrupt1')}
);
my $file_corrupt2 = $node->safe_psql('postgres',
	q{SELECT b INTO corrupt2 FROM generate_series(1,2) AS b; ALTER TABLE corrupt2 SET (autovacuum_enabled=false); SELECT pg_relation_filepath('corrupt2')}
);

# get block size for corruption steps
my $block_size = $node->safe_psql('postgres', 'SHOW block_size;');

# induce corruption
# Greenplum invokes pg_ctl to start, haven't defined $self->{_pid}
# $node->stop;
system_or_bail 'pg_ctl', '-D', $pgdata, 'stop';
$node->corrupt_page_checksum($file_corrupt1, 0);
system_or_bail 'pg_ctl', '-o', '-c gp_role=utility --gp_dbid=1 --gp_contentid=-1', '-D', $pgdata, 'start';

$node->command_checks_all(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backup_corrupt" ],
	1,
	[qr{^$}],
	[qr/^WARNING.*checksum verification failed/s],
	'pg_basebackup reports checksum mismatch');
rmtree("$tempdir/backup_corrupt");

# induce further corruption in 5 more blocks
# Greenplum invokes pg_ctl to start, haven't defined $self->{_pid}
# $node->stop;
system_or_bail 'pg_ctl', '-D', $pgdata, 'stop';
for my $i (1 .. 5)
{
	$node->corrupt_page_checksum($file_corrupt1, $i * $block_size);
}
system_or_bail 'pg_ctl', '-o', '-c gp_role=utility --gp_dbid=1 --gp_contentid=-1', '-D', $pgdata, 'start';

$node->command_checks_all(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backup_corrupt2" ],
	1,
	[qr{^$}],
	[qr/^WARNING.*further.*failures.*will.not.be.reported/s],
	'pg_basebackup does not report more than 5 checksum mismatches');
rmtree("$tempdir/backup_corrupt2");

# induce corruption in a second file
# Greenplum invokes pg_ctl to start, haven't defined $self->{_pid}
# $node->stop;
system_or_bail 'pg_ctl', '-D', $pgdata, 'stop';
$node->corrupt_page_checksum($file_corrupt2, 0);
system_or_bail 'pg_ctl', '-o', '-c gp_role=utility --gp_dbid=1 --gp_contentid=-1', '-D', $pgdata, 'start';

$node->command_checks_all(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backup_corrupt3" ],
	1,
	[qr{^$}],
	[qr/^WARNING.*7 total checksum verification failures/s],
	'pg_basebackup correctly report the total number of checksum mismatches');
rmtree("$tempdir/backup_corrupt3");

# do not verify checksums, should return ok
$node->command_ok(
	[
		'pg_basebackup', '--target-gp-dbid', '123',            '-D',
		"$tempdir/backup_corrupt4", '--no-verify-checksums'
	],
	'pg_basebackup with -k does not report checksum mismatch');
rmtree("$tempdir/backup_corrupt4");

$node->safe_psql('postgres', "DROP TABLE corrupt1;");
$node->safe_psql('postgres', "DROP TABLE corrupt2;");

# Some additional GPDB tests
my $twenty_characters = '11111111112222222222';
my $longer_tempdir = "$tempdir/some_long_directory_path_$twenty_characters$twenty_characters$twenty_characters$twenty_characters$twenty_characters";
my $some_backup_dir = "$tempdir/backup_dir";
my $some_other_backup_dir = "$tempdir/other_backup_dir";

mkdir "$longer_tempdir";
mkdir "$some_backup_dir";
$node->psql('postgres', "CREATE TABLESPACE too_long_tablespace LOCATION '$longer_tempdir';");
$node->command_checks_all(
	[ 'pg_basebackup', '-D', "$some_backup_dir", '--target-gp-dbid', '99'],
	1,
	[qr{^$}],
	[qr/symbolic link ".*" target is too long and will not be added to the backup/],
	'basebackup with a tablespace that has a very long location should error out with target is too long.');

mkdir "$some_other_backup_dir";
$node->command_checks_all(
	['pg_basebackup', '-D', "$some_other_backup_dir", '--target-gp-dbid', '99'],
	1,
	[qr{^$}],
	[qr/The symbolic link with target ".*" is too long. Symlink targets with length greater than 100 characters would be truncated./],
	'basebackup with a tablespace that has a very long location should error out link not added to the backup.');

$node->command_checks_all(
	['ls', "$some_other_backup_dir/pg_tblspc/*"],
	2,
	[qr{^$}],
	[qr/No such file/],
	'tablespace directory should be empty');

$node->psql('postgres', "DROP TABLESPACE too_long_tablespace;");

#
# GPDB: Exclude some files with the --exclude-from option
#

my $exclude_tempdir = "$tempdir/backup_exclude";
my $excludelist = "$tempdir/exclude.list";

mkdir "$exclude_tempdir";
mkdir "$pgdata/exclude";

open EXCLUDELIST, ">$excludelist";

# Put a large amount of non-exist patterns in the exclude-from file,
# the pattern matching is efficient enough to handle them.
for my $i (1..1000000) {
	print EXCLUDELIST "./exclude/non_exist.$i\n";
}

# Create some files to exclude
for my $i (1..1000) {
	print EXCLUDELIST "./exclude/$i\n";

	open FILE, ">$pgdata/exclude/$i";
	close FILE;
}

# Below file should not be excluded
open FILE, ">$pgdata/exclude/keep";
close FILE;

close EXCLUDELIST;

$node->command_ok(
	[	'pg_basebackup',
		'-D', "$exclude_tempdir",
		'--target-gp-dbid', '123',
		'--exclude-from', "$excludelist" ],
	'pg_basebackup runs with exclude-from file');
ok(! -f "$exclude_tempdir/exclude/0", 'excluded files were not created');
ok(-f "$exclude_tempdir/exclude/keep", 'other files were created');

# GPDB: Exclude gpbackup default directory
my $gpbackup_test_dir = "$tempdir/gpbackup_test_dir";
mkdir "$pgdata/backups";
append_to_file("$pgdata/backups/random_backup_file", "some random backup data");

$node->command_ok([ 'pg_basebackup', '-D', $gpbackup_test_dir, '--target-gp-dbid', '123' ],
	'pg_basebackup does not copy over \'backups/\' directory created by gpbackup');

ok(! -d "$gpbackup_test_dir/backups", 'gpbackup default backup directory should be excluded');
rmtree($gpbackup_test_dir);

#GPDB: write config files only
mkdir("$tempdir/backup");

$node->command_fails([ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123',
	                   '--write-conf-files-only', '--create-slot', '--slot', "wal_replication_slot"],
	                  'pg_basebackup --write-conf-files-only fails with --create_slot');

$node->command_fails([ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123',
	                   '--write-conf-files-only', '--write-recovery-conf' ],
	                   'pg_basebackup --write-conf-files-only fails with --write-recovery-conf');

$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123', '--write-conf-files-only' ],
	'pg_basebackup runs with write-conf-files-only');
ok(-f "$tempdir/backup/internal.auto.conf", 'internal.auto.conf was created');
ok(-f "$tempdir/backup/postgresql.auto.conf", 'postgresql.auto.conf was created');
ok(-f "$tempdir/backup/standby.signal",       'standby.signal was created');
rmtree("$tempdir/backup");

done_testing();
