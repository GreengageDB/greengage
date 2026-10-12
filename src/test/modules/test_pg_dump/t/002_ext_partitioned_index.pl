# Test dumping partitions (direct, sub-partitions, primary key) of an
# extension-owned partitioned table with an extension-owned index: the child
# index must not be dumped separately, except in a binary upgrade dump, and
# pg_dump must warn about it.  Also check that the comment of a renamed child
# index is lost on restore, and that the child index of an index the user
# added to the extension table is lost.

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $tempdir = PostgreSQL::Test::Utils::tempdir;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# regress_pg_dump_schema.parttab and its unique index on (col1, col2) are
# created by the extension; the partition is not.  The extension script
# grants privileges to regress_dump_test_role, so it has to exist.
$node->safe_psql(
	'postgres', q{
	CREATE ROLE regress_dump_test_role;
	CREATE EXTENSION test_pg_dump;
	CREATE TABLE regress_pg_dump_schema.parttab_1
		PARTITION OF regress_pg_dump_schema.parttab
		FOR VALUES FROM (1) TO (100);
	COMMENT ON INDEX regress_pg_dump_schema.parttab_1_col1_col2_idx IS 'child';
	CREATE TABLE regress_pg_dump_schema.parttab_2
		PARTITION OF regress_pg_dump_schema.parttab
		FOR VALUES FROM (100) TO (200) PARTITION BY RANGE (col1);
	CREATE TABLE regress_pg_dump_schema.parttab_2_1
		PARTITION OF regress_pg_dump_schema.parttab_2
		FOR VALUES FROM (1) TO (100);
	CREATE TABLE regress_pg_dump_schema.parttab_pk_1
		PARTITION OF regress_pg_dump_schema.parttab_pk
		FOR VALUES FROM (1) TO (100);
});

my $create_child_index = qr/^
	\QCREATE UNIQUE INDEX parttab_1_col1_col2_idx ON regress_pg_dump_schema.parttab_1 USING btree (col1, col2);\E
	$/xm;
my $attach_child_index = qr/^
	\QALTER INDEX regress_pg_dump_schema.parttab_col1_col2_idx ATTACH PARTITION regress_pg_dump_schema.parttab_1_col1_col2_idx;\E
	$/xm;
my $create_grandchild_index = qr/^
	\QCREATE UNIQUE INDEX parttab_2_1_col1_col2_idx ON regress_pg_dump_schema.parttab_2_1 USING btree (col1, col2);\E
	$/xm;
my $attach_grandchild_index = qr/^
	\QALTER INDEX regress_pg_dump_schema.parttab_2_col1_col2_idx ATTACH PARTITION regress_pg_dump_schema.parttab_2_1_col1_col2_idx;\E
	$/xm;
my $add_child_pkey = qr/^
	\s+\QADD CONSTRAINT parttab_pk_1_pkey PRIMARY KEY (col1, col2);\E
	$/xm;
my $comment_child_index = qr/^
	\QCOMMENT ON INDEX regress_pg_dump_schema.parttab_1_col1_col2_idx IS 'child';\E
	$/xm;
my $attach_partition = qr/^
	\QALTER TABLE ONLY regress_pg_dump_schema.parttab ATTACH PARTITION regress_pg_dump_schema.parttab_1 FOR VALUES FROM (1) TO (100);\E
	$/xm;

#########################################
# Regular dump

# Expected pg_dump warning for a child index that is not dumped.
sub skip_warning
{
	my ($index, $partition, $extindex) = @_;
	return qr/\Qwarning: index "regress_pg_dump_schema.$index" on partition "$partition" is not dumped; it is restored only if CREATE EXTENSION creates index "regress_pg_dump_schema.$extindex"\E/;
}

$node->command_checks_all(
	[ 'pg_dump', '--no-sync', "--file=$tempdir/defaults.sql", 'postgres' ],
	0,
	[qr/^$/],
	[
		skip_warning('parttab_1_col1_col2_idx', 'parttab_1',
			'parttab_col1_col2_idx'),
		skip_warning('parttab_2_col1_col2_idx', 'parttab_2',
			'parttab_col1_col2_idx'),
		skip_warning('parttab_2_1_col1_col2_idx', 'parttab_2_1',
			'parttab_col1_col2_idx'),
		skip_warning('parttab_pk_1_pkey', 'parttab_pk_1', 'parttab_pk_pkey'),
	],
	'pg_dump warns about the child indexes it does not dump');

my $dump = slurp_file("$tempdir/defaults.sql");

like($dump, $attach_partition, 'dump attaches the partition');
unlike($dump, $create_child_index,
	'dump does not create the child index of the extension index');
unlike($dump, $attach_child_index,
	'dump does not attach the child index of the extension index');
unlike($dump, $create_grandchild_index,
	'dump does not create the grandchild index of the extension index');
unlike($dump, $attach_grandchild_index,
	'dump does not attach the grandchild index of the extension index');
unlike($dump, $add_child_pkey,
	'dump does not add the child primary key of the extension table');
like($dump, $comment_child_index, 'dump keeps the child index comment');

# Restore the dump and check that each partition ends up with exactly one
# index, the one created by ATTACH PARTITION.
$node->safe_psql('postgres', 'CREATE DATABASE restored');
$node->command_ok(
	[
		'psql', '--no-psqlrc', '--set=ON_ERROR_STOP=1', '--quiet',
		"--file=$tempdir/defaults.sql", 'restored'
	],
	'dump restores');

is( $node->safe_psql(
		'restored', q{
		SELECT indrelid::regclass, indexrelid::regclass, i.inhparent::regclass
		FROM pg_index
		LEFT JOIN pg_inherits i ON i.inhrelid = indexrelid
		WHERE indrelid::regclass::text IN
			('regress_pg_dump_schema.parttab_1',
			 'regress_pg_dump_schema.parttab_2',
			 'regress_pg_dump_schema.parttab_2_1',
			 'regress_pg_dump_schema.parttab_pk_1')
		ORDER BY indrelid::regclass::text;
	}),
	join("\n",
		'regress_pg_dump_schema.parttab_1|regress_pg_dump_schema.parttab_1_col1_col2_idx|regress_pg_dump_schema.parttab_col1_col2_idx',
		'regress_pg_dump_schema.parttab_2|regress_pg_dump_schema.parttab_2_col1_col2_idx|regress_pg_dump_schema.parttab_col1_col2_idx',
		'regress_pg_dump_schema.parttab_2_1|regress_pg_dump_schema.parttab_2_1_col1_col2_idx|regress_pg_dump_schema.parttab_2_col1_col2_idx',
		'regress_pg_dump_schema.parttab_pk_1|regress_pg_dump_schema.parttab_pk_1_pkey|regress_pg_dump_schema.parttab_pk_pkey'),
	'restored partitions have a single index each, attached to the parent index'
);

is( $node->safe_psql(
		'restored',
		q{SELECT obj_description('regress_pg_dump_schema.parttab_1_col1_col2_idx'::regclass)}),
	'child',
	'restored child index keeps its comment');

#########################################
# Dumps without index definitions don't warn

$node->command_checks_all(
	[ 'pg_dump', '--no-sync', '--data-only', 'postgres' ],
	0,
	[qr/^/],
	[qr/^$/],
	'pg_dump --data-only runs without warnings');

$node->command_checks_all(
	[ 'pg_dump', '--no-sync', '--section=pre-data', 'postgres' ],
	0,
	[qr/^/],
	[qr/^$/],
	'pg_dump --section=pre-data runs without warnings');

#########################################
# Binary upgrade dump

$node->command_checks_all(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/binary_upgrade.sql",
		'--schema-only', '--binary-upgrade', 'postgres'
	],
	0,
	[qr/^$/],
	[qr/^$/],
	'pg_dump --binary-upgrade runs without warnings');

$dump = slurp_file("$tempdir/binary_upgrade.sql");

like($dump, $create_child_index,
	'binary upgrade dump creates the child index of the extension index');
like($dump, $attach_child_index,
	'binary upgrade dump attaches the child index of the extension index');
like($dump, $create_grandchild_index,
	'binary upgrade dump creates the grandchild index of the extension index');
like($dump, $attach_grandchild_index,
	'binary upgrade dump attaches the grandchild index of the extension index');
like($dump, $add_child_pkey,
	'binary upgrade dump adds the child primary key of the extension table');

#########################################
# Renamed child index with a comment: the comment is dumped under the old
# name, which ATTACH PARTITION does not recreate.
#
# TODO: this is a known limitation, not the intended behavior.  If it is
# fixed, the child indexes should keep their names and comments, and the
# checks below should be inverted.

$node->safe_psql('postgres', 'CREATE DATABASE renamed');
$node->safe_psql(
	'renamed', q{
	CREATE EXTENSION test_pg_dump;
	CREATE TABLE regress_pg_dump_schema.parttab_1
		PARTITION OF regress_pg_dump_schema.parttab
		FOR VALUES FROM (1) TO (100);
	ALTER INDEX regress_pg_dump_schema.parttab_1_col1_col2_idx
		RENAME TO parttab_1_renamed_idx;
	COMMENT ON INDEX regress_pg_dump_schema.parttab_1_renamed_idx IS 'renamed';
	CREATE TABLE regress_pg_dump_schema.parttab_pk_1
		PARTITION OF regress_pg_dump_schema.parttab_pk
		FOR VALUES FROM (1) TO (100);
	ALTER INDEX regress_pg_dump_schema.parttab_pk_1_pkey
		RENAME TO parttab_pk_1_renamed_pkey;
	COMMENT ON CONSTRAINT parttab_pk_1_renamed_pkey
		ON regress_pg_dump_schema.parttab_pk_1 IS 'renamed pkey';
});

$node->command_ok(
	[ 'pg_dump', '--no-sync', "--file=$tempdir/renamed.sql", 'renamed' ],
	'pg_dump of renamed child indexes runs');

$dump = slurp_file("$tempdir/renamed.sql");

like(
	$dump,
	qr/^\QCOMMENT ON INDEX regress_pg_dump_schema.parttab_1_renamed_idx IS 'renamed';\E$/m,
	'dump comments on the child index under its old name');
like(
	$dump,
	qr/^\QCOMMENT ON CONSTRAINT parttab_pk_1_renamed_pkey ON regress_pg_dump_schema.parttab_pk_1 IS 'renamed pkey';\E$/m,
	'dump comments on the child constraint under its old name');
unlike($dump, qr/CREATE UNIQUE INDEX parttab_1_renamed_idx/,
	'dump does not create the renamed child index');

$node->safe_psql('postgres', 'CREATE DATABASE renamed_restored_stop');
my ($ret, $stdout, $stderr) =
  $node->psql('renamed_restored_stop', $dump, on_error_stop => 1);
isnt($ret, 0, 'restore with ON_ERROR_STOP fails on the renamed child index');

$node->safe_psql('postgres', 'CREATE DATABASE renamed_restored');
($ret, $stdout, $stderr) =
  $node->psql('renamed_restored', $dump, on_error_stop => 0);
like(
	$stderr,
	qr/relation "regress_pg_dump_schema.parttab_1_renamed_idx" does not exist/,
	'restore reports the missing renamed child index');
like(
	$stderr,
	qr/constraint "parttab_pk_1_renamed_pkey" for table "parttab_pk_1" does not exist/,
	'restore reports the missing renamed child constraint');

is( $node->safe_psql(
		'renamed_restored', q{
		SELECT indexrelid::regclass, obj_description(indexrelid, 'pg_class')
		FROM pg_index
		WHERE indrelid::regclass::text IN
			('regress_pg_dump_schema.parttab_1',
			 'regress_pg_dump_schema.parttab_pk_1')
		ORDER BY indrelid::regclass::text;
	}),
	join("\n",
		'regress_pg_dump_schema.parttab_1_col1_col2_idx|',
		'regress_pg_dump_schema.parttab_pk_1_pkey|'),
	'restored child indexes get generated names and lose their comments');

#########################################
# An index the user added to the extension table is not dumped and not
# recreated by CREATE EXTENSION, so the child index is lost on restore.
# pg_dump can't tell this from an index created by the extension, so it
# only warns about it.

$node->safe_psql('postgres', 'CREATE DATABASE user_index');
$node->safe_psql(
	'user_index', q{
	CREATE EXTENSION test_pg_dump;
	CREATE INDEX parttab_user_idx ON regress_pg_dump_schema.parttab (col1);
	CREATE TABLE regress_pg_dump_schema.parttab_1
		PARTITION OF regress_pg_dump_schema.parttab
		FOR VALUES FROM (1) TO (100);
});

$node->command_checks_all(
	[ 'pg_dump', '--no-sync', "--file=$tempdir/user_index.sql", 'user_index' ],
	0,
	[qr/^$/],
	[ skip_warning('parttab_1_col1_idx', 'parttab_1', 'parttab_user_idx') ],
	'pg_dump warns about the child index of the user-created index');

$node->safe_psql('postgres', 'CREATE DATABASE user_index_restored');
$node->command_ok(
	[
		'psql', '--no-psqlrc', '--set=ON_ERROR_STOP=1', '--quiet',
		"--file=$tempdir/user_index.sql", 'user_index_restored'
	],
	'dump with a user-created extension table index restores');

is( $node->safe_psql(
		'user_index_restored', q{
		SELECT indexrelid::regclass FROM pg_index
		WHERE indrelid = 'regress_pg_dump_schema.parttab_1'::regclass
		ORDER BY 1::text;
	}),
	'regress_pg_dump_schema.parttab_1_col1_col2_idx',
	'child index of the user-created index is lost on restore');

$node->stop('fast');

done_testing();
