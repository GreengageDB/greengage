# Test dumping a partition of an extension-owned partitioned table that
# has an extension-owned partitioned index.
#
# The child index is created automatically when the partition is attached,
# so a regular dump must not contain a separate CREATE INDEX for it, or the
# restore would end up with a duplicate index on the partition.  A binary
# upgrade dump still has to create and attach the child index explicitly.

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
});

my $create_child_index = qr/^
	\QCREATE UNIQUE INDEX parttab_1_col1_col2_idx ON regress_pg_dump_schema.parttab_1 USING btree (col1, col2);\E
	$/xm;
my $attach_child_index = qr/^
	\QALTER INDEX regress_pg_dump_schema.parttab_col1_col2_idx ATTACH PARTITION regress_pg_dump_schema.parttab_1_col1_col2_idx;\E
	$/xm;
my $attach_partition = qr/^
	\QALTER TABLE ONLY regress_pg_dump_schema.parttab ATTACH PARTITION regress_pg_dump_schema.parttab_1 FOR VALUES FROM (1) TO (100);\E
	$/xm;

#########################################
# Regular dump

$node->command_ok(
	[ 'pg_dump', '--no-sync', "--file=$tempdir/defaults.sql", 'postgres' ],
	'pg_dump runs');

my $dump = slurp_file("$tempdir/defaults.sql");

like($dump, $attach_partition, 'dump attaches the partition');
unlike($dump, $create_child_index,
	'dump does not create the child index of the extension index');
unlike($dump, $attach_child_index,
	'dump does not attach the child index of the extension index');

# Restore the dump and check that the partition ends up with exactly one
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
		SELECT indexrelid::regclass, i.inhparent::regclass
		FROM pg_index
		LEFT JOIN pg_inherits i ON i.inhrelid = indexrelid
		WHERE indrelid = 'regress_pg_dump_schema.parttab_1'::regclass
		ORDER BY 1;
	}),
	'regress_pg_dump_schema.parttab_1_col1_col2_idx|regress_pg_dump_schema.parttab_col1_col2_idx',
	'restored partition has a single index, attached to the extension index'
);

#########################################
# Binary upgrade dump

$node->command_ok(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/binary_upgrade.sql",
		'--schema-only', '--binary-upgrade', 'postgres'
	],
	'pg_dump --binary-upgrade runs');

$dump = slurp_file("$tempdir/binary_upgrade.sql");

like($dump, $create_child_index,
	'binary upgrade dump creates the child index of the extension index');
like($dump, $attach_child_index,
	'binary upgrade dump attaches the child index of the extension index');

$node->stop('fast');

done_testing();
