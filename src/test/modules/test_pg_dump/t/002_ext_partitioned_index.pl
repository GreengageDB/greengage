# Test dumping partitions (direct, sub-partitions, primary key) of a
# partitioned table whose definition is not dumped, either because it is an
# extension member or because only the partition is selected.  Such a
# partition must be attached in post-data, after its own indexes, so that
# ATTACH PARTITION attaches them to the existing parent indexes instead of
# creating duplicates.  The child indexes keep their names and comments, and
# a parent index that is not restored makes the restore fail loudly instead
# of silently losing the child indexes.

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
	INSERT INTO regress_pg_dump_schema.parttab VALUES (1, 1), (1, 150);
});

my $create_child_index =
  'CREATE UNIQUE INDEX parttab_1_col1_col2_idx ON regress_pg_dump_schema.parttab_1 USING btree (col1, col2);';
my $attach_child_index =
  'ALTER INDEX regress_pg_dump_schema.parttab_col1_col2_idx ATTACH PARTITION regress_pg_dump_schema.parttab_1_col1_col2_idx;';
my $create_grandchild_index =
  'CREATE UNIQUE INDEX parttab_2_1_col1_col2_idx ON regress_pg_dump_schema.parttab_2_1 USING btree (col1, col2);';
my $attach_grandchild_index =
  'ALTER INDEX regress_pg_dump_schema.parttab_2_col1_col2_idx ATTACH PARTITION regress_pg_dump_schema.parttab_2_1_col1_col2_idx;';
my $add_child_pkey =
  'ADD CONSTRAINT parttab_pk_1_pkey PRIMARY KEY (col1, col2);';
my $comment_child_index =
  "COMMENT ON INDEX regress_pg_dump_schema.parttab_1_col1_col2_idx IS 'child';";
my $attach_partition =
  'ALTER TABLE ONLY regress_pg_dump_schema.parttab ATTACH PARTITION regress_pg_dump_schema.parttab_1 FOR VALUES FROM (1) TO (100);';
my $attach_sub_partition =
  'ALTER TABLE ONLY regress_pg_dump_schema.parttab ATTACH PARTITION regress_pg_dump_schema.parttab_2 FOR VALUES FROM (100) TO (200);';
my $attach_grandchild_partition =
  'ALTER TABLE ONLY regress_pg_dump_schema.parttab_2 ATTACH PARTITION regress_pg_dump_schema.parttab_2_1 FOR VALUES FROM (1) TO (100);';
my $attach_pk_partition =
  'ALTER TABLE ONLY regress_pg_dump_schema.parttab_pk ATTACH PARTITION regress_pg_dump_schema.parttab_pk_1 FOR VALUES FROM (1) TO (100);';

# Check that $first appears in $dump, and before $second.
sub dumped_before
{
	my ($dump, $first, $second, $name) = @_;
	my $first_pos = index($dump, $first);
	my $second_pos = index($dump, $second);

	ok($first_pos >= 0 && $second_pos >= 0 && $first_pos < $second_pos,
		$name)
	  or diag("'$first' at $first_pos, '$second' at $second_pos");
	return;
}

#########################################
# Regular dump

$node->command_ok(
	[ 'pg_dump', '--no-sync', "--file=$tempdir/defaults.sql", 'postgres' ],
	'pg_dump runs');

my $dump = slurp_file("$tempdir/defaults.sql");

ok(index($dump, $attach_child_index) >= 0,
	'dump attaches the child index of the extension index');
ok(index($dump, $comment_child_index) >= 0,
	'dump keeps the child index comment');
dumped_before($dump, $create_child_index, $attach_partition,
	'dump creates the child index before attaching the partition');
dumped_before($dump, $attach_grandchild_index, $attach_sub_partition,
	'dump attaches the grandchild index before attaching the sub-partition');
dumped_before($dump, $create_grandchild_index, $attach_grandchild_index,
	'dump creates the grandchild index before attaching it');
dumped_before($dump, $attach_grandchild_partition, $create_grandchild_index,
	'dump attaches the grandchild partition to the dumped table in pre-data'
);
dumped_before($dump, $add_child_pkey, $attach_pk_partition,
	'dump adds the child primary key before attaching the partition');

# A pre-data dump does not attach the partitions of the extension table, but
# still attaches the partitions of dumped tables.
$node->command_ok(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/pre_data.sql",
		'--section=pre-data', 'postgres'
	],
	'pg_dump --section=pre-data runs');

$dump = slurp_file("$tempdir/pre_data.sql");

ok(index($dump, $attach_partition) < 0,
	'pre-data dump does not attach the partition of the extension table');
ok(index($dump, $attach_grandchild_partition) >= 0,
	'pre-data dump attaches the partition of the dumped table');

# Restore the dump and check that each partition ends up with exactly one
# index, the dumped one, attached to the parent index.
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
		'restored', q{
		SELECT bool_and(indisvalid) FROM pg_index
		WHERE indexrelid = 'regress_pg_dump_schema.parttab_col1_col2_idx'::regclass
		   OR indexrelid IN (SELECT inhrelid FROM pg_inherits)
	}),
	't',
	'restored partitioned indexes are valid');

is( $node->safe_psql(
		'restored',
		q{SELECT obj_description('regress_pg_dump_schema.parttab_1_col1_col2_idx'::regclass)}),
	'child',
	'restored child index keeps its comment');

is( $node->safe_psql(
		'restored',
		'SELECT tableoid::regclass, * FROM regress_pg_dump_schema.parttab ORDER BY col2'),
	join("\n",
		'regress_pg_dump_schema.parttab_1|1|1',
		'regress_pg_dump_schema.parttab_2_1|1|150'),
	'restored partitions keep their data');

#########################################
# With --load-via-partition-root the data is loaded through the extension
# table, so the partitions must be attached in pre-data.  This keeps the
# duplicate child index, but must not lose the data.

$node->command_ok(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/via_root.sql",
		'--load-via-partition-root', 'postgres'
	],
	'pg_dump --load-via-partition-root runs');

$dump = slurp_file("$tempdir/via_root.sql");

dumped_before($dump, $attach_partition,
	'COPY regress_pg_dump_schema.parttab ',
	'load-via-partition-root dump attaches the partition before the data');
dumped_before($dump, $attach_sub_partition,
	'COPY regress_pg_dump_schema.parttab ',
	'load-via-partition-root dump attaches the sub-partition before the data');

$node->safe_psql('postgres', 'CREATE DATABASE via_root_restored');
$node->psql('via_root_restored', $dump, on_error_stop => 0);

is( $node->safe_psql(
		'via_root_restored',
		'SELECT tableoid::regclass, * FROM regress_pg_dump_schema.parttab ORDER BY col2'),
	join("\n",
		'regress_pg_dump_schema.parttab_1|1|1',
		'regress_pg_dump_schema.parttab_2_1|1|150'),
	'load-via-partition-root restore keeps the data');

#########################################
# Binary upgrade dump

$node->command_ok(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/binary_upgrade.sql",
		'--schema-only', '--binary-upgrade', 'postgres'
	],
	'pg_dump --binary-upgrade runs');

$dump = slurp_file("$tempdir/binary_upgrade.sql");

ok(index($dump, $create_child_index) >= 0,
	'binary upgrade dump creates the child index of the extension index');
ok(index($dump, $attach_child_index) >= 0,
	'binary upgrade dump attaches the child index of the extension index');
ok(index($dump, $create_grandchild_index) >= 0,
	'binary upgrade dump creates the grandchild index of the extension index');
ok(index($dump, $attach_grandchild_index) >= 0,
	'binary upgrade dump attaches the grandchild index of the extension index');
ok(index($dump, $add_child_pkey) >= 0,
	'binary upgrade dump adds the child primary key of the extension table');
dumped_before($dump, $attach_partition, $create_child_index,
	'binary upgrade dump attaches the partition in pre-data');

#########################################
# Renamed child indexes with comments keep their names and comments.

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

$node->safe_psql('postgres', 'CREATE DATABASE renamed_restored');
$node->command_ok(
	[
		'psql', '--no-psqlrc', '--set=ON_ERROR_STOP=1', '--quiet',
		"--file=$tempdir/renamed.sql", 'renamed_restored'
	],
	'dump of renamed child indexes restores');

is( $node->safe_psql(
		'renamed_restored', q{
		SELECT indexrelid::regclass, i.inhparent::regclass,
			obj_description(indexrelid, 'pg_class'),
			obj_description(c.oid, 'pg_constraint')
		FROM pg_index
		LEFT JOIN pg_inherits i ON i.inhrelid = indexrelid
		LEFT JOIN pg_constraint c ON c.conindid = indexrelid
		WHERE indrelid::regclass::text IN
			('regress_pg_dump_schema.parttab_1',
			 'regress_pg_dump_schema.parttab_pk_1')
		ORDER BY indrelid::regclass::text;
	}),
	join("\n",
		'regress_pg_dump_schema.parttab_1_renamed_idx|regress_pg_dump_schema.parttab_col1_col2_idx|renamed|',
		'regress_pg_dump_schema.parttab_pk_1_renamed_pkey|regress_pg_dump_schema.parttab_pk_pkey||renamed pkey'),
	'restored child indexes keep their names and comments');

#########################################
# A user-created index on the extension table is not dumped and is not
# recreated by CREATE EXTENSION.  Restoring its child index must fail loudly,
# and the partition must keep the child index.

$node->safe_psql('postgres', 'CREATE DATABASE user_index');
$node->safe_psql(
	'user_index', q{
	CREATE EXTENSION test_pg_dump;
	CREATE INDEX parttab_user_idx ON regress_pg_dump_schema.parttab (col1);
	CREATE TABLE regress_pg_dump_schema.parttab_1
		PARTITION OF regress_pg_dump_schema.parttab
		FOR VALUES FROM (1) TO (100);
});

$node->command_ok(
	[ 'pg_dump', '--no-sync', "--file=$tempdir/user_index.sql", 'user_index' ],
	'pg_dump with a user-created extension table index runs');

$dump = slurp_file("$tempdir/user_index.sql");

$node->safe_psql('postgres', 'CREATE DATABASE user_index_stop');
my ($ret, $stdout, $stderr) =
  $node->psql('user_index_stop', $dump, on_error_stop => 1);
isnt($ret, 0,
	'restore with ON_ERROR_STOP fails on the user-created parent index');
like(
	$stderr,
	qr/relation "regress_pg_dump_schema.parttab_user_idx" does not exist/,
	'restore reports the missing user-created parent index');

$node->safe_psql('postgres', 'CREATE DATABASE user_index_restored');
$node->psql('user_index_restored', $dump, on_error_stop => 0);

is( $node->safe_psql(
		'user_index_restored', q{
		SELECT indexrelid::regclass, i.inhparent::regclass
		FROM pg_index
		LEFT JOIN pg_inherits i ON i.inhrelid = indexrelid
		WHERE indrelid = 'regress_pg_dump_schema.parttab_1'::regclass
		ORDER BY indexrelid::regclass::text;
	}),
	join("\n",
		'regress_pg_dump_schema.parttab_1_col1_col2_idx|regress_pg_dump_schema.parttab_col1_col2_idx',
		'regress_pg_dump_schema.parttab_1_col1_idx|'),
	'partition keeps the child index of the user-created parent index');

#########################################
# Dumping only a partition of an ordinary table and restoring it into a
# database where the parent table and its index already exist.

$node->safe_psql('postgres', 'CREATE DATABASE plain');
$node->safe_psql('postgres', 'CREATE DATABASE plain_restored');
foreach my $db ('plain', 'plain_restored')
{
	$node->safe_psql(
		$db, q{
		CREATE TABLE p (a int, b int) PARTITION BY RANGE (b);
		CREATE UNIQUE INDEX ON p (a, b);
	});
}
$node->safe_psql('plain',
	'CREATE TABLE p_1 PARTITION OF p FOR VALUES FROM (1) TO (100)');

$node->command_ok(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/plain.sql",
		'--table=public.p_1', 'plain'
	],
	'pg_dump of a single partition runs');

$node->command_ok(
	[
		'psql', '--no-psqlrc', '--set=ON_ERROR_STOP=1', '--quiet',
		"--file=$tempdir/plain.sql", 'plain_restored'
	],
	'dump of a single partition restores into the existing parent');

is( $node->safe_psql(
		'plain_restored', q{
		SELECT indexrelid::regclass, i.inhparent::regclass
		FROM pg_index
		LEFT JOIN pg_inherits i ON i.inhrelid = indexrelid
		WHERE indrelid = 'p_1'::regclass;
	}),
	'p_1_a_b_idx|p_a_b_idx',
	'restored partition has a single index, attached to the parent index');

#########################################
# Parallel restore with a DEFAULT partition.  Attaching a partition locks the
# DEFAULT partition, so attaching the DEFAULT partition's index at the same
# time could deadlock.  The deadlock depends on timing, so restore a few
# times.

$node->safe_psql('postgres', 'CREATE DATABASE parallel');
$node->safe_psql(
	'parallel', q{
	CREATE EXTENSION test_pg_dump;
	DO $$ BEGIN FOR i IN 0..11 LOOP
		EXECUTE format('CREATE TABLE regress_pg_dump_schema.parttab_p%s
			PARTITION OF regress_pg_dump_schema.parttab
			FOR VALUES FROM (%s) TO (%s)', i, i * 1000, (i + 1) * 1000);
	END LOOP; END $$;
	CREATE TABLE regress_pg_dump_schema.parttab_def
		PARTITION OF regress_pg_dump_schema.parttab DEFAULT;
	INSERT INTO regress_pg_dump_schema.parttab
		SELECT g, g % 14000 FROM generate_series(1, 100000) g;
});

$node->command_ok(
	[
		'pg_dump', '--no-sync', '--format=directory',
		"--file=$tempdir/parallel", 'parallel'
	],
	'pg_dump --format=directory runs');

my $parallel_ok = 1;
for my $run (1 .. 5)
{
	$node->safe_psql('postgres',
		'DROP DATABASE IF EXISTS parallel_restored; CREATE DATABASE parallel_restored'
	);
	my ($stdout, $stderr);
	my $ret = IPC::Run::run(
		[
			'pg_restore', '--jobs=8', '--exit-on-error',
			'--port=' . $node->port, '--host=' . $node->host,
			'--dbname=parallel_restored', "$tempdir/parallel"
		],
		'>', \$stdout, '2>', \$stderr);
	if (!$ret)
	{
		diag("parallel restore run $run failed: $stderr");
		$parallel_ok = 0;
		last;
	}
}
ok($parallel_ok, 'parallel restore with a DEFAULT partition succeeds');

is( $node->safe_psql(
		'parallel_restored', q{
		SELECT count(*), count(DISTINCT i.inhparent), bool_and(x.indisvalid)
		FROM pg_inherits t
		JOIN pg_index x ON x.indrelid = t.inhrelid
		LEFT JOIN pg_inherits i ON i.inhrelid = x.indexrelid
		WHERE t.inhparent = 'regress_pg_dump_schema.parttab'::regclass
	}),
	'13|1|t',
	'parallel restore attaches every partition with one valid index');

is( $node->safe_psql(
		'parallel_restored',
		'SELECT count(*) FROM regress_pg_dump_schema.parttab'),
	'100000',
	'parallel restore keeps the data');

#########################################
# A partition of a parent without indexes is attached in pre-data: there is
# no index to attach, and ATTACH PARTITION would have to scan the data.

$node->safe_psql(
	'plain', q{
	CREATE TABLE q (a int, b int) PARTITION BY RANGE (b);
	CREATE TABLE q_1 PARTITION OF q FOR VALUES FROM (1) TO (100);
	INSERT INTO q VALUES (1, 1);
});

$node->command_ok(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/no_index.sql",
		'--table=public.q_1', 'plain'
	],
	'pg_dump of a partition of a parent without indexes runs');

dumped_before(
	slurp_file("$tempdir/no_index.sql"),
	'ALTER TABLE ONLY public.q ATTACH PARTITION public.q_1 ',
	'COPY public.q_1 ',
	'partition of a parent without indexes is attached before the data');

#########################################
# Hash partitioning on an enum forces loading via the partition root, so the
# partitions must be attached in pre-data even if the parent is not dumped.

foreach my $db ('plain', 'plain_restored')
{
	$node->safe_psql(
		$db, q{
		CREATE TYPE e AS ENUM ('x', 'y');
		CREATE TABLE h (a e, b int) PARTITION BY HASH (a);
		CREATE INDEX ON h (b);
	});
}
$node->safe_psql(
	'plain', q{
	CREATE TABLE h_0 PARTITION OF h FOR VALUES WITH (MODULUS 2, REMAINDER 0);
	CREATE TABLE h_1 PARTITION OF h FOR VALUES WITH (MODULUS 2, REMAINDER 1);
	INSERT INTO h VALUES ('x', 1), ('y', 2);
});

$node->command_ok(
	[
		'pg_dump', '--no-sync', "--file=$tempdir/hash_enum.sql",
		'--table=public.h_0', '--table=public.h_1', 'plain'
	],
	'pg_dump of hash partitions on an enum runs');

$dump = slurp_file("$tempdir/hash_enum.sql");

dumped_before($dump, 'ALTER TABLE ONLY public.h ATTACH PARTITION public.h_0 ',
	'COPY public.h ',
	'hash partition on an enum is attached before the data');

$node->psql('plain_restored', $dump, on_error_stop => 0);

is($node->safe_psql('plain_restored', 'SELECT a, b FROM h ORDER BY b'),
	"x|1\ny|2", 'hash partitions on an enum keep their data');

$node->stop('fast');

done_testing();
