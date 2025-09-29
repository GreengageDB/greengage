use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->issues_sql_like(
	[ 'vacuumdb', '-a' ],
	qr/statement: VACUUM.*statement: VACUUM/s,
	'vacuum all databases');

$node->safe_psql(
	'postgres', q(
	CREATE DATABASE regression_invalid;
	SET allow_system_table_mods = on;
	UPDATE pg_database SET datconnlimit = -2 WHERE datname = 'regression_invalid';
	RESET allow_system_table_mods;
));
$node->command_ok([ 'vacuumdb', '-a' ],
  'invalid database not targeted by vacuumdb -a');

# Doesn't quite belong here, but don't want to waste time by creating an
# invalid database in 010_vacuumdb.pl as well.
$node->command_fails([ 'vacuumdb', '-d', 'regression_invalid'],
  'vacuumdb cannot target invalid database');

done_testing();
