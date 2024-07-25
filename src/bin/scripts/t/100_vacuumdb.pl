use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

program_help_ok('vacuumdb');
program_version_ok('vacuumdb');
program_options_handling_ok('vacuumdb');

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->issues_sql_like(
	[ 'vacuumdb', 'postgres' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS\);/,
	'SQL VACUUM run');
$node->issues_sql_like(
	[ 'vacuumdb', '-f', 'postgres' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, FULL\);/,
	'vacuumdb -f');
$node->issues_sql_like(
	[ 'vacuumdb', '-F', 'postgres' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, FREEZE\);/,
	'vacuumdb -F');
$node->issues_sql_like(
	[ 'vacuumdb', '-zj2', 'postgres' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, ANALYZE\).*;/,
	'vacuumdb -zj2');
$node->issues_sql_like(
	[ 'vacuumdb', '-Z', 'postgres' ],
	qr/statement: ANALYZE;/,
	'vacuumdb -Z');
$node->issues_sql_like(
	[ 'vacuumdb', '--disable-page-skipping', 'postgres' ],
	qr/statement: VACUUM \(DISABLE_PAGE_SKIPPING, SKIP_DATABASE_STATS\);/,
	'vacuumdb --disable-page-skipping');
$node->issues_sql_like(
	[ 'vacuumdb', '--skip-locked', 'postgres' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, SKIP_LOCKED\);/,
	'vacuumdb --skip-locked');
$node->issues_sql_like(
	[ 'vacuumdb', '--skip-locked', '--analyze-only', 'postgres' ],
	qr/statement: ANALYZE \(SKIP_LOCKED\);/,
	'vacuumdb --skip-locked --analyze-only');
$node->command_fails(
	[ 'vacuumdb', '--analyze-only', '--disable-page-skipping', 'postgres' ],
	'--analyze-only and --disable-page-skipping specified together');
$node->command_ok([qw(vacuumdb -Z --table=pg_am dbname=template1)],
	'vacuumdb with connection string');

$node->command_fails(
	[qw(vacuumdb -Zt pg_am;ABORT postgres)],
	'trailing command in "-t", without COLUMNS');

# Unwanted; better if it failed.
$node->command_ok(
	[qw(vacuumdb -Zt pg_am(amname);ABORT postgres)],
	'trailing command in "-t", with COLUMNS');

$node->safe_psql(
	'postgres', q|
  CREATE TABLE "need""q(uot" (")x" text);
  CREATE TABLE vactable (a int, b int);
	CREATE TABLE vactable2 (a int, b int);
  CREATE VIEW vacview AS SELECT 1 as a;

  CREATE FUNCTION f0(int) RETURNS int LANGUAGE SQL AS 'SELECT $1 * $1';
  CREATE FUNCTION f1(int) RETURNS int LANGUAGE SQL AS 'SELECT f0($1)';
  CREATE TABLE funcidx (x int);
  INSERT INTO funcidx VALUES (0),(1),(2),(3);
  CREATE INDEX i0 ON funcidx ((f1(x)));
|);
$node->command_ok([qw|vacuumdb -Z --table="need""q(uot"(")x") postgres|],
	'column list');
$node->command_fails(
	[qw|vacuumdb -Zt funcidx postgres|],
	'unqualifed name via functional index');

$node->command_fails(
	[ 'vacuumdb', '--analyze', '--table', 'vactable(c)', 'postgres' ],
	'incorrect column name with ANALYZE');
$node->issues_sql_like(
	[ 'vacuumdb', '--analyze', '--table', 'vactable(a, b)', 'postgres' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, ANALYZE\) public.vactable\(a, b\);/,
	'vacuumdb --analyze with complete column list');
$node->issues_sql_like(
	[ 'vacuumdb', '--analyze-only', '--table', 'vactable(b)', 'postgres' ],
	qr/statement: ANALYZE public.vactable\(b\);/,
	'vacuumdb --analyze-only with partial column list');
$node->command_checks_all(
	[ 'vacuumdb', '--analyze', '--table', 'vacview', 'postgres' ],
	0,
	[qr/^.*vacuuming database "postgres"/],
	[qr/^WARNING.*cannot vacuum non-tables or special system tables/s],
	'vacuumdb with view');
$node->command_fails(
	[ 'vacuumdb', '--table', 'vactable', '--min-mxid-age', '0', 'postgres' ],
	'vacuumdb --min-mxid-age with incorrect value');
$node->command_fails(
	[ 'vacuumdb', '--table', 'vactable', '--min-xid-age', '0', 'postgres' ],
	'vacuumdb --min-xid-age with incorrect value');
$node->issues_sql_like(
	[
		'vacuumdb',   '--table', 'vactable', '--min-mxid-age',
		'2147483000', 'postgres'
	],
	qr/GREATEST.*relminmxid.*2147483000/,
	'vacuumdb --table --min-mxid-age');
$node->issues_sql_like(
	[ 'vacuumdb', '--min-xid-age', '2147483001', 'postgres' ],
	qr/GREATEST.*relfrozenxid.*2147483001/,
	'vacuumdb --table --min-xid-age');
$node->issues_sql_like(
	[ 'vacuumdb', '--table', 'vactable', '--table', 'vactable2', 'postgres' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS\) public.vactable, public.vactable2;/,
	'vacuumdb with two tables');
$node->issues_sql_like(
	[ 'vacuumdb', '-j2', 'postgres' ],
	qr/statement:\ VACUUM\ .*;
								.*statement:\ VACUUM\ /sx,
	'vacuumdb -j2 issues two vacuum commands');
$node->issues_sql_like(
	[ 'vacuumdb', '-j4', 'postgres' ],
	qr/statement:\ VACUUM\ .*;
								.*statement:\ VACUUM\ .*;
								.*statement:\ VACUUM\ .*;
								.*statement:\ VACUUM\ .*/sx,
	'vacuumdb -j4 issues four vacuum commands');
$node->issues_sql_like(
	[ 'vacuumdb', '-j4', '--table', 'vactable', '--table', 'vactable2', 'postgres' ],
	qr/statement:\ VACUUM\ .*;
								.*statement:\ VACUUM\ /sx,
	'vacuumdb -j4 with two tables issues two vacuum commands');
$node->safe_psql(
	'postgres', q|
  CREATE TABLE vactable3 (a int, b int);
	CREATE TABLE vactable4 (a int, b int);
	CREATE TABLE vactable5 (a int, b int);
	CREATE TABLE vactable6 (a int, b int);
	CREATE TABLE vactable7 (a int, b int);
	CREATE TABLE vactable8 (a int, b int);
	CREATE TABLE vactable9 (a int, b int);
	CREATE TABLE vactable10 (a int, b int);
|);
$node->issues_sql_like(
	[ 'vacuumdb', '-j4',
	'--table', 'vactable', '--table', 'vactable2',
	'--table', 'vactable3', '--table', 'vactable4',
	'--table', 'vactable5', '--table', 'vactable6',
	'--table', 'vactable7', '--table', 'vactable8',
	'--table', 'vactable9', '--table', 'vactable10', 'postgres' ],
	qr/statement:\ VACUUM\ \(SKIP_DATABASE_STATS\)\ .*;
								.*statement:\ VACUUM\ \(SKIP_DATABASE_STATS\)\ .*;
								.*statement:\ VACUUM\ \(SKIP_DATABASE_STATS\)\ .*;
								.*statement:\ VACUUM\ \(SKIP_DATABASE_STATS\)\ .*;
								.*statement:\ VACUUM\ \(SKIP_DATABASE_STATS\)\ .*;/sx,
	'vacuumdb -j4 with ten tables issues five vacuum commands');

done_testing();
