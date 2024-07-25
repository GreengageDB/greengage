use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

program_help_ok('reindexdb');
program_version_ok('reindexdb');
program_options_handling_ok('reindexdb');

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

$ENV{PGOPTIONS} = '-c gp_session_role=utility --client-min-messages=WARNING';

$node->issues_sql_like(
	[ 'reindexdb', 'postgres' ],
	qr/statement: REINDEX DATABASE postgres;/,
	'SQL REINDEX run');

$node->safe_psql('postgres',
	'CREATE TABLE test1 (a int); CREATE INDEX test1x ON test1 (a);');
$node->issues_sql_like(
	[ 'reindexdb', '-t', 'test1', 'postgres' ],
	qr/statement: REINDEX TABLE public\.test1;/,
	'reindex specific table');
$node->issues_sql_like(
	[ 'reindexdb', '-i', 'test1x', 'postgres' ],
	qr/statement: REINDEX INDEX public\.test1x;/,
	'reindex specific index');
$node->issues_sql_like(
	[ 'reindexdb', '-S', 'pg_catalog', 'postgres' ],
	qr/statement: REINDEX SCHEMA pg_catalog;/,
	'reindex specific schema');
$node->issues_sql_like(
	[ 'reindexdb', '-s', 'postgres' ],
	qr/statement: REINDEX SYSTEM postgres;/,
	'reindex system tables');
$node->issues_sql_like(
	[ 'reindexdb', '-v', '-t', 'test1', 'postgres' ],
	qr/statement: REINDEX \(VERBOSE\) TABLE public\.test1;/,
	'reindex with verbose output');

# the same with --concurrently
# GPDB: REINDEX CONCURRENTLY doesn't work on GPDB, skip.
SKIP: {
	skip "REINDEX CONCURRENTLY not implemented on GPDB", 9;

$node->issues_sql_like(
	[ 'reindexdb', '--concurrently', 'postgres' ],
	qr/statement: REINDEX DATABASE CONCURRENTLY postgres;/,
	'SQL REINDEX CONCURRENTLY run');

$node->issues_sql_like(
	[ 'reindexdb', '--concurrently', '-t', 'test1', 'postgres' ],
	qr/statement: REINDEX TABLE CONCURRENTLY public\.test1;/,
	'reindex specific table concurrently');
$node->issues_sql_like(
	[ 'reindexdb', '--concurrently', '-i', 'test1x', 'postgres' ],
	qr/statement: REINDEX INDEX CONCURRENTLY public\.test1x;/,
	'reindex specific index concurrently');
$node->issues_sql_like(
	[ 'reindexdb', '--concurrently', '-S', 'public', 'postgres' ],
	qr/statement: REINDEX SCHEMA CONCURRENTLY public;/,
	'reindex specific schema concurrently');
$node->command_fails([ 'reindexdb', '--concurrently', '-s', 'postgres' ],
	'reindex system tables concurrently');
$node->issues_sql_like(
	[ 'reindexdb', '--concurrently', '-v', '-t', 'test1', 'postgres' ],
	qr/statement: REINDEX \(VERBOSE\) TABLE CONCURRENTLY public\.test1;/,
	'reindex with verbose output concurrently');
} # end SKIP

# connection strings
$node->command_ok([qw(reindexdb --echo --table=pg_am dbname=template1)],
	'reindexdb table with connection string');
$node->command_ok(
	[qw(reindexdb --echo dbname=template1)],
	'reindexdb database with connection string');
$node->command_ok(
	[qw(reindexdb --echo --system dbname=template1)],
	'reindexdb system with connection string');

done_testing();