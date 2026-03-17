# Test for sasl authentication in backend-ed libpq
use strict;
use warnings;
use File::Path qw(rmtree);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

$ENV{PGDATABASE} = 'postgres';

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);

# Set scram-sha-256 password to user
$node_primary->safe_psql(
	'postgres',
	"SET password_encryption='scram-sha-256';
SET client_encoding='utf8';
ALTER ROLE gpadmin PASSWORD 'gpadmin';
");

# Delete pg_hba.conf from the primary node, add a new entries to it
# and then execute a reload to refresh it.
unlink($node_primary->data_dir . '/pg_hba.conf');
$node_primary->append_conf('pg_hba.conf', "local all all scram-sha-256");
$node_primary->append_conf('pg_hba.conf', "local replication all scram-sha-256");
$node_primary->reload;

# Create standby linking to it
my $node_standby = PostgreSQL::Test::Cluster->new('standby');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);

# Use invalid password
$ENV{"PGPASSWORD"} = 'password';
$node_standby->start;

my $logfile = slurp_file($node_standby->logfile());
ok($logfile =~ qr/FATAL:  password authentication failed for user "gpadmin"/,
	'password authentication failed');

done_testing();
