
# Test that password history mutations done on the primary are shipped
# through the credcheck custom WAL resource manager and applied on a
# streaming standby, and that the history survives a standby promotion.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Set up primary
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'credcheck'
credcheck.password_reuse_history = 3
credcheck.password_min_length = 1
credcheck.password_min_lower = 0
credcheck.password_min_upper = 0
credcheck.password_min_digit = 0
credcheck.password_min_special = 0
credcheck.password_contain_username = false
});
$primary->start;
$primary->safe_psql('postgres', 'CREATE EXTENSION credcheck');

# Set up standby
$primary->backup('bk1');
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, 'bk1', has_streaming => 1);
# credcheck must be preloaded on the standby as well, so that its custom
# rmgr is registered before recovery starts replaying credcheck records.
$standby->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'credcheck'
credcheck.password_reuse_history = 3
});
$standby->start;

# Confirm the custom rmgr is registered on both nodes.
is( $primary->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_get_wal_resource_managers() WHERE rm_name = 'credcheck'"
	),
	'1',
	'credcheck rmgr is registered on the primary');
is( $standby->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_get_wal_resource_managers() WHERE rm_name = 'credcheck'"
	),
	'1',
	'credcheck rmgr is registered on the standby');

# Create users on the primary
$primary->safe_psql('postgres',
	q{CREATE ROLE alice LOGIN PASSWORD 'pwd_alice_1'});
$primary->safe_psql('postgres',
	q{CREATE ROLE bob   LOGIN PASSWORD 'pwd_bob_1'});

# Generate a few password updates so the history contains multiple entries.
$primary->safe_psql('postgres',
	q{ALTER ROLE alice PASSWORD 'pwd_alice_2'});
$primary->safe_psql('postgres',
	q{ALTER ROLE alice PASSWORD 'pwd_alice_3'});

$primary->wait_for_catchup($standby);

# Verify history content on both nodes
my $sql_count =
  q{SELECT rolename, count(*) FROM pg_password_history
    GROUP BY rolename ORDER BY rolename};

my $primary_state = $primary->safe_psql('postgres', $sql_count);
my $standby_state = $standby->safe_psql('postgres', $sql_count);

is($standby_state, $primary_state,
	'password history on standby matches the primary after WAL replay');
like($primary_state, qr/^alice\|3\nbob\|1$/m,
	'primary contains the expected number of history entries');

# Rename a role
$primary->safe_psql('postgres', q{ALTER ROLE bob RENAME TO bobby});
$primary->wait_for_catchup($standby);

is( $standby->safe_psql('postgres',
		q{SELECT count(*) FROM pg_password_history WHERE rolename = 'bobby'}),
	'1',
	'rename is replicated to the standby');
is( $standby->safe_psql('postgres',
		q{SELECT count(*) FROM pg_password_history WHERE rolename = 'bob'}),
	'0',
	'old name is gone from the standby history');

# Drop a role
$primary->safe_psql('postgres', q{DROP ROLE bobby});
$primary->wait_for_catchup($standby);

is( $standby->safe_psql('postgres',
		q{SELECT count(*) FROM pg_password_history WHERE rolename = 'bobby'}),
	'0',
	'DROP ROLE removes user history on the standby');

# Explicit reset
$primary->safe_psql('postgres',
	q{SELECT pg_password_history_reset('alice')});
$primary->wait_for_catchup($standby);

is( $standby->safe_psql('postgres',
		q{SELECT count(*) FROM pg_password_history}),
	'0',
	'pg_password_history_reset() is replicated to the standby');

# Restart standby and re-check persistence: push another change, then
# restart the standby so we exercise loading pg_password_history from
# the file written by redo.
$primary->safe_psql('postgres',
	q{CREATE ROLE carol LOGIN PASSWORD 'pwd_carol_1'});
$primary->wait_for_catchup($standby);

$standby->restart;

is( $standby->safe_psql('postgres',
		q{SELECT count(*) FROM pg_password_history WHERE rolename = 'carol'}),
	'1',
	'history file persisted by redo is reloaded on standby restart');

# Promote the standby and check that the policy is still enforced.
# One last password change on the primary so the promoted node has two
# entries for carol to verify against.
$primary->safe_psql('postgres',
	q{ALTER ROLE carol PASSWORD 'pwd_carol_2'});
$primary->wait_for_catchup($standby);

$primary->stop;
$standby->promote;

$standby->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()');

# carol has two entries replicated from the old primary:
# pwd_carol_1 (CREATE ROLE) and pwd_carol_2 (ALTER ROLE).
is( $standby->safe_psql('postgres',
		q{SELECT count(*) FROM pg_password_history WHERE rolename = 'carol'}),
	'2',
	'promoted standby retains full history replicated from the old primary');

# pwd_carol_1 is still in the history window, so reusing it must be rejected.
my ($ret, $stdout, $stderr) = $standby->psql('postgres',
	q{ALTER ROLE carol PASSWORD 'pwd_carol_1'});
isnt($ret, 0,
	'promoted standby rejects a password that is in the replicated history');
like($stderr, qr/cannot use this credential following the password reuse policy/i,
	'error message mentions reuse restriction');

# An unused password must be accepted.
$standby->safe_psql('postgres',
	q{ALTER ROLE carol PASSWORD 'pwd_carol_new'});

is( $standby->safe_psql('postgres',
		q{SELECT count(*) FROM pg_password_history WHERE rolename = 'carol'}),
	'3',
	'new password is stored in history on the promoted standby');

$standby->stop;

done_testing();
