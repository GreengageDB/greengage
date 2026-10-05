
# Regression test for the "lastlog" feature: a utmp/wtmp-like login history
# persisted in a memory-mapped file. Exercises the pieces that depend on the
# server life cycle (and therefore cannot be checked with pg_regress):
#   - the boot marker written at startup;
#   - user sessions recorded at connection / finalized at disconnect;
#   - capture of the last executed query (credcheck.lastlog_track_query);
#   - the "still connected" live state for an open session;
#   - persistence of the history across a restart (mmap file reload);
#   - a clean restart must NOT produce a false crash marker, but must leave
#     a shutdown marker;
#   - an unclean stop (immediate) must be detected as a crash at next boot;
#   - the pg_lastlog view is readable by pg_read_all_stats but not PUBLIC.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# background_psql() (used for the "still connected" check) is available with
# a stable API from PostgreSQL 15 on.
my ($pg_major) = (`pg_config --version` =~ /(\d+)/);
plan skip_all => "this test requires PostgreSQL 15 or later"
  if $pg_major < 15;

my $node = PostgreSQL::Test::Cluster->new('lastlog');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'credcheck'
credcheck.lastlog = on
credcheck.lastlog_max = 128
credcheck.lastlog_track_query = on
credcheck.lastlog_flush_interval = 1
# keep the username/password policy out of the way of role creation
credcheck.password_min_length = 1
credcheck.password_min_lower = 0
credcheck.password_min_upper = 0
credcheck.password_min_digit = 0
credcheck.password_min_special = 0
credcheck.password_contain_username = false
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION credcheck');

# Small helper: count rows of pg_lastlog matching an optional WHERE clause.
sub ll_count
{
	my ($where) = @_;
	my $q = 'SELECT count(*) FROM pg_lastlog';
	$q .= " WHERE $where" if defined $where;
	return $node->safe_psql('postgres', $q);
}

# ---------------------------------------------------------------------------
# 1. The boot marker is written at startup.
# ---------------------------------------------------------------------------
ok(ll_count("type = 'boot'") >= 1, 'a boot marker is recorded at startup');

# ---------------------------------------------------------------------------
# 2. A user session is recorded and finalized at disconnect.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE ROLE lluser LOGIN');

# connect as lluser, run a distinctive query, then disconnect
$node->safe_psql('postgres', "SELECT 'llprobe_marker_42'",
	extra_params => [ '--username', 'lluser' ]);

# the record is written during the backend's proc_exit, which races with
# psql returning: poll until it shows up
$node->poll_query_until('postgres',
	"SELECT count(*) > 0 FROM pg_lastlog "
  . "WHERE type = 'user' AND username = 'lluser' AND state = 'disconnected'",
	't')
  or die "timed out waiting for the lluser session to be finalized";

is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_lastlog "
	  . "WHERE type = 'user' AND username = 'lluser' AND state = 'disconnected'"
	) >= 1 ? 1 : 0,
	1,
	'a finished user session is recorded as disconnected');

# ---------------------------------------------------------------------------
# 3. The last executed query is captured (lastlog_track_query = on).
# ---------------------------------------------------------------------------
is( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_lastlog "
	  . "WHERE username = 'lluser' AND query LIKE '%llprobe_marker_42%'"
	) >= 1 ? 1 : 0,
	1,
	'the last executed query of a session is captured');

# ---------------------------------------------------------------------------
# 4. login_time and duration are populated for a finished session.
# ---------------------------------------------------------------------------
is( $node->safe_psql(
		'postgres',
		"SELECT bool_and(login_time IS NOT NULL AND logout_time IS NOT NULL "
	  . "AND duration IS NOT NULL) FROM pg_lastlog "
	  . "WHERE username = 'lluser' AND state = 'disconnected'"
	),
	't',
	'finished sessions carry login_time, logout_time and duration');

# ---------------------------------------------------------------------------
# 5. An open session shows up as "still connected", then flips to
#    "disconnected" once it is closed.
# ---------------------------------------------------------------------------
my $bg = $node->background_psql('postgres', on_error_stop => 1);
my $bgpid = $bg->query_safe('SELECT pg_backend_pid()');
$bgpid =~ s/\D//g;

ok( $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_lastlog "
	  . "WHERE pid = $bgpid AND state = 'still connected'") >= 1,
	'an open session is reported as still connected');

$bg->quit;

$node->poll_query_until('postgres',
	"SELECT count(*) > 0 FROM pg_lastlog "
  . "WHERE pid = $bgpid AND state = 'disconnected'",
	't')
  or die "timed out waiting for the background session to disconnect";
pass('a closed session flips to disconnected');

# ---------------------------------------------------------------------------
# 6. A clean restart preserves the history (mmap reload), writes a shutdown
#    marker, and does NOT create a false crash marker.
# ---------------------------------------------------------------------------
my $users_before  = ll_count("username = 'lluser'");
my $crashes_before = ll_count("type = 'crash'");

$node->restart;

ok(ll_count("type = 'shutdown'") >= 1,
	'a shutdown marker is recorded on clean stop');
is(ll_count("type = 'crash'"), $crashes_before,
	'a clean restart does not create a crash marker');
ok(ll_count("username = 'lluser'") >= $users_before,
	'history survives a restart (memory-mapped file reloaded)');

# a fresh boot marker must have been added by the restart
ok(ll_count("type = 'boot'") >= 2, 'the restart adds a new boot marker');

# ---------------------------------------------------------------------------
# 7. An unclean stop is detected as a crash at the next startup.
# ---------------------------------------------------------------------------
my $crashes_pre = ll_count("type = 'crash'");
$node->stop('immediate');
$node->start;

ok(ll_count("type = 'crash'") > $crashes_pre,
	'an unclean (immediate) stop is detected as a crash at next boot');

# ---------------------------------------------------------------------------
# 8. Access control: pg_lastlog is readable by pg_read_all_stats members but
#    not by an unprivileged role.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE ROLE ll_nopriv LOGIN');
$node->safe_psql('postgres', 'GRANT EXECUTE ON FUNCTION credcheck_lastlog TO pg_read_all_stats');
$node->safe_psql('postgres', 'CREATE ROLE ll_priv LOGIN IN ROLE pg_read_all_stats');

my ($rc_no, $out_no, $err_no) = $node->psql(
	'postgres', 'SELECT count(*) FROM pg_lastlog',
	extra_params => [ '--username', 'll_nopriv' ]);
isnt($rc_no, 0, 'an unprivileged role cannot read pg_lastlog');
like($err_no, qr/permission denied/, 'the denial mentions permission denied');

my ($rc_yes, $out_yes, $err_yes) = $node->psql(
	'postgres', 'SELECT count(*) FROM pg_lastlog',
	extra_params => [ '--username', 'll_priv' ]);
is($rc_yes, 0, 'a pg_read_all_stats member can read pg_lastlog');

$node->stop;

done_testing();
