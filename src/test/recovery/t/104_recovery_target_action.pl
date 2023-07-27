# Check that backported recovery_target_action feature works
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 7;

# Initialize node to backup
my $node_to_backup = get_new_node('to_backup');
$node_to_backup->init(
	has_archiving    => 1);
$node_to_backup->start;

# Take backup
my $backup_name = 'my_backup';
$node_to_backup->backup_fs_hot($backup_name);

# Create first restore point with some data
$node_to_backup->safe_psql('postgres', "CREATE TABLE table1 AS SELECT generate_series(1, 10) AS a1;");
$node_to_backup->safe_psql('postgres', "CHECKPOINT;");
$node_to_backup->safe_psql('postgres', "SELECT pg_create_restore_point('first_restore_point');");
$node_to_backup->safe_psql('postgres', "SELECT pg_switch_xlog();");

# Create second restore point with some data
$node_to_backup->safe_psql('postgres', "CREATE TABLE table2 AS SELECT generate_series(1, 20) AS a2;");
$node_to_backup->safe_psql('postgres', "CHECKPOINT;");
$node_to_backup->safe_psql('postgres', "SELECT pg_create_restore_point('second_restore_point');");
$node_to_backup->safe_psql('postgres', "SELECT pg_switch_xlog();");

# Create new node from from backup
my $node_restored = get_new_node('restored');
my $delay         = 5;
$node_restored->init_from_backup($node_to_backup, $backup_name,
	has_restoring => 1);

## Test 1: recovery_target_action=shutdown works
$node_restored->append_conf(
	'recovery.conf', qq(
recovery_target_name = 'first_restore_point'
recovery_target_action = 'shutdown'
pause_at_recovery_target = false # deprecated but should not FATAL if provided
));
$node_restored->start(fail_ok => 1); # non-zero return code since pg_ctl -w expects db to be up

# We should see that recovery stopped at the first restore point and
# that the database is no longer up.
command_ok(["grep", 'recovery stopping at restore point "first_restore_point"', $node_restored->logfile],
	"recovered up to first_restore_point");
ok(!-f $node_restored->data_dir . "/postmaster.pid", "recovery_target_action=shutdown was successful");

## Test 2: recovery_target_action=promote works
command_ok(["echo", "''", ">", $node_restored->data_dir . "/recovery.conf"],
	"truncate recovery.conf to reset for next test");
$node_restored->append_conf(
	'recovery.conf', qq(
recovery_target_name = 'second_restore_point'
recovery_target_action = 'promote'
));
$node_restored->start;

# We should see that recovery stopped at the second restore point and
# that the database has been promoted and is now queryable.
command_ok(["grep", 'recovery stopping at restore point "second_restore_point"', $node_restored->logfile],
	"recovered up to second_restore_point");
ok(-f $node_restored->data_dir . "/postmaster.pid", "recovery_target_action=promote was successful");
my $result1 = $node_restored->safe_psql('postgres', "SELECT count(*) FROM table1;");
is($result1, qq(10), 'check table table1 after promote');
my $result2 = $node_restored->safe_psql('postgres', "SELECT count(*) FROM table2;");
is($result2, qq(20), 'check table table2 after promote');
