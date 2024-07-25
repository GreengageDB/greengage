use strict;
use warnings;
use PostgreSQL::Test::Utils;
use Test::More tests => 3;

use FindBin;
use lib $FindBin::RealBin;

use RewindTest;

# Test that running pg_rewind if the two clusters are on the same
# timeline runs successfully.

RewindTest::setup_cluster();
RewindTest::start_primary();
RewindTest::create_standby();

local $ENV{"SUSPEND_PG_REWIND"} = '10';
my $pg_rewind_start_time = time();
RewindTest::run_pg_rewind('local');
ok(time() - $pg_rewind_start_time >= 10,
	'pg_rewind delay');

my $logfile = slurp_file("${PostgreSQL::Test::Utils::tmp_check}/log/regress_log_005_same_timeline");
ok($logfile =~ qr/pg_rewind suspended for 10 seconds/,
	'check for suspended pg_rewind log');

RewindTest::clean_rewind_test();

done_testing();
