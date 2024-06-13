#!/bin/bash

# This file has the .sql extension, but it is actually launched as a shell
# script. This contortion is necessary because pg_regress normally uses
# psql to run the input scripts, and requires them to have the .sql
# extension, but we use a custom launcher script that runs the scripts using
# a shell instead.

TESTNAME=empty_conf

. sql/config_test.sh

# pg_rewind should handle empty (or even removed) postgres.conf
# gp_dbid is taken from internal.auto.conf
function after_promotion
{
cp $TEST_MASTER/postgresql.conf $TESTROOT/master-postgresql-full.conf.tmp
echo "" > $TEST_MASTER/postgresql.conf
}

# Move postgresql.conf back and start master instance
# pg_rewind is already finished
function before_master_restart_after_rewind
{
mv $TESTROOT/master-postgresql-full.conf.tmp $TEST_MASTER/postgresql.conf
}

# Run the test
. sql/run_test.sh
