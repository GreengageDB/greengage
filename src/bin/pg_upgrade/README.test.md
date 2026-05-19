# How to run tests

## Overview

pg_upgrade has several greengage-specific testing files. These include:

- `test_gpdb.sh` - the core script to perform and test the upgrade, which does all the heavy lifting. It is directly run from the regression test suite via `make check` recipe, but only between the same version (e.g from Greengage7 to Greengage7).
- `pg_upgrade_run_6X_to_7X_migration.sh` - small wrapper to run the above test between Greengage6 and Greengage7.
- `Dockerfile.pg_upgrade` - creates an image intended to run the above tests on the CI.

Here it is shown how to run pg_upgrade tests using `pg_upgrade_run_6X_to_7X_migration.sh`. `test_gpdb.sh` can be run by manually following steps in the `pg_upgrade_run_6X_to_7X_migration.sh`.

## Running tests locally

To run `pg_upgrade_run_6X_to_7X_migration.sh` locally, it is necessary to specify the following environment variables:
- GREENGAGE6_SRC - directory of the Greengage6 source code.
- GREENGAGE7_SRC - directory of the Greengage7 source code.
- GREENGAGE6_INSTALLATION - directory where the Greengage6 installation is located (i.e., after make install).
- GREENGAGE7_INSTALLATION - directory where the Greengage7 installation is located (i.e., after make install).
- SQL_SCHEMA - optional path to an sql file that will be loaded before performing the upgrade. If it is not specified,
  the upgrade will be performed on an empty cluster.

For example:
```bash
export GREENGAGE6_SRC=/home/gpadmin/ggdb6_src
export GREENGAGE7_SRC=/home/gpadmin/gpdb7_src
export GREENGAGE6_INSTALLATION=/usr/local/greengage-db-6X
export GREENGAGE7_INSTALLATION=/usr/local/greengage-db-7X
export SQL_SCHEMA=/home/gpadmin/dump.sql
bash   ${GREENGAGE7_SRC}/src/bin/pg_upgrade/pg_upgrade_run_6X_to_7X_migration.sh
```

## Running tests from the image

Docker image already has all the necessary environment variables set to run the test WITHOUT a schema. The only thing left is to setup gpadmin user:
```bash
docker build -f ci/Dockerfile.pg_upgrade -t gpdb7_pgupgrade:latest .
docker run --rm gpdb7_pgupgrade bash -c \
    "gpdb_src/concourse/scripts/setup_gpadmin_user.bash && su gpadmin /home/gpadmin/gpdb_src/src/bin/pg_upgrade/pg_upgrade_run_6X_to_7X_migration.sh"
```

To specify a schema, mount a directory with it into the docker image:
```bash
mkdir dump_dir
echo "create database test2;" > dump_dir/dump.sql
docker run --rm \
	-v $(pwd)/dump_dir:/dump_dir \
	gpdb7_pgupgrade bash -c \
	"export SQL_SCHEMA=/dump_dir/dump.sql; \
	 gpdb_src/concourse/scripts/setup_gpadmin_user.bash; \
	 su gpadmin /home/gpadmin/gpdb_src/src/bin/pg_upgrade/pg_upgrade_run_6X_to_7X_migration.sh;"
```

To collect execution logs, mount an additional volume:
```bash
mkdir logs
docker run --rm \
	-v $(pwd)/dump_dir:/dump_dir \
	-v $(pwd)/logs:/logs \
	gpdb7_pgupgrade bash -c \
	"export SQL_SCHEMA=/dump_dir/dump.sql; \
	 gpdb_src/concourse/scripts/setup_gpadmin_user.bash; \
	 su gpadmin /home/gpadmin/gpdb_src/src/bin/pg_upgrade/pg_upgrade_run_6X_to_7X_migration.sh; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/tmp_check/dump1.sql /logs/dump1.sql; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/tmp_check/dump2.sql /logs/dump2.sql; \
	 cp /home/gpadmin/gpdb_src/src/bin/pg_upgrade/regression.diffs /logs/regression.diffs;"
```
