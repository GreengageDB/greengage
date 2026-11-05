Steps to check Greengage in High Availability mode with Patroni.

In greengage root:
```
docker build -t gpdb7_u22:latest -f ci/Dockerfile.ubuntu .

export IMAGE=gpdb7_u22:latest
export PATRONI_DIR=<path to src root of Patroni with Greengage support>

./ci/patroni/scripts/run_patroni_demo.sh
```
In container:
```
export PATH=$PATH:/home/gpadmin/.local/bin/

# Init the cluster in non-HA mode as 1st step
. ./gpdb_src/ci/patroni/scripts/bringup_cluster.sh

#check initial cluster is workable
psql postgres -c "select * from gp_segment_configuration;"


psql -v ON_ERROR_STOP=1 postgres <<'SQL'
CREATE USER patroni WITH SUPERUSER ENCRYPTED PASSWORD 'patroni_pass';
-- replication user
CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';
-- pg_basebackup of Greengage runs `SHOW log_directory` on the source server
GRANT pg_read_all_settings TO replicator;
-- pg_rewind user (optional, but recommended)
CREATE USER rewind_user WITH ENCRYPTED PASSWORD 'rewind_pass';
GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_dir(text, boolean, boolean) TO rewind_user;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_stat_file(text, boolean) TO rewind_user;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text) TO rewind_user;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint, boolean) TO rewind_user;
GRANT pg_read_all_settings TO rewind_user;
SQL

# Needed: Patroni connects as `patroni` to the already running coordinators
# before it rewrites pg_hba.conf from the yaml, and exits if it is rejected.
echo 'host    all     patroni 127.0.0.1/32    md5' >> $COORDINATOR_DATA_DIRECTORY/pg_hba.conf
ssh scdw 'echo "host    all     patroni 127.0.0.1/32    md5" >> /home/gpadmin/.data/standby/pg_hba.conf'

gpconfig -c hot_standby -v on
# Stop our non-HA cluster
gpstop -a

# Now start our cluster in HA-mode
gpstart --HA --config-file-coordinator /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml --config-file-standby /home/gpadmin/gpdb_src/ci/patroni/configs/smdw.yml

# We can check Patroni status via
patronictl -c /home/gpadmin/gpdb_src/ci/patroni/configs/mdw.yml list

# And we can stop cluster
gpstop --HA
```

To run behave test in container (be sure to remove `@skip` tag before running):
```
cd ~/gpdb_src/
./configure
make -C ./gpMgmt -f Makefile.behave behave tags=auto_failover
```
