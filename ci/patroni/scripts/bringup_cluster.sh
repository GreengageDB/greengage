#!/usr/bin/env bash

export COORDINATOR_DATA_DIRECTORY=/home/gpadmin/.data/gpseg-1
export PGPORT=7000
source /usr/local/greengage-db-devel/greengage_path.sh

mkdir -p /home/gpadmin/.data
mkdir -p /home/gpadmin/gpAdminLogs

DATADIRS_PRIM=/home/gpadmin/.data/primary
DATADIRS_MIR=/home/gpadmin/.data/mirror

cat > /home/gpadmin/gpdb_src/gpAux/gpdemo/hostfile <<-EOF
sdw1 sdw2 sdw3
	EOF

SEG_HOSTS=$(cat /home/gpadmin/gpdb_src/gpAux/gpdemo/hostfile)
for SEG_HOST in ${SEG_HOSTS}; do
	echo "====================   HOST ${SEG_HOST} ====================";
	echo "Creating ${DATADIRS_PRIM}"
	ssh $SEG_HOST mkdir -p $DATADIRS_PRIM
	echo "Creating ${DATADIRS_MIR}"
	ssh $SEG_HOST mkdir -p $DATADIRS_MIR
done

cat > /home/gpadmin/gpdb_src/gpAux/gpdemo/inputConfigFile <<-EOF

TRUSTED_SHELL=ssh
ENCODING=UNICODE
SEG_PREFIX=demoDataDir
HEAP_CHECKSUM=on
HBA_HOSTNAMES=0
QD_PRIMARY_ARRAY=cdw~cdw~7000~/home/gpadmin/.data/gpseg-1~1~-1~0
declare -a PRIMARY_ARRAY=(
sdw1~sdw1~10100~/home/gpadmin/.data/primary/gpseg0~2~0
sdw2~sdw2~10110~/home/gpadmin/.data/primary/gpseg1~3~1
sdw3~sdw3~10120~/home/gpadmin/.data/primary/gpseg2~4~2
)
declare -a MIRROR_ARRAY=(
sdw2~sdw2~50130~/home/gpadmin/.data/mirror/gpseg0~5~0
sdw3~sdw3~50140~/home/gpadmin/.data/mirror/gpseg1~6~1
sdw1~sdw1~50150~/home/gpadmin/.data/mirror/gpseg2~7~2
)

	EOF

cat > /home/gpadmin/gpdb_src/gpAux/gpdemo/clusterConfigPostgresAddonsFile <<-EOF
statement_mem = 250MB
fsync=off
	EOF

ssh scdw mkdir -p /home/gpadmin/.data
ssh scdw mkdir -p /home/gpadmin/gpAdminLogs

gpinitsystem -a -I /home/gpadmin/gpdb_src/gpAux/gpdemo/inputConfigFile \
    -l /home/gpadmin/gpAdminLogs -p /home/gpadmin/gpdb_src/gpAux/gpdemo/clusterConfigPostgresAddonsFile \
    -s scdw -P 7001 -S /home/gpadmin/.data/standby ""

