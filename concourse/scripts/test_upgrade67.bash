#!/bin/bash

set -ux

CWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

MASTER_HOST=localhost
OLD_GPHOME=/home/gpadmin/gpdb6
NEW_GPHOME=/usr/local/greengage-db-devel
DATADIR_PREFIX=/home/gpadmin/gpdb7data
OLD_DATADIR_PREFIX=/home/gpadmin/gpdb6data
OLD_COORDINATOR_DATA_DIRECTORY=/home/gpadmin/gpdb6data/qddir/demoDataDir-1
NEW_COORDINATOR_DATA_DIRECTORY=/home/gpadmin/gpdb7data/qddir/demoDataDir-1
OLD_GPDB_SRC_PATH=/home/gpadmin/gpdb_src6
NEW_GPDB_SRC_PATH=/home/gpadmin/gpdb_src
OLD_DATABASE=${CWDIR}/test_upgrade67.sql
CHECK_NEW_DATABASE=${CWDIR}/test_upgrade67_check.sql

create_old_cluster() {

    # create cluster version 6
su - gpadmin -c bash -- -e <<EOF
    source ${OLD_GPHOME}/greengage_path.sh    
    export LANG=en_US.UTF-8

    cd ${OLD_GPDB_SRC_PATH}
    export DATADIRS=${OLD_DATADIR_PREFIX}

    make create-demo-cluster

    cp gpAux/gpdemo/gpdemo-env.sh ${OLD_DATADIR_PREFIX}/gpdemo-env.sh
    source ${OLD_DATADIR_PREFIX}/gpdemo-env.sh

    psql postgres -c "CREATE DATABASE adb;"

    psql adb -f ${OLD_DATABASE}

    psql postgres --quiet --no-align --tuples-only -F$'\t' \
        -c "SELECT hostname, datadir FROM gp_segment_configuration WHERE content <> -1 AND role = 'p'" \
        > "/tmp/segment_datadirs.txt"

    gpstop -a
EOF
}

create_new_cluster() {

    # create cluster version 7
su - gpadmin -c bash -- -e <<EOF
    source ${NEW_GPHOME}/greengage_path.sh    
    export LANG=en_US.UTF-8

    cd ${NEW_GPDB_SRC_PATH}  
    export DATADIRS=${DATADIR_PREFIX} 

    make create-demo-cluster

    cp gpAux/gpdemo/gpdemo-env.sh ${DATADIR_PREFIX}/gpdemo-env.sh

    source ${DATADIR_PREFIX}/gpdemo-env.sh

    gpstop -a
EOF
}

check_new_cluster() {

    # create cluster version 7
su - gpadmin -c bash -- -e <<EOF
    source ${NEW_GPHOME}/greengage_path.sh    
    source ${DATADIR_PREFIX}/gpdemo-env.sh

    gpstart -a

    psql adb -f ${CHECK_NEW_DATABASE} 2>/tmp/errors

    gpstop -a
EOF

    if (test -f /tmp/errors ) ; then
        if (grep -q . /tmp/errors); then
            cat /tmp/errors
            return 1
        else
            return 0
        fi
    else
        return 1
    fi
}

service ssh start

create_old_cluster

create_new_cluster

echo "Upgrading master at ${MASTER_HOST}..."

su - gpadmin -c bash -- -e <<EOF
set -ux
source ${NEW_GPHOME}/greengage_path.sh
source ${DATADIR_PREFIX}/gpdemo-env.sh

time pg_upgrade --mode=dispatcher \
            -b "${OLD_GPHOME}/bin/" -B "${NEW_GPHOME}/bin/" \
            -d "${OLD_COORDINATOR_DATA_DIRECTORY}" \
            -D "${NEW_COORDINATOR_DATA_DIRECTORY}"

while read -u30 hostname datadir; do
    echo "Upgrading segment at '\$hostname' (\$datadir)..."

    newdatadir=\${datadir/gpdb6data/gpdb7data}
    echo "newdatadir \$newdatadir"

    # NOTE: the trailing slash on the rsync source directory is important! It
    # means to transfer the directory's contents and not the directory itself.
    ssh -n ${MASTER_HOST} rsync -r --delete "${NEW_COORDINATOR_DATA_DIRECTORY}/" "\$hostname:\$newdatadir" \
        --exclude /postgresql.conf \
        --exclude /pg_hba.conf \
        --exclude /postmaster.opts \
        --exclude /gp_replication.conf \
        --exclude /gp_dbid \
        --exclude /gpssh.conf

    time pg_upgrade --mode=segment \
            -b "${OLD_GPHOME}/bin/" -B "${NEW_GPHOME}/bin/" \
            -d "\$datadir" \
            -D "\$newdatadir"
done 30< /tmp/segment_datadirs.txt
EOF

# checking the updated cluster
if ! check_new_cluster; then
    echo 'error in pg_upgrade from Greengage 6'
    exit 1
fi

echo Complete