#!/bin/bash

project="resgroup"

mkdir ssh_keys -p
if [ ! -e "ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ssh_keys/id_rsa
fi

#install gpdb and setup gpadmin user
bash arenadata/scripts/init_containers.sh $project cdw sdw1

for service in 'cdw' 'sdw1'
do
  #grant access rights to group controllers
  docker-compose -p $project -f arenadata/docker-compose.yaml exec $service bash -c "
    chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset} &&
    mkdir /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
    chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
    chown -R gpadmin:gpadmin /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb"
done

#create cluster
docker-compose -p $project -f arenadata/docker-compose.yaml exec cdw \
 bash -c "source gpdb_src/concourse/scripts/common.bash && HOSTS_LIST='sdw1' make_cluster"

#run tests
docker-compose -p $project -f arenadata/docker-compose.yaml exec -Tu gpadmin cdw bash -ex <<EOF
        source /usr/local/greenplum-db-devel/greenplum_path.sh
        source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
        export LDFLAGS="-L\${GPHOME}/lib"
        export CPPFLAGS="-I\${GPHOME}/include"
        export USER=gpadmin

        cd /home/gpadmin/gpdb_src
        ./configure --prefix=/usr/local/greenplum-db-devel \
            --without-zlib --without-rt --without-libcurl \
            --without-libedit-preferred --without-docdir --without-readline \
            --disable-gpcloud --disable-gpfdist --disable-orca \
            ${CONFIGURE_FLAGS}

        make -C /home/gpadmin/gpdb_src/src/test/regress
        ssh sdw1 mkdir -p /home/gpadmin/gpdb_src/src/test/{regress,isolation2} </dev/null
        scp /home/gpadmin/gpdb_src/src/test/regress/regress.so \
            gpadmin@sdw1:/home/gpadmin/gpdb_src/src/test/regress/

        make PGOPTIONS="-c optimizer=off" installcheck-resgroup || (
            errcode=\$?
            find src/test/isolation2 -name regression.diffs \
            | while read diff; do
                cat <<EOF1

======================================================================
DIFF FILE: \$diff
----------------------------------------------------------------------

EOF1
                cat \$diff
              done
            exit \$errcode
        )
EOF

#clear
docker-compose -p $project -f arenadata/docker-compose.yaml --env-file arenadata/.env down
