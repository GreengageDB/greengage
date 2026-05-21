# Helper script for running src/bin/pg_upgrade/test_gpdb.sh to
# upgrade between Greengage6 and Greengage7

set -euo pipefail

# We could accept command line arguments instead of relying on environment variables,
# but it seems too much for a 50-line script. In any case, perform some redundant
# checks before running the script.


all_paths_are_set=true

for path in GREENGAGE7_INSTALLATION \
            GREENGAGE6_INSTALLATION \
            GREENGAGE7_SRC \
            GREENGAGE6_SRC;
do
    if [ -z ${!path:-} ]; then
        echo "This test expects the following environment variable: ${path}"
        all_paths_are_set=false
    fi
done

if [ ${all_paths_are_set} = false ]; then
    echo "Not all required enviroment variable are set. Exiting."
    exit 1
fi

SQL_SCHEMA=${SQL_SCHEMA:-}
if [ -z "${SQL_SCHEMA}" ]; then
    echo "SQL_SCHEMA environment variable is not set. This test will be performed on an empty cluster."
else
    echo "SQL_SCHEMA is set. This test will load \"${SQL_SCHEMA}\" before performing upgrade."
fi

set -x

# Create Greengage 6 demo cluster.
# Its default PORT_BASE is 6000, which is inconsistent with Greengage 7 and PGPORT
# environment variable below.
export PORT_BASE=7000
cd     ${GREENGAGE6_SRC}/gpAux/gpdemo
source ${GREENGAGE6_INSTALLATION}/greengage_path.sh
make   create-demo-cluster

# Setup environment
export PGPORT=7000
export MASTER_DATA_DIRECTORY=${GREENGAGE6_SRC}/gpAux/gpdemo/datadirs/qddir/demoDataDir-1

# Load the schema
if [ -n "${SQL_SCHEMA}" ]; then
    psql template1 -f ${SQL_SCHEMA}
fi

# Create regression database if it doesn't exist.
# psql can report an error if it does,
# but the returned exit code is 0 either way.
echo 'CREATE DATABASE regression;' | psql template1 -f-

# And finally, run the test
pushd ${GREENGAGE7_SRC}/src/bin/pg_upgrade
./test_gpdb.sh \
    -b ${GREENGAGE7_INSTALLATION}/bin \
    -B ${GREENGAGE6_INSTALLATION}/bin \
    -O ${GREENGAGE6_SRC}/gpAux/gpdemo/datadirs/
popd
