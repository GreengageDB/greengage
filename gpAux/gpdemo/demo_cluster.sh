#!/usr/bin/env bash

# ======================================================================
# Configuration Variables
# ======================================================================

# Set to zero to force cluster to be created without data checksums
DATACHECKSUMS=1

# ======================================================================
# Data Directories
# ======================================================================

if [ -z "${COORDINATOR_DATADIR}" ]; then
  DATADIRS=${DATADIRS:-`pwd`/datadirs}
else
  DATADIRS="${COORDINATOR_DATADIR}/datadirs"
fi

QDDIR=$DATADIRS/qddir
SEG_PREFIX=demoDataDir

STANDBYDIR=$DATADIRS/standby

# ======================================================================
# Database Ports
# ======================================================================

COORDINATOR_DEMO_PORT=${DEMO_PORT_BASE}
STANDBY_DEMO_PORT=`expr ${DEMO_PORT_BASE} + 1`
DEMO_PORT_BASE=`expr ${DEMO_PORT_BASE} + 2`
if [ "${WITH_MIRRORS}" == "true" ]; then
    for (( i=0; i<`expr 2 \* $NUM_PRIMARY_MIRROR_PAIRS`; i++ )); do
        PORT_NUM=`expr $DEMO_PORT_BASE + $i`
        DEMO_SEG_PORTS_LIST="$DEMO_SEG_PORTS_LIST $PORT_NUM"
    done
elif [ "${WITH_MIRRORS}" == "false" ]; then
    for (( i=0; i<${NUM_PRIMARY_MIRROR_PAIRS}; i++ )); do
        PORT_NUM=`expr $DEMO_PORT_BASE + $i`
        DEMO_SEG_PORTS_LIST="$DEMO_SEG_PORTS_LIST $PORT_NUM"
    done
fi

DEMO_SEG_PORTS_LIST=${DEMO_SEG_PORTS_LIST#* }

# ======================================================================
# Functions
# ======================================================================

checkDemoConfig(){
    echo "----------------------------------------------------------------------"
    echo "                   Checking for port availability"
    echo "----------------------------------------------------------------------"
    echo ""
    # Check if Coordinator_DEMO_Port is free
    echo "  Coordinator port check ... : ${COORDINATOR_DEMO_PORT}"
    PORT_FILE="/tmp/.s.PGSQL.${COORDINATOR_DEMO_PORT}"
    if [ -f ${PORT_FILE} -o  -S ${PORT_FILE} ] ; then 
        echo ""
        echo -n " Port ${COORDINATOR_DEMO_PORT} appears to be in use. " 
        echo " This port is needed by the Coordinator Database instance. "
        echo ">>> Edit Makefile to correct the port number (COORDINATOR_PORT). <<<" 
        echo -n " Check to see if the port is free by using : "
        echo " 'ss -an | grep ${COORDINATOR_DEMO_PORT}'"
        echo " If the port is not used please make sure files ${PORT_FILE}* are deleted"
        echo ""
        return 1
    fi

    # Check if Standby_DEMO_Port is free
    echo "  Standby port check ... : ${STANDBY_DEMO_PORT}"
    PORT_FILE="/tmp/.s.PGSQL.${STANDBY_DEMO_PORT}"
    if [ -f ${PORT_FILE} -o  -S ${PORT_FILE} ] ; then
        echo ""
        echo -n " Port ${STANDBY_DEMO_PORT} appears to be in use. "
        echo " This port is needed by the Standby Database instance. "
        echo ">>> Edit Makefile to correct the port number (STANDBY_PORT). <<<"
        echo -n " Check to see if the port is free by using : "
        echo " 'ss -an | grep ${STANDBY_DEMO_PORT}'."
        echo " If the port is not used please make sure files ${PORT_FILE}* are deleted"
        echo ""
        return 1
    fi

    # Check if all Segment Ports are free
    for PORT_NUM in ${DEMO_SEG_PORTS_LIST}; do
        echo "  Segment port check .. : ${PORT_NUM}"
        PORT_FILE="/tmp/.s.PGSQL.${PORT_NUM}"
        if [ -f ${PORT_FILE} -o -S ${PORT_FILE} ] ; then 
            echo ""
            echo -n "Port ${PORT_NUM} appears to be in use."
            echo " This port is needed for segment database instance."
            echo ">>> Edit Makefile to correct the base ports (PORT_BASE). <<<"
            echo -n " Check to see if the port is free by using : "
            echo " 'ss -an | grep ${PORT_NUM}'"
            echo " If the port is not used please make sure files ${PORT_FILE}* are deleted"
            echo ""
            return 1
        fi
    done
    return 0
}

USAGE(){
    echo ""
    echo " `basename $0` {-c | -d | -u} <-K>"
    echo " -c : Check if demo is possible."
    echo " -d : Delete the demo."
    echo " -K : Create cluster without data checksums."
    echo " -u : Usage, prints this message."
    echo ""
}

#
# Clean up the demo
#

cleanDemo(){

    ##
    ## Attempt to bring down using GPDB cluster instance using gpstop
    ##

    (export COORDINATOR_DATA_DIRECTORY=$QDDIR/${SEG_PREFIX}-1;
     source ${GPHOME}/greengage_path.sh;
     gpstop -ai)

    ##
    ## Remove the files and directories created; allow test harnesses
    ## to disable this
    ##

    if [ "${GPDEMO_DESTRUCTIVE_CLEAN}" != "false" ]; then
        if [ -f hostfile ]; then
            echo "Deleting hostfile"
            rm -f hostfile
        fi
        if [ -f clusterConfigFile ]; then
            echo "Deleting clusterConfigFile"
            rm -f clusterConfigFile
        fi
        if [ -f clusterConfigPostgresAddonsFile ]; then
            echo "Deleting clusterConfigPostgresAddonsFile"
            rm -f clusterConfigPostgresAddonsFile
        fi
        if [ -d ${DATADIRS} ]; then
            echo "Deleting ${DATADIRS}"
            rm -rf ${DATADIRS}
        fi
        if [ -d logs ]; then
            rm -rf logs
        fi
        rm -f optimizer-state.log gpdemo-env.sh
    fi
}

#*****************************************************************************
# Main Section
#*****************************************************************************

while getopts ":cdK'?'" opt
do
	case $opt in 
		'?' ) USAGE ;;
        c) checkDemoConfig
           RETVAL=$?
           if [ $RETVAL -ne 0 ]; then
               echo "Checking failed "
               exit 1
           fi
           exit 0
           ;;
        d) cleanDemo
           exit 0
           ;;
        K) DATACHECKSUMS=0
           shift
           ;;
        *) USAGE
           exit 0
           ;;
	esac
done

if [ -z "${GPHOME}" ]; then
    echo "FATAL: The GPHOME environment variable is not set."
    echo ""
    echo "  You can set it by sourcing the greengage_path.sh"
    echo "  file in your Greengage installation directory."
    echo ""
    exit 1
fi

cat <<-EOF
	======================================================================
	            ______  _____  ______  _______ _______  _____
	           |  ____ |_____] |     \ |______ |  |  | |     |
	           |_____| |       |_____/ |______ |  |  | |_____|

	----------------------------------------------------------------------

EOF

if [ "${WITH_MIRRORS}" == "true" ]; then
cat <<-EOF
	  This is a MIRRORED demo of the Greengage Database system.  We will
	  create a cluster installation with coordinator and `expr 2 \* ${NUM_PRIMARY_MIRROR_PAIRS}` segment instances
	  (${NUM_PRIMARY_MIRROR_PAIRS} primary & ${NUM_PRIMARY_MIRROR_PAIRS} mirror).
EOF
elif [ "${WITH_MIRRORS}" == "false" ]; then
cat <<-EOF
	  This is a MIRRORLESS demo of the Greengage Database system.  We will create
	  a cluster installation with coordinator and ${NUM_PRIMARY_MIRROR_PAIRS} segment instances.
EOF
fi

cat <<-EOF

	    GPHOME ................... : ${GPHOME}
	    COORDINATOR_DATA_DIRECTORY : ${QDDIR}/${SEG_PREFIX}-1

	    COORDINATOR PORT (PGPORT). : ${COORDINATOR_DEMO_PORT}
	    STANDBY PORT ............. : ${STANDBY_DEMO_PORT}
	    SEGMENT PORTS ............ : ${DEMO_SEG_PORTS_LIST}

	  NOTE(s):

	    * The DB ports identified above must be available for use.
	    * An environment file gpdemo-env.sh has been created for your use.

	======================================================================

EOF

GPPATH=`find -H $GPHOME -name gpstart| tail -1`
RETVAL=$?

if [ "$RETVAL" -ne 0 ]; then
    echo "Error attempting to find Greengage executables in $GPHOME"
    exit 1
fi

if [ ! -x "$GPPATH" ]; then
    echo "No executables found for Greengage installation in $GPHOME"
    exit 1
fi
GPPATH=`dirname $GPPATH`
if [ ! -x $GPPATH/gpinitsystem ]; then
    echo "No mgmt executables found for Greengage installation in $GPPATH"
    exit 1
fi

if [ -d $DATADIRS ]; then
  rm -rf $DATADIRS
fi
mkdir $DATADIRS
mkdir $QDDIR
mkdir $DATADIRS/gpAdminLogs

for (( i=1; i<=$NUM_PRIMARY_MIRROR_PAIRS; i++ ))
do
  PRIMARY_DIR=$DATADIRS/dbfast$i
  mkdir -p $PRIMARY_DIR
  PRIMARY_DIRS_LIST="$PRIMARY_DIRS_LIST $PRIMARY_DIR"

  if [ "${WITH_MIRRORS}" == "true" ]; then
    MIRROR_DIR=$DATADIRS/dbfast_mirror$i
    mkdir -p $MIRROR_DIR
    MIRROR_DIRS_LIST="$MIRROR_DIRS_LIST $MIRROR_DIR"
  fi
done
PRIMARY_DIRS_LIST=${PRIMARY_DIRS_LIST#* }
MIRROR_DIRS_LIST=${MIRROR_DIRS_LIST#* }

#*****************************************************************************************
# Host configuration
#*****************************************************************************************

LOCALHOST=`hostname`
echo $LOCALHOST > hostfile

#*****************************************************************************************
# Name of the system configuration file.
#*****************************************************************************************

CLUSTER_CONFIG=clusterConfigFile
CLUSTER_CONFIG_POSTGRES_ADDONS=clusterConfigPostgresAddonsFile

rm -f ${CLUSTER_CONFIG}
rm -f ${CLUSTER_CONFIG_POSTGRES_ADDONS}

#*****************************************************************************************
# Create the system configuration file
#*****************************************************************************************

cat >> $CLUSTER_CONFIG <<-EOF
	# This file must exist in the same directory that you execute gpinitsystem in
	MACHINE_LIST_FILE=`pwd`/hostfile
	
	# This names the data directories for the Segment Instances and the Entry Postmaster
	SEG_PREFIX=$SEG_PREFIX
	
	# This is the port at which to contact the resulting Greengage database, e.g.
	#   psql -p \$PORT_BASE -d template1
	PORT_BASE=${DEMO_PORT_BASE}
	
	# Array of data locations for each hosts Segment Instances, the number of directories in this array will
	# set the number of segment instances per host
	declare -a DATA_DIRECTORY=(${PRIMARY_DIRS_LIST})
	
	# Name of host on which to setup the QD
	COORDINATOR_HOSTNAME=$LOCALHOST
	
	# Name of directory on that host in which to setup the QD
	COORDINATOR_DIRECTORY=$QDDIR
	
	COORDINATOR_PORT=${COORDINATOR_DEMO_PORT}
	
	# Shell to use to execute commands on all hosts
	TRUSTED_SHELL="`pwd`/lalshell"
	
	ENCODING=UNICODE
EOF

if [ "${DATACHECKSUMS}" == "0" ]; then
    cat >> $CLUSTER_CONFIG <<-EOF
	# Turn off data checksums
	HEAP_CHECKSUM=off
EOF
fi

if [ "${WITH_MIRRORS}" == "true" ]; then
    cat >> $CLUSTER_CONFIG <<-EOF

		# Array of mirror data locations for each hosts Segment Instances, the number of directories in this array will
		# set the number of segment instances per host
		declare -a MIRROR_DATA_DIRECTORY=(${MIRROR_DIRS_LIST})

		MIRROR_PORT_BASE=`expr $DEMO_PORT_BASE + $NUM_PRIMARY_MIRROR_PAIRS`
	EOF
fi


STANDBY_INIT_OPTS=""
if [ "${WITH_STANDBY}" == "true" ]; then
	STANDBY_INIT_OPTS="-s ${LOCALHOST} -P ${STANDBY_DEMO_PORT} -S ${STANDBYDIR}"
fi

if [ ! -z "${EXTRA_CONFIG}" ]; then
  echo ${EXTRA_CONFIG} >> $CLUSTER_CONFIG
fi

if [ -z "${DEFAULT_QD_MAX_CONNECT}" ]; then
   DEFAULT_QD_MAX_CONNECT=25
fi

cat >> $CLUSTER_CONFIG <<-EOF

	# Keep max_connection settings to reasonable values for
	# installcheck good execution.

	DEFAULT_QD_MAX_CONNECT=$DEFAULT_QD_MAX_CONNECT
	QE_CONNECT_FACTOR=5

EOF

if [ -n "${STATEMENT_MEM}" ]; then
	cat >> $CLUSTER_CONFIG_POSTGRES_ADDONS<<-EOF
		statement_mem = ${STATEMENT_MEM}
	EOF
fi

if [ "${ONLY_PREPARE_CLUSTER_ENV}" == "true" ]; then
    echo "ONLY_PREPARE_CLUSTER_ENV set, generated clusterConf file: $CLUSTER_CONFIG, exiting"
    exit 0
fi

## ======================================================================
## Create cluster
## ======================================================================

##
## Provide support to pass dynamic values to ${CLUSTER_CONFIG_POSTGRES_ADDONS}
## which is used during gpinitsystems.
##

if [ "${BLDWRAP_POSTGRES_CONF_ADDONS}" != "__none__" ]  && \
   [ "${BLDWRAP_POSTGRES_CONF_ADDONS}" != "__unset__" ] && \
   [ -n "${BLDWRAP_POSTGRES_CONF_ADDONS}" ]; then

    [ -f ${CLUSTER_CONFIG_POSTGRES_ADDONS} ] && chmod a+w ${CLUSTER_CONFIG_POSTGRES_ADDONS}

    echo ${BLDWRAP_POSTGRES_CONF_ADDONS} | sed -e 's/\[//g' -e 's/\]//g' | tr "," "\n" | sed -e 's/^\"//g' -e 's/\"$//g' >> ${CLUSTER_CONFIG_POSTGRES_ADDONS}
fi

# Add fsync=off for all gpdemo deployments
grep -q 'fsync=off' ${CLUSTER_CONFIG_POSTGRES_ADDONS} && echo "fsync=off already exists in ${CLUSTER_CONFIG_POSTGRES_ADDONS}." || echo "fsync=off" >> ${CLUSTER_CONFIG_POSTGRES_ADDONS}

echo ""
echo "======================================================================"
echo "CLUSTER_CONFIG_POSTGRES_ADDONS: ${CLUSTER_CONFIG_POSTGRES_ADDONS}"
echo "----------------------------------------------------------------------"
cat ${CLUSTER_CONFIG_POSTGRES_ADDONS}
echo "======================================================================"
echo ""

echo "=========================================================================================="
echo "executing:"
echo "  $GPPATH/gpinitsystem -a -c $CLUSTER_CONFIG -l $DATADIRS/gpAdminLogs -p ${CLUSTER_CONFIG_POSTGRES_ADDONS} ${STANDBY_INIT_OPTS} \"$@\""
echo "=========================================================================================="
echo ""
$GPPATH/gpinitsystem -a -c $CLUSTER_CONFIG -l $DATADIRS/gpAdminLogs -p ${CLUSTER_CONFIG_POSTGRES_ADDONS} ${STANDBY_INIT_OPTS} "$@"
RETURN=$?

echo "========================================"
echo "gpinitsystem returned: ${RETURN}"
echo "========================================"
echo ""

if [ "$enable_gpfdist" = "yes" ] && [ "$with_openssl" = "yes" ]; then
	echo "======================================================================"
	echo "Generating SSL certificates for gpfdists:"
	echo "======================================================================"
	echo ""

	./generate_certs.sh >> generate_certs.log

	cp -r certificate/gpfdists $QDDIR/$SEG_PREFIX-1/

	for (( i=1; i<=$NUM_PRIMARY_MIRROR_PAIRS; i++ ))
	do
		cp -r certificate/gpfdists $DATADIRS/dbfast$i/${SEG_PREFIX}$((i-1))/
		cp -r certificate/gpfdists $DATADIRS/dbfast_mirror$i/${SEG_PREFIX}$((i-1))/
	done
	echo ""
fi

OPTIMIZER=$(psql -t -p ${COORDINATOR_DEMO_PORT} -d template1 -c "show optimizer"   2>&1)

echo "======================================================================" 2>&1 | tee -a optimizer-state.log
echo "                           OPTIMIZER STATE"                             2>&1 | tee -a optimizer-state.log
echo "----------------------------------------------------------------------" 2>&1 | tee -a optimizer-state.log
echo "  Optimizer state .. : ${OPTIMIZER}"                                    2>&1 | tee -a optimizer-state.log
echo "======================================================================" 2>&1 | tee -a optimizer-state.log
echo ""                                                                       2>&1 | tee -a optimizer-state.log

psql -p ${COORDINATOR_DEMO_PORT} -d template1 -c "select version();"               2>&1 | tee -a optimizer-state.log

psql -p ${COORDINATOR_DEMO_PORT} -d template1 -c "show optimizer;" > /dev/null     2>&1
if [ $? = 0 ]; then
    psql -p ${COORDINATOR_DEMO_PORT} -d template1 -c "show optimizer;"             2>&1 | tee -a optimizer-state.log
fi

psql -p ${COORDINATOR_DEMO_PORT} -d template1 -c "select gp_opt_version();" > /dev/null 2>&1
if [ $? = 0 ]; then
    psql -p ${COORDINATOR_DEMO_PORT} -d template1 -c "select gp_opt_version();"    2>&1 | tee -a optimizer-state.log
fi

echo "======================================================================" 2>&1 | tee -a optimizer-state.log
echo ""                                                                       2>&1 | tee -a optimizer-state.log

cat > gpdemo-env.sh <<-EOF
	## ======================================================================
	##                                gpdemo
	## ----------------------------------------------------------------------
	## timestamp: $( date )
	## ======================================================================

	export PGPORT=${COORDINATOR_DEMO_PORT}
	export COORDINATOR_DATA_DIRECTORY=$QDDIR/${SEG_PREFIX}-1
	export MASTER_DATA_DIRECTORY=$QDDIR/${SEG_PREFIX}-1
EOF

if [ "${RETURN}" -gt 1 ];
then
    # gpinitsystem will return warnings as exit code 1
    exit ${RETURN}
else
    exit 0
fi
