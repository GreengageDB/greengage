import abc

from contextlib import closing
from gppylib.db import dbconn
from gppylib import gplog
from gppylib.mainUtils import ExceptionNoStackTraceNeeded
from gppylib.operations.detect_unreachable_hosts import get_unreachable_segment_hosts
from gppylib.parseutils import line_reader, check_values, canonicalize_address
from gppylib.utils import checkNotNone, normalizeAndValidateInputPath, validateHostnameAddress
from gppylib.gparray import GpArray, Segment
from gppylib.operations.get_segments_in_recovery import is_seg_in_backup_mode
from gppylib.commands.gp import RECOVERY_REWIND_APPNAME

logger = gplog.get_default_logger()


def get_segments_with_running_basebackup():
    """
    Returns a list of contentIds of source segments of running pg_basebackup processes
    At present gp_stat_replication table does not contain any info about datadir and dbid of the target of running pg_basebackup
    """

    sql = "select gp_segment_id from gp_stat_replication where application_name = 'pg_basebackup'"

    try:
        with closing(dbconn.connect(dbconn.DbURL())) as conn:
            res = dbconn.execSQL(conn, sql)
            rows = res.fetchall()
    except Exception as e:
        raise Exception("Failed to query gp_stat_replication: %s" %str(e))

    segments_with_running_basebackup = {row[0] for row in rows}

    if len(segments_with_running_basebackup) == 0:
        logger.debug("No basebackup running")
    return segments_with_running_basebackup


def is_pg_rewind_running(hostname, port):
    """
        Returns true if a pg_rewind process is running for the given segment
    """
    logger.debug(
        "Checking for running instances of pg_rewind with host {} and port {} as source server".format(hostname, port))

    """
        Reasons to depend on pg_stat_activity table:
            * pg_rewind is invoked using --source-server connection string as it needs libpq connection
              with source server, which will be remote to the target server and --source-pgdata can not
              be used in that case. Thus pg_stat_activity will always contain entry for active pg_rewind.
            * pg_rewind keeps a connection open throughout it's lifecycle, so pg_stat_activity will contain
              entries for active pg_rewind process till the end of execution.
            * gpstate uses the above mentioned approach (pg_stat_activity entry check) to check for
              incremental recoveries in progress.Thus, using the same approach will make it consistent
              everywhere.
    """

    sql = "SELECT count(*) FROM pg_stat_activity WHERE application_name = '{}'".format(RECOVERY_REWIND_APPNAME)
    try:
        url = dbconn.DbURL(hostname=hostname, port=port, dbname='template1')
        with closing(dbconn.connect(url, utility=True)) as conn:
            res = dbconn.execSQLForSingleton(conn, sql)
            return res > 0

    except Exception as e:
        raise Exception("Failed to query pg_stat_activity for segment hostname: {}, port: {}, error: {}".format(
            hostname, str(port), str(e)))


def extract_recovery_config_info(parts):
    """
    Extracts relevant recovery configuration information from a list of parts.
    """
    address, port, datadir = parts[-3:]
    recovery_type = "Incremental"
    hostname_check_required = False
    hostname = address

    recovery_type_flag = False
    # If 5 fields are there in part then first field represents recovery type and it should any of I,D,F.i,d,f
    if len(parts) == 5:
        if parts[0] not in {"I", "D", "F", "i", "d", "f"}:
            raise Exception("Invalid recovery type provided, please provide any of I,D,F,i,d,f as recovery_type")
        hostname = parts[1]
        hostname_check_required = True
        recovery_type_flag = True

    # If 4 fields are there in recovery part then first field can be either recovery_type or hostname
    # if first field part[0] is not in {I,D,F,i,d,f} then it is hostname
    if len(parts) == 4:
        if parts[0] in {"I", "D", "F", "i", "d", "f"}:
            recovery_type_flag = True
        else:
            hostname = parts[0]
            hostname_check_required = True

    # If recovery_type field has been provided by user then sync_mode needs be decided based on input
    if recovery_type_flag:
        sync_mode = parts[0]
        if sync_mode in {"D", "d"}:
            recovery_type = "Differential"
        elif sync_mode in {"F", "f"}:
            recovery_type = "Full"

    return hostname, address, port, datadir, recovery_type, hostname_check_required


class RecoveryTriplet:
    """
    Represents the segments needed to perform a recovery on a given segment.
    failed   = acting mirror that needs to be recovered
    live     = acting primary to use to recover that failed segment
    failover = failed segment will be recovered here
    """
    def __init__(self, failed, live, failover, recovery_type=None):
        self.failed = failed
        self.live = live
        self.failover = failover
        self.validate(failed, live, failover)
        self.recovery_type = recovery_type

    def __repr__(self):
        return "Failed: {0} Live: {1} Failover: {2}".format(self.failed, self.live, self.failover)

    def __eq__(self, other):
        if not isinstance(other, RecoveryTriplet):
            return NotImplemented

        return self.failed == other.failed and self.live == other.live and self.failover == other.failover

    @staticmethod
    def validate(failed, live, failover):

        msg = "liveSegment" if not failed else "No peer found for dbid {}. liveSegment".format(failed.getSegmentDbId())
        checkNotNone(msg, live)

        if failed is None and failover is None:
            raise Exception("internal error: insufficient information to recover a mirror")

        if not live.isSegmentQE():
            raise ExceptionNoStackTraceNeeded("Segment to recover from for content %s is not a correct segment "
                                              "(it is a master or standby master)" % live.getSegmentContentId())
        if not live.isSegmentPrimary(True):
            raise ExceptionNoStackTraceNeeded(
                "Segment to recover from for content %s is not a primary" % live.getSegmentContentId())
        if not live.isSegmentUp():
            raise ExceptionNoStackTraceNeeded(
                "Primary segment is not up for content %s" % live.getSegmentContentId())
        if live.unreachable:
            raise ExceptionNoStackTraceNeeded(
                "The recovery source segment %s (content %s) is unreachable." % (live.getSegmentHostName(),
                                                                                 live.getSegmentContentId()))

        if failed is not None:
            if failed.getSegmentContentId() != live.getSegmentContentId():
                raise ExceptionNoStackTraceNeeded(
                    "The primary is not of the same content as the failed mirror.  Primary content %d, "
                    "mirror content %d" % (live.getSegmentContentId(), failed.getSegmentContentId()))
            if failed.getSegmentDbId() == live.getSegmentDbId():
                raise ExceptionNoStackTraceNeeded("For content %d, the dbid values are the same.  "
                                                  "A segment may not be recovered from itself" %
                                                  live.getSegmentDbId())

        if failover is not None:
            if failover.getSegmentContentId() != live.getSegmentContentId():
                raise ExceptionNoStackTraceNeeded(
                    "The primary is not of the same content as the mirror.  Primary content %d, "
                    "mirror content %d" % (live.getSegmentContentId(), failover.getSegmentContentId()))
            if failover.getSegmentDbId() == live.getSegmentDbId():
                raise ExceptionNoStackTraceNeeded("For content %d, the dbid values are the same.  "
                                                  "A segment may not be built from itself"
                                                  % live.getSegmentDbId())
            if failover.unreachable:
                raise ExceptionNoStackTraceNeeded(
                    "The recovery target segment %s (content %s) is unreachable." % (failover.getSegmentHostName(),
                                                                                     failover.getSegmentContentId()))

        if failed is not None and failover is not None:
            # for now, we require the code to have produced this -- even when moving the segment to another
            #  location, we preserve the directory
            assert failed.getSegmentDbId() == failover.getSegmentDbId()


class RecoveryTripletRequest:
    def __init__(self, failed, failover_host_name=None, failover_host_address=None, failover_port=None, failover_datadir=None, is_new_host=False, recovery_type=None):
        self.failed = failed

        self.failover_host_name = failover_host_name
        self.failover_host_address = failover_host_address
        self.failover_port = failover_port
        self.failover_datadir = failover_datadir
        self.failover_to_new_host = is_new_host
        self.recovery_type = recovery_type


# TODO: gparray is mutated for all triplets, even if we skip recovery for them(if they are unreachable)
class RecoveryTripletsFactory:
    @staticmethod
    def instance(gpArray, config_file=None, new_hosts=[], outputConfigFile=None, paralleldegree=1):
        """
        :param gpArray: The variable gpArray may get mutated when the getMirrorTriples function is called on this instance.
        :param config_file: user passed in config file, if any
        :param new_hosts: user passed in new hosts, if any
        :param paralleldegree: num of max parallel threads to run, default is 1
        :return:
        """
        if config_file:
            return RecoveryTripletsUserConfigFile(gpArray, config_file, paralleldegree)
        else:
            if not new_hosts:
                return RecoveryTripletsInplace(gpArray, outputConfigFile, paralleldegree)
            else:
                return RecoveryTripletsNewHosts(gpArray, new_hosts, outputConfigFile, paralleldegree)



class RecoveryTriplets:
    __metaclass__ = abc.ABCMeta

    def __init__(self, gpArray, outputConfigFile=None, paralleldegree=1):
        """
        :param gpArray: Needs to be a shallow copy since we may return a mutated gpArray
        """
        self.gpArray = gpArray
        self.interfaceHostnameWarnings = []
        self.paralleldegree = paralleldegree
        self.outputConfigFile = outputConfigFile

    @abc.abstractmethod
    def getTriplets(self):
        """
        This function ignores the status (i.e. u or d) of the segment because this function is used by gpaddmirrors,
        gpmovemirrors and gprecoverseg. For gpaddmirrors and gpmovemirrors, the segment to be moved should not
        be marked as down whereas for gprecoverseg the segment to be recovered needs to marked as down.
        """
        pass

    def _get_unreachable_failover_hosts(self, requests):
        hostlist = {req.failover_host_address for req in requests if req.failover_host_address}
        return get_unreachable_segment_hosts(hostlist, min(self.paralleldegree, len(hostlist)))

    def getInterfaceHostnameWarnings(self):
        return self.interfaceHostnameWarnings

    # TODO: the returned RecoveryTriplet(s) reflect (failed, live, failover) with failover reflecting the recovery
    # information of the new segment(that which will replace failed).  This is what is acted upon by
    # pg_rewind/pg_basebackup.  But as an artifact of the implementation, the caller's original gparray is mutated to
    # reflect this failover segment.  This is how we implement the `-o` option.
    # The returned RecoveryTriplets specify the recovery that needs to be done, but the gparray is mutated to reflect
    # the state as if that recovery had already completed.
    def _convert_requests_to_triplets(self, requests):
        triplets = []

        # Get the list of unreachable hosts from the request
        unreachable_failover_hosts = self._get_unreachable_failover_hosts(requests)

        dbIdToPeerMap = self.gpArray.getDbIdToPeerMap()

        failed_segments_with_running_basebackup = []
        failed_segments_with_running_pgrewind = []
        failed_segments_in_backup_mode = []
        segments_with_running_basebackup = get_segments_with_running_basebackup()

        for req in requests:
            """
                When running gprecoverseg (any sort of recovery full/incremental), if the pg_rewind, pg_basebackup is 
                already running for a segment, that segment should be skipped from the recovery. The reason being that 
                there should be only one writer per target segment at a time. Having several writers to a target will 
                eventually make the segment inconsistent and in a weird state. 

                Although technically we could allow user to run a full recovery to a new host even if there is a 
                pg_rewind/pg_basebackup running for that segment. This is a pretty rare scenario and we have decided not 
                to over complicates the code just to support this scenario.
            """
            failed_segment_dbid = req.failed.getSegmentDbId()
            peer = dbIdToPeerMap.get(failed_segment_dbid)
            if peer is None:
                raise Exception("No peer found for dbid {}. liveSegment is None".format(failed_segment_dbid))
            peer_contentid = peer.getSegmentContentId()

            if peer_contentid in segments_with_running_basebackup:
                failed_segments_with_running_basebackup.append(peer_contentid)
                continue

            if is_pg_rewind_running(peer.getSegmentHostName(), peer.getSegmentPort()):
                failed_segments_with_running_pgrewind.append(peer_contentid)
                continue

            # if source server(peer) is already in backup, we can not start recovery of the failed segment
            if is_seg_in_backup_mode(peer.getSegmentHostName(), peer.getSegmentPort()):
                failed_segments_in_backup_mode.append(peer_contentid)
                continue

            # TODO: These 2 cases have different behavior which might be confusing to the user.
            # "<failed_address>|<failed_port>|<failed_data_dir> <failed_address>|<failed_port>|<failed_data_dir>" does full recovery
            # "<failed_address>|<failed_port>|<failed_data_dir>" does incremental recovery
            # Changes made to support hostname in input configuration file jira# GPCM-207
            # Full recovery: "<failed_hostname|<failed_address>|<failed_port>|<failed_data_dir> SPACE <failed_hostname>|<failed_address>|<failed_port>|<failed_data_dir>"
            # Incremental recovery: "<failed_hostname>|<failed_address>|<failed_port>|<failed_data_dir>"
            failover = None

            if req.failover_host_address:
                # these two lines make it so that failover points to the object that is registered in gparray
                #   as the failed segment(!).
                failover = req.failed
                req.failed = failover.copy()

                # now update values in failover segment
                if req.failover_host_name != req.failover_host_address:
                    # Validate if the hostname and address are of the same host
                    if not validateHostnameAddress(req.failover_host_name, req.failover_host_address):
                        logger.warning(
                            "Not able to co-relate hostname:{0} with address:{1}. "
                            "Skipping recovery for segments with contentId {2}"
                            .format(req.failover_host_name, req.failover_host_address, peer_contentid))
                        continue

                failover.setSegmentHostName(req.failover_host_name)
                failover.setSegmentAddress(req.failover_host_address)
                failover.setSegmentPort(int(req.failover_port))
                failover.setSegmentDataDirectory(req.failover_datadir)
                failover.unreachable = failover.getSegmentHostName() in unreachable_failover_hosts
            else:
                # recovery in place, check for host reachable
                # Recovery triplet should be added to the final list if output config file has been requested. As the
                # output config file should have row for of each failed segment in segment configuration.
                if req.failed.unreachable and self.outputConfigFile is None:
                    # skip the recovery
                    continue

            peer = dbIdToPeerMap.get(req.failed.getSegmentDbId())

            triplets.append(RecoveryTriplet(req.failed, peer, failover, req.recovery_type))

        if len(failed_segments_with_running_basebackup) > 0:
            logger.warning(
                "Found pg_basebackup running for segments with contentIds %s, skipping recovery of these segments" % (
                    failed_segments_with_running_basebackup))

        if len(failed_segments_with_running_pgrewind) > 0:
            logger.warning(
                "Found pg_rewind running for segments with contentIds %s, skipping recovery of these segments" % (
                    failed_segments_with_running_pgrewind))

        if len(failed_segments_in_backup_mode) > 0:
            logger.warning(
                "Found differential recovery running for segments with contentIds %s, skipping recovery of these segments" % (
                    failed_segments_in_backup_mode))

        return triplets


class RecoveryTripletsInplace(RecoveryTriplets):
    def __init__(self, gpArray, outputConfigFile, paralleldegree):
        super(RecoveryTripletsInplace, self).__init__(gpArray, outputConfigFile, paralleldegree)

    def getTriplets(self):
        requests = []

        # TODO: only get failed mirrors explicitly here? gp_segment_configuration should
        # guarantee this but what if we are called on a failed cluster(primary and mirror both down).
        failedSegments = [seg for seg in self.gpArray.getSegDbList() if seg.isSegmentDown()]
        for failedSeg in failedSegments:
            req = RecoveryTripletRequest(failedSeg)
            requests.append(req)

        return self._convert_requests_to_triplets(requests)


class RecoveryTripletsNewHosts(RecoveryTriplets):
    def __init__(self, gpArray, newHosts, outputConfigFile, paralleldegree):
        super(RecoveryTripletsNewHosts, self).__init__(gpArray, outputConfigFile, paralleldegree)
        self.newHosts = [] if not newHosts else newHosts[:]
        self.portAssigner = self._PortAssigner(gpArray)

    #TODO improvement: skip unreachable new hosts and choose from the rest; right now we fail
    # if the first new host is unreachable even if there is an unused one later in the list.
    # NOTE: (add to gprecoverseg doc) this assigns host in some order; figure out if that is
    #  stable and document it.
    def getTriplets(self):
        def _check_new_hosts():
            if len(self.newHosts) > len(failedSegments):
                self.interfaceHostnameWarnings.append("The following recovery hosts were not needed:")
                for h in self.newHosts[len(failedSegments):]:
                    self.interfaceHostnameWarnings.append("\t%s" % h)

            if len(self.newHosts) < len(failedSegments):
                raise Exception('Not enough new recovery hosts given for recovery.')

            unreachable_hosts = get_unreachable_segment_hosts(self.newHosts[:len(failedSegments)], len(failedSegments))
            if unreachable_hosts:
                raise ExceptionNoStackTraceNeeded("Cannot recover. The following recovery target hosts are "
                                                  "unreachable: %s" % unreachable_hosts)

        failedSegments = GpArray.getSegmentsByHostName([seg for seg in self.gpArray.getSegDbList() if seg.isSegmentDown()])
        _check_new_hosts()

        requests = []
        for failedHost, failoverHost in zip(sorted(failedSegments.keys()), self.newHosts):
            for failed in failedSegments[failedHost]:
                failoverPort = self.portAssigner.findAndReservePort(failoverHost, failoverHost)
                req = RecoveryTripletRequest(failed, failover_host_name=failoverHost, failover_host_address=failoverHost,
                      failover_port=failoverPort, failover_datadir=failed.getSegmentDataDirectory(), is_new_host=True)
                requests.append(req)

        return self._convert_requests_to_triplets(requests)

    class _PortAssigner:
        """
        Used to assign new ports to segments on a host

        Note that this could be improved so that we re-use ports for segments that are being recovered but this
          does not seem necessary.

        """

        MAX_PORT_EXCLUSIVE = 65536

        def __init__(self, gpArray):
            #
            # determine port information for recovering to a new host --
            #   we need to know the ports that are in use and the valid range of ports
            #
            segments = gpArray.getDbList()
            ports = [seg.getSegmentPort() for seg in segments if seg.isSegmentQE()]
            if len(ports) > 0:
                self.__minPort = min(ports)
            else:
                raise Exception("No segment ports found in array.")
            self.__usedPortsByHostName = {}

            byHost = GpArray.getSegmentsByHostName(segments)
            for hostName, segments in byHost.items():
                usedPorts = self.__usedPortsByHostName[hostName] = {}
                for seg in segments:
                    usedPorts[seg.getSegmentPort()] = True

        def findAndReservePort(self, hostName, address):
            """
            Find a port not used by any postmaster process.
            When found, add an entry:  usedPorts[port] = True   and return the port found
            Otherwise raise an exception labeled with the given address
            """
            if hostName not in self.__usedPortsByHostName:
                self.__usedPortsByHostName[hostName] = {}
            usedPorts = self.__usedPortsByHostName[hostName]

            minPort = self.__minPort
            for port in range(minPort, RecoveryTripletsNewHosts._PortAssigner.MAX_PORT_EXCLUSIVE):
                if port not in usedPorts:
                    usedPorts[port] = True
                    return port
            raise Exception("Unable to assign port on %s" % address)


class RecoveryTripletsUserConfigFile(RecoveryTriplets):
    def __init__(self, gpArray, config_file, paralleldegree):
        super(RecoveryTripletsUserConfigFile, self).__init__(gpArray, paralleldegree)
        self.config_file = config_file
        self.rows = self._parseConfigFile(self.config_file)

    def getTriplets(self):
        def _find_failed_from_row():
            failed = None
            for segment in self.gpArray.getDbList():
                # In case the input configuration file contains 4 parameters then hostname provided should match
                # with the hostname in segment configuration table, if it does not match an exception will be thrown
                if row['hostname_check_required'] and segment.getSegmentHostName() != row['failedHostname']:
                    continue
                if (segment.getSegmentAddress() == row['failedAddress']
                        and str(segment.getSegmentPort()) == row['failedPort']
                        and segment.getSegmentDataDirectory() == row['failedDataDirectory']):
                    failed = segment
                    break

            if failed is None:
                msg =  "A segment to recover was not found in configuration. This segment is described by "

                if row['hostname_check_required']:
                    msg += "hostname|address|port|directory '{}|{}|{}|{}'".format(
                          row['failedHostname'], row['failedAddress'], row['failedPort'], row['failedDataDirectory'])


                else:
                    msg += "address|port|directory '{}|{}|{}'".format(row['failedAddress'],
                                                                      row['failedPort'], row['failedDataDirectory'])
                raise Exception(msg)
            return failed

        requests = []
        for row in self.rows:
            req = RecoveryTripletRequest(_find_failed_from_row(), failover_host_name=row.get('newHostname'),
                                         failover_host_address=row.get('newAddress'),
                                         failover_port=row.get('newPort'), failover_datadir=row.get('newDataDirectory'),
                                         recovery_type=row.get('recovery_type'))
            requests.append(req)

        return self._convert_requests_to_triplets(requests)


    @staticmethod
    def _parseConfigFile(config_file):
        """
        Parse the config file

        Note: if the hostname is not mentioned, the provided address will be populated as host-name

        :param config_file:
        :return: List of dictionaries with each dictionary containing the failed and failover information??
        """
        rows = []
        with open(config_file) as f:
            for lineno, line in line_reader(f):
                groups = line.split()  # NOT line.split(' ') due to MPP-15675
                if len(groups) not in [1, 2]:
                    msg = "line %d of file %s: expected 1 or 2 groups but found %d" % (lineno, config_file, len(groups))
                    raise ExceptionNoStackTraceNeeded(msg)
                parts = groups[0].split('|')

                if len(parts) not in {3, 4, 5}:
                    msg = "line {0} of file {1}: expected 3, 4 or 5 parts on failed segment group, obtained {2}".format(
                        lineno, config_file, len(parts))
                    raise ExceptionNoStackTraceNeeded(msg)
                hostname, address, port, datadir, recovery_type, hostname_check_required = extract_recovery_config_info(parts)
                check_values(lineno, hostname=hostname, address=address, port=port, datadir=datadir)
                datadir = normalizeAndValidateInputPath(datadir, f.name, lineno)

                row = {
                    'failedHostname': hostname,
                    'failedAddress': address,
                    'failedPort': port,
                    'failedDataDirectory': datadir,
                    'lineno': lineno,
                    'hostname_check_required': hostname_check_required,
                    'recovery_type': recovery_type
                }
                if len(groups) == 2:
                    parts2 = groups[1].split('|')
                    if len(parts2) not in [3, 4] or len(parts) != len(parts2):
                        msg = "line {0} of file {1}: expected equal parts, either 3 or 4 on both segment group, obtained {2} on " \
                              "group1 and {3} on group2" .format(
                            lineno, config_file, len(parts), len(parts2))
                        raise ExceptionNoStackTraceNeeded(msg)
                    if len(parts2) == 4:
                        hostname2, address2, port2, datadir2 = parts2
                    else:
                        address2, port2, datadir2 = parts2
                        hostname2 = address2

                    check_values(lineno, hostname=hostname2, address=address2, port=port2, datadir=datadir2)
                    datadir2 = normalizeAndValidateInputPath(datadir2, f.name, lineno)

                    row['recovery_type'] = "Full"
                    row.update({
                        'newHostname': hostname2,
                        'newAddress': address2,
                        'newPort': port2,
                        'newDataDirectory': datadir2
                    })

                rows.append(row)

        RecoveryTripletsUserConfigFile._validate(rows)

        return rows

    @staticmethod
    def _validate(rows):
        """
        Runs checks for making sure all the rows are consistent with each other.
        :param rows:
        :return:
        """
        failed = {}
        new = {}
        for row in rows:
            address, port, datadir, lineno = \
                row['failedAddress'], row['failedPort'], row['failedDataDirectory'], row['lineno']

            if address+datadir in failed:
                msg = 'config file lines {0} and {1} conflict: ' \
                      'Cannot recover the same failed segment {2} and data directory {3} twice.' \
                    .format(failed[address+datadir], lineno, address, datadir)
                raise ExceptionNoStackTraceNeeded(msg)

            failed[address+datadir] = lineno

            if 'newAddress' not in row:
                if address+datadir in new:
                    msg = 'config file lines {0} and {1} conflict: ' \
                          'Cannot recover segment {2} with data directory {3} in place if it is used as a recovery segment.' \
                        .format(new[address+datadir], lineno, address, datadir)
                    raise ExceptionNoStackTraceNeeded(msg)
            else:
                address2, port2, datadir2 = row['newAddress'], row['newPort'], row['newDataDirectory']

                if address2+datadir2 in new:
                    msg = 'config file lines {0} and {1} conflict: ' \
                          'Cannot recover to the same segment {2} and data directory {3} twice.' \
                        .format(new[address2+datadir2], lineno, address2, datadir2)
                    raise ExceptionNoStackTraceNeeded(msg)

                new[address2+datadir2] = lineno

