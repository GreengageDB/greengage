#!/usr/bin/env python
# Line too long            - pylint: disable=C0301
# Invalid name             - pylint: disable=C0103
#
# Copyright (c) Greenplum Inc 2010. All Rights Reserved.
#
# Note: the option to recover to a new host is not very good if we have a multi-home configuration
#
# Options removed when 4.0 gprecoverseg was implemented:
#        --version
#       -S "Primary segment dbid to force recovery": I think this is done now by bringing the primary down, waiting for
#           failover, and then doing recover full
#       -z "Primary segment data dir and host to force recovery" see removed -S option for comment
#       -f        : force Greenplum Database instance shutdown and restart
#       -F (HAS BEEN CHANGED) -- used to mean "force recovery" and now means "full recovery)
#
# import mainUtils FIRST to get python version check
# THIS IMPORT SHOULD COME FIRST
from gppylib.mainUtils import *

from optparse import OptionGroup
import glob, os, sys, signal, shutil, time
from gppylib import gparray, gplog, userinput, utils
from gppylib.util import gp_utils
from gppylib.commands import gp, pg, unix
from gppylib.commands.base import Command, WorkerPool
from gppylib.db import dbconn
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.operations.detect_unreachable_hosts import get_unreachable_segment_hosts
from gppylib.operations.startSegments import *
from gppylib.operations.buildMirrorSegments import *
from gppylib.operations.rebalanceSegments import GpSegmentRebalanceOperation
from gppylib.operations.update_pg_hba_on_segments import update_pg_hba_on_segments
from gppylib.programs import programIoUtils
from gppylib.system import configurationInterface as configInterface
from gppylib.system.environment import GpMasterEnvironment
from gppylib.parseutils import line_reader, check_values, canonicalize_address
from gppylib.utils import writeLinesToFile, normalizeAndValidateInputPath, TableLogger
from gppylib.operations.utils import ParallelOperation
from gppylib.operations.package import SyncPackages
from gppylib.heapchecksum import HeapChecksum
from gppylib.mainUtils import ExceptionNoStackTraceNeeded
from gppylib.programs.clsRecoverSegment_triples import RecoveryTripletsFactory

logger = gplog.get_default_logger()


class GpRecoverSegmentProgram:
    #
    # Constructor:
    #
    # @param options the options as returned by the options parser
    #
    def __init__(self, options):
        self.__options = options
        self.__pool = None
        self.logger = logger

        # If user did not specify a value for showProgressInplace and
        # stdout is a tty then send escape sequences to gprecoverseg
        # output. Otherwise do not show progress inplace.
        if self.__options.showProgressInplace is None:
            self.__options.showProgressInplace = sys.stdout.isatty()


    def getProgressMode(self):
        if self.__options.showProgress:
            if self.__options.showProgressInplace:
                progressMode = GpMirrorListToBuild.Progress.INPLACE
            else:
                progressMode = GpMirrorListToBuild.Progress.SEQUENTIAL
        else:
            progressMode = GpMirrorListToBuild.Progress.NONE

        return progressMode


    def outputToFile(self, mirrorBuilder, gpArray, fileName):
        lines = []

        # Entry for a failed segment will be commented if failed segment host is unreachable to inform the user about
        # those unreachable hosts. As we know gprecoverseg skips the recovery of a segment if host is unreachable so if
        # the user wants to recover it to another host, they can do so by uncommenting the line and adding the
        # failover details.
        lines.append("# If any entry is commented, please know that it belongs to failed segment which is unreachable."
                     "\n# If you need to recover them, please modify the segment entry and add failover details "
                     "\n# (failed_addresss|failed_port|failed_dataDirectory<space>failover_addresss|failover_port|failover_dataDirectory) "
                     "to recover it to another host.\n")

        # one entry for each failure
        for mirror in mirrorBuilder.getMirrorsToBuild():
            output_str = ""
            seg = mirror.getFailedSegment()
            addr = canonicalize_address(seg.getSegmentAddress())
            output_str += ('%s|%d|%s' % (addr, seg.getSegmentPort(), seg.getSegmentDataDirectory()))
            if seg.unreachable:
                # Entry is commented if failed segment host is unreachable
                output_str = "#{}".format(output_str)

            seg = mirror.getFailoverSegment()
            if seg is not None:

                output_str += ' '
                addr = canonicalize_address(seg.getSegmentAddress())
                output_str += ('%s|%d|%s' % (
                    addr, seg.getSegmentPort(), seg.getSegmentDataDirectory()))

            lines.append(output_str)
        writeLinesToFile(fileName, lines)

    def getRecoveryActionsBasedOnOptions(self, gpEnv, gpArray):
        if self.__options.rebalanceSegments:
            return GpSegmentRebalanceOperation(gpEnv, gpArray, self.__options.parallelDegree, self.__options.parallelPerHost, self.__options.replayLag)
        instance = RecoveryTripletsFactory.instance(gpArray, self.__options.recoveryConfigFile, self.__options.newRecoverHosts, self.__options.outputSampleConfigFile, self.__options.parallelDegree)
        segs = [GpMirrorToBuild(t.failed, t.live, t.failover, self.__options.forceFullResynchronization, self.__options.differentialResynchronization, t.recovery_type)
                for t in instance.getTriplets()]
        return GpMirrorListToBuild(segs, self.__pool, self.__options.quiet,
                                   self.__options.parallelDegree,
                                   instance.getInterfaceHostnameWarnings(),
                                   forceoverwrite=True,
                                   progressMode=self.getProgressMode(),
                                   parallelPerHost=self.__options.parallelPerHost)

    def syncPackages(self, new_hosts):
        # The design decision here is to squash any exceptions resulting from the
        # synchronization of packages. We should *not* disturb the user's attempts to recover.
        try:
            self.logger.info('Syncing Greenplum Database extensions')
            operations = [SyncPackages(host) for host in new_hosts]
            ParallelOperation(operations, self.__options.parallelDegree).run()
            # introspect outcomes
            for operation in operations:
                operation.get_ret()
        except:
            self.logger.exception('Syncing of Greenplum Database extensions has failed.')
            self.logger.warning('Please run gppkg --clean after successful segment recovery.')

    def displayRecovery(self, mirrorBuilder, gpArray):
        self.logger.info('Greenplum instance recovery parameters')
        self.logger.info('---------------------------------------------------------')

        if self.__options.recoveryConfigFile:
            self.logger.info('Recovery from configuration -i option supplied')
        elif self.__options.newRecoverHosts is not None:
            self.logger.info('Recovery type              = Pool Host')
            for h in self.__options.newRecoverHosts:
                self.logger.info('Pool host for recovery     = %s' % h)
        elif self.__options.rebalanceSegments:
            self.logger.info('Recovery type              = Rebalance')
        else:
            self.logger.info('Recovery type              = Standard')

        if self.__options.rebalanceSegments:
            i = 1
            total = len(gpArray.get_unbalanced_segdbs())
            for toRebalance in gpArray.get_unbalanced_segdbs():
                tabLog = TableLogger()
                self.logger.info('---------------------------------------------------------')
                self.logger.info('Unbalanced segment %d of %d' % (i, total))
                self.logger.info('---------------------------------------------------------')
                programIoUtils.appendSegmentInfoForOutput("Unbalanced", gpArray, toRebalance, tabLog)
                tabLog.info(["Balanced role", "= Primary" if toRebalance.preferred_role == 'p' else "= Mirror"])
                tabLog.info(["Current role", "= Primary" if toRebalance.role == 'p' else "= Mirror"])
                tabLog.outputTable()
                i += 1
        else:
            i = 0
            total = len(mirrorBuilder.getMirrorsToBuild())
            for toRecover in mirrorBuilder.getMirrorsToBuild():
                self.logger.info('---------------------------------------------------------')
                self.logger.info('Recovery %d of %d' % (i + 1, total))
                self.logger.info('---------------------------------------------------------')

                tabLog = TableLogger()

                syncMode = "Incremental"
                if toRecover.isFullSynchronization():
                    syncMode = "Full"
                elif toRecover.isDifferentialSynchronization():
                    syncMode = "Differential"
                tabLog.info(["Synchronization mode", "= " + syncMode])
                programIoUtils.appendSegmentInfoForOutput("Failed", gpArray, toRecover.getFailedSegment(), tabLog)
                programIoUtils.appendSegmentInfoForOutput("Recovery Source", gpArray, toRecover.getLiveSegment(),
                                                          tabLog)

                if toRecover.getFailoverSegment() is not None:
                    programIoUtils.appendSegmentInfoForOutput("Recovery Target", gpArray,
                                                              toRecover.getFailoverSegment(), tabLog)
                else:
                    tabLog.info(["Recovery Target", "= in-place"])
                tabLog.outputTable()

                i = i + 1

        self.logger.info('---------------------------------------------------------')

    def __getSimpleSegmentLabel(self, seg):
        addr = canonicalize_address(seg.getSegmentAddress())
        return "%s:%s" % (addr, seg.getSegmentDataDirectory())

    def __displayRecoveryWarnings(self, mirrorBuilder):
        for warning in self._getRecoveryWarnings(mirrorBuilder):
            self.logger.warn(warning)

    def _getRecoveryWarnings(self, mirrorBuilder):
        """
        return an array of string warnings regarding the recovery
        """
        res = []
        for toRecover in mirrorBuilder.getMirrorsToBuild():

            if toRecover.getFailoverSegment() is not None:
                #
                # user specified a failover location -- warn if it's the same host as its primary
                #
                src = toRecover.getLiveSegment()
                dest = toRecover.getFailoverSegment()

                if src.getSegmentHostName() == dest.getSegmentHostName():
                    res.append("Segment is being recovered to the same host as its primary: "
                               "primary %s    failover target: %s"
                               % (self.__getSimpleSegmentLabel(src), self.__getSimpleSegmentLabel(dest)))

        for warning in mirrorBuilder.getAdditionalWarnings():
            res.append(warning)

        return res

    def _get_dblist(self):
        # template0 does not accept any connections so we exclude it
        with dbconn.connect(dbconn.DbURL()) as conn:
            res = dbconn.execSQL(conn, "SELECT datname FROM PG_DATABASE WHERE datname != 'template0'")
        return res.fetchall()

    def run(self):
        if self.__options.parallelDegree < 1 or self.__options.parallelDegree > gp.MAX_MASTER_NUM_WORKERS:
            raise ProgramArgumentValidationException(
                "Invalid parallelDegree value provided with -B argument: %d" % self.__options.parallelDegree)
        if self.__options.parallelPerHost < 1 or self.__options.parallelPerHost > gp.MAX_SEGHOST_NUM_WORKERS:
            raise ProgramArgumentValidationException(
                "Invalid parallelPerHost value provided with -b argument: %d" % self.__options.parallelPerHost)

        self.__pool = WorkerPool(self.__options.parallelDegree)
        gpEnv = GpMasterEnvironment(self.__options.masterDataDirectory, True)

        # verify "where to recover" options
        optionCnt = 0
        if self.__options.newRecoverHosts is not None:
            optionCnt += 1
        if self.__options.rebalanceSegments:
            optionCnt += 1
        if optionCnt > 1:
            raise ProgramArgumentValidationException("Only one of -p and -r may be specified")
        if optionCnt > 0 and self.__options.recoveryConfigFile is not None:
            raise ProgramArgumentValidationException("Only one of -i, -p and -r may be specified")

        if optionCnt > 0 and self.__options.differentialResynchronization:
            raise ProgramArgumentValidationException("Only one of -p, -r and --differential may be specified")

        # verify "mode to recover" options
        if self.__options.forceFullResynchronization and self.__options.differentialResynchronization:
            raise ProgramArgumentValidationException("Only one of -F and --differential may be specified")

        # verify differential supported options
        if self.__options.differentialResynchronization and self.__options.outputSampleConfigFile:
            raise ProgramArgumentValidationException("Invalid -o provided with --differential argument")

        if self.__options.replayLag and not self.__options.rebalanceSegments:
            raise ProgramArgumentValidationException("--replay-lag should be used only with -r")

        # Checking rsync version before performing a differential recovery operation.
        # the --info=progress2 option, which provides whole file transfer progress, requires rsync 3.1.0 or above
        min_rsync_ver = "3.1.0"
        if self.__options.differentialResynchronization and not unix.validate_rsync_version(min_rsync_ver):
            raise ProgramArgumentValidationException("To perform a differential recovery, a minimum rsync version "
                                                         "of {0} is required. Please ensure that rsync is updated to "
                                                         "version {0} or higher.".format(min_rsync_ver))

        faultProberInterface.getFaultProber().initializeProber(gpEnv.getMasterPort())

        confProvider = configInterface.getConfigurationProvider().initializeProvider(gpEnv.getMasterPort())

        gpArray = confProvider.loadSystemConfig(useUtilityMode=False)

        if not gpArray.hasMirrors:
            raise ExceptionNoStackTraceNeeded(
                'GPDB Mirroring replication is not configured for this Greenplum Database instance.')

        num_workers = min(len(gpArray.get_hostlist()), self.__options.parallelDegree)
        hosts = set(gpArray.get_hostlist(includeMaster=False))
        unreachable_hosts = get_unreachable_segment_hosts(hosts, num_workers)
        for i, segmentPair in enumerate(gpArray.segmentPairs):
            if segmentPair.primaryDB.getSegmentHostName() in unreachable_hosts:
                logger.warning("Not recovering segment %d because %s is unreachable" % (segmentPair.primaryDB.dbid, segmentPair.primaryDB.getSegmentHostName()))
                gpArray.segmentPairs[i].primaryDB.unreachable = True

            if segmentPair.mirrorDB.getSegmentHostName() in unreachable_hosts:
                logger.warning("Not recovering segment %d because %s is unreachable" % (segmentPair.mirrorDB.dbid, segmentPair.mirrorDB.getSegmentHostName()))
                gpArray.segmentPairs[i].mirrorDB.unreachable = True

        # We have phys-rep/filerep mirrors.

        if self.__options.newRecoverHosts is not None:
            try:
                uniqueHosts = []
                for h in self.__options.newRecoverHosts.split(','):
                    if h.strip() not in uniqueHosts:
                        uniqueHosts.append(h.strip())
                self.__options.newRecoverHosts = uniqueHosts
            except Exception, ex:
                raise ProgramArgumentValidationException( \
                    "Invalid value for recover hosts: %s" % ex)

        # retain list of hosts that were existing in the system prior to getRecoverActions...
        # this will be needed for later calculations that determine whether
        # new hosts were added into the system
        existing_hosts = set(gpArray.getHostList())

        # figure out what needs to be done
        mirrorBuilder = self.getRecoveryActionsBasedOnOptions(gpEnv, gpArray)

        if self.__options.outputSampleConfigFile is not None:
            # just output config file and done
            self.outputToFile(mirrorBuilder, gpArray, self.__options.outputSampleConfigFile)
            self.logger.info('Configuration file output to %s successfully.' % self.__options.outputSampleConfigFile)
        elif self.__options.rebalanceSegments:
            assert (isinstance(mirrorBuilder, GpSegmentRebalanceOperation))

            # Make sure we have work to do
            if len(gpArray.get_unbalanced_segdbs()) == 0:
                self.logger.info("No segments are running in their non-preferred role and need to be rebalanced.")
            else:
                self.displayRecovery(mirrorBuilder, gpArray)

                if self.__options.interactive:
                    self.logger.warn("This operation will cancel queries that are currently executing.")
                    self.logger.warn("Connections to the database however will not be interrupted.")
                    if not userinput.ask_yesno(None, "\nContinue with segment rebalance procedure", 'N'):
                        raise UserAbortedException()

                fullRebalanceDone = mirrorBuilder.rebalance()
                self.logger.info("******************************************************************")
                if fullRebalanceDone:
                    self.logger.info("The rebalance operation has completed successfully.")
                else:
                    self.logger.info("The rebalance operation has completed with WARNINGS."
                                     " Please review the output in the gprecoverseg log.")
                self.logger.info("******************************************************************")

        elif len(mirrorBuilder.getMirrorsToBuild()) == 0:
            self.logger.info('No segments to recover')
        else:
            #TODO this already happens in buildMirrors function
            mirrorBuilder.checkForPortAndDirectoryConflicts(gpArray)
            self.validate_heap_checksum_consistency(gpArray, mirrorBuilder)

            self.displayRecovery(mirrorBuilder, gpArray)
            self.__displayRecoveryWarnings(mirrorBuilder)

            if self.__options.interactive:
                if not userinput.ask_yesno(None, "\nContinue with segment recovery procedure", 'N'):
                    raise UserAbortedException()

            # sync packages
            current_hosts = set(gpArray.getHostList())
            new_hosts = current_hosts - existing_hosts
            if new_hosts:
                self.syncPackages(new_hosts)

            contentsToUpdate = [seg.getLiveSegment().getSegmentContentId() for seg in mirrorBuilder.getMirrorsToBuild()]
            update_pg_hba_on_segments(gpArray, self.__options.hba_hostnames, self.__options.parallelDegree, contentsToUpdate)
            if not mirrorBuilder.recover_mirrors(gpEnv, gpArray):
                if self.__options.differentialResynchronization:
                    self.logger.error("gprecoverseg differential recovery failed. Please check the gpsegrecovery.py log"
                                      " file and rsync log file for more details.")
                else:
                    self.logger.error("gprecoverseg failed. Please check the output for more details.")
                sys.exit(1)

            self.logger.info("********************************")
            self.logger.info(
                "Future gprecoverseg executions might remove the currently created pg_basebackup/pg_rewind/rsync "
                "progress files, please save these files if needed.")
            self.logger.info("********************************")
            self.logger.info("Segments successfully recovered.")
            self.logger.info("********************************")

            self.logger.info("Recovered mirror segments need to sync WAL with primary segments.")
            self.logger.info("Use 'gpstate -e' to check progress of WAL sync remaining bytes")

        sys.exit(0)

    def validate_heap_checksum_consistency(self, gpArray, mirrorBuilder):
        live_segments = [target.getLiveSegment() for target in mirrorBuilder.getMirrorsToBuild()]
        failed_segments = [target.getFailedSegment() for target in mirrorBuilder.getMirrorsToBuild()]
        if len(live_segments) == 0:
            self.logger.info("No checksum validation necessary when there are no segments to recover.")
            return

        heap_checksum = HeapChecksum(gpArray, num_workers=min(self.__options.parallelDegree, len(live_segments)), logger=self.logger)
        successes, failures = heap_checksum.get_segments_checksum_settings(live_segments + failed_segments)
        # go forward if we have at least one segment that has replied
        if len(successes) == 0:
            raise Exception("No segments responded to ssh query for heap checksum validation.")
        consistent, inconsistent, master_checksum_value = heap_checksum.check_segment_consistency(successes)
        if len(inconsistent) > 0:
            self.logger.fatal("Heap checksum setting differences reported on segments")
            self.logger.fatal("Failed checksum consistency validation:")
            for gpdb in inconsistent:
                segment_name = gpdb.getSegmentHostName()
                checksum = gpdb.heap_checksum
                self.logger.fatal("%s checksum set to %s differs from master checksum set to %s" %
                                  (segment_name, checksum, master_checksum_value))
            raise Exception("Heap checksum setting differences reported on segments")
        self.logger.info("Heap checksum setting is consistent between master and the segments that are candidates "
                         "for recoverseg")

    def cleanup(self):
        if self.__pool:
            self.__pool.haltWork()  # \  MPP-13489, CR-2572
            self.__pool.joinWorkers()  # > all three of these appear necessary
            self.__pool.join()  # /  see MPP-12633, CR-2252 as well

    # -------------------------------------------------------------------------

    @staticmethod
    def createParser():

        description = ("Recover a failed segment")
        help = [""]

        parser = OptParser(option_class=OptChecker,
                           description=' '.join(description.split()),
                           version='%prog version $Revision$')
        parser.setHelp(help)

        loggingGroup = addStandardLoggingAndHelpOptions(parser, True)
        loggingGroup.add_option("-s", None, default=None, action='store_false',
                                dest='showProgressInplace',
                                help='Show pg_basebackup progress sequentially instead of inplace')
        loggingGroup.add_option("--no-progress",
                                dest="showProgress", default=True, action="store_false",
                                help="Suppress pg_basebackup progress output")

        addTo = OptionGroup(parser, "Connection Options")
        parser.add_option_group(addTo)
        addMasterDirectoryOptionForSingleClusterProgram(addTo)

        addTo = OptionGroup(parser, "Recovery Source Options")
        parser.add_option_group(addTo)
        addTo.add_option("-i", None, type="string",
                         dest="recoveryConfigFile",
                         metavar="<configFile>",
                         help="Recovery configuration file")
        addTo.add_option("-o", None,
                         dest="outputSampleConfigFile",
                         metavar="<configFile>", type="string",
                         help="Sample configuration file name to output; "
                              "this file can be passed to a subsequent call using -i option")

        addTo = OptionGroup(parser, "Recovery Destination Options")
        parser.add_option_group(addTo)
        addTo.add_option("-p", None, type="string",
                         dest="newRecoverHosts",
                         metavar="<targetHosts>",
                         help="Spare new hosts to which to recover segments")

        addTo = OptionGroup(parser, "Recovery Options")
        parser.add_option_group(addTo)
        addTo.add_option('-F', None, default=False, action='store_true',
                         dest="forceFullResynchronization",
                         metavar="<forceFullResynchronization>",
                         help="Force full segment resynchronization")
        addTo.add_option('--differential', None, default=False, action='store_true',
                         dest="differentialResynchronization",
                         metavar="<differentialResynchronization>",
                         help="differential segment resynchronization")
        addTo.add_option("-B", None, type="int", default=gp.DEFAULT_MASTER_NUM_WORKERS,
                         dest="parallelDegree",
                         metavar="<parallelDegree>",
                         help="Max number of hosts to operate on in parallel. Valid values are: 1-%d"
                              % gp.MAX_MASTER_NUM_WORKERS)
        addTo.add_option("-b", None, type="int", default=gp.DEFAULT_SEGHOST_NUM_WORKERS,
                         dest="parallelPerHost",
                         metavar="<parallelPerHost>",
                         help="Max number of segments per host to operate on in parallel. Valid values are: 1-%d"
                              % gp.MAX_SEGHOST_NUM_WORKERS)

        addTo.add_option("-r", None, default=False, action='store_true',
                         dest='rebalanceSegments', help='Rebalance synchronized segments.')
        addTo.add_option("--replay-lag", None, type="float",
                         dest="replayLag",
                         metavar="<replayLag>", help='Allowed replay lag on mirror, lag should be provided in GBs')
        addTo.add_option('', '--hba-hostnames', action='store_true', dest='hba_hostnames',
                         help='use hostnames instead of CIDR in pg_hba.conf')

        parser.set_defaults()
        return parser

    @staticmethod
    def createProgram(options, args):
        if len(args) > 0:
            raise ProgramArgumentValidationException("too many arguments: only options may be specified", True)
        return GpRecoverSegmentProgram(options)

    @staticmethod
    def mainOptions():
        """
        The dictionary this method returns instructs the simple_main framework
        to check for a gprecoverseg.lock file under MASTER_DATA_DIRECTORY to
        prevent the customer from trying to run more than one instance of
        gprecoverseg at the same time.
        """
        return {'pidlockpath': 'gprecoverseg.lock', 'parentpidvar': 'GPRECOVERPID'}
