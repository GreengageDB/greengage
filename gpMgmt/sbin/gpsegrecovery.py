#!/usr/bin/env python3
import re

from gppylib.recoveryinfo import RecoveryErrorType
from gppylib.commands.pg import PgBaseBackup, PgRewind
from recovery_base import RecoveryBase, set_recovery_cmd_results
from gppylib.commands.base import Command
from gppylib.commands.gp import SegmentStart
from gppylib.gparray import Segment
from gppylib.commands.gp import ModifyConfSetting


class FullRecovery(Command):
    def __init__(self, name, recovery_info, forceoverwrite, logger, era):
        self.name = name
        self.recovery_info = recovery_info
        self.replicationSlotName = 'internal_wal_replication_slot'
        self.forceoverwrite = forceoverwrite
        self.era = era
        # FIXME test for this cmdstr. also what should this cmdstr be ?
        cmdStr = ''
        #cmdstr = 'TODO? : {} {}'.format(str(recovery_info), self.verbose)
        Command.__init__(self, self.name, cmdStr)
        #FIXME this logger has to come after the init and is duplicated in all the 4 classes
        self.logger = logger
        self.error_type = RecoveryErrorType.DEFAULT_ERROR

    @set_recovery_cmd_results
    def run(self):
        self.error_type = RecoveryErrorType.BASEBACKUP_ERROR
        cmd = PgBaseBackup(self.recovery_info.target_datadir,
                           self.recovery_info.source_hostname,
                           str(self.recovery_info.source_port),
                           create_slot=True,
                           replication_slot_name=self.replicationSlotName,
                           forceoverwrite=self.forceoverwrite,
                           target_gp_dbid=self.recovery_info.target_segment_dbid,
                           progress_file=self.recovery_info.progress_file)
        self.logger.info("Running pg_basebackup with progress output temporarily in %s" % self.recovery_info.progress_file)
        cmd.run(validateAfter=True)
        self.error_type = RecoveryErrorType.DEFAULT_ERROR
        self.logger.info("Successfully ran pg_basebackup for dbid: {}".format(
            self.recovery_info.target_segment_dbid))

        # Updating port number on conf after recovery
        self.error_type = RecoveryErrorType.UPDATE_ERROR
        update_port_in_conf(self.recovery_info, self.logger)

        self.error_type = RecoveryErrorType.START_ERROR
        start_segment(self.recovery_info, self.logger, self.era)


class IncrementalRecovery(Command):
    def __init__(self, name, recovery_info, logger, era):
        self.name = name
        self.recovery_info = recovery_info
        self.era = era
        cmdStr = ''
        Command.__init__(self, self.name, cmdStr)
        self.logger = logger
        self.error_type = RecoveryErrorType.DEFAULT_ERROR

    @set_recovery_cmd_results
    def run(self):
        self.logger.info("Running pg_rewind with progress output temporarily in %s" % self.recovery_info.progress_file)
        self.error_type = RecoveryErrorType.REWIND_ERROR
        cmd = PgRewind('rewind dbid: {}'.format(self.recovery_info.target_segment_dbid),
                       self.recovery_info.target_datadir, self.recovery_info.source_hostname,
                       self.recovery_info.source_port, self.recovery_info.progress_file)
        cmd.run(validateAfter=True)
        self.logger.info("Successfully ran pg_rewind for dbid: {}".format(self.recovery_info.target_segment_dbid))

        # Updating port number on conf after recovery
        self.error_type = RecoveryErrorType.UPDATE_ERROR
        update_port_in_conf(self.recovery_info, self.logger)

        self.error_type = RecoveryErrorType.START_ERROR
        start_segment(self.recovery_info, self.logger, self.era)


def start_segment(recovery_info, logger, era):
    seg = Segment(None, None, None, None, None, None, None, None,
                  recovery_info.target_port, recovery_info.target_datadir)
    cmd = SegmentStart(
        name="Starting new segment with dbid %s:" % (str(recovery_info.target_segment_dbid))
        , gpdb=seg
        , numContentsInCluster=0
        , era=era
        , mirrormode="mirror"
        , utilityMode=True)
    logger.info(str(cmd))
    cmd.run(validateAfter=True)


def update_port_in_conf(recovery_info, logger):
    logger.info("Updating %s/postgresql.conf" % recovery_info.target_datadir)
    conf_file = "{}/{}".format(recovery_info.target_datadir, 'postgresql.conf')
    modifyConfCmd = ModifyConfSetting('Updating %s/postgresql.conf' % recovery_info.target_datadir,
                                      conf_file,
                                      'port', recovery_info.target_port, optType='number')
    modifyConfCmd.run(validateAfter=True)

    # ModifyConfSetting's in-place substitution reports success even when its
    # pattern matched nothing. If the port is left at the basebackup source's
    # value, the segment still starts fine (pg_ctl is pointed at the catalog
    # port) but the config on disk is silently wrong. Fail the recovery loudly
    # instead.
    port_pattern = re.compile(r'^\s*port\s*=\s*{}(\D|$)'.format(int(recovery_info.target_port)))
    with open(conf_file) as fd:
        if not any(port_pattern.match(line) for line in fd):
            raise Exception("Failed to update port to {} in {}".format(
                recovery_info.target_port, conf_file))


#FIXME we may not need this class
class SegRecovery(object):
    def __init__(self):
        pass

    def main(self):
        recovery_base = RecoveryBase(__file__)
        recovery_base.main(self.get_recovery_cmds(recovery_base.seg_recovery_info_list, recovery_base.options.forceoverwrite,
                                                  recovery_base.logger, recovery_base.options.era))

    def get_recovery_cmds(self, seg_recovery_info_list, forceoverwrite, logger, era):
        cmd_list = []
        for seg_recovery_info in seg_recovery_info_list:
            if seg_recovery_info.is_full_recovery:
                cmd = FullRecovery(name='Run pg_basebackup',
                                   recovery_info=seg_recovery_info,
                                   forceoverwrite=forceoverwrite,
                                   logger=logger,
                                   era=era)
            else:
                cmd = IncrementalRecovery(name='Run pg_rewind',
                                          recovery_info=seg_recovery_info,
                                          logger=logger,
                                          era=era)
            cmd_list.append(cmd)
        return cmd_list


if __name__ == '__main__':
    SegRecovery().main()
