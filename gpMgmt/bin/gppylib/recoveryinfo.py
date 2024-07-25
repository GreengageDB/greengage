from collections import defaultdict
import datetime
import json
import os

from gppylib import gplog
from gppylib.commands.base import Command, REMOTE


class RecoveryInfo(object):
    """
    This class encapsulates the information needed on a segment host
    to run full/incremental recovery for a segment.

    Note: we don't have target hostname, since an object of this class will be accessed by the target host directly
    """
    def __init__(self, target_datadir, target_port, target_segment_dbid, source_hostname, source_port,
                 source_datadir, is_full_recovery, is_differential_recovery,  progress_file):
        self.target_datadir = target_datadir
        self.target_port = target_port
        self.target_segment_dbid = target_segment_dbid

        # FIXME: use address instead of hostname ?
        self.source_hostname = source_hostname
        self.source_port = source_port
        # source data directory is required in case of differential recovery
        # When doing rsync from source to target
        self.source_datadir = source_datadir
        self.is_full_recovery = is_full_recovery
        self.is_differential_recovery = is_differential_recovery
        self.progress_file = progress_file

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def __eq__(self, cmp_recovery_info):
        return str(self) == str(cmp_recovery_info)


def build_recovery_info(mirrors_to_build):
    """
    This function is used to format recovery information to send to each segment host

    @param mirrors_to_build:  list of mirrors that need recovery

    @return A dictionary with the following format:

            Key   =   <host name>
            Value =   list of RecoveryInfos - one RecoveryInfo per segment on that host
    """
    timestamp = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')

    recovery_info_by_host = defaultdict(list)
    for to_recover in mirrors_to_build:

        source_segment = to_recover.getLiveSegment()
        target_segment = to_recover.getFailoverSegment() or to_recover.getFailedSegment()

        # FIXME: move the progress file naming to gpsegrecovery
        process_name = 'pg_rewind'
        if to_recover.isFullSynchronization():
            process_name = 'pg_basebackup'
        elif to_recover.isDifferentialSynchronization():
            process_name = 'rsync'
        progress_file = '{}/{}.{}.dbid{}.out'.format(gplog.get_logger_dir(), process_name, timestamp,
                                                     target_segment.getSegmentDbId())

        hostname = target_segment.getSegmentHostName()

        recovery_info_by_host[hostname].append(RecoveryInfo(
            target_segment.getSegmentDataDirectory(), target_segment.getSegmentPort(),
            target_segment.getSegmentDbId(), source_segment.getSegmentHostName(),
            source_segment.getSegmentPort(), source_segment.getSegmentDataDirectory(),
            to_recover.isFullSynchronization(), to_recover.isDifferentialSynchronization(), progress_file))
    return recovery_info_by_host


def serialize_list(recovery_info_list):
    return json.dumps(recovery_info_list, default=lambda o: o.__dict__)


#FIXME should we add a test for this function ?
def deserialize_list(serialized_string, class_name=RecoveryInfo):
    if not serialized_string:
        return []
    try:
        deserialized_list = json.loads(serialized_string)
    except ValueError:
        #FIXME should we log the exception ?
        return []
    return [class_name(**i) for i in deserialized_list]


class RecoveryErrorType(object):
    VALIDATION_ERROR = 'validation'
    REWIND_ERROR = 'incremental'
    DIFFERENTIAL_ERROR = 'differential'
    BASEBACKUP_ERROR = 'full'
    START_ERROR = 'start'
    UPDATE_ERROR = 'update'
    DEFAULT_ERROR = 'default'


class RecoveryError(object):
    def __init__(self, error_type, error_msg, dbid, datadir, port, progress_file):
        self.error_type = error_type if error_type else RecoveryErrorType.DEFAULT_ERROR
        self.error_msg = error_msg
        self.dbid = dbid
        self.datadir = datadir
        self.port = port
        self.progress_file = progress_file

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__)

    def __repr__(self):
        return self.__str__()


class RecoveryResult(object):
    def __init__(self, action_name, results, logger):
        self.action_name = action_name
        self._logger = logger
        self._invalid_recovery_errors = defaultdict(list)
        self._setup_recovery_errors = defaultdict(list)
        self._bb_errors = defaultdict(list)
        self._rewind_errors = defaultdict(list)
        self._differential_errors = defaultdict(list)
        self._dbids_that_failed_bb_rewind_differential = set()
        self._start_errors = defaultdict(list)
        self._update_errors = defaultdict(list)
        self._parse_results(results)

    def _parse_results(self, results):
        for host_result in results:
            results = host_result.get_results()
            if not results or results.wasSuccessful():
                continue
            errors_on_host = deserialize_list(results.stderr, class_name=RecoveryError)

            if not errors_on_host:
                self._invalid_recovery_errors[host_result.remoteHost] = results.stderr #FIXME add behave test for invalid errors
            for error in errors_on_host:
                if not error:
                    continue
                if error.error_type == RecoveryErrorType.BASEBACKUP_ERROR:
                    self._bb_errors[host_result.remoteHost].append(error)
                    self._dbids_that_failed_bb_rewind_differential.add(error.dbid)
                elif error.error_type == RecoveryErrorType.REWIND_ERROR:
                    self._dbids_that_failed_bb_rewind_differential.add(error.dbid)
                    self._rewind_errors[host_result.remoteHost].append(error)
                elif error.error_type == RecoveryErrorType.DIFFERENTIAL_ERROR:
                    self._dbids_that_failed_bb_rewind_differential.add(error.dbid)
                    self._differential_errors[host_result.remoteHost].append(error)
                elif error.error_type == RecoveryErrorType.START_ERROR:
                    self._start_errors[host_result.remoteHost].append(error)
                elif error.error_type == RecoveryErrorType.VALIDATION_ERROR:
                    self._setup_recovery_errors[host_result.remoteHost].append(error)
                elif error.error_type == RecoveryErrorType.UPDATE_ERROR:
                    self._update_errors[host_result.remoteHost].append(error)

                #FIXME what should we do for default errors ?

    def _print_invalid_errors(self):
        if self._invalid_recovery_errors:
            for hostname, error in self._invalid_recovery_errors.items():
                self._logger.error("Unable to parse recovery error. hostname: {}, error: {}".format(hostname, error))

    def setup_successful(self):
        return len(self._setup_recovery_errors) == 0 and len(self._invalid_recovery_errors) == 0

    def full_recovery_successful(self):
        return len(self._setup_recovery_errors) == 0 and len(self._bb_errors) == 0 and len(self._invalid_recovery_errors) == 0

    def recovery_successful(self):
        return len(self._setup_recovery_errors) == 0 and len(self._bb_errors) == 0 and len(self._rewind_errors) == 0 and \
               len(self._differential_errors) == 0 and len(self._start_errors) == 0 and len(self._invalid_recovery_errors) == 0 and len(self._update_errors) == 0

    def was_bb_rewind_rsync_successful(self, dbid):
        return dbid not in self._dbids_that_failed_bb_rewind_differential

    def print_setup_recovery_errors(self):
        setup_recovery_error_pattern = " hostname: {}; port: {}; error: {}"
        if len(self._setup_recovery_errors) > 0:
            self._logger.info("----------------------------------------------------------")
            self._logger.info("Failed to setup recovery for the following segments")
            for hostname, errors in self._setup_recovery_errors.items():
                for error in errors:
                    self._logger.error(setup_recovery_error_pattern.format(hostname, error.port, error.error_msg))
        self._print_invalid_errors()

    def get_timestamp(self, errLine):
        date_str = errLine.split()[0]
        time_str = errLine.split()[1]
        timestamp_str = date_str + " " + time_str
        return datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')


    def get_last_reported_error(self, gpdb_err, start_err):
        """
        Segment startup related errors can be either reported in startup.log or gpdb*.csv file.
        This depends on when the error is encountered during postmaster startup process.
        This function fetches and compares the timestamp of errors found in both the files and returns the
        error with latest timestamp.
        """
        if not gpdb_err and not start_err:
            return "None"
        if not gpdb_err:
            return start_err
        if not start_err:
            return gpdb_err

        timestamp1 = self.get_timestamp(gpdb_err)
        timestamp2 = self.get_timestamp(start_err)
        if timestamp1 >= timestamp2:
            return gpdb_err

        return start_err

    def get_current_logfile_path(self, datadir, hostname):
        current_logfile_path = os.path.join(datadir, 'current_logfiles')
        cmdStr = "set -o pipefail; cat {} | awk '{{print $ 2}}'".format(current_logfile_path)
        cmd = Command(name="Get current logfile that has startUp errors", cmdStr=cmdStr, ctxt=REMOTE, remoteHost=hostname)
        cmd.run()

        if cmd.get_results().rc != 0:
            self._logger.warn("Failed to read current_logfile %s" % current_logfile_path)
            return None

        return cmd.get_results().stdout.strip()

    def get_error_from_logfile(self, progress_file, hostname):
        """
        Traverse the error file to fetch last registered occurrence of 'error, panic or fatal'.
        'fatal: postgres single-user mode of target instance failed for command' is removed from the search pattern
        as it is logged after every failed pg_rewind operation.
        To ensure grep doesn't return a non-zero exit code, it is ORed with true.
        """
        cmdStr = 'set -o pipefail; cat {} | (grep -i "ERROR\|PANIC\|FATAL" | grep -v "fatal: postgres single-user mode of target instance failed for command" || true) | tail -1'.format(progress_file)
        cmd = Command(name="Parse logfile for errors on a remote host", cmdStr=cmdStr, ctxt=REMOTE, remoteHost=hostname)
        cmd.run()

        if cmd.get_results().rc != 0:
            self._logger.debug("Failed while parsing the logfile %s" % progress_file)
            return None

        return cmd.get_results().stdout.strip()

    def print_bb_rewind_differential_update_and_start_errors(self):
        bb_rewind_differential_error_pattern = " hostname: {}; port: {}; logfile: {}; recoverytype: {}; error: {}"
        if len(self._bb_errors) > 0 or len(self._rewind_errors) > 0 or len(self._differential_errors) > 0:
            self._logger.info("----------------------------------------------------------")
            if len(self._rewind_errors) > 0:
                self._logger.info("Failed to {} the following segments. You must run either gprecoverseg --differential"
                                  " or gprecoverseg -F for all incremental failures".format(self.action_name))
            elif len(self._differential_errors) > 0:
                self._logger.info("Failed to {} the following segments. You must run either gprecoverseg --differential"
                                  " or gprecoverseg -F for all differential failures".format(self.action_name))
            else:
                self._logger.info("Failed to {} the following segments".format(self.action_name))
            for hostname, errors in self._rewind_errors.items():
                for error in errors:
                    self._logger.info(bb_rewind_differential_error_pattern.format(hostname, error.port, error.progress_file,
                                                                     error.error_type, self.get_error_from_logfile(error.progress_file, hostname)))

            for hostname, errors in self._differential_errors.items():
                for error in errors:
                    self._logger.info(bb_rewind_differential_error_pattern.format(hostname, error.port, error.progress_file,
                                                                     error.error_type, self.get_error_from_logfile(error.progress_file, hostname)))
            for hostname, errors in self._bb_errors.items():
                for error in errors:
                    self._logger.info(bb_rewind_differential_error_pattern.format(hostname, error.port, error.progress_file,
                                                                 error.error_type, self.get_error_from_logfile(error.progress_file, hostname)))

        if len(self._start_errors) > 0:
            start_error_pattern = " hostname: {}; port: {}; datadir: {}; error: {}"
            self._logger.info("----------------------------------------------------------")
            self._logger.info("Failed to start the following segments. "
                              "Please check the latest logs located in segment's data directory")

            for hostname, errors in self._start_errors.items():
                for error in errors:
                    gpdb_logfile_error = ''
                    startup_logfile_error = ''

                    #Fetch error from current gpdb-*.csv logfile
                    gpdb_logfile_path = self.get_current_logfile_path(error.datadir, hostname)
                    if gpdb_logfile_path:
                        gpdb_logfile = os.path.join(error.datadir, gpdb_logfile_path)
                        gpdb_logfile_error = self.get_error_from_logfile(gpdb_logfile, hostname)

                    #Fetch error from startup logfile
                    startup_logfile = os.path.join(error.datadir, 'log/startup.log')
                    startup_logfile_error = self.get_error_from_logfile(startup_logfile, hostname)

                    #Report startup error with latest timestamp
                    self._logger.info(start_error_pattern.format(hostname, error.port, error.datadir,
                                      self.get_last_reported_error(gpdb_logfile_error, startup_logfile_error)))


        if len(self._update_errors) > 0:
            update_error_pattern = " hostname: {}; port: {}; datadir: {}"
            self._logger.info("----------------------------------------------------------")
            self._logger.info("Did not start the following segments due to failure while updating the port."
                              "Please update the port in postgresql.conf located in the segment's data directory")

            for hostname, errors in self._update_errors.items():
                for error in errors:
                    self._logger.info(update_error_pattern.format(hostname, error.port, error.datadir))

        self._print_invalid_errors()
