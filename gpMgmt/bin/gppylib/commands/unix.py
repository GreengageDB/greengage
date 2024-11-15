#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved. 
#
"""
Set of Classes for executing unix commands.
"""
import os
import platform
import psutil
import pwd
import socket
import signal
import uuid
import pipes
import re
from distutils.version import LooseVersion

from gppylib.gplog import get_default_logger
from gppylib.commands.base import *

logger = get_default_logger()

# ---------------platforms--------------------
# global variable for our platform
SYSTEM = "unknown"

LINUX = "linux"
DARWIN = "darwin"
FREEBSD = "freebsd"
OPENBSD = "openbsd"
platform_list = [LINUX, DARWIN, FREEBSD, OPENBSD]

curr_platform = platform.uname()[0].lower()

GPHOME = os.environ.get('GPHOME', None)

# ---------------command path--------------------
CMDPATH = ['/usr/kerberos/bin', '/usr/sfw/bin', '/opt/sfw/bin', '/usr/local/bin', '/bin',
           '/usr/bin', '/sbin', '/usr/sbin', '/usr/ucb', '/sw/bin', '/opt/Navisphere/bin']

if GPHOME:
    CMDPATH.append(GPHOME)

CMD_CACHE = {}


# ----------------------------------
class CommandNotFoundException(Exception):
    def __init__(self, cmd, paths):
        self.cmd = cmd
        self.paths = paths

    def __str__(self):
        return "Could not locate command: '%s' in this set of paths: %s" % (self.cmd, repr(self.paths))


def findCmdInPath(cmd):
    global CMD_CACHE

    if cmd not in CMD_CACHE:
        for p in CMDPATH:
            f = os.path.join(p, cmd)
            if os.path.exists(f):
                CMD_CACHE[cmd] = f
                return f

        logger.critical('Command %s not found' % cmd)
        search_path = CMDPATH[:]
        raise CommandNotFoundException(cmd, search_path)
    else:
        return CMD_CACHE[cmd]


# For now we'll leave some generic functions outside of the Platform framework
def getLocalHostname():
    return socket.gethostname().split('.')[0]


def getUserName():
    return pwd.getpwuid(os.getuid()).pw_name


def getHomePath():
    return pwd.getpwuid(os.getuid()).pw_dir


def check_pid_on_remotehost(pid, host):
    """ Check For the existence of a unix pid on remote host. """

    if pid == 0:
        return False

    cmd = Command(name='check pid on remote host', cmdStr='kill -0 %d' % pid, ctxt=REMOTE, remoteHost=host)
    cmd.run()
    if cmd.get_results().rc == 0:
        return True

    return False


def check_pid(pid):
    """ Check For the existence of a unix pid. """

    if pid == 0:
        return False

    try:
        os.kill(int(pid), signal.SIG_DFL)
    except OSError:
        return False
    else:
        return True


"""
Given the data directory, pid list and host,
kill -9 all the processes from the pid list.
"""
def kill_9_segment_processes(datadir, pids, host):
    logger.info('Terminating processes for segment {0}'.format(datadir))

    for pid in pids:
        if check_pid_on_remotehost(pid, host):

            cmd = Command("kill -9 process", ("kill -9 {0}".format(pid)), ctxt=REMOTE, remoteHost=host)
            cmd.run()

            if cmd.get_results().rc != 0:
                logger.error('Failed to kill process {0} for segment {1}: {2}'.format(pid, datadir, cmd.get_results().stderr))


def logandkill(pid, sig):
    msgs = {
        signal.SIGCONT: "Sending SIGSCONT to %d",
        signal.SIGTERM: "Sending SIGTERM to %d (smart shutdown)",
        signal.SIGINT: "Sending SIGINT to %d (fast shutdown)",
        signal.SIGQUIT: "Sending SIGQUIT to %d (immediate shutdown)",
        signal.SIGABRT: "Sending SIGABRT to %d"
    }
    logger.info(msgs[sig] % pid)
    os.kill(pid, sig)


def kill_sequence(pid):
    if not check_pid(pid): return

    # first send SIGCONT in case the process is stopped
    logandkill(pid, signal.SIGCONT)

    # next try SIGTERM (smart shutdown)
    logandkill(pid, signal.SIGTERM)

    # give process a few seconds to exit
    for i in range(0, 3):
        time.sleep(1)
        if not check_pid(pid):
            return

    # next try SIGINT (fast shutdown)
    logandkill(pid, signal.SIGINT)

    # give process a few more seconds to exit
    for i in range(0, 3):
        time.sleep(1)
        if not check_pid(pid):
            return

    # next try SIGQUIT (immediate shutdown)
    logandkill(pid, signal.SIGQUIT)

    # give process a final few seconds to exit
    for i in range(0, 5):
        time.sleep(1)
        if not check_pid(pid):
            return

    # all else failed - try SIGABRT
    logandkill(pid, signal.SIGABRT)


def get_remote_link_path(path, host):
    """
      Function to get symlink target path for a given path on given host.
      :param  path: path for which symlink has to be found
      :param  host: host on which the given path is available
      :return: returns symlink target path
    """

    cmdStr = """python -c 'import os; print(os.readlink("%s"))'""" % path
    cmd = Command('get remote link path', cmdStr=cmdStr, ctxt=REMOTE,
                       remoteHost=host)
    cmd.run(validateAfter=True)
    return cmd.get_stdout()

# ---------------Platform Framework--------------------

""" The following platform framework is used to handle any differences between
    the platform's we support.  The GenericPlatform class is the base class
    that a supported platform extends from and overrides any of the methods
    as necessary.
    
    TODO:  should the platform stuff be broken out to separate module?
"""


class GenericPlatform():
    def getName(self):
        "unsupported"

    def get_machine_arch_cmd(self):
        return 'uname -i'

    def getDiskFreeCmd(self):
        return findCmdInPath('df') + " -k"

    def getTarCmd(self):
        return findCmdInPath('tar')

    def getIfconfigCmd(self):
        return findCmdInPath('ifconfig')


class LinuxPlatform(GenericPlatform):
    def __init__(self):
        pass

    def getName(self):
        return "linux"

    def getDiskFreeCmd(self):
        # -P is for POSIX formatting.  Prevents error 
        # on lines that would wrap
        return findCmdInPath('df') + " -Pk"

    def getPing6(self):
        return findCmdInPath('ping6')


class DarwinPlatform(GenericPlatform):
    def __init__(self):
        pass

    def getName(self):
        return "darwin"

    def get_machine_arch_cmd(self):
        return 'uname -m'

    def getPing6(self):
        return findCmdInPath('ping6')


class FreeBsdPlatform(GenericPlatform):
    def __init__(self):
        pass

    def getName(self):
        return "freebsd"

    def get_machine_arch_cmd(self):
        return 'uname -m'

class OpenBSDPlatform(GenericPlatform):
    def __init__(self):
        pass

    def getName(self):
        return "openbsd"

    def get_machine_arch_cmd(self):
        return 'uname -m'

    def getPing6(self):
        return findCmdInPath('ping6')


# ---------------ping--------------------
class Ping(Command):
    def __init__(self, name, hostToPing, ctxt=LOCAL, remoteHost=None, obj=None):
        self.hostToPing = hostToPing
        self.obj = obj
        self.pingToUse = findCmdInPath('ping')
        cmdStr = "%s -c 1 %s" % (self.pingToUse, self.hostToPing)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    def run(self, validateAfter=False):
        if curr_platform == LINUX or curr_platform == DARWIN or curr_platform == OPENBSD:
            # Get the family of the address we need to ping.  If it's AF_INET6
            # we must use ping6 to ping it.

            try:
                addrinfo = socket.getaddrinfo(self.hostToPing, None)
                if addrinfo and addrinfo[0] and addrinfo[0][0] == socket.AF_INET6:
                    self.pingToUse = SYSTEM.getPing6()
                    self.cmdStr = "%s -c 1 %s" % (self.pingToUse, self.hostToPing)
            except Exception as e:
                self.results = CommandResult(1, '', 'Failed to get ip address: ' + str(e), False, True)
                if validateAfter:
                    self.validate()
                else:
                    # we know the next step of running ping is useless
                    return

        super(Ping, self).run(validateAfter)

    @staticmethod
    def ping_list(host_list):
        for host in host_list:
            yield Ping("ping", host, ctxt=LOCAL, remoteHost=None)

    @staticmethod
    def local(name, hostToPing):
        p = Ping(name, hostToPing)
        p.run(validateAfter=True)

    @staticmethod
    def remote(name, hostToPing, hostToPingFrom):
        p = Ping(name, hostToPing, ctxt=REMOTE, remoteHost=hostToPingFrom)
        p.run(validateAfter=True)


# -------------df----------------------
class DiskFree(Command):
    def __init__(self, name, directory, ctxt=LOCAL, remoteHost=None):
        self.directory = directory
        cmdStr = "%s %s" % (SYSTEM.getDiskFreeCmd(), directory)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def get_size(name, remote_host, directory):
        dfCmd = DiskFree(name, directory, ctxt=REMOTE, remoteHost=remote_host)
        dfCmd.run(validateAfter=True)
        return dfCmd.get_bytes_free()

    @staticmethod
    def get_size_local(name, directory):
        dfCmd = DiskFree(name, directory)
        dfCmd.run(validateAfter=True)
        return dfCmd.get_bytes_free()

    @staticmethod
    def get_disk_free_info_local(name, directory):
        dfCmd = DiskFree(name, directory)
        dfCmd.run(validateAfter=True)
        return dfCmd.get_disk_free_output()

    def get_disk_free_output(self):
        '''expected output of the form:
           Filesystem   512-blocks      Used Available Capacity  Mounted on
           /dev/disk0s2  194699744 158681544  35506200    82%    /

           Returns data in list format:
           ['/dev/disk0s2', '194699744', '158681544', '35506200', '82%', '/']
        '''
        rawIn = self.results.stdout.split('\n')[1]
        return rawIn.split()

    def get_bytes_free(self):
        disk_free = self.get_disk_free_output()
        bytesFree = int(disk_free[3]) * 1024
        return bytesFree


# -------------mkdir------------------
class MakeDirectory(Command):
    def __init__(self, name, directory, ctxt=LOCAL, remoteHost=None):
        self.directory = directory
        cmdStr = "%s -p %s" % (findCmdInPath('mkdir'), directory)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def local(name, directory):
        mkdirCmd = MakeDirectory(name, directory)
        mkdirCmd.run(validateAfter=True)

    @staticmethod
    def remote(name, remote_host, directory):
        mkdirCmd = MakeDirectory(name, directory, ctxt=REMOTE, remoteHost=remote_host)
        mkdirCmd.run(validateAfter=True)


# ------------- remove a directory recursively ------------------
class RemoveDirectory(Command):
    """
    remove a directory recursively, including the directory itself.
    Uses rsync for efficiency.
    """
    def __init__(self, name, directory, ctxt=LOCAL, remoteHost=None):
        unique_dir = "/tmp/emptyForRemove%s" % uuid.uuid4()
        cmd_str = "if [ -d {target_dir} ]; then " \
                  "mkdir -p {unique_dir}  &&  " \
                  "{cmd} -a --delete {unique_dir}/ {target_dir}/  &&  " \
                  "rmdir {target_dir} {unique_dir} ; fi".format(
                    unique_dir=unique_dir,
                    cmd=findCmdInPath('rsync'),
                    target_dir=directory
        )
        Command.__init__(self, name, cmd_str, ctxt, remoteHost)

    @staticmethod
    def remote(name, remote_host, directory):
        rm_cmd = RemoveDirectory(name, directory, ctxt=REMOTE, remoteHost=remote_host)
        rm_cmd.run(validateAfter=True)

    @staticmethod
    def local(name, directory):
        rm_cmd = RemoveDirectory(name, directory)
        rm_cmd.run(validateAfter=True)


# -------------rm -rf ------------------
class RemoveFile(Command):
    def __init__(self, name, filepath, ctxt=LOCAL, remoteHost=None):
        cmdStr = "%s -f %s" % (findCmdInPath('rm'), filepath)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def remote(name, remote_host, filepath):
        rmCmd = RemoveFile(name, filepath, ctxt=REMOTE, remoteHost=remote_host)
        rmCmd.run(validateAfter=True)

    @staticmethod
    def local(name, filepath):
        rmCmd = RemoveFile(name, filepath)
        rmCmd.run(validateAfter=True)


class RemoveDirectoryContents(Command):
    """
    remove contents of a directory recursively, excluding the parent directory.
    Uses rsync for efficiency.
    """
    def __init__(self, name, directory, ctxt=LOCAL, remoteHost=None):
        unique_dir = "/tmp/emptyForRemove%s" % uuid.uuid4()
        cmd_str = "if [ -d {target_dir} ]; then " \
                  "mkdir -p {unique_dir}  &&  " \
                  "{cmd} -a --no-perms --delete {unique_dir}/ {target_dir}/  &&  " \
                  "rmdir {unique_dir} ; fi".format(
                    unique_dir=unique_dir,
                    cmd=findCmdInPath('rsync'),
                    target_dir=directory
        )
        Command.__init__(self, name, cmd_str, ctxt, remoteHost)

    @staticmethod
    def remote(name, remote_host, directory):
        rm_cmd = RemoveDirectoryContents(name, directory, ctxt=REMOTE, remoteHost=remote_host)
        rm_cmd.run(validateAfter=True)

    @staticmethod
    def local(name, directory):
        rm_cmd = RemoveDirectoryContents(name, directory)
        rm_cmd.run(validateAfter=True)


class RemoveGlob(Command):
    """
    This glob removal tool uses rm -rf, so it can fail OoM if there are too many files that match.
    """

    def __init__(self, name, glob, ctxt=LOCAL, remoteHost=None):
        cmd_str = "%s -rf %s" % (findCmdInPath('rm'), glob)
        Command.__init__(self, name, cmd_str, ctxt, remoteHost)

    @staticmethod
    def remote(name, remote_host, directory):
        rm_cmd = RemoveGlob(name, directory, ctxt=REMOTE, remoteHost=remote_host)
        rm_cmd.run(validateAfter=True)

    @staticmethod
    def local(name, directory):
        rm_cmd = RemoveGlob(name, directory)
        rm_cmd.run(validateAfter=True)




class FileDirExists(Command):
    def __init__(self, name, directory, ctxt=LOCAL, remoteHost=None):
        self.directory = directory
        cmdStr = "[ -d '%s' ]" % directory
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def remote(name, remote_host, directory):
        cmd = FileDirExists(name, directory, ctxt=REMOTE, remoteHost=remote_host)
        cmd.run(validateAfter=False)
        return cmd.filedir_exists()

    def filedir_exists(self):
        return (not self.results.rc)


# -------------scp------------------

# MPP-13617
def canonicalize(addr):
    if ':' not in addr: return addr
    if '[' in addr: return addr
    return '[' + addr + ']'


class Rsync(Command):
    def __init__(self, name, srcFile, dstFile, srcHost=None, dstHost=None, recursive=False,
                 verbose=True, archive_mode=True, checksum=False, delete=False, progress=False,
                 stats=False, dry_run=False, bwlimit=None, exclude_list=[], ctxt=LOCAL,
                 remoteHost=None, compress=False, progress_file=None):

        """
            rsync options:
                srcFile: source datadir/file
                        If source is a directory, make sure you add a '/' at the end of its path. When using "/" at the
                        end of source, rsync will copy the content of the last directory. When not using "/" at the end
                        of source, rsync will copy the last directory and the content of the directory.
                dstFile: destination datadir or file that needs to be synced
                srcHost: source host
                exclude_list: to exclude specified files and directories to copied or synced with target
                delete: delete the files on target which do not exist on source
                checksum: to skip files being synced based on checksum, not modification time and size
                bwlimit: to control the I/O bandwidth
                stats: give some file-transfer stats
                dry_run: perform a trial run with no changes made
                compress: compress file data during the transfer
                progress: to show the progress of rsync execution, like % transferred
        """

        cmd_tokens = [findCmdInPath('rsync')]

        if recursive:
            cmd_tokens.append('-r')

        if verbose:
            cmd_tokens.append('-v')

        if archive_mode:
            cmd_tokens.append('-a')

        # To skip the files based on checksum, not modification time and size
        if checksum:
            cmd_tokens.append('-c')

        # Shows the progress of the whole transfer,
        # Note : It is only supported with rsync 3.1.0 or above
        if progress:
            cmd_tokens.append('--info=progress2,name0')

        # To show file transfer stats
        if stats:
            cmd_tokens.append('--stats')

        if bwlimit is not None:
            cmd_tokens.append('--bwlimit')
            cmd_tokens.append(bwlimit)

        if dry_run:
            cmd_tokens.append('--dry-run')

        if delete:
            cmd_tokens.append('--delete')

        if compress:
            cmd_tokens.append('--compress')

        if srcHost:
            cmd_tokens.append(canonicalize(srcHost) + ":" + srcFile)
        else:
            cmd_tokens.append(srcFile)

        if dstHost:
            cmd_tokens.append(canonicalize(dstHost) + ":" + dstFile)
        else:
            cmd_tokens.append(dstFile)

        exclude_str = ["--exclude={} ".format(pattern) for pattern in exclude_list]

        cmd_tokens.extend(exclude_str)

        # Combines output streams, uses 'sed' to find lines with 'kB/s' or 'MB/s' and appends ':%s' as suffix to the end
        # of each line and redirects it to progress_file
        if progress_file:
            cmd_tokens.append(
                '2>&1 | tr "\\r" "\\n" |sed -E "/[0-9]+%/ s/$/ :{0}/" > {1}'.format(name, pipes.quote(progress_file)))

        cmdStr = ' '.join(cmd_tokens)
        cmdStr = "set -o pipefail; {}".format(cmdStr)
        self.command_tokens = cmd_tokens

        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    # Overriding validate() of Command class to handle few specific return codes of rsync which can be ignored
    def validate(self, expected_rc=0):
        """
            During differential recovery, pg_wal is synced using rsync. During pg_wal sync, some of the xlogtemp files
            are present on source when rsync builds the list of files to be transferred but are vanished before
            transferring. In this scenario rsync gives warning "some files vanished before they could be transferred
            (code 24)". This return code can be ignored in case of rsync command.
        """
        if self.results.rc != 24 and self.results.rc != expected_rc:
            self.logger.debug(self.results)
            raise ExecutionError("non-zero rc: %d" % self.results.rc, self)


class Scp(Command):
    def __init__(self, name, srcFile, dstFile, srcHost=None, dstHost=None, recursive=False, ctxt=LOCAL,
                 remoteHost=None):
        cmdStr = findCmdInPath('scp') + " "

        if recursive:
            cmdStr = cmdStr + "-r "

        if srcHost:
            cmdStr = cmdStr + canonicalize(srcHost) + ":"
        cmdStr = cmdStr + srcFile + " "

        if dstHost:
            cmdStr = cmdStr + canonicalize(dstHost) + ":"
        cmdStr = cmdStr + dstFile

        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

# -------------create tar------------------
class CreateTar(Command):
    def __init__(self, name, srcDirectory, dstTarFile, ctxt=LOCAL, remoteHost=None):
        self.srcDirectory = srcDirectory
        self.dstTarFile = dstTarFile
        tarCmd = SYSTEM.getTarCmd()
        cmdStr = "%s cvPf %s -C %s  ." % (tarCmd, self.dstTarFile, srcDirectory)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)


# -------------extract tar---------------------
class ExtractTar(Command):
    def __init__(self, name, srcTarFile, dstDirectory, ctxt=LOCAL, remoteHost=None):
        self.srcTarFile = srcTarFile
        self.dstDirectory = dstDirectory
        tarCmd = SYSTEM.getTarCmd()
        cmdStr = "%s -C %s -xf %s" % (tarCmd, dstDirectory, srcTarFile)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

# --------------kill ----------------------
class Kill(Command):
    def __init__(self, name, pid, signal, ctxt=LOCAL, remoteHost=None):
        self.pid = pid
        self.signal = signal
        cmdStr = "%s -s %s %s" % (findCmdInPath('kill'), signal, pid)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def local(name, pid, signal):
        cmd = Kill(name, pid, signal)
        cmd.run(validateAfter=True)

    @staticmethod
    def remote(name, pid, signal, remote_host):
        cmd = Kill(name, pid, signal, ctxt=REMOTE, remoteHost=remote_host)
        cmd.run(validateAfter=True)

# --------------hostname ----------------------
class Hostname(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        self.remotehost = remoteHost
        Command.__init__(self, name, findCmdInPath('hostname'), ctxt, remoteHost)

    def get_hostname(self):
        if not self.results:
            raise Exception, 'Command not yet executed'
        return self.results.stdout.strip()


# todo: This class should be replaced with gp.IfAddrs
class InterfaceAddrs(Command):
    """Returns list of interface IP Addresses.  List does not include loopback."""

    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        ifconfig = SYSTEM.getIfconfigCmd()
        grep = findCmdInPath('grep')
        awk = findCmdInPath('awk')
        cut = findCmdInPath('cut')
        cmdStr = 'echo "START_CMD_OUTPUT";%s|%s "inet "|%s -v "127.0.0"|%s \'{print \$2}\'|%s -d: -f2' % (ifconfig, grep, grep, awk, cut)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def local(name):
        cmd = InterfaceAddrs(name)
        cmd.run(validateAfter=True)
        return cmd.get_results().stdout.split('START_CMD_OUTPUT\n')[1].split()

    @staticmethod
    def remote(name, remoteHost):
        cmd = InterfaceAddrs(name, ctxt=REMOTE, remoteHost=remoteHost)
        cmd.run(validateAfter=True)
        return cmd.get_results().stdout.split('START_CMD_OUTPUT\n')[1].split()


# --------------tcp port is active -----------------------
class PgPortIsActive(Command):
    def __init__(self, name, port, file, ctxt=LOCAL, remoteHost=None):
        self.port = port
        try:
            cmdStr = "%s -an 2>/dev/null | %s %s | %s '{print $NF}'" % \
                     (findCmdInPath('netstat'), findCmdInPath('grep'), file, findCmdInPath('awk'))
        except CommandNotFoundException:
            try:
                cmdStr = "%s -an 2>/dev/null | %s %s | %s '{print $5}'" % \
                         (findCmdInPath('ss'), findCmdInPath('grep'), file, findCmdInPath('awk'))
            except CommandNotFoundException as err:
                logger.critical(
                    "Failed to find active tcp port.")
                raise

        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    def contains_port(self):
        rows = self.results.stdout.strip().split()

        if len(rows) == 0:
            return False

        for r in rows:
            val = r.split('.')
            tcp_port = int(val[len(val) - 1])
            if tcp_port == self.port:
                return True

        return False

    @staticmethod
    def local(name, file, port):
        cmd = PgPortIsActive(name, port, file)
        cmd.run(validateAfter=True)
        return cmd.contains_port()

    @staticmethod
    def remote(name, file, port, remoteHost):
        cmd = PgPortIsActive(name, port, file, ctxt=REMOTE, remoteHost=remoteHost)
        cmd.run(validateAfter=True)
        return cmd.contains_port()


# --------------chmod ----------------------
class Chmod(Command):
    def __init__(self, name, dir, perm, ctxt=LOCAL, remoteHost=None):
        cmdStr = '%s %s %s' % (findCmdInPath('chmod'), perm, dir)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def local(name, dir, perm):
        cmd = Chmod(name, dir, perm)
        cmd.run(validateAfter=True)

    @staticmethod
    def remote(name, hostname, dir, perm):
        cmd = Chmod(name, dir, perm, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)


# --------------echo ----------------------
class Echo(Command):
    def __init__(self, name, echoString, ctxt=LOCAL, remoteHost=None):
        cmdStr = '%s "%s"' % (findCmdInPath('echo'), echoString)
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def remote(name, echoString, hostname):
        cmd = Echo(name, echoString, ctxt=REMOTE, remoteHost=hostname)
        cmd.run(validateAfter=True)


# --------------get user id ----------------------
class UserId(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        cmdStr = "%s -un" % findCmdInPath('id')
        Command.__init__(self, name, cmdStr, ctxt, remoteHost)

    @staticmethod
    def local(name):
        cmd = UserId(name)
        cmd.run(validateAfter=True)
        return cmd.results.stdout.strip()


# --------------get list of descendant processes -------------------
def getDescendentProcesses(pid):
    ''' return all process pids which are descendant from the given processid '''

    children_pids = []

    for p in psutil.Process(pid).children(recursive=True):
        if p.is_running():
            children_pids.append(p.pid)

    return children_pids


# --------------global variable initialization ----------------------

if curr_platform == LINUX:
    SYSTEM = LinuxPlatform()
elif curr_platform == DARWIN:
    SYSTEM = DarwinPlatform()
elif curr_platform == FREEBSD:
    SYSTEM = FreeBsdPlatform()
elif curr_platform == OPENBSD:
    SYSTEM = OpenBSDPlatform();
else:
    raise Exception("Platform %s is not supported.  Supported platforms are: %s", SYSTEM, str(platform_list))


# --------------check SCP is available and has execute permission on host-------------------
def isScpEnabled(hostlist):
    """
    check SCP is available and has execute permission on hosts
    :param hostlist: list of hosts involved in gpcheckperf operation
    :return: true if scp is enabled on all hosts else false
    """
    for host in hostlist:
        cmd = Command("locate executable file of scp", cmdStr="which scp", ctxt=REMOTE, remoteHost=host)
        try:
            cmd.run(validateAfter=True)
        except Exception as e:
            print('[Warning] Either scp is not available or does not have execute permission on host:{0}' .format(host))
            return False

    return True



def validate_rsync_version(min_ver):
    """
    checks the version of the 'rsync' command and compares it with a required version.
    If the current version is lower than the required version, it raises an exception
    """
    rsync_version_info = get_rsync_version()
    pattern = r"version (\d+\.\d+\.\d+)"
    match = re.search(pattern, rsync_version_info)
    current_rsync_version = match.group(1)
    if LooseVersion(current_rsync_version) < LooseVersion(min_ver):
        return False
    return True

def get_rsync_version():
    """ get the rsync current version """
    cmdStr = findCmdInPath("rsync") + " --version"
    cmd = Command("get rsync version", cmdStr=cmdStr)
    cmd.run(validateAfter=True)
    return cmd.get_stdout()
