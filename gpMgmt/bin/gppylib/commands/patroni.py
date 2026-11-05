#!/usr/bin/env python3
#
# Copyright (c) 2026-Present, Greengage Community
#

"""Commands for managing Patroni agents and clusters."""

from gppylib.commands.base import Command, LOCAL, REMOTE


class PatroniCtlVersion(Command):
    def __init__(self, name):
        Command.__init__(self, name, "patronictl version")

    @staticmethod
    def local(name):
        cmd = PatroniCtlVersion(name)
        cmd.run(validateAfter=False)
        if cmd.get_return_code() != 0:
            raise RuntimeError(
                "Cannot run 'patronictl version'. Ensure Patroni is installed and "
                "add the directory containing 'patronictl' to your PATH before "
                "running the tool.")
        return cmd.get_results().stdout.strip()


class PatroniCtlList(Command):
    def __init__(self, name, config_file, cluster_name, ctxt=LOCAL, remoteHost=None):
        Command.__init__(self, name, "patronictl -c %s list %s -f json" % (config_file, cluster_name), ctxt, remoteHost)

    @staticmethod
    def local(name, config_file, cluster_name):
        cmd = PatroniCtlList(name, config_file, cluster_name)
        cmd.run(validateAfter = True)
        return cmd.get_results().stdout or ""

class PatroniCtlPause(Command):
    def __init__(self, name, config_file, cluster_name, ctxt=LOCAL, remoteHost=None):
        Command.__init__(self, name, "patronictl -c %s pause %s --wait" % (config_file, cluster_name), ctxt, remoteHost)

    @staticmethod
    def local(name, config_file, cluster_name):
        cmd = PatroniCtlPause(name, config_file, cluster_name)
        cmd.run(validateAfter = False)
        # if patroni wasn't just initially configured, ignore errors,
        # otherwise let standard 'validate()' throw an error
        if (cmd.get_return_code() != 0 and
            "Can not find accessible cluster member" not in cmd.get_results().stderr):
            cmd.validate()

class PatroniCtlResume(Command):
    def __init__(self, name, config_file, cluster_name, ctxt=LOCAL, remoteHost=None):
        Command.__init__(self, name, "patronictl -c %s resume %s --wait" % (config_file, cluster_name), ctxt, remoteHost)

    @staticmethod
    def local(name, config_file, cluster_name):
        cmd = PatroniCtlResume(name, config_file, cluster_name)
        cmd.run(validateAfter = False)
        # if patroni wasn't just initially configured and, therefore, paused, ignore errors,
        # otherwise let standard 'validate()' throw an error
        if (cmd.get_return_code() != 0 and
            "Error: Cluster is not paused" not in cmd.get_results().stderr):
            cmd.validate()

class PatroniCtlIsPaused(Command):
    def __init__(self, name, config_file, cluster_name, ctxt=LOCAL, remoteHost=None):
        Command.__init__(self, name, 'patronictl -c %s list %s | grep "Maintenance mode: on"' % (config_file, cluster_name), ctxt, remoteHost)

    @staticmethod
    def local(name, config_file, cluster_name):
        cmd = PatroniCtlIsPaused(name, config_file, cluster_name)
        cmd.run(validateAfter = False)
        return cmd.get_return_code() == 0

class PatroniCtlRestart(Command):
    def __init__(self, name, config_file, cluster_name, member_name, ctxt=LOCAL, remoteHost=None):
        Command.__init__(self, name, "patronictl -c %s restart %s %s --force" % (config_file, cluster_name, member_name), ctxt, remoteHost)

    @staticmethod
    def local(name, config_file, cluster_name, member_name):
        cmd = PatroniCtlRestart(name, config_file, cluster_name, member_name)
        cmd.run(validateAfter = True)

class PatroniAgentsCheck(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        Command.__init__(self, name, "pgrep -f 'python3 -m [p]atroni'", ctxt, remoteHost)

    @staticmethod
    def local(name):
        cmd = PatroniAgentsCheck(name)
        cmd.run(validateAfter = False)
        return cmd.get_return_code() == 0

    @staticmethod
    def remote(name, host):
        cmd = PatroniAgentsCheck(name, ctxt = REMOTE, remoteHost = host)
        cmd.run(validateAfter = False)
        return cmd.get_return_code() == 0

class PatroniAgentsStop(Command):
    def __init__(self, name, ctxt=LOCAL, remoteHost=None):
        Command.__init__(self, name, "pkill -TERM -f 'python3 -m [p]atroni'", ctxt, remoteHost)

    @staticmethod
    def local(name):
        cmd = PatroniAgentsStop(name)
        cmd.run(validateAfter = True)

    @staticmethod
    def remote(name, host):
        cmd = PatroniAgentsStop(name, ctxt = REMOTE, remoteHost = host)
        cmd.run(validateAfter = True)

class PatroniAgentsStart(Command):
    def __init__(self, name, config_file, ctxt=LOCAL, remoteHost=None):
        # Note: if one changes the way patroni agent is started,
        # also take a look at PatroniAgentsCheck and PatroniAgentsStop -
        # they need to be aligned.
        Command.__init__(self, name, "python3 -m patroni %s > /home/gpadmin/gpAdminLogs/patroni_agent.log 2>&1 &" % (config_file), ctxt, remoteHost)

    @staticmethod
    def local(name, config_file):
        cmd = PatroniAgentsStart(name, config_file)
        cmd.run(validateAfter = True)

    @staticmethod
    def remote(name, config_file, host):
        cmd = PatroniAgentsStart(name, config_file, ctxt = REMOTE, remoteHost = host)
        cmd.run(validateAfter = True)
