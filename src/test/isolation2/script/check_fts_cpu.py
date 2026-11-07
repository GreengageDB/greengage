#!/usr/bin/env python
#
# Measure the CPU time consumed by the "ftsprobe process" bgworker between
# two invocations, to verify that it does not busy-spin while a probed
# segment is down (the 1-second retry backoff in processRetry(), ftsprobe.c,
# used to burn 100% CPU until the segment came back or the retries ran out).
#
# Usage (run from src/test/isolation2, with the cluster environment sourced,
# e.g. gpAux/gpdemo/gpdemo-env.sh):
#
#   python ./script/check_fts_cpu.py start
#   ... make FTS probe a down segment (retry backoff) ...
#   python ./script/check_fts_cpu.py end --max-cpu-percent 30
#
# "start" records the worker's pid, the wall clock and its utime+stime into
# a state file, written atomically (temp file + rename).  "end" resolves the
# worker again, requires the same pid, and exits with a non-zero status if
# the worker has burned more than the allowed percentage of one CPU over
# the elapsed wall time.
#
# Both invocations exit non-zero on any error (worker not found, missing or
# stale state, measurement window too short), so a broken check can never
# pass silently.

from __future__ import print_function

import argparse
import json
import os
import sys
import time

FTS_WORKER_NAME = "ftsprobe process"
STATE_FILE = os.path.join("results", "check_fts_cpu.state")

try:
    monotonic = time.monotonic
except AttributeError:            # Python 2
    monotonic = time.time


def fail(msg):
    print("check_fts_cpu: %s" % msg, file=sys.stderr)
    sys.exit(2)


def find_fts_worker_pid():
    """Return the pid of the ftsprobe bgworker, a child of the master."""
    master_datadir = os.environ.get("MASTER_DATA_DIRECTORY")
    if not master_datadir:
        fail("MASTER_DATA_DIRECTORY is not set")

    try:
        with open(os.path.join(master_datadir, "postmaster.pid")) as f:
            master_pid = int(f.readline().strip())
    except (IOError, OSError, ValueError):
        fail("cannot read postmaster.pid in MASTER_DATA_DIRECTORY=%s"
             % master_datadir)

    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open("/proc/%s/stat" % entry) as f:
                stat = f.read()
            # comm (field 2) may contain spaces and parens, so parse the
            # fields after the last ')'.
            rest = stat[stat.rfind(")") + 2:].split()
            ppid = int(rest[1])                 # field 4: ppid
            if ppid != master_pid:
                continue
            with open("/proc/%s/cmdline" % entry) as f:
                cmdline = f.read().replace("\0", " ")
            if FTS_WORKER_NAME in cmdline:
                return int(entry)
        except (IOError, OSError, IndexError, ValueError):
            continue                # process went away meanwhile
    fail("ftsprobe process not found among children of postmaster pid %d"
         % master_pid)


def read_cpu_ticks(pid):
    """Return utime+stime of the given process, in clock ticks."""
    try:
        with open("/proc/%d/stat" % pid) as f:
            stat = f.read()
    except (IOError, OSError):
        fail("cannot read /proc/%d/stat (process gone?)" % pid)
    rest = stat[stat.rfind(")") + 2:].split()
    utime = int(rest[11])                   # field 14
    stime = int(rest[12])                   # field 15
    return utime + stime


def write_state_atomic(state):
    directory = os.path.dirname(STATE_FILE)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)
    tmp = "%s.tmp.%d" % (STATE_FILE, os.getpid())
    with open(tmp, "w") as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.replace(tmp, STATE_FILE)         # atomic update
    except AttributeError:                  # Python 2
        os.rename(tmp, STATE_FILE)


def do_start():
    pid = find_fts_worker_pid()
    write_state_atomic({"pid": pid,
                        "wall": monotonic(),
                        "ticks": read_cpu_ticks(pid)})
    print("check_fts_cpu: sampling ftsprobe process (pid %d)" % pid)
    sys.exit(0)


def do_end(max_cpu_percent, min_window):
    pid = find_fts_worker_pid()
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
    except (IOError, OSError, ValueError):
        fail("no valid state file %s: run 'start' first" % STATE_FILE)

    if state["pid"] != pid:
        fail("ftsprobe process restarted during the measurement "
             "(pid %d -> %d), result would be bogus"
             % (state["pid"], pid))

    window = monotonic() - state["wall"]
    if window <= 0:
        fail("wall clock went backwards, refusing to measure")
    if window < min_window:
        fail("measurement window %.2fs is shorter than the minimum %.2fs"
             % (window, min_window))

    ticks_used = read_cpu_ticks(pid) - state["ticks"]
    if ticks_used < 0:
        fail("CPU tick counter went backwards, refusing to measure")

    ticks_per_sec = os.sysconf("SC_CLK_TCK")
    cpu_percent = 100.0 * ticks_used / ticks_per_sec / window

    print("check_fts_cpu: ftsprobe process (pid %d) used %.1f%% of a CPU "
          "over %.1fs" % (pid, cpu_percent, window))
    if cpu_percent > max_cpu_percent:
        print("check_fts_cpu: expected at most %.0f%% -- FTS is busy-spinning"
              % max_cpu_percent, file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description="check that the ftsprobe process does not busy-spin "
                    "between the 'start' and 'end' invocations")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("start", help="record pid/wallclock/CPU ticks")
    end = sub.add_parser("end", help="compare against the recorded sample")
    end.add_argument("--max-cpu-percent", type=float, default=30.0,
                     help="allowed average percentage of one CPU "
                          "over the window (default %(default)s)")
    end.add_argument("--min-window", type=float, default=3.0,
                     help="refuse to judge windows shorter than this many "
                          "seconds (default %(default)s)")
    args = parser.parse_args()

    if args.command == "start":
        do_start()
    elif args.command == "end":
        do_end(args.max_cpu_percent, args.min_window)
    else:
        parser.error("command required: 'start' or 'end'")


if __name__ == "__main__":
    main()
