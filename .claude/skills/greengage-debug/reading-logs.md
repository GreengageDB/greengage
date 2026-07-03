# Reading GGDB logs

How to find, parse, and correlate the CSV server logs of a gpdemo cluster when
hunting crashes, asserts, and hangs.

## Where the logs live

All paths relative to the repo root (or `$MASTER_DATA_DIRECTORY/..` on an
installed cluster). Each demo-cluster process has its own datadir + `log/` dir:

| Process | Datadir | Logs |
|---|---|---|
| Coordinator (QD) | `gpAux/gpdemo/datadirs/qddir/demoDataDir-1/` | `log/gpdb-*.csv` |
| Primary seg N | `gpAux/gpdemo/datadirs/dbfast<N+1>/demoDataDir<N>/` | `log/gpdb-*.csv` |
| Mirror seg N | `gpAux/gpdemo/datadirs/dbfast_mirror<N+1>/demoDataDir<N>/` | `log/gpdb-*.csv` |
| Standby coordinator | `gpAux/gpdemo/datadirs/standby/` | `log/gpdb-*.csv` |
| pg_ctl startup output (nodes started via the gpMgmt pg_ctl wrapper, e.g. the standby) | same `log/` dir | `log/startup.log` |
| gp management utilities (gpstart/gpstop/gprecoverseg/gpinitsystem) | `gpAux/gpdemo/datadirs/gpAdminLogs/` (also `~/gpAdminLogs/`) | `<utility>_<date>.log` |

Grep everything at once:
```bash
grep -a "TRAP\|terminated by signal\|PANIC" \
    gpAux/gpdemo/datadirs/*/demoDataDir*/log/gpdb-*.csv \
    gpAux/gpdemo/datadirs/standby/log/gpdb-*.csv
```
CI job logs and artifact tarballs are a different topic — see
[greengage-ci-triage](../greengage-ci-triage/SKILL.md).

## The CSV line format

GGDB logs CSV with GGDB-specific columns (more than upstream csvlog). The
authoritative column list is the external-table definition in
`src/backend/catalog/gp_toolkit.sql` (`gp_toolkit.__gp_log_segment_ext`).
The fields you actually grep by:

| # | Column | Example | Use |
|---|---|---|---|
| 1 | logtime | `2026-06-01 13:28:01.500984 UTC` | time-window correlation |
| 2/3 | loguser/logdatabase | `"gpadmin","regression"` | which suite/db |
| 4 | logpid | `p643130` | note the `p` prefix |
| 10 | logsession | `con8` | **QD↔QE correlation key** |
| 11 | logcmdcount | `cmd1` | statement number in session |
| 12 | logsegment | `seg-1` = QD, `seg0`.. = segments | who logged it |
| 13 | logslice | `slice1` | which plan slice |
| 17 | logseverity | `"LOG"`,`"ERROR"`,`"PANIC"` | severity |
| 19 | logmessage | quoted text | the message |
| 28/29 | logfile/logline | `"postmaster.c",1350` | source location |

Parsing notes:
- Messages span MULTIPLE physical lines (quoted CSV fields contain newlines) —
  use `grep -a -A40` to pull continuation lines, or a real CSV parser.
- Quotes inside messages are DOUBLED per CSV: an assert appears as
  `File: ""nodeAgg.c"", Line: 2012` — single-quote regexes miss it.
- With a live cluster, the `gp_toolkit` views query all logs as tables:
  `gp_toolkit.gp_log_system` (whole cluster), `gp_log_database`,
  `gp_log_master_concise`, `gp_log_command_timings`.

## Failure signatures

All strings verified against the source tree:

| Signature (grep) | Meaning | Next step |
|---|---|---|
| `TRAP: FailedAssertion` (≤PG15) / `TRAP: failed Assert` (PG16+) | assert-build crash, SIGABRT follows | extract file:line + backtrace (below) |
| `terminated by signal 6` | abort — usually an assert or PANIC | pair with the TRAP/PANIC line |
| `terminated by signal 11` | segfault | core file / gdb |
| `Failed process was running: <SQL>` | postmaster names the crashing statement | your repro query |
| `PANIC` | crash-restart of the node; on a mirror may be DESIGNED self-heal | check for a validating test first (see SKILL.md) |
| `Out of memory` / `ENOMEM` | OOM — often host overcommit or too-parallel schedule | [greengage-cluster-ops](../greengage-cluster-ops/SKILL.md) |
| `failed to acquire resources on one or more segments` | gang creation failed (seg down/recovering) | check segment health |
| `segment is in reset/recovery mode` | QE still in post-crash recovery | wait/retry; check what crashed it |
| `received dbid:N doesn't match this segments configured dbid` | FTS probe hitting a mis-configured segment (e.g. clobbered `internal.auto.conf`) | ftsmessagehandler.c |
| `still waiting for backend with PID N to accept ProcSignalBarrier` | barrier hang; if PID is the waiter itself → leaked interrupt holdoff | SKILL.md "Hangs" |
| `Interconnect` errors in motion code | UDP/TCP interconnect failure between QEs | often secondary to a QE crash |
| `Cluster validation failed` (pg_regress output, not the log) | pg_regress ran NOTHING (`All 0 tests passed`, rc=0, ~4s) because a segment is down | recover the cluster first |

## Assert-forensics method (assert builds)

Distilled from a campaign that hunted 11 distinct crashers in one schedule:

1. **Disable cores or watch disk.** Assert cores are 100-250MB each and a
   crash-looping test fills the disk:
   `echo "|/bin/true" > /proc/sys/kernel/core_pattern` (root). Re-enable a real
   `core_pattern` only when you actually need the core.
2. **The signal-6 count is the crash GATE; the filename grep only NAMES them.**
   ```bash
   grep -ac "terminated by signal 6" <all csv>          # reliable count
   grep -oaE 'File: ""[A-Za-z0-9_]+\.c"", Line: [0-9]+' <all csv> | sort | uniq -c
   ```
   **CRITICAL GREP TRAP:** the filename class must be `[A-Za-z0-9_]+\.c`, NOT
   `[a-z_]+\.c` — a lowercase-only class silently misses every mixed-case/digit
   filename (`nodeModifyTable.c`, `execMain.c`, `nodeAgg.c`, `md5.c`) and once
   hid a real crasher across three full runs. If the signal-6 count is nonzero
   but the assert grep is empty, your pattern is wrong, not the logs.
3. **Backtrace symbols** follow the TRAP on later CSV lines:
   `grep -aA40 '<assert expr>' *.csv | grep -oaE '[A-Za-z_]+\+0x[0-9a-f]+'`.
4. **Count DISTINCT asserts, not failed tests.** One crash fails ~20 tests of a
   parallel group; deduplicate by assert file:line before claiming "N crashers".
5. **One-run log isolation.** Loggers hold `.csv` files open and gpstop rewrites
   mtimes, so old logs look fresh — never dedupe runs by mtime. Clean protocol:
   `gpstop -a` → `find gpAux/gpdemo/datadirs -name 'gpdb-*.csv' -delete` →
   `gpstart -a` → run → read ALL csv. A freshly recreated cluster has empty logs,
   so anything in them is this run's.
6. **The syslogger survives postmaster crash-resets.** If you deleted the active
   `.csv` while the cluster runs, the logger keeps writing to the unlinked
   inode. Recover it: find logger pids (`ps -ef | grep "logger process"`), then
   read `/proc/<logger_pid>/fd/<n>` (the open csv fd).

## Correlating QD ↔ QE, and finding WHICH segment crashed

Every backend of one user session logs the same `con<N>` (gp_session_id), and
process titles carry it too (`postgres: 7002, gpadmin regression 172.. con8 seg0 ...`).

```bash
# get the session id from the repro session itself
SELECT current_setting('gp_session_id');
# pull that session's lines from every node, in time order
grep -ah ",con8," gpAux/gpdemo/datadirs/*/demoDataDir*/log/gpdb-*.csv | sort | less
```
- `logsegment` (`seg-1` vs `seg0`/`seg1`/...) tells you which node wrote each
  line; `logslice` which part of the plan it ran.
- The `terminated by signal` report for a QE crash is written by the CRASHED
  SEGMENT's own postmaster (so it appears in that segment's log), while the QD
  logs only the resulting interconnect/dispatch error. To find the crashed
  segment: grep all segment logs for
  `terminated by signal` within the failure's time window, or grep for the TRAP
  itself — the file that has it is the crashed node.
- Errors dispatched from QEs are wrapped by the QD with the segment id in the
  message (`(seg0 slice1 172.17.0.2:7002 pid=...)`) — that string alone
  pinpoints the node; go to that segment's log for the full context.

## Core files

- Check where cores go: `cat /proc/sys/kernel/core_pattern` (containers often
  inherit the host's pattern; a piped pattern like `|/usr/share/apport/apport`
  means no file appears in the datadir).
- For a usable core: `echo "core.%e.%p" > /proc/sys/kernel/core_pattern` (root),
  crash again, then `gdb $GPHOME/bin/postgres core.postgres.<pid> -batch -ex bt`.
- ERRORs (elog ERROR) leave NO core — for those, use the live-gdb or elog
  techniques in [SKILL.md](SKILL.md).
- Remember the disk trap: assert builds + crash loops = disk full of cores.
