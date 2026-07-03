---
name: greengage-debug
description: Diagnose GGDB crashes, asserts, hangs, and wrong-results - find the crash in the segment/QD CSV logs, reproduce reliably (base64 SQL, forced heisenbug conditions), instrument with temporary elog, use gp_inject_fault to make races deterministic, and attach gdb to live backends (ptrace-capable container or nsenter from the host). Use when a query crashes a backend, hits a FailedAssertion, hangs forever, wedges the cluster, or returns wrong data.
---

# Debugging GGDB

## Find the crash in the logs

Start in the CSV logs — QD at `gpAux/gpdemo/datadirs/qddir/demoDataDir-1/log/*.csv`,
segments under `dbfast*/demoDataDir*/log/`. Full map of log locations, the CSV
column layout, the grep-able failure signatures, and the assert-forensics method
(including the grep traps that hide crashers) are in **[reading-logs.md](reading-logs.md)**
— read it first when triaging any crash or red suite.

Short version: grep for `terminated by signal` (the reliable crash gate), take the
SQL from `Failed process was running:`, then narrow to a minimal query.

## Reproduce reliably

- **base64 the SQL** to avoid nested-quote mangling: building SQL inside
  `bash -lc '...'` eats inner single quotes (so `'x'` becomes `x` and string
  literals break, or `$abc$` dollar-quoting expands `$` to the PID). Write the
  `.sql` on the host, `base64 -w0`, pass via `-e B64=...`, `base64 -d` in-container.
- Match the EXACT failing shape: column types, GROUP BY, FILTER, optimizer
  on/off, even JIT (`set jit=on; set jit_above_cost=0`) — small differences flip
  the code path (e.g. multi-DQA needs `FILTER` to fail, not just DISTINCT).
- Match the failing job's build flags too: `--enable-cassert` and ORCA on/off each
  expose paths the other build never runs (see [greengage-build](../greengage-build/SKILL.md)).

## Before you "fix" anything — three misdiagnosis guards

1. **Is the PANIC intentional?** Search `src/test/` for a test that VALIDATES the
   exact behavior before suppressing an assert/PANIC. Example: the mirror PANIC
   `WAL contains references to invalid pages` on `APPENDONLY_TRUNCATE` replay of a
   missing segfile is DESIGNED self-heal (mirror goes down, gets rebuilt from the
   primary) and is explicitly asserted by `src/test/regress` test `mirror_replay`
   (see `greenplum_schedule`). A "fix" that suppresses it breaks that test.
   A PANIC a test deliberately provokes is a feature.
2. **Is the state contaminated?** A leftover injected fault, crash debris, or 2PC
   residue from earlier runs makes later runs lie. A famous phantom: an
   "infinite hang / mirror never replays" diagnosis that was really a stale
   `inside_move_db_transaction` error-fault left armed by an interrupted prior run.
   Before concluding, reset all faults
   (`select gp_inject_fault('all','reset',dbid::int) from gp_segment_configuration`),
   restart or recreate the cluster ([greengage-cluster-ops](../greengage-cluster-ops/SKILL.md)),
   and re-observe on a clean cluster.
3. **Is the binary stale?** Committed fixes may not be in the running postgres
   (mtime-preserving sync, root-owned `.o`, orphan QD on port 7000 running an old
   binary). Verify the live binary before trusting any result —
   [greengage-build](../greengage-build/SKILL.md) has the detection and deploy recipe.

## Force a heisenbug condition

When a bug depends on transient state (e.g. a relcache descriptor mutated in
place that only bites once an invalidation reloads it), inject the condition to
make it deterministic. Example used for the matview REFRESH CONCURRENTLY bug:
```c
CacheInvalidateRelcacheByRelid(relid);   /* temporary, in the suspect function */
AcceptInvalidationMessages();
```
Rebuild, reproduce (now the unfixed code fails every time), apply the real fix,
confirm it passes WITH the forced condition still active, then remove the
injection. This converts "passes locally, fails in CI" into a real local repro.

## Fault injection (gp_inject_fault)

The `gpcontrib/gp_inject_fault` extension turns races into deterministic repros —
fire a named fault point on a chosen segment. Fault types (from
`src/include/utils/faultinjector_lists.h`): `sleep`, `fatal`, `panic`, `error`,
`infinite_loop`, `suspend`, `resume`, `skip`, `segv`, `interrupt`, plus the
control verbs `reset` and `status`.
```sql
CREATE EXTENSION gp_inject_fault;  -- needed once per db; --load-extension does NOT
                                   -- install into a pre-existing db
SELECT gp_inject_fault('<faultname>', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=1;
SELECT gp_wait_until_triggered_fault('<faultname>', 1, dbid) FROM ...;
SELECT gp_inject_fault('<faultname>', 'reset', dbid) FROM ...;
```
Grep `src/test/isolation2/sql/` for hundreds of usage examples. Find injectable
points with `rg SIMPLE_FAULT_INJECTOR src/backend`. ALWAYS reset faults when done;
a leftover fault wedges later tests (guard 2 above). Killing a fault-injection
test mid-run leaves the fault armed — see cluster-ops for recovery.

## Instrument with temporary elog

Add `elog(LOG, "DBG ... %s", ...)` at the suspect point, rebuild
([greengage-build](../greengage-build/SKILL.md)), run, and read it back from the
QD CSV log (`grep "DBG" $QDLOG`). Print the concrete values you can't infer —
e.g. the generated SQL string and the actual column names of a transient
relation. Remove all instrumentation before committing; verify `git diff` is
exactly the fix. Note: `errbacktrace()`/`backtrace_functions` output has NOT
reliably shown up in segment CSV logs; use a gdb breakpoint or `elog(FATAL)`
(which triggers GGDB's own stack dumper) when you need a backtrace.

## gdb on a live backend

**Getting ptrace.** Containers usually lack CAP_SYS_PTRACE and yama blocks
attach. Two ways in:
- **Recreate the container with the cap** (preserves build + datadirs):
  `docker commit <container> <container>:dbg` → `docker rm -f <container>` →
  `docker run -d --name <container> --cap-add=SYS_PTRACE --security-opt
  seccomp=unconfined ... <container>:dbg sleep infinity`. After restart:
  `service ssh start` (gp utilities need ssh) and recreate the cluster
  (processes are gone; datadirs persist). Verify: `gdb -p <pid> -batch -ex bt`.
- **nsenter from the host** (no container changes; host root bypasses yama):
  ```bash
  cpid=$(docker inspect <container> --format '{{.State.Pid}}')
  sudo nsenter -t $cpid -p -m gdb -p <pid-inside-container> -batch -x /tmp/script.gdb
  ```
  gdb runs with host-root caps inside the container's pid/mount namespaces.

**Catching a short-lived QE.** QEs are forked per query; insert
`SELECT pg_sleep(45);` in the repro BEFORE the failing statement — the gang stays
idle during the sleep. Find QE pids from process titles, which include dbname,
session and segment: `postgres: <port>, gpadmin <dbname> <ip> con<N> seg<M> ...`.
Then `timeout 45 gdb -p <qe> -batch -nx -ex "set pagination off" -ex "break <fn>"
-ex continue -ex bt -ex detach &`.

**Debug info varies by build.** `--enable-debug` builds have full DWARF (print
structs directly). Release `-O3` builds keep function-name symbols only:
backtraces resolve names (enough to locate a crash), but struct fields must be
read by byte offset from the arg registers (`$rdi,$rsi,...`), e.g. TupleTableSlot
tts_flags `*(unsigned short*)($rdi+4)`, tts_nvalid `*(short*)($rdi+6)`; resolve
addresses with `addr2line`/`objdump`.

**Safety rules (each learned the hard way):**
- NEVER attach while a regress suite is live — freezing a suite's QE causes a
  shared-memory reset that poisons unrelated tests. Filter candidate pids by DB
  NAME in the process title, and only probe when no suite runs.
- Resume any gdb-stopped process before killing:
  `ps -eo pid,stat,comm | awk '$2~/T/&&$3~/postgres/{print $1}' | xargs -r kill -CONT`.
- Kill helpers by EXACT name (`pkill -9 -x psql`, `-x gdb`) — `pkill -f <pattern>`
  matching your own shell's cmdline SIGKILLs your own script.

## Hangs

Distinguish hang from slow: check `pg_stat_activity` wait events on QD and
segments. A backend stuck in `IPC/ProcSignalBarrier` with `still waiting for
backend with PID N to accept ProcSignalBarrier` naming ITSELF = interrupts held
(`InterruptHoldoffCount>0` in gdb) → `CHECK_FOR_INTERRUPTS` is a no-op and the
emitter never absorbs its own barrier. Such a backend ignores
`pg_terminate_backend` — only SIGKILL (crash-recovery restart) or cluster
recreate unblocks it. Root-cause pattern: a `PG_CATCH` that restores a saved
`InterruptHoldoffCount` then lets the error unwind past the saving frame leaks
the count into the next command (fixed on claude-merge-3 as `eb937c899ee`:
reset the holdoff counts in PostgresMain's outer error handler).

## Blast-radius triage

A single root cause often produces many `regression.diffs` (one missing libpq
command-queue entry broke COPY/sequences MPP-wide; one over-strict ORCA assert
crashed 25 tests). Group failures by the common error string/assert/PID, fix the
one root cause, re-measure — don't chase each diff. Count DISTINCT asserts from
the logs, not failed tests: one crash fails a whole parallel group.

See also: [reading-logs.md](reading-logs.md), [greengage-internals](../greengage-internals/SKILL.md)
(what the fix should be), [greengage-build](../greengage-build/SKILL.md),
[greengage-cluster-ops](../greengage-cluster-ops/SKILL.md),
[greengage-ci-triage](../greengage-ci-triage/SKILL.md) (CI-side evidence).
