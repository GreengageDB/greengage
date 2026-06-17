---
name: greengage-debug
description: Diagnose GGDB crashes, asserts, and wrong-results - find the crash in the segment/QD logs, reproduce reliably (base64 SQL, forced heisenbug conditions), instrument with temporary elog, and read structs live with gdb on a no-DWARF binary. Use when a query crashes a backend, hits a FailedAssertion, hangs, or returns wrong data.
---

# Debugging GGDB

## Find the crash in the logs

QD/segment logs are CSV at
`gpAux/gpdemo/datadirs/qddir/demoDataDir-1/log/*.csv` (and `dbfast*/.../log/*.csv`
for segments). Signatures:
- `TRAP: FailedAssertion("<cond>", File: "<f>.c", Line: N)` — assert build crash.
- `server process ... was terminated by signal 6/11` + `Failed process was running: <SQL>` — abort/segfault; grep the crashing PID's lines.
- `PANIC`, `Out of memory`, `failed to acquire resources` (see [greengage-cluster-ops] for the OOM/crash recovery).
Take the failing statement from the CSV, then narrow it to a minimal query.

## Reproduce reliably

- **base64 the SQL** to avoid nested-quote mangling: building SQL inside
  `bash -lc '...'` eats inner single quotes (so `'x'` becomes `x` and string
  literals break, or `$abc$` dollar-quoting expands `$` to the PID). Write the
  `.sql` on the host, `base64 -w0`, pass via `-e B64=...`, `base64 -d` in-container.
- Match the EXACT failing shape: column types, GROUP BY, FILTER, optimizer
  on/off, even JIT (`set jit=on; set jit_above_cost=0`) — small differences flip
  the code path (e.g. multi-DQA needs `FILTER` to fail, not just DISTINCT).

## Force a heisenbug condition

When a bug depends on transient state (e.g. a relcache descriptor that is mutated
in place and only bites once an invalidation reloads it), inject the condition to
make it deterministic. Example used for the matview REFRESH bug:
```c
CacheInvalidateRelcacheByRelid(relid);   /* temporary, in the suspect function */
AcceptInvalidationMessages();
```
Rebuild, reproduce (now the unfixed code fails every time), apply the real fix,
confirm it passes WITH the forced condition still active, then remove the
injection. This converts "passes locally, fails in CI" into a real local repro.

## Instrument with temporary elog

Add `elog(LOG, "DBG ... %s", ...)` at the suspect point, rebuild
([greengage-build]), run, and read it back from the QD CSV log
(`grep "DBG" $QDLOG`). Print the concrete values you can't infer — e.g. the
generated SQL string and the actual column names of a transient relation — to
settle "what does the code really see". Remove all instrumentation before
committing; verify `git diff` is exactly the one-line fix.

## Live gdb (ptrace container, no DWARF)

Commit + re-run the container with `--cap-add=SYS_PTRACE --security-opt
seccomp=unconfined` (yama ptrace_scope=1 → attach as root). Add a `pg_sleep(30)`
in the query to open a window, find the per-query QE pid, `gdb -p`. The release
binary has no DWARF: read structs by **byte offset** and resolve addresses with
`addr2line`/`objdump`. Avoids the ereport-probe rebuild loop.

## Blast-radius triage

A single root cause often produces many `regression.diffs` (e.g. one missing
libpq command-queue entry broke COPY/sequences MPP-wide). Group failures by the
common error string/PID, fix the one root cause, re-measure — don't chase each
diff. Beware measuring against a [stale binary](greengage-build).

See also: [greengage-internals] (what the fix should be), [greengage-build].
