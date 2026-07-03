---
name: greengage-cluster-ops
description: Operate the gpdemo MPP cluster during long test/build cycles - source the right environment, read segment health correctly, create/recreate the demo cluster, monitor disk and long-running jobs, detect OOM, recover a degraded/crashed/wedged cluster (gprecoverseg vs full recreate), and stabilize back-to-back HA/failover tests. Use when a run hangs/crashes, a segment goes down, "Cluster validation failed" or "Segments are in reset/recovery mode" appears, a killed fault-injection test wedged the cluster, disk fills, or a fresh cluster is needed.
---

# Operating & recovering the gpdemo cluster

## Environment first (silent-failure trap)

Always source BOTH before any `gp*` utility:
```bash
source /usr/local/greenplum-db-devel/greenplum_path.sh   # or your --prefix
source gpAux/gpdemo/gpdemo-env.sh   # generated at cluster creation, git-ignored
```
`gpdemo-env.sh` sets `PGPORT=7000` and `COORDINATOR_DATA_DIRECTORY`/
`MASTER_DATA_DIRECTORY`. Without it `gpstop -ar` fails (`COORDINATOR_DATA_DIRECTORY
not set!`) and segments silently keep running the OLD binary — you chase phantom bugs.
Verify a restart happened: `ps -eo pid,lstart,cmd | grep gp_role=execute` — `lstart` recent.

The cluster lives inside the campaign's docker container (each campaign pairs
one container with one git worktree); operate it as `gpadmin`:
`sudo docker exec -u gpadmin -e USER=gpadmin <container> bash -lc '...'`
(some tests break subtly without `USER=gpadmin`).

## Segment layout (gpdemo, 3 primaries + mirrors + standby)

- Coordinator: content **-1**, dbid1, port **7000**, datadir `gpAux/gpdemo/datadirs/qddir/demoDataDir-1`. Standby: dbid8/7001.
- content 0: primary dbid2/7002, mirror dbid5/7005. content 1: dbid3/7003 + dbid6/7006. content 2: dbid4/7004 + dbid7/7007. Primaries in `datadirs/dbfast{1,2,3}/`, mirrors in `datadirs/dbfast_mirror{1,2,3}/`.

## Read health correctly

```sql
select role,preferred_role,mode,status,count(*) from gp_segment_configuration group by 1,2,3,4 order by 1,2;
```
Healthy looks like `m|m|s|u|4`, `p|p|s|u|3`, **`p|p|n|u|1`**. The last is the
coordinator (content -1) — **mode `n` for content -1 is NORMAL** (its standby is
tracked via `pg_stat_replication`, `gp_walreceiver streaming sync`), do not treat
it as degraded. Trouble = `status=d` (down) or `role<>preferred_role` (failed over).

## Create / recreate the demo cluster

```bash
make create-demo-cluster     # top-level target -> gpAux/gpdemo/demo_cluster.sh
make destroy-demo-cluster    # also reclaims the (up to 13GB) datadirs
source gpAux/gpdemo/gpdemo-env.sh
```
Run via `make`, not `./demo_cluster.sh` directly: the Makefile exports the
required env (`DEMO_PORT_BASE=7000 NUM_PRIMARY_MIRROR_PAIRS=3 WITH_MIRRORS=true
WITH_STANDBY=true`); a bare invocation breaks on the unset port variables.

Full recreate after a bad crash (~1 min, keeps the installed binaries):
```bash
pkill -9 -f "bin/post[g]res"   # bracket the pattern: pkill -f matches its own cmdline
rm -f /tmp/.s.PGSQL.700*       # stale socket/lock files block a fresh start
make destroy-demo-cluster && make create-demo-cluster
```

**Recreation RESETS custom config.** postgresql.conf addons come from
`BLDWRAP_POSTGRES_CONF_ADDONS` (written to `gpAux/gpdemo/clusterConfigPostgresAddonsFile`)
and anything set later via `gpconfig`/ALTER SYSTEM is gone. Notably isolation2
`prepare_limit` expects `max_prepared_transactions=250` at start — after a
recreate: `gpconfig -c max_prepared_transactions -v 250 --skipvalidation` + restart.

## Host prerequisites for heavy runs

- `sysctl -w vm.overcommit_memory=1` — with `=2` (strict accounting), parallel
  ORCA load hits `fork(): Cannot allocate memory`, tests hang, FTS marks
  segments down. Host setting; re-apply after host resets.
- PG15+: `recovery_prefetch=off`. Upstream 15beta2's WAL-prefetcher cassert
  (`xlogprefetcher.c:1061`) crashes mirror WAL replay reproducibly; claude-merge-3
  flipped the boot default off — if your branch didn't, `gpconfig -c recovery_prefetch -v off`.

## Disk hygiene (regress workflows leak disk)

- `df -h` between runs. Cores, 13 GB datadirs, and docker images fill the disk;
  a **full disk can truncate host source files to 0 bytes** — after any
  disk-full, scan tracked files for empties and restore from git/container.
- Set `core_pattern=|/bin/true` so assert cores don't pile up. A crash-LOOPING
  auxiliary process (checkpointer/bgwriter/startup) dumps a core per restart —
  hundreds of GB in minutes. Treat any looping aux-process crash as top
  priority, and keep a background core cleaner during suites:
  `while pgrep -f installcheck >/dev/null; do ls -t /tmp/core.* 2>/dev/null | tail -n +6 | xargs -r rm -f; sleep 20; done &`
- A runaway spilling query fills the disk faster than `statement_timeout` fires —
  cap with `gp_workfile_limit_per_query`/`_per_segment`. Clean `regression.diffs`,
  `results/`, leftover temp dbs, and `/tmp/*.log` promptly.

## Monitor a long run (build or full schedule, 20-40 min)

Run it detached and poll its log; don't block one command on the whole run:
```bash
nohup env PGOPTIONS="-c optimizer=off" make installcheck-good > /tmp/run.log 2>&1 &
for i in $(seq 1 90); do grep -qE "tests passed|tests, .* failed|make.*Error" /tmp/run.log && break; sleep 6; done
grep -c "\.\.\. FAILED" /tmp/run.log
```

## OOM under load (small gpdemo)

The full concurrent schedule OOMs the small gpdemo: `ERROR: Out of memory` /
`failed to acquire resources on one or more segments`, and a heavy parallel group
can **crash a segment** (it flips to `m|p|n|d`), after which `pg_regress` prints
`Cluster validation failed` and runs 0 tests. Mitigate with `MAX_CONNECTIONS=3
make ...` (passes `--max-connections`), or run a schedule prefix that excludes
the OOM-heavy `qp_*` (those are in greenplum_schedule, not parallel_schedule).

## Recover a degraded/crashed cluster

```bash
gprecoverseg -a                         # recover the down segment (as a mirror)
# wait for WAL resync: poll until count(mode<>'s' and content>=0)=0
gprecoverseg -ar                        # rebalance: restore preferred primary/mirror roles
# if the coordinator itself is down:
gpstart -a                              # or gpstop -ar to restart
```

- `gprecoverseg` FAILS while segments are still in reset/recovery mode — wait
  and retry, or use the gang-retry GUCs below for scripted runs.
- A wedged coordinator stuck at "waiting for distributed transaction recovery
  to complete" = mirrors waiting for WAL from dead primaries → full recreate.
- Restart fragility: `gpstop -ar` with a hung motion-IPC QE FAILS to shut down
  cleanly (SIGQUIT then SIGABRT), crashing the cluster — `gpstart -a` +
  `gprecoverseg`. Such a hang ignores `statement_timeout`; `pg_terminate_backend` it.
- After many crashes, 2PC/pgstat debris causes cascade errors (e.g. duplicate
  prepared-transaction locks) — `gpstop -ar` and re-probe a CREATE/DROP DATABASE
  before believing a fresh bug.
- After a full recovery / mirror move, cross-check the segment's
  `postgresql.conf` port against `gp_segment_configuration`: segments are
  started on the CATALOG port, so a conf still carrying the basebackup
  source's port is invisible at runtime (`update_port_in_conf`'s `perl -i`
  edit used to report success even when it substituted nothing — it now
  verifies, but the class generalizes to every in-place config edit).

## Fault-test hygiene (isolation2 fault injection)

Killing a fault-injection test mid-flight **wedges the cluster** (suspended
workers + in-doubt 2PC; a plain restart then hits the mirror-waiting-for-WAL
deadlock). Recovery ladder:
1. Kill the harness **by PID** — `pkill -f pg_isolation2_regress` SELF-KILLS the
   docker-exec shell whose args match; kill the `sql_isolation_testcase.py` PIDs instead.
2. Reset leftover faults: `select gp_inject_fault('<fault>','reset',dbid) from
   gp_segment_configuration ...` (may error for the standby dbid — ignore).
3. `gpstop -afr` ONLY if all mirrors show `mode='s'` and no fault remains
   active; otherwise **recreate** (see above) — it's faster than debugging a
   wedged restart.
- A session stuck in `END;` that survives `pg_terminate_backend`: if no fault
  injection was involved (e.g. the gdd deadlock tests), `gpstop -afr` is safe.

## Stabilize back-to-back HA/failover tests

A just-promoted/recovered segment is transiently unavailable; two race classes, two cures:
1. **Gang creation** (dispatched queries, gprecoverseg's own sessions) →
   `Segments are in reset/recovery mode`. Covered by the gang-retry GUCs — defaults
   only 5 retries x 2 s; suite pattern (see `src/test/isolation2/sql/segwalrep/mirror_promotion.sql`):
   ```bash
   gpconfig -c gp_gang_creation_retry_count -v 120 --skipvalidation --masteronly
   gpconfig -c gp_gang_creation_retry_timer -v 1000 --skipvalidation --masteronly
   gpstop -u          # reload; gpconfig -r both + gpstop -u when done
   ```
   Both GUCs are `GUC_NO_SHOW_ALL` (absent from pg_settings — use `SHOW`); count max 128.
2. **Direct utility connection** to the segment (isolation2 `NU:` sessions) →
   `FATAL: the database system is not accepting connections` — NOT covered by
   gang retry. Poll readiness first: isolation2 `sql/setup.sql` provides
   `wait_until_segment_accepts_connections(content_id)` (plpython `pg_isready`
   poll + `gp_request_fts_probe_scan` nudge each round).

HA tests are inherently flaky: one standalone pass proves little — verify in schedule order.

See also: [greengage-build](../greengage-build/SKILL.md) (restart after install),
[greengage-regress-tests](../greengage-regress-tests/SKILL.md) (isolation2 harness), [greengage-debug](../greengage-debug/SKILL.md).
