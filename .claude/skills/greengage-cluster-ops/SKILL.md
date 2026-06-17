---
name: greengage-cluster-ops
description: Operate the gpdemo MPP cluster during long test/build cycles - read segment health correctly, monitor disk and long-running jobs, detect OOM, and recover a degraded/crashed cluster with gprecoverseg. Use when a run hangs/crashes, a segment goes down, "Cluster validation failed" appears, or disk fills.
---

# Operating & recovering the gpdemo cluster

## Segment layout (gpdemo, 3 primaries + mirrors + standby)

- Coordinator: content **-1**, dbid1, port **7000**, `MASTER_DATA_DIRECTORY=.../qddir/demoDataDir-1`. Standby: dbid8/7001.
- content 0: primary dbid2/7002, mirror dbid5/7005. content 1: dbid3/7003 + dbid6/7006. content 2: dbid4/7004 + dbid7/7007.

## Read health correctly

```sql
select role,preferred_role,mode,status,count(*) from gp_segment_configuration group by 1,2,3,4 order by 1,2;
```
Healthy looks like `m|m|s|u|4`, `p|p|s|u|3`, **`p|p|n|u|1`**. The last is the
coordinator (content -1) — **mode `n` for content -1 is NORMAL** (its standby is
tracked via `pg_stat_replication`, `gp_walreceiver streaming sync`), do not treat
it as degraded. Trouble = `status=d` (down) or `role<>preferred_role` (failed over).

## Disk hygiene (regress workflows leak disk)

- `df -h` between runs. Cores, 13 GB datadirs, and docker images fill the disk;
  a full disk can truncate host source files (restore from container/HEAD).
- Set `core_pattern=|/bin/true` so assert cores don't pile up.
- Clean `regression.diffs`, `results/`, leftover temp dbs, and `/tmp/*.log` promptly.

## Monitor a long run (build or full schedule, 20-40 min)

Run it detached and poll its log; don't block one command on the whole run:
```bash
# launch detached inside the container
nohup env PGOPTIONS="-c optimizer=off" make installcheck-good > /tmp/run.log 2>&1 &
# poll for completion markers (until-loop; cache-friendly 6s ticks)
for i in $(seq 1 90); do grep -qE "tests passed|tests, .* failed|make.*Error" /tmp/run.log && break; sleep 6; done
grep -c "\.\.\. FAILED" /tmp/run.log
```

## OOM under load (small gpdemo)

The full concurrent schedule OOMs the small gpdemo: `ERROR: Out of memory` /
`failed to acquire resources on one or more segments`, and a heavy parallel group
can **crash a segment** (it flips to `m|p|n|d`), after which `pg_regress` prints
`Cluster validation failed` and runs 0 tests. Mitigate with `--max-connections=3`
(or `MAX_CONNECTIONS=3 make ...`), or run a lighter schedule prefix that excludes
the OOM-heavy `qp_*` (those are in greenplum_schedule, not parallel_schedule).

## Recover a degraded/crashed cluster

```bash
source greenplum_path.sh; export PGPORT=7000 MASTER_DATA_DIRECTORY=.../qddir/demoDataDir-1
gprecoverseg -a                         # recover the down segment (as a mirror)
# wait for WAL resync: poll until count(mode<>'s' and content>=0)=0
gprecoverseg -ar                        # rebalance: restore preferred primary/mirror roles
# if the coordinator itself is down:
gpstart -a                              # or gpstop -ar to restart
```

Restart fragility: `gpstop -ar` on a cluster with a hung motion-IPC QE FAILS to
shut down cleanly (sends SIGQUIT then SIGABRT), crashing it — then `gpstart -a`
to bring it back, `gprecoverseg` to re-sync. An uninterruptible motion-IPC hang
(e.g. an executor infinite loop) cannot be killed by `statement_timeout`; you must
`pg_terminate_backend` the stuck pid, and it may leave a degraded segment.

See also: [greengage-build] (restart after install), [greengage-debug].
