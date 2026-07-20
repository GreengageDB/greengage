# TPC-DS benchmark for GreengageDB

Runs the [dimoffon/TPC-DS](https://github.com/dimoffon/TPC-DS) benchmark against a
Greengage demo cluster in Docker, writes a markdown report per version, and can
compare versions side-by-side (e.g. **`adb-8.x`** vs **`claude-merge-7`**).

## How it's shaped (and why one container)

The TPC-DS harness is **coordinator-coupled**: `02_init`/`01_gen_data`/`04_load`
call `gpconfig`/`gpstop`/`gpstate` and a *local* `psql` (reading
`MASTER_DATA_DIRECTORY`), and `scp` `dsdgen` to segment hosts to generate data
there. It therefore has to run *on the coordinator host*. So a single container
plays both roles:

* the **Greengage cluster** — a demo cluster (coordinator + N primary segments), and
* the **TPC-DS utility** — the benchmark driver (`dsdgen`/`dsqgen` + the harness).

The segment host is the container itself (`segment_hosts.txt` == its hostname),
so data generation and `gpfdist` happen over ssh-to-self. This keeps the
topology identical across versions, which is what you want for a fair compare.

The **only** thing that differs between versions is the base image
(`GREENGAGE_IMAGE`), built from `ci/Dockerfile.ubuntu` on the branch under test.

## GreengageDB-8 compatibility overlay

The stock harness only recognises "Greenplum Database 4.3/5/6". GG8 reports
`PostgreSQL 18.4 (Greenplum Database 8.0.0…)` and would fall through to
single-node PostgreSQL mode (heap tables, no segment distribution, and the
removed `pg_filespace_entry`). `patches/apply.sh` (applied at image build):

1. maps **GG7/GG8 → the `gpdb_6` code path** (AO column storage, distributed
   `dsdgen`, `gpfdist` load, `gp_segment_configuration.datadir`);
2. makes `02_init`'s GP6-era GUC / resource-group tuning **best-effort** so one
   incompatible knob (e.g. `gpconfig --masteronly`, or `admin_group` resource-
   group ALTERs on a resource-*queue* cluster) can't abort the whole run.

> **First-run note:** the `gpdb_6` path is close but not guaranteed 1:1 on GG8.
> Likely shake-out points on the first real run: `gpconfig` flag names,
> `pg_compression`/`quicklz` probe, resource-group vs resource-queue defaults,
> and any GP6-only SQL in individual query templates. Check
> `logs/tpcds_<LABEL>_run.log`; extend `patches/apply.sh` as needed.

## Prerequisites

Build the Greengage image for each branch you want to benchmark (from a checkout
of that branch, at the repo root):

```bash
# on claude-merge-7
docker build -t greengage8_u22:claude-merge-7 -f ci/Dockerfile.ubuntu .
# on adb-8.x
git worktree add /tmp/adb8 adb-8.x && cd /tmp/adb8
docker build -t greengage8_u22:adb-8.x -f ci/Dockerfile.ubuntu .
```

## Run one version

```bash
cd ci/tpcds
GREENGAGE_IMAGE=greengage8_u22:claude-merge-7 LABEL=claude-merge-7 ./run.sh
# -> results/tpcds_claude-merge-7.md
```

Knobs (caller env > `.env` > default): `SCALE` (GB, default 1), `MULTI_USER_COUNT`,
`SINGLE_USER_ITERATIONS`, `RUN_MULTI_USER`, `EXPLAIN_ANALYZE`,
`NUM_PRIMARY_MIRROR_PAIRS`, `WITH_MIRRORS`, `KEEP_UP=true` (leave the container up
for debugging), `TPCDS_REF` (pin the harness commit).

## Compare adb-8.x vs claude-merge-7

```bash
cd ci/tpcds
GREENGAGE_IMAGE=greengage8_u22:adb-8.x        LABEL=adb-8.x        SCALE=1 ./run.sh
GREENGAGE_IMAGE=greengage8_u22:claude-merge-7 LABEL=claude-merge-7 SCALE=1 ./run.sh
./compare.sh adb-8.x claude-merge-7
# -> results/comparison.md   (per-query seconds + Δ%, positive = cm7 slower)
```

## Outputs

| Path | Contents |
|---|---|
| `results/tpcds_<LABEL>.md` | summary (load/analyze/single-user/multi-user/**Score**) + per-query timings |
| `results/<LABEL>/queries.tsv` | raw per-query seconds (input to `compare.sh`) |
| `results/comparison.md` | side-by-side across labels with Δ% |
| `logs/tpcds_<LABEL>_run.log` | full harness output (data gen, load, every query, score) |

## Files

```
ci/tpcds/
  docker-compose.yaml     single `tpcds` service (cluster + driver)
  Dockerfile              GREENGAGE_IMAGE + TPC-DS harness + overlay
  .env                    default knobs
  run.sh                  build → up → bring-up → run → aggregate (one version)
  compare.sh              merge per-version TSVs into comparison.md
  patches/apply.sh        GreengageDB-8 compatibility overlay
  scripts/
    bringup_cluster.sh    install + demo cluster + ssh-to-self + benchmark db
    run_benchmark.sh      run the harness (rollout.sh) as gpadmin
    aggregate_results.sh  cluster tables -> markdown + queries.tsv
```

## Cleanup

`run.sh` tears the container down (`docker compose down -v`) on exit unless
`KEEP_UP=true`. `results/` and `logs/` are host bind-mounts and persist.
