---
name: greengage-build
description: Build and install the GGDB backend (and ORCA) inside the docker test container, and avoid the stale-binary trap where committed source fixes silently don't take effect. Use whenever a C/C++ source change needs to become a running binary, before re-testing.
---

# Building GGDB in the container

Builds run in the `gpdb_clean` docker container (not natively), as `gpadmin`,
with `sudo`. The source tree the build compiles is `/home/gpadmin/gpdb_src`
inside the container — **this is a plain copy, not a git checkout** — while the
git working tree is on the host at `/home/dvoronkov/ws/gpdb`. You must copy
edited files in before building.

## Edit → build → install → run loop

```bash
# 1. copy the edited file into the container source tree
sudo docker cp src/backend/<path>.c gpdb_clean:/home/gpadmin/gpdb_src/src/backend/<path>.c

# 2. CRITICAL: touch after docker cp — docker cp preserves the host mtime, which
#    is often OLDER than the .o, so make would NOT recompile (the "stale" trap).
sudo docker exec -u gpadmin gpdb_clean bash -lc 'touch /home/gpadmin/gpdb_src/src/backend/<path>.c'

# 3. rebuild the backend and (re)link + install
sudo docker exec -u gpadmin gpdb_clean bash -lc \
  'cd /home/gpadmin/gpdb_src/src/backend && make >/tmp/bld.log 2>&1 && make install >>/tmp/bld.log 2>&1 && echo BUILD_OK || (echo BUILD_FAIL; grep -iE "error:" /tmp/bld.log | head)'

# 4. the running cluster only picks up the new postgres binary after a restart
sudo docker exec -e USER=gpadmin -u gpadmin gpdb_clean bash -lc \
  'source /usr/local/greenplum-db-devel/greenplum_path.sh; export PGPORT=7000 \
     MASTER_DATA_DIRECTORY=/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1; \
   gpstop -ar'
```

`GPHOME=/usr/local/greenplum-db-devel`. `docker exec` does NOT set `$USER`;
pass `-e USER=gpadmin` or `gpstop`/`gprecoverseg`/`gpconfig` abort with
"USER environment variable must be set".

## The stale-binary trap (read this before concluding "my fix doesn't work")

The installed `/usr/local/greenplum-db-devel/bin/postgres` can be timestamp-
stale relative to the source: a committed fix is in the `.c` but NOT in the
running binary, so behavior looks unfixed and pass-counts are measured against
stale code. Symptoms: a fix you can read in the source does not change runtime
behavior. Verify with `ls -la --time-style=+%Y-%m-%d_%H:%M` on the `.o`, the
`.c`, and `bin/postgres`; the binary must be newer than the source.

## ORCA vs assert builds (two different binaries surface different bugs)

- The assert campaign builds `--enable-cassert --disable-orca` (optimizer=off);
  `set optimizer=on` then errors "ORCA is not supported by this build".
- The ORCA jobs build `--enable-orca` (optimizer on works).
- The **assert∩ORCA intersection** (cassert + ORCA) was historically never
  tested and finds real crashes ("jit/regression tests with orca").
- To switch a container to ORCA+assert: drop `--disable-orca` from the configure
  line and rebuild. The gporca `.o` are usually already in-tree, so the rebuild
  is ~3 min (not from scratch).

## Don't `make clean`

`make clean` is destructive: it nukes the ORCA build (`src/backend/gporca/`),
forcing a long from-scratch rebuild and `-j` link races. Prefer `touch` + `make`
for targeted rebuilds. If you must rebuild ORCA, build `gporca` before linking.

See also: [greengage-cluster-ops] (restart/recovery), [greengage-debug]
(instrumentation rebuilds), [greengage-internals] (what to change).
