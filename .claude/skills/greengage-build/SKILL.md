---
name: greengage-build
description: Build and install the GGDB backend (and ORCA) inside the per-campaign docker test container, and avoid the stale-binary trap where committed source fixes silently don't take effect. Covers the edit→sync→touch→make→install→restart loop, root-owned-artifact and mtime traps, configure flag sets (cassert/ORCA/LLVM) and what each surfaces, the no-make-clean rule, ORCA relink gotchas, and the CI build entry point. Use whenever a C/C++ source change needs to become a running binary, before re-testing, or when a build fails with permission/link/stale-object symptoms.
---

# Building GGDB in the container

Builds run inside docker (not natively on the host), as user `gpadmin`.
Docker needs `sudo`. **Each merge campaign pairs one container with one git
worktree** (own branch, own cluster, own install dir) — never mix them; below,
`<container>` is that campaign's container. Two source-tree styles exist:

- **Copied tree** — source lives at `/home/gpadmin/gpdb_src` inside the
  container, a plain copy (NOT a git checkout). Host worktree is the source of
  truth; you must sync edits in before building.
- **Bind-mounted worktree** — `docker run -v <host-ws>:<same-path>` mounts the
  whole workspace (mount the parent so git-worktree `.git` pointer files
  resolve); run `git config --global --add safe.directory <path>` inside.
  Start containers with `--sysctl "kernel.sem=500 1024000 200 4096"` if a
  cluster will run there.

`GPHOME=/usr/local/greenplum-db-devel` (must be `mkdir` + `chown gpadmin`
before the first `make install`). `docker exec` does NOT set `$USER`; pass
`-e USER=gpadmin` or `gpstop`/`gprecoverseg`/`gpconfig` abort with
"USER environment variable must be set".

## Edit → build → install → run loop

```bash
# 1. sync edits into a copied tree (skip if bind-mounted).
#    Single file:
sudo docker cp src/backend/<path>.c <container>:/home/gpadmin/gpdb_src/src/backend/<path>.c
#    Many files: tar czf /tmp/fix.tgz src/include src/backend && docker cp ... &&
#    in-container: tar --no-same-owner --overwrite -xzf + chown -R gpadmin

# 2. CRITICAL: touch after any copy — docker cp/tar preserve the HOST mtime,
#    often OLDER than the .o, so make silently skips recompiling (stale trap).
sudo docker exec -u gpadmin <container> bash -lc 'touch /home/gpadmin/gpdb_src/src/backend/<path>.c'

# 3. rebuild and install
sudo docker exec -u gpadmin <container> bash -lc \
  'cd /home/gpadmin/gpdb_src/src/backend && make -j8 >/tmp/bld.log 2>&1 && make install >>/tmp/bld.log 2>&1 \
   && echo BUILD_OK || (echo BUILD_FAIL; grep -iE "error:" /tmp/bld.log | head)'

# 4. the running cluster only picks up the new binary after a restart.
#    You must source BOTH env files or gpstop silently fails
#    ("COORDINATOR_DATA_DIRECTORY not set!") and segments keep the OLD binary.
sudo docker exec -e USER=gpadmin -u gpadmin <container> bash -lc \
  'source /usr/local/greenplum-db-devel/greenplum_path.sh; \
   source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh; \
   gpstop -ar -M fast'
```

Since PG14, `gpstop -afr -M fast` ERRORS ("Can not mix --mode options with
older deprecated -f,-i,-s") — use `gpstop -ar -M fast` and always check
rc=0. Verify the restart happened: `ps -eo pid,lstart,cmd | grep
gp_role=execute` — `lstart` must be recent.

## The stale-binary trap (read before concluding "my fix doesn't work")

Symptom: a fix you can read in the source does not change runtime behavior,
and pass-counts are measured against stale code. Causes and checks:

| Cause | Detect / fix |
|---|---|
| `docker cp`/tar kept old mtime, `.o` looks current | `ls --time-style=+%H:%M x.c x.o`; always `touch` after sync |
| Installed but never restarted | `gpstop -ar` (see above); verify `lstart` of segment procs |
| Orphan coordinator survives `gpstop -af` on port 7000 | `readlink /proc/<qd-pid>/exe` ends `(deleted)` = stale; hard-kill all postgres as root, confirm `ss -ltn | grep :7000` free, gpstart, re-verify |
| Header changed without `--enable-depend` | every includer's `.o` is stale → mixed-ABI SIGSEGV; `find src/backend -name '*.o' -not -path './gporca/*' -delete` then full make |
| Per-dir `make -C` on a partially-synced tree | FALSE RC=0 from stale `.o`. Authoritative check = full re-sync + touch-all + full `make` |
| Clock-skewed future-dated binary | `touch -d <future+1>` the source so make relinks |

Gold-standard verification: `objdump`/`nm` the installed binary (or
`strings /proc/<pid>/exe | grep <probe-tag>`) for one changed symbol before
trusting any test result.

## Root-owned build artifacts

A build ever run as root (default `docker exec` user, or root `make install`)
leaves root-owned `.o`/`.bc` files; gpadmin's next build dies with
`unable to open output file 'X.bc': Operation not permitted`. Fix:
`chown -R gpadmin:gpadmin <srctree>` (and `$GPHOME`, `/home/gpadmin`,
`~gpadmin/gpAdminLogs`) before building as gpadmin.

## Configure flag sets

Canonical campaign configure (assert build):

```bash
./configure --prefix=/usr/local/greenplum-db-devel --disable-orca \
  --enable-gpcloud --enable-mapreduce --enable-orafce --with-gssapi \
  --with-libxml --with-openssl --with-perl --with-python --with-uuid=e2fs \
  --with-llvm --enable-cassert --enable-debug --enable-debug-extensions \
  --enable-depend PYTHON=python3
```

- `--enable-cassert` — assertion checks; surfaces FailedAssertion crashers
  invisible in production builds. Debug-friendly (`--enable-debug`).
- `--enable-orca` (default) vs `--disable-orca` — with `--disable-orca`,
  `set optimizer=on` errors "ORCA is not supported by this build". The
  **cassert ∩ ORCA intersection** was historically never CI-tested and finds
  real crashes — build it deliberately.
- `--with-llvm` — JIT (`llvmjit.so`); required to reproduce the JIT CI jobs.
- `--enable-depend` — header-dependency tracking; without it see the
  mixed-ABI trap above.

If `configure.ac` pins autoconf 2.69 and the container has 2.71: temporarily
patch the version check, run `autoconf`, restore — never hand-merge a
conflicted `configure`, regenerate it.

## Don't `make clean` — and other ORCA link traps

- `make clean` (and a repo-root `make` in a wrecked tree) regenerates the
  gporca `objfiles.txt` lists and deletes the ORCA C++ objects in
  `src/backend/gpopt/` → postgres link fails with hundreds of
  `ld: cannot find gpopt/*.o / gporca/*.o`. Restore with
  `make -C src/backend/gpopt` (~2 min), then relink. Safer force-rebuild:
  `find src -name '*.c' -o -name '*.h' | xargs touch` then `make`.
- gporca Makefiles have NO header dependency tracking: editing a gporca/libgpos
  header rebuilds NOTHING — `find src/backend/gporca -name '*.o' -delete`,
  then rebuild (~10 min), and objdump-verify.
- Switching a `--disable-orca` tree to `--enable-orca`: the stale
  `src/backend/optimizer/plan/objfiles.txt` omits `orca.o` (only built when
  enable_orca=yes) → `undefined reference to optimize_query` at link. Force
  that dir's objfiles.txt to regenerate. Translator changes (`src/backend/
  gpopt/`) also need the relink to actually land (see `93a1ffbbc9d` on
  claude-merge-3 for a full ORCA-port example).
- `make -j` can race the final link (postgres links before subdir `SUBSYS.o`
  finish → `cannot find access/brin/brin.o`); run a serial `make` finish pass.
- After a wholesale header re-sync, genbki regenerates `_d.h` mid-build and
  `-j` compiles race it (transient "IndexId undeclared" cascades) — just run
  `make` a second time before debugging.

## CI build entry and unit tests

CI builds via `concourse/scripts/compile_gpdb.bash`: `make dist` from
`gpAux/`, then `make -s unittest-check` (GNUmakefile target; recurses
src/backend + src/bin cmockery mock tests). CI stops at the FIRST unit-test
failure, so always run the full `make -s unittest-check` locally (serially —
`-j` races shared mock objects) to find them all. Running and fixing suites:
[greengage-regress-tests](../greengage-regress-tests/SKILL.md).

## Native (non-docker) build

Only when explicitly wanted: `./configure --prefix=... && make world &&
make install` per the top-level README/CLAUDE.md. Plain `make` builds `src/`
only; `make world` adds contrib. The docker container is the supported path —
it carries the exact dependency versions the merge compiles against.

See also: [greengage-cluster-ops](../greengage-cluster-ops/SKILL.md)
(restart/recovery, demo cluster), [greengage-debug](../greengage-debug/SKILL.md)
(instrumentation rebuilds), [greengage-internals](../greengage-internals/SKILL.md)
(what to change), [greengage-pg-merge](../greengage-pg-merge/SKILL.md)
(build phase within a merge).
