# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This is **ArenaDatabaseDB (ADB)** — an MPP (Massively Parallel Processing) database fork of Greenplum Database (GPDB), itself built on PostgreSQL. The repo tracks upstream PostgreSQL and periodically merges new major PostgreSQL versions into GGDB-specific branches.

**Key branches:**
- `adb-6.x` — production ADB 6.x line (main branch for PRs)
- `ai-merge-stage1` — staging branch for the current PG 8.0.0-alpha.0 → GGDB merge
- `gg_upgrade` — tracks upstream PostgreSQL code

## Build

Requires: GNU make, autoconf 2.69, Bison, Flex, Perl, and standard C toolchain.

```bash
# Configure (first time or after configure.in changes)
./configure --prefix=/usr/local/pgsql

# Build everything
make world

# Install
make install

# Clean
make distclean
```

`make world` builds `src/` and `contrib/`. Plain `make` builds `src/` only.

## Running tests

```bash
# Run regression tests against a temporary installation (no running server needed)
make check

# Run against an already-running server
make installcheck

# Run all test suites (regress, isolation, pl, contrib, bin)
make check-world
make installcheck-world

# Run parallel regression tests
make installcheck-parallel

# Run a specific test file by name against a running server
cd src/test/regress && ./pg_regress --inputdir=. --schedule=serial_schedule <testname>

# Run isolation (concurrent transaction) tests
cd src/test/isolation && make installcheck
# Or a specific spec: ./pg_isolation_regress <specname>
```

Set `MAX_CONNECTIONS=N` to cap parallelism: `make check MAX_CONNECTIONS=4`.

## Code style

- **C/Perl**: tabs, 4-space indent. Run `pgindent` (see `src/tools/pgindent/README.gpdb`) before submitting.
- **Python**: spaces, 4-space indent. Must pass `pylint`.
- **Go**: formatted with `gofmt`.
- Formatting config is in `.editorconfig`.
- Follow [PostgreSQL Coding Conventions](https://www.postgresql.org/docs/current/source.html).

## Architecture

```
src/backend/         Main server process
  access/            Table and index access methods (heap, nbtree, gin, gist, brin, hash, spgist)
  catalog/           System catalog management
  commands/          SQL command execution (DDL)
  executor/          Query execution engine
  nodes/             Node type definitions, copy/equal/out functions
  optimizer/         Query planner (geqo, path, plan, prep, util)
  parser/            SQL parser
  replication/       WAL streaming, logical replication
  storage/           Buffer manager, file I/O, lock manager, page layout
  utils/             Memory management, error handling, type system, caching

src/include/         Header files (mirrors backend/ structure)
src/bin/             Client utilities: psql, pg_dump, pg_ctl, pg_basebackup, pg_upgrade, etc.
src/pl/              Procedural languages: plpgsql, plperl, plpython, tcl
src/test/            Test suites: regress, isolation, authentication, subscription, ssl
contrib/             Optional extensions (pg_stat_statements, pageinspect, postgres_fdw, etc.)
```

GGDB/ADB-specific distributed execution concepts used throughout the codebase:
- **Motion nodes** — data movement operators between MPP segments
- **Slices** — independent units of parallel execution
- **ORCA** — the Greenplum cost-based optimizer (referenced in optimizer/ and JIT-related code)
- **arenadata_toolkit** — ADB-specific monitoring extension (tested in isolation2 tests)

## PostgreSQL major-version merge workflow

The primary ongoing task on `ai-merge-stage1` is merging upstream PostgreSQL into GGDB. The merge re-graft methodology, MPP internals, and recurring bug classes are documented in the [`greengage-internals`](./.claude/skills/greengage-internals/SKILL.md) skill; see also the build/test/debug/regen/cluster skills under [`.claude/skills/`](./.claude/skills/). Key points:

1. `git merge --no-commit --no-ff <upstream-tag>`
2. Record conflicts: `git diff --name-only --diff-filter=U`
3. Resolve semantically — never blindly take `ours` or `theirs`
4. Adopt upstream API shapes first; re-graft GGDB-specific logic into the new shape
5. Verify: `rg "^(<<<<<<<|=======|>>>>>>>)"` must return nothing
6. Build and run targeted regression tests before finalizing

Reference commits for resolution style: `1e11aaff762`, `f2b03841`, `1fa092913d2`, `3e9744465db`, `ed7a5095716ee`, `4dbcb3f844ec`, `a91e2fa94180`, `55a1954da16`, `80831bcdbe`, `eb57bd9c1`.
