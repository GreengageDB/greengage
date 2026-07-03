---
name: greengage-pg-merge
description: Resolve conflicts and bring up a PostgreSQL major-version merge into the GGDB (MPP) fork - the campaign shape (one major version per step, pinned merge targets), semantic conflict resolution with reference branches, organizing hundreds of conflicts into clusters and batch sweeps, the phased bring-up (compile/link -> mock unit tests -> initdb -> regress matrix -> isolation2 -> CI, each catching a class the previous cannot), per-area verification beyond conflict markers, recurring merge-artifact garbage, and clean-merge traps. Use when merging an upstream PG major version, resolving its conflicts, or hunting a regression introduced by such a merge.
---

# Bringing up a PG major-version merge in GGDB

## Campaign shape
Go **one major version per step** (14→15→16→...), never a multi-version jump: a
direct jump stacks several upstream rewrites over each GGDB re-graft with no
buildable checkpoint in between. Pinned upstream targets (each = merge-base of
upstream `master` and the `REL_x_0` tag; add a `postgres/postgres` remote and
`git fetch <remote> master --tags`):

| Step | Target | Notes |
|---|---|---|
| PG15 | `adadae45816` | done on branch claude-merge-3 |
| PG16 | `97d89101045` | in progress on claude-merge-4 |
| PG17 | `7dcc6f8e6d7` | pinned |
| PG18 | `9c5b9a280cb`, then tag `REL_18_4` (`f5cc81719e6`) | pinned |

Each campaign gets its own branch + git worktree + docker build container
(see [greengage-build](../greengage-build/SKILL.md)). Start:
```
git merge --no-commit --no-ff <target>
git diff --name-only --diff-filter=U   # record the conflict inventory
```
The merge state persists on disk; `git add` staged resolutions survive in the
index. Do NOT commit until every conflict is resolved AND build + core regress
are green; recover a mis-resolved unstaged file with `git checkout -m -- <file>`;
`git merge --abort` nukes everything. Calibrate effort: a PG14-sized step was
~2000 commits / ~3600 files / ~530-760 conflicts, plus ~350 follow-up
bug/regen commits after the merge itself.

## Resolution rules (never blind ours/theirs)
1. **Adopt the upstream API shape first, then re-graft GGDB logic** (MPP, AO,
   distribution, ORCA, dispatch) into the new shape; see
   [greengage-internals](../greengage-internals/SKILL.md) for what it *means*.
2. The authoritative sides of a conflicted file:
   `git show :2:<path>` = ours (GGDB before merge; also `<merge>^1:<path>`),
   `git show :3:<path>` = theirs (upstream target; also `<merge>^2:<path>`).
3. To recover a GGDB re-graft the merge dropped/mangled, diff against a
   **reference branch** close to the current base — NOT `adb-6.x` (PG9.4, too old):

   | Branch | What it gives you |
   |---|---|
   | `origin/adb-8.x` | closest settled GGDB state (PG14-based) |
   | `origin/adb-7.2.0` / `origin/adb-7x` | last stable GGDB major |
   | previous `claude-merge-N` | the prior bump's resolution of the same file |
   | `gg_upgrade` | pure upstream (pre-merge PG base) |

   Apache Cloudberry (`cloudberry` remote) is a shape reference for ≤PG14 ONLY
   (it never merged PG15+) — it diverges on optimizer/resource groups. A prior
   branch's resolution can itself be broken; verify before transplanting.
4. Merges use **2-way conflict style, not diff3**: hunks where ours == the
   merge base auto-resolve to THEIRS, so upstream's NEW scaffolding often
   already sits in the "shared" region around a conflict — take-HEAD silently
   orphans it. Read the surrounding clean-merged code first; if it already
   uses the new type/idiom, resolve the conflict to match ("adopt-theirs tell").
5. **"Both" is unsafe at function boundaries.** When two different functions
   share a `/*` opener or trailing `}` across a conflict, naive both-sides
   concatenation loses a delimiter. Same for the *tangled-function pattern*
   (a GGDB fn and an upstream fn interleaved across two hunks): extract each
   full function from `:2:`/`:3:` and write both sequentially. "Both" is fine
   only for additive case-labels / enum members / `#include` lines.
6. For a heavily-customized, self-contained GGDB subsystem where ALL upstream
   changes are unwanted (e.g. syslogger's pipe protocol), take ours (`:2:`)
   entirely rather than building a Frankenstein.

## Organizing hundreds of conflicts
- **Type the inventory first** (`git status --porcelain`): AU (added-by-us,
  usually GGDB-only tests → bulk keep after verifying absent upstream), DU
  (deleted-by-us: `.po` translations, doc sgml → bulk `git rm`), UD/AA, UU
  (the real semantic work). Copyright-only and pgindent-whitespace hunks are
  scriptable. Never hand-merge generated files (`configure`, `gram.c`) —
  resolve the source (`configure.ac`, `gram.y`) and regenerate.
- **Resolve as CLUSTERS, not alphabetically**: co-resolve each header with its
  .c family so signatures/enums stay consistent (node layer, memory contexts,
  xlog+recovery, pgstat, partition pruning, buffile/fileset...). Order:
  node layer → catalog/genbki headers → backend by subsystem → gram.y →
  build files → tests (defer `.out`/`.sql` to the regress phase — they don't
  block the build).
- **Big files get individual sessions** (planner.c, tablecmds.c, xlog.c,
  guc.c, tcop/postgres.c, vacuum.c, xact.c, bufmgr.c, pg_dump.c, ~anything
  ≥6 hunks). Do NOT batch these.
- **The long tail (1-4 hunk files) can be batch-swept** with a repeatable
  per-file recipe: adopt upstream shape + re-graft GGDB behavior; verify per
  file (markers == 0, brace-count delta vs `:3:` == 0) and record an explicit
  *uncertainty note* per file for a later audit pass, plus any **cross-file
  dependency** (e.g. "smgropen must stay 3-arg") so cluster files honor it.
  Nearly every real bug regress finds later is a re-graft a sweep dropped to
  pure-upstream — the uncertainty notes are how you find them.

## Phased bring-up — each phase catches a class the previous cannot
1. **Compile + link** — mechanical API-shape fixes; the compiler enumerates
   every caller of a changed signature. Deliberately defer wide caller sweeps
   ("build-sweep queue") to this phase. Link errors expose functions whose
   *definition* was dropped while decl/callers survived.
2. **Mock unit tests** — `make -s unittest-check`, run SERIAL (`-j` races on
   shared mock objects → spurious failures). Every new upstream GUC must land
   in exactly one of `src/include/utils/{sync,unsync}_guc_name.h` (new
   upstream GUCs → unsync) or the guc-coverage test fails. New params on a
   mocked fn need matching `expect_*`. CI stops at the FIRST unittest failure
   — always run the full suite locally.
3. **initdb / gpdemo bootstrap** — catalog/BKI/genbki regressions that neither
   the compiler nor mocks catch; they fire only when initdb builds template1.
   Run `perl src/include/catalog/duplicate_oids` early; renumber colliding
   GGDB OIDs to gaps from `src/include/catalog/unused_oids` (upstream grabs
   new ranges every version). Per-version catalog churn: [pg14 notes](pg14-notes.md).
4. **Regress matrix** — BOTH optimizers (`optimizer=off` planner first, then
   `optimizer=on` ORCA — ORCA's translator must be re-grafted for every new
   plan-node field or it silently produces wrong results). Then
   greenplum_schedule, isolation2, contrib, src/bin, and the CI matrix — each
   NEW suite reveals another layer of dropped re-grafts; budget for it. See
   [greengage-regress-tests](../greengage-regress-tests/SKILL.md) and [greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md).

## Markers gone ≠ resolved — per-area verifiers
`rg "^(<<<<<<<|=======|>>>>>>>)"` empty is necessary, not sufficient — run the area's own validator:
- **gram.y**: `bison -Wcounterexamples gram.y` (works standalone) catches what
  markers hide: duplicate %token/precedence declarations, duplicate productions
  (reduce/reduce conflicts), and GGDB nonterminals a new upstream feature made dead.
- **Node layer (PG16+)**: `src/backend/nodes/gen_node_support.pl` must exit 0
  after annotating GGDB nodes — see [pg16 notes](pg16-notes.md).
- **Catalogs**: `duplicate_oids` clean + initdb succeeds.
- **Changed signatures**: sweep ALL callers with a multi-line-aware search —
  clean-merged callers keep the old arity without any conflict marker.
- **WAL record layout**: the write path's serialization order must match the
  parse authorities (`ParseCommitRecord`/`ParseAbortRecord`) — grep those FIRST.
- **Brace balance**: depth != 0 after resolution means a hunk's structure
  diverged (Frankenstein function) — re-extract from `:2:`/`:3:`.

## Recurring merge-artifact garbage (recognize on sight)
- **Duplicate-and-truncate signatures**: both the old prototype and a
  truncated new one survive → "storage class specified for parameter" cascade.
- **Lost opening `/*` or `#ifdef`**: orphan `* foo` / `#else` → "missing
  terminating", "#endif without #if", or double-include redeclaration spam.
- **Header splits**: upstream splits a header; GGDB additions must MOVE into
  the new home, not stay duplicated in the old umbrella include.
- **Duplicate symbols/fields**: before taking THEIRS, check the clean-merged
  region doesn't already contain the same symbol (double definition).
- **Orphaned duplicate functions** after a file split (e.g. xlog.c →
  xlogrecovery.c): the old copies remain, dead or link-colliding.

## Clean-merge traps — files with NO markers can still be wrong
- Auto-merge keeps **old-signature callers** in GGDB-only code paths upstream
  never touched (PG15 tablecmds.c: 5 old-arity `ATSimplePermissions` calls in GGDB `ATPrepCmd` cases).
- A file split can drop a GGDB function's **definition and call** while the
  forward-decl survives (PG15 xlog split dropped `XLogProcessCheckpointRecord`
  → DTX checkpoint payload silently never replayed). Cross-check: any header
  extern with no definition left in the split-file family = dropped function.
- If the merge target **predates** an upstream fix GGDB had already
  backported, git takes upstream wholesale and silently drops the backport
  (PG14 `xactCompletionCount` bump in `XidCacheRemoveRunningXids`).

Per-version class references: [pg14-notes.md](pg14-notes.md) ·
[pg15-notes.md](pg15-notes.md) · [pg16-notes.md](pg16-notes.md).
Siblings: [greengage-internals](../greengage-internals/SKILL.md) ·
[greengage-build](../greengage-build/SKILL.md) · [greengage-regress-tests](../greengage-regress-tests/SKILL.md) ·
[greengage-answer-file-regen](../greengage-answer-file-regen/SKILL.md) · [greengage-ci-triage](../greengage-ci-triage/SKILL.md) ·
[greengage-cluster-ops](../greengage-cluster-ops/SKILL.md) · [greengage-debug](../greengage-debug/SKILL.md).
