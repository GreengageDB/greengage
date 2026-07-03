# Greengage project skills

Reusable skills distilled from the PostgreSQL major-version bump campaigns
(PG14 → claude-merge-2 / ai-bump-1, PG15 → claude-merge-3, PG16 → claude-merge-4).
Each is a Claude Code skill (`.claude/skills/<name>/SKILL.md`, auto-discovered by
Claude Code sessions in this repo) and doubles as developer documentation — a
human can just read it.

| Skill | Use it when |
|---|---|
| [greengage-pg-merge](greengage-pg-merge/SKILL.md) | Merging a pinned upstream PG major version: conflict clustering, semantic resolution with reference branches, phased bring-up, clean-merge traps. Per-version trap notes: [PG14](greengage-pg-merge/pg14-notes.md) · [PG15](greengage-pg-merge/pg15-notes.md) · [PG16](greengage-pg-merge/pg16-notes.md) |
| [greengage-internals](greengage-internals/SKILL.md) | Writing/reviewing a backend fix that must be MPP-correct — QD/QE dispatch, Motion/locus, DTX, FTS, AO tables, recurring GGDB bug classes. Full [GGDB-vs-vanilla-PostgreSQL map](greengage-internals/greengage-vs-postgres.md) |
| [greengage-build](greengage-build/SKILL.md) | Turning a C/C++ edit into the running binary in the campaign container — stale-binary, root-owned-artifact, and ORCA-relink traps |
| [greengage-regress-tests](greengage-regress-tests/SKILL.md) | Running one test or a whole suite against gpdemo, optimizer on/off, expected-file selection, false-pass traps. Suite inventory: [suites.md](greengage-regress-tests/suites.md) |
| [greengage-answer-file-regen](greengage-answer-file-regen/SKILL.md) | A failing test's diff looks cosmetic — regen (or better, init_file-mask) without burying a real bug |
| [greengage-cluster-ops](greengage-cluster-ops/SKILL.md) | gpdemo is down/degraded/wedged/out of disk, or needs creating, recreating, or stabilizing for HA tests |
| [greengage-debug](greengage-debug/SKILL.md) | A crash/assert/hang/wrong-result — repro, elog, fault injection, gdb. Log locations & signatures: [reading-logs.md](greengage-debug/reading-logs.md) |
| [greengage-ci-triage](greengage-ci-triage/SKILL.md) | A CI run is red — fetch artifacts, classify cosmetic vs real vs flaky across the shared-answer-file job matrix |

These complement `CLAUDE.md` (build/test/style basics).

## Hard rules

The non-negotiables every campaign re-learned the hard way; each skill has the detail.

1. **Never resolve a conflict by blindly taking ours/theirs.** Adopt the upstream
   API shape, then re-graft the GGDB logic into it.
2. **Conflict markers gone ≠ resolved.** Run the area's verifier: `bison
   -Wcounterexamples` for gram.y, `gen_node_support.pl` for the node layer,
   `duplicate_oids` + initdb for catalogs, a caller sweep for changed signatures.
   And files with NO markers can still be wrong (clean-merge traps).
3. **The success→error safety gate.** Never regenerate an answer file whose gpdiff
   shows a committed result replaced by an ERROR — that is a real bug, not drift.
4. **All regress CI jobs share `expected/*.out`.** A regen that greens one job can
   break another; regenerate only from the failing job's CI results tarball.
5. **Prefer an `init_file` mask over baking environment-specific output** (addresses,
   ports, db names) into an expected file.
6. **Distrust binaries you didn't just build.** Verify `.c` → `.o` → installed
   binary mtimes before concluding a fix "doesn't work" (stale-binary trap).
7. **No `make clean`** in campaign containers — it nukes the ORCA build.
8. **Before "fixing" an assert/PANIC, look for the test that validates it** — some
   panics are designed behavior (e.g. mirror self-heal).
9. **`--use-existing` single-test runs are not authoritative.** Pollution and missing
   setup fake failures; the fresh sequential installcheck run is the truth.
10. **Flaky failures rotate — don't regen them.** Confirm by rerun; a regen just
    bakes one run's nondeterminism.
