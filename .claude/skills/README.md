# Project skills

Reusable skills distilled from the PG14 → GGDB merge, fix, and
regression-reconciliation work. Each is a Claude Code skill
(`.claude/skills/<name>/SKILL.md`, auto-discovered); a human can also just read it.

| Skill | Use it when |
|---|---|
| [greengage-build](greengage-build/SKILL.md) | A source change must become a running binary; avoiding the stale-binary trap |
| [greengage-regress-tests](greengage-regress-tests/SKILL.md) | Running a regress/isolation2 test under optimizer on/off, with setup deps |
| [greengage-answer-file-regen](greengage-answer-file-regen/SKILL.md) | A test's diff is cosmetic drift — regenerate `.out` without masking a real bug |
| [greengage-cluster-ops](greengage-cluster-ops/SKILL.md) | Monitoring a long run, disk/health/OOM, recovering a degraded/crashed cluster |
| [greengage-debug](greengage-debug/SKILL.md) | A crash/assert/hang/wrong-result — logs, repro, instrumentation, gdb |
| [greengage-internals](greengage-internals/SKILL.md) | Writing/reviewing a backend fix — MPP planner/executor, merge re-graft method |

These complement `CLAUDE.md` (build/test/style) and the
major-version-merge workflow it references.
