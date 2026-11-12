# GPORCA development

Read the relevant subsystem documentation before changing statistics,
transformations, or costing. GPORCA contains the `libgpos`, `libgpopt`,
`libgpdbcost`, and `libnaucrates` libraries. PostgreSQL tree and metadata
translation is in the sibling `src/backend/gpopt` directory.

## Search model

GPORCA is a top-down, cost-based optimizer in the Volcano lineage, refined by
Cascades: optimization starts at the root of the query and recursively asks
each group of equivalent expressions for a plan that satisfies the required
properties. The PostgreSQL planner instead works bottom up in the System R
tradition, combining base-relation paths pairwise and keeping the cheapest
survivor per relation set and sort order, with the GEQO genetic search past
roughly 12 relations.

The search space lives in a Memo of equivalence groups. Exploration rules
(unnesting, join ordering) run before implementation rules, and required
properties are enforced by explicit operators: `Sort` for order,
`Materialize` for rewindability, and Motion (`Redistribute`, `Broadcast`,
`Gather`) for distribution. Distribution is therefore costed like any other
property; the legacy planner instead inserts Motions through heuristics in
the cdb planning code. Queries GPORCA cannot optimize fall back to the
legacy planner; `optimizer_log_failure` controls what gets logged for each
fallback.

## Formatting

Use clang-format 11 through `src/tools/fmt` from the repository root; it also
covers `src/backend/gpopt` and `src/include/gpopt`. Format only the intended
change and inspect the diff.

```bash
CLANG_FORMAT=clang-format-11 src/tools/fmt chk
```

The CI linter regenerates configuration, checks `git diff --exit-code`, and
runs the format check; its worktree must be clean. See `README.format.md`
for format intent, but use the version pinned in `ci/Dockerfile.linter`.

## Builds and tests

The database build includes GPORCA. Run its unit tests through the repository
container entry point with an existing image built from the intended source.

```bash
docker run --rm -it gpdb7_regress:latest \
  bash -c 'gpdb_src/concourse/scripts/unit_tests_gporca.bash'
```

On ARM hosts add `--platform linux/amd64` when emulation is available. The
runner builds and tests both RelWithDebInfo and Debug configurations.
Database regressions require rebuilt and installed database binaries as well
as the GPORCA libraries. Use the root guide for expected-output selection.
