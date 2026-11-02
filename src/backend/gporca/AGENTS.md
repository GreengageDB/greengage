# GPORCA development

Read the relevant subsystem documentation before changing statistics,
transformations, or costing. GPORCA contains libgpos, libgpopt, libgpdbcost,
and libnaucrates. PostgreSQL tree and metadata translation is in the sibling
`src/backend/gpopt` directory.

## Formatting

Use clang-format 11 through `src/tools/fmt`, from the repository root.
The formatter also covers `src/backend/gpopt` and `src/include/gpopt`.
Limit formatting to the intended change and inspect the resulting diff.

```bash
CLANG_FORMAT=clang-format-11 src/tools/fmt chk
```

The CI linter regenerates configuration, checks `git diff --exit-code`, and
runs the format check. Its worktree must be clean. See `README.format.md`
for format intent, but use the version pinned in `ci/Dockerfile.linter`.

## Builds and tests

The database build includes GPORCA. Run its unit tests through the repository
container entry point with an existing image built from the intended source.

```bash
docker run --rm -it gpdb7_regress:latest \
  bash -c 'gpdb_src/concourse/scripts/unit_tests_gporca.bash'
```

On ARM hosts add `--platform linux/amd64` when emulation is available.
The runner builds and tests both RelWithDebInfo and Debug configurations.
Database regressions require rebuilt and installed database binaries as well
as the GPORCA libraries. Use the root guide for expected-output selection.
