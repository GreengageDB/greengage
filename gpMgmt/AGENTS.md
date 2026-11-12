# Management development

Management code and tests support Python 2.7 and Python 3.9 through 3.12, use
four-space indentation and Pylint, and must run on every runtime the affected
component requires. Ubuntu 22.04 development images use Python 2 for
management and Behave tests; Python 3 builds install PyGreSQL wheels for
isolation2. Reuse existing Behave step libraries. Add scenarios under
`gpMgmt/test/behave` to a suitable existing feature file, or create one when
none covers the command.

## Behave tests

Run selected tags or flags from `gpMgmt` with the installed database
environment sourced; the make target requires `tags` or `flags`.

```bash
make -f Makefile.behave behave tags=smoke
make -f Makefile.behave behave flags='--tags ~concourse_cluster'
```

The bundled Behave is 1.2.6; the tag syntax described under Behave 1.2.4 in
`gpMgmt/test/README` still applies. Tag scenarios that need the multi-host
cluster with `@concourse_cluster`. Prefer selected features or tags; when the
scenarios covering a change pass, ask the user to push and let CI run the
full suite.

## Compose cluster

Run from the repository root with an existing suitable image. The cluster
has one `cdw` and six segment hosts, and the runner runs tagged and untagged
scenarios separately.

```bash
mkdir -p sqldump
touch sqldump/dump.sql
export IMAGE='gpdb6_regress:latest'
bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

Make `sqldump/dump.sql` a file before starting Compose. The runner creates
`allure-results`, `coverage`, and `ssh_keys`; `CI=1` skips the
`docker compose down -v` cleanup, so use it when containers and volumes must
stay for inspection. See `ci/readme.md` for container setup.
