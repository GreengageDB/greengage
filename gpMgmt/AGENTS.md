# Management development

Management code and tests support Python 2.7 and Python 3.9 through 3.12, and
use four-space indentation and Pylint. Ubuntu 22.04 development images use
Python 2 for management and Behave tests. Python 3 builds and installs PyGreSQL
wheels for isolation2. Use syntax and dependencies supported by every runtime
required by the affected component. Reuse existing Behave step libraries.
Add scenarios under `gpMgmt/test/behave` to a suitable existing feature file,
or create one when none covers the command.

## Behave tests

Run selected tags or flags from `gpMgmt` with the installed database environment
sourced. The make target requires `tags` or `flags`.

```bash
make -f Makefile.behave behave tags=smoke
make -f Makefile.behave behave flags='--tags ~concourse_cluster'
```

The bundled Behave is version 1.2.6. The tag syntax described under Behave
1.2.4 in `gpMgmt/test/README` still applies. Tag scenarios that need the
multi-host cluster with `@concourse_cluster`. Prefer selected features or tags.
Run the full suite only when requested or required by the change.

## Compose cluster

Run the following from the repository root using an existing suitable image.
The cluster contains one `cdw` and six segment hosts. The runner runs tagged
and untagged scenarios separately.

```bash
mkdir -p sqldump
touch sqldump/dump.sql
export IMAGE='gpdb6_regress:latest'
bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

Ensure `sqldump/dump.sql` is a file before starting Compose. The runner creates
`allure-results`, `coverage`, and `ssh_keys`. `CI=1` prevents it from running
`docker compose down -v`, so use `CI=1` when containers and volumes must remain
for inspection. See `ci/readme.md` for container setup.
