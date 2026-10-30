# Management development

Management code and tests use Python 3, four-space indentation, and Pylint.
Database connections use `psycopg2`. Reuse existing Behave step libraries.
Add scenarios under `gpMgmt/test/behave` to a suitable existing feature file,
or create one when none covers the command.

## Behave tests

Run selected tags or flags from `gpMgmt` with the installed database environment
sourced. The make target requires `tags` or `flags`.

```bash
make -f Makefile.behave behave tags=smoke
make -f Makefile.behave behave flags='--tags ~concourse_cluster'
```

Images install Behave 1.2.6. The tag syntax described under Behave 1.2.4 in
`gpMgmt/test/README` still applies. Scenarios tagged `@concourse_cluster`
require a multi-host Concourse cluster and are excluded by the Compose runner.
Prefer selected features or tags. Run the full suite only when requested or
required by the change.

## Compose cluster

Run the following from the repository root using an existing suitable image.
The cluster contains one `cdw` and three segment hosts.

```bash
mkdir -p sqldump
touch sqldump/dump.sql
export IMAGE='gpdb7_regress:latest'
bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

Ensure `sqldump/dump.sql` is a file before starting Compose. The runner creates
`allure-results` and `ssh_keys`, and removes containers and volumes with
`docker compose down -v` after each feature. `CI=1` adds log collection before
cleanup. See `ci/readme.md` for container setup.
