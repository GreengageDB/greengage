# Management development

Management code and tests use Python 3, four-space indentation, and Pylint;
database connections use `psycopg2`. Reuse existing Behave step libraries.
Add scenarios under `gpMgmt/test/behave` to a suitable existing feature file,
or create one when none covers the command.

## Behave tests

Run selected tags or flags from `gpMgmt` with the installed database
environment sourced; the make target requires `tags` or `flags`.

```bash
make -f Makefile.behave behave tags=smoke
make -f Makefile.behave behave flags='--tags ~concourse_cluster'
```

Images install Behave 1.2.6; the tag syntax described under Behave 1.2.4 in
`gpMgmt/test/README` still applies. Scenarios tagged `@concourse_cluster`
need a multi-host Concourse cluster and are excluded by the Compose runner.
Prefer selected features or tags; when the scenarios covering a change pass,
ask the user to push and let CI run the full suite.

## Compose cluster

Run from the repository root with an existing suitable image. The cluster has
one `cdw` and three segment hosts.

```bash
mkdir -p sqldump
touch sqldump/dump.sql
export IMAGE='gpdb7_regress:latest'
bash ci/scripts/run_behave_tests.bash gpstart gpstop
```

Make `sqldump/dump.sql` a file before starting Compose. The runner creates
`allure-results` and `ssh_keys`, and removes containers and volumes with
`docker compose down -v` after each feature; setting `CI` skips that cleanup,
leaving containers and volumes in place. See `ci/readme.md` for container
setup.
