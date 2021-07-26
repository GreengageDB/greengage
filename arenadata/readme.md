Behave tests now can run locally with docker-compose.

Feature files are located in `gpMgmt/test/behave/mgmt_utils`
Before run tests you need to build a docker-image
```bash
docker build -t "hub.adsw.io/library/gpdb6_regress:${BRANCH_NAME}" -f arenadata/Dockerfile .
```

Command to run features:
```bash
# Run all tests
bash arenadata/scripts/run_behave_tests.bash

# Run specific features
bash arenadata/scripts/run_behave_tests.bash gpstart gpstop
```


Tests use `allure-behave` package and store allure output files in `allure-results` folder
**NOTE** that `allure-behave` has too old a version because it is compatible with `python2`.
Also, the allure report for each failed test has gpdb logs attached files. See `gpMgmt/test/behave_utils/arenadata/formatter.py`
It required to add `gpMgmt/tests` directory to `PYTHONPATH`. 

Greenplum cluster in Docker containers has its own peculiarities in preparing a cluster for tests.
All tests are run in one way or another on the demo cluster, wherever possible. 
For example, cross_subnet tests or tests with tag `concourse_cluster` currently not worked because of too complex cluster preconditions.

Tests in a docker-compose cluster use the same ssh keys for `gpadmin` user and pre-add the cluster hosts to `.ssh/know_hosts` and `/etc/hosts`.

Docker containers have installed `sigar` libraries. It is required only for `gpperfmon` tests.
