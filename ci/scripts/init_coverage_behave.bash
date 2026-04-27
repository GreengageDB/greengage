#!/bin/bash
# Setup coverage.

#TODO remove hard-coded links.
#This file is suppose to run after creation of hostfile_all file.
cat > /tmp/sitecustomize.py <<INNER_EOF
import coverage
coverage.process_startup()
INNER_EOF

# Now copy everything over to the hosts.
while read -r host; do
    scp /tmp/sitecustomize.py "$host":/usr/local/greengage-db-devel/lib/python
    ssh "$host" "echo 'export COVERAGE_PROCESS_START=/home/gpadmin/gpdb_src/gpMgmt/test/coveragerc_behave' >> /usr/local/greengage-db-devel/greengage_path.sh" </dev/null
done < /tmp/hostfile_all