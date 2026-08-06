-- Restore the default resource manager (none) after the resqueue schedule.
-- Fast-mode restart (not immediate): an immediate shutdown leaves
-- crash-recovery debt and can wedge everything that runs afterwards.
!\retcode gpconfig -c gp_resource_manager -v none;
!\retcode gpstop -ra -M fast;
-- Barrier: wait until the restarted cluster dispatches to all segments.
!\retcode bash -c 'for i in $(seq 1 120); do psql -X -Atq -d postgres -c "select count(*) from gp_dist_random(\$\$gp_id\$\$)" >/dev/null 2>&1 && exit 0; sleep 1; done; exit 1';
1: show gp_resource_manager;
