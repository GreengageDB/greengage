-- Restore the default resource manager (none) after the resqueue test block.
--
-- Use a fast-mode (not immediate) restart: an immediate shutdown leaves
-- crash-recovery debt, and losing the race against the recovering
-- coordinator wedges the cluster for every suite that runs after this one.
-- start_ignore
\! gpconfig -c gp_resource_manager -v none
-- end_ignore

\! echo $?

-- start_ignore
\! gpstop -ra -M fast
-- end_ignore

\! echo $?

-- Barrier: do not let the schedule continue until the restarted cluster
-- dispatches to all segments and the new setting is in effect.  (This must
-- stay a shell psql: this test's own connection died with the restart.)
\! for i in $(seq 1 120); do psql -X -Atq -d postgres -c "select count(*) from gp_dist_random('gp_id')" >/dev/null 2>&1 && { echo "cluster ready"; break; }; sleep 1; done
\! psql -X -Atq -d postgres -c "show gp_resource_manager"
