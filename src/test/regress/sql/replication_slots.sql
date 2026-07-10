-- When I create a replication slot on the master
-- Then I should expect an error warning me that
--   my command is only run on the current segment.
select pg_create_physical_replication_slot('some_replication_slot');

-- And I should see that my replication slot exists.
-- GPDB: PG17 added the inactive_since column to pg_get_replication_slots(); it
-- holds a wall-clock timestamp whose textual width (fractional seconds) varies
-- between runs and perturbs psql's column alignment, so select the deterministic
-- columns explicitly instead of "*".
select slot_name, plugin, slot_type, datoid, temporary, active, active_pid,
       xmin, catalog_xmin, restart_lsn, confirmed_flush_lsn, wal_status,
       safe_wal_size, two_phase, conflicting, invalidation_reason, failover,
       synced
  from pg_get_replication_slots() where not active;

-- Cleanup:
select pg_drop_replication_slot('some_replication_slot');
