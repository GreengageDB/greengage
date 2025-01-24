-- start_ignore
drop table if exists t_prepare_lockmode;
-- end_ignore

create table t_prepare_lockmode(c1 int, c2 int) distributed by (c1);
prepare myupdate as update t_prepare_lockmode set c2 = $1;

show gp_enable_global_deadlock_detector;
-- See github issue: https://github.com/GreengageDB/greengage/issues/9446
-- Previously, when executing prepare statement, the lock mode is
-- determined by the function CondUpgradeRelLock. However, it did not
-- consider the GUC gp_enable_global_deadlock_detector's value. When
-- gp_enable_global_deadlock_detector is set off, previously, we would
-- hold RowExclusiveLock for the cached plan and then in the function
-- InitPlan we try to hold ExclusiveLock. The lock mode upgrade will
-- lead to deadlock on QD.
-- Now things get correct. If gp_enable_global_deadlock_detector is set
-- off, then no RowExclusiveLock will be held on the table.
-- The following test query the lock mode of RowExclusiveLock, this should
-- give empty results.

begin;
execute myupdate(1);
select mode, granted
from pg_locks
where
  gp_segment_id = -1 and
  locktype = 'relation' and
  mode = 'RowExclusiveLock' and
  relation::regclass::text = 't_prepare_lockmode';
abort;
