-- Test setting numsegments value for table creation during rebalance operation
-- Test error throwing when catalog is not locked by gprebalance
select gp_toolkit.gp_set_rebalance_numsegments(2);
select gp_toolkit.gp_get_rebalance_numsegments();
select gp_toolkit.gp_reset_rebalance_numsegments();
-- Test the numsegment value setting mechanics
1: begin;
1: select gp_expand_lock_catalog();
2: create table t_reb (i int) distributed by (i);
1: select gp_toolkit.gp_get_rebalance_numsegments();
1: select gp_toolkit.gp_set_rebalance_numsegments(2);
1: select gp_toolkit.gp_get_rebalance_numsegments();
1: select gp_toolkit.gp_reset_rebalance_numsegments();
1: select gp_toolkit.gp_get_rebalance_numsegments();
1: select gp_toolkit.gp_set_rebalance_numsegments(2);
1: commit;
-- Test that newly created tables are using updated number of segmetns
1: create table t_reb (i int) distributed by (i);
1: select numsegments from gp_distribution_policy where localoid = 't_reb'::regclass;
1: drop table t_reb;

1: begin;
1: select gp_expand_lock_catalog();
1: select gp_toolkit.gp_reset_rebalance_numsegments();
1: create table t_reb (i int) distributed by (i);
1: select  numsegments from gp_distribution_policy where localoid = 't_reb'::regclass;
1: rollback;
1q:
2q:
