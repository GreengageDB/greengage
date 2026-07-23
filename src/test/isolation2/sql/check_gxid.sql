select gp_segment_id, gp_get_next_gxid() < (select gp_get_next_gxid()) from gp_dist_random('gp_id');
-- start_ignore
select gp_segment_id,gp_get_next_gxid() on_seg, (select gp_get_next_gxid() on_cor) from gp_dist_random('gp_id');
-- end_ignore

-- trigger master panic and wait until master down before running any new query.
1&: SELECT wait_till_master_shutsdown();
2: SELECT gp_inject_fault('before_read_command', 'panic', 1);
2: SELECT 1;
1<:

-- wait until master is up for querying.
3: SELECT 1;

-- the coordinator's FTS view can still be stale right after the crash-reset;
-- refresh it (and confirm segments are synchronized) before the first gang
-- dispatch below, so gp_dist_random does not race the post-recovery window.
3: SELECT wait_until_all_segments_synchronized();

3: select gp_segment_id, gp_get_next_gxid() < (select gp_get_next_gxid()) from gp_dist_random('gp_id');
-- start_ignore
3: select gp_segment_id,gp_get_next_gxid() on_seg, (select gp_get_next_gxid() on_cor) from gp_dist_random('gp_id');
-- end_ignore
