
create extension if not exists test_metadata;

-- Test on all segments
SELECT gp_segment_id, test_send_metadata(150) 
    FROM gp_dist_random('gp_id');

-- Read metadata collected on coordinator
SELECT test_check_metadata();

-- Clean metadata on coordinator
SELECT test_clean_metadata();

-- Check that metadata has been cleaned on coordinator
SELECT test_check_metadata();

-- Test longer metadata
SELECT gp_segment_id, test_send_metadata(150000)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_metadata(1500000)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_metadata(15000000)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_metadata(150000000)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_empty_metadata()
    FROM gp_dist_random('gp_id');

-- Read metadata collected on coordinator
SELECT test_check_metadata();

SELECT test_clean_metadata();

SELECT test_check_metadata();
