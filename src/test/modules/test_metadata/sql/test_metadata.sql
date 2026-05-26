create extension if not exists test_metadata;

begin;
-- Create two queues
SELECT test_create_metadata_queue() as queue1
\gset
SELECT test_create_metadata_queue() as queue2
\gset

-- Test on all segments
SELECT gp_segment_id, test_send_metadata(150, gp_segment_id, :queue1)
    FROM gp_dist_random('gp_id');

-- Read metadata collected on coordinator
SELECT test_check_metadata(:queue1);
SELECT test_check_metadata(:queue2);

-- Clean metadata on coordinator
SELECT test_clean_metadata(:queue1);

-- Check that metadata has been cleaned on coordinator
SELECT test_check_metadata(:queue1);

-- Test longer metadata
SELECT gp_segment_id, test_send_metadata(150000, gp_segment_id, :queue1)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_metadata(1500000, gp_segment_id, :queue2)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_metadata(15000000, gp_segment_id, :queue1)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_metadata(150000000, gp_segment_id, :queue2)
    FROM gp_dist_random('gp_id');

SELECT gp_segment_id, test_send_empty_metadata(:queue1)
    FROM gp_dist_random('gp_id');

-- Read metadata collected on coordinator
SELECT test_check_metadata(:queue1);
SELECT test_check_metadata(:queue2);

SELECT test_count_metadata(:queue1);
SELECT test_count_metadata(:queue2);

SELECT test_clean_metadata(:queue1);
SELECT test_clean_metadata(:queue2);

SELECT test_check_metadata(:queue1);
SELECT test_check_metadata(:queue2);

SELECT test_count_metadata(:queue1);
SELECT test_count_metadata(:queue2);

-- Delete two queues
SELECT test_delete_metadata_queue(:queue1);
SELECT test_delete_metadata_queue(:queue2);

commit;

-- Test error in transaction handling
begin;
SELECT test_create_metadata_queue() as queue1
\gset

SELECT gp_segment_id, test_send_metadata(42, gp_segment_id, :queue1)
    FROM gp_dist_random('gp_id');

SELECT 1/0; -- Error

rollback;

-- Test sending to non-existent queue
begin;

SELECT gp_segment_id, test_send_metadata(42, gp_segment_id, 10000)
    FROM gp_dist_random('gp_id');

rollback;
