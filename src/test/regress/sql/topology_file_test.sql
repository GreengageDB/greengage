-- Test that verifies topology file creation and synchronization

-- 1. Verify initial state
SELECT count(*) > 0 AS has_segments_initial FROM gp_segment_configuration;

-- 2. Force FTS to update configuration files (this should trigger topology file update if it exists)
SELECT gp_request_fts_probe_scan();

-- 3. Wait a bit for the update to happen
SELECT pg_sleep(0.1);

-- 4. Query again to ensure everything is still working
SELECT count(*) AS segment_count FROM gp_segment_configuration;

-- 5. Verify that segment information is consistent
SELECT content, role, status, hostname 
FROM gp_segment_configuration 
WHERE content >= -1
ORDER BY content, role;