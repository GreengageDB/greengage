-- Test external topology file functionality

-- Check if the topology file is created when it doesn't exist
-- This test assumes that the system automatically creates the file on startup

-- Check if gp_segment_configuration table exists and has data
SELECT count(*) > 0 AS has_segments FROM gp_segment_configuration;

-- After the system has had a chance to create the topology file,
-- verify that the file exists in the data directory
-- This would normally be checked externally, but we can check
-- that the system functions properly with the configuration

-- Verify that we can query segment configuration data
SELECT dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir
FROM gp_segment_configuration
ORDER BY content, dbid;

-- Test that the system can handle segment status changes
-- This would trigger updates to both FTS files and topology file if it exists
SELECT gp_request_fts_probe_scan();