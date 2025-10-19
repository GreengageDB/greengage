-- credcheck extension for PostgreSQL
-- Copyright (c) 2024-2025 HexaCluster Corp - All rights reserved.

-- Show rolename instead of role oid
CREATE OR REPLACE VIEW pg_banned_role AS
  SELECT roleid::regrole, failure_count, banned_date FROM pg_banned_role();

GRANT SELECT ON pg_banned_role TO PUBLIC;

