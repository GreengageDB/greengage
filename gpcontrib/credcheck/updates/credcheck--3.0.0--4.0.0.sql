-- credcheck extension for PostgreSQL
-- Copyright (c) 2024-2025 HexaCluster Corp - All rights reserved.

-- Add event trigger for valid until warning
DROP FUNCTION warning_valid_until();
CREATE OR REPLACE FUNCTION warning_valid_until()
  RETURNS event_trigger AS
$$
DECLARE
   warn_days integer;
BEGIN
        SELECT ((extract(epoch from valuntil) - extract(epoch from current_timestamp))/86400)::integer
                INTO warn_days
                FROM pg_catalog.pg_shadow WHERE usename = SESSION_USER ;

        IF ( warn_days <= current_setting('credcheck.password_valid_warning', true)::integer ) THEN
                RAISE WARNING 'your password will expire in % days, please renew your password!', warn_days;
        END IF;
END;
$$
LANGUAGE plpgsql
SECURITY DEFINER
;

-- trigger definition
CREATE EVENT TRIGGER valid_until_warning
  ON login
  EXECUTE FUNCTION warning_valid_until();
ALTER EVENT TRIGGER valid_until_warning ENABLE ALWAYS;

