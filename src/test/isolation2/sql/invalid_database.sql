-- Test that interruption of DROP DATABASE is handled properly. To ensure the
-- interruption happens at the appropriate moment, we lock pg_tablespace. DROP
-- DATABASE scans pg_tablespace once it has reached the "irreversible" part of
-- dropping the database, making it a suitable point to wait.

-- This test should only be run with the autovacuum off.
SHOW autovacuum;

-- start_ignore
DROP DATABASE IF EXISTS regression_invalid_interrupt;
-- end_ignore
-- Create the database
CREATE DATABASE regression_invalid_interrupt;

-- Prevent drop database via lock on pg_tablespace on segment 0
0U: BEGIN;
0U: LOCK pg_tablespace;

-- Try to drop, this will wait due to the still held lock on segment 0
1&: DROP DATABASE regression_invalid_interrupt;

-- Ensure the DROP DATABASE is waiting for the lock
SELECT EXISTS (SELECT FROM pg_locks WHERE NOT granted AND
    relation = 'pg_tablespace'::regclass AND mode = 'AccessShareLock');

-- and finally interrupt the DROP DATABASE on segment 0
0U: SELECT pg_cancel_backend(pid) FROM pg_locks WHERE NOT granted AND
    relation = 'pg_tablespace'::regclass AND mode = 'AccessShareLock';

-- Ensure cancellation be processed
1<:

-- Verify that connections to the database aren't allowed. The backend checks
-- this before relcache init, so the lock won't interfere.
! psql -d regression_invalid_interrupt -c "SELECT 1";

-- To properly drop the database, we need to release the lock previously
-- preventing doing so.
0U: END;

DROP DATABASE regression_invalid_interrupt;
