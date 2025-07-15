-- setup
1: drop table if exists alter_block;
1: create table alter_block(a int, b int) distributed by (a);
1: insert into alter_block select 1, 1;
-- Validate UPDATE blocks the Alter
2: BEGIN;
2: UPDATE alter_block SET b = b + 1;
1&: ALTER TABLE alter_block SET DISTRIBUTED BY (b);
-- Alter process should be blocked
2: SELECT wait_event_type FROM pg_stat_activity where query like 'ALTER TABLE alter_block %';
2: COMMIT;
1<:
-- Now validate ALTER blocks the UPDATE
2: BEGIN;
2: ALTER TABLE alter_block SET DISTRIBUTED BY (a);
1&: UPDATE alter_block SET b = b + 1;
2: SELECT wait_event_type FROM pg_stat_activity where query like 'UPDATE alter_block SET %';
2: COMMIT;
1<:

-- Check ALTER REBALANCE
1: DROP TABLE alter_block;
1: CREATE TABLE alter_block(a int, b int) DISTRIBUTED BY (a);
1: INSERT INTO alter_block SELECT i, i FROM generate_series(1, 20)i;

2: SET gp_target_numsegments = 2;

-- Validate SELECT blocks the ALTER
1: BEGIN;
2: BEGIN;
1: SELECT count(1), gp_segment_id FROM alter_block GROUP BY gp_segment_id ORDER BY gp_segment_id;
2&: ALTER TABLE alter_block REBALANCE;
1: COMMIT;
2<:
2: ROLLBACK;

-- Validate UPDATE blocks the ALTER
1: BEGIN;
2: BEGIN;
1: UPDATE alter_block SET b = b + 1 WHERE a = 1;
2&: ALTER TABLE alter_block REBALANCE;
1: COMMIT;
2<:
2: ROLLBACK;

-- Validate ALTER blocks the SELECT
1: BEGIN;
2: BEGIN;
2: ALTER TABLE alter_block REBALANCE;
1&: SELECT count(1), gp_segment_id FROM alter_block GROUP BY gp_segment_id ORDER BY gp_segment_id;
2: COMMIT;
1<:
1: COMMIT;

2: RESET gp_target_numsegments;
1: DROP TABLE alter_block;
