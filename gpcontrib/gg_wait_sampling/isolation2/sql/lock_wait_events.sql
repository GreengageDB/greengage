CREATE EXTENSION gg_wait_sampling;
CREATE TABLE t_wait_small (id INT, val TEXT) DISTRIBUTED BY (id);
INSERT INTO t_wait_small VALUES (1,'a'),(2,'b'),(3,'c');

!\retcode gpconfig -c gg_wait_sampling.history_period -v 5;
!\retcode gpconfig -c gg_wait_sampling.profile_period -v 5;
!\retcode gpstop -u;

SELECT * FROM gg_wait_sampling_reset_profile;
-- Test event sampling on segments
0U:BEGIN; LOCK TABLE t_wait_small IN ACCESS EXCLUSIVE MODE;

1&: SELECT count(*) FROM t_wait_small;

SELECT segid, event_type, event
FROM gg_wait_sampling_profile
WHERE event_type = 'Lock';

0Uq:
1<:
1q:
!\retcode gpconfig -r gg_wait_sampling.history_period;
!\retcode gpconfig -r gg_wait_sampling.profile_period;
!\retcode gpstop -u;
DROP EXTENSION gg_wait_sampling;
