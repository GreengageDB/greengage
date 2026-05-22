CREATE EXTENSION gg_wait_sampling;

WITH t as (SELECT sum(0) FROM gg_wait_sampling_current)
	SELECT sum(0) FROM generate_series(1, 2), t;

WITH t as (SELECT sum(0) FROM gg_wait_sampling_history)
	SELECT sum(0) FROM generate_series(1, 2), t;

WITH t as (SELECT sum(0) FROM gg_wait_sampling_profile)
	SELECT sum(0) FROM generate_series(1, 2), t;

-- Some dummy checks just to be sure that all our functions work and return something.
SELECT count(*) = 1 as test FROM gg_wait_sampling_get_current_coordinator();
SELECT count(*) = 1 as test FROM gg_wait_sampling_get_current_segments();
SELECT count(*) >= 0 as test FROM gg_wait_sampling_get_profile_coordinator();
SELECT count(*) >= 0 as test FROM gg_wait_sampling_get_profile_segments();
SELECT count(*) >= 0 as test FROM gg_wait_sampling_get_history_coordinator();
SELECT count(*) >= 0 as test FROM gg_wait_sampling_get_history_segments();
SELECT count(*) >= 0 as test FROM gg_wait_sampling_reset_profile_coordinator();
SELECT count(*) >= 0 as test FROM gg_wait_sampling_reset_profile_segments();

DROP EXTENSION gg_wait_sampling;
