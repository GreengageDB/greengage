-- Install the previous version first: the library must keep serving the 1.1
-- declarations (tmid is reported as NULL) until the extension is updated.
CREATE EXTENSION gg_wait_sampling VERSION '1.1';
SELECT count(*) > 0 AS has_rows, bool_and(tmid IS NULL) AS tmid_is_null
FROM gg_wait_sampling_current;
SELECT coalesce(bool_and(tmid IS NULL), true) AS tmid_is_null
FROM gg_wait_sampling_history;
SELECT coalesce(bool_and(tmid IS NULL), true) AS tmid_is_null
FROM gg_wait_sampling_profile;

-- Upgrade to the current version and check the new row types.
ALTER EXTENSION gg_wait_sampling UPDATE;
SELECT extversion FROM pg_extension WHERE extname = 'gg_wait_sampling';
SELECT count(*) > 0 AS has_rows FROM gg_wait_sampling_current;
SELECT count(*) >= 0 AS ok FROM gg_wait_sampling_history;
SELECT count(*) >= 0 AS ok FROM gg_wait_sampling_profile;

\d gg_wait_sampling_current
\d gg_wait_sampling_history
\d gg_wait_sampling_profile
\d gg_wait_sampling_reset_profile

DROP EXTENSION gg_wait_sampling;
