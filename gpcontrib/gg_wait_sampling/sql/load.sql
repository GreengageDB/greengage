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

-- A declaration whose row type differs from what the library fills is rejected.
CREATE FUNCTION bad_current(pid int4 DEFAULT NULL,
  OUT pid int4, OUT event_type text, OUT event text, OUT queryid int8,
  OUT mppsessionid timestamptz, OUT command_id int4, OUT segid int4)
RETURNS SETOF record AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_current'
LANGUAGE C VOLATILE CALLED ON NULL INPUT;
SELECT mppsessionid FROM bad_current();
DROP FUNCTION bad_current(int4);
CREATE FUNCTION bad_history(OUT pid int4, OUT ts timestamptz, OUT event_type text,
  OUT event text, OUT queryid int8, OUT mppsessionid int4, OUT command_id int4,
  OUT tmid int4, OUT segid int4, OUT extra int4)
RETURNS SETOF record AS '$libdir/gg_wait_sampling', 'pg_wait_sampling_get_history'
LANGUAGE C VOLATILE STRICT;
SELECT count(*) FROM bad_history();
DROP FUNCTION bad_history();
