ALTER TABLE cron.job DROP CONSTRAINT IF EXISTS jobname_username_uniq;
ALTER TABLE cron.job ALTER COLUMN jobname TYPE text;
CREATE UNIQUE INDEX jobname_username_idx ON cron.job (jobname, username);
ALTER TABLE cron.job ADD CONSTRAINT jobname_username_uniq UNIQUE USING INDEX jobname_username_idx;

DROP FUNCTION cron.unschedule(name);
CREATE FUNCTION cron.unschedule(job_name text)
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cron_unschedule_named$$;
COMMENT ON FUNCTION cron.unschedule(text)
    IS 'unschedule a pg_cron job';
