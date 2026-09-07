-- gp_local_test--1.1.sql
\echo Use "CREATE EXTENSION gp_local_test" to load this file. \quit

-- Deliberately vanilla: no DISTRIBUTED BY, no Greengage-specific clauses.
-- id is PRIMARY KEY *and* code is independently UNIQUE — two
-- non-overlapping unique constraints, which fails under Greengage's
-- default hash-distributed policy but is perfectly ordinary Postgres.
CREATE TABLE @extschema@.state (
    id         serial PRIMARY KEY,
    code       text UNIQUE NOT NULL,
    value      integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_reset timestamptz
);

INSERT INTO @extschema@.state (code, value) VALUES ('counter', 0);

-- Genuine "config" table: DBA-tunable settings that must survive
-- backup/restore even though the rest of the extension's schema is
-- recreated fresh by re-running this script. Marked via
-- pg_extension_config_dump() per the standard PostgreSQL convention
-- so pg_dump/gpbackup include its row data instead of treating it as
-- pure extension-owned reference data.
CREATE TABLE @extschema@.settings (
    key   text PRIMARY KEY,
    value text NOT NULL
);

INSERT INTO @extschema@.settings (key, value) VALUES ('bump_interval_seconds', '5');

SELECT pg_catalog.pg_extension_config_dump('@extschema@.settings', '');

CREATE TABLE @extschema@.history (
    id         serial PRIMARY KEY,
    code       text NOT NULL,
    old_value  integer NOT NULL,
    new_value  integer NOT NULL,
    changed_at timestamptz UNIQUE NOT NULL DEFAULT now()
);

-- plpgsql, no EXECUTE ON clause — logs each bump into history.
CREATE FUNCTION @extschema@.bump_counter(p_code text) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_old integer;
    v_new integer;
BEGIN
    UPDATE @extschema@.state
       SET value = value + 1,
           updated_at = now()
     WHERE code = p_code
    RETURNING value - 1, value INTO v_old, v_new;

    INSERT INTO @extschema@.history (code, old_value, new_value)
    VALUES (p_code, v_old, v_new);

    RETURN v_new;
END;
$$;

-- Plain SQL-language function, no EXECUTE ON clause at all —
-- exercises the "function defaults" side of the mechanism.
CREATE FUNCTION @extschema@.reset_counter(p_code text) RETURNS void
LANGUAGE sql
AS $$
    UPDATE @extschema@.state
       SET value = 0,
           last_reset = now()
     WHERE code = p_code;
$$;

-- Plain SQL-language read, also no EXECUTE ON clause.
CREATE FUNCTION @extschema@.current_counter(p_code text) RETURNS integer
LANGUAGE sql STRICT
AS $$
    SELECT value FROM @extschema@.state WHERE code = p_code;
$$;
