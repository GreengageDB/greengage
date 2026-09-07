-- gp_local_test--1.0.sql
\echo Use "CREATE EXTENSION gp_local_test" to load this file. \quit

-- Deliberately vanilla: no DISTRIBUTED BY, no Greengage-specific clauses.
-- id is PRIMARY KEY *and* code is independently UNIQUE — two
-- non-overlapping unique constraints, which fails under Greengage's
-- default hash-distributed policy but is perfectly ordinary Postgres.
CREATE TABLE @extschema@.state (
    id         serial PRIMARY KEY,
    code       text UNIQUE NOT NULL,
    value      integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
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

SELECT pg_catalog.pg_extension_config_dump('@extschema@.settings', '');

-- Plain SQL-language function, no EXECUTE ON clause at all —
-- exercises the "function defaults" side of the mechanism.
CREATE FUNCTION @extschema@.bump_counter(p_code text) RETURNS integer
LANGUAGE sql
AS $$
    UPDATE @extschema@.state
       SET value = value + 1,
           updated_at = now()
     WHERE code = p_code
    RETURNING value;
$$;

-- Plain SQL-language read, also no EXECUTE ON clause.
CREATE FUNCTION @extschema@.current_counter(p_code text) RETURNS integer
LANGUAGE sql STRICT
AS $$
    SELECT value FROM @extschema@.state WHERE code = p_code;
$$;
