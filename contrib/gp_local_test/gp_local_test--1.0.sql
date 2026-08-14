-- gp_local_test--1.0.sql
\echo Use "CREATE EXTENSION gp_local_test" to load this file. \quit

-- Deliberately vanilla: no DISTRIBUTED BY, no GPDB-specific clauses.
-- id is PRIMARY KEY *and* code is independently UNIQUE — two
-- non-overlapping unique constraints, which fails under GPDB's
-- default hash-distributed policy but is perfectly ordinary Postgres.
CREATE TABLE @extschema@.state (
    id         serial PRIMARY KEY,
    code       text UNIQUE NOT NULL,
    value      integer NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO @extschema@.state (code, value) VALUES ('counter', 0);

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

-- C function, also no EXECUTE ON — this is the one your bgworker
-- will call internally via SPI.
CREATE FUNCTION @extschema@.current_counter(p_code text) RETURNS integer
AS 'MODULE_PATHNAME', 'gp_local_test_current_counter'
LANGUAGE C STRICT;

