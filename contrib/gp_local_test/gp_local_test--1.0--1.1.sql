-- gp_local_test--1.0--1.1.sql
\echo Use "ALTER EXTENSION gp_local_test UPDATE TO '1.1'" to load this file. \quit

ALTER TABLE @extschema@.state ADD COLUMN last_reset timestamptz;

CREATE TABLE @extschema@.history (
    id         serial PRIMARY KEY,
    code       text NOT NULL,
    old_value  integer NOT NULL,
    new_value  integer NOT NULL,
    changed_at timestamptz UNIQUE NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION @extschema@.bump_counter(p_code text) RETURNS integer
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

CREATE FUNCTION @extschema@.reset_counter(p_code text) RETURNS void
LANGUAGE sql
AS $$
    UPDATE @extschema@.state
       SET value = 0,
           last_reset = now()
     WHERE code = p_code;
$$;
