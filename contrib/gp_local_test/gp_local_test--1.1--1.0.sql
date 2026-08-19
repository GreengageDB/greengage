-- gp_local_test--1.1--1.0.sql
\echo Use "ALTER EXTENSION gp_local_test UPDATE TO '1.0'" to load this file. \quit

DROP MATERIALIZED VIEW @extschema@.counter_summary;

DROP TABLE @extschema@.state_snapshot;

DROP FUNCTION @extschema@.reset_counter(text);

-- Revert bump_counter to the pre-1.1 body (no history logging)
CREATE OR REPLACE FUNCTION @extschema@.bump_counter(p_code text) RETURNS integer
LANGUAGE sql
AS $$
    UPDATE @extschema@.state
       SET value = value + 1,
           updated_at = now()
     WHERE code = p_code
    RETURNING value;
$$;

DROP TABLE @extschema@.history;

ALTER TABLE @extschema@.state DROP COLUMN last_reset;

