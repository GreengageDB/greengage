/* contrib/amcheck/amcheck--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION amcheck" to load this file. \quit

--
-- bt_index_check()
--
CREATE FUNCTION bt_index_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_check'
LANGUAGE C STRICT EXECUTE ON ANY;

CREATE FUNCTION bt_index_check_on_segments(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_check'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION bt_index_check_on_all(index regclass) RETURNS VOID AS
$$
BEGIN
    PERFORM bt_index_check(index);
    PERFORM bt_index_check_on_segments(index);
END
$$
LANGUAGE plpgsql STRICT;

--
-- bt_index_parent_check()
--
CREATE FUNCTION bt_index_parent_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_parent_check'
LANGUAGE C STRICT EXECUTE ON ANY;

CREATE FUNCTION bt_index_parent_check_on_segments(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bt_index_parent_check'
LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION bt_index_parent_check_on_all(index regclass) RETURNS VOID AS
$$
BEGIN
    PERFORM bt_index_parent_check(index);
    PERFORM bt_index_parent_check_on_segments(index);
END
$$
LANGUAGE plpgsql STRICT;

-- Don't want these to be available to public
REVOKE ALL ON FUNCTION bt_index_check(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_parent_check(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_check_on_segments(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_parent_check_on_segments(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_check_on_all(regclass) FROM PUBLIC;
REVOKE ALL ON FUNCTION bt_index_parent_check_on_all(regclass) FROM PUBLIC;
