/* src/test/modules/test_ao_scan/test_ao_scan--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ao_scan" to load this file. \quit

-- Read the table on each segment by blocks and through tuple slots, and
-- check that the two give the same rows. attnums names the attributes to
-- read, all of them when NULL, none when empty. Returns the rows compared
-- on each segment.
CREATE FUNCTION test_ao_scan_compare(rel regclass, attnums int[] DEFAULT NULL)
RETURNS SETOF bigint
AS 'MODULE_PATHNAME'
LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS;
