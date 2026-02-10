-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION temp_tables_stat" to load this file. \quit

CREATE FUNCTION tts_get_seg_files(OUT user_id oid, OUT sess_id int4, OUT path text, OUT content int2, OUT size int8)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'tts_get_seg_files'
LANGUAGE C
EXECUTE ON ALL SEGMENTS;
