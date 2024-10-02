/* gpcontrib/gp_toolkit/gp_toolkit--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_toolkit UPDATE TO '1.6" to load this file. \quit

--
-- Upgrade gp_toolkit.gp_workfile_entries to use pids directly from C function.
--

---------------------------------------------------------------------------------
-- @view:
--        gp_toolkit.gp_workfile_entries
--
-- @doc:
--        List of all the workfile sets currently present on disk
--
--------------------------------------------------------------------------------

CREATE OR REPLACE VIEW gp_toolkit.gp_workfile_entries AS
WITH all_entries AS (
    SELECT C.*
        FROM gp_toolkit.__gp_workfile_entries_f_on_coordinator() AS C (
           segid int,
           prefix text,
           size bigint,
           optype text,
           slice int,
           sessionid int,
           commandid int,
           numfiles int,
           pid int
        )
    UNION ALL
    SELECT C.*
        FROM gp_toolkit.__gp_workfile_entries_f_on_segments() AS C (
            segid int,
            prefix text,
            size bigint,
            optype text,
            slice int,
            sessionid int,
            commandid int,
            numfiles int,
            pid int
        ))
SELECT S.datname,
       S.pid,
       C.sessionid as sess_id,
       C.commandid as command_cnt,
       S.usename,
       S.query,
       C.segid,
       C.slice,
       C.optype,
       C.size,
       C.numfiles,
       C.prefix
FROM all_entries C LEFT OUTER JOIN gp_stat_activity S
ON C.sessionid = S.sess_id and C.pid = S.pid and C.segid=S.gp_segment_id;

GRANT SELECT ON gp_toolkit.gp_workfile_entries TO public;
