-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION credcheck UPDATE TO '6.0.0'" to load this file. \quit

----
-- lastlog: utmp/wtmp-like login history. The data is kept in shared memory
-- and a memory-mapped file managed by the C code; this only exposes it.
----
CREATE FUNCTION credcheck_lastlog (
	OUT type        text,
	OUT username    text,
	OUT pid         integer,
	OUT client_addr text,
	OUT client_port integer,
	OUT login_time  timestamp with time zone,
	OUT logout_time timestamp with time zone,
	OUT duration    interval,
	OUT state       text,
	OUT query       text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'credcheck_lastlog'
LANGUAGE C VOLATILE;

CREATE VIEW pg_lastlog AS
  SELECT * FROM credcheck_lastlog();

REVOKE ALL ON FUNCTION credcheck_lastlog() FROM PUBLIC;
REVOKE ALL ON pg_lastlog FROM PUBLIC;
GRANT SELECT ON pg_lastlog TO pg_read_all_stats;
