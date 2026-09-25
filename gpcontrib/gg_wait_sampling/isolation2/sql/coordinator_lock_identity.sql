-- Session id and command id of a coordinator backend must be reported even
-- when the backend blocks before the planner and executor hooks have run,
-- for example on a relation lock taken during parse analysis.
CREATE EXTENSION gg_wait_sampling;

-- Processes without a session (background workers, auxiliary processes) report
-- mppsessionid 0, never InvalidGpSessionId.
SELECT count(*) = 0 AS no_negative_session_ids FROM gg_wait_sampling_current WHERE mppsessionid < 0;
SELECT count(*) > 0 AS has_sessionless_processes FROM gg_wait_sampling_current WHERE mppsessionid = 0;

CREATE TABLE t_wait_qd (id INT, val TEXT) DISTRIBUTED BY (id);
INSERT INTO t_wait_qd VALUES (1,'a'),(2,'b'),(3,'c');

-- Poll pg_stat_activity until the given query is in the given wait event on
-- the coordinator, so that the checks below don't depend on timing.
CREATE FUNCTION wait_for_wait_event(q text, ev text) RETURNS bool AS $$ DECLARE i int := 0; BEGIN LOOP PERFORM 1 FROM pg_stat_activity WHERE query = q AND wait_event = ev AND pid <> pg_backend_pid(); IF FOUND THEN RETURN true; END IF; i := i + 1; IF i > 600 THEN RETURN false; END IF; PERFORM pg_sleep(0.05); END LOOP; END; $$ LANGUAGE plpgsql;

SELECT * FROM gg_wait_sampling_reset_profile ORDER BY gp_segment_id;

-- Session 1 holds an exclusive lock on the coordinator and on the segments.
1: BEGIN;
1: LOCK TABLE t_wait_qd IN ACCESS EXCLUSIVE MODE;

-- Session 2 blocks on the coordinator while parse analysis opens the table.
2&: SELECT count(*) FROM t_wait_qd;
SELECT wait_for_wait_event('SELECT count(*) FROM t_wait_qd;', 'relation');

-- Current waits: the blocked backend must carry its session id and a
-- command id although no plan exists yet.
SELECT c.event_type, c.event, c.segid,
       c.mppsessionid = s.sess_id AS session_matches,
       c.command_id > 0 AS has_command_id,
       c.tmid = floor(extract(epoch FROM pg_postmaster_start_time()))::int4 AS tmid_matches
FROM gg_wait_sampling_get_current_coordinator() c
JOIN pg_stat_activity s ON s.pid = c.pid
WHERE s.query = 'SELECT count(*) FROM t_wait_qd;';

-- The QEs of session 1 on the segments report the coordinator's start time
-- as tmid, not their own postmaster's.
SELECT c.segid, bool_and(c.tmid = floor(extract(epoch FROM pg_postmaster_start_time()))::int4) AS tmid_matches
FROM gg_wait_sampling_get_current_segments() c
JOIN gp_stat_activity s ON s.pid = c.pid AND s.gp_segment_id = c.segid
WHERE s.sess_id = (SELECT sess_id FROM pg_stat_activity WHERE query = 'LOCK TABLE t_wait_qd IN ACCESS EXCLUSIVE MODE;')
GROUP BY c.segid ORDER BY c.segid;

-- Let the collector take a few samples, then check history and profile too.
SELECT pg_sleep(0.5);

SELECT DISTINCT h.event_type, h.event, h.segid,
       h.mppsessionid = s.sess_id AS session_matches,
       h.command_id > 0 AS has_command_id
FROM gg_wait_sampling_get_history_coordinator() h
JOIN pg_stat_activity s ON s.pid = h.pid
WHERE s.query = 'SELECT count(*) FROM t_wait_qd;' AND h.event_type = 'Lock';

SELECT DISTINCT p.event_type, p.event, p.segid,
       p.mppsessionid = s.sess_id AS session_matches,
       p.command_id > 0 AS has_command_id
FROM gg_wait_sampling_get_profile_coordinator() p
JOIN pg_stat_activity s ON s.pid = p.pid
WHERE s.query = 'SELECT count(*) FROM t_wait_qd;' AND p.event_type = 'Lock';

1: COMMIT;
2<:

-- Once its statement is done the backend waits for the client: it keeps its
-- session id but is attributed to no command.
SELECT wait_for_wait_event('SELECT count(*) FROM t_wait_qd;', 'ClientRead');
SELECT c.event, c.mppsessionid = s.sess_id AS session_matches, c.command_id, c.queryid
FROM gg_wait_sampling_get_current_coordinator() c
JOIN pg_stat_activity s ON s.pid = c.pid
WHERE s.query = 'SELECT count(*) FROM t_wait_qd;' AND s.wait_event = 'ClientRead';

1q:
2q:

DROP FUNCTION wait_for_wait_event(text, text);
DROP TABLE t_wait_qd;
DROP EXTENSION gg_wait_sampling;
