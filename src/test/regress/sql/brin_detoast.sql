-- Ensures that BRIN summaries store detoasted copies of the indexed values
-- instead of raw TOAST pointers (upstream commit 7577dd84807a8 "Properly
-- detoast data in brin_form_tuple"). If a TOAST pointer leaks into the
-- summary, the referenced toast rows can be removed by VACUUM and any later
-- use of the summary fails with "missing chunk number ... for toast value".
--
-- Upstream exercises this at the tail of brin.sql, but that block is a no-op
-- in GGDB for two reasons: it sizes its values (~2000 chars) for the
-- upstream 8K page, while GGDB builds with BLCKSZ = 32K where the TOAST
-- threshold is ~8KB, so those values always stay inline; and it relies on
-- CREATE INDEX CONCURRENTLY as a wait barrier before VACUUM, which GGDB
-- does not support.
--
-- NB: this test must stay in its own parallel group in the schedule.
-- Without the CIC wait barrier the VACUUM below can only remove the TOAST
-- rows if no concurrent transaction holds back the xmin horizon.

CREATE TABLE brin_detoast_tbl (a text, b text, c text, d text) DISTRIBUTED BY (a);

-- helper: TOAST chunk count of brin_detoast_tbl across all segments
CREATE FUNCTION brin_detoast_chunk_count() RETURNS bigint AS $$
DECLARE
	n bigint;
BEGIN
	EXECUTE format('SELECT count(*) FROM gp_dist_random(''pg_toast.%I'')',
				   (SELECT t.relname
					  FROM pg_class c JOIN pg_class t ON t.oid = c.reltoastrelid
					 WHERE c.relname = 'brin_detoast_tbl'))
	INTO n;
	RETURN n;
END
$$ LANGUAGE plpgsql VOLATILE;

-- A long random string for the indexed column so that it is stored out of
-- line: 400 md5s = 12800 chars, above the ~8KB threshold of 32K pages (and
-- pglz cannot compress random hex), while the detoasted min+max pair in the
-- BRIN summary still fits a 32K index page. Upstream indexes two such
-- columns, but two detoasted pairs would not fit.
WITH rand_value AS (SELECT string_agg(md5(i::text),'') AS val FROM generate_series(1,400) s(i))
INSERT INTO brin_detoast_tbl SELECT md5('a'), val, md5('c'), md5('d') FROM rand_value;
CREATE INDEX brin_detoast_idx ON brin_detoast_tbl USING brin (b);

-- sanity: the value must really be stored out of line, otherwise this test
-- exercises nothing
SELECT brin_detoast_chunk_count() > 0 AS has_toast_chunks;

DELETE FROM brin_detoast_tbl;
VACUUM brin_detoast_tbl;

-- The vacuum precondition upstream establishes with the CIC wait: the TOAST
-- rows of the deleted tuple must be gone on every segment. If this ever
-- reports a non-zero count, the environment failed the precondition and the
-- checks below prove nothing - fail here, visibly.
SELECT brin_detoast_chunk_count() AS toast_chunks_after_vacuum;

-- Insert a different value: brin_add_value unions it into the stored
-- summary, which requires reading (and detoasting) the previous min/max.
-- With the bug the summary holds pointers to the vacuumed toast rows and
-- this fails with "missing chunk number ...".
WITH rand_value AS (SELECT string_agg(md5((-i)::text),'') AS val FROM generate_series(1,400) s(i))
INSERT INTO brin_detoast_tbl SELECT md5('a'), val, md5('c'), md5('d') FROM rand_value;

-- And scan through the index: the consistent function compares against the
-- stored summary, detoasting it as well. Pin the planner bitmap scan path
-- so both CI job flavors exercise the index (ORCA ignores enable_seqscan).
SET optimizer = off;
SET enable_seqscan = off;
SELECT count(*) FROM brin_detoast_tbl WHERE b < '0';
SELECT count(*) FROM brin_detoast_tbl WHERE b > '0';
RESET enable_seqscan;
RESET optimizer;

DROP TABLE brin_detoast_tbl;
DROP FUNCTION brin_detoast_chunk_count();
