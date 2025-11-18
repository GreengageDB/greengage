-- Tests EXPLAIN format output

-- ignore the variable JIT gucs and "optimizer = 'off'" in Settings (unaligned mode + text format)
-- start_matchsubs
-- m/^Settings:.*/
-- s/,?\s*optimizer_jit\w*\s*=\s*[^,\n]+//g
-- m/^Settings:.*/
-- s/,?\s*jit\w*\s*=\s*[^,\n]+//g
-- m/^Settings:.*/
-- s/^Settings:[,\s]*/Settings: /
-- m/^Settings:.*/
-- s/,?\s*optimizer\w*\s*=\s*'off'//g
-- end_matchsubs

-- ignore variable JIT gucs which can be shown when SETTINGS=ON
-- start_matchignore
-- m/^\s+jit\w*:/
-- m/\s*optimizer:\s*"off"/
-- m/^\s+optimizer_jit\w*:/
-- end_matchignore

-- start_ignore
drop function if exists explain_filter(text);
drop function if exists explain_filter_to_json(text);
-- end_ignore

-- To produce stable regression test output, it's usually necessary to
-- ignore details such as exact costs or row counts.  These filter
-- functions replace changeable output details with fixed strings.
create function explain_filter(text) returns setof text
language plpgsql as
$$
declare
    ln text;
begin
    for ln in execute $1
    loop
        -- Replace any numeric word with just 'N'
        ln := regexp_replace(ln, '-?\m\d+\M', 'N', 'g');
        -- In sort output, the above won't match units-suffixed numbers
        ln := regexp_replace(ln, '\m\d+kB', 'NkB', 'g');
        ln := regexp_replace(ln, '\m\d+K', 'NK', 'g');
        -- Replace slice and segment numbers with 'N'
        ln := regexp_replace(ln, '\mslice\d+', 'sliceN', 'g');
        ln := regexp_replace(ln, '\mseg\d+', 'segN', 'g');
        -- Ignore text-mode buffers output because it varies depending
        -- on the system state
        CONTINUE WHEN (ln ~ ' +Buffers: .*');
        -- Ignore text-mode "Planning:" line because whether it's output
        -- varies depending on the system state
        CONTINUE WHEN (ln = 'Planning:');
        return next ln;
    end loop;
end;
$$;
-- To produce valid JSON output, replace numbers with "0" or "0.0" not "N"
create function explain_filter_to_json(text) returns jsonb
language plpgsql as
$$
declare
    data text := '';
    ln text;
begin
    for ln in execute $1
    loop
        -- Replace any numeric word with just '0'
        ln := regexp_replace(ln, '\m\d+\M', '0', 'g');
        data := data || ln;
    end loop;
    return data::jsonb;
end;
$$;

-- DEFAULT syntax
CREATE TABLE apples(id int PRIMARY KEY, type text);
INSERT INTO apples(id) SELECT generate_series(1, 100000);
CREATE TABLE box_locations(id int PRIMARY KEY, address text);
CREATE TABLE boxes(id int PRIMARY KEY, apple_id int REFERENCES apples(id), location_id int REFERENCES box_locations(id));

--- Check Explain Text format output
select explain_filter('EXPLAIN SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;');

--- Check Explain Analyze Text output that include the slices information
select explain_filter('EXPLAIN (ANALYZE) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;');

-- Unaligned output format is better for the YAML / XML / JSON outputs.
-- In aligned format, you have end-of-line markers at the end of each line,
-- and its position depends on the longest line. If the width changes, all
-- lines need to be adjusted for the moved end-of-line-marker.
\a

-- YAML Required replaces for costs and time changes

-- Check Explain YAML output
select explain_filter('EXPLAIN (FORMAT YAML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;');
SET random_page_cost = 1;
SET cpu_index_tuple_cost = 0.1;
select explain_filter('EXPLAIN (FORMAT YAML, VERBOSE) SELECT * from boxes;');

select explain_filter('EXPLAIN (FORMAT YAML, VERBOSE, SETTINGS ON) SELECT * from boxes;');

--- Check Explain Analyze YAML output that include the slices information

select explain_filter('EXPLAIN (ANALYZE, FORMAT YAML) SELECT * from boxes LEFT JOIN apples ON apples.id = boxes.apple_id LEFT JOIN box_locations ON box_locations.id = boxes.location_id;');

--- Check explain analyze sort information in verbose mode
select explain_filter('EXPLAIN (ANALYZE, VERBOSE) SELECT * from boxes ORDER BY apple_id;');
RESET random_page_cost;
RESET cpu_index_tuple_cost;

--
-- Test a simple case with JSON and XML output, too.
--
-- This should be enough for those format. The only difference between JSON,
-- XML, and YAML is in the formatting, after all.

-- Check JSON format
select explain_filter_to_json('EXPLAIN (FORMAT JSON, COSTS OFF) SELECT * FROM generate_series(1, 10);');

select explain_filter('EXPLAIN (FORMAT XML, COSTS OFF) SELECT * FROM generate_series(1, 10);');

-- Test for an old bug in printing Sequence nodes in JSON/XML format
-- (https://github.com/greenplum-db/gpdb/issues/9410)
CREATE TABLE jsonexplaintest (i int4) PARTITION BY RANGE (i) (START(1) END(3) EVERY(1));
select explain_filter_to_json('EXPLAIN (FORMAT JSON, COSTS OFF) SELECT * FROM jsonexplaintest WHERE i = 2;');

-- Check Explain Text format output with jit enable
CREATE TABLE jit_explain_output(c1 int);
INSERT INTO jit_explain_output SELECT generate_series(1,100);

SET jit = on;
SET jit_above_cost = 0;
SET gp_explain_jit = on;

-- ORCA GUCs to enable JIT
set optimizer_jit to on;
set optimizer_jit_above_cost to 1;

select explain_filter('EXPLAIN SELECT * FROM jit_explain_output LIMIT 10;');

-- Check explain anlyze text format output with jit enable
select explain_filter('EXPLAIN (ANALYZE) SELECT * FROM jit_explain_output LIMIT 10;');

-- Check explain analyze json format output with jit enable
select explain_filter_to_json('EXPLAIN (ANALYZE, FORMAT json) SELECT * FROM jit_explain_output LIMIT 10;');

RESET jit;
RESET jit_above_cost;
RESET gp_explain_jit;
RESET optimizer_jit;
RESET optimizer_jit_above_cost;
-- Cleanup
DROP TABLE boxes;
DROP TABLE apples;
DROP TABLE box_locations;
DROP TABLE jsonexplaintest;
DROP TABLE jit_explain_output;
DROP FUNCTION explain_filter;
DROP FUNCTION explain_filter_to_json;
