-- start_ignore
DROP TABLE IF EXISTS heap_table_with_toast, heap_table_without_toast,
                     ao_table_with_toast, ao_table_without_toast;
-- end_ignore

CREATE TABLE heap_table_with_toast(a int, b text)
DISTRIBUTED BY (a);

CREATE TABLE heap_table_without_toast(a int, b int)
DISTRIBUTED BY (a);

CREATE TABLE ao_table_with_toast(a int, b text)
WITH (APPENDOPTIMIZED=true)
DISTRIBUTED BY (a);

CREATE TABLE ao_table_without_toast(a int, b int)
WITH (APPENDOPTIMIZED=true)
DISTRIBUTED BY (a);

-- Check that toast exists only for "with_toast" tables
SELECT relname, reltoastrelid != 0 with_toast
FROM pg_class
WHERE relname IN ('heap_table_with_toast', 'heap_table_without_toast',
                  'ao_table_with_toast', 'ao_table_without_toast')
ORDER BY 1;

-- Check with empty tables
SELECT relname, gp_segment_id, size
FROM (VALUES ('ao_table_with_toast'), ('ao_table_without_toast'),
             ('heap_table_with_toast'), ('heap_table_without_toast')) AS tables(relname),
     gp_toolkit.gp_table_size_on_segments(tables.relname::regclass)
ORDER BY 1, 2;

SELECT table_name, content, file_size
FROM gp_toolkit.gp_db_files_current
WHERE table_name = 'heap_table_without_toast'
ORDER BY 1, 2;

-- Insert initial data to tables
INSERT INTO heap_table_with_toast SELECT i, 'short_text' FROM generate_series(1,15) AS i;
INSERT INTO heap_table_without_toast SELECT i, i*10 FROM generate_series(1,15) AS i;
INSERT INTO ao_table_with_toast SELECT i, 'short_text' FROM generate_series(1,15) AS i;
INSERT INTO ao_table_without_toast SELECT i, i*10 FROM generate_series(1,15) AS i;

-- Show number of rows in segments
SELECT 'ao_table_without_toast' AS relname, gp_segment_id, count(*) AS num_rows
FROM heap_table_without_toast GROUP BY gp_segment_id
UNION ALL
SELECT 'ao_table_with_toast', gp_segment_id, count(*) FROM heap_table_with_toast GROUP BY gp_segment_id
UNION ALL
SELECT 'heap_table_without_toast', gp_segment_id, count(*) FROM heap_table_with_toast GROUP BY gp_segment_id
UNION ALL
SELECT 'heap_table_with_toast', gp_segment_id, count(*) FROM heap_table_with_toast GROUP BY gp_segment_id
ORDER BY 1, 2;

-- Check with non-empty tables
SELECT relname, gp_segment_id, size
FROM (VALUES ('ao_table_with_toast'), ('ao_table_without_toast'),
             ('heap_table_with_toast'), ('heap_table_without_toast')) AS tables(relname),
     gp_toolkit.gp_table_size_on_segments(tables.relname::regclass)
ORDER BY 1, 2;

SELECT table_name, content, file_size
FROM gp_toolkit.gp_db_files_current
WHERE table_name = 'heap_table_without_toast'
ORDER BY 1, 2;

-- Add random large data to get non-zero toast table's size
UPDATE heap_table_with_toast SET b = (
    SELECT string_agg( chr(trunc(65+random()*26)::integer), '')
    FROM generate_series(1,50000))
WHERE a = 1;

UPDATE ao_table_with_toast SET b = (
    SELECT string_agg( chr(trunc(65+random()*26)::integer), '')
    FROM generate_series(1,50000))
WHERE a = 1;

-- Check with non-zero toast tables
SELECT relname, gp_segment_id, size
FROM (VALUES ('ao_table_with_toast'), ('heap_table_with_toast')) AS tables(relname),
     gp_toolkit.gp_table_size_on_segments(tables.relname::regclass)
ORDER BY 1, 2;

-- Cleanup
DROP TABLE heap_table_with_toast, heap_table_without_toast,
           ao_table_with_toast, ao_table_without_toast;
