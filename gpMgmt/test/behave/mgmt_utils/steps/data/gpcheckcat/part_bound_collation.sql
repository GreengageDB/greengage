-- Earlier versions stored range partition boundaries with the database default
-- collation. To get such boundaries, copy them from a table with the same
-- partitions and a default-collation key. The database collation must be
-- en_US.utf8.
SET allow_system_table_mods = on;
CREATE FUNCTION copy_bounds(target regclass, source regclass) RETURNS void
LANGUAGE sql AS $$
	UPDATE pg_partition_rule r
	SET parrangestart = s.parrangestart, parrangeend = s.parrangeend
	FROM pg_partition p, pg_partition ps, pg_partition_rule s
	WHERE r.paroid = p.oid AND p.parrelid = target
	  AND s.paroid = ps.oid AND ps.parrelid = source
	  AND ps.parlevel = p.parlevel AND s.parname = r.parname;
$$;

CREATE COLLATION coll_en (locale = 'en_US.utf8');

CREATE TABLE src (id int, k text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('c'), DEFAULT PARTITION d);

CREATE TABLE src_nodef (id int, k text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('c'));

CREATE TABLE src_rev (id int, k text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('A') END ('b'), PARTITION p2 START ('b') END ('C'));

CREATE TABLE src_ovl (id int, k text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('A') END ('b'), PARTITION p2 START ('Ba') END ('Bb'),
 PARTITION p3 START ('Bc') END ('C'));

CREATE TABLE src_sub (id int, k text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (id)
SUBPARTITION BY RANGE (k)
SUBPARTITION TEMPLATE
(SUBPARTITION s1 START ('a') END ('c'), DEFAULT SUBPARTITION d)
(PARTITION p1 START (1) END (10), PARTITION p2 START (10) END (20));

-- Reported: "C" sorts differently from en_US.utf8.
CREATE TABLE c_old (id int, k text COLLATE "C")
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('c'), DEFAULT PARTITION d);
SELECT copy_bounds('c_old', 'src');

-- Reported: the subpartitions have a "C" key.
CREATE TABLE c_old_sub (id int, k text COLLATE "C")
DISTRIBUTED BY (id)
PARTITION BY RANGE (id)
SUBPARTITION BY RANGE (k)
SUBPARTITION TEMPLATE
(SUBPARTITION s1 START ('a') END ('c'), DEFAULT SUBPARTITION d)
(PARTITION p1 START (1) END (10), PARTITION p2 START (10) END (20));
SELECT copy_bounds('c_old_sub', 'src_sub');

-- Not reported: the key's collation sorts like the database default.
CREATE TABLE en_old (id int, k text COLLATE coll_en)
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('c'), DEFAULT PARTITION d);
SELECT copy_bounds('en_old', 'src');

-- Reported, also without a default partition.
CREATE TABLE c_old_nodef (id int, k text COLLATE "C")
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('c'));
SELECT copy_bounds('c_old_nodef', 'src_nodef');

-- Reported by both checks: p2 is valid under en_US.utf8 but its START is
-- greater than its END under "C", so the table cannot be recreated as it is.
CREATE TABLE c_old_rev (id int, k text COLLATE "C")
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('b'), PARTITION p2 START ('b') END ('c'));
SELECT copy_bounds('c_old_rev', 'src_rev');

-- Reported by both checks: under "C", p1 covers the ranges of p2 and p3.
CREATE TABLE c_old_ovl (id int, k text COLLATE "C")
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('b'), PARTITION p2 START ('b') END ('c'),
 PARTITION p3 START ('c') END ('d'));
SELECT copy_bounds('c_old_ovl', 'src_ovl');

RESET allow_system_table_mods;

-- Not reported: boundaries created by this version.
CREATE TABLE c_new (id int, k text COLLATE "C")
DISTRIBUTED BY (id)
PARTITION BY RANGE (k)
(PARTITION p1 START ('a') END ('c'), DEFAULT PARTITION d);

-- Not reported: LIST partitioning does not depend on the order of values.
CREATE TABLE c_list (id int, k text COLLATE "C")
DISTRIBUTED BY (id)
PARTITION BY LIST (k)
(PARTITION p1 VALUES ('a', 'b'), DEFAULT PARTITION d);
