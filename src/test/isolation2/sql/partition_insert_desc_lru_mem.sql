-- gp_max_partition_open_insert_descs actually bounds backend memory.
--
-- Routing rows through a wide AOCS partition root opens one write stack per
-- column for every leaf touched and, unbounded, keeps them all until end of
-- statement -- tens of MB per leaf. With the GUC > 0 only that many descriptors
-- are held at once.
--
-- We suspend an INSERT at its 28th AOCS descriptor open and, from another
-- session, read this session's reserved vmem on the segments. Bounded (5) must
-- be well under half of unbounded (~28 leaves' worth of buffers).

CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
CREATE EXTENSION IF NOT EXISTS gp_internal_tools;

CREATE TABLE pdlru_wide (id int, part int,
  c1 int,c2 int,c3 int,c4 int,c5 int,c6 int,c7 int,c8 int,c9 int,c10 int,
  c11 int,c12 int,c13 int,c14 int,c15 int,c16 int,c17 int,c18 int,c19 int,c20 int,
  c21 int,c22 int,c23 int,c24 int,c25 int,c26 int,c27 int,c28 int,c29 int,c30 int)
  WITH (appendonly=true, orientation=column)
  DISTRIBUTED BY (id)
  PARTITION BY RANGE (part) (START (0) END (30) EVERY (1));

-- staging: every segment routes rows to every one of the 30 leaves
CREATE TABLE pdlru_stg (LIKE pdlru_wide) DISTRIBUTED BY (id);
INSERT INTO pdlru_stg SELECT g, g % 30,
  g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g,g
  FROM generate_series(1, 6000) g;

CREATE TABLE pdlru_vmem (tag text, vmem_mb int);

-- clear any of these faults a previously aborted test may have left set
2: SELECT gp_inject_fault('ao_column_insert_init_1','reset',dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
2: SELECT gp_inject_fault('ao_column_insert_init_2','reset',dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;

-- ===================== unbounded =====================
2: SELECT gp_inject_fault('ao_column_insert_init_1','skip','','','',1,-1,0,dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
2: SELECT gp_inject_fault('ao_column_insert_init_2','suspend','','','',28,28,0,dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
1: SET gp_max_partition_open_insert_descs = 0;
1&: INSERT INTO pdlru_wide SELECT * FROM pdlru_stg;
2: SELECT gp_wait_until_triggered_fault('ao_column_insert_init_2', 1, dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
2: INSERT INTO pdlru_vmem SELECT 'unbounded', max(vmem_mb) FROM session_state.session_level_memory_consumption WHERE query LIKE 'INSERT INTO pdlru_wide SELECT%' AND segid >= 0;
2: SELECT gp_inject_fault('ao_column_insert_init_2','reset',dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
2: SELECT gp_inject_fault('ao_column_insert_init_1','reset',dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
1<:

-- ===================== bounded (5) =====================
1: TRUNCATE pdlru_wide;
2: SELECT gp_inject_fault('ao_column_insert_init_1','skip','','','',1,-1,0,dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
2: SELECT gp_inject_fault('ao_column_insert_init_2','suspend','','','',28,28,0,dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
1: SET gp_max_partition_open_insert_descs = 5;
1&: INSERT INTO pdlru_wide SELECT * FROM pdlru_stg;
2: SELECT gp_wait_until_triggered_fault('ao_column_insert_init_2', 1, dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
2: INSERT INTO pdlru_vmem SELECT 'bounded', max(vmem_mb) FROM session_state.session_level_memory_consumption WHERE query LIKE 'INSERT INTO pdlru_wide SELECT%' AND segid >= 0;
2: SELECT gp_inject_fault('ao_column_insert_init_2','reset',dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
2: SELECT gp_inject_fault('ao_column_insert_init_1','reset',dbid::int) FROM gp_segment_configuration WHERE role='p' AND content>=0;
1<:

-- both loads finished with every row
1: SELECT count(*) FROM pdlru_wide;

-- The point: bounded's peak reserved vmem is a small fraction of unbounded's
-- (in practice ~3.5x smaller here; without the limit it grows with every leaf
-- touched and a wide, many-thousand-partition load exhausts segment memory).
2: SELECT (SELECT vmem_mb FROM pdlru_vmem WHERE tag='bounded') * 2
       < (SELECT vmem_mb FROM pdlru_vmem WHERE tag='unbounded') AS bounded_much_lower;

1q:
2q:
DROP TABLE pdlru_wide;
DROP TABLE pdlru_stg;
DROP TABLE pdlru_vmem;
