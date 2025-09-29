DROP TABLE IF EXISTS reindex_ao;

CREATE TABLE reindex_ao (a INT) WITH (appendonly=true);
insert into reindex_ao select generate_series(1,1000);
insert into reindex_ao select generate_series(1,1000);
create index idx_bitmap_reindex_ao on reindex_ao USING bitmap(a);
-- @Description Ensures that a vacuum during reindex operations is ok
-- 

DELETE FROM reindex_ao WHERE a < 128;
1: BEGIN;
-- Remember index relfilenodes from coordinator and segments before
-- reindex.
1: create temp table old_relfilenodes as
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname = 'idx_bitmap_reindex_ao'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname = 'idx_bitmap_reindex_ao');
1: REINDEX index idx_bitmap_reindex_ao;
2&: VACUUM reindex_ao;
1: COMMIT;
2<:
-- Validate that reindex changed all index relfilenodes on coordinator as well as
-- segments.  The following query should return 0 tuples.
1: select oldrels.* from old_relfilenodes oldrels join
   (select gp_segment_id as dbid, relfilenode, relname from gp_dist_random('pg_class')
    where relname = 'idx_bitmap_reindex_ao'
    union all
    select gp_segment_id as dbid, relfilenode, relname from pg_class
    where relname = 'idx_bitmap_reindex_ao') newrels
    on oldrels.relfilenode = newrels.relfilenode
    and oldrels.dbid = newrels.dbid
    and oldrels.relname = newrels.relname;
2: COMMIT;
3: SELECT COUNT(*) FROM reindex_ao WHERE a = 1500;
3: INSERT INTO reindex_ao VALUES (0);

-- Check the consistency of the bitmap index on the primary and mirror after vacuum.
-- Vacuum with previously opened transaction does not lead to reindex, but changes pages of existing index.
CREATE OR REPLACE FUNCTION seg_datadir(seg int, rol text) RETURNS table (dir text) AS $$ /*in func*/
BEGIN /*in func*/
RETURN QUERY /*in func*/
	select datadir from gp_segment_configuration where content = seg and role = rol; /*in func*/
END $$ /*in func*/
LANGUAGE plpgsql VOLATILE EXECUTE ON ANY;

CREATE OR REPLACE FUNCTION rel_seg_path(relname text, seg int) RETURNS table (path text) AS $$ /*in func*/
BEGIN /*in func*/
	RETURN QUERY /*in func*/
	select pg_relation_filepath(relname) from gp_dist_random('gp_id') where gp_segment_id = seg; /*in func*/
END $$ /*in func*/
LANGUAGE plpgsql VOLATILE EXECUTE ON ALL SEGMENTS;

-- start_ignore
drop table if exists ao_table;
-- end_ignore
create table ao_table (a int) with (appendonly=true);
insert into ao_table select generate_series(1,10);
insert into ao_table select generate_series(1,10);
create index bitmap_idx_ao on ao_table using bitmap(a);
delete from ao_table where a < 8;

1: begin;
2: vacuum ao_table;
1: commit;
1q:
2q:
!\retcode gpstop -afr;
-- Check the identity of index files on primary and mirror.
! seg0_datadir=$(psql -At -c "select * from seg_datadir(0, 'p')" isolation2test) && mir0_datadir=$(psql -At -c "select dir from seg_datadir(0, 'm')" isolation2test) && relfilenode_dir=$(psql -At -c "select path from rel_seg_path('bitmap_idx_ao', 0)" isolation2test) && diff ${seg0_datadir}/${relfilenode_dir} ${mir0_datadir}/${relfilenode_dir};
! seg1_datadir=$(psql -At -c "select * from seg_datadir(1, 'p')" isolation2test) && mir1_datadir=$(psql -At -c "select dir from seg_datadir(1, 'm')" isolation2test) && relfilenode_dir=$(psql -At -c "select path from rel_seg_path('bitmap_idx_ao', 1)" isolation2test) && diff ${seg1_datadir}/${relfilenode_dir} ${mir1_datadir}/${relfilenode_dir};
! seg2_datadir=$(psql -At -c "select * from seg_datadir(2, 'p')" isolation2test) && mir2_datadir=$(psql -At -c "select dir from seg_datadir(2, 'm')" isolation2test) && relfilenode_dir=$(psql -At -c "select path from rel_seg_path('bitmap_idx_ao', 2)" isolation2test) && diff ${seg2_datadir}/${relfilenode_dir} ${mir2_datadir}/${relfilenode_dir};
