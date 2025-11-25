create or replace function junkloop(rel text, numiter int) returns int as $$
declare
  sql text;
begin
  for i in 0..numiter loop
    sql := $sql$ insert into $sql$ || rel || $sql$ select 1, $sql$ || i::text || $sql$, repeat('x', 1000) $sql$;
    execute sql;
  end loop;
  return numiter;
end;
$$ language plpgsql;

drop table if exists vfao;
create table vfao (a, b, c) with (appendonly=true, orientation=column) as
select 1, i, repeat('x', 1000) from generate_series(1, 100)i distributed by (a);
create index ivfao on vfao(b, c);

-- insert many times to populate invisible tuples in pg_aoseg
select junkloop('vfao', 300);

select pg_relation_size((select segrelid from pg_appendonly where relid = 'vfao'::regclass)) from gp_dist_random('gp_id') where gp_segment_id = 1;

vacuum full vfao;
select pg_relation_size((select segrelid from pg_appendonly where relid = 'vfao'::regclass)) from gp_dist_random('gp_id') where gp_segment_id = 1;

-- Test that vacuum can process segment files created in an aborted transaction
create table table_ao_col (i int, j int, k int) with (appendonly='true', orientation="column");

begin;
insert into table_ao_col select i, i + 1, i + 2 from generate_series(1, 20) i;

\! psql regression -c 'begin; insert into table_ao_col select i, i + 1, i + 2 from generate_series(1, 20) i; rollback;'

commit;

create or replace function getTableSegFiles(t regclass, out gp_contentid smallint, out filepath text)
as 'select current_setting(''gp_contentid'')::smallint, pg_relation_filepath(t)'
language sql
execute on all segments;

select '\! (stat --format=''%s'' ' || string_agg(full_path_relfilenode, ' ') || ' 2>/dev/null || echo 0) | awk ''{sum += $1} END {print sum}''' check_segfiles_size from 
(select d.datadir || '/' || f.filepath || '.*' as full_path_relfilenode from getTableSegFiles('table_ao_col'::regclass) f join gp_segment_configuration d
on f.gp_contentid = d.content where d.content <> -1 and d.role = 'p')t
\gset

delete from table_ao_col where true;

vacuum table_ao_col;

:check_segfiles_size

drop function getTableSegFiles(t regclass, out gp_contentid smallint, out filepath text);
drop table table_ao_col;
