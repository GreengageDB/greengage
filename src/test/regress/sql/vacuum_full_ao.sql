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

create or replace function getTableSegFiles
(t regclass) returns
table (gp_contentid smallint, filepath text) as
$function$
select current_setting('gp_contentid')::smallint, pg_relation_filepath(t)
$function$
language sql
execute on all segments;

create or replace function cmdCheckSegmentFileSizes(table_name text) returns text as
$$
declare
  cmd text;
begin
  select '\! (stat --format=''%s'' ' || string_agg(full_path_relfilenode, ' ') || ' 2>/dev/null || echo 0) | awk ''{sum += $1} END {print sum}'''
  into cmd
  from
  (select d.datadir || '/' || f.filepath || '.*' as full_path_relfilenode from getTableSegFiles(table_name::regclass) f join gp_segment_configuration d
    on f.gp_contentid = d.content where d.content <> -1 and d.role = 'p')t;

  return cmd;
end
$$ language plpgsql;

select cmdCheckSegmentFileSizes('table_ao_col') check_segfiles_size
\gset

delete from table_ao_col where true;

vacuum table_ao_col;

:check_segfiles_size

drop table table_ao_col;

-- Test that vacuum can process segment files created for a new column in an aborted transaction (case 1)
create table table_ao_col_1 (i int, j int, k int) with (appendonly='true', orientation="column");
insert into table_ao_col_1 select i, i + 1, i + 2 from generate_series(1, 20) i;
alter table table_ao_col_1 alter column j type bigint;
select attnum, filenum from gp_dist_random('pg_attribute_encoding') where gp_segment_id = 0 and attrelid = 'table_ao_col_1'::regclass;

begin;
alter table table_ao_col_1 add column a int;
alter table table_ao_col_1 add column b int;
update table_ao_col_1 set a = 1, b = 2 where true;
alter table table_ao_col_1 alter column a type bigint;
select attnum, filenum from gp_dist_random('pg_attribute_encoding') where gp_segment_id = 0 and attrelid = 'table_ao_col_1'::regclass;
rollback;

select cmdCheckSegmentFileSizes('table_ao_col_1') check_segfiles_size_1
\gset

delete from table_ao_col_1 where true;

vacuum table_ao_col_1;

:check_segfiles_size_1

drop table table_ao_col_1;

-- Test that vacuum can process segment files created for a new column in an aborted transaction (case 2)
create table table_ao_col_2 (i int, j int, k int) with (appendonly='true', orientation="column");

begin;
insert into table_ao_col_2 select i, i + 1, i + 2 from generate_series(1, 20) i;
alter table table_ao_col_2 alter column j type bigint;
select attnum, filenum from gp_dist_random('pg_attribute_encoding') where gp_segment_id = 0 and attrelid = 'table_ao_col_2'::regclass;
alter table table_ao_col_2 add column a int;
update table_ao_col_2 set a = 1 where true;
rollback;

select cmdCheckSegmentFileSizes('table_ao_col_2') check_segfiles_size_2
\gset

vacuum table_ao_col_2;

:check_segfiles_size_2

drop table table_ao_col_2;

-- Test vacuum for a AORO table after adding column in an aborted transaction
create table table_ao_row (i int, j int, k int) with (appendonly='true', orientation="row");
insert into table_ao_row select i, i + 1, i + 2 from generate_series(1, 20) i;

begin;
alter table table_ao_row add column a int;
update table_ao_row set a = 1 where true;
rollback;

select cmdCheckSegmentFileSizes('table_ao_row') check_segfiles_size_aoro
\gset

delete from table_ao_row where true;

vacuum table_ao_row;

:check_segfiles_size_aoro

drop table table_ao_row;

drop function cmdCheckSegmentFileSizes(table_name text);
drop function getTableSegFiles(t regclass, out gp_contentid smallint, out filepath text);
