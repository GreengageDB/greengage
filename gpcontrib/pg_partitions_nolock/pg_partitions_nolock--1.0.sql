\echo Use "CREATE EXTENSION pg_partitions_nolock" to load this file. \quit


CREATE FUNCTION pg_get_expr_nolock(TEXT)
RETURNS TEXT
AS '$libdir/pg_partitions_nolock', 'pg_get_expr_nolock'
LANGUAGE C STRICT;


SET allow_system_table_mods = true;

ALTER VIEW pg_catalog.pg_partitions RENAME TO pg_partitions_lock;

create view pg_catalog.pg_partitions as
  select 
      schemaname, 
      tablename, 
      partitionschemaname, 
      partitiontablename, 
      partitionname, 
      parentpartitiontablename, 
      parentpartitionname, 
      partitiontype, 
      partitionlevel, 
      -- Only the non-default parts of range partitions have 
      -- a non-null partition rank.  For these the rank is
      -- from (1, 2, ...) in keeping with the use of RANK(n)
      -- to identify the parts of a range partition in the 
      -- ALTER statement.
      case
          when partitiontype <> 'range'::text then null::bigint
          when partitionnodefault > 0 then partitionrank
          when partitionrank = 0 then null::bigint
          else partitionrank
          end as partitionrank, 
      partitionposition, 
      partitionlistvalues, 
      partitionrangestart, 
      case
          when partitiontype = 'range'::text then partitionstartinclusive
          else null::boolean
          end as partitionstartinclusive, partitionrangeend, 
      case
          when partitiontype = 'range'::text then partitionendinclusive
          else null::boolean
          end as partitionendinclusive, 
      partitioneveryclause, 
      parisdefault as partitionisdefault, 
      partitionboundary,
      parentspace as parenttablespace,
      partspace as partitiontablespace
  from 
      ( 
          select 
              n.nspname as schemaname, 
              cl.relname as tablename, 
              n2.nspname as partitionschemaname, 
              cl2.relname as partitiontablename, 
              pr1.parname as partitionname, 
              cl3.relname as parentpartitiontablename, 
              pr2.parname as parentpartitionname, 
              case
                  when pp.parkind = 'h'::"char" then 'hash'::text
                  when pp.parkind = 'r'::"char" then 'range'::text
                  when pp.parkind = 'l'::"char" then 'list'::text
                  else null::text
                  end as partitiontype, 
              pp.parlevel as partitionlevel, 
              pr1.parruleord as partitionposition, 
              case
                  when pp.parkind != 'r'::"char" or pr1.parisdefault then null::bigint
                  else
                      rank() over(
                      partition by pp.oid, cl.relname, pp.parlevel, cl3.relname
                      order by pr1.parisdefault, pr1.parruleord) 
                  end as partitionrank, 
              pg_get_expr_nolock(pr1.parlistvalues) as partitionlistvalues, 
              pg_get_expr_nolock(pr1.parrangestart) as partitionrangestart, 
              pr1.parrangestartincl as partitionstartinclusive, 
              pg_get_expr_nolock(pr1.parrangeend) as partitionrangeend, 
              pr1.parrangeendincl as partitionendinclusive, 
              pg_get_expr_nolock(pr1.parrangeevery) as partitioneveryclause, 
              min(pr1.parruleord) over(
                  partition by pp.oid, cl.relname, pp.parlevel, cl3.relname
                  order by pr1.parruleord) as partitionnodefault, 
              pr1.parisdefault, 
              pg_get_partition_rule_def(pr1.oid, true) as partitionboundary,
              coalesce(sp.spcname, dfltspcname) as parentspace,
              coalesce(sp3.spcname, dfltspcname) as partspace
          from 
              pg_namespace n, 
              pg_namespace n2, 
              pg_class cl
                  left join
              pg_tablespace sp on cl.reltablespace = sp.oid, 
              pg_class cl2
                  left join
              pg_tablespace sp3 on cl2.reltablespace = sp3.oid,
              pg_partition pp, 
              pg_partition_rule pr1
                  left join 
              pg_partition_rule pr2 on pr1.parparentrule = pr2.oid
                  left join 
              pg_class cl3 on pr2.parchildrelid = cl3.oid,
              (select s.spcname
               from pg_database, pg_tablespace s
               where datname = current_database()
                 and dattablespace = s.oid) d(dfltspcname)
      where 
          pp.paristemplate = false and 
          pp.parrelid = cl.oid and 
          pr1.paroid = pp.oid and 
          cl2.oid = pr1.parchildrelid and 
          cl.relnamespace = n.oid and 
          cl2.relnamespace = n2.oid) p1;
