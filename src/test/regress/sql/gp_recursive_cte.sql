-- Tests exercising different behaviour of the WITH RECURSIVE implementation in GPDB
-- GPDB's distributed nature requires thorough testing of many use cases in order to ensure correctness


-- Setup
create schema recursive_cte;
set search_path=recursive_cte;
create table recursive_table_1(id int);
insert into recursive_table_1 values (1), (2), (100);

-- Test the featureblocking GUC for recursive CTE
set gp_recursive_cte to off;
with recursive r(i) as (
   select 1
   union all
   select i + 1 from r
)
select * from recursive_table_1 where recursive_table_1.id IN (select * from r limit 10);
set gp_recursive_cte to on;

-- WITH RECURSIVE ref used with IN without correlation
with recursive r(i) as (
   select 1
   union all
   select i + 1 from r
)
select * from recursive_table_1 where recursive_table_1.id IN (select * from r limit 10);

-- WITH RECURSIVE ref used with NOT IN without correlation
with recursive r(i) as (
   select 1
   union all
   select i + 1 from r
)
select * from recursive_table_1 where recursive_table_1.id NOT IN (select * from r limit 10);

-- WITH RECURSIVE ref used with EXISTS without correlation
with recursive r(i) as (
   select 1
   union all
   select i + 1 from r
)
select * from recursive_table_1 where EXISTS (select * from r limit 10);

-- WITH RECURSIVE ref used with NOT EXISTS without correlation
with recursive r(i) as (
   select 1
   union all
   select i + 1 from r
)
select * from recursive_table_1 where NOT EXISTS (select * from r limit 10);

create table recursive_table_2(id int);
insert into recursive_table_2 values (11) , (21), (31);

-- WITH RECURSIVE ref used with IN & correlation
with recursive r(i) as (
	select * from recursive_table_2
	union all
	select r.i + 1 from r, recursive_table_2 where r.i = recursive_table_2.id
)
select recursive_table_1.id from recursive_table_1, recursive_table_2 where recursive_table_1.id IN (select * from r where r.i = recursive_table_2.id);

-- WITH RECURSIVE ref used with NOT IN & correlation
with recursive r(i) as (
	select * from recursive_table_2
	union all
	select r.i + 1 from r, recursive_table_2 where r.i = recursive_table_2.id
)
select recursive_table_1.id from recursive_table_1, recursive_table_2 where recursive_table_1.id NOT IN (select * from r where r.i = recursive_table_2.id);

-- WITH RECURSIVE ref used with EXISTS & correlation
with recursive r(i) as (
	select * from recursive_table_2
	union all
	select r.i + 1 from r, recursive_table_2 where r.i = recursive_table_2.id
)
select recursive_table_1.id from recursive_table_1, recursive_table_2 where recursive_table_1.id = recursive_table_2.id and EXISTS (select * from r where r.i = recursive_table_2.id);

-- WITH RECURSIVE ref used with NOT EXISTS & correlation
with recursive r(i) as (
	select * from recursive_table_2
	union all
	select r.i + 1 from r, recursive_table_2 where r.i = recursive_table_2.id
)
select recursive_table_1.id from recursive_table_1, recursive_table_2 where recursive_table_1.id = recursive_table_2.id and NOT EXISTS (select * from r where r.i = recursive_table_2.id);

-- WITH RECURSIVE ref used within a Expression sublink
with recursive r(i) as (
   select 1
   union all
   select i + 1 from r
)
select * from recursive_table_1 where recursive_table_1.id >= (select i from r limit 1) order by recursive_table_1.id;

-- WITH RECURSIVE ref used within an EXISTS subquery in another recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and EXISTS (select * from r limit 10)
)
select * from y limit 10;

-- WITH RECURSIVE ref used within a NOT EXISTS subquery in another recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and NOT EXISTS (select * from r limit 10)
)
select * from y limit 10;

-- WITH RECURSIVE ref used within an IN subquery in a non-recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y as (
    select * from recursive_table_1 where recursive_table_1.id IN (select * from r limit 10)
)
select * from y;

-- WITH RECURSIVE ref used within a NOT IN subquery in a non-recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y as (
    select * from recursive_table_1 where recursive_table_1.id NOT IN (select * from r limit 10)
)
select * from y;

-- WITH RECURSIVE ref used within an EXISTS subquery in a non-recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y as (
    select * from recursive_table_1 where EXISTS (select * from r limit 10)
)
select * from y;

-- WITH RECURSIVE ref used within a NOT EXISTS subquery in a non-recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y as (
    select * from recursive_table_1 where NOT EXISTS (select * from r limit 10)
)
select * from y;

-- WITH RECURSIVE non-recursive ref used within an EXISTS subquery in a recursive CTE
with recursive
r as (
    select * from recursive_table_2
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and EXISTS (select * from r)
)
select * from y limit 10;

-- WITH RECURSIVE non-recursive ref used within a NOT EXISTS subquery in a recursive CTE
with recursive
r as (
    select * from recursive_table_2
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and NOT EXISTS (select * from r)
)
select * from y limit 10;

-- WITH ref used within an IN subquery in another CTE
with
r as (
    select * from recursive_table_2 where id < 21
),
y as (
    select * from recursive_table_1 where id IN (select * from r)
)
select * from y;

-- WITH ref used within a NOT IN subquery in another CTE
with
r as (
    select * from recursive_table_2 where id < 21
),
y as (
    select * from recursive_table_1 where id NOT IN (select * from r)
)
select * from y;

-- WITH ref used within an EXISTS subquery in another CTE
with
r as (
    select * from recursive_table_2 where id < 21
),
y as (
    select * from recursive_table_1 where EXISTS (select * from r)
)
select * from y;

-- WITH ref used within a NOT EXISTS subquery in another CTE
with
r as (
    select * from recursive_table_2 where id < 21
),
y as (
    select * from recursive_table_1 where NOT EXISTS (select * from r)
)
select * from y;

-- WITH RECURSIVE ref used within a IN subquery in another recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and i IN (select * from r limit 10)
)
select * from y limit 10;

-- WITH RECURSIVE ref used within a NOT IN subquery in another recursive CTE
with recursive
r(i) as (
    select 1
    union all
    select r.i + 1 from r, recursive_table_2 where i = recursive_table_2.id
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and i NOT IN (select * from r limit 10)
)
select * from y limit 10;

-- WITH RECURSIVE non-recursive ref used within an IN subquery in a recursive CTE
with recursive
r as (
    select * from recursive_table_2
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and i IN (select * from r)
)
select * from y limit 10;

-- WITH RECURSIVE non-recursive ref used within a NOT IN subquery in a recursive CTE
with recursive
r as (
    select * from recursive_table_2
),
y(i) as (
    select 1
    union all
    select i + 1 from y, recursive_table_1 where i = recursive_table_1.id and i NOT IN (select * from r)
)
select * from y limit 10;

create table recursive_table_3(id int, a int);
insert into recursive_table_3 values (1, 2), (2, 3);
-- WITH RECURSIVE ref used within a window function
with recursive r(i, j) as (
	select * from recursive_table_3
	union all
	select i + 1, j from r, recursive_table_3 where r.i < recursive_table_3.id
)
select avg(i) over(partition by j) from r limit 100;

-- WITH RECURSIVE ref used within a UDF
create function sum_to_zero(integer) returns bigint as $$
with recursive r(i) as (
	select $1
	union all
	select i - 1 from r where i > 0
)
select sum(i) from r;
$$ language sql;
select sum_to_zero(10);

-- WITH RECURSIVE ref used within a UDF against a distributed table
create table people(name text, parent_of text);
insert into people values ('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e');
create function get_lineage(text) returns setof text as $$
with recursive r(person) as (
	select name from people where name = $1
	union all
	select name from r, people where people.parent_of = r.person
)
select * from r;
$$ language sql;
select get_lineage('d');

-- non-recursive CTE nested in non-recursive enclosing CTE
INSERT INTO recursive_table_1 SELECT i FROM generate_series(0, 100) i;

SELECT MAX(j)
FROM
(
  WITH nr1(i) AS (SELECT id FROM recursive_table_1 WHERE id >= 10)
  SELECT * FROM
  (
	  WITH nr2(j) AS (SELECT i FROM nr1 WHERE i >= 50)
	  SELECT nr2.j FROM nr2, nr1
  ) sub2
) sub1;

-- non-recursive CTE nested in recursive enclosing CTE
WITH RECURSIVE r1(i) AS
(
  SELECT 1
  UNION ALL
  (
    WITH r2(j) AS
    (
      SELECT id FROM recursive_table_1 WHERE id < 5
    )
    SELECT SUM(j) FROM r2
  )
)
SELECT * FROM r1;

-- recursive CTE nested in recursive enclosing CTE
WITH RECURSIVE r1(i) AS
(
  SELECT 1
  UNION ALL
  (
    WITH RECURSIVE r2(j) AS
    (
      SELECT 1
      UNION ALL
      SELECT j + 1 FROM r2 WHERE j < 5
    ) 
    SELECT i + 1 FROM r1, r2 WHERE i < 5
  )
)
SELECT SUM(i) FROM r1;

-- recursive CTE nested in non-recursive enclosing CTE
WITH nr(i) AS
(
    WITH RECURSIVE r(j) AS
    (
      SELECT 1
      UNION ALL
      SELECT j + 1 FROM r WHERE j < 5
    ) 
    SELECT SUM(j) FROM r
)
SELECT SUM(i) FROM nr;

-- WITH RECURSIVE ref within a correlated subquery
create table recursive_table_4(a int, b int);
create table recursive_table_5(c int, d int);
insert into recursive_table_4 select i, i* 2 from generate_series(1, 10) i;
insert into recursive_table_5 select i/2, i from generate_series(1, 10) i;
select * from recursive_table_4 where a > ALL (
	with recursive r(i) as (
		select sum(c) from recursive_table_5 where d < recursive_table_4.b
		union all
		select i / 2 from r where i > 0
	)
	select * from r
);

with recursive x(i) as (
    select 1
),
y(i) as (
    select sum(i) from x
    union all
    select i + 1 from y
),
z(i) as (
    select avg(i) from x
    union all
    select i + 1 from z
)
(select * from y limit 5)
union
(select * from z limit 10);

-- WTIH RECURSIVE and replicated table
create table t_rep_test_rcte(c int) distributed replicated;
create table t_rand_test_rcte(c int) distributed by (c);
insert into t_rep_test_rcte values (1);
insert into t_rand_test_rcte values (1), (2), (3);

analyze t_rep_test_rcte;
analyze t_rand_test_rcte;

explain
with recursive the_cte_here(n) as (
  select * from t_rep_test_rcte
  union all
  select n+1 from the_cte_here join t_rand_test_rcte
	              on t_rand_test_rcte.c = the_cte_here.n)
select * from the_cte_here;

with recursive the_cte_here(n) as (
  select * from t_rep_test_rcte
  union all
  select n+1 from the_cte_here join t_rand_test_rcte
	              on t_rand_test_rcte.c = the_cte_here.n)
select * from the_cte_here;

-- WTIH RECURSIVE and subquery
with recursive cte (n) as
(
  select 1
  union all
  select * from
  (
    with x(n) as (select n from cte)
    select n + 1 from x where n < 10
  ) q
)
select * from cte;

-- Test recursive CTE when the non-recursive term is a table scan with a
-- predicate on the distribution key, and the recursive term joins the CTE with
-- the same table on its non-distribution key
create table recursive_table_6(a numeric(4), b int);
insert into recursive_table_6 values (0::numeric, 3);
insert into recursive_table_6 values (2::numeric, 0);
insert into recursive_table_6 values (5::numeric, 0);
analyze recursive_table_6;

SELECT $query$
WITH RECURSIVE cte (i, j) AS (
    SELECT a, b FROM recursive_table_6 WHERE a = 0::numeric::numeric
    UNION ALL
    SELECT a, b FROM recursive_table_6, cte WHERE cte.i = recursive_table_6.b
)
SELECT i, j FROM cte;
$query$ AS qry \gset

EXPLAIN (COSTS OFF)
    :qry ;

:qry ;

-- Test recursive CTE doesnt create a plan with motion on top of worktablescan
CREATE TABLE t1 (a int, b int) DISTRIBUTED BY (a);
SET enable_nestloop = off;
SET enable_hashjoin = off;
SET enable_mergejoin = on;

explain (costs off) with recursive rcte as
   (
      ( select a, b, 1::integer recursion_level from t1 order by 1 )
      union all

      select parent_table.a, parent_table.b, rcte.recursion_level + 1
      from
      ( select a, b from t1 order by 1 ) parent_table
      join rcte on rcte.b = parent_table.a
   )
select count(*) from rcte;

RESET enable_nestloop;
RESET enable_hashjoin;
RESET enable_mergejoin;

-- using union rather than union all for recursive union
CREATE TABLE tmp(a int, b int);
INSERT INTO tmp SELECT generate_series(1,5);
INSERT INTO tmp SELECT * FROM tmp;
EXPLAIN (costs off)
WITH RECURSIVE x(a) as
(
    select a from tmp
    union
    select a+1 from x where a<10
)
select * from x ;

WITH RECURSIVE x(a) as
(
    select a from tmp
    union
    select a+1 from x where a<10
)
select * from x ;
-- issues: https://github.com/GreengageDB/greengage/issues/16422
-- Without a reference to CTE in subselect and with a group clause
CREATE TABLE test_cte (a int, b int);
EXPLAIN (costs off)
WITH RECURSIVE r(c1, c2) as
(
  select a as c1, b as c2 from test_cte
  union all
  select r.c1, r.c2 from r
  join
  (
    select a as c1 , max(b) as c2 from test_cte group by c1
  ) as tmp_table
  on r.c1 = tmp_table.c1
)
select * from r join test_cte on r.c1 = a;

-- Without a reference to CTE in subselect and with a distinct clause
EXPLAIN (costs off)
WITH RECURSIVE r(c1, c2) as
(
  select a as c1, b as c2 from test_cte
  union all
  select r.c1, r.c2 from r
  join
  (
    select max (distinct a )as c1 ,max (distinct b) as c2 from test_cte
  ) as tmp_table
  on r.c1 = tmp_table.c1
)
select * from r join test_cte on r.c1 = a;

Drop TABLE test_cte;
