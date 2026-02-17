-- This file contains tests related to the casting behavior, primarily in joins.
-- Some of the tests performed here may already be present inside other files,
-- but it is nice to have a centralized place to observe and compare the behavior of the planners.
-- Tables here are prefixed with cst (which stands for 'cast') to avoid collisions.

-- One of the tests below fails with an assertion, that prints its location in the source code.
-- Make sure that we don't need to update this file when its line changes.
-- start_matchsubs
-- m/DETAIL:  CTranslatorQueryToDXL.cpp:\d+: Failed assertion/
-- s/DETAIL:  CTranslatorQueryToDXL.cpp:\d+: Failed assertion/DETAIL:  CTranslatorQueryToDXL.cpp:###: Failed assertion/
-- end_matchsubs

set optimizer_enable_mergejoin = false;
set optimizer_enable_nljoin = false;
set enable_mergejoin = false;
set enable_nestloop = false;
set optimizer_trace_fallback = true;

-- start_ignore
drop table if exists cst_int2;
drop table if exists cst_int4;
drop table if exists cst_int8;
drop table if exists cst_float4;
drop table if exists cst_float8;
drop table if exists cst_text;
drop table if exists cst_int2_int4;
drop table if exists cst_int4_int8;
-- end_ignore

create table cst_int2 as (select a::int2 from generate_series(0, 10) as a) distributed by (a);
create table cst_int4 as (select a::int4 from generate_series(5, 15) as a) distributed by (a);
create table cst_int8 as (select a::int8 from generate_series(10, 20) as a) distributed by (a);
create table cst_float4 as (select a::float4 from generate_series(5, 15, 0.5) as a) distributed by (a);
create table cst_float8 as (select a::float8 from generate_series(5, 15, 0.5) as a) distributed by (a);
create table cst_text as (select a::text from generate_series(5, 15) as a) distributed by (a);
create table cst_varchar as (select a::varchar from generate_series(5, 15) as a) distributed by (a);
create table cst_int2_int4 as (select gen::int2 as a, gen::int4 as b from generate_series(1, 10) as gen) distributed by (a, b);
create table cst_int4_int8 as (select gen::int4 as a, gen::int8 as b from generate_series(1, 10) as gen) distributed by (a, b);

-- Perform an inner join on all hash opfamilies containing more than one type.
-- We shouldn't see any redistributions, because:
--    The postgres-based planner has operators that work on different types.
--    ORCA doesn't support such operations, but it knows when a cast doesn't change a distribution.

set optimizer_join_order = query;

explain (verbose, costs off) select * from cst_int2 join cst_int4 using(a);
select * from cst_int2 join cst_int4 using(a);

explain (verbose, costs off) select * from cst_int2 join cst_int8 using(a);
select * from cst_int2 join cst_int8 using(a);

explain (verbose, costs off) select * from cst_int4 join cst_int8 using(a);
select * from cst_int4 join cst_int8 using(a);

explain (verbose, costs off) select * from cst_float4 join cst_float8 using(a);
select * from cst_float4 join cst_float8 using(a);

-- Same thing with the sides swapped
explain (verbose, costs off) select * from cst_int4 join cst_int2 using(a);
select * from cst_int4 join cst_int2 using(a);

explain (verbose, costs off) select * from cst_int8 join cst_int2 using(a);
select * from cst_int8 join cst_int2 using(a);

explain (verbose, costs off) select * from cst_int8 join cst_int4 using(a);
select * from cst_int8 join cst_int4 using(a);

explain (verbose, costs off) select * from cst_float8 join cst_float4 using(a);
select * from cst_float8 join cst_float4 using(a);

-- Confirm that casting logic works recursively
explain (verbose, costs off)
select * from cst_int2
    join cst_int8 using(a)
    join cst_int4 using(a);

select * from cst_int2
    join cst_int8 using(a)
    join cst_int4 using(a);

reset optimizer_join_order;

-- Confirm that the same logic is correct for other join types
explain (verbose, costs off) select * from cst_int2 full join cst_int4 using(a);
select * from cst_int2 full join cst_int4 using(a);

-- BUG: this test is failing for ORCA.
-- It's the test that requires start_matchsubs.
explain (verbose, costs off) select * from cst_int2 left join cst_int4 using(a);
select * from cst_int2 left join cst_int4 using(a);

explain (verbose, costs off) select * from cst_int2 right join cst_int4 using(a);
select * from cst_int2 right join cst_int4 using(a);

explain (verbose, costs off)
select * from cst_int4
where exists (select *
              from cst_int2
              where cst_int4.a = cst_int2.a);

select * from cst_int4
where exists (select *
              from cst_int2
              where cst_int4.a = cst_int2.a);

explain (verbose, costs off)
select * from cst_int4
where not exists (select *
                  from cst_int2
                  where cst_int4.a = cst_int2.a);

select * from cst_int4
where not exists (select *
                  from cst_int2
                  where cst_int4.a = cst_int2.a);

explain (verbose, costs off)
select * from cst_int4
where cst_int4.a not in (select * from cst_int2);

select * from cst_int4
where cst_int4.a not in (select * from cst_int2);

explain (verbose, costs off)
select * from cst_int2 natural join cst_int4;
select * from cst_int2 natural join cst_int4;


-- Here, instead of an implicit cast, an explicit one is present.
--    The postgres-based planner should require a redistribution, because
--    distribution of the cst_int2 table is not directly equal to the left-hand side of the expression.
--    ORCA, on the other hand, can see that redistribution is unnecessary in this case.
explain (verbose, costs off) select * from cst_int2 join cst_int4 on cst_int2.a::int4 = cst_int4.a;
select * from cst_int2 join cst_int4 on cst_int2.a::int4 = cst_int4.a;


-- The same thing, but with multiple casts in a row.
-- Because the first cast tends to be converted directly to a conversion function,
-- ORCA shouldn't be able to detect the first coercion and should require a redistribution.
explain (verbose, costs off) select * from cst_float4 as cst_float4_f join cst_float4 as cst_float4_s on cst_float4_f.a::int::float4 = cst_float4_s.a;
select * from cst_float4 as cst_float4_f join cst_float4 as cst_float4_s on cst_float4_f.a::int::float4 = cst_float4_s.a;

-- Check that binary coercible casts are ruled out
explain (verbose, costs off) select * from cst_varchar as foo join cst_varchar as bar using(a);
select * from cst_varchar as foo join cst_varchar as bar using(a);

explain (verbose, costs off) select * from cst_varchar join cst_text using(a);
select * from cst_varchar join cst_text using(a);

-- Сheck that we don't rule out necessary distributions.
-- Most basic cases:
explain (verbose, costs off) select * from cst_float4 join cst_int4 using(a);
select * from cst_float4 join cst_int4 using(a);

explain (verbose, costs off) select * from cst_int4 join cst_text on cst_int4.a = cst_text.a::int4;
select * from cst_int4 join cst_text on cst_int4.a = cst_text.a::int4;


-- ORCA: in order for there queries to work, equivalent expressions should be matched correctly
explain (verbose, costs off)
with int8_cte as (select * from cst_int8)
select * from (cst_int2 join int8_cte as cte_1 using(a))
    join (int8_cte as cte_2 join cst_int4 using(a)) using(a);

with int8_cte as (select * from cst_int8)
select * from (cst_int2 join int8_cte as cte_1 using(a))
    join (int8_cte as cte_2 join cst_int4 using(a)) using(a);

explain (verbose, costs off)
with int8_cte as (select * from cst_int8)
select * from (cst_int2 join int8_cte as cte_1 using(a))
    join (cst_int4 join int8_cte as cte_2 using(a)) using(a);

with int8_cte as (select * from cst_int8)
select * from (cst_int2 join int8_cte as cte_1 using(a))
    join (cst_int4 join int8_cte as cte_2 using(a)) using(a);


-- Test distribution by multiple keys
explain (verbose, costs off)
select * from cst_int2_int4 as t1 join cst_int4_int8 as t2 on (t1.a = t2.a and t1.b = t2.b);
select * from cst_int2_int4 as t1 join cst_int4_int8 as t2 on (t1.a = t2.a and t1.b = t2.b);

explain (verbose, costs off)
select * from cst_int2_int4 as t1 join cst_int4_int8 as t2 on (t1.a = t2.b and t1.b = t2.a);
select * from cst_int2_int4 as t1 join cst_int4_int8 as t2 on (t1.a = t2.b and t1.b = t2.a);

explain (verbose, costs off)
select * from cst_int2_int4 as t1 join cst_int2 as t2 on (t1.a = t2.a);
select * from cst_int2_int4 as t1 join cst_int2 as t2 on (t1.a = t2.a);

set optimizer_join_order = query;
explain (verbose, costs off)
with cst_int2_int4_copy as (select * from cst_int2_int4)
select * from cst_int2_int4
    natural join cst_int4_int8
    natural join cst_int2_int4_copy;

with cst_int2_int4_copy as (select * from cst_int2_int4)
select * from cst_int2_int4
    natural join cst_int4_int8
    natural join cst_int2_int4_copy;


-- Test several queries with nested-loop join
set optimizer_enable_nljoin = true;
set enable_nestloop = true;
set optimizer_enable_hashjoin = false;
set enable_hashjoin = false;

explain (verbose, costs off) select * from cst_int2 join cst_int4 using(a);
select * from cst_int2 join cst_int4 using(a);

-- Subtle behavior ahead: in ORCA, nested loop joins don't support equivalent expressions
-- like hash joins do.
-- So, when join order is fixed, we need to be careful with the join condition,
-- otherwise we might get a broadcast motion.
explain (verbose, costs off)
select * from cst_int2
    join cst_int4 on cst_int2.a = cst_int4.a
    join cst_int8 on cst_int4.a = cst_int8.a;

select * from cst_int2
    join cst_int4 on cst_int2.a = cst_int4.a
    join cst_int8 on cst_int4.a = cst_int8.a;

-- Luckily, when join order is not fixed, optimizer finds a join path that
-- gets rid of the broadcast.
reset optimizer_join_order;
explain (verbose, costs off)
select * from cst_int2
    join cst_int4 on cst_int2.a = cst_int4.a
    join cst_int8 on cst_int4.a = cst_int8.a;

select * from cst_int2
    join cst_int4 on cst_int2.a = cst_int4.a
    join cst_int8 on cst_int4.a = cst_int8.a;

-- Test very specific logic for a join on several keys with an index in ORCA
create index cst_int4_int8_idx on cst_int4_int8 (b);
explain (verbose, costs off)
select * from cst_int2_int4 as t1 join cst_int4_int8 as t2 on (t1.a = t2.a and t1.b = t2.b);
select * from cst_int2_int4 as t1 join cst_int4_int8 as t2 on (t1.a = t2.a and t1.b = t2.b);
drop index cst_int4_int8_idx;

-- Binary coercible casts should be ruled out too
explain (verbose, costs off) select * from cst_varchar as foo join cst_varchar as bar using(a);
select * from cst_varchar as foo join cst_varchar as bar using(a);

explain (verbose, costs off) select * from cst_varchar join cst_text using(a);
select * from cst_varchar join cst_text using(a);


drop table cst_int2;
drop table cst_int4;
drop table cst_int8;
drop table cst_float4;
drop table cst_float8;
drop table cst_text;
drop table cst_int2_int4;
drop table cst_int4_int8;

reset optimizer_trace_fallback;
reset enable_hashjoin;
reset enable_nestloop;
reset enable_mergejoin;
reset optimizer_enable_hashjoin;
reset optimizer_enable_nljoin;
reset optimizer_enable_mergejoin;
