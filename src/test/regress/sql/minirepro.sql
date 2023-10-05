-- start_ignore
drop database if exists gpsd_db_with_hll;
-- end_ignore
create database gpsd_db_with_hll;
\c gpsd_db_with_hll
create sequence minirepro_foo_c_seq cache 1;
create table minirepro_foo(a int, b int[], c int default nextval('minirepro_foo_c_seq'::regclass), d serial);

insert into minirepro_foo values(1, '{10000, 10000, 10000, 10000}');
insert into minirepro_foo values(1, '{10000, 10000, 10000, 10000}');
insert into minirepro_foo values(1, '{10000, 10000, 10000, 10000}');
analyze minirepro_foo;

\! echo "select * from minirepro_foo;" > ./data/minirepro_q.sql
-- start_ignore
\! minirepro gpsd_db_with_hll -q data/minirepro_q.sql -f data/minirepro.sql
-- end_ignore

drop table minirepro_foo;
drop sequence minirepro_foo_c_seq;

\! psql -f data/minirepro.sql gpsd_db_with_hll




