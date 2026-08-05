--
-- Extra GPDB tests on INSERT/UPDATE/DELETE RETURNING
--

CREATE TABLE returning_parttab (distkey int4, partkey int4, i int, t text)
DISTRIBUTED BY (distkey)
PARTITION BY RANGE (partkey) (START (1) END (10));

--
-- Test INSERT RETURNING with partitioning
--
insert into returning_parttab values (1, 1, 1, 'single insert') returning *;
insert into returning_parttab
select 1, g, g, 'multi ' || g from generate_series(1, 5) g
returning distkey, partkey, i, t;

-- Drop a column, and create a new partition. The new partition will not have
-- the dropped column, while in the old partition, it's still physically there,
-- just marked as dropped. Make sure the executor maps the columns correctly.
ALTER TABLE returning_parttab DROP COLUMN i;

alter table returning_parttab add partition newpart start (10) end (20);

insert into returning_parttab values (1, 10, 'single2 insert') returning *;
insert into returning_parttab select 2, g + 10, 'multi2 ' || g from generate_series(1, 5) g
returning distkey, partkey, t;

--
-- Test UPDATE/DELETE RETURNING with partitioning
--
update returning_parttab set partkey = 9 where partkey = 3 returning *;
update returning_parttab set partkey = 19 where partkey = 13 returning *;

-- update that moves the tuple across partitions
update returning_parttab set partkey = 18 where partkey = 4 returning *;

-- delete
delete from returning_parttab where partkey = 14 returning *;


-- Check table contents, to be sure that all the commands did what they claimed.
select * from returning_parttab;

--
-- DELETE ... RETURNING on AO tables reads the deleted row from the wholerow
-- junk column (AO cannot fetch a row by TID).
--
CREATE TEMP TABLE returning_aotab (id int4) WITH (appendonly=true);
INSERT INTO returning_aotab VALUES (1);
DELETE FROM returning_aotab RETURNING *;
-- PG18 OLD/NEW on DELETE: OLD is the deleted row, NEW is all-NULL
INSERT INTO returning_aotab VALUES (2);
DELETE FROM returning_aotab RETURNING old.id, new.id;
SELECT count(*) FROM returning_aotab;

--
-- PG18 RETURNING OLD on AO tables: the old tuple cannot be fetched by TID,
-- so the planner ships it in the wholerow junk column when RETURNING
-- references OLD.
--
CREATE TEMP TABLE returning_ao_old (id int4, t text) WITH (appendonly=true) DISTRIBUTED BY (id);
INSERT INTO returning_ao_old VALUES (1, 'a');
UPDATE returning_ao_old SET t = 'b' RETURNING old.t, new.t;
UPDATE returning_ao_old SET t = 'b' RETURNING new.t;
UPDATE returning_ao_old SET t = 'c' RETURNING t;
-- ... but a distribution-key UPDATE is a Split Update, where the previous
-- values are not available: still rejected
UPDATE returning_ao_old SET id = id + 1 RETURNING old.id;
-- column-oriented AO behaves identically
CREATE TEMP TABLE returning_aoco_old (id int4, t text) USING ao_column DISTRIBUTED BY (id);
INSERT INTO returning_aoco_old VALUES (1, 'a');
UPDATE returning_aoco_old SET t = 'b' RETURNING old.t;
UPDATE returning_aoco_old SET t = 'b' RETURNING id, t;
DELETE FROM returning_aoco_old RETURNING id, t;


--
-- Test UPDATE RETURNING with a split update, i.e. an update of the distribution
-- key.
--
CREATE TEMP TABLE returning_disttest (id int4) DISTRIBUTED BY (id);
INSERT INTO returning_disttest VALUES (1), (2);

-- Disable QUIET mode, so that we get some testing of the command tag as well.
-- (At one point, each split update incorrectly counted as two updated rows.)
\set QUIET off

UPDATE returning_disttest SET id = id + 1;

SELECT * FROM returning_disttest;

-- Test an UPDATE that moves the row to different segment and different partition
UPDATE returning_parttab SET distkey = 2, partkey = 13 WHERE partkey = 2 RETURNING distkey, partkey, t;
select * from returning_parttab WHERE distkey = 2 AND partkey = 13;
