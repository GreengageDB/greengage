-- Tests exception handling of GPDB PL/PgSQL UDF
-- It exercises:
--  1. Several transaction control SQL statement
--  2. Various levels of sub-transactions
--  3. dtx protocol command: subtransaction_begin, subtransaction_rollback or subtransaction_release
--  4. Errors are tested at beginning and end of command, panic only on
--- beginning
--
--
-- start_matchsubs
-- s/\s+\(.*\.[ch]:\d+\)/ (SOMEFILE:SOMEFUNC)/
-- m/ /
-- m/transaction \d+/
-- s/transaction \d+/transaction /
-- m/transaction -\d+/
-- s/transaction -\d+/transaction/
-- end_matchsubs

-- skip FTS probes always
SELECT gp_inject_fault_infinite('fts_probe', 'skip', 1);
SELECT gp_request_fts_probe_scan();
select gp_wait_until_triggered_fault('fts_probe', 1, 1);
CREATE OR REPLACE FUNCTION test_excep (arg INTEGER) RETURNS INTEGER
AS $$
    DECLARE res INTEGER; /* in func */
    BEGIN /* in func */
        res := 100 / arg; /* in func */
        RETURN res; /* in func */
    EXCEPTION /* in func */
        WHEN division_by_zero /* in func */
        THEN  RETURN 999; /* in func */
    END; /* in func */
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_protocol_allseg(mid int, mshop int, mgender character) RETURNS VOID AS
$$
DECLARE tfactor int default 0; /* in func */
BEGIN /* in func */
  BEGIN /* in func */
  CREATE TABLE employees(id int, shop_id int, gender character) DISTRIBUTED BY (id); /* in func */
  
  INSERT INTO employees VALUES (0, 1, 'm'); /* in func */
  END; /* in func */
 BEGIN /* in func */
  BEGIN /* in func */
    IF EXISTS (select 1 from employees where id = mid) THEN /* in func */
        RAISE EXCEPTION 'Duplicate employee id'; /* in func */
    ELSE /* in func */
         IF NOT (mshop between 1 AND 2) THEN /* in func */
            RAISE EXCEPTION 'Invalid shop id' ; /* in func */
        END IF; /* in func */
    END IF; /* in func */
    SELECT * INTO tfactor FROM test_excep(0); /* in func */
    BEGIN /* in func */
        INSERT INTO employees VALUES (mid, mshop, mgender); /* in func */
    EXCEPTION /* in func */
            WHEN OTHERS THEN /* in func */
            BEGIN /* in func */
                RAISE NOTICE 'catching the exception ...3'; /* in func */
            END; /* in func */
    END; /* in func */
   EXCEPTION /* in func */
       WHEN OTHERS THEN /* in func */
          RAISE NOTICE 'catching the exception ...2'; /* in func */
   END; /* in func */
 EXCEPTION /* in func */
     WHEN OTHERS THEN /* in func */
          RAISE NOTICE 'catching the exception ...1'; /* in func */
 END; /* in func */
END; /* in func */
$$
LANGUAGE plpgsql;
SELECT role, preferred_role, content, mode, status FROM gp_segment_configuration;
--
--
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'panic', 'subtransaction_begin', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = '0' AND role = 'p';
DROP TABLE IF EXISTS employees;
select test_protocol_allseg(1, 2,'f');
-- make sure segment recovery is complete after panic.
0U: select 1;
0Uq:
select * from employees;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = '0' AND role = 'p';
--
--
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'panic',
                       'subtransaction_release', '', '', 1, -1, 0, dbid)
  FROM gp_segment_configuration WHERE content = '0' AND role = 'p';
DROP TABLE IF EXISTS employees;
select test_protocol_allseg(1, 2,'f');
-- make sure segment recovery is complete after panic.
0U: select 1;
0Uq:
select * from employees;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'reset', dbid)
  FROM gp_segment_configuration WHERE content = '0' AND role = 'p';
--
--
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'panic',
	               'subtransaction_release', '', '', 1, -1, 0, dbid, -1, 4)
       FROM gp_segment_configuration WHERE role = 'p' AND content = '0';
DROP TABLE IF EXISTS employees;
select test_protocol_allseg(1, 2,'f');
-- make sure segment recovery is complete after panic.
0U: select 1;
0Uq:
select * from employees;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'reset', dbid)
       FROM gp_segment_configuration where role = 'p' AND content = '0';
--
--
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'panic',
                       'subtransaction_rollback', '', '', 1, -1, 0, dbid, -1, 3)
  FROM gp_segment_configuration where role = 'p' and content = '0';
DROP TABLE IF EXISTS employees;
select test_protocol_allseg(1, 2,'f');
-- make sure segment recovery is complete after panic.
0U: select 1;
0Uq:
select * from employees;
select gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = '0';
--
--
select gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'panic',
                       'subtransaction_rollback', '', '', 1, -1, 0, dbid)
  from gp_segment_configuration where role = 'p' and content = '0';
DROP TABLE IF EXISTS employees;
select test_protocol_allseg(1, 2,'f');
-- make sure segment recovery is complete after panic.
0U: select 1;
0Uq:
select * from employees;
select gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = '0';
--
--
select gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'panic',
                       'subtransaction_begin', '', '', 1, -1, 0, dbid, -1, 3)
  from gp_segment_configuration where role = 'p' and content = '0';
DROP TABLE IF EXISTS employees;
select test_protocol_allseg(1, 2,'f');
-- make sure segment recovery is complete after panic.
0U: select 1;
0Uq:
select * from employees;
select gp_inject_fault('exec_mpp_dtx_protocol_command_start', 'reset', dbid)
  from gp_segment_configuration where role = 'p' and content = '0';

SELECT gp_inject_fault('fts_probe', 'reset', 1);
