-- Used to be a bug where we release the gangs and reset context. The
-- subsequent retry succeeds with the new gang. When resetting the
-- session, as the warning message says, we drop ongoing temporary
-- namespace. However, whenever new temporary namespace is created, we
-- install shmem_exit callback for this namespace clean up. We earlier
-- missed to uninstall this callback on resetting the gang. That was
-- the reason this test exposed out of shmem_exit slots. Currently
-- MAX_ON_EXITS is set to 20 hence creates 20 transactions. Erroring
-- commit_prepared first time is used as vehicle to trigger gang
-- reset.

-- start_matchsubs
-- m/WARNING:.*Any temporary tables for this session have been dropped because the gang was disconnected/
-- s/session id \=\s*\d+/session id \= DUMMY/gm
-- end_matchsubs

CREATE TABLE foo(a int, b int);
-- 1
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error','commit_prepared',
	'','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 2
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error','commit_prepared',
	'','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 3
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	               'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;

-- 4
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error','commit_prepared',
	'','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 5
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error','commit_prepared',
	'','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 6
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
                       'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 7
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
                       'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 8
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	               'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 9
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
                       'commit_prepared','','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 10
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error','commit_prepared',
	'','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 11
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 12
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 13
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 14
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;

-- 15
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;

-- 16
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;

-- 17
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 18
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;

SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- 19
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;

-- 20
CREATE TEMP TABLE foo_stg AS SELECT * FROM foo;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','error',
	'commit_prepared', '','',1,1,0,dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
DROP TABLE foo_stg;
SELECT gp_inject_fault('exec_mpp_dtx_protocol_command_start','reset',dbid)
  FROM gp_segment_configuration WHERE mode='s' and content=1 and role='p' ;
-- start_ignore
-- After error, temp schemas may still exist at segments
-- Let's remove all such temporary schemas for inactive connections
\i sql/remove_temp_schemas.sql
-- end_ignore
