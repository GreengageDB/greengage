-- This test checks for correct AO temp relfile deletion after session end.

CREATE OR REPLACE FUNCTION relfile_test_cmd(tbl regclass) RETURNS text
AS $$
  SELECT format('[ -f %L ] && echo 1 || echo 0',
         (SELECT setting FROM pg_settings WHERE name = 'data_directory')
         || '/' || pg_relation_filepath(tbl)); $$ LANGUAGE sql;

1: CREATE TEMP TABLE t2 (a int) WITH (APPENDONLY = TRUE, ORIENTATION = ROW);
1: CREATE TEMP TABLE t3 (a int) WITH (APPENDONLY = TRUE, ORIENTATION = COLUMN);

1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/check_ao_relfile_t2.sh' :
     select * from relfile_test_cmd('t2');
1: ! sh /tmp/check_ao_relfile_t2.sh;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/check_ao_relfile_t3.sh' :
     select * from relfile_test_cmd('t3');
1: ! sh /tmp/check_ao_relfile_t3.sh;

1q:

-- Wait some time until file cleanup
select wait_until_segment_synchronized(0);

! sh /tmp/check_ao_relfile_t2.sh; 
! sh /tmp/check_ao_relfile_t3.sh; 

-- Cleanup
DROP FUNCTION relfile_test_cmd(tbl regclass);
! rm /tmp/check_ao_relfile_t2.sh;
! rm /tmp/check_ao_relfile_t3.sh;