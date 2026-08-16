-- This test checks for correct AO temp relfile deletion after session end.

CREATE OR REPLACE FUNCTION relfile_test_cmd(tbl regclass) RETURNS text
AS $$
  SELECT format('[ -f %L ] && echo 1 || echo 0',
         (SELECT setting FROM pg_settings WHERE name = 'data_directory')
         || '/' || pg_relation_filepath(tbl)); $$ LANGUAGE sql;

1: CREATE TEMP TABLE temp_relfile_aoro (a int) WITH (APPENDONLY = TRUE, ORIENTATION = ROW);
1: CREATE TEMP TABLE temp_relfile_aoco (a int) WITH (APPENDONLY = TRUE, ORIENTATION = COLUMN);

1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/check_ao_relfile_aoro.sh' :
     select * from relfile_test_cmd('temp_relfile_aoro');
1: ! sh /tmp/check_ao_relfile_aoro.sh;
1: @post_run 'echo "${RAW_STR}" | awk \'NR==3\' >> /tmp/check_ao_relfile_aoco.sh' :
     select * from relfile_test_cmd('temp_relfile_aoco');
1: ! sh /tmp/check_ao_relfile_aoco.sh;

1q:

-- Wait some time until file cleanup
2: select pg_sleep(10);

2: ! sh /tmp/check_ao_relfile_aoro.sh; 
2: ! sh /tmp/check_ao_relfile_aoco.sh; 

2q:

-- Cleanup
DROP FUNCTION relfile_test_cmd(tbl regclass);
! rm /tmp/check_ao_relfile_aoro.sh;
! rm /tmp/check_ao_relfile_aoco.sh;