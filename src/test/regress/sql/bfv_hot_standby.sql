-- start_matchsubs
-- m/WARNING\:\s+could not translate host name \"foo\".*/
-- s/WARNING\:\s+could not translate host name \"foo\".*/could not translate host name \"foo\"/
-- end_matchsubs

-- start_ignore
\! gpconfig -c hot_standby -v on
\! gpstop -raf
-- end_ignore

\c
-- must show on
show hot_standby;

CREATE TABLE temp_gp_segment_configuration AS
	SELECT * FROM gp_segment_configuration;

SET allow_system_table_mods to ON;

UPDATE gp_segment_configuration
SET address = 'foo', hostname = 'foo'
WHERE content = -1 AND role = 'm';

SET allow_system_table_mods to OFF;

-- Should fail because address and hostname are incorrect
\! psql -p $((PGPORT+1)) -d postgres -c "select dbid from gp_segment_configuration where content = -1 AND role = 'm';"

SET allow_system_table_mods to ON;

UPDATE gp_segment_configuration
SET (address, hostname) =
	(SELECT address, hostname FROM temp_gp_segment_configuration
	 WHERE content = -1 AND role = 'm')
WHERE content = -1 AND role = 'm';
SET allow_system_table_mods to OFF;

DROP TABLE temp_gp_segment_configuration;

\! psql -p $((PGPORT+1)) -d postgres -c "select dbid from gp_segment_configuration where content = -1 AND role = 'm';"

-- start_ignore
\! gpconfig -r hot_standby
\! gpstop -raf
-- end_ignore
