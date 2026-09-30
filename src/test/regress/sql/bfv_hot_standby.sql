-- start_ignore
\! gpconfig -c hot_standby -v on
\! gpstop -raf
-- end_ignore

\c

CREATE TABLE temp_gp_segment_configuration AS
	SELECT * FROM gp_segment_configuration;

SET allow_system_table_mods to ON;

UPDATE gp_segment_configuration
SET address = 'foo', hostname = 'foo'
WHERE content = -1 AND role = 'm';

SET allow_system_table_mods to OFF;

-- Should fail becase address and hostname are incorrect
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
