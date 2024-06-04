create or replace language plpythonu;


--
-- pg_ctl:
--   datadir: data directory of process to target with `pg_ctl`
--   command: commands valid for `pg_ctl`
--   command_mode: modes valid for `pg_ctl -m`  
--
create or replace function pg_ctl(datadir text, command text, command_mode text default 'immediate')
returns text as $$
    class PgCtlError(Exception):
        def __init__(self, errmsg):
            self.errmsg = errmsg
        def __str__(self):
            return repr(self.errmsg)

    import subprocess
    if command == 'promote':
        cmd = 'pg_ctl promote -D %s' % datadir
    elif command in ('stop', 'restart'):
        cmd = 'pg_ctl -l postmaster.log -D %s ' % datadir
        cmd = cmd + '-w -t 600 -m %s %s' % (command_mode, command)
    else:
        return 'Invalid command input'

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    stdout, stderr = proc.communicate()

    # GPDB_12_MERGE_FIXME: upstream patch f13ea95f9e473a43ee4e1baeb94daaf83535d37c
    # (Change pg_ctl to detect server-ready by watching status in postmaster.pid.)
    # makes pg_ctl return 1 when the postgres is still starting up after timeout
    # so there is only need of checking of returncode then. For now we still
    # need to check stdout additionally since if the postgres is starting up
    # pg_ctl still returns 0 after timeout.

    if proc.returncode == 0 and stdout.find("server is still starting up") == -1:
        return 'OK'
    else:
        raise PgCtlError(stdout+'|'+stderr)
$$ language plpythonu;

--
-- pg_ctl_start:
--
-- Start a specific greenplum segment
--
-- intentionally separate from pg_ctl() because it needs more information
--
--   datadir: data directory of process to target with `pg_ctl`
--   port: which port the server should start on
--
create or replace function pg_ctl_start(datadir text, port int)
returns text as $$
    import subprocess
    cmd = 'pg_ctl -l postmaster.log -D %s ' % datadir
    opts = '-p %d' % (port)
    cmd = cmd + '-o "%s" start' % opts
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).replace('.', '')
$$ language plpythonu;


--
-- restart_primary_segments_containing_data_for(table_name text):
--     table_name: the table containing data whose segment that needs a restart
--
-- Note: this does an immediate restart, which forces recovery
--
create or replace function restart_primary_segments_containing_data_for(table_name text) returns setof integer as $$
declare
	segment_id integer;
begin
	for segment_id in select * from primary_segments_containing_data_for(table_name)
	loop
		perform pg_ctl(
      (select get_data_directory_for(segment_id)),
      'restart',
      'immediate'
    );
	end loop;
end;
$$ language plpgsql;


--
-- clean_restart_primary_segments_containing_data_for(table_name text):
--     table_name: the table containing data whose segment that needs a restart
--
-- Note: this does a fast restart, which does not require recovery
--
create or replace function clean_restart_primary_segments_containing_data_for(table_name text) returns setof integer as $$
declare
	segment_id integer;
begin
	for segment_id in select * from primary_segments_containing_data_for(table_name)
	loop
		perform pg_ctl(
      (select get_data_directory_for(segment_id)),
      'restart',
      'fast'
    );
	end loop;
end;
$$ language plpgsql;


create or replace function primary_segments_containing_data_for(table_name text) returns setof integer as $$
begin
	return query execute 'select distinct gp_segment_id from ' || table_name;
end;
$$ language plpgsql;


create or replace function get_data_directory_for(segment_number int, segment_role text default 'p') returns text as $$
BEGIN
	return (
		select datadir 
		from gp_segment_configuration 
		where role=segment_role and 
		content=segment_number
	);
END;
$$ language plpgsql;

create or replace function master() returns setof gp_segment_configuration as $$
	select * from gp_segment_configuration where role='p' and content=-1;
$$ language sql;

create or replace function wait_until_segment_synchronized(segment_number int) returns text as $$
begin
	for i in 1..1200 loop
		if (select count(*) = 0 from gp_segment_configuration where content = segment_number and mode != 's') then
			return 'OK';
		end if;
		perform pg_sleep(0.1);
		perform gp_request_fts_probe_scan();
	end loop;
	return 'Fail';
end;
$$ language plpgsql;

create or replace function wait_until_all_segments_synchronized() returns text as $$
begin
	/* no-op for a mirrorless cluster */
	if (select count(*) = 0 from gp_segment_configuration where role = 'm') then
		return 'OK'; /* in func */
	end if; /* in func */
	for i in 1..1200 loop
		if (select count(*) = 0 from gp_segment_configuration where content != -1 and mode != 's') then
			return 'OK';
		end if;
		perform pg_sleep(0.1);
		perform gp_request_fts_probe_scan();
	end loop;
	return 'Fail';
end;
$$ language plpgsql;

create or replace function wait_for_replication_replay (segid int, retries int) returns bool as
$$
declare
	i int;
	result bool;
begin
	i := 0;
	-- Wait until the mirror/standby has replayed up to flush location
	loop
		SELECT flush_location = replay_location INTO result from gp_stat_replication where gp_segment_id = segid;
		if result then
			return true;
		end if;

		if i >= retries then
		   return false;
		end if;
		perform pg_sleep(0.1);
		perform pg_stat_clear_snapshot();
		i := i + 1;
	end loop;
end;
$$ language plpgsql;

create or replace function wait_until_standby_in_state(targetstate text)
returns text as $$
declare
   replstate text;
   i int;
begin
   i := 0;
   while i < 1200 loop
      select state into replstate from pg_stat_replication;
      if replstate = targetstate then
          return replstate;
      end if;
      perform pg_sleep(0.1);
      perform pg_stat_clear_snapshot();
      i := i + 1;
   end loop;
   return replstate;
end;
$$ language plpgsql;

--
-- pg_controldata_redo_lsn:
--
-- Perform pg_controldata and find out latest checkpoint's REDO location
--   datadir: data directory of segment to target with `pg_controldata`
--
create or replace function pg_controldata_redo_lsn(datadir text)
    returns pg_lsn as $$
    import subprocess
    cmd = 'pg_controldata %s | grep \"Latest checkpoint\'s REDO location\"' % datadir
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).split(':')[1].strip()
$$ language plpythonu;

--
-- exec_cmd_on_segments:
--   Execute shell command on all segments
--
create or replace function exec_cmd_on_segments(cmd text)
returns text as $$
    import subprocess
    returncode = subprocess.call(cmd, shell=True)
    if returncode == 0:
        return 'OK'
    else:
        return 'Fail'
$$ language plpythonu execute on all segments;
