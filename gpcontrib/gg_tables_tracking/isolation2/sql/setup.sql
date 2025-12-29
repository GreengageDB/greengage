create or replace function wait_until_all_segments_synchronized() returns text as $$
begin
	/* no-op for a mirrorless cluster */
	if (select count(*) = 0 from gp_segment_configuration where role = 'm') then
		return 'OK'; /* in func */
	end if; /* in func */
	for i in 1..1200 loop
		if (select count(*) = 0 from gp_segment_configuration where content != -1 and mode != 's') then
			return 'OK'; /* in func */
		end if; /* in func */
		perform pg_sleep(0.1); /* in func */
		perform gp_request_fts_probe_scan(); /* in func */
 	end loop; /* in func */
	return 'Fail'; /* in func */
end; /* in func */
$$ language plpgsql;

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

    if proc.returncode == 0:
        return 'OK'
    else:
        raise PgCtlError(stdout.decode()+'|'+stderr.decode())
$$ language plpython3u;

create or replace function wait_until_segments_are_down(num_segs int)
returns bool as
$$
declare
retries int; /* in func */
begin /* in func */
  retries := 1200; /* in func */
  loop /* in func */
    if (select count(*) = num_segs from gp_segment_configuration where status = 'd') then /* in func */
      return TRUE; /* in func */
    end if; /* in func */
    if retries <= 0 then /* in func */
      return FALSE; /* in func */
    end if; /* in func */
    perform pg_sleep(0.1); /* in func */
    retries := retries - 1; /* in func */
  end loop; /* in func */
end; /* in func */
$$ language plpgsql;
