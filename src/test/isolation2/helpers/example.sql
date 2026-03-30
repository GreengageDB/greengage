do $$
begin
  execute $func$
    create or replace language plpythonu;
  $func$;
exception
  when others then
    if SQLERRM = 'could not access file "$libdir/plpython2": No such file or directory'
    then
      begin
        if not exists (
		  select 1 from pg_language where lanname = 'plpythonu'
		) then
		  execute $func$
		    create or replace language plpython3u;
		    alter language plpython3u rename to plpythonu;
		  $func$;
		end if;
      exception
      when others then 
        raise notice 'Could not create or rename PL/Python language: %', SQLERRM;
      end;
    end if;
end;
$$;

create or replace function example() returns text as $$
	return "WITHIN TESTING"
$$ language plpythonu;
