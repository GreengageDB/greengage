do $$
begin /* in func */
  execute $func$ /* in func */
    create or replace language plpythonu; /* in func */
  $func$; /* in func */
exception /* in func */
  when others then /* in func */
    if SQLERRM = 'could not access file "$libdir/plpython2": No such file or directory' /* in func */
    then /* in func */
      begin /* in func */
        if not exists ( /* in func */
		  select 1 from pg_language where lanname = 'plpythonu' /* in func */
		) then /* in func */
		  execute $func$ /* in func */
		    create or replace language plpython3u; /* in func */
		    alter language plpython3u rename to plpythonu; /* in func */
		  $func$; /* in func */
		end if; /* in func */
      exception /* in func */
      when others then /* in func */
        raise notice 'Could not create or rename PL/Python language: %', SQLERRM; /* in func */
      end; /* in func */
    end if; /* in func */
end; /* in func */
$$;

create or replace function example() returns text as $$
	return "WITHIN TESTING"
$$ language plpythonu;
