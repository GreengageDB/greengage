do $$ 
begin /*in func*/
  execute $func$ /*in func*/
    drop language if exists plpythonu cascade; /*in func*/
  $func$; /*in func*/
  begin /*in func*/
    execute $func$ /*in func*/
      create language plpythonu; /*in func*/
    $func$; /*in func*/
  exception /*in func*/
    when others then /*in func*/
      if SQLERRM = 'could not access file "$libdir/plpython2": No such file or directory' then /*in func*/
        execute $func$ /*in func*/
          drop language if exists plpython3u cascade; /*in func*/
        $func$; /*in func*/
        begin /*in func*/
          execute $func$ /*in func*/
            create language plpython3u; /*in func*/
            alter language plpython3u rename to plpythonu; /*in func*/
          $func$; /*in func*/
        exception /*in func*/
          when others then /*in func*/
            raise exception 'Could not create or rename PL/Python language: %', SQLERRM; /*in func*/
        end; /*in func*/
      else /*in func*/
        raise exception 'Failed to create PL/Pythonu: %', SQLERRM; /*in func*/
      end if; /*in func*/
  end; /*in func*/
end; /*in func*/
$$;