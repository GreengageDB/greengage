DO $$
DECLARE
	nsp pg_namespace.nspname%type;
BEGIN
	FOR nsp IN
		SELECT nspname
		FROM (
			SELECT nspname
			FROM gp_dist_random('pg_namespace')
			UNION
			SELECT nspname
			FROM pg_namespace
		) n
		LEFT JOIN pg_stat_activity ON
			sess_id = regexp_replace(nspname, 'pg(_toast)?_temp_', '')::int
		WHERE sess_id is null AND nspname ~ '^pg(_toast)?_temp_[0-9]+'
	LOOP
		EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', nsp);
	END LOOP;
END $$;
