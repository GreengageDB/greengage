CREATE OR REPLACE FUNCTION gp_execute_on_server(content int, query text)
returns text language C as '$libdir/regress.so', 'gp_execute_on_server';

DO $$
DECLARE
	schema_rec record;
BEGIN
	FOR schema_rec IN
		SELECT nspname, gp_segment_id
		FROM (
			SELECT nspname, gp_segment_id
			FROM gp_dist_random('pg_namespace')
			UNION
			SELECT nspname, gp_segment_id
			FROM pg_namespace
		) n
		LEFT JOIN pg_stat_activity ON
			sess_id = regexp_replace(nspname, 'pg(_toast)?_temp_', '')::int
		WHERE sess_id is null AND nspname ~ '^pg(_toast)?_temp_[1-9]{1}[0-9]*'
	LOOP
		IF schema_rec.gp_segment_id = -1 then
			EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_rec.nspname);
		ELSE
		PERFORM gp_execute_on_server(schema_rec.gp_segment_id,
				format('DROP SCHEMA IF EXISTS %I CASCADE', schema_rec.nspname));
		END IF;
	END LOOP;
END $$;

DROP function gp_execute_on_server(content int, query text);
