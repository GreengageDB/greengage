from gppylib.utils import escapeDoubleQuoteInSQLString

class LeakedSchemaDropper:

    # This query does a union of all the leaked temp schemas on the coordinator as well as all the segments.
    # Also note autovacuum lancher and worker will not generate temp namespace
    # Additionally, we should take into account orphaned schemas, whose session_id may
    # conflict with coordinator bgworkers, which shouldn't create orphaned schemas.
    # Also try to avoid connection id collision with truly orphaned schema id.
    leaked_schema_query = """
        WITH temp_schemas AS (
          SELECT nspname, 
                 CASE 
                   WHEN nspname ~ '^pg_temp_[0-9]+' THEN replace(nspname, 'pg_temp_', '')::int
                   ELSE replace(nspname, 'pg_toast_temp_', '')::int
                 END as sess_id
          FROM gp_dist_random('pg_namespace')
          WHERE nspname ~ '^pg_t(emp|oast_temp)_[0-9]+'
          UNION
          SELECT nspname,
                 CASE 
                   WHEN nspname ~ '^pg_temp_[0-9]+' THEN replace(nspname, 'pg_temp_', '')::int
                   ELSE replace(nspname, 'pg_toast_temp_', '')::int
                 END as sess_id
          FROM pg_namespace
          WHERE nspname ~ '^pg_t(emp|oast_temp)_[0-9]+'
        )
        SELECT DISTINCT nspname as schema
        FROM temp_schemas n 
        LEFT JOIN pg_stat_activity x USING (sess_id)
        WHERE x.sess_id IS NULL 
           OR x.backend_type LIKE 'autovacuum%'
           OR x.datname IS NULL 
           OR x.datname <> current_database()
           OR x.pid = pg_backend_pid()
    """

    def __get_leaked_schemas(self, db_connection):
        with db_connection.cursor() as curs:
            curs.execute(self.leaked_schema_query)
            leaked_schemas = curs.fetchall()

            if not leaked_schemas:
                return []

            return [row[0] for row in leaked_schemas if row[0]]

    def drop_leaked_schemas(self, db_connection):
        leaked_schemas = self.__get_leaked_schemas(db_connection)
        for leaked_schema in leaked_schemas:
            escaped_schema_name = escapeDoubleQuoteInSQLString(leaked_schema)
            with db_connection.cursor() as curs:
                curs.execute('DROP SCHEMA IF EXISTS %s CASCADE;' % (escaped_schema_name))
        return leaked_schemas
