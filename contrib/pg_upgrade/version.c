/*
 *	version.c
 *
 *	Postgres-version-specific routines
 *
 *	Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/version.c
 */

#include "postgres_fe.h"

#include "pg_upgrade.h"

#include "catalog/pg_class.h"


#if 0
/*
 * In Greengage, large objects are not supported, so the below
 * code is not required
 */
/*
 * new_9_0_populate_pg_largeobject_metadata()
 *	new >= 9.0, old <= 8.4
 *	9.0 has a new pg_largeobject permission table
 */
void
new_9_0_populate_pg_largeobject_metadata(ClusterInfo *cluster, bool check_mode)
{
	int			dbnum;
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status("Checking for large objects");

	snprintf(output_path, sizeof(output_path), "pg_largeobject.sql");

	for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		int			i_count;
		DbInfo	   *active_db = &cluster->dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(cluster, active_db->db_name);

		/* find if there are any large objects */
		res = executeQueryOrDie(conn,
								"SELECT count(*) "
								"FROM	pg_catalog.pg_largeobject ");

		i_count = PQfnumber(res, "count");
		if (atoi(PQgetvalue(res, 0, i_count)) != 0)
		{
			found = true;
			if (!check_mode)
			{
				PQExpBufferData connectbuf;

				if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
					pg_fatal("could not open file \"%s\": %s\n", output_path, getErrorText());

				initPQExpBuffer(&connectbuf);
				appendPsqlMetaConnect(&connectbuf, active_db->db_name);
				fputs(connectbuf.data, script);
				termPQExpBuffer(&connectbuf);

				fprintf(script,
						"SELECT pg_catalog.lo_create(t.loid)\n"
						"FROM (SELECT DISTINCT loid FROM pg_catalog.pg_largeobject) AS t;\n");
			}
		}

		PQclear(res);
		PQfinish(conn);
	}

	if (script)
		fclose(script);

	if (found)
	{
		report_status(PG_WARNING, "warning");
		if (check_mode)
			pg_log(PG_WARNING, "\n"
				   "Your installation contains large objects.  The new database has an\n"
				   "additional large object permission table.  After upgrading, you will be\n"
				   "given a command to populate the pg_largeobject permission table with\n"
				   "default permissions.\n\n");
		else
			pg_log(PG_WARNING, "\n"
				   "Your installation contains large objects.  The new database has an\n"
				   "additional large object permission table, so default permissions must be\n"
				   "defined for all large objects.  The file\n"
				   "    %s\n"
				   "when executed by psql by the database superuser will set the default\n"
				   "permissions.\n\n",
				   output_path);
	}
	else
		check_ok();
}

#endif

#if 0
/*
 * GPDB: This is dead code as the only erstwhile caller is
 * old_9_3_check_for_line_data_type_usage(), which is not applicable to upgrades
 * from GPDB 5. See old_9_3_check_for_line_data_type_usage() for details.
 *
 * Besides, this makes use of a recursive CTE, which will result in:
 * ERROR:  RECURSIVE option in WITH clause is not supported
 * when executed on a source 5X cluster.
 *
 * To make use of this function for things like the name datatype for instance,
 * we would need to rewrite the query.
 */
/*
 * check_for_data_type_usage
 *	Detect whether there are any stored columns depending on the given type
 *
 * If so, write a report to the given file name, and return true.
 *
 * We check for the type in tables, matviews, and indexes, but not views;
 * there's no storage involved in a view.
 */
static bool
check_for_data_type_usage(ClusterInfo *cluster, const char *typename,
						  char *output_path)
{
	bool		found = false;
	FILE	   *script = NULL;
	int			dbnum;

	for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
	{
		DbInfo	   *active_db = &cluster->dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(cluster, active_db->db_name);
		PQExpBufferData querybuf;
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_relname,
					i_attname;

		/*
		 * The type of interest might be wrapped in a domain, array,
		 * composite, or range, and these container types can be nested (to
		 * varying extents depending on server version, but that's not of
		 * concern here).  To handle all these cases we need a recursive CTE.
		 */
		initPQExpBuffer(&querybuf);
		appendPQExpBuffer(&querybuf,
						  "WITH RECURSIVE oids AS ( "
		/* the target type itself */
						  "	SELECT '%s'::pg_catalog.regtype AS oid "
						  "	UNION ALL "
						  "	SELECT * FROM ( "
		/* inner WITH because we can only reference the CTE once */
						  "		WITH x AS (SELECT oid FROM oids) "
		/* domains on any type selected so far */
						  "			SELECT t.oid FROM pg_catalog.pg_type t, x WHERE typbasetype = x.oid AND typtype = 'd' "
						  "			UNION ALL "
		/* arrays over any type selected so far */
						  "			SELECT t.oid FROM pg_catalog.pg_type t, x WHERE typelem = x.oid AND typtype = 'b' "
						  "			UNION ALL "
		/* composite types containing any type selected so far */
						  "			SELECT t.oid FROM pg_catalog.pg_type t, pg_catalog.pg_class c, pg_catalog.pg_attribute a, x "
						  "			WHERE t.typtype = 'c' AND "
						  "				  t.oid = c.reltype AND "
						  "				  c.oid = a.attrelid AND "
						  "				  NOT a.attisdropped AND "
						  "				  a.atttypid = x.oid ",
						  typename);

		/* Ranges came in in 9.2 */
		if (GET_MAJOR_VERSION(cluster->major_version) >= 902)
			appendPQExpBuffer(&querybuf,
							  "			UNION ALL "
			/* ranges containing any type selected so far */
							  "			SELECT t.oid FROM pg_catalog.pg_type t, pg_catalog.pg_range r, x "
							  "			WHERE t.typtype = 'r' AND r.rngtypid = t.oid AND r.rngsubtype = x.oid");

		appendPQExpBuffer(&querybuf,
						  "	) foo "
						  ") "
		/* now look for stored columns of any such type */
						  "SELECT n.nspname, c.relname, a.attname "
						  "FROM	pg_catalog.pg_class c, "
						  "		pg_catalog.pg_namespace n, "
						  "		pg_catalog.pg_attribute a "
						  "WHERE	c.oid = a.attrelid AND "
						  "		NOT a.attisdropped AND "
						  "		a.atttypid IN (SELECT oid FROM oids) AND "
						  "		c.relkind IN ("
						  CppAsString2(RELKIND_RELATION) ", "
						  CppAsString2(RELKIND_MATVIEW) ", "
						  CppAsString2(RELKIND_INDEX) ") AND "
						  "		c.relnamespace = n.oid AND "
		/* exclude possible orphaned temp tables */
						  "		n.nspname !~ '^pg_temp_' AND "
						  "		n.nspname !~ '^pg_toast_temp_' AND "
		/* exclude system catalogs, too */
						  "		n.nspname NOT IN ('pg_catalog', 'information_schema')");

		res = executeQueryOrDie(conn, "%s", querybuf.data);

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_relname = PQfnumber(res, "relname");
		i_attname = PQfnumber(res, "attname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
				pg_fatal("could not open file \"%s\": %s\n", output_path, getErrorText());
			if (!db_used)
			{
				fprintf(script, "In database: %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_relname),
					PQgetvalue(res, rowno, i_attname));
		}

		PQclear(res);

		termPQExpBuffer(&querybuf);

		PQfinish(conn);
	}

	if (script)
		fclose(script);

	return found;
}
#endif

#if 0
/*
 * GPDB 5 does not support the line datatype.
 */
/*
 * old_9_3_check_for_line_data_type_usage()
 *	9.3 -> 9.4
 *	Fully implement the 'line' data type in 9.4, which previously returned
 *	"not enabled" by default and was only functionally enabled with a
 *	compile-time switch; as of 9.4 "line" has a different on-disk
 *	representation format.
 */
void
old_9_3_check_for_line_data_type_usage(ClusterInfo *cluster)
{
	char		output_path[MAXPGPATH];

	prep_status("Checking for invalid \"line\" user columns");

	snprintf(output_path, sizeof(output_path), "tables_using_line.txt");

	if (check_for_data_type_usage(cluster, "pg_catalog.line", output_path))
	{
		pg_log(PG_REPORT, "fatal\n");
		pg_fatal("Your installation contains the \"line\" data type in user tables.  This\n"
		"data type changed its internal and input/output format between your old\n"
				 "and new clusters so this cluster cannot currently be upgraded.  You can\n"
		"remove the problem tables and restart the upgrade.  A list of the problem\n"
				 "columns is in the file:\n"
				 "    %s\n\n", output_path);
	}
	else
		check_ok();
}
#endif
