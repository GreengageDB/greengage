/*
 *	version_gp.c
 *
 *	Greenplum version-specific routines for upgrades
 *
 *	Copyright (c) 2016-Present VMware, Inc. or its affiliates
 *	contrib/pg_upgrade/version_gp.c
 */
#include "postgres_fe.h"

#include "pg_upgrade_greenplum.h"

#include "access/transam.h"

#define NUMERIC_ALLOC 100

/*
 *	check_hash_partition_usage()
 *	8.3 -> 8.4
 *
 *	Hash partitioning was never officially supported in GPDB5 and was removed
 *	in GPDB6, but better check just in case someone has found the hidden GUC
 *	and used them anyway.
 *
 *	The hash algorithm was changed in 8.4, so upgrading is impossible anyway.
 *	This is basically the same problem as with hash indexes in PostgreSQL.
 */
void
check_hash_partition_usage(void)
{
	int				dbnum;
	FILE		   *script = NULL;
	bool			found = false;
	char			output_path[MAXPGPATH];

	/* Merge with PostgreSQL v11 introduced hash partitioning again. */
	if (GET_MAJOR_VERSION(old_cluster.major_version) >= 1100)
		return;

	prep_status("Checking for hash partitioned tables");

	snprintf(output_path, sizeof(output_path), "%s/%s",
			 log_opts.basedir, "hash_partitioned_tables.txt");

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_relname;
		DbInfo	   *active_db = &old_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&old_cluster, active_db->db_name);

		res = executeQueryOrDie(conn,
								"SELECT n.nspname, c.relname "
								"FROM pg_catalog.pg_partition p, pg_catalog.pg_class c, pg_catalog.pg_namespace n "
								"WHERE p.parrelid = c.oid AND c.relnamespace = n.oid "
								"AND parkind = 'h'");

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_relname = PQfnumber(res, "relname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen(output_path, "w")) == NULL)
				pg_log(PG_FATAL, "Could not create necessary file:  %s\n", output_path);
			if (!db_used)
			{
				fprintf(script, "Database:  %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_relname));
		}

		PQclear(res);

		PQfinish(conn);
	}

	if (found)
	{
		fclose(script);
		pg_log(PG_REPORT, "fatal\n");
		gp_fatal_log(
			   "| Your installation contains hash partitioned tables.\n"
			   "| Upgrading hash partitioned tables is not supported,\n"
			   "| so this cluster cannot currently be upgraded.  You\n"
			   "| can remove the problem tables and restart the\n"
			   "| migration.  A list of the problem tables is in the\n"
			   "| file:\n"
			   "| \t%s\n\n", output_path);
	}
	else
		check_ok();
}

/*
 * old_GPDB6_check_for_unsupported_sha256_password_hashes()
 *
 *  Support for password_hash_algorithm='sha-256' was removed in GPDB 7. Check if
 *  any roles have SHA-256 password hashes.
 */
void
old_GPDB6_check_for_unsupported_sha256_password_hashes(void)
{
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status("Checking for SHA-256 hashed passwords");

	snprintf(output_path, sizeof(output_path), "%s/%s",
			 log_opts.basedir, "roles_using_sha256_passwords.txt");

	/* It's enough to check this in one database, pg_authid is a shared catalog. */
	{
		PGresult   *res;
		int			ntups;
		int			rowno;
		int			i_rolname;
		DbInfo	   *active_db = &old_cluster.dbarr.dbs[0];
		PGconn	   *conn = connectToServer(&old_cluster, active_db->db_name);

		res = executeQueryOrDie(conn,
								"SELECT rolname FROM pg_catalog.pg_authid "
								"WHERE rolpassword LIKE 'sha256%%'");

		ntups = PQntuples(res);
		i_rolname = PQfnumber(res, "rolname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
				pg_fatal("Could not open file \"%s\": %s\n",
						 output_path, strerror(errno));
			fprintf(script, "  %s\n",
					PQgetvalue(res, rowno, i_rolname));
		}

		PQclear(res);

		PQfinish(conn);
	}

	if (script)
		fclose(script);

	if (found)
	{
		pg_log(PG_REPORT, "fatal\n");
		gp_fatal_log(
				 "| Your installation contains roles with SHA-256 hashed passwords. Using\n"
				 "| SHA-256 for password hashes is no longer supported. You can use\n"
				 "| ALTER ROLE <role name> WITH PASSWORD NULL as superuser to clear passwords,\n"
				 "| and restart the upgrade.  A list of the problem roles is in the file:\n"
				 "|    %s\n\n", output_path);
	}
	else
		check_ok();
}

/*
 * new_gpdb_invalidate_bitmap_indexes()
 *
 * GPDB_UPGRADE_FIXME: We are currently missing the support to migrate over bitmap indexes.
 * Hence, mark all bitmap indexes as invalid.
 */
void
new_gpdb_invalidate_bitmap_indexes(void)
{
	int			dbnum;

	prep_status("Invalidating bitmap indexes in new cluster");

	for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++)
	{
		DbInfo	   *olddb = &new_cluster.dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(&new_cluster, olddb->db_name);

		/*
		 * GPDB doesn't allow hacking the catalogs without setting
		 * allow_system_table_mods first.
		 */
		PQclear(executeQueryOrDie(conn, "set allow_system_table_mods=true"));

		/*
		 * check mode doesn't do much interesting for this but at least
		 * we'll know we are allowed to change allow_system_table_mods
		 * which is required
		 */
		if (!user_opts.check)
		{
			PQclear(executeQueryOrDie(conn,
									  "UPDATE pg_index SET indisvalid = false "
									  "  FROM pg_class c "
									  " WHERE c.oid = indexrelid AND "
									  "       indexrelid >= %u AND "
									  "       relam = 3013;",
									  FirstNormalObjectId));
		}
		PQfinish(conn);
	}

	check_ok();
}
