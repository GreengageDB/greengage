/*
 *	check.c
 *
 *	server checks and output routines
 *
 *	Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/check.c
 */

#include "postgres_fe.h"

#include "mb/pg_wchar.h"
#include "pg_upgrade.h"
#include "greengage/pg_upgrade_greengage.h"

static void set_locale_and_encoding(ClusterInfo *cluster);
static void check_new_cluster_is_empty(void);
static void check_locale_and_encoding(ControlData *oldctrl,
						  ControlData *newctrl);
static bool equivalent_locale(int category, const char *loca, const char *locb);
static bool equivalent_encoding(const char *chara, const char *charb);
static void check_is_super_user(ClusterInfo *cluster);
static void check_proper_datallowconn(ClusterInfo *cluster);
static void check_for_prepared_transactions(ClusterInfo *cluster);
static void check_for_isn_and_int8_passing_mismatch(ClusterInfo *cluster);
static void check_for_reg_data_type_usage(ClusterInfo *cluster);
static void check_for_jsonb_9_4_usage(ClusterInfo *cluster);
static void get_bin_version(ClusterInfo *cluster);
static char *get_canonical_locale_name(int category, const char *locale);


/*
 * fix_path_separator
 * For non-Windows, just return the argument.
 * For Windows convert any forward slash to a backslash
 * such as is suitable for arguments to builtin commands
 * like RMDIR and DEL.
 */
static char *
fix_path_separator(char *path)
{
#ifdef WIN32

	char	   *result;
	char	   *c;

	result = pg_strdup(path);

	for (c = result; *c != '\0'; c++)
		if (*c == '/')
			*c = '\\';

	return result;
#else

	return path;
#endif
}

void
output_check_banner(bool live_check)
{
	if (user_opts.check && live_check)
	{
		pg_log(PG_REPORT, "Performing Consistency Checks on Old Live Server\n");
		pg_log(PG_REPORT, "------------------------------------------------\n");
	}
	else
	{
		pg_log(PG_REPORT, "Performing Consistency Checks\n");
		pg_log(PG_REPORT, "-----------------------------\n");
	}
}


void
check_and_dump_old_cluster(bool live_check, char **sequence_script_file_name)
{
	/* -- OLD -- */

	if (!live_check)
		start_postmaster(&old_cluster, true);

	set_locale_and_encoding(&old_cluster);

	if (is_greengage_dispatcher_mode())
		generate_old_tablespaces_file(&old_cluster);

	/* Extract a list of databases and tables from the old cluster */
	get_db_and_rel_infos(&old_cluster);

	/*
	 * GPDB5: The chkpnt_oldstxid field is missing from a 5X cluster's control
	 * file. So, we have to calculate it ourselves here, before it gets used in
	 * copy_clog_xlog_xid(), to populate the new cluster's oldest XID.
	 */
	if (GET_MAJOR_VERSION(old_cluster.major_version) < 901)
		set_old_cluster_chkpnt_oldstxid();

	if (!user_opts.check || is_greengage_dispatcher_mode())
		init_tablespaces();

	get_loadable_libraries();

	/*
	 * Check for various failure cases
	 */
	report_progress(&old_cluster, CHECK, "Running checks");
	check_is_super_user(&old_cluster); /* GPDB: Don't skip super user check since it populates old_cluster.install_role_oid */
	if (skip_checks())
	{
		prep_status("Skipping Consistency Checks");
		check_ok();
		goto dump_old_cluster;
	}
	check_proper_datallowconn(&old_cluster);
	check_for_prepared_transactions(&old_cluster);
	check_for_reg_data_type_usage(&old_cluster);
	check_for_isn_and_int8_passing_mismatch(&old_cluster);

	/*
	 * Check for various Greengage failure cases. Since the target coordinator
	 * segment's catalog is later copied over to instantiate the target
	 * primary segments and none of the Greengage upgrade checks are strictly
	 * required to be run against the source cluster primary segments, only
	 * run the Greengage upgrade checks against the source coordinator
	 * segment.
	 */
	if (is_greengage_dispatcher_mode())
		check_greengage();

	if (GET_MAJOR_VERSION(old_cluster.major_version) == 904 &&
		old_cluster.controldata.cat_ver < JSONB_FORMAT_CHANGE_CAT_VER)
		check_for_jsonb_9_4_usage(&old_cluster);

dump_old_cluster: /* GPDB: Don't skip checks that output scripts. */
	/* old = PG 8.3 checks? */
	if (GET_MAJOR_VERSION(old_cluster.major_version) == 803)
	{
		old_8_3_check_for_tsquery_usage(&old_cluster);
		old_8_3_check_ltree_usage(&old_cluster);
		check_hash_partition_usage();
		if (user_opts.check)
		{
			old_8_3_rebuild_tsvector_tables(&old_cluster, true);
			old_8_3_invalidate_hash_gin_indexes(&old_cluster, true);
			old_8_3_invalidate_bpchar_pattern_ops_indexes(&old_cluster, true);
		}
		else
		{
			/*
			 * While we have the old server running, create the script to
			 * properly restore its sequence values but we report this at the
			 * end.
			 */
			*sequence_script_file_name =
				old_8_3_create_sequence_script(&old_cluster);
		}
	}

	/*
	 * While not a check option, we do this now because this is the only time
	 * the old server is running.
	 */
	if (!user_opts.check && is_greengage_dispatcher_mode())
		generate_old_dump();

	if (!live_check)
		stop_postmaster(false);
}


void
check_new_cluster(void)
{
	set_locale_and_encoding(&new_cluster);

	check_locale_and_encoding(&old_cluster.controldata, &new_cluster.controldata);

	get_db_and_rel_infos(&new_cluster);

	check_new_cluster_is_empty();

	if (!skip_checks())
		check_loadable_libraries();

	if (user_opts.transfer_mode == TRANSFER_MODE_LINK)
		check_hard_link();

	/*
	 * GPDB: This check is needed even when skipping checks since it has a
	 * side effect of populating new_cluster.install_role_oid
	 */
	check_is_super_user(&new_cluster);

	/*
	 * We don't restore our own user, so both clusters must match have
	 * matching install-user oids.
	 */
	if (old_cluster.install_role_oid != new_cluster.install_role_oid)
		pg_fatal("Old and new cluster install users have different values for pg_authid.oid.\n");

	/*
	 * We only allow the install user in the new cluster because other defined
	 * users might match users defined in the old cluster and generate an
	 * error during pg_dump restore.
 	 *
 	 * However, in Greengage, if we are upgrading a segment, its users have
	 * already been replicated to it from the master via gpupgrade.  Hence,
	 * we only need to do this check for the QD.  In other words, the
	 * Greengage cluster upgrade scheme will overwrite the QE's schema
 	 * with the QD's schema, making this check inappropriate for a QE upgrade.
	 */
	if (is_greengage_dispatcher_mode())
	{
		if (new_cluster.role_count != 1)
			pg_fatal("Only the install user can be defined in the new cluster.\n");
	}

	if (!skip_checks())
		check_for_prepared_transactions(&new_cluster);
}


void
report_clusters_compatible(void)
{
	if (user_opts.check)
	{
		if (get_check_fatal_occurred())
		{
			char		cwd[MAXPGPATH];
			if (!getcwd(cwd, MAXPGPATH))
				pg_fatal("could not determine current directory: %m\n");
			canonicalize_path(cwd);

			pg_log(PG_REPORT, "\n*Some cluster objects are not compatible*\n\npg_upgrade check output files are located:\n%s\n\n", cwd);
		} else
			pg_log(PG_REPORT, "\n*Clusters are compatible*\n");


		/* stops new cluster */
		stop_postmaster(false);
		if (get_check_fatal_occurred())
			exit(1);
		exit(0);
	}

	pg_log(PG_REPORT, "\n"
		   "If pg_upgrade fails after this point, you must re-initdb the\n"
		   "new cluster before continuing.\n");
}


void
issue_warnings_and_set_wal_level(char *sequence_script_file_name)
{
	/*
	 * We unconditionally start/stop the new server because pg_resetwal -o
	 * set wal_level to 'minimum'.  If the user is upgrading standby
	 * servers using the rsync instructions, they will need pg_upgrade
	 * to write its final WAL record with the proper wal_level.
	 */
	start_postmaster(&new_cluster, true);

	if (GET_MAJOR_VERSION(old_cluster.major_version) == 803)
	{
		/* restore proper sequence values using file created from old server */
		if (sequence_script_file_name)
		{
			prep_status("Adjusting sequences");
			exec_prog(UTILITY_LOG_FILE, NULL, true, true,
					  PG_OPTIONS_UTILITY_MODE
					  "\"%s/psql\" " EXEC_PSQL_ARGS " %s -f \"%s\"",
					  new_cluster.bindir, cluster_conn_opts(&new_cluster),
					  sequence_script_file_name);
			unlink(sequence_script_file_name);
			check_ok();
		}

		old_8_3_rebuild_tsvector_tables(&new_cluster, false);
		old_8_3_invalidate_hash_gin_indexes(&new_cluster, false);
		old_8_3_invalidate_bpchar_pattern_ops_indexes(&new_cluster, false);
	}

#if 0
	/*
	 * GPDB 6 does not support large objects
	 */
	/* Create dummy large object permissions for old < PG 9.0? */
	if (GET_MAJOR_VERSION(old_cluster.major_version) <= 804)
		new_9_0_populate_pg_largeobject_metadata(&new_cluster, false);
#endif

	stop_postmaster(false);
}


void
output_completion_banner(char *analyze_script_file_name,
						 char *deletion_script_file_name)
{
	/* Did we copy the free space files? */
	if (GET_MAJOR_VERSION(old_cluster.major_version) >= 804)
		pg_log(PG_REPORT,
			   "Optimizer statistics are not transferred by pg_upgrade so,\n"
			   "once you start the new server, consider running:\n"
			   "    %s\n\n", analyze_script_file_name);
	else
		pg_log(PG_REPORT,
			   "Optimizer statistics and free space information are not transferred\n"
		"by pg_upgrade so, once you start the new server, consider running:\n"
			   "    %s\n\n", analyze_script_file_name);


	if (deletion_script_file_name)
		pg_log(PG_REPORT,
			"Running this script will delete the old cluster's data files:\n"
			   "    %s\n",
			   deletion_script_file_name);
	else
		pg_log(PG_REPORT,
		  "Could not create a script to delete the old cluster's data files\n"
		  "because user-defined tablespaces or the new cluster's data directory\n"
		  "exist in the old cluster directory.  The old cluster's contents must\n"
		  "be deleted manually.\n");
}


void
check_cluster_versions(void)
{
	prep_status("Checking cluster versions");

	/* get old cluster versions */
	old_cluster.major_version = get_major_server_version(&old_cluster);

	/*
	 * Upgrading from anything older than an 8.3 based Greenplum (GPDB5) is not supported.
	 */

	if (GET_MAJOR_VERSION(old_cluster.major_version) < 803)
		pg_fatal("This utility can only upgrade from Greenplum version 5 and later.\n");

	/* get old and new binary versions */
	get_bin_version(&old_cluster);

	/* Ensure binaries match the designated data directories */
	if (GET_MAJOR_VERSION(old_cluster.major_version) !=
		GET_MAJOR_VERSION(old_cluster.bin_version))
		pg_fatal("Old cluster data and binary directories are from different major versions.\n");

	if(is_skip_target_check())
	{
		check_ok();
		return;
	}

	/* get new cluster versions */
	new_cluster.major_version = get_major_server_version(&new_cluster);

	/* Only current PG version is supported as a target */
	if (GET_MAJOR_VERSION(new_cluster.major_version) != GET_MAJOR_VERSION(PG_VERSION_NUM))
		pg_fatal("This utility can only upgrade to Greengage version %s.\n",
				 PG_MAJORVERSION);

	/*
	 * We can't allow downgrading because we use the target pg_dump, and
	 * pg_dump cannot operate on newer database versions, only current and
	 * older versions.
	 */
	if (old_cluster.major_version > new_cluster.major_version)
		pg_fatal("This utility cannot be used to downgrade to older major Greengage versions.\n");

	/* new binary versions */
	get_bin_version(&new_cluster);

	/* Ensure binaries match the designated data directories */
	if (GET_MAJOR_VERSION(new_cluster.major_version) !=
		GET_MAJOR_VERSION(new_cluster.bin_version))
		pg_fatal("New cluster data and binary directories are from different major versions.\n");

	check_ok();
}


void
check_cluster_compatibility(bool live_check)
{
	/* get/check pg_control data of servers */
	get_control_data(&old_cluster, live_check);

	if(!is_skip_target_check())
	{
		get_control_data(&new_cluster, false);
		check_control_data(&old_cluster.controldata, &new_cluster.controldata);
	}

	/* Is it 9.0 but without tablespace directories? */
	if(!is_skip_target_check())
		if (GET_MAJOR_VERSION(new_cluster.major_version) == 900 &&
			new_cluster.controldata.cat_ver < TABLE_SPACE_SUBDIRS_CAT_VER)
			pg_fatal("This utility can only upgrade to PostgreSQL version 9.0 after 2010-01-11\n"
					 "because of backend API changes made during development.\n");

	/* We read the real port number for PG >= 9.1 */
	if (live_check && GET_MAJOR_VERSION(old_cluster.major_version) < 901 &&
		old_cluster.port == DEF_PGUPORT)
		pg_fatal("When checking a pre-PG 9.1 live old server, "
				 "you must specify the old server's port number.\n");

	if(!is_skip_target_check())
		if (live_check && old_cluster.port == new_cluster.port)
			pg_fatal("When checking a live server, "
					 "the old and new port numbers must be different.\n");
}


/*
 * set_locale_and_encoding()
 *
 * query the database to get the template0 locale
 */
static void
set_locale_and_encoding(ClusterInfo *cluster)
{
	ControlData *ctrl = &cluster->controldata;
	PGconn	   *conn;
	PGresult   *res;
	int			i_encoding;
	int			cluster_version = cluster->major_version;

	conn = connectToServer(cluster, "template1");

	/* for pg < 80400, we got the values from pg_controldata */
	if (cluster_version >= 80400)
	{
		int			i_datcollate;
		int			i_datctype;

		res = executeQueryOrDie(conn,
								"SELECT datcollate, datctype "
								"FROM	pg_catalog.pg_database "
								"WHERE	datname = 'template0' ");
		assert(PQntuples(res) == 1);

		i_datcollate = PQfnumber(res, "datcollate");
		i_datctype = PQfnumber(res, "datctype");

		ctrl->lc_collate = pg_strdup(PQgetvalue(res, 0, i_datcollate));
		ctrl->lc_ctype = pg_strdup(PQgetvalue(res, 0, i_datctype));

		PQclear(res);
	}

	res = executeQueryOrDie(conn,
							"SELECT pg_catalog.pg_encoding_to_char(encoding) "
							"FROM	pg_catalog.pg_database "
							"WHERE	datname = 'template0' ");
	assert(PQntuples(res) == 1);

	i_encoding = PQfnumber(res, "pg_encoding_to_char");
	ctrl->encoding = pg_strdup(PQgetvalue(res, 0, i_encoding));

	PQclear(res);

	PQfinish(conn);
}


/*
 * check_locale_and_encoding()
 *
 * Check that old and new locale and encoding match.  Even though the backend
 * tries to canonicalize stored locale names, the platform often doesn't
 * cooperate, so it's entirely possible that one DB thinks its locale is
 * "en_US.UTF-8" while the other says "en_US.utf8".  Try to be forgiving.
 */
static void
check_locale_and_encoding(ControlData *oldctrl,
						  ControlData *newctrl)
{
	if (!equivalent_locale(LC_COLLATE, oldctrl->lc_collate, newctrl->lc_collate))
		gp_fatal_log("| lc_collate cluster values do not match:  old \"%s\", new \"%s\"\n",
				 oldctrl->lc_collate, newctrl->lc_collate);
	if (!equivalent_locale(LC_CTYPE, oldctrl->lc_ctype, newctrl->lc_ctype))
		gp_fatal_log("| lc_ctype cluster values do not match:  old \"%s\", new \"%s\"\n",
				 oldctrl->lc_ctype, newctrl->lc_ctype);
	if (!equivalent_encoding(oldctrl->encoding, newctrl->encoding))
		gp_fatal_log("| encoding cluster values do not match:  old \"%s\", new \"%s\"\n",
				 oldctrl->encoding, newctrl->encoding);
}

/*
 * equivalent_locale()
 *
 * Best effort locale-name comparison.  Return false if we are not 100% sure
 * the locales are equivalent.
 *
 * Note: The encoding parts of the names are ignored. This function is
 * currently used to compare locale names stored in pg_database, and
 * pg_database contains a separate encoding field. That's compared directly
 * in check_locale_and_encoding().
 */
static bool
equivalent_locale(int category, const char *loca, const char *locb)
{
	const char *chara;
	const char *charb;
	char	   *canona;
	char	   *canonb;
	int			lena;
	int			lenb;

	/*
	 * If the names are equal, the locales are equivalent. Checking this
	 * first avoids calling setlocale() in the common case that the names
	 * are equal. That's a good thing, if setlocale() is buggy, for example.
	 */
	if (pg_strcasecmp(loca, locb) == 0)
		return true;

	/*
	 * Not identical. Canonicalize both names, remove the encoding parts,
	 * and try again.
	 */
	canona = get_canonical_locale_name(category, loca);
	chara = strrchr(canona, '.');
	lena = chara ? (chara - canona) : strlen(canona);

	canonb = get_canonical_locale_name(category, locb);
	charb = strrchr(canonb, '.');
	lenb = charb ? (charb - canonb) : strlen(canonb);

	if (lena == lenb && pg_strncasecmp(canona, canonb, lena) == 0)
	{
		pg_free(canona);
		pg_free(canonb);
		return true;
	}

	pg_free(canona);
	pg_free(canonb);
	return false;
}

/*
 * equivalent_encoding()
 *
 * Best effort encoding-name comparison.  Return true only if the encodings
 * are valid server-side encodings and known equivalent.
 *
 * Because the lookup in pg_valid_server_encoding() does case folding and
 * ignores non-alphanumeric characters, this will recognize many popular
 * variant spellings as equivalent, eg "utf8" and "UTF-8" will match.
 */
static bool
equivalent_encoding(const char *chara, const char *charb)
{
	int			enca = pg_valid_server_encoding(chara);
	int			encb = pg_valid_server_encoding(charb);

	if (enca < 0 || encb < 0)
		return false;

	return (enca == encb);
}


static void
check_new_cluster_is_empty(void)
{
	int			dbnum;

	/*
	 * If we are upgrading a segment we expect to have a complete datadir in
	 * place from the QD at this point, so the cluster cannot be tested for
	 * being empty.
	 */
	if (!is_greengage_dispatcher_mode())
		return;

	for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++)
	{
		int			relnum;
		RelInfoArr *rel_arr = &new_cluster.dbarr.dbs[dbnum].rel_arr;

		for (relnum = 0; relnum < rel_arr->nrels;
			 relnum++)
		{
			/* pg_largeobject and its index should be skipped */
			if (strcmp(rel_arr->rels[relnum].nspname, "pg_catalog") != 0)
				gp_fatal_log("| New cluster database \"%s\" is not empty\n",
						 new_cluster.dbarr.dbs[dbnum].db_name);
		}
	}

}


/*
 * create_script_for_cluster_analyze()
 *
 *	This incrementally generates better optimizer statistics
 */
void
create_script_for_cluster_analyze(char **analyze_script_file_name)
{
	FILE	   *script = NULL;
	PQExpBufferData user_specification;

	prep_status("Creating script to analyze new cluster");

	initPQExpBuffer(&user_specification);
	if (os_info.user_specified)
	{
		appendPQExpBufferStr(&user_specification, "-U ");
		appendShellString(&user_specification, os_info.user);
		appendPQExpBufferChar(&user_specification, ' ');
	}

	*analyze_script_file_name = psprintf("analyze_new_cluster.%s", SCRIPT_EXT);

	if ((script = fopen_priv(*analyze_script_file_name, "w")) == NULL)
		pg_fatal("Could not open file \"%s\": %s\n",
				 *analyze_script_file_name, getErrorText());

#ifndef WIN32
	/* add shebang header */
	fprintf(script, "#!/bin/sh\n\n");
#else
	/* suppress command echoing */
	fprintf(script, "@echo off\n");
#endif

	fprintf(script, "echo %sThis script will generate minimal optimizer statistics rapidly%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo %sso your system is usable, and then gather statistics twice more%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo %swith increasing accuracy.  When it is done, your system will%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo %shave the default level of optimizer statistics.%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo%s\n\n", ECHO_BLANK);

	fprintf(script, "echo %sIf you have used ALTER TABLE to modify the statistics target for%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo %sany tables, you might want to remove them and restore them after%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo %srunning this script because they will delay fast statistics generation.%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo%s\n\n", ECHO_BLANK);

	fprintf(script, "echo %sIf you would like default statistics as quickly as possible, cancel%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo %sthis script and run:%s\n",
			ECHO_QUOTE, ECHO_QUOTE);
	fprintf(script, "echo %s    \"%s/vacuumdb\" %s--all %s%s\n", ECHO_QUOTE,
			new_cluster.bindir, user_specification.data,
	/* Did we copy the free space files? */
			(GET_MAJOR_VERSION(old_cluster.major_version) >= 804) ?
			"--analyze-only" : "--analyze", ECHO_QUOTE);
	fprintf(script, "echo%s\n\n", ECHO_BLANK);

	fprintf(script, "\"%s/vacuumdb\" %s--all --analyze-in-stages\n",
			new_cluster.bindir, user_specification.data);
	/* Did we copy the free space files? */
	if (GET_MAJOR_VERSION(old_cluster.major_version) < 804)
		fprintf(script, "\"%s/vacuumdb\" %s--all\n", new_cluster.bindir,
				user_specification.data);

	fprintf(script, "echo%s\n\n", ECHO_BLANK);
	fprintf(script, "echo %sDone%s\n",
			ECHO_QUOTE, ECHO_QUOTE);

	fclose(script);

#ifndef WIN32
	if (chmod(*analyze_script_file_name, S_IRWXU) != 0)
		pg_fatal("Could not add execute permission to file \"%s\": %s\n",
				 *analyze_script_file_name, getErrorText());
#endif

	termPQExpBuffer(&user_specification);

	check_ok();
}


static void
check_proper_datallowconn(ClusterInfo *cluster)
{
	int			dbnum;
	PGconn	   *conn_template1;
	PGresult   *dbres;
	int			ntups;
	int			i_datname;
	int			i_datallowconn;

	prep_status("Checking database connection settings");

	conn_template1 = connectToServer(cluster, "template1");

	/* get database names */
	dbres = executeQueryOrDie(conn_template1,
							  "SELECT	datname, datallowconn "
							  "FROM	pg_catalog.pg_database");

	i_datname = PQfnumber(dbres, "datname");
	i_datallowconn = PQfnumber(dbres, "datallowconn");

	ntups = PQntuples(dbres);
	for (dbnum = 0; dbnum < ntups; dbnum++)
	{
		char	   *datname = PQgetvalue(dbres, dbnum, i_datname);
		char	   *datallowconn = PQgetvalue(dbres, dbnum, i_datallowconn);

		if (strcmp(datname, "template0") == 0)
		{
			/* avoid restore failure when pg_dumpall tries to create template0 */
			if (strcmp(datallowconn, "t") == 0)
				pg_fatal("template0 must not allow connections, "
						 "i.e. its pg_database.datallowconn must be false\n");
		}
		else
		{
			/* avoid datallowconn == false databases from being skipped on restore */
			if (strcmp(datallowconn, "f") == 0)
				pg_fatal("All non-template0 databases must allow connections, "
						 "i.e. their pg_database.datallowconn must be true\n");
		}
	}

	PQclear(dbres);

	PQfinish(conn_template1);

	check_ok();
}

/*
 * create_script_for_old_cluster_deletion()
 *
 *	This is particularly useful for tablespace deletion.
 */
void
create_script_for_old_cluster_deletion(char **deletion_script_file_name)
{
	FILE	   *script = NULL;
	int			tblnum;
	char		old_cluster_pgdata[MAXPGPATH], new_cluster_pgdata[MAXPGPATH];

	*deletion_script_file_name = psprintf("delete_old_cluster.%s", SCRIPT_EXT);

	strlcpy(old_cluster_pgdata, old_cluster.pgdata, MAXPGPATH);
	canonicalize_path(old_cluster_pgdata);

	strlcpy(new_cluster_pgdata, new_cluster.pgdata, MAXPGPATH);
	canonicalize_path(new_cluster_pgdata);

	/* Some people put the new data directory inside the old one. */
	if (path_is_prefix_of_path(old_cluster_pgdata, new_cluster_pgdata))
	{
		pg_log(PG_WARNING,
		   "\nWARNING:  new data directory should not be inside the old data directory, e.g. %s\n", old_cluster_pgdata);

		/* Unlink file in case it is left over from a previous run. */
		unlink(*deletion_script_file_name);
		pg_free(*deletion_script_file_name);
		*deletion_script_file_name = NULL;
		return;
	}

	/*
	 * Some users (oddly) create tablespaces inside the cluster data
	 * directory.  We can't create a proper old cluster delete script in that
	 * case.
	 */
	for (tblnum = 0; tblnum < os_info.num_old_tablespaces; tblnum++)
	{
		char		old_tablespace_dir[MAXPGPATH];

		strlcpy(old_tablespace_dir, os_info.old_tablespaces[tblnum], MAXPGPATH);
		canonicalize_path(old_tablespace_dir);
		if (path_is_prefix_of_path(old_cluster_pgdata, old_tablespace_dir))
		{
			/* Unlink file in case it is left over from a previous run. */
			unlink(*deletion_script_file_name);
			pg_free(*deletion_script_file_name);
			*deletion_script_file_name = NULL;
			return;
		}
	}

	prep_status("Creating script to delete old cluster");

	if ((script = fopen_priv(*deletion_script_file_name, "w")) == NULL)
		pg_fatal("Could not open file \"%s\": %s\n",
				 *deletion_script_file_name, getErrorText());

#ifndef WIN32
	/* add shebang header */
	fprintf(script, "#!/bin/sh\n\n");
#endif

	/* delete old cluster's default tablespace */
	fprintf(script, RMDIR_CMD " \"%s\"\n", fix_path_separator(old_cluster.pgdata));

	/* delete old cluster's alternate tablespaces */
	for (tblnum = 0; tblnum < os_info.num_old_tablespaces; tblnum++)
	{
		/*
		 * Do the old cluster's per-database directories share a directory
		 * with a new version-specific tablespace?
		 */
		if (strlen(old_cluster.tablespace_suffix) == 0)
		{
			/* delete per-database directories */
			int			dbnum;

			fprintf(script, "\n");
			/* remove PG_VERSION? */
			if (GET_MAJOR_VERSION(old_cluster.major_version) <= 804)
				fprintf(script, RM_CMD " %s%cPG_VERSION\n",
						fix_path_separator(os_info.old_tablespaces[tblnum]),
						PATH_SEPARATOR);

			for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
				fprintf(script, RMDIR_CMD " \"%s%c%d\"\n",
						fix_path_separator(os_info.old_tablespaces[tblnum]),
						PATH_SEPARATOR, old_cluster.dbarr.dbs[dbnum].db_oid);
		}
		else
		{
			char	   *suffix_path = pg_strdup(old_cluster.tablespace_suffix);

			/*
			 * Simply delete the tablespace directory, which might be ".old"
			 * or a version-specific subdirectory.
			 */
			fprintf(script, RMDIR_CMD " \"%s%s\"\n",
					fix_path_separator(os_info.old_tablespaces[tblnum]),
					fix_path_separator(suffix_path));
			pfree(suffix_path);
		}
	}

	fclose(script);

#ifndef WIN32
	if (chmod(*deletion_script_file_name, S_IRWXU) != 0)
		pg_fatal("Could not add execute permission to file \"%s\": %s\n",
				 *deletion_script_file_name, getErrorText());
#endif

	check_ok();
}


/*
 *	check_is_super_user()
 *
 *	Check we are superuser, and out user id and user count
 */
static void
check_is_super_user(ClusterInfo *cluster)
{
	PGresult   *res;
	PGconn	   *conn = connectToServer(cluster, "template1");

	prep_status("Checking database user is a superuser");

	/* Can't use pg_authid because only superusers can view it. */
	res = executeQueryOrDie(conn,
							"SELECT rolsuper, oid "
							"FROM pg_catalog.pg_roles "
							"WHERE rolname = current_user");

	if (PQntuples(res) != 1 || strcmp(PQgetvalue(res, 0, 0), "t") != 0)
		pg_fatal("database user \"%s\" is not a superuser\n",
				 os_info.user);

	cluster->install_role_oid = atooid(PQgetvalue(res, 0, 1));

	PQclear(res);

	res = executeQueryOrDie(conn,
							"SELECT COUNT(*) "
							"FROM pg_catalog.pg_roles ");

	if (PQntuples(res) != 1)
		pg_fatal("could not determine the number of users\n");

	cluster->role_count = atoi(PQgetvalue(res, 0, 0));

	PQclear(res);

	PQfinish(conn);

	check_ok();
}


/*
 *	check_for_prepared_transactions()
 *
 *	Make sure there are no prepared transactions because the storage format
 *	might have changed.
 */
static void
check_for_prepared_transactions(ClusterInfo *cluster)
{
	PGresult   *res;
	PGconn	   *conn = connectToServer(cluster, "template1");
	bool 			 found = false;

	prep_status("Checking for prepared transactions");

	res = executeQueryOrDie(conn,
							"SELECT * "
							"FROM pg_catalog.pg_prepared_xacts");

	if (PQntuples(res) != 0)
	{
		found = true;
		pg_log(PG_REPORT, "fatal\n");
		gp_fatal_log("| The %s cluster contains prepared transactions\n",
				 CLUSTER_NAME(cluster));
	}

	PQclear(res);

	PQfinish(conn);

	if (!found)
		check_ok();
}


/*
 *	check_for_isn_and_int8_passing_mismatch()
 *
 *	contrib/isn relies on data type int8, and in 8.4 int8 can now be passed
 *	by value.  The schema dumps the CREATE TYPE PASSEDBYVALUE setting so
 *	it must match for the old and new servers.
 */
static void
check_for_isn_and_int8_passing_mismatch(ClusterInfo *cluster)
{
	int			dbnum;
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status("Checking for contrib/isn with bigint-passing mismatch");

	if (old_cluster.controldata.float8_pass_by_value ==
		new_cluster.controldata.float8_pass_by_value)
	{
		/* no mismatch */
		check_ok();
		return;
	}

	snprintf(output_path, sizeof(output_path),
			 "contrib_isn_and_int8_pass_by_value.txt");

	for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_proname;
		DbInfo	   *active_db = &cluster->dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(cluster, active_db->db_name);

		/* Find any functions coming from contrib/isn */
		res = executeQueryOrDie(conn,
								"SELECT n.nspname, p.proname "
								"FROM	pg_catalog.pg_proc p, "
								"		pg_catalog.pg_namespace n "
								"WHERE	p.pronamespace = n.oid AND "
								"		p.probin = '$libdir/isn'");

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_proname = PQfnumber(res, "proname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
				pg_fatal("Could not open file \"%s\": %s\n",
						 output_path, getErrorText());
			if (!db_used)
			{
				fprintf(script, "Database: %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_proname));
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
				"| Your installation contains \"contrib/isn\" functions which rely on the\n"
				"| bigint data type.  Your old and new clusters pass bigint values\n"
				"| differently so this cluster cannot currently be upgraded.  You can\n"
				"| manually upgrade databases that use \"contrib/isn\" facilities and remove\n"
				"| \"contrib/isn\" from the old cluster and restart the upgrade.  A list of\n"
				"| the problem functions is in the file:\n"
				"|     %s\n\n", output_path);
	}
	else
		check_ok();
}


/*
 * check_for_reg_data_type_usage()
 *	pg_upgrade only preserves these system values:
 *		pg_class.oid
 *		pg_type.oid
 *		pg_enum.oid
 *
 *	Many of the reg* data types reference system catalog info that is
 *	not preserved, and hence these data types cannot be used in user
 *	tables upgraded by pg_upgrade.
 */
static void
check_for_reg_data_type_usage(ClusterInfo *cluster)
{
	int			dbnum;
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status("Checking for reg* system OID user data types");

	snprintf(output_path, sizeof(output_path), "tables_using_reg.txt");

	for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_relname,
					i_attname;
		DbInfo	   *active_db = &cluster->dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(cluster, active_db->db_name);
		/*
		 * While several relkinds don't store any data, e.g. views, they can
		 * be used to define data types of other columns, so we check all
		 * relkinds.
		 */
		res = executeQueryOrDie(conn,
								"SELECT n.nspname, c.relname, a.attname "
								"FROM	pg_catalog.pg_class c, "
								"		pg_catalog.pg_namespace n, "
								"		pg_catalog.pg_attribute a, "
								"		pg_catalog.pg_type t "
								"WHERE	c.oid = a.attrelid AND "
								"		NOT a.attisdropped AND "
								"       a.atttypid = t.oid AND "
								"       t.typnamespace = "
								"           (SELECT oid FROM pg_namespace "
								"            WHERE nspname = 'pg_catalog') AND"
								"		t.typname IN ( "
		/* regclass.oid is preserved, so 'regclass' is OK */
								"           'regconfig', "
								"           'regdictionary', "
								"           'regnamespace', "
								"           'regoper', "
								"           'regoperator', "
								"           'regproc', "
								"           'regprocedure', "
								"           'pg_catalog.regconfig'::pg_catalog.regtype::pg_catalog.text, "
								"           'pg_catalog.regdictionary'::pg_catalog.regtype::pg_catalog.text "
								"			) AND "
								"		c.relnamespace = n.oid AND "
							  "		n.nspname NOT IN ('pg_catalog', 'information_schema')");

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_relname = PQfnumber(res, "relname");
		i_attname = PQfnumber(res, "attname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
				pg_fatal("Could not open file \"%s\": %s\n",
						 output_path, getErrorText());
			if (!db_used)
			{
				fprintf(script, "Database: %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_relname),
					PQgetvalue(res, rowno, i_attname));
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
				"| Your installation contains one of the reg* data types in user tables.\n"
				"| These data types reference system OIDs that are not preserved by\n"
				"| pg_upgrade, so this cluster cannot currently be upgraded.  You can\n"
				"| remove the problem tables and restart the upgrade.  A list of the problem\n"
				"| columns is in the file:\n"
				"|     %s\n\n", output_path);
	}
	else
		check_ok();
}


/*
 * check_for_jsonb_9_4_usage()
 *
 *	JSONB changed its storage format during 9.4 beta, so check for it.
 */
static void
check_for_jsonb_9_4_usage(ClusterInfo *cluster)
{
	int			dbnum;
	FILE	   *script = NULL;
	bool		found = false;
	char		output_path[MAXPGPATH];

	prep_status("Checking for JSONB user data types");

	snprintf(output_path, sizeof(output_path), "tables_using_jsonb.txt");

	for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
	{
		PGresult   *res;
		bool		db_used = false;
		int			ntups;
		int			rowno;
		int			i_nspname,
					i_relname,
					i_attname;
		DbInfo	   *active_db = &cluster->dbarr.dbs[dbnum];
		PGconn	   *conn = connectToServer(cluster, active_db->db_name);

		/*
		 * While several relkinds don't store any data, e.g. views, they can
		 * be used to define data types of other columns, so we check all
		 * relkinds.
		 */
		res = executeQueryOrDie(conn,
								"SELECT n.nspname, c.relname, a.attname "
								"FROM	pg_catalog.pg_class c, "
								"		pg_catalog.pg_namespace n, "
								"		pg_catalog.pg_attribute a "
								"WHERE	c.oid = a.attrelid AND "
								"		NOT a.attisdropped AND "
								"		a.atttypid = 'pg_catalog.jsonb'::pg_catalog.regtype AND "
								"		c.relnamespace = n.oid AND "
		/* exclude possible orphaned temp tables */
								"  		n.nspname !~ '^pg_temp_' AND "
							  "		n.nspname NOT IN ('pg_catalog', 'information_schema')");

		ntups = PQntuples(res);
		i_nspname = PQfnumber(res, "nspname");
		i_relname = PQfnumber(res, "relname");
		i_attname = PQfnumber(res, "attname");
		for (rowno = 0; rowno < ntups; rowno++)
		{
			found = true;
			if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
				pg_fatal("Could not open file \"%s\": %s\n",
						 output_path, getErrorText());
			if (!db_used)
			{
				fprintf(script, "Database: %s\n", active_db->db_name);
				db_used = true;
			}
			fprintf(script, "  %s.%s.%s\n",
					PQgetvalue(res, rowno, i_nspname),
					PQgetvalue(res, rowno, i_relname),
					PQgetvalue(res, rowno, i_attname));
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
				"| Your installation contains the \"jsonb\" data type in user tables.\n"
				"| The internal format of \"jsonb\" changed during 9.4 beta so this cluster cannot currently\n"
				"| be upgraded.  You can remove the problem tables and restart the upgrade.  A list\n"
				"| of the problem columns is in the file:\n"
				"|     %s\n\n", output_path);
	}
	else
		check_ok();
}


static void
get_bin_version(ClusterInfo *cluster)
{
	char		cmd[MAXPGPATH],
				cmd_output[MAX_STRING];
	FILE	   *output;
	int			pre_dot,
				post_dot;

	snprintf(cmd, sizeof(cmd), "\"%s/pg_ctl\" --version", cluster->bindir);

	if ((output = popen(cmd, "r")) == NULL ||
		fgets(cmd_output, sizeof(cmd_output), output) == NULL)
		pg_fatal("Could not get pg_ctl version data using %s: %s\n",
				 cmd, getErrorText());

	pclose(output);

	/* Remove trailing newline */
	if (strchr(cmd_output, '\n') != NULL)
		*strchr(cmd_output, '\n') = '\0';

	if (sscanf(cmd_output, "%*s %*s %*s %d.%d", &pre_dot, &post_dot) != 2)
		pg_fatal("could not get version from %s\n", cmd);

	cluster->bin_version = (pre_dot * 100 + post_dot) * 100;
}


/*
 * get_canonical_locale_name
 *
 * Send the locale name to the system, and hope we get back a canonical
 * version.  This should match the backend's check_locale() function.
 */
static char *
get_canonical_locale_name(int category, const char *locale)
{
	char	   *save;
	char	   *res;

	/* get the current setting, so we can restore it. */
	save = setlocale(category, NULL);
	if (!save)
		pg_fatal("failed to get the current locale\n");

	/* 'save' may be pointing at a modifiable scratch variable, so copy it. */
	save = (char *) pg_strdup(save);

	/* set the locale with setlocale, to see if it accepts it. */
	res = setlocale(category, locale);

	if (!res)
		pg_fatal("failed to get system locale name for \"%s\"\n", locale);

	res = pg_strdup(res);

	/* restore old value. */
	if (!setlocale(category, save))
		pg_fatal("failed to restore old locale \"%s\"\n", save);

	pg_free(save);

	return res;
}
