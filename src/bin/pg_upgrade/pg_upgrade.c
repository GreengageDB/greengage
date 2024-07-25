/*
 *	pg_upgrade.c
 *
 *	main source file
 *
 *	Portions Copyright (c) 2016-Present, VMware, Inc. or its affiliates
 *	Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/pg_upgrade.c
 */

/*
 *	To simplify the upgrade process, we force certain system values to be
 *	identical between old and new clusters:
 *
 *	We control all assignments of pg_class.oid (and relfilenode) so toast
 *	oids are the same between old and new clusters.  This is important
 *	because toast oids are stored as toast pointers in user tables.
 *
 *	While pg_class.oid and pg_class.relfilenode are initially the same
 *	in a cluster, they can diverge due to CLUSTER, REINDEX, or VACUUM
 *	FULL.  In the new cluster, pg_class.oid and pg_class.relfilenode will
 *	be the same and will match the old pg_class.oid value.  Because of
 *	this, old/new pg_class.relfilenode values will not match if CLUSTER,
 *	REINDEX, or VACUUM FULL have been performed in the old cluster.
 *
 *	We control all assignments of pg_type.oid because these oids are stored
 *	in user composite type values.
 *
 *	We control all assignments of pg_enum.oid because these oids are stored
 *	in user tables as enum values.
 *
 *	We control all assignments of pg_authid.oid for historical reasons (the
 *	oids used to be stored in pg_largeobject_metadata, which is now copied via
 *	SQL commands), that might change at some point in the future.
 */



#include "postgres_fe.h"

#include <time.h>

#include "catalog/pg_class_d.h"
#include "common/file_perm.h"
#include "common/logging.h"
#include "common/restricted_token.h"
#include "fe_utils/string_utils.h"

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif

#include "greenplum/pg_upgrade_greenplum.h"

static void prepare_new_cluster(void);
static void prepare_new_globals(void);
static void create_new_objects(void);
static void copy_xact_xlog_xid(void);
static void set_frozenxids(bool minmxid_only);
static void make_outputdirs(char *pgdata);
static void setup(char *argv0, bool *live_check);

static void copy_subdir_files(const char *old_subdir, const char *new_subdir);

#ifdef WIN32
static int	CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo, const char *progname);
#endif

ClusterInfo old_cluster,
			new_cluster;
OSInfo		os_info;

char	   *output_files[] = {
	SERVER_LOG_FILE,
#ifdef WIN32
	/* unique file for pg_ctl start */
	SERVER_START_LOG_FILE,
#endif
	UTILITY_LOG_FILE,
	INTERNAL_LOG_FILE,
	NULL
};

#ifdef WIN32
static char *restrict_env;
#endif

/* This is the database used by pg_dumpall to restore global tables */
#define GLOBAL_DUMP_DB	"postgres"

ClusterInfo old_cluster,
			new_cluster;
OSInfo		os_info;

int
main(int argc, char **argv)
{
	char       *sequence_script_file_name = NULL;
	char	   *analyze_script_file_name = NULL;
	char	   *deletion_script_file_name = NULL;
	char	   *output_dir = NULL;
	bool		live_check = false;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("pg_upgrade"));

	/* Set default restrictive mask until new cluster permissions are read */
	umask(PG_MODE_MASK_OWNER);

	parseCommandLine(argc, argv);

	get_restricted_token();

	adjust_data_dir(&old_cluster);

	if(!is_skip_target_check())
		adjust_data_dir(&new_cluster);

	/*
	 * Set mask based on PGDATA permissions, needed for the creation of the
	 * output directories with correct permissions.
	 */
	if (!GetDataDirectoryCreatePerm(new_cluster.pgdata))
		pg_fatal("could not read permissions of directory \"%s\": %s\n",
				 new_cluster.pgdata, strerror(errno));

	umask(pg_mode_mask);

	/*
	 * This needs to happen after adjusting the data directory of the new
	 * cluster in adjust_data_dir().
	 *
	 * GPDB allows for relocateable output with the --output-dir flag
	 *
	 * Use make_outputdirs() for the default option; this ensures that there is a
	 * unique directory for pg_upgrade on the data directory. If not,
	 * pg_upgrade will fail immediately. The default option will create the directory
	 * `<data-directory>/pg_upgrade_output.d/<timestamp>` for pg_upgrade. Otherwise, use
	 * make_outputdirs_gp() when the user knows the exact directory to put the
	 * files and logs that pg_upgrade generates.
	 */
	if ((output_dir = get_output_dir()) != NULL)
		make_outputdirs_gp(output_dir);
	else
		make_outputdirs(new_cluster.pgdata);

	setup(argv[0], &live_check);

	report_progress(NULL, CHECK, "Checking cluster compatibility");
	output_check_banner(live_check);

	check_cluster_versions();

	get_sock_dir(&old_cluster, live_check);

	if(!is_skip_target_check())
		get_sock_dir(&new_cluster, false);

	/* not skipped for is_skip_target_check because of some checks on
	 * old_cluster are done independently of new_cluster
	 */
	check_cluster_compatibility(live_check);

	check_and_dump_old_cluster(live_check, &sequence_script_file_name);

	/* -- NEW -- */

	if(!is_skip_target_check() || !skip_checks())
	{
		start_postmaster(&new_cluster, true);
		check_new_cluster();
	}

	report_clusters_compatible();

	pg_log(PG_REPORT,
		   "\n"
		   "Performing Upgrade\n"
		   "------------------\n");

	prepare_new_cluster();

	stop_postmaster(false);

	/*
	 * Destructive Changes to New Cluster
	 */

	copy_xact_xlog_xid();

	/*
	 * GPDB: This used to be right before syncing the data directory to disk
	 * but is needed here before create_new_objects() due to our usage of a
	 * preserved oid list. When creating new objects on the target cluster,
	 * objects that do not have a preassigned oid will try to get a new oid
	 * from the oid counter. This works in upstream Postgres but can be slow
	 * in GPDB because the new oid is checked against the preserved oid
	 * list. If the new oid is in the preserved oid list, a new oid is
	 * generated from the oid counter until a valid oid is found. In
	 * production scenarios, it would be very common to have a very, very
	 * large preserved oid list and starting the oid counter from
	 * FirstNormalObjectId (16384) would make object creation slower than
	 * usual near the beginning of pg_restore. To prevent pg_restore
	 * performance degradation from so many invalid new oids from the oid
	 * counter, bump the oid counter to what the source cluster has via
	 * pg_resetwal. If the preserved oid list logic is removed from
	 * pg_upgrade, move this step back to where it was before.
	 */
	prep_status("Setting next OID for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" --binary-upgrade -o %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtoid,
			  new_cluster.pgdata);
	check_ok();

	/*
	 * GPDB_UPGRADE_FIXME: Copy the pg_distributedlog over in vanilla.
	 * The assumption that this works needs to be verified
	 */
	copy_subdir_files("pg_distributedlog", "pg_distributedlog");

	/* New now using xids of the old system */

	/* -- NEW -- */
	start_postmaster(&new_cluster, true);

	if (is_greenplum_dispatcher_mode())
	{
		prepare_new_globals();

		create_new_objects();
	}

	/*
	 * In a segment, the data directory already contains all the objects,
	 * because the segment is initialized by taking a physical copy of the
	 * upgraded QD data directory. The auxiliary AO tables - containing
	 * information about the segment files, are different in each server,
	 * however. So we still need to restore those separately on each
	 * server.
	 */
	restore_aosegment_tables();

	if (is_greenplum_dispatcher_mode())
	{
		/* freeze master data *right before* stopping */
		freeze_master_data();
	}

	stop_postmaster(false);

	/*
	 * Most failures happen in create_new_objects(), which has completed at
	 * this point.  We do this here because it is just before linking, which
	 * will link the old and new cluster data files, preventing the old
	 * cluster from being safely started once the new cluster is started.
	 */
	if (user_opts.transfer_mode == TRANSFER_MODE_LINK)
		disable_old_cluster();

	transfer_all_new_tablespaces(&old_cluster.dbarr, &new_cluster.dbarr,
								 old_cluster.pgdata, new_cluster.pgdata);

	/* For non-master segments, uniquify the system identifier. */
	if (!is_greenplum_dispatcher_mode())
		reset_system_identifier();

	prep_status("Sync data directory to disk");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/initdb\" --sync-only \"%s\"", new_cluster.bindir,
			  new_cluster.pgdata);
	check_ok();

	create_script_for_cluster_analyze(&analyze_script_file_name);
	create_script_for_old_cluster_deletion(&deletion_script_file_name);

	issue_warnings_and_set_wal_level(sequence_script_file_name);

	pg_log(PG_REPORT,
		   "\n"
		   "Upgrade Complete\n"
		   "----------------\n");

	report_progress(NULL, DONE, "Upgrade complete");
	close_progress();

	output_completion_banner(analyze_script_file_name,
							 deletion_script_file_name);

	pg_free(analyze_script_file_name);
	pg_free(deletion_script_file_name);

	cleanup_output_dirs();

	return 0;
}

#ifdef WIN32
typedef BOOL(WINAPI * __CreateRestrictedToken) (HANDLE, DWORD, DWORD, PSID_AND_ATTRIBUTES, DWORD, PLUID_AND_ATTRIBUTES, DWORD, PSID_AND_ATTRIBUTES, PHANDLE);

/* Windows API define missing from some versions of MingW headers */
#ifndef  DISABLE_MAX_PRIVILEGE
#define DISABLE_MAX_PRIVILEGE	0x1
#endif

/*
* Create a restricted token and execute the specified process with it.
*
* Returns 0 on failure, non-zero on success, same as CreateProcess().
*
* On NT4, or any other system not containing the required functions, will
* NOT execute anything.
*/
static int
CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo, const char *progname)
{
	BOOL		b;
	STARTUPINFO si;
	HANDLE		origToken;
	HANDLE		restrictedToken;
	SID_IDENTIFIER_AUTHORITY NtAuthority = { SECURITY_NT_AUTHORITY };
	SID_AND_ATTRIBUTES dropSids[2];
	__CreateRestrictedToken _CreateRestrictedToken = NULL;
	HANDLE		Advapi32Handle;

	ZeroMemory(&si, sizeof(si));
	si.cb = sizeof(si);

	Advapi32Handle = LoadLibrary("ADVAPI32.DLL");
	if (Advapi32Handle != NULL)
	{
		_CreateRestrictedToken = (__CreateRestrictedToken)GetProcAddress(Advapi32Handle, "CreateRestrictedToken");
	}

	if (_CreateRestrictedToken == NULL)
	{
		fprintf(stderr, _("%s: WARNING: cannot create restricted tokens on this platform\n"), progname);
		if (Advapi32Handle != NULL)
			FreeLibrary(Advapi32Handle);
		return 0;
	}

	/* Open the current token to use as a base for the restricted one */
	if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ALL_ACCESS, &origToken))
	{
		fprintf(stderr, _("%s: could not open process token: error code %lu\n"), progname, GetLastError());
		return 0;
	}

	/* Allocate list of SIDs to remove */
	ZeroMemory(&dropSids, sizeof(dropSids));
	if (!AllocateAndInitializeSid(&NtAuthority, 2,
		SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS, 0, 0, 0, 0, 0,
		0, &dropSids[0].Sid) ||
		!AllocateAndInitializeSid(&NtAuthority, 2,
		SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_POWER_USERS, 0, 0, 0, 0, 0,
		0, &dropSids[1].Sid))
	{
		fprintf(stderr, _("%s: could not allocate SIDs: error code %lu\n"), progname, GetLastError());
		return 0;
	}

	b = _CreateRestrictedToken(origToken,
						DISABLE_MAX_PRIVILEGE,
						sizeof(dropSids) / sizeof(dropSids[0]),
						dropSids,
						0, NULL,
						0, NULL,
						&restrictedToken);

	FreeSid(dropSids[1].Sid);
	FreeSid(dropSids[0].Sid);
	CloseHandle(origToken);
	FreeLibrary(Advapi32Handle);

	if (!b)
	{
		fprintf(stderr, _("%s: could not create restricted token: error code %lu\n"), progname, GetLastError());
		return 0;
	}

#ifndef __CYGWIN__
	AddUserToTokenDacl(restrictedToken);
#endif

	if (!CreateProcessAsUser(restrictedToken,
							NULL,
							cmd,
							NULL,
							NULL,
							TRUE,
							CREATE_SUSPENDED,
							NULL,
							NULL,
							&si,
							processInfo))

	{
		fprintf(stderr, _("%s: could not start process for command \"%s\": error code %lu\n"), progname, cmd, GetLastError());
		return 0;
	}

	return ResumeThread(processInfo->hThread);
}
#endif

/*
 * Create and assign proper permissions to the set of output directories
 * used to store any data generated internally, filling in log_opts in
 * the process.
 */
static void
make_outputdirs(char *pgdata)
{
	FILE	   *fp;
	char	  **filename;
	time_t		run_time = time(NULL);
	char		filename_path[MAXPGPATH];
	char		timebuf[128];
	struct timeval time;
	time_t		tt;
	int			len;

	log_opts.rootdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.rootdir, MAXPGPATH, "%s/%s", pgdata, BASE_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long\n");

	/* BASE_OUTPUTDIR/$timestamp/ */
	gettimeofday(&time, NULL);
	tt = (time_t) time.tv_sec;
	strftime(timebuf, sizeof(timebuf), "%Y%m%dT%H%M%S", localtime(&tt));
	/* append milliseconds */
	snprintf(timebuf + strlen(timebuf), sizeof(timebuf) - strlen(timebuf),
			 ".%03d", (int) (time.tv_usec / 1000));
	log_opts.basedir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.basedir, MAXPGPATH, "%s/%s", log_opts.rootdir,
				   timebuf);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long\n");

	/* BASE_OUTPUTDIR/$timestamp/dump/ */
	log_opts.dumpdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.dumpdir, MAXPGPATH, "%s/%s/%s", log_opts.rootdir,
				   timebuf, DUMP_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long\n");

	/* BASE_OUTPUTDIR/$timestamp/log/ */
	log_opts.logdir = (char *) pg_malloc0(MAXPGPATH);
	len = snprintf(log_opts.logdir, MAXPGPATH, "%s/%s/%s", log_opts.rootdir,
				   timebuf, LOG_OUTPUTDIR);
	if (len >= MAXPGPATH)
		pg_fatal("directory path for new cluster is too long\n");

	/*
	 * Ignore the error case where the root path exists, as it is kept the
	 * same across runs.
	 */
	if (mkdir(log_opts.rootdir, pg_dir_create_mode) < 0 && errno != EEXIST)
		pg_fatal("could not create directory \"%s\": %m\n", log_opts.rootdir);
	if (mkdir(log_opts.basedir, pg_dir_create_mode) < 0)
		pg_fatal("could not create directory \"%s\": %m\n", log_opts.basedir);
	if (mkdir(log_opts.dumpdir, pg_dir_create_mode) < 0)
		pg_fatal("could not create directory \"%s\": %m\n", log_opts.dumpdir);
	if (mkdir(log_opts.logdir, pg_dir_create_mode) < 0)
		pg_fatal("could not create directory \"%s\": %m\n", log_opts.logdir);

	len = snprintf(filename_path, sizeof(filename_path), "%s/%s",
				   log_opts.logdir, INTERNAL_LOG_FILE);
	if (len >= sizeof(filename_path))
		pg_fatal("directory path for new cluster is too long\n");

	if ((log_opts.internal = fopen_priv(filename_path, "a")) == NULL)
		pg_fatal("could not open log file \"%s\": %m\n", filename_path);

	/* label start of upgrade in logfiles */
	for (filename = output_files; *filename != NULL; filename++)
	{
		len = snprintf(filename_path, sizeof(filename_path), "%s/%s",
					   log_opts.logdir, *filename);
		if (len >= sizeof(filename_path))
			pg_fatal("directory path for new cluster is too long\n");
		if ((fp = fopen_priv(filename_path, "a")) == NULL)
			pg_fatal("could not write to log file \"%s\": %m\n", filename_path);

		fprintf(fp,
				"-----------------------------------------------------------------\n"
				"  pg_upgrade run on %s"
				"-----------------------------------------------------------------\n\n",
				ctime(&run_time));
		fclose(fp);
	}
}


static void
setup(char *argv0, bool *live_check)
{
	/*
	 * make sure the user has a clean environment, otherwise, we may confuse
	 * libpq when we connect to one (or both) of the servers.
	 */
	check_pghost_envvar();

	/*
	 * In case the user hasn't specified the directory for the new binaries
	 * with -B, default to using the path of the currently executed pg_upgrade
	 * binary.
	 */
	if (!new_cluster.bindir)
	{
		char		exec_path[MAXPGPATH];

		if (find_my_exec(argv0, exec_path) < 0)
			pg_fatal("%s: could not find own program executable\n", argv0);
		/* Trim off program name and keep just path */
		*last_dir_separator(exec_path) = '\0';
		canonicalize_path(exec_path);
		new_cluster.bindir = pg_strdup(exec_path);
	}

	verify_directories();

	/* no postmasters should be running, except for a live check */
	if (pid_lock_file_exists(old_cluster.pgdata))
	{
		/*
		 * If we have a postmaster.pid file, try to start the server.  If it
		 * starts, the pid file was stale, so stop the server.  If it doesn't
		 * start, assume the server is running.  If the pid file is left over
		 * from a server crash, this also allows any committed transactions
		 * stored in the WAL to be replayed so they are not lost, because WAL
		 * files are not transferred from old to new servers.  We later check
		 * for a clean shutdown.
		 */
		if (start_postmaster(&old_cluster, false))
			stop_postmaster(false);
		else
		{
			if (!user_opts.check)
				pg_fatal("There seems to be a postmaster servicing the old cluster.\n"
						 "Please shutdown that postmaster and try again.\n");
			else
				*live_check = true;
		}
	}

	/* same goes for the new postmaster */
	if (!is_skip_target_check())
	{
		if (pid_lock_file_exists(new_cluster.pgdata))
		{
			if (start_postmaster(&new_cluster, false))
				stop_postmaster(false);
			else
				pg_fatal("There seems to be a postmaster servicing the new cluster.\n"
						 "Please shutdown that postmaster and try again.\n");
		}
	}
}


static void
prepare_new_cluster(void)
{
	/*
	 * It would make more sense to freeze after loading the schema, but that
	 * would cause us to lose the frozenids restored by the load. We use
	 * --analyze so autovacuum doesn't update statistics later
	 *
	 * GPDB: after we've copied the master data directory to the segments,
	 * AO tables can't be analyzed because their aoseg tuple counts don't match
	 * those on disk. We therefore skip this step for segments.
	 */
	if (is_greenplum_dispatcher_mode())
	{
		prep_status("Analyzing all rows in the new cluster");
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "%s \"%s/vacuumdb\" %s --all --analyze %s",
				  PG_OPTIONS_UTILITY_MODE_VERSION(new_cluster.major_version),
				  new_cluster.bindir, cluster_conn_opts(&new_cluster),
				  log_opts.verbose ? "--verbose" : "");
		check_ok();
	}


	/*
	 * We do freeze after analyze so pg_statistic is also frozen. template0 is
	 * not frozen here, but data rows were frozen by initdb, and we set its
	 * datfrozenxid, relfrozenxids, and relminmxid later to match the new xid
	 * counter later.
	 */
	prep_status("Freezing all rows in the new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "%s \"%s/vacuumdb\" %s --all --freeze %s",
			  PG_OPTIONS_UTILITY_MODE_VERSION(new_cluster.major_version),
			  new_cluster.bindir, cluster_conn_opts(&new_cluster),
			  log_opts.verbose ? "--verbose" : "");
	check_ok();
}


static void
prepare_new_globals(void)
{
	/*
	 * Before we restore anything, set frozenxids of initdb-created tables.
	 */
	set_frozenxids(false);

	/*
	 * Now restore global objects (roles and tablespaces).
	 */
	prep_status("Restoring global objects in the new cluster");

	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "%s \"%s/psql\" " EXEC_PSQL_ARGS " %s -f \"%s/%s\"",
			  PG_OPTIONS_UTILITY_MODE_VERSION(new_cluster.major_version),
			  new_cluster.bindir, cluster_conn_opts(&new_cluster),
			  log_opts.dumpdir,
			  GLOBALS_DUMP_FILE);
	check_ok();
}


static void
create_new_objects(void)
{
	int			dbnum;

	prep_status_progress("Restoring database schemas in the new cluster");

	/*
	 * We cannot process the template1 database concurrently with others,
	 * because when it's transiently dropped, connection attempts would fail.
	 * So handle it in a separate non-parallelized pass.
	 */
	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		char		sql_file_name[MAXPGPATH],
					log_file_name[MAXPGPATH];
		DbInfo	   *old_db = &old_cluster.dbarr.dbs[dbnum];
		const char *create_opts;

		/* Process only template1 in this pass */
		if (strcmp(old_db->db_name, "template1") != 0)
			continue;

		pg_log(PG_STATUS, "%s", old_db->db_name);
		snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
		snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

		/*
		 * template1 database will already exist in the target installation,
		 * so tell pg_restore to drop and recreate it; otherwise we would fail
		 * to propagate its database-level properties.
		 */
		create_opts = "--clean --create";

		exec_prog(log_file_name,
				  NULL,
				  true,
				  true,
				  "\"%s/pg_restore\" %s %s --exit-on-error --verbose "
				  "--binary-upgrade "
				  "--dbname postgres \"%s/%s\"",
				  new_cluster.bindir,
				  cluster_conn_opts(&new_cluster),
				  create_opts,
				  log_opts.dumpdir,
				  sql_file_name);

		break;					/* done once we've processed template1 */
	}

	for (dbnum = 0; dbnum < old_cluster.dbarr.ndbs; dbnum++)
	{
		char		sql_file_name[MAXPGPATH],
					log_file_name[MAXPGPATH];
		DbInfo	   *old_db = &old_cluster.dbarr.dbs[dbnum];
		const char *create_opts;

		/* Skip template1 in this pass */
		if (strcmp(old_db->db_name, "template1") == 0)
			continue;

		pg_log(PG_STATUS, "%s", old_db->db_name);
		snprintf(sql_file_name, sizeof(sql_file_name), DB_DUMP_FILE_MASK, old_db->db_oid);
		snprintf(log_file_name, sizeof(log_file_name), DB_DUMP_LOG_FILE_MASK, old_db->db_oid);

		/*
		 * postgres database will already exist in the target installation, so
		 * tell pg_restore to drop and recreate it; otherwise we would fail to
		 * propagate its database-level properties.
		 */
		if (strcmp(old_db->db_name, "postgres") == 0)
			create_opts = "--clean --create";
		else
			create_opts = "--create";

		parallel_exec_prog(log_file_name,
						   NULL,
						   "%s \"%s/pg_restore\" %s %s --exit-on-error --verbose "
						   "--binary-upgrade "
						   "--dbname template1 \"%s/%s\"",
						   PG_OPTIONS_UTILITY_MODE_VERSION(new_cluster.major_version),
						   new_cluster.bindir,
						   cluster_conn_opts(&new_cluster),
						   create_opts,
						   log_opts.dumpdir,
						   sql_file_name);
	}

	/* reap all children */
	while (reap_child(true) == true)
		;

	end_progress_output();
	check_ok();

	/* update new_cluster info now that we have objects in the databases */
	get_db_and_rel_infos(&new_cluster);

	/* Bitmap indexes are not currently supported, so mark them as invalid. */
	new_gpdb_invalidate_bitmap_indexes();
}


/*
 * Delete the given subdirectory contents from the new cluster
 */
static void
remove_new_subdir(const char *subdir, bool rmtopdir)
{
	char		new_path[MAXPGPATH];

	prep_status("Deleting files from new %s", subdir);

	snprintf(new_path, sizeof(new_path), "%s/%s", new_cluster.pgdata, subdir);
	if (!rmtree(new_path, rmtopdir))
		pg_fatal("could not delete directory \"%s\"\n", new_path);

	check_ok();
}

/*
 * Copy the files from the old cluster into it
 */
static void
copy_subdir_files(const char *old_subdir, const char *new_subdir)
{
	char		old_path[MAXPGPATH];
	char		new_path[MAXPGPATH];

	remove_new_subdir(new_subdir, true);

	snprintf(old_path, sizeof(old_path), "%s/%s", old_cluster.pgdata, old_subdir);
	snprintf(new_path, sizeof(new_path), "%s/%s", new_cluster.pgdata, new_subdir);

	prep_status("Copying old %s to new server", old_subdir);

	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
#ifndef WIN32
			  "cp -Rf \"%s\" \"%s\"",
#else
	/* flags: everything, no confirm, quiet, overwrite read-only */
			  "xcopy /e /y /q /r \"%s\" \"%s\\\"",
#endif
			  old_path, new_path);

	check_ok();
}

static void
copy_xact_xlog_xid(void)
{
	/*
	 * GPDB_UPGRADE_FIXME: Definitely need more work to make pre-gp7 to gp7 upgrade
	 * work for the 64bit gxid work.
	 */
	/* set the next distributed transaction id of the new cluster */
	prep_status("Setting next distributed transaction ID for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" --binary-upgrade -f --next-gxid "UINT64_FORMAT" \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtgxid,
			  new_cluster.pgdata);
	check_ok();

	/*
	 * Copy old commit logs to new data dir. pg_clog has been renamed to
	 * pg_xact in post-10 clusters.
	 */
	copy_subdir_files(GET_MAJOR_VERSION(old_cluster.major_version) <= 906 ?
					  "pg_clog" : "pg_xact",
					  GET_MAJOR_VERSION(new_cluster.major_version) <= 906 ?
					  "pg_clog" : "pg_xact");

	prep_status("Setting oldest XID for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" --binary-upgrade -f -u %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_oldstxid,
			  new_cluster.pgdata);
	check_ok();

	/* set the next transaction id and epoch of the new cluster */
	prep_status("Setting next transaction ID and epoch for new cluster");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" --binary-upgrade -f -x %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtxid,
			  new_cluster.pgdata);
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" --binary-upgrade -f -e %u \"%s\"",
			  new_cluster.bindir, old_cluster.controldata.chkpnt_nxtepoch,
			  new_cluster.pgdata);
	/* must reset commit timestamp limits also */
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
			  "\"%s/pg_resetwal\" --binary-upgrade -f -c %u,%u \"%s\"",
			  new_cluster.bindir,
			  old_cluster.controldata.chkpnt_nxtxid,
			  old_cluster.controldata.chkpnt_nxtxid,
			  new_cluster.pgdata);
	check_ok();

	/*
	 * If the old server is before the MULTIXACT_FORMATCHANGE_CAT_VER change
	 * (see pg_upgrade.h) and the new server is after, then we don't copy
	 * pg_multixact files, but we need to reset pg_control so that the new
	 * server doesn't attempt to read multis older than the cutoff value.
	 */
	if (old_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER &&
		new_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER)
	{
		copy_subdir_files("pg_multixact/offsets", "pg_multixact/offsets");
		copy_subdir_files("pg_multixact/members", "pg_multixact/members");

		prep_status("Setting next multixact ID and offset for new cluster");

		/*
		 * we preserve all files and contents, so we must preserve both "next"
		 * counters here and the oldest multi present on system.
		 */
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" --binary-upgrade -O %u -m %u,%u \"%s\"",
				  new_cluster.bindir,
				  old_cluster.controldata.chkpnt_nxtmxoff,
				  old_cluster.controldata.chkpnt_nxtmulti,
				  old_cluster.controldata.chkpnt_oldstMulti,
				  new_cluster.pgdata);
		check_ok();
	}
	else if (new_cluster.controldata.cat_ver >= MULTIXACT_FORMATCHANGE_CAT_VER)
	{
		/*
		 * Remove offsets/0000 file created by initdb that no longer matches
		 * the new multi-xid value.  "members" starts at zero so no need to
		 * remove it.
		 */
		remove_new_subdir("pg_multixact/offsets", false);

		prep_status("Setting oldest multixact ID in new cluster");

		/*
		 * We don't preserve files in this case, but it's important that the
		 * oldest multi is set to the latest value used by the old system, so
		 * that multixact.c returns the empty set for multis that might be
		 * present on disk.  We set next multi to the value following that; it
		 * might end up wrapped around (i.e. 0) if the old cluster had
		 * next=MaxMultiXactId, but multixact.c can cope with that just fine.
		 */
		exec_prog(UTILITY_LOG_FILE, NULL, true, true,
				  "\"%s/pg_resetwal\" --binary-upgrade -m %u,%u \"%s\"",
				  new_cluster.bindir,
				  old_cluster.controldata.chkpnt_nxtmulti + 1,
				  old_cluster.controldata.chkpnt_nxtmulti,
				  new_cluster.pgdata);
		check_ok();
	}

	/* now reset the wal archives in the new cluster */
	prep_status("Resetting WAL archives");
	exec_prog(UTILITY_LOG_FILE, NULL, true, true,
	/* use timeline 1 to match controldata and no WAL history file */
			  "\"%s/pg_resetwal\" --binary-upgrade -l 00000001%s \"%s\"", new_cluster.bindir,
			  old_cluster.controldata.nextxlogfile + 8,
			  new_cluster.pgdata);
	check_ok();
}


/*
 *	set_frozenxids()
 *
 * This is called on the new cluster before we restore anything, with
 * minmxid_only = false.  Its purpose is to ensure that all initdb-created
 * vacuumable tables have relfrozenxid/relminmxid matching the old cluster's
 * xid/mxid counters.  We also initialize the datfrozenxid/datminmxid of the
 * built-in databases to match.
 *
 * As we create user tables later, their relfrozenxid/relminmxid fields will
 * be restored properly by the binary-upgrade restore script.  Likewise for
 * user-database datfrozenxid/datminmxid.  However, if we're upgrading from a
 * pre-9.3 database, which does not store per-table or per-DB minmxid, then
 * the relminmxid/datminmxid values filled in by the restore script will just
 * be zeroes.
 *
 * Hence, with a pre-9.3 source database, a second call occurs after
 * everything is restored, with minmxid_only = true.  This pass will
 * initialize all tables and databases, both those made by initdb and user
 * objects, with the desired minmxid value.  frozenxid values are left alone.
 */
static void
set_frozenxids(bool minmxid_only)
{
	int			dbnum;
	PGconn	   *conn,
			   *conn_template1;
	PGresult   *dbres;
	int			ntups;
	int			i_datname;
	int			i_datallowconn;

	if (!minmxid_only)
		prep_status("Setting frozenxid and minmxid counters in new cluster");
	else
		prep_status("Setting minmxid counter in new cluster");

	conn_template1 = connectToServer(&new_cluster, "template1");

	/*
	 * GPDB doesn't allow hacking the catalogs without setting
	 * allow_system_table_mods first.
	 */
	PQclear(executeQueryOrDie(conn_template1,
							  "set allow_system_table_mods=true"));

	if (!minmxid_only)
		/* set pg_database.datfrozenxid */
		PQclear(executeQueryOrDie(conn_template1,
								  "UPDATE pg_catalog.pg_database "
								  "SET	datfrozenxid = '%u'",
								  old_cluster.controldata.chkpnt_nxtxid));

	/* set pg_database.datminmxid */
	PQclear(executeQueryOrDie(conn_template1,
							  "UPDATE pg_catalog.pg_database "
							  "SET	datminmxid = '%u'",
							  old_cluster.controldata.chkpnt_nxtmulti));

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


		/*
		 * We must update databases where datallowconn = false, e.g.
		 * template0, because autovacuum increments their datfrozenxids,
		 * relfrozenxids, and relminmxid even if autovacuum is turned off, and
		 * even though all the data rows are already frozen.  To enable this,
		 * we temporarily change datallowconn.
		 */
		if (strcmp(datallowconn, "f") == 0)
			PQclear(executeQueryOrDie(conn_template1,
									  "ALTER DATABASE %s ALLOW_CONNECTIONS = true",
									  quote_identifier(datname)));

		conn = connectToServer(&new_cluster, datname);

		/*
		 * GPDB doesn't allow hacking the catalogs without setting
		 * allow_system_table_mods first.
		 */
		PQclear(executeQueryOrDie(conn, "set allow_system_table_mods=true"));

		/*
		 * Instead of assuming template0 will be frozen by initdb, its worth
		 * making sure we freeze it here before updating the relfrozenxid
		 * directly for the tables in pg_class and datfrozenxid for the
		 * database in pg_database. Its fast and safe worth than assuming for
		 * template0.
		 */
		if (!minmxid_only && strcmp(datallowconn, "f") == 0)
		{
			PQclear(executeQueryOrDie(conn, "VACUUM FREEZE"));
		}

		if (!minmxid_only)
			/* set pg_class.relfrozenxid */
			PQclear(executeQueryOrDie(conn,
									  "UPDATE	pg_catalog.pg_class "
									  "SET	relfrozenxid = '%u' "
			/* only heap, materialized view, and TOAST are vacuumed */
									  "WHERE	relkind IN ("
									  CppAsString2(RELKIND_RELATION) ", "
									  CppAsString2(RELKIND_MATVIEW) ", "
									  CppAsString2(RELKIND_TOASTVALUE) ")",
									  old_cluster.controldata.chkpnt_nxtxid));

		/* set pg_class.relminmxid */
		PQclear(executeQueryOrDie(conn,
								  "UPDATE	pg_catalog.pg_class "
								  "SET	relminmxid = '%u' "
		/* only heap, materialized view, and TOAST are vacuumed */
								  "WHERE	relkind IN ("
								  CppAsString2(RELKIND_RELATION) ", "
								  CppAsString2(RELKIND_MATVIEW) ", "
								  CppAsString2(RELKIND_TOASTVALUE) ")",
								  old_cluster.controldata.chkpnt_nxtmulti));
		PQfinish(conn);

		/* Reset datallowconn flag */
		if (strcmp(datallowconn, "f") == 0)
		{
			PQclear(executeQueryOrDie(conn_template1,
									  "ALTER DATABASE %s ALLOW_CONNECTIONS = false",
									  quote_identifier(datname)));
		}
	}

	PQclear(dbres);

	PQfinish(conn_template1);

	check_ok();
}
