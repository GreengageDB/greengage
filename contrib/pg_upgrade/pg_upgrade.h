#ifndef PG_UPGRADE_H
#define PG_UPGRADE_H
/*
 *	pg_upgrade.h
 *
 *	Copyright (c) 2010-2014, PostgreSQL Global Development Group
 *	Portions Copyright (c) 2016-Present, Pivotal Software Inc
 *	contrib/pg_upgrade/pg_upgrade.h
 */

#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "postgres.h"
#include "libpq-fe.h"
#include "pqexpbuffer.h"

#include "greengage/greengage_cluster_info.h"

/* Use port in the private/dynamic port number range */
#define DEF_PGUPORT			50432

/* Allocate for null byte */
#define USER_NAME_SIZE		128

#define MAX_STRING			1024
#define LINE_ALLOC			4096
#define QUERY_ALLOC			8192

#define MIGRATOR_API_VERSION	1

#define MESSAGE_WIDTH		60

#define GET_MAJOR_VERSION(v)	((v) / 100)

/* contains both global db information and CREATE DATABASE commands */
#define GLOBALS_DUMP_FILE	"pg_upgrade_dump_globals.sql"
#define DB_DUMP_FILE_MASK	"pg_upgrade_dump_%u.custom"

#define DB_DUMP_LOG_FILE_MASK	"pg_upgrade_dump_%u.log"
#define SERVER_LOG_FILE		"pg_upgrade_server.log"
#define UTILITY_LOG_FILE	"pg_upgrade_utility.log"
#define INTERNAL_LOG_FILE	"pg_upgrade_internal.log"

extern char *output_files[];

/*
 * WIN32 files do not accept writes from multiple processes
 *
 * On Win32, we can't send both pg_upgrade output and command output to the
 * same file because we get the error: "The process cannot access the file
 * because it is being used by another process." so send the pg_ctl
 * command-line output to a new file, rather than into the server log file.
 * Ideally we could use UTILITY_LOG_FILE for this, but some Windows platforms
 * keep the pg_ctl output file open by the running postmaster, even after
 * pg_ctl exits.
 *
 * We could use the Windows pgwin32_open() flags to allow shared file
 * writes but is unclear how all other tools would use those flags, so
 * we just avoid it and log a little differently on Windows;  we adjust
 * the error message appropriately.
 */
#ifndef WIN32
#define SERVER_START_LOG_FILE	SERVER_LOG_FILE
#define SERVER_STOP_LOG_FILE	SERVER_LOG_FILE
#else
#define SERVER_START_LOG_FILE	"pg_upgrade_server_start.log"
/*
 *	"pg_ctl start" keeps SERVER_START_LOG_FILE and SERVER_LOG_FILE open
 *	while the server is running, so we use UTILITY_LOG_FILE for "pg_ctl
 *	stop".
 */
#define SERVER_STOP_LOG_FILE	UTILITY_LOG_FILE
#endif


#ifndef WIN32
#define pg_mv_file			rename
#define pg_link_file		link
#define PATH_SEPARATOR		'/'
#define RM_CMD				"rm -f"
#define RMDIR_CMD			"rm -rf"
#define SCRIPT_EXT			"sh"
#define ECHO_QUOTE	"'"
#define ECHO_BLANK	""
#else
#define pg_mv_file			pgrename
#define pg_link_file		win32_pghardlink
#define PATH_SEPARATOR		'\\'
#define RM_CMD				"DEL /q"
#define RMDIR_CMD			"RMDIR /s/q"
#define SCRIPT_EXT			"bat"
#define EXE_EXT				".exe"
#define ECHO_QUOTE	""
#define ECHO_BLANK	"."
#endif

#define CLUSTER_NAME(cluster)	((cluster) == &old_cluster ? "old" : \
								 (cluster) == &new_cluster ? "new" : "none")

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

/* OID system catalog preservation added during PG 9.0 development */
#define TABLE_SPACE_SUBDIRS_CAT_VER 201001111
/* postmaster/postgres -b (binary_upgrade) flag added during PG 9.1 development */
/* In GPDB, it was introduced during GPDB 5.0 development. */
#define BINARY_UPGRADE_SERVER_FLAG_CAT_VER 301607301
/*
 *	Visibility map changed with this 9.2 commit,
 *	8f9fe6edce358f7904e0db119416b4d1080a83aa; pick later catalog version.
 */
#define VISIBILITY_MAP_CRASHSAFE_CAT_VER 201107031

/*
 * change in JSONB format during 9.4 beta
 */
#define JSONB_FORMAT_CHANGE_CAT_VER 201409291

/*
 * pg_multixact format changed in 9.3 commit 0ac5ad5134f2769ccbaefec73844f85,
 * ("Improve concurrency of foreign key locking") which also updated catalog
 * version to this value.  pg_upgrade behavior depends on whether old and new
 * server versions are both newer than this, or only the new one is.
 *
 * In GPDB: that upstream change was merged into GPDB in the big 9.3 merge
 * commit.
 */
#define MULTIXACT_FORMATCHANGE_CAT_VER 301809211

typedef struct
{
	int16		attlen;
	char		attalign;
	bool		is_numeric;
} AttInfo;

typedef enum
{
	HEAP,
	AO,
	AOCS,
	FSM
} RelType;


/*
 * Each relation is represented by a relinfo structure.
 */
typedef struct
{
	/* Can't use NAMEDATALEN;  not guaranteed to fit on client */
	char	   *nspname;		/* namespace name */
	char	   *relname;		/* relation name */
	Oid			reloid;			/* relation oid */
	char		relstorage;
	Oid			relfilenode;	/* relation relfile node */
	/* GPDB_96_MERGE_FIXME: indtable and toastheap are backported from 9.6. */
	Oid			indtable;		/* if index, OID of its table, else 0 */
	Oid			toastheap;		/* if toast table, OID of base table, else 0 */
	/* relation tablespace path, or "" for the cluster default */
	char	   *tablespace;
	bool		nsp_alloc;
	bool		tblsp_alloc;

	RelType		reltype;

	/* Extra information for heap tables */
	AttInfo	   *atts;
	int			natts;
} RelInfo;

typedef struct
{
	RelInfo    *rels;
	int			nrels;
} RelInfoArr;

/*
 * The following structure represents a relation mapping.
 */
typedef struct
{
	const char *old_tablespace;
	const char *new_tablespace;
	const char *old_tablespace_suffix;
	const char *new_tablespace_suffix;
	Oid			old_db_oid;
	Oid			new_db_oid;

	/*
	 * old/new relfilenodes might differ for pg_largeobject(_metadata) indexes
	 * due to VACUUM FULL or REINDEX.  Other relfilenodes are preserved.
	 */
	Oid			old_relfilenode;
	Oid			new_relfilenode;
	/* the rest are used only for logging and error reporting */
	char	   *nspname;		/* namespaces */
	char	   *relname;

	bool		missing_seg0_ok;

	RelType		type;			/* Type of relation */

	/* Extra information for heap tables */
	AttInfo	   *atts;
	int			natts;
} FileNameMap;

/*
 * Structure to store database information
 */
typedef struct
{
	Oid			db_oid;			/* oid of the database */
	char	   *db_name;		/* database name */
	char		db_tablespace[MAXPGPATH];		/* database default tablespace
												 * path */
	uint32 		datfrozenxid;
	uint32 		datminmxid;
	RelInfoArr	rel_arr;		/* array of all user relinfos */
} DbInfo;

typedef struct
{
	DbInfo	   *dbs;			/* array of db infos */
	int			ndbs;			/* number of db infos */
} DbInfoArr;

/*
 * The following structure is used to hold pg_control information.
 * Rather than using the backend's control structure we use our own
 * structure to avoid pg_control version issues between releases.
 */
typedef struct
{
	uint32		ctrl_ver;
	uint32		cat_ver;
	char		nextxlogfile[25];
	uint32		chkpnt_nxtxid;
	uint32		chkpnt_nxtepoch;
	uint32		chkpnt_nxtoid;
	uint32		chkpnt_nxtmulti;
	uint32		chkpnt_nxtmxoff;
	uint32		chkpnt_oldstMulti;
	uint32		chkpnt_oldstxid;
	uint32		align;
	uint32		blocksz;
	uint32		largesz;
	uint32		walsz;
	uint32		walseg;
	uint32		ident;
	uint32		index;
	uint32		toast;
	bool		date_is_int;
	bool		float8_pass_by_value;
	bool		data_checksum_version;
	char	   *lc_collate;
	char	   *lc_ctype;
	char	   *encoding;
} ControlData;

/*
 * Enumeration to denote link modes
 */
typedef enum
{
	TRANSFER_MODE_COPY,
	TRANSFER_MODE_LINK
} transferMode;

/*
 * Enumeration to denote pg_log modes
 */
typedef enum
{
	PG_VERBOSE,
	PG_STATUS,
	PG_REPORT,
	PG_WARNING,
	PG_FATAL
} eLogType;


typedef long pgpid_t;


/*
 * cluster
 *
 *	information about each cluster
 */
typedef struct
{
	ControlData controldata;	/* pg_control information */
	DbInfoArr	dbarr;			/* dbinfos array */
	char	   *pgdata;			/* pathname for cluster's $PGDATA directory */
	char	   *pgconfig;		/* pathname for cluster's config file
								 * directory */
	char	   *bindir;			/* pathname for cluster's executable directory */
	char	   *pgopts;			/* options to pass to the server, like pg_ctl
								 * -o */
	char	   *sockdir;		/* directory for Unix Domain socket, if any */
	unsigned short port;		/* port number where postmaster is waiting */
	uint32		major_version;	/* PG_VERSION of cluster */
	char		major_version_str[64];	/* string PG_VERSION of cluster */
	uint32		bin_version;	/* version returned from pg_ctl */
	Oid			pg_database_oid;	/* OID of pg_database relation */
	Oid			install_role_oid;		/* OID of connected role */
	Oid			role_count;		/* number of roles defined in the cluster */
	const char *tablespace_suffix;		/* directory specification */

	GreengageClusterInfo *greengage_cluster_info;
} ClusterInfo;

/*
 *	LogOpts
*/
typedef struct
{
	FILE	   *internal;		/* internal log FILE */
	bool		verbose;		/* TRUE -> be verbose in messages */
	bool		retain;			/* retain log files on success */
} LogOpts;


/*
 *	UserOpts
*/
typedef struct
{
	bool		check;			/* TRUE -> ask user for permission to make
								 * changes */
	transferMode transfer_mode; /* copy files or link them? */
	int			jobs;			/* number of processes/threads to use */
	char	   *socketdir;		/* directory to use for Unix sockets */
} UserOpts;


/*
 * OSInfo
 */
typedef struct
{
	const char *progname;		/* complete pathname for this program */
	char	   *exec_path;		/* full path to my executable */
	char	   *user;			/* username for clusters */
	bool		user_specified; /* user specified on command-line */
	char	  **old_tablespaces;	/* tablespaces */
	int			num_old_tablespaces;
	char	  **libraries;		/* loadable libraries */
	int			num_libraries;
	ClusterInfo *running_cluster;
} OSInfo;


/*
 * Global variables
 */
extern LogOpts log_opts;
extern UserOpts user_opts;
extern ClusterInfo old_cluster,
			new_cluster;
extern OSInfo os_info;

/* check.c */

void		output_check_banner(bool live_check);
void check_and_dump_old_cluster(bool live_check,
						   char **sequence_script_file_name);
void		check_new_cluster(void);
void		report_clusters_compatible(void);
void		issue_warnings_and_set_wal_level(char *sequence_script_file_name);
void output_completion_banner(char *analyze_script_file_name,
						 char *deletion_script_file_name);
void		check_cluster_versions(void);
void		check_cluster_compatibility(bool live_check);
void		create_script_for_old_cluster_deletion(char **deletion_script_file_name);
void		create_script_for_cluster_analyze(char **analyze_script_file_name);


/* controldata.c */

void		get_control_data(ClusterInfo *cluster, bool live_check);
void		check_control_data(ControlData *oldctrl, ControlData *newctrl);
void		disable_old_cluster(void);


/* dump.c */

void		generate_old_dump(void);


/* exec.c */

#define EXEC_PSQL_ARGS "--echo-queries --set ON_ERROR_STOP=on --no-psqlrc --dbname=template1"

bool exec_prog(const char *log_file, const char *opt_log_file,
		  bool report_error, bool exit_on_error, const char *fmt,...)
		  __attribute__((format(PG_PRINTF_ATTRIBUTE, 5, 6)));
void		verify_directories(void);
bool		pid_lock_file_exists(const char *datadir);


/* file.c */

const char *copyFile(const char *src, const char *dst, bool force);
const char *linkFile(const char *src, const char *dst);

void		check_hard_link(void);

/* fopen_priv() is no longer different from fopen() */
#define fopen_priv(path, mode)	fopen(path, mode)

/* function.c */

void		install_support_functions_in_new_db(const char *db_name);
void		uninstall_support_functions_from_new_cluster(void);
void		get_loadable_libraries(void);
void		check_loadable_libraries(void);

/* info.c */

FileNameMap *gen_db_file_maps(DbInfo *old_db,
				 DbInfo *new_db, int *nmaps, const char *old_pgdata,
				 const char *new_pgdata);
void		get_db_and_rel_infos(ClusterInfo *cluster);
void print_maps(FileNameMap *maps, int n,
		   const char *db_name);

/* option.c */

void		parseCommandLine(int argc, char *argv[]);
void		adjust_data_dir(ClusterInfo *cluster);
void		get_sock_dir(ClusterInfo *cluster, bool live_check);

/* relfilenode.c */

void transfer_all_new_tablespaces(DbInfoArr *old_db_arr,
				  DbInfoArr *new_db_arr, char *old_pgdata, char *new_pgdata);
void transfer_all_new_dbs(DbInfoArr *old_db_arr,
				   DbInfoArr *new_db_arr, char *old_pgdata, char *new_pgdata,
					 char *old_tablespace);

/* tablespace.c */

void		init_tablespaces(void);

/* server.c */

PGconn	   *connectToServer(ClusterInfo *cluster, const char *db_name);
PGresult *
executeQueryOrDie(PGconn *conn, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

char	   *cluster_conn_opts(ClusterInfo *cluster);

bool		start_postmaster(ClusterInfo *cluster, bool report_and_exit_on_error);
void		stop_postmaster(bool in_atexit);
uint32		get_major_server_version(ClusterInfo *cluster);
void		check_pghost_envvar(void);


/* util.c */

char	   *quote_identifier(const char *s);
extern void appendShellString(PQExpBuffer buf, const char *str);
extern void appendConnStrVal(PQExpBuffer buf, const char *str);
extern void appendPsqlMetaConnect(PQExpBuffer buf, const char *dbname);
int			get_user_info(char **user_name_p);
void		check_ok(void);
void		parallel_check_ok(const char *check_name);
void		start_parallel_check(const char *check_name);
void
report_status(eLogType type, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));
void
pg_log(eLogType type, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));
void
pg_fatal(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2), noreturn));
void		end_progress_output(void);
void
prep_status(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
void		check_ok(void);
const char *getErrorText(void);
unsigned int str2uint(const char *str);
void		pg_putenv(const char *var, const char *val);
void 		gp_fatal_log(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
void 		parallel_gp_fatal_log(const char *check_name, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

/* version.c */
#if 0
/*
 * In Greengage, large objects are not supported, so the below
 * code is not required
 */
void new_9_0_populate_pg_largeobject_metadata(ClusterInfo *cluster,
										 bool check_mode);
#endif
#if 0
/*
 * GPDB 5 does not support the line datatype.
 */
void old_9_3_check_for_line_data_type_usage(ClusterInfo *cluster);
#endif
/* version_old_8_3.c */

void		old_8_3_check_for_tsquery_usage(ClusterInfo *cluster);
void		old_8_3_check_ltree_usage(ClusterInfo *cluster);
void		old_8_3_rebuild_tsvector_tables(ClusterInfo *cluster, bool check_mode);
void		old_8_3_invalidate_hash_gin_indexes(ClusterInfo *cluster, bool check_mode);
void old_8_3_invalidate_bpchar_pattern_ops_indexes(ClusterInfo *cluster,
											  bool check_mode);
char	   *old_8_3_create_sequence_script(ClusterInfo *cluster);

/* parallel.c */
void
parallel_exec_prog(const char *log_file, const char *opt_log_file,
				   const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));
void parallel_transfer_all_new_dbs(DbInfoArr *old_db_arr, DbInfoArr *new_db_arr,
							  char *old_pgdata, char *new_pgdata,
							  char *old_tablespace);
bool		reap_child(bool wait_for_child);

/*
 * Hack to make backend macros that check for assertions to work.
 */
#ifdef AssertMacro
#undef AssertMacro
#endif
#define AssertMacro(condition) ((void) true)
#ifdef Assert
#undef Assert
#endif
#define Assert(condition) ((void) (true || (condition)))

#endif /* PG_UPGRADE_H */
