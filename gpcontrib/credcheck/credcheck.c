/*-------------------------------------------------------------------------
 *
 * credcheck.c:
 * 		This file has the general PostgreSQL credential checks.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2021-2023: MigOps Inc
 * Copyright (C) 2023: Gilles Darold
 *
 *-------------------------------------------------------------------------
 */
#include <ctype.h>
#include <limits.h>
#include <unistd.h>

#ifdef USE_CRACKLIB
#include <crack.h>
#endif

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"

#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "commands/user.h"
#if PG_VERSION_NUM >= 140000
#include "common/hmac.h"
#endif
#include "common/sha2.h"
#include "executor/spi.h"
#include "libpq/auth.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "postmaster/postmaster.h"
#include "tcop/utility.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

/* Default passord encryption */
#define Password_encryption = PASSWORD_TYPE_SCRAM_SHA_256;
/* Name of external file to store password history in the PGDATA */
#define PGPH_DUMP_FILE  "global/pg_password_history"

/* Number of output arguments (columns) in the pg_password_history pseudo table */
#define PG_PASSWORD_HISTORY_COLS	3
/* Number of output arguments (columns) in the pg_banned_role pseudo table */
#define PG_BANNED_ROLE_COLS		3

/* Magic number identifying the stats file format */
static const uint32 PGPH_FILE_HEADER = 0x48504750;
/* credcheck password history version, changes in which invalidate all entries */
static const uint32 PGPH_VERSION = 100;
#define PGPH_TRANCHE_NAME                "credcheck_history"
#define PGAF_TRANCHE_NAME                "credcheck_auth_failure"

static bool statement_has_password = false;
static bool no_password_logging    = true;

#if PG_VERSION_NUM < 120000
#define table_open(r,l)         heap_open(r,l)
#define table_openrv(r,l)       heap_openrv(r,l)
#define table_close(r,l)        heap_close(r,l)
#endif

#if PG_VERSION_NUM < 100000
#error Minimum version of PostgreSQL required is 10
#endif

/* Define ProcessUtility hook proto/parameters following the PostgreSQL version */
#if PG_VERSION_NUM >= 140000
#define PEL_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
				       bool readOnlyTree, \
					ProcessUtilityContext context, ParamListInfo params, \
					QueryEnvironment *queryEnv, DestReceiver *dest, \
					QueryCompletion *qc
#define PEL_PROCESSUTILITY_ARGS pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc
#else
#if PG_VERSION_NUM >= 130000
#define PEL_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
					ProcessUtilityContext context, ParamListInfo params, \
					QueryEnvironment *queryEnv, DestReceiver *dest, \
					QueryCompletion *qc
#define PEL_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, qc
#else
#define PEL_PROCESSUTILITY_PROTO PlannedStmt *pstmt, const char *queryString, \
					ProcessUtilityContext context, ParamListInfo params, \
					QueryEnvironment *queryEnv, DestReceiver *dest, \
					char *completionTag
#define PEL_PROCESSUTILITY_ARGS pstmt, queryString, context, params, queryEnv, dest, completionTag
#endif
#endif

PG_MODULE_MAGIC;

/* Hooks */
static check_password_hook_type prev_check_password_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
/* Hold previous client authent hook */
static ClientAuthentication_hook_type prev_ClientAuthentication = NULL;
/* Hold previous logging hook */
static emit_log_hook_type prev_log_hook = NULL;


/* In memory storage of password history */
typedef struct pgphHashKey
{
        char rolename[NAMEDATALEN];
        char password_hash[PG_SHA256_DIGEST_STRING_LENGTH];
} pgphHashKey;

typedef struct pgphEntry
{
	pgphHashKey key;                        /* hash key of entry - MUST BE FIRST */
        TimestampTz password_date;
} pgphEntry;

/* Global shared state */
typedef struct pgphSharedState
{
        LWLock     *lock;                   /* protects hashtable search/modification */
	int	    num_entries;            /* number of entries in the password history */
} pgphSharedState;


/* Links to shared memory state */
static pgphSharedState *pgph = NULL;
static HTAB *pgph_hash = NULL;
static int pgph_max = 65535;
static int pgaf_max = 1024;
static int fail_max = 0;
static bool reset_superuser = false;
static bool encrypted_password_allowed = false;

/* In memory storage of auth failure history */
typedef struct pgafHashKey
{
	Oid  roleid;
} pgafHashKey;

typedef struct pgafEntry
{
	pgafHashKey key;                        /* hash key of entry - MUST BE FIRST */
        float failure_count;
        TimestampTz banned_date;
} pgafEntry;

/* Global shared state */
typedef struct pgafSharedState
{
        LWLock     *lock;                   /* protects hashtable search/modification */
	int	    num_entries;            /* number of entries in the auth failure history */
} pgafSharedState;

static pgafSharedState *pgaf = NULL;
static HTAB *pgaf_hash = NULL;


/* Functions */
extern void _PG_init(void);
extern void _PG_fini(void);
static void cc_ProcessUtility(PEL_PROCESSUTILITY_PROTO);

static void flush_password_history(void);
static pgphEntry *pgph_entry_alloc(pgphHashKey *key, TimestampTz password_date);
static pgafEntry *pgaf_entry_alloc(pgafHashKey *key, float failure_count);
#if PG_VERSION_NUM >= 150000
static void pghist_shmem_request(void);
#endif
static void pghist_shmem_startup(void);
static void pgph_shmem_startup(void);
static void pgaf_shmem_startup(void);
static int  entry_cmp(const void *lhs, const void *rhs);
static Size pgph_memsize(void);
static void pg_password_history_internal(FunctionCallInfo fcinfo);
static void fix_log(ErrorData *edata);
static Size pgaf_memsize(void);
static void credcheck_max_auth_failure(Port *port, int status);
static float get_auth_failure(const char *username, Oid userid, int status);
static float save_auth_failure(const char *username, Oid userid);
static void remove_auth_failure(const char *username, Oid userid);
static void pg_banned_role_internal(FunctionCallInfo fcinfo);

/* Username flags*/
static int username_min_length = 1;
static int username_min_special = 0;
static int username_min_digit = 0;
static int username_min_upper = 0;
static int username_min_lower = 0;
static int username_min_repeat = 0;
static char *username_not_contain = NULL;
static char *username_contain = NULL;
static bool username_contain_password = true;
static bool username_ignore_case = false;
static char *username_whitelist = NULL;

/* Password flags*/
static int password_min_length = 1;
static int password_min_special = 0;
static int password_min_digit = 0;
static int password_min_upper = 0;
static int password_min_lower = 0;
static int password_min_repeat = 0;
static char *password_not_contain = NULL;
static char *password_contain = NULL;
static bool password_contain_username = true;
static bool password_ignore_case = false;
static int password_valid_until = 0;
static int password_valid_max = 0;
static int auth_delay_milliseconds = 0;

#if PG_VERSION_NUM >= 120000
/*
 password_reuse_history:
	number of distinct passwords set before a password can be reused.
 password_reuse_interval:
	amount of time it takes before a password can be reused again.
*/
static int password_reuse_history = 0;
static int password_reuse_interval = 0;

char *str_to_sha256(const char *str, const char *salt);
#endif

bool check_whitelist(char **newval, void **extra, GucSource source);
bool is_in_whitelist(char *username);

static char *to_nlower(const char *str, size_t max) {
  char *lower_str;
  int i = 0;

  lower_str = (char *)calloc(strlen(str), sizeof(char));

  for (const char *p = str; *p && i < max; p++) {
    lower_str[i++] = tolower(*p);
  }
  lower_str[i] = '\0';
  return lower_str;
}

static bool str_contains(const char *chars, const char *str) {
  for (const char *i = str; *i; i++) {
    for (const char *j = chars; *j; j++) {
      if (*i == *j) {
        return true;
      }
    }
  }

  return false;
}

static void check_str_counters(const char *str, int *lower, int *upper,
                               int *digit, int *special) {
  for (const char *i = str; *i; i++) {
    if (islower(*i)) {
      (*lower)++;
    } else if (isupper(*i)) {
      (*upper)++;
    } else if (isdigit(*i)) {
      (*digit)++;
    } else {
      (*special)++;
    }
  }
}

static bool char_repeat_exceeds(const char *str, int max_repeat) {
  int occurred = 1;
  size_t len = strlen(str);

  /*if string has only one character, then no need to proceed further*/
  if (len==1) {
	  return false;
  }

  for (size_t i = 0; i < len;) {
    occurred = 1;
    /*first character = str[i]
     second character = str[i+1]
     search for an adjacent repeated characters
     for example, in this string "weekend summary"
     search for the series "ee", "mm"
     */
    for (size_t j = (i + 1), k = 1; j < len; j++, k++) {
      /* character matched*/
      if (str[i] == str[j]) {
        /* is the previous, current character positions are adjacent*/
        if (i + k == j) {
          occurred++;
          if (occurred > max_repeat) {
            return true;
          }
        }
      }

      /* if we reach an end of the string, no need to process further*/
      if (j + 1 == len) {
        return false;
      }

      /* if the characters are not equal then point "i" to "j"*/
      if (str[i] != str[j]) {
        i = j;
        break;
      }
    }
  }
  return false;
}

static void
username_check(const char *username, const char *password)
{
	int user_total_special = 0;
	int user_total_digit = 0;
	int user_total_upper = 0;
	int user_total_lower = 0;

	char *tmp_pass = NULL;
	char *tmp_user = NULL;
	char *tmp_contains = NULL;
	char *tmp_not_contains = NULL;

	if (strcasestr(debug_query_string, "PASSWORD") != NULL)
		statement_has_password = true;

	/* checks has to be done by ignoring case */
	if (username_ignore_case)
	{
		if (password != NULL && strlen(password) > 0)
			tmp_pass = to_nlower(password, INT_MAX);
		tmp_user = to_nlower(username, INT_MAX);
		tmp_contains = to_nlower(username_contain, INT_MAX);
		tmp_not_contains = to_nlower(username_not_contain, INT_MAX);
	}
	else
	{
		if (password != NULL && strlen(password) > 0)
			tmp_pass = strndup(password, INT_MAX);
		tmp_user = strndup(username, INT_MAX);
		tmp_contains = strndup(username_contain, INT_MAX);
		tmp_not_contains = strndup(username_not_contain, INT_MAX);
	}

	/* Rule 1: username length */
	if (strnlen(tmp_user, INT_MAX) < username_min_length)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg(gettext_noop("username length should match the configured %s"), 
				     "credcheck.username_min_length")));
		goto clean;
	}

	/* Rule 2: username contains password
	 * Note:
	 * tmp_pass is NULL for ALTER USER ... RENAME TO ...;
	 * statement so this rule can not be applied.
	 */
	if (tmp_pass != NULL && username_contain_password)
	{
		if (strstr(tmp_user, tmp_pass)) {
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					errmsg(gettext_noop("username should not contain password"))));
			goto clean;
		}
	}

	/* Rule 3: contain characters */
	if (tmp_contains != NULL && strlen(tmp_contains) > 0)
	{
		if (str_contains(tmp_contains, tmp_user) == false)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					errmsg(gettext_noop("username does not contain the configured %s characters"),
	 						"credcheck.username_contain")));
			goto clean;
		}
	}

	/* Rule 4: not contain characters */
	if (tmp_not_contains != NULL && strlen(tmp_not_contains) > 0)
	{
		if (str_contains(tmp_not_contains, tmp_user) == true)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					errmsg(gettext_noop("username contains the configured %s unauthorized characters"),
					       "credcheck.username_not_contain")));
			goto clean;
		}
	}

	check_str_counters(tmp_user, &user_total_lower, &user_total_upper,
		     &user_total_digit, &user_total_special);

	/* Rule 5: total upper characters */
	if (!username_ignore_case && user_total_upper < username_min_upper)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("username does not contain the configured %s characters",
				     "credcheck.username_min_upper")));
		goto clean;
	}

	/* Rule 6: total lower characters */
	if (!username_ignore_case && user_total_lower < username_min_lower)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("username does not contain the configured %s characters",
				     "credcheck.username_min_lower")));
		goto clean;
	}

	/* Rule 7: total digits */
	if (user_total_digit < username_min_digit)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("username does not contain the configured %s characters",
				     "credcheck.username_min_digit")));
		goto clean;
	}

	/* Rule 8: total special */
	if (user_total_special < username_min_special)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("username does not contain the configured %s characters",
				     "credcheck.username_min_special")));
		goto clean;
	}

	/* Rule 9: minimum char repeat */
	if (username_min_repeat)
	{
		if (char_repeat_exceeds(tmp_user, username_min_repeat))
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				   errmsg(gettext_noop("%s characters are repeated more than the "
						"configured %s times"), "username", "credcheck.username_min_repeat")));
			goto clean;
		}
	}
	clean:

	free(tmp_pass);
	free(tmp_user);
	free(tmp_contains);
	free(tmp_not_contains);
}

/* We just check that the list is valid, no username existing check */
bool
check_whitelist(char **newval, void **extra, GucSource source)
{
	char       *rawstring;
	List       *elemlist;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);
	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(elemlist);
		return false;
	}

	pfree(rawstring);
	list_free(elemlist);

	return true;
}

/* check if the username is in the whitelist */
bool
is_in_whitelist(char *username)
{
	char       *rawstring;
	List       *elemlist;
	ListCell   *l;
	int len =  strlen(username_whitelist);

	Assert(username != NULL);

	if (len == 0)
		return false;

	/* Need a modifiable copy of string */
	rawstring = palloc0(sizeof(char) * (len+1));
	strcpy(rawstring, username_whitelist);
	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("%s username list is invalid: %s",
				     "credcheck.password_min_length", username_whitelist)));
		list_free(elemlist);
		pfree(rawstring);
		return false;
	}

        foreach(l, elemlist)
        {
                char       *tok = (char *) lfirst(l);

		/* the username is in the list */
                if (pg_strcasecmp(tok, username) == 0)
		{
                        list_free(elemlist);
			pfree(rawstring);
                        return true;
                }
        }

	list_free(elemlist);
	pfree(rawstring);

	return false;
}

static void password_check(const char *username, const char *password)
{

	int pass_total_special = 0;
	int pass_total_digit = 0;
	int pass_total_upper = 0;
	int pass_total_lower = 0;

	char *tmp_pass = NULL;
	char *tmp_user = NULL;
	char *tmp_contains = NULL;
	char *tmp_not_contains = NULL;

	Assert(username != NULL);
	Assert(password != NULL);

	/* checks has to be done by ignoring case */
	if (password_ignore_case)
	{
		tmp_pass = to_nlower(password, INT_MAX);
		tmp_user = to_nlower(username, INT_MAX);
		tmp_contains = to_nlower(password_contain, INT_MAX);
		tmp_not_contains = to_nlower(password_not_contain, INT_MAX);
	}
	else
	{
		tmp_pass = strndup(password, INT_MAX);
		tmp_user = strndup(username, INT_MAX);
		tmp_contains = strndup(password_contain, INT_MAX);
		tmp_not_contains = strndup(password_not_contain, INT_MAX);
	}

	/* Rule 1: password length */
	if (strnlen(tmp_pass, INT_MAX) < password_min_length)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg(gettext_noop("password length should match the configured %s"),
				     "credcheck.password_min_length")));
		goto clean;
	}

	/* Rule 2: password contains username */
	if (password_contain_username)
	{
		if (strstr(tmp_pass, tmp_user))
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					errmsg(gettext_noop("password should not contain username"))));
			goto clean;
		}
	}

	/* Rule 3: contain characters */
	if (tmp_contains != NULL && strlen(tmp_contains) > 0)
	{
		if (str_contains(tmp_contains, tmp_pass) == false)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					errmsg(gettext_noop("password does not contain the configured %s characters"), 
					       "credcheck.password_contain")));
			goto clean;
		}
	}

	/* Rule 4: not contain characters */
	if (tmp_not_contains != NULL && strlen(tmp_not_contains) > 0)
	{
		if (str_contains(tmp_not_contains, tmp_pass) == true)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
					errmsg(gettext_noop("password contains the configured %s unauthorized characters"),
					       "credcheck.password_not_contain")));
			goto clean;
		}
	}

	check_str_counters(tmp_pass, &pass_total_lower, &pass_total_upper,
		     &pass_total_digit, &pass_total_special);

	/* Rule 5: total upper characters */
	if (!password_ignore_case && pass_total_upper < password_min_upper)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("password does not contain the configured %s characters",
				     "credcheck.password_min_upper")));
		goto clean;
	}

	/* Rule 6: total lower characters */
	if (!password_ignore_case && pass_total_lower < password_min_lower)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("password does not contain the configured %s characters",
				     "credcheck.password_min_lower")));
		goto clean;
	}

	/* Rule 7: total digits */
	if (pass_total_digit < password_min_digit)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("password does not contain the configured %s characters",
				     "credcheck.password_min_digit")));
		goto clean;
	}

	/* Rule 8: total special */
	if (pass_total_special < password_min_special)
	{
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg("password does not contain the configured %s characters",
				     "credcheck.password_min_special")));
		goto clean;
	}

	/* Rule 9: minimum char repeat */
	if (password_min_repeat)
	{
		if (char_repeat_exceeds(tmp_pass, password_min_repeat))
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				   errmsg("%s characters are repeated more than the "
						"configured %s times", "password", "credcheck.password_min_repeat")));
			goto clean;
		}
	}

	clean:

	free(tmp_pass);
	free(tmp_user);
	free(tmp_contains);
	free(tmp_not_contains);
}

static void
username_guc()
{
	DefineCustomIntVariable("credcheck.username_min_length",
				gettext_noop("minimum username length"), NULL,
				&username_min_length, 1, 1, INT_MAX, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.username_min_special",
				gettext_noop("minimum username special characters"),
				NULL, &username_min_special, 0, 0, INT_MAX,
				PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.username_min_digit",
				gettext_noop("minimum username digits"), NULL,
				&username_min_digit, 0, 0, INT_MAX, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.username_min_upper",
				gettext_noop("minimum username uppercase letters"),
				NULL, &username_min_upper, 0, 0, INT_MAX, PGC_SUSET,
				0, NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.username_min_lower",
				gettext_noop("minimum username lowercase letters"),
				NULL, &username_min_lower, 0, 0, INT_MAX, PGC_SUSET,
				0, NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.username_min_repeat",
				gettext_noop("minimum username characters repeat"),
				NULL, &username_min_repeat, 0, 0, INT_MAX,
				PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("credcheck.username_contain_password",
				gettext_noop("username contains password"), NULL,
				&username_contain_password, true, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomBoolVariable("credcheck.username_ignore_case",
				gettext_noop("ignore case while username checking"),
				NULL, &username_ignore_case, false, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomStringVariable(
				"credcheck.username_not_contain",
				gettext_noop("username should not contain these characters"), NULL,
				&username_not_contain, "", PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomStringVariable(
				"credcheck.username_contain",
				gettext_noop("password should contain these characters"), NULL,
				&username_contain, "", PGC_SUSET, 0, NULL, NULL, NULL);
}

static void
password_guc()
{
	DefineCustomIntVariable("credcheck.password_min_length",
				gettext_noop("minimum password length"), NULL,
				&password_min_length, 1, 1, INT_MAX, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.password_min_special",
				gettext_noop("minimum special characters"), NULL,
				&password_min_special, 0, 0, INT_MAX, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.password_min_digit",
				gettext_noop("minimum password digits"), NULL,
				&password_min_digit, 0, 0, INT_MAX, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.password_min_upper",
				gettext_noop("minimum password uppercase letters"),
				NULL, &password_min_upper, 0, 0, INT_MAX, PGC_SUSET,
				0, NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.password_min_lower",
				gettext_noop("minimum password lowercase letters"),
				NULL, &password_min_lower, 0, 0, INT_MAX, PGC_SUSET,
				0, NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.password_min_repeat",
				gettext_noop("minimum password characters repeat"),
				NULL, &password_min_repeat, 0, 0, INT_MAX,
				PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("credcheck.password_contain_username",
				gettext_noop("password contains username"), NULL,
				&password_contain_username, true, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomBoolVariable("credcheck.password_ignore_case",
				gettext_noop("ignore case while password checking"),
				NULL, &password_ignore_case, false, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomStringVariable(
				"credcheck.password_not_contain",
				gettext_noop("password should not contain these characters"), NULL,
				&password_not_contain, "", PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomStringVariable(
				"credcheck.password_contain",
				gettext_noop("password should contain these characters"), NULL,
				&password_contain, "", PGC_SUSET, 0, NULL, NULL, NULL);

#if PG_VERSION_NUM >= 120000
	DefineCustomIntVariable("credcheck.password_reuse_history",
				gettext_noop("minimum number of password changes before permitting reuse"),
				NULL, &password_reuse_history, 0, 0, 100,
				PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.password_reuse_interval",
				gettext_noop("minimum number of days elapsed before permitting reuse"),
				NULL, &password_reuse_interval, 0, 0, 730, /* max 2 years */
				PGC_SUSET, 0, NULL, NULL, NULL);
#endif

	DefineCustomIntVariable("credcheck.password_valid_until",
				gettext_noop("force use of VALID UNTIL clause in CREATE ROLE statement"
					" with a minimum number of days"),
				NULL, &password_valid_until, 0, 0, INT_MAX,
				PGC_SUSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.password_valid_max",
				gettext_noop("force use of VALID UNTIL clause in CREATE ROLE statement"
					" with a maximum number of days"),
				NULL, &password_valid_max, 0, 0, INT_MAX,
				PGC_SUSET, 0, NULL, NULL, NULL);
}

#if PG_VERSION_NUM >= 120000
static void
save_password_in_history(const char *username, const char *password)
{
	char       *encrypted_password;
	pgphHashKey key;
	pgphEntry  *entry;
	TimestampTz dt_now = GetCurrentTimestamp();

	Assert(username != NULL);
	Assert(password != NULL);

	if (password_reuse_history == 0 && password_reuse_interval == 0)
		return;

	/* Safety check... */
	if (!pgph || !pgph_hash)
		return;

	/* Encrypt the password to the requested format. */
	encrypted_password = strdup(str_to_sha256(password, username));

	/* Store the password into share memory and password history file */
	/* Set up key for hashtable search */
        strcpy(key.rolename, username) ;
        strcpy(key.password_hash, encrypted_password);

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgph->lock, LW_EXCLUSIVE);

	/* Create new entry, if not present */
	entry = (pgphEntry *) hash_search(pgph_hash, &key, HASH_FIND, NULL);
	if (!entry)
	{
		dt_now = GetCurrentTimestamp();

		elog(DEBUG1, "Add new entry in history hash table: (%s, '%s', '%s')",
							username, encrypted_password,
							timestamptz_to_str(dt_now));

		/* OK to create a new hashtable entry */
		entry = pgph_entry_alloc(&key, dt_now);

		/* Flush the new entry to disk */
		if (entry)
		{
			elog(DEBUG1, "entry added, flush change to disk");
			flush_password_history();
		}
	}

	LWLockRelease(pgph->lock);

	free(encrypted_password);
}

static void
rename_user_in_history(const char *username, const char *newname)
{
        pgphEntry  *entry;
	HASH_SEQ_STATUS hash_seq;
	int         num_changed = 0;

	if (password_reuse_history == 0 && password_reuse_interval == 0)
		return;

	Assert(username != NULL);
	Assert(newname != NULL);

        /* Safety check ... shouldn't get here unless shmem is set up. */
        if (!pgph || !pgph_hash)
                return;

	elog(DEBUG1, "renaming user %s to %s into password history", username, newname);

	LWLockAcquire(pgph->lock, LW_EXCLUSIVE);

        hash_seq_init(&hash_seq, pgph_hash);
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
		/* update the key of matching entries */
                if (strcmp(entry->key.rolename, username) == 0)
                {
			pgphHashKey key;
			strcpy(key.rolename, newname) ;
			strcpy(key.password_hash, entry->key.password_hash);
			hash_update_hash_key(pgph_hash, entry, &key);
			num_changed++;
                }
        }

	if (num_changed > 0)
	{
		elog(DEBUG1, "%d entries in paswword history hash table have been mofidied for user %s",
													num_changed,
													username);

		/* Flush the new entry to disk */
		flush_password_history();
	}

	LWLockRelease(pgph->lock);
}

/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
        TimestampTz l_password_date = (*(pgphEntry *const *) lhs)->password_date;
        TimestampTz r_password_date = (*(pgphEntry *const *) rhs)->password_date;

        if (l_password_date < r_password_date)
                return -1;
        else if (l_password_date > r_password_date)
                return +1;
        else
                return 0;
}

static void
remove_password_from_history(const char *username, const char *password, int numentries)
{
	char         *encrypted_password;
        int32         num_entries;
        int32         num_user_entries = 0;
        int32         num_removed = 0;
        pgphEntry    *entry;
	HASH_SEQ_STATUS hash_seq;
	pgphEntry   **entries;
	int           i = 0;


	if (password_reuse_history == 0 && password_reuse_interval == 0)
		return;

	Assert(username != NULL);
	Assert(password != NULL);

        /* Safety check ... shouldn't get here unless shmem is set up. */
        if (!pgph || !pgph_hash)
                return;

	/* Encrypt the password to the requested format. */
	encrypted_password = strdup(str_to_sha256(password, username));

	elog(DEBUG1, "attempting to remove historized password = '%s' for user = '%s'", encrypted_password, username);

	LWLockAcquire(pgph->lock, LW_EXCLUSIVE);

        num_entries = hash_get_num_entries(pgph_hash);
        hash_seq_init(&hash_seq, pgph_hash);

	entries = palloc(num_entries * sizeof(pgphEntry *));

	/* stores entries related to the username to be sorted by date */
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
                if (strcmp(entry->key.rolename, username) == 0)
			entries[i++] = entry;
	}

	if (i == 0)
	{
		elog(DEBUG1, "no entry in the history for user: %s", username);

		LWLockRelease(pgph->lock);

		pfree(entries);

		return;
	}

	num_user_entries = i;

	/* Sort into increasing order by date */
	qsort(entries, i, sizeof(pgphEntry *), entry_cmp);

	/*
	 * Remove the oldest tuples when password_reuse_history is reached
	 * until password_reuse_history size is respected for this user,
	 * except if password_reuse_interval is enabled and not reached.
	 *
	 * A ascending index must exits on the date column of the table,
	 * we use this index to treat the oldest entries first in the scan.
	 */
	for (i = 0; i < num_user_entries; i++)
        {
		bool keep = false;

		/* if we have a retention delay remove entries that has expired */
		if (password_reuse_interval > 0)
		{
			TimestampTz     dt_now = GetCurrentTimestamp();
			float8          result;

			result = ((float8) (dt_now - entries[i]->password_date)) / 1000000.0; /* in seconds */
			result /= 86400; /* in days */

			elog(DEBUG1, "password_reuse_interval: %d, entry age: %d",
										password_reuse_interval,
										(int) result);
			/*
			 * When the delay have not expired, keep the entry if the
			 * number of entry exceed password_reuse_history
			 */
			if (password_reuse_interval >= (int) result)
				keep = true;
			else
				elog(DEBUG1, "remove_password_from_history(): this history entry has expired");
		}

		if (!keep)
		{
			/* we need to remove the entries that exceed history size */
			if ((num_user_entries - i) >= password_reuse_history)
			{
				elog(DEBUG1, "removing entry %d from the history (%s, %s)", i,
											entries[i]->key.rolename,
											entries[i]->key.password_hash);
				hash_search(pgph_hash, &entries[i]->key, HASH_REMOVE, NULL);
				num_removed++;
			}
		}
	}
	pfree(entries);

	/* Flush the new entry to disk */
	if (num_removed > 0)
		flush_password_history();

	LWLockRelease(pgph->lock);
}

static void
remove_user_from_history(const char *username)
{
        int32       num_removed = 0;
        pgphEntry  *entry;
	HASH_SEQ_STATUS hash_seq;

	if (password_reuse_history == 0 && password_reuse_interval == 0)
		return;

	Assert(username != NULL);

        /* Safety check ... shouldn't get here unless shmem is set up. */
        if (!pgph || !pgph_hash)
                return;

	elog(DEBUG1, "removing user %s from password history", username);

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgph->lock, LW_EXCLUSIVE);

        hash_seq_init(&hash_seq, pgph_hash);

	/* Sequential scan of the hash table to find the entries to remove */
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
		if (strcmp(entry->key.rolename, username) == 0)
		{
			hash_search(pgph_hash, &entry->key, HASH_REMOVE, NULL);
			num_removed++;
		}
	}

	/* Flush the new entry to disk */
	if (num_removed > 0)
		flush_password_history();

	LWLockRelease(pgph->lock);
}

/* Check if the password can be reused */
static bool
check_password_reuse(const char *username, const char *password)
{
	int           count_in_history = 0;
	pgphEntry    *entry;
	bool          found = false;
	char         *encrypted_password;
	HASH_SEQ_STATUS hash_seq;

	Assert(username != NULL);

	if (password == NULL)
		return false;

	if (password_reuse_history == 0 && password_reuse_interval == 0)
		return false;

	/* Safety check... */
	if (!pgph || !pgph_hash)
		return false;

	/* Encrypt the password to the requested format. */
	encrypted_password = strdup(str_to_sha256(password, username));

	elog(DEBUG1, "Looking for registered password = '%s' for username = '%s'", encrypted_password, username);

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgph->lock, LW_SHARED);

        hash_seq_init(&hash_seq, pgph_hash);
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
                if (strcmp(entry->key.rolename, username) == 0)
                {
			/* if the password is found in the history remove it if the interval is passed */
			if (strcmp(encrypted_password, entry->key.password_hash) == 0)
			{
				elog(DEBUG1, "password found in history, username = '%s',"
					     " password: '%s', saved at date: '%s'", username, 
									     entry->key.password_hash,
									     timestamptz_to_str(entry->password_date));

				/* mark that the password hash was found in the history */
				found = true;

				/* Check the password age again the reuse interval */
				if (password_reuse_interval > 0)
				{
					TimestampTz       dt_now = GetCurrentTimestamp();
					float8          result;
					result = ((float8) (dt_now - entry->password_date)) / 1000000.0; /* in seconds */
					result /= 86400; /* in days */
					elog(DEBUG1, "password_reuse_interval: %d, entry age: %d",
												password_reuse_interval,
												(int) result);

					/*
					 * if the delay have expired skip the entry, it will be
					 * removed later in remove_password_from_history()
					 */
					if (password_reuse_interval < (int) result)
					{
						elog(DEBUG1, "this history entry has expired");
						found = false;
						count_in_history--;
					}
				}
			}

			/*
			 * Even if the password was found we continue to count the number of
			 * password stored in the history for this user. This count is used
			 * to remove the oldest password that exceed the password_reuse_history
			 */
			count_in_history++;
                }
	}

	LWLockRelease(pgph->lock);

	free(encrypted_password);

	if (found)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				errmsg(gettext_noop("Cannot use this credential following the password reuse policy"))));

	/* Password not found, remove passwords exceeding the history size */
	remove_password_from_history(username, password, count_in_history);

	/* The password was not found, add the password to the history */
	return true;
}
#endif

/* Return the number of days between current timestamp and the date given as parameter */
static int
check_valid_until(char *valid_until_date)
{
	int days = 0;

	elog(DEBUG1, "option VALID UNTIL date: %s", valid_until_date);

	if (valid_until_date)
	{
		Datum           validUntil_datum;
		TimestampTz       dt_now = GetCurrentTimestamp();
		TimestampTz       valid_date;
		float8          result;

		validUntil_datum = DirectFunctionCall3(timestamptz_in,
									CStringGetDatum(valid_until_date),
									ObjectIdGetDatum(InvalidOid),
									Int32GetDatum(-1));
		valid_date = DatumGetTimestampTz(validUntil_datum);

		result = ((float8) (valid_date - dt_now)) / 1000000.0; /* in seconds */
		result /= 86400; /* in days */
		days = (int) result;

		elog(DEBUG1, "option VALID UNTIL in days: %d", days);
	}

	return days;
}

static void
check_password(const char *username, const char *password,
                           PasswordType password_type, Datum validuntil_time,
                           bool validuntil_null)
{

	switch (password_type)
	{
		case PASSWORD_TYPE_PLAINTEXT:
		{
#ifdef USE_CRACKLIB
			const char *reason;
#endif
			if (is_in_whitelist((char *)username))
				break;

			statement_has_password = true;
			username_check(username, password);
			if (password != NULL)
			{
				password_check(username, password);
#ifdef USE_CRACKLIB
				/* call cracklib to check password */
				if ((reason = FascistCheck(password, CRACKLIB_DICTPATH)))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("password is easily cracked"),
							 errdetail_log("cracklib diagnostic: %s", reason)));
#endif
			}
			break;
		}
		default:
			if (!encrypted_password_allowed)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
						errmsg(gettext_noop("password type is not a plain text"))));
			break;
	}
}

void
_PG_init(void)
{
	/* Defined GUCs */
	username_guc();
	password_guc();

	if (process_shared_preload_libraries_in_progress)
	{
		DefineCustomIntVariable("credcheck.history_max_size",
					gettext_noop("maximum of entries in the password history"), NULL,
					&pgph_max, 65535, 1, (INT_MAX / 1024), PGC_POSTMASTER, 0,
					NULL, NULL, NULL);

		DefineCustomIntVariable("credcheck.auth_failure_cache_size",
					gettext_noop("maximum of entries in the auth failure cache"), NULL,
					&pgaf_max, 1024, 1, (INT_MAX / 1024), PGC_POSTMASTER, 0,
					NULL, NULL, NULL);
	}

	DefineCustomBoolVariable("credcheck.no_password_logging",
				gettext_noop("prevent exposing the password in error messages logged"),
				NULL, &no_password_logging, true, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomIntVariable("credcheck.max_auth_failure",
				gettext_noop("maximum number of authentication failure before"
				" the user loggin account be invalidated"), NULL,
				&fail_max, 0, 0, 64, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomBoolVariable("credcheck.reset_superuser",
				gettext_noop("restore superuser acces when he have been banned."),
				NULL, &reset_superuser, false, PGC_SIGHUP, 0,
				NULL, NULL, NULL);

	DefineCustomBoolVariable("credcheck.encrypted_password_allowed",
				gettext_noop("allow encrypted password to be used or throw an error"),
				NULL, &encrypted_password_allowed, false, PGC_SUSET, 0,
				NULL, NULL, NULL);

	DefineCustomStringVariable(
				"credcheck.whitelist",
				gettext_noop("comma separated list of username to exclude from password policy check"), NULL,
				&username_whitelist, "", PGC_SUSET, 0, check_whitelist, NULL, NULL);

	DefineCustomIntVariable("credcheck.auth_delay_ms",
				"Milliseconds to delay before reporting authentication failure",
				NULL,
				&auth_delay_milliseconds,
				0,
				0, INT_MAX / 1000,
				PGC_SIGHUP,
				GUC_UNIT_MS,
				NULL,
				NULL,
				NULL);

	MarkGUCPrefixReserved("credcheck");

#if PG_VERSION_NUM < 150000
        /*
         * Request additional shared resources.  (These are no-ops if we're not in
         * the postmaster process.)  We'll allocate or attach to the shared
         * resources in pgph_shmem_startup().
         */
        RequestAddinShmemSpace(pgph_memsize());
        RequestNamedLWLockTranche(PGPH_TRANCHE_NAME, 1);
        RequestAddinShmemSpace(pgaf_memsize());
        RequestNamedLWLockTranche(PGAF_TRANCHE_NAME, 1);
#endif

	/* Install hooks */
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = cc_ProcessUtility;
	prev_check_password_hook = check_password_hook;
	check_password_hook = check_password;
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pghist_shmem_request;
#endif
        prev_shmem_startup_hook = shmem_startup_hook;
        shmem_startup_hook = pghist_shmem_startup;

	prev_log_hook = emit_log_hook;
	emit_log_hook = fix_log;

	prev_ClientAuthentication = ClientAuthentication_hook;
	ClientAuthentication_hook = credcheck_max_auth_failure;
}

void
_PG_fini(void)
{
	/* Uninstall hooks */
	check_password_hook = prev_check_password_hook;
	ProcessUtility_hook = prev_ProcessUtility;
	emit_log_hook = prev_log_hook;
#if PG_VERSION_NUM >= 150000
	shmem_request_hook = prev_shmem_request_hook;
#endif
	shmem_startup_hook = prev_shmem_startup_hook;
	ClientAuthentication_hook = prev_ClientAuthentication;
}

static void
cc_ProcessUtility(PEL_PROCESSUTILITY_PROTO)
{
	Node *parsetree = pstmt->utilityStmt;

	/* Execute the utility command before */
	if (prev_ProcessUtility)
		prev_ProcessUtility(PEL_PROCESSUTILITY_ARGS);
	else
		standard_ProcessUtility(PEL_PROCESSUTILITY_ARGS);

	statement_has_password = false;

	switch (nodeTag(parsetree))
	{
		/* Intercept ALTER USER .. RENAME statements */
		case T_RenameStmt:
		{
			RenameStmt *stmt = (RenameStmt *)parsetree;
			/* We only take care of user renaming */
			if (stmt->renameType == OBJECT_ROLE && stmt->newname != NULL)
			{
				if (is_in_whitelist(stmt->newname) || is_in_whitelist(stmt->subname))
					break;
			
				/* check the validity of the username */
				username_check(stmt->newname, NULL);

#if PG_VERSION_NUM >= 120000
				/* rename the user in the history table */
				rename_user_in_history(stmt->subname, stmt->newname);
#endif
			}
			break;
		}

		case T_AlterRoleStmt:
		{
			AlterRoleStmt *stmt = (AlterRoleStmt *)parsetree;
			ListCell      *option;
			char          *password;
			bool           save_password = false;
			DefElem    *dpassword = NULL;
			DefElem    *dvalidUntil = NULL;

			if (is_in_whitelist(stmt->role->rolename))
				break;

			/* Extract options from the statement node tree */
			foreach(option, stmt->options)
			{
				DefElem    *defel = (DefElem *) lfirst(option);

				if (strcmp(defel->defname, "password") == 0)
				{
					dpassword = defel;
				}
				else if (strcmp(defel->defname, "validUntil") == 0)
				{
					dvalidUntil = defel;
				}
			}

#if PG_VERSION_NUM >= 120000
			if (dpassword && dpassword->arg)
			{
				statement_has_password = true;
				password = strVal(dpassword->arg);
				save_password = check_password_reuse(stmt->role->rolename, password);
			}
#endif
			if (dvalidUntil && dvalidUntil->arg && password_valid_until > 0)
			{
				int valid_until = check_valid_until(strVal(dvalidUntil->arg));
				if (valid_until < password_valid_until)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
							errmsg(gettext_noop("the VALID UNTIL option must have a date older than %d days"), password_valid_until)));
			}
			if (dvalidUntil && dvalidUntil->arg && password_valid_max > 0)
			{
				int valid_max = check_valid_until(strVal(dvalidUntil->arg));
				if (valid_max > password_valid_max)
					ereport(ERROR,
						(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
							errmsg(gettext_noop("the VALID UNTIL option must NOT have a date beyond %d days"), password_valid_max)));
			}

			/* The password can be saved into the history */
			if (save_password)
				save_password_in_history(stmt->role->rolename, password);
			break;
		}

		case T_CreateRoleStmt:
		{
			CreateRoleStmt *stmt = (CreateRoleStmt *)parsetree;
			ListCell       *option;
			int             valid_until = 0;
			int             valid_max = 0;
			bool            has_valid_until = false; 
			bool            save_password = false;
			char           *password;
			DefElem    *dpassword = NULL;
			DefElem    *dvalidUntil = NULL;

			if (is_in_whitelist(stmt->role))
				break;

			/* check the validity of the username */
			username_check(stmt->role, NULL);

			/* Extract options from the statement node tree */
			foreach(option, stmt->options)
			{
				DefElem    *defel = (DefElem *) lfirst(option);

				if (strcmp(defel->defname, "password") == 0)
				{
					dpassword = defel;
				}
				else if (strcmp(defel->defname, "validUntil") == 0)
				{
					dvalidUntil = defel;
				}
			}

#if PG_VERSION_NUM >= 120000
			if (dpassword && dpassword->arg)
			{
				statement_has_password = true;
				password = strVal(dpassword->arg);
				save_password = check_password_reuse(stmt->role, password);
			}
#endif
			if (dvalidUntil && dvalidUntil->arg && password_valid_until > 0)
			{
				valid_until = check_valid_until(strVal(dvalidUntil->arg));
				has_valid_until = true;
			}
			if (dvalidUntil && dvalidUntil->arg && password_valid_max > 0)
			{
				valid_max = check_valid_until(strVal(dvalidUntil->arg));
				has_valid_until = true;
			}

			/* check that a VALID UNTIL option is present */
			if ( !has_valid_until && (password_valid_until > 0 || password_valid_max > 0) )
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
						errmsg(gettext_noop("require a VALID UNTIL option"))));

			/* check that a minimum number of days for password validity is defined */
			if (password_valid_until > 0 && valid_until < password_valid_until)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
						errmsg(gettext_noop("require a VALID UNTIL option with a date older than %d days"), password_valid_until)));

			/* check that a maximum number of days for password validity is defined */
			if (password_valid_max > 0 && valid_max > password_valid_max)
				ereport(ERROR,
					(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
						errmsg(gettext_noop("require a VALID UNTIL option with a date beyond %d days"), password_valid_max)));
			/* The password can be saved into the history */
			if (save_password)
				save_password_in_history(stmt->role, password);
			break;
		}

#if PG_VERSION_NUM >= 120000
		case T_DropRoleStmt:
		{
			DropRoleStmt *stmt = (DropRoleStmt *)parsetree;
			ListCell   *item;

			foreach(item, stmt->roles)
			{
				RoleSpec   *rolspec = lfirst(item);

				remove_user_from_history(rolspec->rolename);
			}
			break;
		}
#endif
		default:
			break;
	}
}

#if PG_VERSION_NUM >= 120000
#if PG_VERSION_NUM >= 140000
char *
str_to_sha256(const char *password, const char *salt)
{
	int          password_len = strlen(password);
	int          saltlen = strlen(salt);
	uint8        checksumbuf[PG_SHA256_DIGEST_LENGTH];
	char        *result = palloc0(sizeof (char) * PG_SHA256_DIGEST_STRING_LENGTH);
	pg_hmac_ctx *hmac_ctx = pg_hmac_create(PG_SHA256);

	if (hmac_ctx == NULL)
	{
		pfree(result);
		elog(ERROR, gettext_noop("credcheck could not initialize checksum context"));
	}

	if (pg_hmac_init(hmac_ctx, (uint8 *) password, password_len) < 0 ||
			pg_hmac_update(hmac_ctx, (uint8 *) salt, saltlen) < 0 ||
			pg_hmac_final(hmac_ctx, checksumbuf, sizeof(checksumbuf)) < 0)
	{
		pfree(result);
		pg_hmac_free(hmac_ctx);
		elog(ERROR, gettext_noop("credcheck could not initialize checksum"));
	}
	hex_encode((char *) checksumbuf, sizeof checksumbuf, result);
	result[PG_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';

	pg_hmac_free(hmac_ctx);

	return result;
}
#else
char *
str_to_sha256(const char *password, const char *salt)
{
	int          password_len = strlen(password);
	uint8        checksumbuf[PG_SHA256_DIGEST_LENGTH];
	char        *result = palloc0(sizeof (char) * PG_SHA256_DIGEST_STRING_LENGTH);
	pg_sha256_ctx sha256_ctx;

	pg_sha256_init(&sha256_ctx);
	pg_sha256_update(&sha256_ctx, (uint8 *) password, password_len);
	pg_sha256_final(&sha256_ctx, checksumbuf);
	hex_encode((char *) checksumbuf, sizeof checksumbuf, result);
	result[PG_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';

	return result;
}
#endif
#endif

/****
 * Password history feature
 ****/

/*
 * Estimate shared memory space needed for password history.
 */
static Size
pgph_memsize(void)
{
	Size            size;

	size = MAXALIGN(sizeof(pgphSharedState));
	size = add_size(size, hash_estimate_size(pgph_max, sizeof(pgphEntry)));

	return size;
}

/*
 * Estimate shared memory space needed for auth failure history.
 */
static Size
pgaf_memsize(void)
{
	Size            size;

	size = MAXALIGN(sizeof(pgafSharedState));
	size = add_size(size, hash_estimate_size(pgaf_max, sizeof(pgafEntry)));

	return size;
}


#if PG_VERSION_NUM >= 150000
static void
pghist_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	/*
	 * If you change code here, don't forget to also report the modifications in
	 * _PG_init() for pg14 and below.
	 */
	RequestAddinShmemSpace(pgph_memsize());
	RequestNamedLWLockTranche(PGPH_TRANCHE_NAME, 1);
	RequestAddinShmemSpace(pgaf_memsize());
	RequestNamedLWLockTranche(PGAF_TRANCHE_NAME, 1);
}
#endif


static void
pghist_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	pgph_shmem_startup();

	pgaf_shmem_startup();
}

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing password history from text file
 * or create it (even if empty) while the module is enabled.
 */
static void
pgph_shmem_startup(void)
{
	bool        found;
	HASHCTL     info;
	FILE       *file = NULL;
	uint32      header;
	int32       pgphver;
	int32       num;
	int32       i;

	/* reset in case this is a restart within the postmaster */
	pgph = NULL;
	pgph_hash = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgph = ShmemInitStruct("pg_password_history",
						   sizeof(pgphSharedState),
						   &found);

	if (!found)
	{
		/* First time through ... */
		pgph->lock = &(GetNamedLWLockTranche(PGPH_TRANCHE_NAME))->lock;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgphHashKey);
	info.entrysize = sizeof(pgphEntry);
	pgph_hash = ShmemInitHash("pg_password_history hash",
							  pgph_max, pgph_max,
							  &info,
							  HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
		return;

	/*
	 * Note: we don't bother with locks here, because there should be no other
	 * processes running when this code is reached.
	 */

	/*
	 * Attempt to load old history from the dump file.
	 */
	file = AllocateFile(PGPH_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;
		/* No existing persisted stats file, so we're done */
		return;
	}

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		fread(&pgphver, sizeof(uint32), 1, file) != 1 ||
		fread(&num, sizeof(int32), 1, file) != 1)
		goto read_error;

	if (header != PGPH_FILE_HEADER || pgphver != PGPH_VERSION)
		goto data_error;

	for (i = 0; i < num; i++)
	{
		pgphEntry   temp;
		pgphEntry  *entry;

		if (fread(&temp, sizeof(pgphEntry), 1, file) != 1)
		{
			ereport(LOG,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ignoring invalid data in pg_password_history file \"%s\"",
							PGPH_DUMP_FILE)));
			goto fail;
		}

		/* make the hashtable entry (discards old entries if too many) */
		entry = pgph_entry_alloc(&temp.key, temp.password_date);
		if (!entry)
			goto fail;
	}
	FreeFile(file);
 
	pgph->num_entries = i + 1;

	return;

read_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_password_history file \"%s\": %m",
					PGPH_DUMP_FILE)));
	goto fail;
data_error:
	ereport(LOG,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("ignoring invalid data in file \"%s\"",
					PGPH_DUMP_FILE)));
fail:
	if (file)
		FreeFile(file);
}

static pgphEntry *
pgph_entry_alloc(pgphHashKey *key, TimestampTz password_date)
{
	pgphEntry  *entry;
	bool        found;

	if (hash_get_num_entries(pgph_hash) >= pgph_max)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("can not allocate enough memory for new entry in password history cache."),
				 errhint("You shoul increase credcheck.history_max_size.")));
		return NULL;
	}

	/* Find or create an entry with desired hash code */
	entry = (pgphEntry *) hash_search(pgph_hash, key, HASH_ENTER, &found);

	/* New entry, set the timestamp */
	if (!found)
		entry->password_date = password_date;

	return entry;
}

static pgafEntry *
pgaf_entry_alloc(pgafHashKey *key, float failure_count)
{
	pgafEntry  *entry;
	bool        found;

	if (hash_get_num_entries(pgaf_hash) >= pgph_max)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("can not allocate enough memory for new entry in auth failure cache."),
				 errhint("You shoul increase credcheck.history_max_size.")));
		return NULL;
	}

	/* Find or create an entry with desired hash code */
	entry = (pgafEntry *) hash_search(pgaf_hash, key, HASH_ENTER, &found);

	/* New entry */
	if (!found)
	{
		entry->failure_count = failure_count;
		if (failure_count >= fail_max)
			entry->banned_date = GetCurrentTimestamp();
	}

	return entry;
}


/*
 * Flush password history to disk.
 *
 * IMPORTANT: the caller is responsible to emit
 * an exclusive lock on pgph->lock otherwise the
 * file can be corrupted.
 */
static void
flush_password_history(void)
{
        FILE       *file;
        int32       num_entries;
        pgphEntry  *entry;
	HASH_SEQ_STATUS hash_seq;

        /* Safety check ... shouldn't get here unless shmem is set up. */
        if (!pgph || !pgph_hash)
                return;

	elog(DEBUG1, "flushing password history to file %s", PGPH_DUMP_FILE);

        file = AllocateFile(PGPH_DUMP_FILE ".tmp", PG_BINARY_W);
        if (file == NULL)
                goto error;

        if (fwrite(&PGPH_FILE_HEADER, sizeof(uint32), 1, file) != 1)
                goto error;
        if (fwrite(&PGPH_VERSION, sizeof(uint32), 1, file) != 1)
                goto error;
        num_entries = hash_get_num_entries(pgph_hash);
        if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
                goto error;

        hash_seq_init(&hash_seq, pgph_hash);
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
                if (fwrite(entry, sizeof(pgphEntry), 1, file) != 1)
                {
                        /* note: we assume hash_seq_term won't change errno */
                        hash_seq_term(&hash_seq);
                        goto error;
                }
        }
	/*
	 * Fill the file until a size divisible by page size 8192
	 * to fix a complain of pgBackRest backup: file size X is
	 * not divisible by page size 8192
	 */
	fseek(file, 0, SEEK_END);
	while ((ftell(file) % BLCKSZ) != 0)
		putc(0, file);

	/* close the file */
        if (FreeFile(file))
        {
                file = NULL;
                goto error;
        }

	elog(DEBUG1, "history hash table written to disk");

        /*
         * Rename file into place, so we atomically replace any old one.
         */
        (void) durable_rename(PGPH_DUMP_FILE ".tmp", PGPH_DUMP_FILE, LOG);

        return;

error:
        ereport(LOG,
                        (errcode_for_file_access(),
                         errmsg("could not write password history file \"%s\": %m",
                                        PGPH_DUMP_FILE ".tmp")));
        if (file)
                FreeFile(file);

        unlink(PGPH_DUMP_FILE ".tmp");
}

static void
pgaf_shmem_startup(void)
{
	bool        found;
	HASHCTL     info;

	/* reset in case this is a restart within the postmaster */
	pgaf = NULL;
	pgaf_hash = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgaf = ShmemInitStruct("pg_auth_failure_history",
						   sizeof(pgafSharedState),
						   &found);

	if (!found)
	{
		/* First time through ... */
		pgaf->lock = &(GetNamedLWLockTranche(PGAF_TRANCHE_NAME))->lock;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgafHashKey);
	info.entrysize = sizeof(pgafEntry);
	pgaf_hash = ShmemInitHash("pg_auth_failure_history hash",
							  pgaf_max, pgaf_max,
							  &info,
							  HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

PG_FUNCTION_INFO_V1(pg_password_history_reset);

/*
 * Reset password history.
 */
Datum
pg_password_history_reset(PG_FUNCTION_ARGS)
{
	char       *username;
	int         num_removed = 0;
	HASH_SEQ_STATUS hash_seq;
        pgphEntry  *entry;

        /* Safety check... */
        if (!pgph || !pgph_hash)
                return 0;

        /* Only superusers can reset the history */
	if (!superuser())
		ereport(ERROR, (errmsg("only superuser can reset password history")));

	/* Get the username to filter the entries to remove if one specified */
	if (PG_NARGS() > 0)
		username = PG_GETARG_CSTRING(0);
	else
		username = NULL;

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgph->lock, LW_EXCLUSIVE);

        hash_seq_init(&hash_seq, pgph_hash);

	/* Sequential scan of the hash table to find the entries to remove */
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
		if (username == NULL || strcmp(entry->key.rolename, username) == 0)
		{
			hash_search(pgph_hash, &entry->key, HASH_REMOVE, NULL);
			num_removed++;
		}
	}

	/* Flush the new entry to disk */
	if (num_removed > 0)
		flush_password_history();

	LWLockRelease(pgph->lock);

        PG_RETURN_INT32(num_removed);
}

PG_FUNCTION_INFO_V1(pg_password_history);

/*
 * Show content of the password history.
 */
Datum
pg_password_history(PG_FUNCTION_ARGS)
{
	pg_password_history_internal(fcinfo);

	return (Datum) 0;
}

/* Common code for all versions of pg_password_history() */
static void
pg_password_history_internal(FunctionCallInfo fcinfo)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc       tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	pgphEntry  *entry;

	/* Safety check... */
	if (!pgph || !pgph_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("credcheck must be loaded via shared_preload_libraries to use password history")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Get shared lock, iterate over the hashtable entries.
	 *
	 * With a large hash table, we might be holding the lock rather longer
	 * than one could wish.  However, this only blocks creation of new hash
	 * table entries, and the larger the hash table the less likely that is to
	 * be needed.
	 */
	LWLockAcquire(pgph->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgph_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum           values[PG_PASSWORD_HISTORY_COLS];
		bool            nulls[PG_PASSWORD_HISTORY_COLS];
		int                     i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = CStringGetDatum(entry->key.rolename);
		values[i++] = TimestampTzGetDatum(entry->password_date);
		values[i++] = CStringGetTextDatum(entry->key.password_hash);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	LWLockRelease(pgph->lock);

	tuplestore_donestoring(tupstore);
}

PG_FUNCTION_INFO_V1(pg_password_history_timestamp);

/*
 * Change the password_date of all entries in password history
 * for a specified user. Proposed for testing purpose only.
 */
Datum
pg_password_history_timestamp(PG_FUNCTION_ARGS)
{
	char       *username = PG_GETARG_CSTRING(0);
	TimestampTz new_timestamp = PG_GETARG_TIMESTAMPTZ(1);
        pgphEntry  *entry;
	int         num_changed = 0;
	HASH_SEQ_STATUS hash_seq;

        /* Safety check... */
        if (!pgph || !pgph_hash)
                return 0;

        /* Only superusers can reset the history */
	if (!superuser())
		ereport(ERROR, (errmsg("only superuser can change timestamp in password history")));

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgph->lock, LW_EXCLUSIVE);

        hash_seq_init(&hash_seq, pgph_hash);
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
		if (strcmp(entry->key.rolename, username) == 0)
                {
			entry->password_date = new_timestamp;
			num_changed++;
                }
        }

	/* Flush the new entry to disk */
	if (num_changed > 0)
		flush_password_history();

	LWLockRelease(pgph->lock);

        PG_RETURN_INT32(num_changed);
}

static void
fix_log(ErrorData *edata)
{
	if (edata->elevel != ERROR)
	{
		/* Continue chain to previous hook */
		if (prev_log_hook)
			(*prev_log_hook) (edata);
		return;
	}

        /*
	 * Error should not expose the password in the log.
	 */
	if (statement_has_password && no_password_logging)
		edata->hide_stmt = true;

	statement_has_password = false;

	/* Continue chain to previous hook */
	if (prev_log_hook)
		(*prev_log_hook) (edata);
}

static void
credcheck_max_auth_failure(Port *port, int status)
{

	/* Inject a short delay if authentication failed. */
	if (status != STATUS_OK)
		pg_usleep(1000L * auth_delay_milliseconds);

	/* check for max auth failure */
	if (fail_max > 0 && status != STATUS_EOF)
	{
		Oid userOid =  get_role_oid(port->user_name, true);

		if (userOid != InvalidOid)
		{
			float fail_num = get_auth_failure(port->user_name, userOid, status);

			/* register the auth failure if we not reach allowed max failure */
			if (status == STATUS_ERROR && fail_num <= fail_max)
				fail_num = save_auth_failure(port->user_name, userOid);

			/* reject, this account has been banned */
			if (fail_num >= fail_max)
			{
				/*
				 * if superuser have been banned, restore the access if requested
				 * through credcheck.reset_superuser and a configuration reload
				 */
				if (reset_superuser && userOid == 10)
					remove_auth_failure(port->user_name, userOid);
				else
					ereport(FATAL, (errmsg("rejecting connection, user '%s' has been banned", port->user_name)));
			}

			/* connection is ok and we have not reach the failure limit, let's reset the counter */
			if (status == STATUS_OK  && fail_num < fail_max)
				remove_auth_failure(port->user_name, userOid);
		}
	}

	if (prev_ClientAuthentication)
		prev_ClientAuthentication(port, status);

}

static float
get_auth_failure(const char *username, Oid userid, int status)
{
	pgafHashKey key;
	pgafEntry  *entry;
	float fail_cnt = 0;

	Assert(username != NULL);

	if (fail_max == 0)
		return 0;

	/* Safety check... */
	if (!pgaf || !pgaf_hash)
		return 0;

	/* Set up key for hashtable search */
        key.roleid = userid ;

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgaf->lock, LW_EXCLUSIVE);

	/* Create new entry, if not present */
	entry = (pgafEntry *) hash_search(pgaf_hash, &key, HASH_FIND, NULL);
	if (entry)
		fail_cnt = entry->failure_count;

	elog(DEBUG1, "Auth failure count for user %s is %f, fired by status: %d", username, fail_cnt, status);

	LWLockRelease(pgaf->lock);

	return fail_cnt;
}

static float
save_auth_failure(const char *username, Oid userid)
{
	pgafHashKey key;
	pgafEntry  *entry;
	float fail_cnt = 0.5;

	if (!EnableSSL)
		fail_cnt = 1;

	Assert(username != NULL);

	if (fail_max == 0)
		return 0;

	/* Safety check... */
	if (!pgaf || !pgaf_hash)
		return 0;

	/* Set up key for hashtable search */
        key.roleid = userid ;

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgaf->lock, LW_EXCLUSIVE);

	/* Create new entry, if not present */
	entry = (pgafEntry *) hash_search(pgaf_hash, &key, HASH_FIND, NULL);
	if (entry)
	{
		if (EnableSSL)
			fail_cnt = entry->failure_count + 0.5;
		else
			fail_cnt = entry->failure_count + 1;

		elog(DEBUG1, "Remove entry in auth failure hash table for user %s", username);
		hash_search(pgaf_hash, &entry->key, HASH_REMOVE, NULL);
	}
	elog(DEBUG1, "Add new entry in auth failure hash table for user %s (%d, %f)", username, userid, fail_cnt);

	/* OK to create a new hashtable entry */
	entry = pgaf_entry_alloc(&key, fail_cnt);

	LWLockRelease(pgaf->lock);

	return fail_cnt;
}

static void
remove_auth_failure(const char *username, Oid userid)
{
	pgafHashKey key;

	Assert(username != NULL);

	if (fail_max == 0)
		return;

	/* Safety check... */
	if (!pgaf || !pgaf_hash)
		return;

	/* Set up key for hashtable search */
        key.roleid = userid;

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgaf->lock, LW_EXCLUSIVE);

	elog(DEBUG1, "Remove entry in auth failure hash table for user %s", username);
	hash_search(pgaf_hash, &key, HASH_REMOVE, NULL);

	LWLockRelease(pgaf->lock);
}

PG_FUNCTION_INFO_V1(pg_banned_role_reset);

/*
 * Reset banned role cache.
 */
Datum
pg_banned_role_reset(PG_FUNCTION_ARGS)
{
	char       *username;
	int         num_removed = 0;
	HASH_SEQ_STATUS hash_seq;
        pgafEntry  *entry;

        /* Safety check... */
        if (!pgaf || !pgaf_hash)
                return 0;

        /* Only superusers can reset the history */
	if (!superuser())
		ereport(ERROR, (errmsg("only superuser can reset banned roles cache")));

	/* Get the username to filter the entries to remove if one specified */
	if (PG_NARGS() > 0)
		username = PG_GETARG_CSTRING(0);
	else
		username = NULL;

	/* Lookup the hash table entry with exclusive lock. */
	LWLockAcquire(pgaf->lock, LW_EXCLUSIVE);

        hash_seq_init(&hash_seq, pgaf_hash);

	/* Sequential scan of the hash table to find the entries to remove */
        while ((entry = hash_seq_search(&hash_seq)) != NULL)
        {
		if (username == NULL || (entry->key.roleid == get_role_oid(username, true)))
		{
			hash_search(pgaf_hash, &entry->key, HASH_REMOVE, NULL);
			num_removed++;
		}
	}

	LWLockRelease(pgaf->lock);

        PG_RETURN_INT32(num_removed);
}

PG_FUNCTION_INFO_V1(pg_banned_role);

/*
 * Show list of the banned role
 */
Datum
pg_banned_role(PG_FUNCTION_ARGS)
{
	pg_banned_role_internal(fcinfo);

	return (Datum) 0;
}

/* Common code for all versions of pg_banned_role() */
static void
pg_banned_role_internal(FunctionCallInfo fcinfo)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc       tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	pgafEntry  *entry;

	/* Safety check... */
	if (!pgaf || !pgaf_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("credcheck must be loaded via shared_preload_libraries to use auth failure feature")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Get shared lock, iterate over the hashtable entries.
	 *
	 * With a large hash table, we might be holding the lock rather longer
	 * than one could wish.  However, this only blocks creation of new hash
	 * table entries, and the larger the hash table the less likely that is to
	 * be needed.
	 */
	LWLockAcquire(pgaf->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgaf_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum           values[PG_BANNED_ROLE_COLS];
		bool            nulls[PG_BANNED_ROLE_COLS];
		int             i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = Int8GetDatum(entry->key.roleid);
		values[i++] = Int8GetDatum(entry->failure_count);
		if (entry->banned_date)
			values[i++] = TimestampTzGetDatum(entry->banned_date);
		else
			nulls[i++] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	LWLockRelease(pgaf->lock);

	tuplestore_donestoring(tupstore);
}



