/*-------------------------------------------------------------------------
 *
 * credcheck.c:
 * 		This file has the general PostgreSQL credential checks.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2021: MigOps Inc
 *
 *-------------------------------------------------------------------------
 */
#include <ctype.h>
#include <limits.h>

#include "postgres.h"

#if PG_VERSION_NUM < 120000
#include "access/heapam.h"
#include "access/htup_details.h"
#else
#include "access/table.h"
#endif

#include "catalog/catalog.h"
#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "nodes/nodes.h"
#include "tcop/utility.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "nodes/pg_list.h"


#if PG_VERSION_NUM < 120000
#define table_open(r,l)         heap_open(r,l)
#define table_openrv(r,l)       heap_openrv(r,l)
#define table_close(r,l)        heap_close(r,l)
#endif

#if PG_VERSION_NUM < 100000
#error Minimum version of PostgreSQL required is 10
#endif

/* Define ProcessUtility hook proto/parameters following the PostgreSQL version */
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

PG_MODULE_MAGIC;

/* Hooks */
static check_password_hook_type prev_check_password_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Functions */
extern void _PG_init(void);
extern void _PG_fini(void);
static void cc_ProcessUtility(PEL_PROCESSUTILITY_PROTO);

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

  for (size_t i = 0; i < len;) {
    occurred = 1;
    // first character = str[i]
    // second character = str[i+1]
    // search for an adjacent repeated characters
    // for example, in this string "weekend summary"
    // search for the series "ee", "mm"
    for (size_t j = (i + 1), k = 1; j < len; j++, k++) {
      // character matched
      if (str[i] == str[j]) {
        // is the previous, current character positions are adjacent
        if (i + k == j) {
          occurred++;
          if (occurred > max_repeat) {
            return true;
          }
        }
      }

      // if we reach an end of the string, no need to process further
      if (j + 1 == len) {
        return false;
      }

      // if the characters are not equal then point "i" to "j"
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
		elog(ERROR, gettext_noop("username length should match the configured "
				     "credcheck.username_min_length"));
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
			elog(ERROR, gettext_noop("username should not contain password"));
			goto clean;
		}
	}

	/* Rule 3: contain characters */
	if (tmp_contains != NULL && strlen(tmp_contains) > 0)
	{
		if (str_contains(tmp_contains, tmp_user) == false)
		{
			elog(ERROR, gettext_noop("username does not contain the configured "
					       "credcheck.username_contain characters"));
			goto clean;
		}
	}

	/* Rule 4: not contain characters */
	if (tmp_not_contains != NULL && strlen(tmp_not_contains) > 0)
	{
		if (str_contains(tmp_not_contains, tmp_user) == true)
		{
			elog(ERROR, gettext_noop("username contains the configured "
					       "credcheck.username_not_contain unauthorized characters"));
			goto clean;
		}
	}

	check_str_counters(tmp_user, &user_total_lower, &user_total_upper,
		     &user_total_digit, &user_total_special);

	/* Rule 5: total upper characters */
	if (!username_ignore_case && user_total_upper < username_min_upper)
	{
		elog(ERROR, gettext_noop("username does not contain the configured "
				     "credcheck.username_min_upper characters"));
		goto clean;
	}

	/* Rule 6: total lower characters */
	if (!username_ignore_case && user_total_lower < username_min_lower)
	{
		elog(ERROR, gettext_noop("username does not contain the configured "
				     "credcheck.username_min_lower characters"));
		goto clean;
	}

	/* Rule 7: total digits */
	if (user_total_digit < username_min_digit)
	{
		elog(ERROR, gettext_noop("username does not contain the configured "
				     "credcheck.username_min_digit characters"));
		goto clean;
	}

	/* Rule 8: total special */
	if (user_total_special < username_min_special)
	{
		elog(ERROR, gettext_noop("username does not contain the configured "
				     "credcheck.username_min_special characters"));
		goto clean;
	}

	/* Rule 9: minimum char repeat */
	if (username_min_repeat)
	{
		if (char_repeat_exceeds(tmp_user, username_min_repeat))
		{
			elog(ERROR,
			   gettext_noop("username characters are repeated more than the "
					"configured credcheck.username_min_repeat times"));
			goto clean;
		}
	}
	clean:

	free(tmp_pass);
	free(tmp_user);
	free(tmp_contains);
	free(tmp_not_contains);
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
		elog(ERROR, gettext_noop("password length should match the configured "
				     "credcheck.password_min_length"));
		goto clean;
	}

	/* Rule 2: password contains username */
	if (password_contain_username)
	{
		if (strstr(tmp_pass, tmp_user))
		{
			elog(ERROR, gettext_noop("password should not contain username"));
			goto clean;
		}
	}

	/* Rule 3: contain characters */
	if (tmp_contains != NULL && strlen(tmp_contains) > 0)
	{
		if (str_contains(tmp_contains, tmp_pass) == false)
		{
			elog(ERROR, gettext_noop("password does not contain the configured "
					       "credcheck.password_contain characters"));
			goto clean;
		}
	}

	/* Rule 4: not contain characters */
	if (tmp_not_contains != NULL && strlen(tmp_not_contains) > 0)
	{
		if (str_contains(tmp_not_contains, tmp_pass) == true)
		{
			elog(ERROR, gettext_noop("password contains the configured "
					       "credcheck.password_not_contain unauthorized characters"));
			goto clean;
		}
	}

	check_str_counters(tmp_pass, &pass_total_lower, &pass_total_upper,
		     &pass_total_digit, &pass_total_special);

	/* Rule 5: total upper characters */
	if (!password_ignore_case && pass_total_upper < password_min_upper)
	{
		elog(ERROR, gettext_noop("password does not contain the configured "
				     "credcheck.password_min_upper characters"));
		goto clean;
	}

	/* Rule 6: total lower characters */
	if (!password_ignore_case && pass_total_lower < password_min_lower)
	{
		elog(ERROR, gettext_noop("password does not contain the configured "
				     "credcheck.password_min_lower characters"));
		goto clean;
	}

	/* Rule 7: total digits */
	if (pass_total_digit < password_min_digit)
	{
		elog(ERROR, gettext_noop("password does not contain the configured "
				     "credcheck.password_min_digit characters"));
		goto clean;
	}

	/* Rule 8: total special */
	if (pass_total_special < password_min_special)
	{
		elog(ERROR, gettext_noop("password does not contain the configured "
				     "credcheck.password_min_special characters"));
		goto clean;
	}

	/* Rule 9: minimum char repeat */
	if (password_min_repeat)
	{
		if (char_repeat_exceeds(tmp_pass, password_min_repeat))
		{
			elog(ERROR,
			   gettext_noop("password characters are repeated more than the "
					"configured credcheck.password_min_repeat times"));
			goto clean;
		}
	}

	clean:

	free(tmp_pass);
	free(tmp_user);
	free(tmp_contains);
	free(tmp_not_contains);
}

static void username_guc() {
  DefineCustomIntVariable("credcheck.username_min_length",
                          gettext_noop("minimum username length"), NULL,
                          &username_min_length, 1, 1, INT_MAX, PGC_USERSET, 0,
                          NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.username_min_special",
                          gettext_noop("minimum username special characters"),
                          NULL, &username_min_special, 0, 0, INT_MAX,
                          PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.username_min_digit",
                          gettext_noop("minimum username digits"), NULL,
                          &username_min_digit, 0, 0, INT_MAX, PGC_USERSET, 0,
                          NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.username_min_upper",
                          gettext_noop("minimum username uppercase letters"),
                          NULL, &username_min_upper, 0, 0, INT_MAX, PGC_USERSET,
                          0, NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.username_min_lower",
                          gettext_noop("minimum username lowercase letters"),
                          NULL, &username_min_lower, 0, 0, INT_MAX, PGC_USERSET,
                          0, NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.username_min_repeat",
                          gettext_noop("minimum username characters repeat"),
                          NULL, &username_min_repeat, 0, 0, INT_MAX,
                          PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("credcheck.username_contain_password",
                           gettext_noop("username contains password"), NULL,
                           &username_contain_password, true, PGC_USERSET, 0,
                           NULL, NULL, NULL);

  DefineCustomBoolVariable("credcheck.username_ignore_case",
                           gettext_noop("ignore case while username checking"),
                           NULL, &username_ignore_case, false, PGC_USERSET, 0,
                           NULL, NULL, NULL);

  DefineCustomStringVariable(
      "credcheck.username_not_contain",
      gettext_noop("username should not contain these characters"), NULL,
      &username_not_contain, "", PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomStringVariable(
      "credcheck.username_contain",
      gettext_noop("password should contain these characters"), NULL,
      &username_contain, "", PGC_USERSET, 0, NULL, NULL, NULL);
}

static void password_guc() {
  DefineCustomIntVariable("credcheck.password_min_length",
                          gettext_noop("minimum password length"), NULL,
                          &password_min_length, 1, 1, INT_MAX, PGC_USERSET, 0,
                          NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.password_min_special",
                          gettext_noop("minimum special characters"), NULL,
                          &password_min_special, 0, 0, INT_MAX, PGC_USERSET, 0,
                          NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.password_min_digit",
                          gettext_noop("minimum password digits"), NULL,
                          &password_min_digit, 0, 0, INT_MAX, PGC_USERSET, 0,
                          NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.password_min_upper",
                          gettext_noop("minimum password uppercase letters"),
                          NULL, &password_min_upper, 0, 0, INT_MAX, PGC_USERSET,
                          0, NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.password_min_lower",
                          gettext_noop("minimum password lowercase letters"),
                          NULL, &password_min_lower, 0, 0, INT_MAX, PGC_USERSET,
                          0, NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.password_min_repeat",
                          gettext_noop("minimum password characters repeat"),
                          NULL, &password_min_repeat, 0, 0, INT_MAX,
                          PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("credcheck.password_contain_username",
                           gettext_noop("password contains username"), NULL,
                           &password_contain_username, true, PGC_USERSET, 0,
                           NULL, NULL, NULL);

  DefineCustomBoolVariable("credcheck.password_ignore_case",
                           gettext_noop("ignore case while password checking"),
                           NULL, &password_ignore_case, false, PGC_USERSET, 0,
                           NULL, NULL, NULL);

  DefineCustomStringVariable(
      "credcheck.password_not_contain",
      gettext_noop("password should not contain these characters"), NULL,
      &password_not_contain, "", PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomStringVariable(
      "credcheck.password_contain",
      gettext_noop("password should contain these characters"), NULL,
      &password_contain, "", PGC_USERSET, 0, NULL, NULL, NULL);
}

static void
check_password(const char *username, const char *password,
                           PasswordType password_type, Datum validuntil_time,
                           bool validuntil_null)
{
	switch (password_type)
	{
		case PASSWORD_TYPE_PLAINTEXT:
			username_check(username, password);
			password_check(username, password);
			break;

		default:
			elog(ERROR, "password type is not a plain text");
			break;
	}
}

void
_PG_init(void)
{
	/* Defined GUCs */
	username_guc();
	password_guc();

	/* Install hooks */
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = cc_ProcessUtility;
	prev_check_password_hook = check_password_hook;
	check_password_hook = check_password;
}

void
_PG_fini(void)
{
	/* Uninstall hooks */
	check_password_hook = prev_check_password_hook;
	ProcessUtility_hook = prev_ProcessUtility;
}

static void
cc_ProcessUtility(PEL_PROCESSUTILITY_PROTO)
{
	Node *parsetree = pstmt->utilityStmt;

	switch (nodeTag(parsetree))
	{
		/* Intercept ALTER USER .. RENAME statements */
		case T_RenameStmt:
		{
			RenameStmt *stmt = (RenameStmt *)parsetree;
			/* We only take care of user renaming */
			if (stmt->renameType == OBJECT_ROLE && stmt->newname != NULL)
			{
				/* only user with password are checked */
				HeapTuple       oldtuple;
				TupleDesc       dsc;
				Relation        rel;
				bool            isnull;
				Form_pg_authid  authform;

				rel = table_open(AuthIdRelationId, RowExclusiveLock);
				dsc = RelationGetDescr(rel);

				oldtuple = SearchSysCache1(AUTHNAME, CStringGetDatum(stmt->subname));
				if (!HeapTupleIsValid(oldtuple))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("role \"%s\" does not exist", stmt->subname)));
				authform = (Form_pg_authid) GETSTRUCT(oldtuple);
				/*
				 * Check that this is not a system role or a 
				 * role into the reserved "pg_" namespace.
				 */
				if (IsReservedName(NameStr(authform->rolname)))
					ereport(ERROR,
							(errcode(ERRCODE_RESERVED_NAME),
							 errmsg("role name \"%s\" is reserved",
									NameStr(authform->rolname)),
							 errdetail("Role names starting with \"pg_\" are reserved.")));

				if (IsReservedName(stmt->newname))
					ereport(ERROR,
							(errcode(ERRCODE_RESERVED_NAME),
							 errmsg("role name \"%s\" is reserved",
									stmt->newname),
							 errdetail("Role names starting with \"pg_\" are reserved.")));

				/* look if the password is null */
				(void) heap_getattr(oldtuple, Anum_pg_authid_rolpassword, dsc, &isnull);
				ReleaseSysCache(oldtuple);
				table_close(rel, NoLock);
				if (isnull)
					break;
				/* check the validity of the username */
				username_check(stmt->newname, NULL);
			}
			break;
		}

		default:
			break;
	}
	
	/* Execute the utility command, we are not concerned */
	if (prev_ProcessUtility)
		prev_ProcessUtility(PEL_PROCESSUTILITY_ARGS);
	else
		standard_ProcessUtility(PEL_PROCESSUTILITY_ARGS);
}
