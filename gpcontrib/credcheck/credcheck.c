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

//#undef OPENSSL_API_COMPAT

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "access/sysattr.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif

#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "commands/user.h"
#if PG_VERSION_NUM >= 140000
#include "common/hmac.h"
#endif
#include "common/sha2.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "utils/inval.h"

#define Password_encryption = PASSWORD_TYPE_SCRAM_SHA_256;

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

#if PG_VERSION_NUM >= 120000
/*
 password_reuse_history:
	number of distinct passwords set before a password can be reused.
 password_reuse_interval:
	amount of time it takes before a password can be reused again.
*/
static int password_reuse_history = 0;
static int password_reuse_interval = 0;

typedef struct FormData_password_reuse_history
{
        NameData rolename;
        TimestampTz password_date;
        text password_text;
} FormData_password_reuse_history;

typedef FormData_password_reuse_history *Form_password_reuse_history;

#define CATALOG_PASSWORD_HIST  "pg_auth_history"
#define CREDCHECK_SCHEMA       "credcheck"
#define PASSWORD_HIST_DATE_IDX "pg_auth_history_password_date_rolename_idx"

enum Anum_pg_auth_history
{
	Anum_pgauthist_rolename = 1,
	Anum_pgauthist_date,
	Anum_pgauthist_password,
};

char *str_to_sha256(const char *str, const char *salt);
#endif

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

#if PG_VERSION_NUM >= 120000
  DefineCustomIntVariable("credcheck.password_reuse_history",
                          gettext_noop("minimum number of password changes before permitting reuse"),
                          NULL, &password_reuse_history, 0, 0, 100,
                          PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomIntVariable("credcheck.password_reuse_interval",
                          gettext_noop("minimum number of days elapsed before permitting reuse"),
                          NULL, &password_reuse_interval, 0, 0, 730, /* max 2 years */
                          PGC_USERSET, 0, NULL, NULL, NULL);
#endif
}

#if PG_VERSION_NUM >= 120000
static void
save_password_in_history(const char *username, const char *password)
{
	RangeVar     *rv;
	Relation      rel;
	HeapTuple     tuple;
	Datum         new_record[3] = {0};
	bool          new_record_nulls[3] = {false};
	NameData      uname;
	char         *encrypted_password;

	if (password_reuse_history == 0 && password_reuse_interval == 0)
		return;

	/* Encrypt the password to the requested format. */
	encrypted_password = strdup(str_to_sha256(password, username));

	rv = makeRangeVar(CREDCHECK_SCHEMA, CATALOG_PASSWORD_HIST, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	elog(DEBUG1, "Insert new record in history table: (%s, '%s', '%s')",
							username, encrypted_password,
							timestamptz_to_str(GetCurrentTimestamp()));

	/* Build a tuple to insert */
	namestrcpy(&uname, username);
	new_record[Anum_pgauthist_rolename - 1] = NameGetDatum(&uname);
	new_record[Anum_pgauthist_date - 1] = TimestampGetDatum(GetCurrentTimestamp());
	new_record[Anum_pgauthist_password - 1] = CStringGetTextDatum(encrypted_password);
	tuple = heap_form_tuple(RelationGetDescr(rel), new_record, new_record_nulls);
	CatalogTupleInsert(rel, tuple);

	table_close(rel, RowExclusiveLock);
	free(encrypted_password);
}

static void
rename_user_in_history(const char *username, const char *newname)
{
	RangeVar     *rv;
	Relation      rel;
	HeapTuple     tuple;
	ScanKeyData   key[1];
	SysScanDesc   scan;

	rv = makeRangeVar(CREDCHECK_SCHEMA, CATALOG_PASSWORD_HIST, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Define scanning */
	ScanKeyInit(&key[0], Anum_pgauthist_rolename, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(username));

	/* Start search of user entries into relation ordered by creation date */
	scan = systable_beginscan(rel, 0, true, NULL, lengthof(key), key);
	/* Remove all entries from the history table for this user */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		FormData_password_reuse_history *ph = (FormData_password_reuse_history *) GETSTRUCT(tuple);
		NameData        uname;
		Datum           new_values[3];
		bool            new_values_nulls[3] = {false};
		HeapTuple       new_tuple;
		const char     *passwd;

		passwd = text_to_cstring(&(ph->password_text));

		elog(DEBUG1, "Renaming user %s with password '%s' into password history table with new username: %s", username, passwd, newname);

		namestrcpy(&uname, newname);
		new_values[Anum_pgauthist_rolename - 1] = NameGetDatum(&uname);
		new_values[Anum_pgauthist_date - 1] = TimestampTzGetDatum(ph->password_date);
		new_values[Anum_pgauthist_password - 1] = CStringGetDatum(passwd);
		new_tuple = heap_form_tuple(RelationGetDescr(rel), new_values, new_values_nulls);
		simple_heap_update(rel, &tuple->t_self, new_tuple);
	}
	/* Cleanup */
	systable_endscan(scan);

	table_close(rel, RowExclusiveLock);
}

static void
remove_password_from_history(const char *username, const char *password, int numentries)
{
	RangeVar     *rv;
	RangeVar     *irv;
	Relation      rel;
	Relation      irel;
	HeapTuple     tuple;
	ScanKeyData   key[1];
	SysScanDesc   scan;
	int           i = 0;
	char         *encrypted_password;

	/* Encrypt the password to the requested format. */
	encrypted_password = strdup(str_to_sha256(password, username));

	elog(DEBUG1, "Looking for removing password = '%s' for username = '%s'", encrypted_password, username);

	rv = makeRangeVar(CREDCHECK_SCHEMA, CATALOG_PASSWORD_HIST, -1);
	irv = makeRangeVar(CREDCHECK_SCHEMA, PASSWORD_HIST_DATE_IDX, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	irel = relation_openrv(irv, RowExclusiveLock);

	/* Define scanning */
	ScanKeyInit(&key[0], Anum_pgauthist_rolename, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(username));

	/* Start search of user entries into relation ordered by creation date */
	scan = systable_beginscan_ordered(rel, irel, NULL, lengthof(key), key);
	/*
	 * Remove the oldest tuples when password_reuse_history is reached
	 * until password_reuse_history size is respected for this user,
	 * except if password_reuse_interval is enabled and not reached.
	 *
	 * A ascending index must exits on the date column of the table,
	 * we use this index to treat the oldest entries first in the scan.
	 */
	while (HeapTupleIsValid(tuple = systable_getnext_ordered(scan, ForwardScanDirection)))
	{
		FormData_password_reuse_history *ph = (FormData_password_reuse_history *) GETSTRUCT(tuple);
		bool keep = false;

		/* if we have a retention delay remove entries that has expired */
		if (password_reuse_interval > 0)
		{
			Timestamp       dt_now = GetCurrentTimestamp();
			float8          result;
			result = ((float8) (dt_now - ph->password_date)) / 1000000.0; /* in seconds */
			result /= 86400; /* in days */

			elog(DEBUG1, "password_reuse_interval: %d, entry age: %d",
										password_reuse_interval,
										(int) result);
			/*
			 * if the delay have not expired keep the tuple if the
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
			if ((numentries - i) >= password_reuse_history)
			{
				elog(DEBUG1, "removing the entry from the history (%s, %s)",
												username,
												encrypted_password);
				simple_heap_delete(rel, &tuple->t_self);
			}
		}
		i++;
	}
	/* Cleanup */
	systable_endscan_ordered(scan);

	table_close(rel, RowExclusiveLock);
	relation_close(irel, RowExclusiveLock);
}

static void
remove_user_from_history(const char *username)
{
	RangeVar     *rv;
	Relation      rel;
	HeapTuple     tuple;
	ScanKeyData   key[1];
	SysScanDesc   scan;

	rv = makeRangeVar(CREDCHECK_SCHEMA, CATALOG_PASSWORD_HIST, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Define scanning */
	ScanKeyInit(&key[0], Anum_pgauthist_rolename, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(username));

	/* Start search of user entries into relation ordered by creation date */
	scan = systable_beginscan(rel, 0, true, NULL, lengthof(key), key);
	/* Remove all entries from the history table for this user */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		FormData_password_reuse_history *ph = (FormData_password_reuse_history *) GETSTRUCT(tuple);
		elog(DEBUG1, "Removing user from password history table tuple (%s, '%s')", username, timestamptz_to_str(TimestampTzGetDatum(ph->password_date)));
		simple_heap_delete(rel, &tuple->t_self);
	}
	/* Cleanup */
	systable_endscan(scan);

	table_close(rel, RowExclusiveLock);
}

/* Check if the password can be reused */
static void
check_password_reuse(const char *username, const char *password)
{
	RangeVar     *rv;
	Relation      rel;
	ScanKeyData   key[1];
	SysScanDesc   scan;
	HeapTuple     tuple;
	int           count_in_history;
	bool          found;
	char         *encrypted_password;

	if (password_reuse_history == 0 && password_reuse_interval == 0)
		return;

	/* Encrypt the password to the requested format. */
	encrypted_password = strdup(str_to_sha256(password, username));

	elog(DEBUG1, "Looking for registered password = '%s' for username = '%s'", encrypted_password, username);

	/* open the password history relation */
	rv = makeRangeVar(CREDCHECK_SCHEMA, CATALOG_PASSWORD_HIST, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Define scanning */
	ScanKeyInit(&key[0], Anum_pgauthist_rolename, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(username));

	/* Start search of user/password pair into relation */
	scan = systable_beginscan(rel, 0, true, NULL, lengthof(key), key);
	count_in_history = 0;
	found = false;
	/* Loop through all tuples to count the number of entries in the history for this user */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		FormData_password_reuse_history *ph = (FormData_password_reuse_history *) GETSTRUCT(tuple);
		const char *passwd;

		Assert(ph->rolename != NULL);
		Assert(ph->password_date != NULL);
		Assert(ph->password_text != NULL);

		passwd = text_to_cstring(&(ph->password_text));

		elog(DEBUG1, "Found in password history tuple with username = '%s',"
			     " password: '%s' cmp '%s', at date: '%s'", username, encrypted_password,
			     passwd, timestamptz_to_str(TimestampTzGetDatum(ph->password_date)));

		/* if the password is found in the history remove it if the interval is passed */
		if (strcmp(encrypted_password, passwd) == 0)
		{
			elog(DEBUG1, "password found in history");

			/* mark that the password hash was found in the history */
			found = true;

			if (password_reuse_interval > 0)
			{
				Timestamp       dt_now = GetCurrentTimestamp();
				float8          result;
				result = ((float8) (dt_now - ph->password_date)) / 1000000.0; /* in seconds */
				result /= 86400; /* in days */
				elog(DEBUG1, "password_reuse_interval: %d, entry age: %d",
											password_reuse_interval,
											(int) result);

				/* if the delay have expired skip the entry, it will be removed */
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
	/* Cleanup. */
	systable_endscan(scan);

	table_close(rel, RowExclusiveLock);
	free(encrypted_password);

	if (found)
		elog(ERROR, "Cannot use this credential following the password reuse policy");

	/* Password not found, remove passwords exceeding the history size */
	remove_password_from_history(username, password, count_in_history);

	/* The password was not found, add the password to the history */
	save_password_in_history(username, password);
}
#endif

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

#if PG_VERSION_NUM >= 120000
				/* rename the user in the history table */
				rename_user_in_history(stmt->subname, stmt->newname);
#endif
			}
			break;
		}

#if PG_VERSION_NUM >= 120000
		case T_AlterRoleStmt:
		{
			AlterRoleStmt *stmt = (AlterRoleStmt *)parsetree;
			ListCell      *option;

			/* Extract options from the statement node tree */
			foreach(option, stmt->options)
			{
				DefElem    *defel = (DefElem *) lfirst(option);

				if (strcmp(defel->defname, "password") == 0)
				{
					check_password_reuse(stmt->role->rolename, strVal(defel->arg));
				}
			}
			break;
		}

		case T_CreateRoleStmt:
		{
			CreateRoleStmt *stmt = (CreateRoleStmt *)parsetree;
			ListCell      *option;

			/* Extract options from the statement node tree */
			foreach(option, stmt->options)
			{
				DefElem    *defel = (DefElem *) lfirst(option);

				if (strcmp(defel->defname, "password") == 0)
				{
					check_password_reuse(stmt->role, strVal(defel->arg));
				}
			}
			break;
		}

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
	
	/* Execute the utility command, we are not concerned */
	if (prev_ProcessUtility)
		prev_ProcessUtility(PEL_PROCESSUTILITY_ARGS);
	else
		standard_ProcessUtility(PEL_PROCESSUTILITY_ARGS);
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
		elog(ERROR, "credcheck could not initialize checksum context");
	}

	if (pg_hmac_init(hmac_ctx, (uint8 *) password, password_len) < 0 ||
			pg_hmac_update(hmac_ctx, (uint8 *) salt, saltlen) < 0 ||
			pg_hmac_final(hmac_ctx, checksumbuf, sizeof(checksumbuf)) < 0)
	{
		pfree(result);
		pg_hmac_free(hmac_ctx);
		elog(ERROR, "credcheck could not initialize checksum");
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
