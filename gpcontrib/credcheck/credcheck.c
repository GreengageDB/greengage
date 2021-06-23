/*
 * credcheck
 */

#include <ctype.h>
#include <limits.h>
#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "libpq/crypt.h"
#include "commands/user.h"

PG_MODULE_MAGIC;

extern void _PG_init(void);

// username flags
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

// password flags
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

  for (int i = 0; i < strlen(str); i++) {
    occurred = 1;
    // first character = str[i]
    // second character = str[i+1]
    // hunt for a series of same character
    // for example, in this string "weekend summary"
    // search for the series "ee", "mm"
    for (int j = (i + 1), k = 1; j < strlen(str); j++, k++) {
      // character matched
      if (str[i] == str[j]) {
        // is the previous, current character positions are adjacent
        //
        if (i + k == j) {
          occurred++;
          if (occurred > max_repeat) {
            return true;
          }
        }
      } else {
        break;
      }
    }
  }
  return false;
}

static void username_check(const char *username, const char *password) {

  int user_total_special = 0;
  int user_total_digit = 0;
  int user_total_upper = 0;
  int user_total_lower = 0;

  char *tmp_pass = NULL;
  char *tmp_user = NULL;
  char *tmp_contains = NULL;
  char *tmp_not_contains = NULL;

  // checks
  //
  // checks has to be done by ignoring case
  if (username_ignore_case) {
    tmp_pass = to_nlower(password, INT_MAX);
    tmp_user = to_nlower(username, INT_MAX);
    tmp_contains = to_nlower(username_contain, INT_MAX);
    tmp_not_contains = to_nlower(username_not_contain, INT_MAX);
  } else {
    tmp_pass = strndup(password, INT_MAX);
    tmp_user = strndup(username, INT_MAX);
    tmp_contains = strndup(username_contain, INT_MAX);
    tmp_not_contains = strndup(username_not_contain, INT_MAX);
  }

  // 1
  // username length
  if (strnlen(tmp_user, INT_MAX) < username_min_length) {
    elog(ERROR, gettext_noop("username length should match the configured "
                             "credcheck.username_min_length"));
    goto clean;
  }

  // 2
  // username contains password
  if (username_contain_password) {
    if (strstr(tmp_user, tmp_pass)) {
      elog(ERROR, gettext_noop("username should not contain password"));
      goto clean;
    }
  }

  // 3
  // contain characters
  // if credcheck.username_contain is not an empty string
  if (strncmp(tmp_contains, "", strlen(tmp_contains)) != 0) {
    if (str_contains(tmp_contains, tmp_user) == false) {
      elog(ERROR, gettext_noop("username does not contain the configured "
                               "credcheck.username_contain characters"));
      goto clean;
    }
  }

  // 4
  // not contain characters
  // if credcheck.username_not_contain is not an empty string
  if (strncmp(tmp_not_contains, "", strlen(tmp_not_contains)) != 0) {
    if (str_contains(tmp_not_contains, tmp_user) == true) {
      elog(ERROR, gettext_noop("username does contain the configured "
                               "credcheck.username_not_contain characters"));
      goto clean;
    }
  }

  check_str_counters(tmp_user, &user_total_lower, &user_total_upper,
                     &user_total_digit, &user_total_special);

  // 5
  // total upper characters
  if (!username_ignore_case && user_total_upper < username_min_upper) {
    elog(ERROR, gettext_noop("username does not contain the configured "
                             "credcheck.username_min_upper characters"));
    goto clean;
  }

  // 6
  // total lower characters
  if (!username_ignore_case && user_total_lower < username_min_lower) {
    elog(ERROR, gettext_noop("username does not contain the configured "
                             "credcheck.username_min_lower characters"));
    goto clean;
  }

  // 7
  // total digits
  if (user_total_digit < username_min_digit) {
    elog(ERROR, gettext_noop("username does not contain the configured "
                             "credcheck.username_min_digit characters"));
    goto clean;
  }

  // 8
  // total special
  if (user_total_special < username_min_special) {
    elog(ERROR, gettext_noop("username does not contain the configured "
                             "credcheck.username_min_special characters"));
    goto clean;
  }

  // 9
  // minium char repeat
  if (username_min_repeat) {
    if (char_repeat_exceeds(tmp_user, username_min_repeat)) {
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

static void password_check(const char *username, const char *password) {

  int pass_total_special = 0;
  int pass_total_digit = 0;
  int pass_total_upper = 0;
  int pass_total_lower = 0;

  char *tmp_pass = NULL;
  char *tmp_user = NULL;
  char *tmp_contains = NULL;
  char *tmp_not_contains = NULL;

  // checks
  //
  // checks has to be done by ignoring case
  if (password_ignore_case) {
    tmp_pass = to_nlower(password, INT_MAX);
    tmp_user = to_nlower(username, INT_MAX);
    tmp_contains = to_nlower(password_contain, INT_MAX);
    tmp_not_contains = to_nlower(password_not_contain, INT_MAX);
  } else {
    tmp_pass = strndup(password, INT_MAX);
    tmp_user = strndup(username, INT_MAX);
    tmp_contains = strndup(password_contain, INT_MAX);
    tmp_not_contains = strndup(password_not_contain, INT_MAX);
  }

  // 1
  // password length
  if (strnlen(tmp_pass, INT_MAX) < password_min_length) {
    elog(ERROR, gettext_noop("password length should match the configured "
                             "credcheck.password_min_length"));
    goto clean;
  }

  // 2
  // password contains username
  if (password_contain_username) {
    if (strstr(tmp_pass, tmp_user)) {
      elog(ERROR, gettext_noop("password should not contain username"));
      goto clean;
    }
  }

  // 3
  // contain characters
  // if credcheck.password_contain is not an empty string
  if (strncmp(tmp_contains, "", strlen(tmp_contains)) != 0) {
    if (str_contains(tmp_contains, tmp_pass) == false) {
      elog(ERROR, gettext_noop("password does not contain the configured "
                               "credcheck.password_contain characters"));
      goto clean;
    }
  }

  // 4
  // not contain characters
  // if credcheck.password_not_contain is not an empty string
  if (strncmp(tmp_not_contains, "", strlen(tmp_not_contains)) != 0) {
    if (str_contains(tmp_not_contains, tmp_pass) == true) {
      elog(ERROR, gettext_noop("password does contain the configured "
                               "credcheck.password_not_contain characters"));
      goto clean;
    }
  }

  check_str_counters(tmp_pass, &pass_total_lower, &pass_total_upper,
                     &pass_total_digit, &pass_total_special);

  // 5
  // total upper characters
  if (!password_ignore_case && pass_total_upper < password_min_upper) {
    elog(ERROR, gettext_noop("password does not contain the configured "
                             "credcheck.password_min_upper characters"));
    goto clean;
  }

  // 6
  // total lower characters
  if (!password_ignore_case && pass_total_lower < password_min_lower) {
    elog(ERROR, gettext_noop("password does not contain the configured "
                             "credcheck.password_min_lower characters"));
    goto clean;
  }

  // 7
  // total digits
  if (pass_total_digit < password_min_digit) {
    elog(ERROR, gettext_noop("password does not contain the configured "
                             "credcheck.password_min_digit characters"));
    goto clean;
  }

  // 8
  // total special
  if (pass_total_special < password_min_special) {
    elog(ERROR, gettext_noop("password does not contain the configured "
                             "credcheck.password_min_special characters"));
    goto clean;
  }

  // 9
  // minium char repeat
  if (password_min_repeat) {
    if (char_repeat_exceeds(tmp_pass, password_min_repeat)) {
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

static void check_password(const char *username, const char *password,
                           PasswordType password_type, Datum validuntil_time,
                           bool validuntil_null) {
  switch (password_type) {
  case PASSWORD_TYPE_PLAINTEXT:

    username_check(username, password);
    password_check(username, password);

    break;

  default:
    elog(ERROR, "password type is not a plain text");
    break;
  }
}

void _PG_init(void) {

  static bool inited = false;

  if (inited) {
    return;
  }
  username_guc();
  password_guc();

  check_password_hook = check_password;

  inited = true;
}
