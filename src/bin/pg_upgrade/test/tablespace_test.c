#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../pg_upgrade.h"
#include "../tablespace.c"

static jmp_buf fatal_tag;

FILE *__wrap_fopen (const char *__restrict __filename,
			 const char *__restrict __modes);
char *__wrap_fgets (char *__restrict __s, int __n, FILE *__restrict __stream);
int __wrap_ferror (FILE *__stream);
int __wrap_fclose (FILE *__stream);
void __wrap_pg_fatal (const char* fmt, ...);

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

FILE
*__wrap_fopen (const char *__restrict __filename,
					const char *__restrict __modes)
{
	return (FILE *) mock();
}

char
*__wrap_fgets (char *__restrict __s, int __n, FILE *__restrict __stream)
{
	char *mock_text = (char*) mock();
	if (mock_text == NULL)
	{
		return NULL;
	}
	strncpy(__s, mock_text, __n);
	__s[__n - 1] = '\0';
	return (char *) mock_text;
}

int
__wrap_ferror (FILE *__stream)
{
	return mock();
}

int
__wrap_fclose (FILE *__stream)
{
	check_expected(__stream);
	return 0;
}

void
__wrap_pg_fatal (const char* fmt, ...)
{
	check_expected(fmt);
	longjmp(fatal_tag, 1);
}

static void
test__get_dbid (void **state)
{
	ClusterInfo clusterInfo = {0};
	clusterInfo.pgdata = "/tmp/pgdata";

	// Find a gp_dbid in the first line
	will_return(__wrap_fopen, 0x10);
	will_return(__wrap_fgets, "gp_dbid=1");
	will_return(__wrap_ferror, 0);
	expect_value(__wrap_fclose, __stream, 0x10);

	assert_int_equal(get_dbid(&clusterInfo), 1);

	// The gp_dbid line with spaces
	will_return(__wrap_fopen, 0x10);
	will_return(__wrap_fgets, "gp_dbid = 1");
	will_return(__wrap_ferror, 0);
	expect_value(__wrap_fclose, __stream, 0x10);
	
	assert_int_equal(get_dbid(&clusterInfo), 1);
	
	// Find a gp_dbid in the second line
	will_return(__wrap_fopen, 0x10);
	will_return(__wrap_fgets, "test=1");
	will_return(__wrap_fgets, "gp_dbid=10");
	will_return(__wrap_ferror, 0);
	expect_value(__wrap_fclose, __stream, 0x10);

	assert_int_equal(get_dbid(&clusterInfo), 10);
	
	// The fopen fail
	will_return(__wrap_fopen, NULL);
	expect_string(__wrap_pg_fatal, fmt,
				  "could not open file \"%s\": %m\n");
	if (setjmp(fatal_tag) == 0)
	{
		get_dbid(&clusterInfo);
		fail();
	}

	// The fgets fail with I/O error
	will_return(__wrap_fopen, 0x10);
	will_return(__wrap_fgets, NULL);
	will_return(__wrap_ferror, EIO);
	expect_string(__wrap_pg_fatal, fmt,
				  "An error occurred during %s file read: %m\n");
	if (setjmp(fatal_tag) == 0)
	{
		get_dbid(&clusterInfo);
		fail();
	}
	
	// Could not find a dbid in the file
	will_return(__wrap_fopen, 0x10);
	will_return(__wrap_fgets, "test=1");
	will_return(__wrap_fgets, NULL);
	will_return(__wrap_ferror, 0);
	expect_value(__wrap_fclose, __stream, 0x10);
	expect_string(__wrap_pg_fatal, fmt, "could not find dbid in %s\n");
	if (setjmp(fatal_tag) == 0)
	{
		get_dbid(&clusterInfo);
		fail();
	}
}

static void
test__set_tablespace_directory_suffix(void **state)
{
	ClusterInfo clusterInfo = {0};
	clusterInfo.pgdata = "/tmp/pgdata";
	
	// Greengage 6 cluster
	clusterInfo.major_version = GPDB6_MAJOR_PGVERSION;
	clusterInfo.dbid = 1;
	clusterInfo.controldata.cat_ver = 301908232;
	set_tablespace_directory_suffix(&clusterInfo);
	assert_string_equal(clusterInfo.tablespace_suffix, "/1/GPDB_6_301908232");

	// Greengage 7 cluster
	clusterInfo.major_version = GPDB7_MAJOR_PGVERSION;
	clusterInfo.dbid = 2;
	clusterInfo.controldata.cat_ver = 302307241;
	set_tablespace_directory_suffix(&clusterInfo);
	assert_string_equal(clusterInfo.tablespace_suffix, "/2/GPDB_7_302307241");
	
	// Unknown Greengage version
	clusterInfo.major_version = 150000;
	clusterInfo.dbid = 3;
	clusterInfo.controldata.cat_ver = 302610061;
	expect_string(__wrap_pg_fatal, fmt,
				  "Unknown or unsupported Greengage version: %s\n");
	if (setjmp(fatal_tag) == 0)
	{
		set_tablespace_directory_suffix(&clusterInfo);
		fail();
	}
}

int
main (int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__get_dbid),
			unit_test(test__set_tablespace_directory_suffix)
	};
	return run_tests(tests);
}
