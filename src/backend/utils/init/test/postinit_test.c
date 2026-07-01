#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Fetch definition of PG_exception_stack */
#include "postgres.h"

#undef PG_RE_THROW
#define PG_RE_THROW() siglongjmp(*PG_exception_stack, 1)

static void
_errfinish_impl()
{
	PG_RE_THROW();
}

#define EXPECT_EREPORT(LOG_LEVEL)     \
	if (__builtin_constant_p(LOG_LEVEL) && (LOG_LEVEL) >= ERROR) \
	{ \
		expect_value(errstart_cold, elevel, (LOG_LEVEL)); \
		expect_any(errstart_cold, domain); \
	} \
	else \
	{ \
		expect_value(errstart, elevel, (LOG_LEVEL)); \
		expect_any(errstart, domain); \
	} \
	if (LOG_LEVEL < ERROR) \
	{ \
		will_return(errstart, false); \
	} \
    else \
    { \
		if (__builtin_constant_p(LOG_LEVEL)) \
			will_return_with_sideeffect(errstart_cold, false, &_errfinish_impl, NULL); \
		else \
			will_return_with_sideeffect(errstart, false, &_errfinish_impl, NULL); \
    } \

#include "../postinit.c"

static void
test_check_superuser_connection_limit_error(void **state)
{
	am_ftshandler = false;

	expect_value(HaveNFreeProcs, n, RESERVED_FTS_CONNECTIONS);
	will_return(HaveNFreeProcs, false);

	EXPECT_EREPORT(FATAL);

	/*
	 * Expect ERROR
	 */
	PG_TRY();
	{
		check_superuser_connection_limit();
		fail();
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
}

static void
test_check_superuser_connection_limit_ok_with_free_procs(void **state)
{
	am_ftshandler = false;

	expect_value(HaveNFreeProcs, n, RESERVED_FTS_CONNECTIONS);
	will_return(HaveNFreeProcs, true);

	/*
	 * Expect OK
	 */
	check_superuser_connection_limit();
}

static void
test_check_superuser_connection_limit_ok_for_ftshandler(void **state)
{
	am_ftshandler = true;

	/*
	 * Expect OK
	 */
	check_superuser_connection_limit();
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_check_superuser_connection_limit_ok_with_free_procs),
		unit_test(test_check_superuser_connection_limit_ok_for_ftshandler),
		unit_test(test_check_superuser_connection_limit_error),
	};

	return run_tests(tests);
}
