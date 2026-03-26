/*-------------------------------------------------------------------------
 *
 * gp_hyperloglog_test.c
 *	  HyperLogLog algorithm test program
 *
 * Portions Copyright (c) 2026, Greengage Community.
 *
 *
 * IDENTIFICATION
 *		src/backend/utils/hyperloglog/test/gp_hyperloglog_test.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>

#include "cmockery.h"

#include "postgres.h"
#include "utils/hyperloglog/gp_hyperloglog.h"


#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"

#include <math.h>
#include <stdio.h>

#include "../gp_hyperloglog.c"

/*
	Uncomment the following to check a lot of variants. It will take a long time.
*/
/*	#define TEST_ACCURACY_LONG 1 */

/*
	Uncomment the following to report maximum error.
*/
/* #define REPORT_MAX_ERROR 1 */

#if REPORT_MAX_ERROR
float		max_error = 0;
#endif

static void
hll_statistical_accuracy(size_t cardinality, size_t step)
{
	GpHLLCounter hll = gp_hyperloglog_init_def();

	/*
	 * Insert unique values
	 */

	int			nitems = 0;

	for (uint64 i = 0; i < cardinality; i += step)
	{
		Datum		d = Int64GetDatum(i);

		hll = gp_hyperloglog_add_item(
									  hll,
									  d,
									  sizeof(uint64),	/* typlen */
									  true,		/* typbyval */
									  'd');		/* typalign */
		nitems++;
	}

	double		estimate = gp_hyperloglog_estimate(hll);

	double		rel_error = fabs(estimate - nitems) / nitems;

	printf("actual=%d estimate=%f rel_error=%f\n",
		   nitems,
		   estimate,
		   rel_error);

	/*
	 * Expected HLL error. Use the formula from the original paper. See
	 * gp_hyperloglog.h for the details.
	 */
	double		expected_error = 1.04 / sqrt(POW2(hll->b));

	/*
	 * Allow some safety margin (2x theoretical error)
	 */
	assert_true(rel_error < expected_error * 2);

#ifdef REPORT_MAX_ERROR
	if (rel_error > max_error)
	{
		max_error = rel_error;
	}
#endif
}

static void
test_hll_statistical_accuracy100K_1(void **state)
{
	hll_statistical_accuracy(100000, 1);
}

static void
test_hll_statistical_accuracy100K_2(void **state)
{
	hll_statistical_accuracy(100000, 2);
}

static void
test_hll_statistical_accuracy100K_3(void **state)
{
	hll_statistical_accuracy(100000, 3);
}

static void
test_hll_statistical_accuracy100K_4(void **state)
{
	hll_statistical_accuracy(100000, 4);
}

static void
test_hll_statistical_accuracy300K_1(void **state)
{
	hll_statistical_accuracy(300000, 1);
}

static void
test_hll_statistical_accuracy300K_2(void **state)
{
	hll_statistical_accuracy(300000, 2);
}

static void
test_hll_statistical_accuracy300K_3(void **state)
{
	hll_statistical_accuracy(300000, 3);

}

static void
test_hll_statistical_accuracy300K_4(void **state)
{
	hll_statistical_accuracy(300000, 4);
}

static void
test_hll_statistical_accuracy1M_1(void **state)
{
	hll_statistical_accuracy(1000000, 1);
}

static void
test_hll_statistical_accuracy1M_2(void **state)
{
	hll_statistical_accuracy(1000000, 2);
}

static void
test_hll_statistical_accuracy1M_3(void **state)
{
	hll_statistical_accuracy(1000000, 3);
}

static void
test_hll_statistical_accuracy1M_4(void **state)
{
	hll_statistical_accuracy(1000000, 4);
}

static void
test_hll_statistical_accuracy1M_5(void **state)
{
	hll_statistical_accuracy(1000000, 5);
}

static void
test_hll_statistical_accuracy1M_6(void **state)
{
	hll_statistical_accuracy(1000000, 6);
}

static void
test_hll_statistical_accuracy10M_1(void **state)
{
	hll_statistical_accuracy(10000000, 1);
}

static void
test_hll_statistical_accuracy10M_2(void **state)
{
	hll_statistical_accuracy(10000000, 2);
}

static void
test_hll_statistical_accuracy10M_3(void **state)
{
	hll_statistical_accuracy(10000000, 3);
}

static void
test_hll_statistical_accuracy10M_4(void **state)
{
	hll_statistical_accuracy(10000000, 4);
}

static void
test_hll_statistical_accuracy10M_5(void **state)
{
	hll_statistical_accuracy(10000000, 5);
}

#ifdef TEST_ACCURACY_LONG
static void
test_hll_statistical_accuracy_var(void **state)
{
	for (int i = 100000; i <= 200000; i += 13)
	{
		printf("cardinality=%d\n", i);
		hll_statistical_accuracy(i, 3);
	}
}
#endif

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const		UnitTest tests[] = {

		unit_test(test_hll_statistical_accuracy100K_1),
		unit_test(test_hll_statistical_accuracy100K_2),
		unit_test(test_hll_statistical_accuracy100K_3),
		unit_test(test_hll_statistical_accuracy100K_4),


		unit_test(test_hll_statistical_accuracy300K_1),
		unit_test(test_hll_statistical_accuracy300K_2),
		unit_test(test_hll_statistical_accuracy300K_3),
		unit_test(test_hll_statistical_accuracy300K_4),

		unit_test(test_hll_statistical_accuracy1M_1),
		unit_test(test_hll_statistical_accuracy1M_2),
		unit_test(test_hll_statistical_accuracy1M_3),
		unit_test(test_hll_statistical_accuracy1M_4),
		unit_test(test_hll_statistical_accuracy1M_5),
		unit_test(test_hll_statistical_accuracy1M_6),

		unit_test(test_hll_statistical_accuracy10M_1),
		unit_test(test_hll_statistical_accuracy10M_2),
		unit_test(test_hll_statistical_accuracy10M_3),
		unit_test(test_hll_statistical_accuracy10M_4),
		unit_test(test_hll_statistical_accuracy10M_5),

		/*
		 * Enable the following to check a lot of variants. It will take a
		 * long time.
		 */
#ifdef TEST_ACCURACY_LONG
		unit_test(test_hll_statistical_accuracy_var),
#endif
	};

	MemoryContextInit();

	int			ret = run_tests(tests);

#ifdef REPORT_MAX_ERROR
	printf("max error: %g\n", max_error);
#endif

	return ret;
}
