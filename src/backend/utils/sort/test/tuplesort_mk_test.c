#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>

#include "cmockery.h"

#include "postgres.h"
#include "utils/elog.h"
#include "utils/tuplesort.h"
#include "utils/tuplesort_mk.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"

#include <sys/stat.h>

#define get_compare_function_for_ordering_op mock_get_compare_function_for_ordering_op
#define ScanKeyEntryInitialize mock_ScanKeyEntryInitialize

void run_sort_test(int nattrs, int nkeys);

void
mock_ScanKeyEntryInitialize(ScanKey entry,
					   int flags,
					   AttrNumber attributeNumber,
					   StrategyNumber strategy,
					   Oid subtype,
					   Oid collation,
					   RegProcedure procedure,
					   Datum argument);

#undef USE_ASSERT_CHECKING
#include "../tuplesort_mk.c"

#define NTEST_TUPLES 10

struct LogicalTapeSet
{
	BufFile    *pfile;			/* underlying file for whole tape set */
	long		nFileBlocks;	/* # of blocks used in underlying file */
	bool		forgetFreeSpace;		/* if we need to keep track of free
										 * space */
	bool		blocksSorted;	/* is freeBlocks[] currently in order? */
	long	   *freeBlocks;		/* resizable array */
	long		nFreeBlocks;	/* # of currently free blocks */
	long		freeBlocksLen;	/* current allocated length of freeBlocks[] */

	int			nTapes;			/* # of logical tapes in set */
};

bool
mock_get_compare_function_for_ordering_op(Oid opno, Oid *cmpfunc, bool *reverse)
{
	*cmpfunc = InvalidOid;
	*reverse = false;
	return true;
}


void
mock_ScanKeyEntryInitialize(ScanKey entry,
					   int flags,
					   AttrNumber attributeNumber,
					   StrategyNumber strategy,
					   Oid subtype,
					   Oid collation,
					   RegProcedure procedure,
					   Datum argument)
{
	entry->sk_flags = flags;
	entry->sk_attno = attributeNumber;
	entry->sk_strategy = strategy;
	entry->sk_subtype = subtype;
	entry->sk_collation = collation;
	entry->sk_argument = argument;

	FmgrInfo *finfo = &entry->sk_func;

	finfo->fn_oid = InvalidOid;
	finfo->fn_extra = NULL;
	finfo->fn_mcxt = CurrentMemoryContext;
	finfo->fn_expr = NULL;		/* caller may set this later */

	finfo->fn_nargs = 2; // Binary comparison in this test
	finfo->fn_strict = false;
	finfo->fn_addr = btint4cmp; //TODO use valid compare function
	finfo->fn_retset = false;
}

static void
test_tuplesort_mk_readtup_heap_fail_len(void **test_state)
{
	gp_debug_linger = 0;

	elog(LOG, "Running test: readtup_heap_fail_len");

	struct LogicalTapeSet lts = {.nFileBlocks = 42 * 1024};

	Tuplesortstate_mk state = {
		.sortcontext = CurrentMemoryContext,
		.status = TSS_SORTEDONTAPE,
		.tapeset = &lts,
		.nKeys = 42,
		.randomAccess = false,
		.memAllowed = 64LL * 1024 * 1024 * 1024,
		.maxTapes = 42,
		.tapeRange = 468
	};

	const char *expected_message = 
		"invalid tuple len 0. Sort method: external sort, space type: Disk, "
		"space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	char	   *message = NULL;
	bool		error_thrown = false;

	/* Check zero length */
	PG_TRY();
	{
		readtup_heap(&state, 0 /* pos */ , NULL /* stup */ , 
			NULL /* tape */ , 0		/* len, too small and no flag */ );
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);

	expected_message = 
		"invalid tuple len 2147483652. Sort method: external sort, "
		"space type: Disk, space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	message = NULL;
	error_thrown = false;
	state.sortcontext = CurrentMemoryContext;

	/* Check uint32 length */
	PG_TRY();
	{
		readtup_heap(&state, 0 /* pos */ , NULL /* stup */ , NULL /* tape */ , 
			sizeof(uint32) | MEMTUP_LEAD_BIT /* len, too small but with flag */);
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);
}

static void
test_tuplesort_mk_writetup_heap_fail_len(void **test_state)
{
	gp_debug_linger = 0;

	elog(LOG, "Running test: writetup_heap_fail_len");

	struct LogicalTapeSet lts = {.nFileBlocks = 42 * 1024};

	Tuplesortstate_mk state = {
		.sortcontext = CurrentMemoryContext,
		.status = TSS_SORTEDONTAPE,
		.tapeset = &lts,
		.nKeys = 42,
		.randomAccess = false,
		.memAllowed = 64LL * 1024 * 1024 * 1024,
		.maxTapes = 42,
		.tapeRange = 468
	};

	const char *expected_message = 
		"invalid tuple len 0. Sort method: external sort, space type: Disk, "
		"space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	char	   *message = NULL;
	bool		error_thrown = false;

	MemTupleData mtup = {
		.PRIVATE_mt_len = MEMTUP_LEAD_BIT | 0
	};

	MKEntry		entry = {
		.ptr = &mtup
	};

	/* Check zero length */
	PG_TRY();
	{
		writetup_heap(&state, 0, &entry);
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);

	mtup.PRIVATE_mt_len = MEMTUP_LEAD_BIT | sizeof(uint32);
	expected_message = 
		"invalid tuple len 0. Sort method: external sort, space type: Disk, "
		"space used: 1376256, sort nkeys=42, randomAccess=0, "
		"memAllowed=68719476736, maxTapes=42, tapeRange=468";
	message = NULL;
	error_thrown = false;
	state.sortcontext = CurrentMemoryContext;

	/* Check uint32 length */
	PG_TRY();
	{
		writetup_heap(&state, 0, &entry);
	}
	PG_CATCH();
	{
		message = elog_message();

		if (message != NULL)
		{
			error_thrown = true;
		}
	}
	PG_END_TRY();

	assert_true(message != NULL);
	if (message != NULL)
	{
		assert_string_equal(message, expected_message);
	}
	assert_true(error_thrown);
}

void run_sort_test(int nattrs, int nkeys)
{
	int i;
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "test_shm_mq worker");

    MemoryContext mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                               "mk_test",
                                               ALLOCSET_DEFAULT_SIZES);


	MemoryContext old_cxt = MemoryContextSwitchTo(mcxt);

	FormData_pg_attribute *attributes = (FormData_pg_attribute *)palloc0(sizeof(FormData_pg_attribute) * nattrs);
	Form_pg_attribute *attrs = (Form_pg_attribute *)palloc0(sizeof(Form_pg_attribute) * nattrs);
	for (i = 0; i < nattrs; i++) 
	{
		attributes[i].attrelid = InvalidOid;
		sprintf(attributes[i].attname.data, "att%d", i);
		attributes[i].atttypid = InvalidOid;
		attributes[i].attstattarget = -1;
		attributes[i].attndims = 0;
		attributes[i].atttypmod = -1;
		attributes[i].attcollation = InvalidOid;
		attributes[i].attlen = 4;
		attributes[i].attbyval = true;
		attributes[i].attalign = 'i';
		attributes[i].attcacheoff = -1;

		attrs[i] = &attributes[i];
	}
	
	TupleDesc tupdesc = (TupleDesc)palloc0(sizeof(struct tupleDesc));
	tupdesc->attrs = attrs;
	tupdesc->natts = 2;
	tupdesc->constr = NULL;
	tupdesc->tdhasoid = false;
	tupdesc->tdrefcount = -1;
	tupdesc->tdtypeid = -1;
	tupdesc->tdtypmod = -1;

    ScanState ss;

    AttrNumber *attNums = (AttrNumber *)palloc0(sizeof(AttrNumber) * nkeys);
	Oid *sortOperators = (Oid *)palloc0(sizeof(Oid) * nkeys);
	Oid *sortCollations = (Oid *)palloc0(sizeof(Oid) * nkeys);
	bool *nullsFirstFlags = (bool *)palloc0(sizeof(bool) * nkeys);
	for (int i = 0; i < nkeys; i++)
	{
		attNums[i] = i + 1;
		sortOperators[i] = Int4LessOperator;
		sortCollations[i] = InvalidOid;
		nullsFirstFlags[i] = false;
	}

    // Tuplesortstate_mk *sortstate = tuplesort_begin_heap_mk(&ss, tupdesc, nkeys, attNums,
    //                                 sortOperators,
    //                                 sortCollations,
    //                                 nullsFirstFlags,
    //                                 work_mem,
    //                                 false /* randomAccess */);

	Tuplesortstate_mk *sortstate = tuplesort_begin_heap_file_readerwriter_mk(
		&ss,
		"test_sort_mk",
		true,
		tupdesc,
		nkeys,
		attNums,
		sortOperators,
		sortCollations,
		nullsFirstFlags,
		work_mem,
		true
	);

	Datum		*values = (Datum *)palloc0(sizeof(Datum) * nattrs);
	bool		*isnull = (bool *)palloc0(sizeof(bool) * nattrs);

	for (i = 0; i < nattrs; i++)
	{
		values[i] = Int32GetDatum(0);
		isnull[i] = false;
	}

	// HeapTuple tuple = heap_form_tuple(&tupdesc, values, isnull);

	TupleTableSlot *slot = MakeTupleTableSlot();
	ExecSetSlotDescriptor(slot, tupdesc);

	slot->tts_tupleDescriptor = tupdesc;
	slot->PRIVATE_tts_heaptuple = NULL;
	slot->PRIVATE_tts_isnull = isnull;
	slot->PRIVATE_tts_values = values;

    /* Insert tuples in reverse order */
    for (i = NTEST_TUPLES; i > 0; i--)
    {
		values[0] = Int32GetDatum(NTEST_TUPLES - i);
		values[1] = Int32GetDatum(i);

		slot->PRIVATE_tts_heaptuple = heap_form_tuple(tupdesc, values, isnull);

		tuplesort_puttupleslot_mk(sortstate, slot);
    }

    tuplesort_performsort_mk(sortstate);

    /* Read back and verify ascending order */
    // int prev = -1;
    for (i = 0; ; i++)
    {
        Datum values[2];
        bool isnull[2];

		tuplesort_gettupleslot_mk(sortstate,
								  true,
								  slot);

		if (TupIsNull(slot))
			break;

		HeapTuple tuple = ExecFetchSlotHeapTuple(slot);
		heap_deform_tuple(tuple, tupdesc, values, isnull);

		int v0 = DatumGetInt32(values[0]);
		int v1 = DatumGetInt32(values[1]);
		
		printf("(%d,%d)", v0, v1);
    }
	printf("\n");

    tuplesort_end_mk(sortstate);
	MemoryContextSwitchTo(old_cxt);
    MemoryContextDelete(mcxt);
}

/*
 * Test: basic ascending sort of integers.
 */
static void
test_basic_int_sort(void **stateptr)
{
	int nkeys = 2;
	int nattrs = 2;

	mkdir("base", S_IRWXU);

	for (nattrs = 2; nattrs <= 30; nattrs++)
		for (nkeys = 1; nkeys <= nattrs; nkeys++)
		{
			printf("nkeys: %d, nattrs: %d\n", nkeys, nattrs);
			run_sort_test(nattrs, nkeys);
		}
}

#if 0
/*
 * Test: fuzz with random data lengths.
 */
static void
test_fuzz_random(void **stateptr)
{
    MemoryContext mcxt = AllocSetContextCreate(CurrentMemoryContext,
                                               "mk_test_fuzz",
                                               ALLOCSET_DEFAULT_SIZES);
    Tuplesortstate_mk *sortstate = mk_test_sort_state(mcxt, NULL, 32, true);
    int i;

    for (i = 0; i < NTEST_TUPLES; i++)
    {
        Datum values[2];
        bool isnull[2];

        values[0] = Int32GetDatum(random() % 10000);
        if (random() % 10 == 0)
        {
            values[1] = (Datum) 0;
            isnull[1] = true;
        }
        else
        {
            char buf[32];
            snprintf(buf, sizeof(buf), "val%ld", random() % 10000);
            values[1] = CStringGetTextDatum(buf);
            isnull[1] = false;
        }
        isnull[0] = false;

        tuplesort_putdatum_mk(sortstate, values[0], isnull[0]);
    }

    tuplesort_performsort_mk(sortstate);

    /* Verify monotonicity: previous <= current */
    Datum prev = Int32GetDatum(-1);
    for (i = 0; i < NTEST_TUPLES; i++)
    {
        Datum values[2];
        bool isnull[2];
        bool shouldFree = false;

        assert_true(tuplesort_getdatum_mk(sortstate, true, values, isnull));
        assert_true(DatumGetInt32(values[0]) >= DatumGetInt32(prev));
        prev = values[0];

        if (shouldFree)
            pfree(DatumGetPointer(values[1]));
    }

    tuplesort_end_mk(sortstate);
    MemoryContextDelete(mcxt);
}
#endif

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const		UnitTest tests[] = {
		unit_test(test_tuplesort_mk_readtup_heap_fail_len),
		unit_test(test_tuplesort_mk_writetup_heap_fail_len),
		unit_test(test_basic_int_sort)
		// unit_test(test_fuzz_random)
	};

	MemoryContextInit();

	DataDir = ".";

	PGPROC proc;
	MyProc = &proc;


	// Workfile manager uses locks
	PGShmemHeader *shim = NULL;
	InitShmemAccess(PGSharedMemoryCreate(3000000, 6000, &shim));
	InitShmemAllocation();
	CreateLWLocks();
	InitShmemIndex();
	WorkFileShmemInit();
	InitFileAccess();

	return run_tests(tests);
}
