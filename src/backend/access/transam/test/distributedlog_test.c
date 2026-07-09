#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

/* Also ignore elog */
#include "utils/elog.h"

#undef elog
#define elog(elevel, ...) {}

/* Actual function body */
#include "../distributedlog.c"

#define DtxLogStartupNumPage 2
#define PageEntryToTransactionId(page, entry) \
	((page) * (TransactionId) ENTRIES_PER_PAGE + (TransactionId) (entry))

/*
 * Under the PG17 SLRU model the distributed log is protected by per-bank
 * LWLocks obtained via SimpleLruGetBankLock() instead of a single control
 * lock.  For the unit tests we point the SlruCtl at a single, statically
 * allocated bank lock (bank_mask == 0), so SimpleLruGetBankLock() always
 * returns a predictable pointer that the LWLock mocks can match against.
 */
static LWLockPadded distributedlog_bank_locks[1];
#define DistributedLogBankLock (&distributedlog_bank_locks[0].lock)

/* Backing store for TransamVariables consulted by DistributedLog_InitOldestXmin(). */
static TransamVariablesData distributedlog_transam_vars;

/*
 * A bug found in MPP-20426 was we were overrunnig to the next page
 * of DistributedLog.  The intention of the memset with zeors is to
 * reset the reset of the current page if we are in the middle of page,
 * so that we won't see uncommited data due to some recovery work.
 * However, we were doing the wrong math that calculates the size of
 * rest of page as the size of the part preceding to the current xid.
 * The worst scenario was for the subtransaction shared memory, which
 * follows distributed log shared memory to be overwritten.
 */
static void
MPP_20426(void **state, TransactionId nextXid)
{
	char			pages[BLCKSZ * DtxLogStartupNumPage];
	char			zeros[BLCKSZ];
	int				bytes;

	/* Setup TransamVariables */
	TransamVariablesData data;
	TransamVariables = &data;
	TransamVariables->oldestXid = 3;
	TransamVariables->latestCompletedXid = FullTransactionIdFromEpochAndXid(0, 4);
	DistributedLogShmem dls;
	DistributedLogShared = &dls;

	/* Setup DistributedLogCtl */
	DistributedLogCtl->shared = (SlruShared) malloc(sizeof(SlruSharedData));
	DistributedLogCtl->shared->page_buffer =
			(char **) malloc(DtxLogStartupNumPage * sizeof(char *));
	DistributedLogCtl->shared->page_dirty =
			(bool *) malloc(DtxLogStartupNumPage * sizeof(bool));
	DistributedLogCtl->shared->page_buffer[0] = &pages[0];
	DistributedLogCtl->shared->page_buffer[1] = &pages[BLCKSZ];
	memset(pages, 0x7f, sizeof(pages));
	memset(zeros, 0, sizeof(zeros));

	/* Point the SLRU at a single, predictable bank lock. */
	DistributedLogCtl->shared->bank_locks = distributedlog_bank_locks;
	DistributedLogCtl->bank_mask = 0;

	expect_value(LWLockAcquire, lock, DistributedLogBankLock);
	expect_value(LWLockAcquire, mode, LW_EXCLUSIVE);
	will_return(LWLockAcquire, true);

	/* This test is only for the case xid is not on the boundary. */
	expect_value(SimpleLruDoesPhysicalPageExist, ctl, DistributedLogCtl);
	expect_any(SimpleLruDoesPhysicalPageExist, pageno);
	will_return(SimpleLruDoesPhysicalPageExist, true);

	expect_value(SimpleLruReadPage, ctl, DistributedLogCtl);
	expect_any(SimpleLruReadPage, pageno);
	expect_any(SimpleLruReadPage, write_ok);
	expect_value(SimpleLruReadPage, xid, nextXid);
	will_return(SimpleLruReadPage, 0);

	expect_value(LWLockRelease, lock, DistributedLogBankLock);
	will_be_called(LWLockRelease);

	/* Run the function. */
	DistributedLog_Startup(nextXid, nextXid);

	/* DistributedLog_Startup should not overwrite the subsequent block. */
	assert_true(pages[BLCKSZ] == 0x7f);

	/* Make sure the part following the xid is zeroed. */
	bytes = TransactionIdToEntry(nextXid) * sizeof(DistributedLogEntry);
	assert_memory_equal(&pages[bytes], zeros, BLCKSZ - bytes);

	free(DistributedLogCtl->shared->page_dirty);
	free(DistributedLogCtl->shared->page_buffer);
	free(DistributedLogCtl->shared);
}

static void
test_DistributedLog_Startup_MPP_20426(void **state)
{
	MPP_20426(state, 115064091); /* from observed issue */
	MPP_20426(state, PageEntryToTransactionId(1, 10));
	MPP_20426(state, PageEntryToTransactionId(0x100, ENTRIES_PER_PAGE - 1));
	MPP_20426(state, PageEntryToTransactionId(0x100, 1));
}

static void
setup(TransactionId nextXid)
{
	char *pages = malloc(sizeof(char) * BLCKSZ);

	/* Setup DistributedLogCtl with a single page buffer */
	DistributedLogCtl->shared = (SlruShared) malloc(sizeof(SlruSharedData));
	DistributedLogCtl->shared->page_buffer =
			(char **) malloc(sizeof(char *));
	DistributedLogCtl->shared->page_dirty =
			(bool *) malloc(sizeof(bool));
	DistributedLogCtl->shared->page_buffer[0] = &pages[0];
	memset(pages, 0x7f, sizeof(char) * BLCKSZ);

	/* Point the SLRU at a single, predictable bank lock. */
	DistributedLogCtl->shared->bank_locks = distributedlog_bank_locks;
	DistributedLogCtl->bank_mask = 0;

	/* DistributedLog_InitOldestXmin() consults TransamVariables. */
	distributedlog_transam_vars.oldestXid = FirstNormalTransactionId;
	distributedlog_transam_vars.latestCompletedXid =
		FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId + 100);
	TransamVariables = &distributedlog_transam_vars;

	/*
	 * Map every page to buffer 0; we're only testing that the correct calls are
	 * made to SimpleLruZeroPage().
	 *
	 * DistributedLog_InitOldestXmin() peeks at the first page's existence and
	 * then stops.
	 */
	expect_value(SimpleLruDoesPhysicalPageExist, ctl, DistributedLogCtl);
	expect_any(SimpleLruDoesPhysicalPageExist, pageno);
	will_return(SimpleLruDoesPhysicalPageExist, true);

	/* Zeroing the remainder of the trailing page reads it back in. */
	expect_value(SimpleLruReadPage, ctl, DistributedLogCtl);
	expect_any(SimpleLruReadPage, pageno);
	expect_any(SimpleLruReadPage, write_ok);
	expect_value(SimpleLruReadPage, xid, nextXid);
	will_return(SimpleLruReadPage, 0);
}

/*
 * Set up the bank-lock acquire/release expectations for the binary-upgrade /
 * segment-conversion paths.  'nzeroed' pages are zeroed under a bank lock in
 * the fill loop, plus one more acquisition for the trailing-page zero-fill.
 */
static void
expect_bank_locks(int nzeroed)
{
	int			nlocks = nzeroed + 1;

	expect_value_count(LWLockAcquire, lock, DistributedLogBankLock, nlocks);
	expect_value_count(LWLockAcquire, mode, LW_EXCLUSIVE, nlocks);
	will_return_count(LWLockAcquire, true, nlocks);

	expect_value_count(LWLockRelease, lock, DistributedLogBankLock, nlocks);
	will_be_called_count(LWLockRelease, nlocks);
}

static void
test_BinaryUpgradeZeroesOutDistributedLogFittingOnSinglePage(void **state)
{
	TransactionId oldestActiveXid = FirstNormalTransactionId + 1;
	TransactionId nextXid = FirstNormalTransactionId + 10;

	setup(nextXid);

	expect_value(SimpleLruTruncateWithLock, ctl, DistributedLogCtl);
	expect_value(SimpleLruTruncateWithLock, cutoffPage, TransactionIdToPage(oldestActiveXid));
	will_be_called(SimpleLruTruncateWithLock);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid));
	will_return(SimpleLruZeroPage, 0);

	expect_bank_locks(1);

	IsBinaryUpgrade = true;

	/* Run the function. */
	DistributedLog_Startup(oldestActiveXid, nextXid);

	IsBinaryUpgrade = false;
}

static void
test_BinaryUpgradeZeroesOutDistributedLogFittingOnThreePages(void **state)
{
	TransactionId oldestActiveXid = FirstNormalTransactionId + 1;
	TransactionId nextXid = FirstNormalTransactionId + ENTRIES_PER_PAGE * 2;

	setup(nextXid);

	expect_value(SimpleLruTruncateWithLock, ctl, DistributedLogCtl);
	expect_value(SimpleLruTruncateWithLock, cutoffPage, TransactionIdToPage(oldestActiveXid));
	will_be_called(SimpleLruTruncateWithLock);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid));
	will_return(SimpleLruZeroPage, 0);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid) + 1);
	will_return(SimpleLruZeroPage, 0);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(nextXid));
	will_return(SimpleLruZeroPage, 0);

	expect_bank_locks(3);

	IsBinaryUpgrade = true;

	/* Run the function. */
	DistributedLog_Startup(oldestActiveXid, nextXid);

	IsBinaryUpgrade = false;
}

static void
test_BinaryUpgradeZeroesOutDistributedLogWithTransactionIdWraparound(void **state)
{
	TransactionId oldestActiveXid = MaxTransactionId;
	TransactionId nextXid = MaxTransactionId + 10;

	setup(nextXid);

	expect_value(SimpleLruTruncateWithLock, ctl, DistributedLogCtl);
	expect_value(SimpleLruTruncateWithLock, cutoffPage, TransactionIdToPage(oldestActiveXid));
	will_be_called(SimpleLruTruncateWithLock);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid));
	will_return(SimpleLruZeroPage, 0);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(nextXid));
	will_return(SimpleLruZeroPage, 0);

	expect_bank_locks(2);

	IsBinaryUpgrade = true;

	/* Run the function. */
	DistributedLog_Startup(oldestActiveXid, nextXid);

	IsBinaryUpgrade = false;
}

static void
test_ConvertMasterDataDirToSegmentZeroesOutDistributedLogFittingOnSinglePage(void **state)
{
	TransactionId oldestActiveXid = FirstNormalTransactionId + 1;
	TransactionId nextXid = FirstNormalTransactionId + 10;

	setup(nextXid);

	expect_value(SimpleLruTruncateWithLock, ctl, DistributedLogCtl);
	expect_value(SimpleLruTruncateWithLock, cutoffPage, TransactionIdToPage(oldestActiveXid));
	will_be_called(SimpleLruTruncateWithLock);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid));
	will_return(SimpleLruZeroPage, 0);

	expect_bank_locks(1);

	ConvertMasterDataDirToSegment = true;

	/* Run the function. */
	DistributedLog_Startup(oldestActiveXid, nextXid);

	ConvertMasterDataDirToSegment = false;
}

static void
test_ConvertMasterDataDirToSegmentZeroesOutDistributedLogFittingOnThreePages(void **state)
{
	TransactionId oldestActiveXid = FirstNormalTransactionId + 1;
	TransactionId nextXid = FirstNormalTransactionId + ENTRIES_PER_PAGE * 2;

	setup(nextXid);

	expect_value(SimpleLruTruncateWithLock, ctl, DistributedLogCtl);
	expect_value(SimpleLruTruncateWithLock, cutoffPage, TransactionIdToPage(oldestActiveXid));
	will_be_called(SimpleLruTruncateWithLock);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid));
	will_return(SimpleLruZeroPage, 0);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid) + 1);
	will_return(SimpleLruZeroPage, 0);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(nextXid));
	will_return(SimpleLruZeroPage, 0);

	expect_bank_locks(3);

	ConvertMasterDataDirToSegment = true;

	/* Run the function. */
	DistributedLog_Startup(oldestActiveXid, nextXid);

	ConvertMasterDataDirToSegment = false;
}

static void
test_ConvertMasterDataDirToSegmentZeroesOutDistributedLogWithTransactionIdWraparound(void **state)
{
	TransactionId oldestActiveXid = MaxTransactionId;
	TransactionId nextXid = MaxTransactionId + 10;

	setup(nextXid);

	expect_value(SimpleLruTruncateWithLock, ctl, DistributedLogCtl);
	expect_value(SimpleLruTruncateWithLock, cutoffPage, TransactionIdToPage(oldestActiveXid));
	will_be_called(SimpleLruTruncateWithLock);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(oldestActiveXid));
	will_return(SimpleLruZeroPage, 0);

	expect_value(SimpleLruZeroPage, ctl, DistributedLogCtl);
	expect_value(SimpleLruZeroPage, pageno, TransactionIdToPage(nextXid));
	will_return(SimpleLruZeroPage, 0);

	expect_bank_locks(2);

	ConvertMasterDataDirToSegment = true;

	/* Run the function. */
	DistributedLog_Startup(oldestActiveXid, nextXid);

	ConvertMasterDataDirToSegment = false;
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_DistributedLog_Startup_MPP_20426),
		unit_test(test_BinaryUpgradeZeroesOutDistributedLogWithTransactionIdWraparound),
		unit_test(test_BinaryUpgradeZeroesOutDistributedLogFittingOnThreePages),
		unit_test(test_BinaryUpgradeZeroesOutDistributedLogFittingOnSinglePage),
		unit_test(test_ConvertMasterDataDirToSegmentZeroesOutDistributedLogWithTransactionIdWraparound),
		unit_test(test_ConvertMasterDataDirToSegmentZeroesOutDistributedLogFittingOnThreePages),
		unit_test(test_ConvertMasterDataDirToSegmentZeroesOutDistributedLogFittingOnSinglePage)
	};
	return run_tests(tests);
}
