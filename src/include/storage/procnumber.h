/*-------------------------------------------------------------------------
 *
 * procnumber.h
 *	  definition of process number
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procnumber.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCNUMBER_H
#define PROCNUMBER_H

/*
 * ProcNumber uniquely identifies an active backend or auxiliary process.
 * It's assigned at backend startup after authentication, when the process
 * adds itself to the proc array.  It is an index into the proc array,
 * starting from 0. Note that a ProcNumber can be reused for a different
 * backend immediately after a backend exits.
 */
typedef int ProcNumber;

#define INVALID_PROC_NUMBER		(-1)

/*
 * Note: MAX_BACKENDS_BITS is 18 as that is the space available for buffer
 * refcounts in buf_internals.h.  This limitation could be lifted by using a
 * 64bit state; but it's unlikely to be worthwhile as 2^18-1 backends exceed
 * currently realistic configurations. Even if that limitation were removed,
 * we still could not a) exceed 2^23-1 because inval.c stores the ProcNumber
 * as a 3-byte signed integer, b) INT_MAX/4 because some places compute
 * 4*MaxBackends without any overflow check.  We check that the configured
 * number of backends does not exceed MAX_BACKENDS in InitializeMaxBackends().
 */
/*
 * GPDB: reduced from upstream's 18 to 17 bits.  Greengage steals one
 * buffer-header flag bit for BM_TEMP, leaving only 17 refcount bits (see
 * buf_internals.h), and the StaticAssert there requires
 * MAX_BACKENDS_BITS <= BUF_REFCOUNT_BITS.  2^17-1 = 131071 backends still
 * far exceeds any realistic Greengage configuration.
 */
#define MAX_BACKENDS_BITS		17
#define MAX_BACKENDS			((1U << MAX_BACKENDS_BITS)-1)

/*
 * Proc number of this backend (same as GetNumberFromPGProc(MyProc))
 */
extern PGDLLIMPORT ProcNumber MyProcNumber;

/* proc number of our parallel session leader, or INVALID_PROC_NUMBER if none */
extern PGDLLIMPORT ProcNumber ParallelLeaderProcNumber;

/*
 * TempRelBackendId (GPDB) is used in place of a real ProcNumber in the few
 * places where we deal with temporary relations.  Unlike upstream, GPDB keeps
 * temporary relations in the *shared* buffer pool, so their smgr/buffer cache
 * key must be backend-AGNOSTIC: a single fixed sentinel is used for every
 * temp relation regardless of which backend owns it.  This is what allows the
 * BM_TEMP shared-buffer hack and the temp/normal relfilenode collision check
 * in GetNewRelFileNumber() to work.
 *
 * Future enhancement: to align closer with upstream, this constant could be
 * replaced with gp_session_id once BufferTag is augmented with the session ID.
 */
#define TempRelBackendId		(-2)

/*
 * The ProcNumber to use for our session's temp relations.
 *
 * Upstream normally returns MyProcNumber (or the parallel leader's), but in
 * GPDB temp relations live in shared buffers and must use the fixed,
 * backend-agnostic TempRelBackendId sentinel so that every code path (relcache
 * rd_backend, smgr open, buffer flush, relfilenode collision check) agrees on
 * the same cache key.
 */
#define ProcNumberForTempRelations() \
	(TempRelBackendId)

#endif							/* PROCNUMBER_H */
