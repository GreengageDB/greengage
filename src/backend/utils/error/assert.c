/*-------------------------------------------------------------------------
 *
 * assert.c
 *	  Assert support code.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/error/assert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq/pqsignal.h"
#include "cdb/cdbvars.h"                /* gp_reraise_signal */

#include <unistd.h>
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

/*
 * ExceptionalCondition - Handles the failure of an Assert()
 *
 * We intentionally do not go through elog() here, on the grounds of
 * wanting to minimize the amount of infrastructure that has to be
 * working to report an assertion failure.
 */
void
ExceptionalCondition(const char *conditionName,
					 const char *fileName,
					 int lineNumber)
{
	/*
	 * CDB: Try to tell the QD or client what happened, going through the
	 * error-reporting machinery so an assertion failure on a segment is
	 * surfaced to the coordinator instead of just closing the connection.
	 * errFatalReturn(gp_reraise_signal) makes errfinish() return to us (see
	 * the CDB branch in errfinish) so we can fall through to abort() and
	 * produce a core dump when gp_reraise_signal is set.
	 */
	if (!PointerIsValid(conditionName)
		|| !PointerIsValid(fileName))
		ereport(FATAL,
				errFatalReturn(gp_reraise_signal),
				errmsg("TRAP: ExceptionalCondition: bad arguments in PID %d",
					   (int) getpid()));
	else
		ereport(FATAL,
				errFatalReturn(gp_reraise_signal),
				errmsg("Unexpected internal error"),
				errdetail("FailedAssertion(\"%s\", File: \"%s\", Line: %d, PID: %d)\n",
						  conditionName, fileName, lineNumber,
						  (int) getpid()));

	/* Usually this shouldn't be needed, but make sure the msg went out */
	fflush(stderr);

	/* If we have support for it, dump a simple backtrace */
#ifdef HAVE_BACKTRACE_SYMBOLS
	{
		void	   *buf[100];
		int			nframes;

		nframes = backtrace(buf, lengthof(buf));
		backtrace_symbols_fd(buf, nframes, fileno(stderr));
	}
#endif

	/*
	 * If configured to do so, sleep indefinitely to allow user to attach a
	 * debugger.  It would be nice to use pg_usleep() here, but that can sleep
	 * at most 2G usec or ~33 minutes, which seems too short.
	 */
#ifdef SLEEP_ON_ASSERT
	sleep(1000000);
#endif

	abort();
}
