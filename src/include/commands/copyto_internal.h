/*-------------------------------------------------------------------------
 *
 * copyto_internal.h
 *	  Internal definitions for COPY TO command.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyto_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYTO_INTERNAL_H
#define COPYTO_INTERNAL_H

#include "commands/copy.h"
#include "commands/trigger.h"

/*
 * This struct contains all the state variables used throughout a COPY TO
 * operation.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is true
 * when we have to do it the hard way.
 */
typedef struct CopyToStateData
{
	/* low-level state data */
	CopySrcDest	copy_dest;		/* type of copy destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO */

	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */

	CopyFormatOptions opts;
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/*
	 * Working state
	 */
	CopyDispatchMode dispatch_mode;
	MemoryContext copycontext;	/* per-copy execution context */

	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */
	uint64		bytes_processed;	/* number of bytes processed so far */

	/* GPDB specific variables */
	FmgrInfo   *enc_conversion_proc; /* conv proc from exttbl encoding to
										server or the other way around */

	ProgramPipes	*program_pipes; /* COPY PROGRAM pipes for data and stderr */


	/* Information on the connections to QEs. */
	CdbCopy    *cdbCopy;

	/* end GPDB specific variables */
} CopyToStateData;

/*
 * GPDB: set by BeginCopyToCommon() as soon as the state is allocated, so that
 * DoCopy()'s PG_CATCH block can run mppExecutorCleanup() even when the error
 * is thrown inside BeginCopyTo() after ExecutorStart().  Reset in DoCopy().
 */
extern volatile CopyToState glob_cstate;

extern CopyToState BeginCopyToOnSegment(QueryDesc *queryDesc);
extern void EndCopyToOnSegment(CopyToState cstate);
extern uint64 CopyToQueryOnSegment(CopyToState cstate);
extern CopyToState BeginCopyToForeignTable(Relation forrel, List *options);
extern void CopyOneRowTo(CopyToState cstate, TupleTableSlot *slot);
extern void CopyOneCustomRowTo(CopyToState cstate, bytea *value);
extern void CopySendEndOfRow(CopyToState cstate);

#endif							/* COPYTO_INTERNAL_H */
