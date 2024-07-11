/*-------------------------------------------------------------------------
 *
 * copy.c
 *		Implements the COPY utility command
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-int.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/progress.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "nodes/makefuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "storage/execute_pipe.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"

#include "access/external.h"
#include "access/url.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_extprotocol.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbcopy.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "commands/queue.h"
#include "nodes/makefuncs.h"
#include "postmaster/autostats.h"
#include "utils/metrics_utils.h"
#include "utils/resscheduler.h"
#include "utils/string_utils.h"
#include "partitioning/partdesc.h"

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')

/*
 * Represents the heap insert method to be used during COPY FROM.
 */
typedef enum CopyInsertMethod
{
	CIM_SINGLE,					/* use table_tuple_insert or fdw routine */
	CIM_MULTI,					/* always use table_multi_insert */
	CIM_MULTI_CONDITIONAL		/* use table_multi_insert only if valid */
} CopyInsertMethod;

/*
 * No more than this many tuples per CopyMultiInsertBuffer
 *
 * Caution: Don't make this too big, as we could end up with this many
 * CopyMultiInsertBuffer items stored in CopyMultiInsertInfo's
 * multiInsertBuffers list.  Increasing this can cause quadratic growth in
 * memory requirements during copies into partitioned tables with a large
 * number of partitions.
 */
#define MAX_BUFFERED_TUPLES		1000

/*
 * Flush buffers if there are >= this many bytes, as counted by the input
 * size, of tuples stored.
 */
#define MAX_BUFFERED_BYTES		65535

/* Trim the list of buffers back down to this number after flushing */
#define MAX_PARTITION_BUFFERS	32

/* Stores multi-insert data related to a single relation in CopyFrom. */
typedef struct CopyMultiInsertBuffer
{
	TupleTableSlot *slots[MAX_BUFFERED_TUPLES]; /* Array to store tuples */
	ResultRelInfo *resultRelInfo;	/* ResultRelInfo for 'relid' */
	BulkInsertState bistate;	/* BulkInsertState for this rel */
	int			nused;			/* number of 'slots' containing tuples */
	uint64		linenos[MAX_BUFFERED_TUPLES];	/* Line # of tuple in copy
												 * stream */
} CopyMultiInsertBuffer;

/*
 * Stores one or many CopyMultiInsertBuffers and details about the size and
 * number of tuples which are stored in them.  This allows multiple buffers to
 * exist at once when COPYing into a partitioned table.
 */
typedef struct CopyMultiInsertInfo
{
	List	   *multiInsertBuffers; /* List of tracked CopyMultiInsertBuffers */
	int			bufferedTuples; /* number of tuples buffered over all buffers */
	int			bufferedBytes;	/* number of bytes from all buffered tuples */
	CopyState	cstate;			/* Copy state for this CopyMultiInsertInfo */
	EState	   *estate;			/* Executor state used for COPY */
	CommandId	mycid;			/* Command Id used for COPY */
	int			ti_options;		/* table insert options */
} CopyMultiInsertInfo;


/*
 * These macros centralize code used to process line_buf and raw_buf buffers.
 * They are macros because they often do continue/break control and to avoid
 * function call overhead in tight COPY loops.
 *
 * We must use "if (1)" because the usual "do {...} while(0)" wrapper would
 * prevent the continue/break processing from working.  We end the "if (1)"
 * with "else ((void) 0)" to ensure the "if" does not unintentionally match
 * any "else" in the calling code, and to avoid any compiler warnings about
 * empty statements.  See http://www.cit.gu.edu.au/~anthony/info/C/C.macros.
 */

/*
 * This keeps the character read at the top of the loop in the buffer
 * even if there is more than one read-ahead.
 */
#define IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen) \
if (1) \
{ \
	if (raw_buf_ptr + (extralen) >= copy_buf_len && !hit_eof) \
	{ \
		raw_buf_ptr = prev_raw_ptr; /* undo fetch */ \
		need_data = true; \
		continue; \
	} \
} else ((void) 0)

/* This consumes the remainder of the buffer and breaks */
#define IF_NEED_REFILL_AND_EOF_BREAK(extralen) \
if (1) \
{ \
	if (raw_buf_ptr + (extralen) >= copy_buf_len && hit_eof) \
	{ \
		if (extralen) \
			raw_buf_ptr = copy_buf_len; /* consume the partial character */ \
		/* backslash just before EOF, treat as data char */ \
		result = true; \
		break; \
	} \
} else ((void) 0)

/*
 * Transfer any approved data to line_buf; must do this to be sure
 * there is some room in raw_buf.
 */
#define REFILL_LINEBUF \
if (1) \
{ \
	if (raw_buf_ptr > cstate->raw_buf_index) \
	{ \
		appendBinaryStringInfo(&cstate->line_buf, \
							 cstate->raw_buf + cstate->raw_buf_index, \
							   raw_buf_ptr - cstate->raw_buf_index); \
		cstate->raw_buf_index = raw_buf_ptr; \
	} \
} else ((void) 0)

/* Undo any read-ahead and jump out of the block. */
#define NO_END_OF_COPY_GOTO \
if (1) \
{ \
	raw_buf_ptr = prev_raw_ptr + 1; \
	goto not_end_of_copy; \
} else ((void) 0)

static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";


/* non-export function prototypes */
static void EndCopy(CopyState cstate);
static CopyState BeginCopyTo(ParseState *pstate, Relation rel, RawStmt *query,
							 Oid queryRelId, const char *filename, bool is_program,
							 List *attnamelist, List *options);
static void EndCopyTo(CopyState cstate, uint64 *processed);
static uint64 DoCopyTo(CopyState cstate);
static uint64 CopyToDispatch(CopyState cstate);
static uint64 CopyTo(CopyState cstate);
static uint64 CopyDispatchOnSegment(CopyState cstate, const CopyStmt *stmt);
static uint64 CopyToQueryOnSegment(CopyState cstate);
static bool CopyReadLine(CopyState cstate);
static bool CopyReadLineText(CopyState cstate);
static int	CopyReadAttributesText(CopyState cstate, int stop_processing_at_field);
static int	CopyReadAttributesCSV(CopyState cstate, int stop_processing_at_field);
static Datum CopyReadBinaryAttribute(CopyState cstate,
									 int column_no, FmgrInfo *flinfo,
									 Oid typioparam, int32 typmod,
									 bool *isnull);
static void CopyAttributeOutText(CopyState cstate, char *string);
static void CopyAttributeOutCSV(CopyState cstate, char *string,
								bool use_quote, bool single_attr);

/* Low-level communications functions */
static void SendCopyBegin(CopyState cstate);
static void ReceiveCopyBegin(CopyState cstate);
static void SendCopyEnd(CopyState cstate);
static void CopySendData(CopyState cstate, const void *databuf, int datasize);
static void CopySendString(CopyState cstate, const char *str);
static void CopySendChar(CopyState cstate, char c);
static int	CopyGetData(CopyState cstate, void *databuf, int datasize);
static void CopySendInt32(CopyState cstate, int32 val);
static bool CopyGetInt32(CopyState cstate, int32 *val);
static void CopySendInt16(CopyState cstate, int16 val);
static bool CopyGetInt16(CopyState cstate, int16 *val);

static void SendCopyFromForwardedTuple(CopyState cstate,
						   CdbCopy *cdbCopy,
						   bool toAll,
						   int target_seg,
						   Relation rel,
						   int64 lineno,
						   char *line,
						   int line_len,
						   Datum *values,
						   bool *nulls);
static void SendCopyFromForwardedHeader(CopyState cstate, CdbCopy *cdbCopy);
static void SendCopyFromForwardedError(CopyState cstate, CdbCopy *cdbCopy, char *errmsg);

static bool NextCopyFromDispatch(CopyState cstate, ExprContext *econtext,
								 Datum *values, bool *nulls);
static bool NextCopyFromExecute(CopyState cstate, ExprContext *econtext, Datum *values, bool *nulls);
static bool NextCopyFromRawFieldsX(CopyState cstate, char ***fields, int *nfields,
								   int stop_processing_at_field);
static bool NextCopyFromX(CopyState cstate, ExprContext *econtext,
						  Datum *values, bool *nulls);
void HandleCopyError(CopyState cstate);
static void HandleQDErrorFrame(CopyState cstate, char *p, int len);

static void CopyInitDataParser(CopyState cstate);
static void setEncodingConversionProc(CopyState cstate, int encoding, bool iswritable);

static GpDistributionData *InitDistributionData(CopyState cstate, EState *estate);
static void FreeDistributionData(GpDistributionData *distData);
static void InitCopyFromDispatchSplit(CopyState cstate, GpDistributionData *distData, EState *estate);
static unsigned int GetTargetSeg(GpDistributionData *distData, TupleTableSlot *slot);
static ProgramPipes *open_program_pipes(CopyState cstate, bool forwrite);
static void close_program_pipes(CopyState cstate, bool ifThrow);
CopyIntoClause*
MakeCopyIntoClause(CopyStmt *stmt);
static List *parse_joined_option_list(char *str, char *delimiter);

/* ==========================================================================
 * The following macros aid in major refactoring of data processing code (in
 * CopyFrom(+Dispatch)). We use macros because in some cases the code must be in
 * line in order to work (for example elog_dismiss() in PG_CATCH) while in
 * other cases we'd like to inline the code for performance reasons.
 *
 * NOTE that an almost identical set of macros exists in fileam.c. If you make
 * changes here you may want to consider taking a look there as well.
 * ==========================================================================
 */

#define RESET_LINEBUF \
cstate->line_buf.len = 0; \
cstate->line_buf.data[0] = '\0'; \
cstate->line_buf.cursor = 0;

#define RESET_ATTRBUF \
cstate->attribute_buf.len = 0; \
cstate->attribute_buf.data[0] = '\0'; \
cstate->attribute_buf.cursor = 0;

#define RESET_LINEBUF_WITH_LINENO \
line_buf_with_lineno.len = 0; \
line_buf_with_lineno.data[0] = '\0'; \
line_buf_with_lineno.cursor = 0;

static volatile CopyState glob_cstate = NULL;


/* GPDB_91_MERGE_FIXME: passing through a global variable like this is ugly */
static CopyStmt *glob_copystmt = NULL;

/*
 * Testing GUC: When enabled, COPY FROM prints an INFO line to indicate which
 * fields are processed in the QD, and which in the QE.
 */
extern bool Test_copy_qd_qe_split;

/*
 * When doing a COPY FROM through the dispatcher, the QD reads the input from
 * the input file (or stdin or program), and forwards the data to the QE nodes,
 * where they will actually be inserted.
 *
 * Ideally, the QD would just pass through each line to the QE as is, and let
 * the QEs to do all the processing. Because the more processing the QD has
 * to do, the more likely it is to become a bottleneck.
 *
 * However, the QD needs to figure out which QE to send each row to. For that,
 * it needs to at least parse the distribution key. The distribution key might
 * also be a DEFAULTed column, in which case the DEFAULT value needs to be
 * evaluated in the QD. In that case, the QD must send the computed value
 * to the QE - we cannot assume that the QE can re-evaluate the expression and
 * arrive at the same value, at least not if the DEFAULT expression is volatile.
 *
 * Therefore, we need a flexible format between the QD and QE, where the QD
 * processes just enough of each input line to figure out where to send it.
 * It must send the values it had to parse and evaluate to the QE, as well
 * as the rest of the original input line, so that the QE can parse the rest
 * of it.
 *
 * The 'copy_from_dispatch_*' structs are used in the QD->QE stream. For each
 * input line, the QD constructs a 'copy_from_dispatch_row' struct, and sends
 * it to the QE. Before any rows, a QDtoQESignature is sent first, followed by
 * a 'copy_from_dispatch_header'. When QD encounters a recoverable error that
 * needs to be logged in the error log (LOG ERRORS SEGMENT REJECT LIMIT), it
 * sends the erroneous raw to a QE, in a 'copy_from_dispatch_error' struct.
 *
 *
 * COPY TO is simpler: The QEs form the output rows in the final form, and the QD
 * just collects and forwards them to the client. The QD doesn't need to parse
 * the rows at all.
 */
static const char QDtoQESignature[] = "PGCOPY-QD-TO-QE\n\377\r\n";

/* Header contains information that applies to all the rows that follow. */
typedef struct
{
	/*
	 * First field that should be processed in the QE. Any fields before
	 * this will be included as Datums in the rows that follow.
	 */
	int16		first_qe_processed_field;
} copy_from_dispatch_header;

typedef struct
{
	/*
	 * Information about this input line.
	 *
	 * 'relid' is the target relation's OID. Normally, the same as
	 * cstate->relid, but for a partitioned relation, it indicates the target
	 * partition. Note: this must be the first field, because InvalidOid means
	 * that this is actually a 'copy_from_dispatch_error' struct.
	 *
	 * 'lineno' is the input line number, for error reporting.
	 */
	int64		lineno;
	Oid			relid;

	uint32		line_len;			/* size of the included input line */
	uint32		residual_off;		/* offset in the line, where QE should
									 * process remaining fields */
	bool		delim_seen_at_end;  /* conveys to QE if QD saw a delim at end
									 * of its processing */
	uint16		fld_count;			/* # of fields that were processed in the
									 * QD. */

	/* The input line follows. */

	/*
	 * For each field that was parsed in the QD already, the following data follows:
	 *
	 * int16	fieldnum;
	 * <data>
	 *
	 * NULL values are not included, any attributes that are not included in
	 * the message are implicitly NULL.
	 *
	 * For pass-by-value datatypes, the <data> is the raw Datum. For
	 * simplicity, it is always sent as a full-width 8-byte Datum, regardless
	 * of the datatype's length.
	 *
	 * For other fixed width datatypes, <data> is the datatype's value.
	 *
	 * For variable-length datatypes, <data> begins with a 4-byte length field,
	 * followed by the data. Cstrings (typlen = -2) are also sent in this
	 * format.
	 */
} copy_from_dispatch_row;

/* Size of the struct, without padding at the end. */
#define SizeOfCopyFromDispatchRow (offsetof(copy_from_dispatch_row, fld_count) + sizeof(uint16))

typedef struct
{
	int64		error_marker;	/* constant -1, to mark that this is an error
								 * frame rather than 'copy_from_dispatch_row' */
	int64		lineno;
	uint32		errmsg_len;
	uint32		line_len;
	bool		line_buf_converted;

	/* 'errmsg' follows */
	/* 'line' follows */
} copy_from_dispatch_error;

/* Size of the struct, without padding at the end. */
#define SizeOfCopyFromDispatchError (offsetof(copy_from_dispatch_error, line_buf_converted) + sizeof(bool))


/*
 * Send copy start/stop messages for frontend copies.  These have changed
 * in past protocol redesigns.
 */
static void
SendCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int			natts = list_length(cstate->attnumlist);
		int16		format = (cstate->binary ? 1 : 0);
		int			i;

		pq_beginmessage(&buf, 'H');
		pq_sendbyte(&buf, format);	/* overall format */
		pq_sendint16(&buf, natts);
		for (i = 0; i < natts; i++)
			pq_sendint16(&buf, format); /* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
	}
	else
	{
		/* old way */
		if (cstate->binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('H');
		/* grottiness needed for old COPY OUT protocol */
		pq_startcopyout();
		cstate->copy_dest = COPY_OLD_FE;
	}
}

static void
ReceiveCopyBegin(CopyState cstate)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int			natts = list_length(cstate->attnumlist);
		int16		format = (cstate->binary ? 1 : 0);
		int			i;

		pq_beginmessage(&buf, 'G');
		pq_sendbyte(&buf, format);	/* overall format */
		pq_sendint16(&buf, natts);
		for (i = 0; i < natts; i++)
			pq_sendint16(&buf, format); /* per-column formats */
		pq_endmessage(&buf);
		cstate->copy_dest = COPY_NEW_FE;
		cstate->fe_msgbuf = makeStringInfo();
	}
	else
	{
		/* old way */
		if (cstate->binary)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('G');
		/* any error in old protocol will make us lose sync */
		pq_startmsgread();
		cstate->copy_dest = COPY_OLD_FE;
	}
	/* We *must* flush here to ensure FE knows it can send. */
	pq_flush();
}

static void
SendCopyEnd(CopyState cstate)
{
	if (cstate->copy_dest == COPY_NEW_FE)
	{
		/* Shouldn't have any unsent data */
		Assert(cstate->fe_msgbuf->len == 0);
		/* Send Copy Done message */
		pq_putemptymessage('c');
	}
	else
	{
		CopySendData(cstate, "\\.", 2);
		/* Need to flush out the trailer (this also appends a newline) */
		CopySendEndOfRow(cstate);
		pq_endcopyout(false);
	}
}

/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
CopySendData(CopyState cstate, const void *databuf, int datasize)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, databuf, datasize);
}

static void
CopySendString(CopyState cstate, const char *str)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
}

static void
CopySendChar(CopyState cstate, char c)
{
	appendStringInfoCharMacro(cstate->fe_msgbuf, c);
}

/* AXG: Note that this will both add a newline AND flush the data.
 * For the dispatcher COPY TO we don't want to use this method since
 * our newlines already exist. We use another new method similar to
 * this one to flush the data
 */
void
CopySendEndOfRow(CopyState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			if (!cstate->binary)
			{
				/* Default line termination depends on platform */
#ifndef WIN32
				CopySendChar(cstate, '\n');
#else
				CopySendString(cstate, "\r\n");
#endif
			}

			if (fwrite(fe_msgbuf->data, fe_msgbuf->len, 1,
					   cstate->copy_file) != 1 ||
				ferror(cstate->copy_file))
			{
				if (cstate->is_program)
				{
					if (errno == EPIPE)
					{
						/*
						 * The pipe will be closed automatically on error at
						 * the end of transaction, but we might get a better
						 * error message from the subprocess' exit code than
						 * just "Broken Pipe"
						 */
						close_program_pipes(cstate, true);

						/*
						 * If close_program_pipes() didn't throw an error,
						 * the program terminated normally, but closed the
						 * pipe first. Restore errno, and throw an error.
						 */
						errno = EPIPE;
					}
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY program: %m")));
				}
				else
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY file: %m")));
			}
			break;
		case COPY_OLD_FE:
			/* The FE/BE protocol uses \n as newline for all platforms */
			if (!cstate->binary)
				CopySendChar(cstate, '\n');

			if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len))
			{
				/* no hope of recovering connection sync, so FATAL */
				ereport(FATAL,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("connection lost during COPY to stdout")));
			}
			break;
		case COPY_NEW_FE:
			/* The FE/BE protocol uses \n as newline for all platforms */
			if (!cstate->binary)
				CopySendChar(cstate, '\n');

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_CALLBACK:
			/* we don't actually do the write here, we let the caller do it */
#ifndef WIN32
			CopySendChar(cstate, '\n');
#else
			CopySendString(cstate, "\r\n");
#endif
			return; /* don't want to reset msgbuf quite yet */
	}

	/* Update the progress */
	cstate->bytes_processed += cstate->fe_msgbuf->len;
	pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate->bytes_processed);

	resetStringInfo(fe_msgbuf);
}

/*
 * AXG: This one is equivalent to CopySendEndOfRow() besides that
 * it doesn't send end of row - it just flushed the data. We need
 * this method for the dispatcher COPY TO since it already has data
 * with newlines (from the executors).
 */
static void
CopyToDispatchFlush(CopyState cstate)
{
	StringInfo	fe_msgbuf = cstate->fe_msgbuf;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:

			(void) fwrite(fe_msgbuf->data, fe_msgbuf->len,
						  1, cstate->copy_file);
			if (ferror(cstate->copy_file))
			{
				if (cstate->is_program)
				{
					if (errno == EPIPE)
					{
						/*
						 * The pipe will be closed automatically on error at
						 * the end of transaction, but we might get a better
						 * error message from the subprocess' exit code than
						 * just "Broken Pipe"
						 */
						close_program_pipes(cstate, true);

						/*
						 * If close_program_pipes() didn't throw an error,
						 * the program terminated normally, but closed the
						 * pipe first. Restore errno, and throw an error.
						 */
						errno = EPIPE;
					}
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not write to COPY program: %m")));
				}
				else
					ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to COPY file: %m")));
			}
			break;
		case COPY_OLD_FE:

			if (pq_putbytes(fe_msgbuf->data, fe_msgbuf->len))
			{
				/* no hope of recovering connection sync, so FATAL */
				ereport(FATAL,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("connection lost during COPY to stdout")));
			}
			break;
		case COPY_NEW_FE:

			/* Dump the accumulated row as one CopyData message */
			(void) pq_putmessage('d', fe_msgbuf->data, fe_msgbuf->len);
			break;
		case COPY_CALLBACK:
			elog(ERROR, "unexpected destination COPY_CALLBACK to flush data");
			break;
	}

	resetStringInfo(fe_msgbuf);
}

/*
 * CopyGetData reads data from the source (file or frontend)
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 *
 * Returns: the number of bytes that were successfully read
 * into the data buffer.
 */
static int
CopyGetData(CopyState cstate, void *databuf, int datasize)
{
	size_t		bytesread = 0;

	switch (cstate->copy_dest)
	{
		case COPY_FILE:
			bytesread = fread(databuf, 1, datasize, cstate->copy_file);
			if (feof(cstate->copy_file))
				cstate->reached_eof = true;
			if (ferror(cstate->copy_file))
			{
				if (cstate->is_program)
				{
					int olderrno = errno;

					close_program_pipes(cstate, true);

					/*
					 * If close_program_pipes() didn't throw an error,
					 * the program terminated normally, but closed the
					 * pipe first. Restore errno, and throw an error.
					 */
					errno = olderrno;

					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not read from COPY program: %m")));
				}
				else
					ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from COPY file: %m")));
			}
			break;
		case COPY_OLD_FE:
			if (pq_getbytes((char *) databuf, datasize))
			{
				/* Only a \. terminator is legal EOF in old protocol */
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unexpected EOF on client connection with an open transaction")));
			}
			bytesread += datasize;		/* update the count of bytes that were
										 * read so far */
			break;
		case COPY_NEW_FE:
			while (datasize > 0 && !cstate->reached_eof)
			{
				int			avail;

				while (cstate->fe_msgbuf->cursor >= cstate->fe_msgbuf->len)
				{
					/* Try to receive another message */
					int			mtype;

			readmessage:
					HOLD_CANCEL_INTERRUPTS();
					pq_startmsgread();
					mtype = pq_getbyte();
					if (mtype == EOF)
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					if (pq_getmessage(cstate->fe_msgbuf, 0))
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
								 errmsg("unexpected EOF on client connection with an open transaction")));
					RESUME_CANCEL_INTERRUPTS();
					switch (mtype)
					{
						case 'd':	/* CopyData */
							break;
						case 'c':	/* CopyDone */
							/* COPY IN correctly terminated by frontend */
							cstate->reached_eof = true;
							return bytesread;
						case 'f':	/* CopyFail */
							ereport(ERROR,
									(errcode(ERRCODE_QUERY_CANCELED),
									 errmsg("COPY from stdin failed: %s",
											pq_getmsgstring(cstate->fe_msgbuf))));
							break;
						case 'H':	/* Flush */
						case 'S':	/* Sync */

							/*
							 * Ignore Flush/Sync for the convenience of client
							 * libraries (such as libpq) that may send those
							 * without noticing that the command they just
							 * sent was COPY.
							 */
							goto readmessage;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("unexpected message type 0x%02X during COPY from stdin",
											mtype)));
							break;
					}
				}
				avail = cstate->fe_msgbuf->len - cstate->fe_msgbuf->cursor;
				if (avail > datasize)
					avail = datasize;
				pq_copymsgbytes(cstate->fe_msgbuf, databuf, avail);
				databuf = (void *) ((char *) databuf + avail);
				bytesread += avail;		/* update the count of bytes that were
										 * read so far */
				datasize -= avail;
			}
			break;
		case COPY_CALLBACK:
			bytesread = cstate->data_source_cb(databuf, datasize, datasize,
											   cstate->data_source_cb_extra);
			break;
	}

	return bytesread;
}


/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
static void
CopySendInt32(CopyState cstate, int32 val)
{
	uint32		buf;

	buf = pg_hton32((uint32) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt32 reads an int32 that appears in network byte order
 *
 * Returns true if OK, false if EOF
 */
static bool
CopyGetInt32(CopyState cstate, int32 *val)
{
	uint32		buf;

	if (CopyGetData(cstate, &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int32) pg_ntoh32(buf);
	return true;
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
static void
CopySendInt16(CopyState cstate, int16 val)
{
	uint16		buf;

	buf = pg_hton16((uint16) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt16 reads an int16 that appears in network byte order
 */
static bool
CopyGetInt16(CopyState cstate, int16 *val)
{
	uint16		buf;

	if (CopyGetData(cstate, &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int16) pg_ntoh16(buf);
	return true;
}


/*
 * CopyLoadRawBuf loads some more data into raw_buf
 *
 * Returns true if able to obtain at least one more byte, else false.
 *
 * If raw_buf_index < raw_buf_len, the unprocessed bytes are transferred
 * down to the start of the buffer and then we load more data after that.
 * This case is used only when a frontend multibyte character crosses a
 * bufferload boundary.
 */
static bool
CopyLoadRawBuf(CopyState cstate)
{
	int			nbytes;
	int			inbytes;

	if (cstate->raw_buf_index < cstate->raw_buf_len)
	{
		/* Copy down the unprocessed data */
		nbytes = cstate->raw_buf_len - cstate->raw_buf_index;
		memmove(cstate->raw_buf, cstate->raw_buf + cstate->raw_buf_index,
				nbytes);
	}
	else
		nbytes = 0;				/* no data need be saved */

	inbytes = CopyGetData(cstate, cstate->raw_buf + nbytes,
						  RAW_BUF_SIZE - nbytes);
	nbytes += inbytes;
	cstate->raw_buf[nbytes] = '\0';
	cstate->raw_buf_index = 0;
	cstate->raw_buf_len = nbytes;
	cstate->bytes_processed += nbytes;
	pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate->bytes_processed);
	return (inbytes > 0);
}


/*
 *	 DoCopy executes the SQL COPY statement
 *
 * Either unload or reload contents of table <relation>, depending on <from>.
 * (<from> = true means we are inserting into the table.)  In the "TO" case
 * we also support copying the output of an arbitrary SELECT, INSERT, UPDATE
 * or DELETE query.
 *
 * If <pipe> is false, transfer is between the table and the file named
 * <filename>.  Otherwise, transfer is between the table and our regular
 * input/output stream. The latter could be either stdin/stdout or a
 * socket, depending on whether we're running under Postmaster control.
 *
 * Do not allow a Postgres user without the 'pg_read_server_files' or
 * 'pg_write_server_files' role to read from or write to a file.
 *
 * Do not allow the copy if user doesn't have proper permission to access
 * the table or the specifically requested columns.
 */
void
DoCopy(ParseState *pstate, const CopyStmt *stmt,
	   int stmt_location, int stmt_len,
	   uint64 *processed)
{
	CopyState	cstate;
	bool		is_from = stmt->is_from;
	bool		pipe = (stmt->filename == NULL || Gp_role == GP_ROLE_EXECUTE);
	Relation	rel;
	Oid			relid;
	RawStmt    *query = NULL;
	Node	   *whereClause = NULL;
	List	   *attnamelist = stmt->attlist;
	List	   *options;

	glob_cstate = NULL;
	glob_copystmt = (CopyStmt *) stmt;

	options = stmt->options;

	if (stmt->sreh && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY single row error handling only available using COPY FROM")));

	/*
	 * Disallow COPY to/from file or program except to users with the
	 * appropriate role.
	 */
	if (!pipe)
	{
		if (stmt->is_program)
		{
			if (!is_member_of_role(GetUserId(), DEFAULT_ROLE_EXECUTE_SERVER_PROGRAM))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser or a member of the pg_execute_server_program role to COPY to or from an external program"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			if (is_from && !is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_SERVER_FILES))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser or a member of the pg_read_server_files role to COPY from a file"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));

			if (!is_from && !is_member_of_role(GetUserId(), DEFAULT_ROLE_WRITE_SERVER_FILES))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("must be superuser or a member of the pg_write_server_files role to COPY to a file"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));
		}
	}

	if (stmt->relation)
	{
		LOCKMODE	lockmode = is_from ? RowExclusiveLock : AccessShareLock;
		RangeTblEntry *rte;
		TupleDesc	tupDesc;
		List	   *attnums;
		ListCell   *cur;

		Assert(!stmt->query);

		/* Open and lock the relation, using the appropriate lock type. */
		rel = table_openrv(stmt->relation, lockmode);

		if (is_from && !allowSystemTableMods && IsUnderPostmaster && IsSystemRelation(rel))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("permission denied: \"%s\" is a system catalog",
								RelationGetRelationName(rel)),
								errhint("Make sure the configuration parameter allow_system_table_mods is set.")));
		}

		relid = RelationGetRelid(rel);

		rte = addRangeTableEntryForRelation(pstate, rel, lockmode,
											NULL, false, false);
		rte->requiredPerms = (is_from ? ACL_INSERT : ACL_SELECT);

		if (stmt->whereClause)
		{
			/* add rte to column namespace  */
			addRTEtoQuery(pstate, rte, false, true, true);

			/* Transform the raw expression tree */
			whereClause = transformExpr(pstate, stmt->whereClause, EXPR_KIND_COPY_WHERE);

			/* Make sure it yields a boolean result. */
			whereClause = coerce_to_boolean(pstate, whereClause, "WHERE");

			/* we have to fix its collations too */
			assign_expr_collations(pstate, whereClause);

			whereClause = eval_const_expressions(NULL, whereClause);

			whereClause = (Node *) canonicalize_qual((Expr *) whereClause, false);
			whereClause = (Node *) make_ands_implicit((Expr *) whereClause);
		}

		tupDesc = RelationGetDescr(rel);
		attnums = CopyGetAttnums(tupDesc, rel, attnamelist);
		foreach (cur, attnums)
		{
			int			attno = lfirst_int(cur) -
			FirstLowInvalidHeapAttributeNumber;

			if (is_from)
				rte->insertedCols = bms_add_member(rte->insertedCols, attno);
			else
				rte->selectedCols = bms_add_member(rte->selectedCols, attno);
		}
		ExecCheckRTPerms(pstate->p_rtable, true);

		/*
		 * Permission check for row security policies.
		 *
		 * check_enable_rls will ereport(ERROR) if the user has requested
		 * something invalid and will otherwise indicate if we should enable
		 * RLS (returns RLS_ENABLED) or not for this COPY statement.
		 *
		 * If the relation has a row security policy and we are to apply it
		 * then perform a "query" copy and allow the normal query processing
		 * to handle the policies.
		 *
		 * If RLS is not enabled for this, then just fall through to the
		 * normal non-filtering relation handling.
		 */
		if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED)
		{
			SelectStmt *select;
			ColumnRef  *cr;
			ResTarget  *target;
			RangeVar   *from;
			List	   *targetList = NIL;

			if (is_from)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("COPY FROM not supported with row-level security"),
						 errhint("Use INSERT statements instead.")));

			/*
			 * Build target list
			 *
			 * If no columns are specified in the attribute list of the COPY
			 * command, then the target list is 'all' columns. Therefore, '*'
			 * should be used as the target list for the resulting SELECT
			 * statement.
			 *
			 * In the case that columns are specified in the attribute list,
			 * create a ColumnRef and ResTarget for each column and add them
			 * to the target list for the resulting SELECT statement.
			 */
			if (!stmt->attlist)
			{
				cr = makeNode(ColumnRef);
				cr->fields = list_make1(makeNode(A_Star));
				cr->location = -1;

				target = makeNode(ResTarget);
				target->name = NULL;
				target->indirection = NIL;
				target->val = (Node *) cr;
				target->location = -1;

				targetList = list_make1(target);
			}
			else
			{
				ListCell   *lc;

				foreach(lc, stmt->attlist)
				{
					/*
					 * Build the ColumnRef for each column.  The ColumnRef
					 * 'fields' property is a String 'Value' node (see
					 * nodes/value.h) that corresponds to the column name
					 * respectively.
					 */
					cr = makeNode(ColumnRef);
					cr->fields = list_make1(lfirst(lc));
					cr->location = -1;

					/* Build the ResTarget and add the ColumnRef to it. */
					target = makeNode(ResTarget);
					target->name = NULL;
					target->indirection = NIL;
					target->val = (Node *) cr;
					target->location = -1;

					/* Add each column to the SELECT statement's target list */
					targetList = lappend(targetList, target);
				}
			}

			/*
			 * Build RangeVar for from clause, fully qualified based on the
			 * relation which we have opened and locked.
			 */
			from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								pstrdup(RelationGetRelationName(rel)),
								-1);

			/* Build query */
			select = makeNode(SelectStmt);
			select->targetList = targetList;
			select->fromClause = list_make1(from);

			query = makeNode(RawStmt);
			query->stmt = (Node *) select;
			query->stmt_location = stmt_location;
			query->stmt_len = stmt_len;

			/*
			 * Close the relation for now, but keep the lock on it to prevent
			 * changes between now and when we start the query-based COPY.
			 *
			 * We'll reopen it later as part of the query-based COPY.
			 */
			table_close(rel, NoLock);
			rel = NULL;
		}
	}
	else
	{
		Assert(stmt->query);

		query = makeNode(RawStmt);
		query->stmt = stmt->query;
		query->stmt_location = stmt_location;
		query->stmt_len = stmt_len;

		relid = InvalidOid;
		rel = NULL;
	}

	if (is_from)
	{
		Assert(rel);

		if (stmt->sreh && Gp_role != GP_ROLE_EXECUTE && !rel->rd_cdbpolicy)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY single row error handling only available for distributed user tables")));

		/*
		 * GPDB_91_MERGE_FIXME: is it possible to get to this point in the code
		 * with a temporary relation that belongs to another session? If so, the
		 * following code doesn't function as expected.
		 */
		/* check read-only transaction and parallel mode */
		if (XactReadOnly && !rel->rd_islocaltemp)
			PreventCommandIfReadOnly("COPY FROM");
		PreventCommandIfParallelMode("COPY FROM");

		cstate = BeginCopyFrom(pstate, rel, stmt->filename, stmt->is_program,
							   NULL, NULL, stmt->attlist, options);
		cstate->whereClause = whereClause;

		/*
		 * Error handling setup
		 */
		if (cstate->sreh)
		{
			/* Single row error handling requested */
			SingleRowErrorDesc *sreh = cstate->sreh;
			char		log_to_file = LOG_ERRORS_DISABLE;

			if (IS_LOG_TO_FILE(sreh->log_error_type))
			{
				cstate->errMode = SREH_LOG;
				/* LOG ERRORS PERSISTENTLY for COPY is not allowed for now. */
				log_to_file = LOG_ERRORS_ENABLE;
			}
			else
			{
				cstate->errMode = SREH_IGNORE;
			}
			cstate->cdbsreh = makeCdbSreh(sreh->rejectlimit,
										  sreh->is_limit_in_rows,
										  cstate->filename,
										  stmt->relation->relname,
										  log_to_file);
			if (rel)
				cstate->cdbsreh->relid = RelationGetRelid(rel);
		}
		else
		{
			/* No single row error handling requested. Use "all or nothing" */
			cstate->cdbsreh = NULL; /* default - no SREH */
			cstate->errMode = ALL_OR_NOTHING; /* default */
		}

		PG_TRY();
		{
			if (Gp_role == GP_ROLE_DISPATCH && cstate->on_segment)
				*processed = CopyDispatchOnSegment(cstate, stmt);
			else
				*processed = CopyFrom(cstate);	/* copy from file to database */
		}
		PG_CATCH();
		{
			if (cstate->cdbCopy)
			{
				MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

				cdbCopyAbort(cstate->cdbCopy);
				cstate->cdbCopy = NULL;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_RE_THROW();
		}
		PG_END_TRY();
		EndCopyFrom(cstate);
	}
	else
	{
		/*
		 * GPDB_91_MERGE_FIXME: ExecutorStart() is called in BeginCopyTo,
		 * but the TRY-CATCH block only starts here. If an error is
		 * thrown in-between, we would fail to call mppExecutorCleanup. We
		 * really should be using a ResourceOwner or something else for
		 * cleanup, instead of TRY-CATCH blocks...
		 *
		 * Update: I tried to fix this using the glob_cstate hack. It's ugly,
		 * but fixes at least some cases that came up in regression tests.
		 */
		PG_TRY();
		{
			cstate = BeginCopyTo(pstate, rel, query, relid,
								 stmt->filename, stmt->is_program,
								 stmt->attlist, options);

			/*
			 * "copy t to file on segment"					CopyDispatchOnSegment
			 * "copy (select * from t) to file on segment"	CopyToQueryOnSegment
			 * "copy t/(select * from t) to file"			DoCopyTo
			 */
			if (Gp_role == GP_ROLE_DISPATCH && cstate->on_segment)
			{
				if (cstate->rel)
					*processed = CopyDispatchOnSegment(cstate, stmt);
				else
					*processed = CopyToQueryOnSegment(cstate);
			}
			else
				*processed = DoCopyTo(cstate);	/* copy from database to file */
		}
		PG_CATCH();
		{
			if (glob_cstate && glob_cstate->queryDesc)
			{
				/* should shutdown the mpp stuff such as interconnect and dispatch thread */
				mppExecutorCleanup(glob_cstate->queryDesc);
			}
			PG_RE_THROW();
		}
		PG_END_TRY();

		EndCopyTo(cstate, processed);
	}
	/*
	 * Close the relation.  If reading, we can release the AccessShareLock we
	 * got; if writing, we should hold the lock until end of transaction to
	 * ensure that updates will be committed before lock is released.
	 */
	if (rel != NULL)
		table_close(rel, (is_from ? NoLock : AccessShareLock));

	/* Issue automatic ANALYZE if conditions are satisfied (MPP-4082). */
	if (Gp_role == GP_ROLE_DISPATCH && is_from)
	{
		bool inFunction = already_under_executor_run() || utility_nested();
		auto_stats(AUTOSTATS_CMDTYPE_COPY, relid, *processed, inFunction);
	}
}

/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into cstate, applying appropriate error checking.
 *
 * cstate is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, cstate should be passed as NULL
 * (since external users don't know sizeof(CopyStateData)) and the collected
 * data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
void
ProcessCopyOptions(ParseState *pstate,
				   CopyState cstate,
				   bool is_from,
				   List *options)
{
	bool		format_specified = false;
	ListCell   *option;

	/* Support external use for option sanity checking */
	if (cstate == NULL)
		cstate = (CopyStateData *) palloc0(sizeof(CopyStateData));

	cstate->escape_off = false;
	cstate->skip_foreign_partitions = false;

	cstate->is_copy_from = is_from;

	cstate->delim_off = false;
	cstate->file_encoding = -1;

	/* Extract options from the statement node tree */
	foreach(option, options)
	{
		DefElem    *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "format") == 0)
		{
			char	   *fmt = defGetString(defel);

			if (format_specified)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			format_specified = true;
			if (strcmp(fmt, "text") == 0)
				 /* default format */ ;
			else if (strcmp(fmt, "csv") == 0)
				cstate->csv_mode = true;
			else if (strcmp(fmt, "binary") == 0)
				cstate->binary = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("COPY format \"%s\" not recognized", fmt),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "freeze") == 0)
		{
			if (cstate->freeze)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->freeze = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			if (cstate->delim)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->delim = defGetString(defel);

			if (cstate->delim && pg_strcasecmp(cstate->delim, "off") == 0)
				cstate->delim_off = true;
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			if (cstate->null_print)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->null_print = defGetString(defel);

			/*
			 * MPP-2010: unfortunately serialization function doesn't
			 * distinguish between 0x0 and empty string. Therefore we
			 * must assume that if NULL AS was indicated and has no value
			 * the actual value is an empty string.
			 */
			if(!cstate->null_print)
				cstate->null_print = "";
		}
		else if (strcmp(defel->defname, "header") == 0)
		{
			if (cstate->header_line)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->header_line = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "quote") == 0)
		{
			if (cstate->quote)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->quote = defGetString(defel);
		}
		else if (strcmp(defel->defname, "escape") == 0)
		{
			if (cstate->escape)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->escape = defGetString(defel);
		}
		else if (strcmp(defel->defname, "force_quote") == 0)
		{
			if (cstate->force_quote || cstate->force_quote_all)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, A_Star))
				cstate->force_quote_all = true;
			else if (defel->arg && IsA(defel->arg, List))
				cstate->force_quote = castNode(List, defel->arg);
			else if (defel->arg && IsA(defel->arg, String))
			{
				if (strcmp(strVal(defel->arg), "*") == 0)
					cstate->force_quote_all = true;
				else
				{
					/* OPTIONS (force_quote 'c1,c2') */
					cstate->force_quote = parse_joined_option_list(strVal(defel->arg), ",");
				}
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_not_null") == 0)
		{
			if (cstate->force_notnull)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_notnull = castNode(List, defel->arg);
			else if (defel->arg && IsA(defel->arg, String))
			{
				/* OPTIONS (force_not_null 'c1,c2') */
				cstate->force_notnull = parse_joined_option_list(strVal(defel->arg), ",");
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_null") == 0)
		{
			if (cstate->force_null)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_null = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "convert_selectively") == 0)
		{
			/*
			 * Undocumented, not-accessible-from-SQL option: convert only the
			 * named columns to binary form, storing the rest as NULLs. It's
			 * allowed for the column list to be NIL.
			 */
			if (cstate->convert_selectively)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->convert_selectively = true;
			if (defel->arg == NULL || IsA(defel->arg, List))
				cstate->convert_select = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "encoding") == 0)
		{
			if (cstate->file_encoding >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->file_encoding = pg_char_to_encoding(defGetString(defel));
			if (cstate->file_encoding < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a valid encoding name",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "fill_missing_fields") == 0)
		{
			if (cstate->fill_missing)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->fill_missing = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "newline") == 0)
		{
			if (cstate->eol_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->eol_str = strVal(defel->arg);
		}
		else if (strcmp(defel->defname, "sreh") == 0)
		{
			if (defel->arg == NULL || !IsA(defel->arg, SingleRowErrorDesc))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname)));
			if (cstate->sreh)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			cstate->sreh = (SingleRowErrorDesc *) defel->arg;
		}
		else if (strcmp(defel->defname, "on_segment") == 0)
		{
			if (cstate->on_segment)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->on_segment = true;
		}
		else if (strcmp(defel->defname, "skip_foreign_partitions") == 0)
		{
			if (cstate->skip_foreign_partitions)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->skip_foreign_partitions = true;
		}
		else if (!rel_is_external_table(cstate->rel->rd_id))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
	}

	/*
	 * Check for incompatible options (must do these two before inserting
	 * defaults)
	 */
	if (cstate->binary && cstate->delim)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify DELIMITER in BINARY mode")));

	if (cstate->binary && cstate->null_print)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify NULL in BINARY mode")));

	cstate->eol_type = EOL_UNKNOWN;

	/* Set defaults for omitted options */
	if (!cstate->delim)
		cstate->delim = cstate->csv_mode ? "," : "\t";

	if (!cstate->null_print)
		cstate->null_print = cstate->csv_mode ? "" : "\\N";
	cstate->null_print_len = strlen(cstate->null_print);

	if (cstate->csv_mode)
	{
		if (!cstate->quote)
			cstate->quote = "\"";
		if (!cstate->escape)
			cstate->escape = cstate->quote;
	}

	if (!cstate->csv_mode && !cstate->escape)
		cstate->escape = "\\";			/* default escape for text mode */

	/* Only single-byte delimiter strings are supported. */
	/* GPDB: This is checked later */
#if 0
	if (strlen(cstate->delim) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must be a single one-byte character")));
#endif

	/* Disallow end-of-line characters */
	if (strchr(cstate->delim, '\r') != NULL ||
		strchr(cstate->delim, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be newline or carriage return")));

	if (strchr(cstate->null_print, '\r') != NULL ||
		strchr(cstate->null_print, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY null representation cannot use newline or carriage return")));

	/*
	 * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
	 * backslash because it would be ambiguous.  We can't allow the other
	 * cases because data characters matching the delimiter must be
	 * backslashed, and certain backslash combinations are interpreted
	 * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
	 * more than strictly necessary, but seems best for consistency and
	 * future-proofing.  Likewise we disallow all digits though only octal
	 * digits are actually dangerous.
	 */
	if (!cstate->csv_mode && !cstate->delim_off &&
		strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789",
			   cstate->delim[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be \"%s\"", cstate->delim)));

	/* Check header */
	/*
	 * In PostgreSQL, HEADER is not allowed in text mode either, but in GPDB,
	 * only forbid it with BINARY.
	 */
	if (cstate->binary && cstate->header_line)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify HEADER in BINARY mode")));

	/* Check quote */
	if (!cstate->csv_mode && cstate->quote != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote available only in CSV mode")));

	if (cstate->csv_mode && strlen(cstate->quote) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote must be a single one-byte character")));

	if (cstate->csv_mode && cstate->delim[0] == cstate->quote[0] && !cstate->delim_off)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter and quote must be different")));

	/* Check escape */
	if (cstate->csv_mode && cstate->escape != NULL && strlen(cstate->escape) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY escape in CSV format must be a single character")));

	if (!cstate->csv_mode && cstate->escape != NULL &&
		(strchr(cstate->escape, '\r') != NULL ||
		strchr(cstate->escape, '\n') != NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY escape representation in text format cannot use newline or carriage return")));

	if (!cstate->csv_mode && cstate->escape != NULL && strlen(cstate->escape) != 1)
	{
		if (pg_strcasecmp(cstate->escape, "off") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY escape must be a single character, or [OFF/off] to disable escapes")));
	}

	/* Check force_quote */
	if (!cstate->csv_mode && (cstate->force_quote || cstate->force_quote_all))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote available only in CSV mode")));
	if ((cstate->force_quote || cstate->force_quote_all) && is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote only available using COPY TO")));

	/* Check force_notnull */
	if (!cstate->csv_mode && cstate->force_notnull != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null available only in CSV mode")));
	if (cstate->force_notnull != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null only available using COPY FROM")));

	/* Check force_null */
	if (!cstate->csv_mode && cstate->force_null != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null available only in CSV mode")));

	if (cstate->force_null != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null only available using COPY FROM")));

	/* Don't allow the delimiter to appear in the null string. */
	if (strchr(cstate->null_print, cstate->delim[0]) != NULL && !cstate->delim_off)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must not appear in the NULL specification")));

	/* Don't allow the CSV quote char to appear in the null string. */
	if (cstate->csv_mode &&
		strchr(cstate->null_print, cstate->quote[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CSV quote character must not appear in the NULL specification")));

	/*
	 * DELIMITER
	 *
	 * Only single-byte delimiter strings are supported. In addition, if the
	 * server encoding is a multibyte character encoding we only allow the
	 * delimiter to be an ASCII character (like postgresql. For more info
	 * on this see discussion and comments in MPP-3756).
	 */
	if (pg_database_encoding_max_length() == 1)
	{
		/* single byte encoding such as ascii, latinx and other */
		if (strlen(cstate->delim) != 1 && !cstate->delim_off)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY delimiter must be a single one-byte character, or \'off\'")));
	}
	else
	{
		/* multi byte encoding such as utf8 */
		if ((strlen(cstate->delim) != 1 || IS_HIGHBIT_SET(cstate->delim[0])) && !cstate->delim_off )
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY delimiter must be a single one-byte character, or \'off\'")));
	}

	if (!cstate->csv_mode && strchr(cstate->delim, '\\') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be backslash")));

	if (cstate->fill_missing && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("fill missing fields only available for data loading, not unloading")));

	/*
	 * NEWLINE
	 */
	if (cstate->eol_str)
	{
		if (!is_from)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_YET),
					errmsg("newline currently available for data loading only, not unloading")));
		}
		else
		{
			if (pg_strcasecmp(cstate->eol_str, "lf") == 0)
				cstate->eol_type = EOL_NL;
			else if (pg_strcasecmp(cstate->eol_str, "cr") == 0)
				cstate->eol_type = EOL_CR;
			else if (pg_strcasecmp(cstate->eol_str, "crlf") == 0)
				cstate->eol_type = EOL_CRNL;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid value for NEWLINE \"%s\"",
								cstate->eol_str),
						 errhint("Valid options are: 'LF', 'CRLF' and 'CR'.")));
		}
	}

	if (cstate->escape != NULL && pg_strcasecmp(cstate->escape, "off") == 0)
	{
		cstate->escape_off = true;
	}
}

/*
 * Common setup routines used by BeginCopyFrom and BeginCopyTo.
 *
 * Iff <binary>, unload or reload in the binary format, as opposed to the
 * more wasteful but more robust and portable text format.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 */
CopyState
BeginCopy(ParseState *pstate,
		  bool is_from,
		  Relation rel,
		  RawStmt *raw_query,
		  Oid queryRelId,
		  List *attnamelist,
		  List *options,
		  TupleDesc tupDesc)
{
	CopyState	cstate;
	int			num_phys_attrs;
	MemoryContext oldcontext;

	/* Allocate workspace and zero all fields */
	cstate = (CopyStateData *) palloc0(sizeof(CopyStateData));

	glob_cstate = cstate;

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Greenplum needs this to detect custom protocol */
	if (rel)
		cstate->rel = rel;

	/* Extract options from the statement node tree */
	ProcessCopyOptions(pstate, cstate, is_from, options);

	if (cstate->delim_off && !rel_is_external_table(rel->rd_id))
	{
		/*
		 * We don't support delimiter 'off' for COPY because the QD COPY
		 * sometimes internally adds columns to the data that it sends to
		 * the QE COPY modules, and it uses the delimiter for it. There
		 * are ways to work around this but for now it's not important and
		 * we simply don't support it.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("using no delimiter is only supported for external tables")));
	}

	/* Process the source/target relation or query */
	if (rel)
	{
		Assert(!raw_query);

		tupDesc = RelationGetDescr(cstate->rel);
	}
	else if(raw_query)
	{
		List	   *rewritten;
		Query	   *query;
		PlannedStmt *plan;
		DestReceiver *dest;

		Assert(!is_from);
		cstate->rel = NULL;

		/*
		 * Run parse analysis and rewrite.  Note this also acquires sufficient
		 * locks on the source table(s).
		 *
		 * Because the parser and planner tend to scribble on their input, we
		 * make a preliminary copy of the source querytree.  This prevents
		 * problems in the case that the COPY is in a portal or plpgsql
		 * function and is executed repeatedly.  (See also the same hack in
		 * DECLARE CURSOR and PREPARE.)  XXX FIXME someday.
		 */
		rewritten = pg_analyze_and_rewrite(copyObject(raw_query),
										   pstate->p_sourcetext, NULL, 0,
										   NULL);

		/* check that we got back something we can work with */
		if (rewritten == NIL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DO INSTEAD NOTHING rules are not supported for COPY")));
		}
		else if (list_length(rewritten) > 1)
		{
			ListCell   *lc;

			/* examine queries to determine which error message to issue */
			foreach(lc, rewritten)
			{
				Query	   *q = lfirst_node(Query, lc);

				if (q->querySource == QSRC_QUAL_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("conditional DO INSTEAD rules are not supported for COPY")));
				if (q->querySource == QSRC_NON_INSTEAD_RULE)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DO ALSO rules are not supported for the COPY")));
			}

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("multi-statement DO INSTEAD rules are not supported for COPY")));
		}

		query = linitial_node(Query, rewritten);


		if (cstate->on_segment && IsA(query, Query))
		{
			query->parentStmtType = PARENTSTMTTYPE_COPY;
		}
		/* Query mustn't use INTO, either */
		if (query->utilityStmt != NULL &&
			IsA(query->utilityStmt, CreateTableAsStmt))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY (SELECT INTO) is not supported")));

		Assert(query->utilityStmt == NULL);

		/*
		 * Similarly the grammar doesn't enforce the presence of a RETURNING
		 * clause, but this is required here.
		 */
		if (query->commandType != CMD_SELECT &&
			query->returningList == NIL)
		{
			Assert(query->commandType == CMD_INSERT ||
				   query->commandType == CMD_UPDATE ||
				   query->commandType == CMD_DELETE);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY query must have a RETURNING clause")));
		}

		/* plan the query */
		int			cursorOptions = CURSOR_OPT_PARALLEL_OK;

		/* GPDB: Pass the IGNORE EXTERNAL PARTITION option to the planner. */
		if (cstate->skip_foreign_partitions)
			cursorOptions |= CURSOR_OPT_SKIP_FOREIGN_PARTITIONS;

		plan = pg_plan_query(query, cursorOptions, NULL);

		/*
		 * With row level security and a user using "COPY relation TO", we
		 * have to convert the "COPY relation TO" to a query-based COPY (eg:
		 * "COPY (SELECT * FROM relation) TO"), to allow the rewriter to add
		 * in any RLS clauses.
		 *
		 * When this happens, we are passed in the relid of the originally
		 * found relation (which we have locked).  As the planner will look up
		 * the relation again, we double-check here to make sure it found the
		 * same one that we have locked.
		 */
		if (queryRelId != InvalidOid)
		{
			/*
			 * Note that with RLS involved there may be multiple relations,
			 * and while the one we need is almost certainly first, we don't
			 * make any guarantees of that in the planner, so check the whole
			 * list and make sure we find the original relation.
			 */
			if (!list_member_oid(plan->relationOids, queryRelId))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("relation referenced by COPY statement has changed")));
		}

		/*
		 * Use a snapshot with an updated command ID to ensure this query sees
		 * results of any previously executed queries.
		 */
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create dest receiver for COPY OUT */
		dest = CreateDestReceiver(DestCopyOut);
		((DR_copy *) dest)->cstate = cstate;

		/* Create a QueryDesc requesting no output */
		cstate->queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
											GetActiveSnapshot(),
											InvalidSnapshot,
											dest, NULL, NULL,
											GP_INSTRUMENT_OPTS);
		if (cstate->on_segment)
			cstate->queryDesc->plannedstmt->copyIntoClause =
					MakeCopyIntoClause(glob_copystmt);

		/* GPDB hook for collecting query info */
		if (query_info_collect_hook)
			(*query_info_collect_hook)(METRICS_QUERY_SUBMIT, cstate->queryDesc);

		/*
		 * Call ExecutorStart to prepare the plan for execution.
		 *
		 * ExecutorStart computes a result tupdesc for us
		 */
		ExecutorStart(cstate->queryDesc, 0);

		tupDesc = cstate->queryDesc->tupDesc;
	}

	cstate->attnamelist = attnamelist;
	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_QUOTE name list to per-column flags, check validity */
	cstate->force_quote_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_quote_all)
	{
		int			i;

		for (i = 0; i < num_phys_attrs; i++)
			cstate->force_quote_flags[i] = true;
	}
	else if (cstate->force_quote)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_quote);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_QUOTE column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_quote_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
	cstate->force_notnull_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_notnull)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_notnull);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NOT_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_notnull_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NULL name list to per-column flags, check validity */
	cstate->force_null_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_null)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_null);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_null_flags[attnum - 1] = true;
		}
	}

	/* Convert convert_selectively name list to per-column flags */
	if (cstate->convert_selectively)
	{
		List	   *attnums;
		ListCell   *cur;

		cstate->convert_select_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->convert_select);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg_internal("selected column \"%s\" not referenced by COPY",
										 NameStr(attr->attname))));
			cstate->convert_select_flags[attnum - 1] = true;
		}
	}

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();

	/*
	 * Set up encoding conversion info.  Even if the file and server encodings
	 * are the same, we must apply pg_any_to_server() to validate data in
	 * multibyte encodings.
	 *
	 * In COPY_EXECUTE mode, the dispatcher has already done the conversion.
	 */
	if (cstate->dispatch_mode != COPY_DISPATCH)
	{
		cstate->need_transcoding =
			((cstate->file_encoding != GetDatabaseEncoding() ||
			  pg_database_encoding_max_length() > 1));
		/* See Multibyte encoding comment above */
		cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
		setEncodingConversionProc(cstate, cstate->file_encoding, !is_from);
	}
	else
	{
		cstate->need_transcoding = false;
		cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
	}

	cstate->copy_dest = COPY_FILE;	/* default */

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Dispatch a COPY ON SEGMENT statement to QEs.
 */
static uint64
CopyDispatchOnSegment(CopyState cstate, const CopyStmt *stmt)
{
	CopyStmt   *dispatchStmt;
	CdbPgResults pgresults = {0};
	int			i;
	uint64		processed = 0;
	uint64		rejected = 0;

	dispatchStmt = copyObject((CopyStmt *) stmt);

	CdbDispatchUtilityStatement((Node *) dispatchStmt,
								DF_NEED_TWO_PHASE |
								DF_WITH_SNAPSHOT |
								DF_CANCEL_ON_ERROR,
								NIL,
								&pgresults);

	/*
	 * GPDB_91_MERGE_FIXME: SREH handling seems to be handled in a different
	 * place for every type of copy. This should be consolidated with the
	 * others.
	 */
	for (i = 0; i < pgresults.numResults; ++i)
	{
		struct pg_result *result = pgresults.pg_results[i];

		processed += result->numCompleted;
		rejected += result->numRejected;
	}

	if (rejected)
		ReportSrehResults(NULL, rejected);

	cdbdisp_clearCdbPgResults(&pgresults);
	return processed;
}


/*
 * Modify the filename in cstate->filename, and cstate->cdbsreh if any,
 * for COPY ON SEGMENT.
 *
 * Replaces the "<SEGID>" token in the filename with this segment's ID.
 */
static void
MangleCopyFileName(CopyState cstate)
{
	char	   *filename = cstate->filename;
	StringInfoData filepath;

	initStringInfo(&filepath);
	appendStringInfoString(&filepath, filename);

	replaceStringInfoString(&filepath, "<SEG_DATA_DIR>", DataDir);

	if (strstr(filename, "<SEGID>") == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("<SEGID> is required for file name")));

	char segid_buf[8];
	snprintf(segid_buf, 8, "%d", GpIdentity.segindex);
	replaceStringInfoString(&filepath, "<SEGID>", segid_buf);

	cstate->filename = filepath.data;
	/* Rename filename if error log needed */
	if (NULL != cstate->cdbsreh)
	{
		snprintf(cstate->cdbsreh->filename,
				 sizeof(cstate->cdbsreh->filename), "%s",
				 filepath.data);
	}
}

/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
static void
EndCopy(CopyState cstate)
{
	if (cstate->is_program)
	{
		close_program_pipes(cstate, true);
	}
	else
	{
		if (cstate->filename != NULL && FreeFile(cstate->copy_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							cstate->filename)));
	}

	/* Clean up single row error handling related memory */
	if (cstate->cdbsreh)
		destroyCdbSreh(cstate->cdbsreh);

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

CopyIntoClause*
MakeCopyIntoClause(CopyStmt *stmt)
{
	CopyIntoClause *copyIntoClause;
	copyIntoClause = makeNode(CopyIntoClause);

	copyIntoClause->is_program = stmt->is_program;
	copyIntoClause->filename = stmt->filename;
	copyIntoClause->options = stmt->options;
	copyIntoClause->attlist = stmt->attlist;

	return copyIntoClause;
}

CopyState
BeginCopyToOnSegment(QueryDesc *queryDesc)
{
	CopyState	cstate;
	MemoryContext oldcontext;
	ListCell   *cur;

	TupleDesc	tupDesc;
	int			num_phys_attrs;
	FormData_pg_attribute *attr;
	char	   *filename;
	CopyIntoClause *copyIntoClause;

	Assert(Gp_role == GP_ROLE_EXECUTE);

	copyIntoClause = queryDesc->plannedstmt->copyIntoClause;
	tupDesc = queryDesc->tupDesc;
	cstate = BeginCopy(false, NULL, NULL, NULL, InvalidOid, copyIntoClause->attlist,
					   copyIntoClause->options, tupDesc);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	cstate->null_print_client = cstate->null_print;		/* default */

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	cstate->filename = pstrdup(copyIntoClause->filename);
	cstate->is_program = copyIntoClause->is_program;

	if (cstate->on_segment)
		MangleCopyFileName(cstate);
	filename = cstate->filename;

	if (cstate->is_program)
	{
		cstate->program_pipes = open_program_pipes(cstate, true);
		cstate->copy_file = fdopen(cstate->program_pipes->pipes[0], PG_BINARY_W);

		if (cstate->copy_file == NULL)
			ereport(ERROR,
					(errmsg("could not execute command \"%s\": %m",
							cstate->filename)));
	}
	else
	{
		mode_t oumask; /* Pre-existing umask value */
		struct stat st;

		/*
		 * Prevent write to relative path ... too easy to shoot oneself in
		 * the foot by overwriting a database file ...
		 */
		if (!is_absolute_path(filename))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
							errmsg("relative path not allowed for COPY to file")));

		oumask = umask(S_IWGRP | S_IWOTH);
		cstate->copy_file = AllocateFile(filename, PG_BINARY_W);
		umask(oumask);
		if (cstate->copy_file == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("could not open file \"%s\" for writing: %m", filename)));

		// Increase buffer size to improve performance  (cmcdevitt)
		setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes

		fstat(fileno(cstate->copy_file), &st);
		if (S_ISDIR(st.st_mode))
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is a directory", filename)));
	}

	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	/* Get info about the columns we need to process. */
	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Oid			out_func_oid;
		bool		isvarlena;

		if (cstate->binary)
			getTypeBinaryOutputInfo(attr[attnum - 1].atttypid,
									&out_func_oid,
									&isvarlena);
		else
			getTypeOutputInfo(attr[attnum - 1].atttypid,
							  &out_func_oid,
							  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	if (cstate->binary)
	{
		/* Generate header for a binary copy */
		int32		tmp;

		/* Signature */
		CopySendData(cstate, BinarySignature, 11);
		/* Flags field */
		tmp = 0;
		CopySendInt32(cstate, tmp);
		/* No header extension */
		tmp = 0;
		CopySendInt32(cstate, tmp);
	}
	else
	{
		/* if a header has been requested send the line */
		if (cstate->header_line)
		{
			bool		hdr_delim = false;

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->delim[0]);
				hdr_delim = true;

				colname = NameStr(attr[attnum - 1].attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			CopySendEndOfRow(cstate);
		}
	}

	MemoryContextSwitchTo(oldcontext);
	return cstate;
}

/*
 * Setup CopyState to read tuples from a table or a query for COPY TO.
 */
static CopyState
BeginCopyTo(ParseState *pstate,
			Relation rel,
			RawStmt *query,
			Oid queryRelId,
			const char *filename,
			bool is_program,
			List *attnamelist,
			List *options)
{
	CopyState	cstate;
	MemoryContext oldcontext;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_TO,
		0
	};

	if (rel != NULL && rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		if (rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from materialized view \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from foreign table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Try the COPY (SELECT ...) TO variant.")));
		else if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from sequence \"%s\"",
							RelationGetRelationName(rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy from non-table relation \"%s\"",
							RelationGetRelationName(rel))));
	}

	cstate = BeginCopy(pstate, false, rel, query, queryRelId, attnamelist,
					   options, NULL);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	if (cstate->on_segment)
		progress_vals[0] = PROGRESS_COPY_COMMAND_TO_ON_SEGMENT;

	/* Determine the mode */
	if (Gp_role == GP_ROLE_DISPATCH && !cstate->on_segment &&
		cstate->rel && cstate->rel->rd_cdbpolicy)
	{
		cstate->dispatch_mode = COPY_DISPATCH;
	}
	else
		cstate->dispatch_mode = COPY_DIRECT;

	bool		pipe = (filename == NULL || (Gp_role == GP_ROLE_EXECUTE && !cstate->on_segment));

	if (cstate->on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* in ON SEGMENT mode, we don't open anything on the dispatcher. */

		if (filename == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("STDOUT is not supported by 'COPY ON SEGMENT'")));
	}
	else if (pipe)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;
		Assert(!is_program || Gp_role == GP_ROLE_EXECUTE);	/* the grammar does not allow this */
		if (whereToSendOutput != DestRemote)
			cstate->copy_file = stdout;
	}
	else
	{
		cstate->filename = pstrdup(filename);
		cstate->is_program = is_program;

		if (cstate->on_segment)
			MangleCopyFileName(cstate);
		filename = cstate->filename;

		if (is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->program_pipes = open_program_pipes(cstate, true);
			cstate->copy_file = fdopen(cstate->program_pipes->pipes[0], PG_BINARY_W);

			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			mode_t oumask; /* Pre-existing umask value */
			struct stat st;

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;

			/*
			 * Prevent write to relative path ... too easy to shoot oneself in
			 * the foot by overwriting a database file ...
			 */
			if (!is_absolute_path(filename))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("relative path not allowed for COPY to file")));

			oumask = umask(S_IWGRP | S_IWOTH);
			PG_TRY();
			{
				cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_W);
			}
			PG_CATCH();
			{
				umask(oumask);
				PG_RE_THROW();
			}
			PG_END_TRY();
			umask(oumask);
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int			save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for writing: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY TO instructs the PostgreSQL server process to write a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));

			// Increase buffer size to improve performance  (cmcdevitt)
			setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes
			}

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
								errmsg("\"%s\" is a directory", filename)));
		}
	}

	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	pgstat_progress_update_multi_param(2, progress_cols, progress_vals);

	cstate->bytes_processed = 0;

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Set up CopyState for writing to a foreign or external table.
 */
CopyState
BeginCopyToForeignTable(Relation forrel, List *options)
{
	CopyState	cstate;

	Assert(forrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);

	cstate = BeginCopy(NULL, false, forrel,
					   NULL, /* raw_query */
					   InvalidOid,
					   NIL, options,
					   RelationGetDescr(forrel));
	cstate->dispatch_mode = COPY_DIRECT;

	/*
	 * We use COPY_CALLBACK to mean that the each line should be
	 * left in fe_msgbuf. There is no actual callback!
	 */
	cstate->copy_dest = COPY_CALLBACK;

	/*
	 * Some more initialization, that in the normal COPY TO codepath, is done
	 * in CopyTo() itself.
	 */
	cstate->null_print_client = cstate->null_print;		/* default */
	if (cstate->need_transcoding)
		cstate->null_print_client = pg_server_to_custom(cstate->null_print,
														cstate->null_print_len,
														cstate->file_encoding,
														cstate->enc_conversion_proc);

	return cstate;
}

/*
 * This intermediate routine exists mainly to localize the effects of setjmp
 * so we don't need to plaster a lot of variables with "volatile".
 */
static uint64
DoCopyTo(CopyState cstate)
{
	bool		pipe = (cstate->filename == NULL);
	bool		fe_copy = (pipe && whereToSendOutput == DestRemote);
	uint64		processed;

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet("DoCopyToFail", DDLNotSpecified, "", "");
#endif

	PG_TRY();
	{
		if (fe_copy)
			SendCopyBegin(cstate);

		/*
		 * We want to dispatch COPY TO commands only in the case that
		 * we are the dispatcher and we are copying from a user relation
		 * (a relation where data is distributed in the segment databases).
		 * Otherwize, if we are not the dispatcher *or* if we are
		 * doing COPY (SELECT) we just go straight to work, without
		 * dispatching COPY commands to executors.
		 */
		if (Gp_role == GP_ROLE_DISPATCH && cstate->rel && cstate->rel->rd_cdbpolicy)
			processed = CopyToDispatch(cstate);
		else
			processed = CopyTo(cstate);

		if (fe_copy)
			SendCopyEnd(cstate);
		else if (Gp_role == GP_ROLE_EXECUTE && cstate->on_segment)
		{
			/*
			 * For COPY ON SEGMENT command, switch back to front end
			 * before sending copy end which is "\."
			 */
			cstate->copy_dest = COPY_NEW_FE;
			SendCopyEnd(cstate);
		}
	}
	PG_CATCH();
	{
		/*
		 * Make sure we turn off old-style COPY OUT mode upon error. It is
		 * okay to do this in all cases, since it does nothing if the mode is
		 * not on.
		 */
		if (Gp_role == GP_ROLE_EXECUTE && cstate->on_segment)
			cstate->copy_dest = COPY_NEW_FE;

		pq_endcopyout(true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return processed;
}

void EndCopyToOnSegment(CopyState cstate)
{
	Assert(Gp_role == GP_ROLE_EXECUTE);

	if (cstate->binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);

		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	MemoryContextDelete(cstate->rowcontext);

	EndCopy(cstate);
}

/*
 * Clean up storage and release resources for COPY TO.
 */
static void
EndCopyTo(CopyState cstate, uint64 *processed)
{
	if (cstate->queryDesc != NULL)
	{
		/* Close down the query and free resources. */
		ExecutorFinish(cstate->queryDesc);
		ExecutorEnd(cstate->queryDesc);
		if (cstate->queryDesc->es_processed > 0)
			*processed = cstate->queryDesc->es_processed;
		FreeQueryDesc(cstate->queryDesc);
		PopActiveSnapshot();
	}

	/* Clean up storage */
	EndCopy(cstate);
}

/*
 * Copy FROM relation TO file, in the dispatcher. Starts a COPY TO command on
 * each of the executors and gathers all the results and writes it out.
 */
static uint64
CopyToDispatch(CopyState cstate)
{
	CopyStmt   *stmt = glob_copystmt;
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	int			attr_count;
	FormData_pg_attribute *attr;
	CdbCopy    *cdbCopy;
	uint64		processed = 0;

	tupDesc = cstate->rel->rd_att;
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	cdbCopy = makeCdbCopy(cstate, false);

	/* XXX: lock all partitions */

	/*
	 * Start a COPY command in every db of every segment in Greenplum Database.
	 *
	 * From this point in the code we need to be extra careful
	 * about error handling. ereport() must not be called until
	 * the COPY command sessions are closed on the executors.
	 * Calling ereport() will leave the executors hanging in
	 * COPY state.
	 */
	elog(DEBUG5, "COPY command sent to segdbs");

	PG_TRY();
	{
		bool		done;

		cdbCopyStart(cdbCopy, stmt, cstate->file_encoding);

		if (cstate->binary)
		{
			/* Generate header for a binary copy */
			int32		tmp;

			/* Signature */
			CopySendData(cstate, (char *) BinarySignature, 11);
			/* Flags field */
			tmp = 0;
			CopySendInt32(cstate, tmp);
			/* No header extension */
			tmp = 0;
			CopySendInt32(cstate, tmp);
		}

		/* if a header has been requested send the line */
		if (cstate->header_line)
		{
			ListCell   *cur;
			bool		hdr_delim = false;

			/*
			 * For non-binary copy, we need to convert null_print to client
			 * encoding, because it will be sent directly with CopySendString.
			 *
			 * MPP: in here we only care about this if we need to print the
			 * header. We rely on the segdb server copy out to do the conversion
			 * before sending the data rows out. We don't need to repeat it here
			 */
			if (cstate->need_transcoding)
				cstate->null_print = (char *)
					pg_server_to_custom(cstate->null_print,
										strlen(cstate->null_print),
										cstate->file_encoding,
										cstate->enc_conversion_proc);

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->delim[0]);
				hdr_delim = true;

				colname = NameStr(attr[attnum - 1].attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			/* add a newline and flush the data */
			CopySendEndOfRow(cstate);
		}

		/*
		 * This is the main work-loop. In here we keep collecting data from the
		 * COPY commands on the segdbs, until no more data is available. We
		 * keep writing data out a chunk at a time.
		 */
		do
		{
			bool		copy_cancel = (QueryCancelPending ? true : false);

			/* get a chunk of data rows from the QE's */
			done = cdbCopyGetData(cdbCopy, copy_cancel, &processed);

			/* send the chunk of data rows to destination (file or stdout) */
			if (cdbCopy->copy_out_buf.len > 0) /* conditional is important! */
			{
				/*
				 * in the dispatcher we receive chunks of whole rows with row endings.
				 * We don't want to use CopySendEndOfRow() b/c it adds row endings and
				 * also b/c it's intended for a single row at a time. Therefore we need
				 * to fill in the out buffer and just flush it instead.
				 */
				CopySendData(cstate, (void *) cdbCopy->copy_out_buf.data, cdbCopy->copy_out_buf.len);
				CopyToDispatchFlush(cstate);
			}
		} while(!done);

		cdbCopyEnd(cdbCopy, NULL, NULL);

		/* now it's safe to destroy the whole dispatcher state */
		CdbDispatchCopyEnd(cdbCopy);
	}
    /* catch error from CopyStart, CopySendEndOfRow or CopyToDispatchFlush */
	PG_CATCH();
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

		cdbCopyAbort(cdbCopy);

		MemoryContextSwitchTo(oldcontext);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (cstate->binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);
		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	/* we can throw the error now if QueryCancelPending was set previously */
	CHECK_FOR_INTERRUPTS();

	pfree(cdbCopy);

	return processed;
}

static uint64
CopyToQueryOnSegment(CopyState cstate)
{
	Assert(Gp_role != GP_ROLE_EXECUTE);

	/* run the plan --- the dest receiver will send tuples */
	ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0L, true);
	return 0;
}

/*
 * Copy from relation or query TO file.
 */
static uint64
CopyTo(CopyState cstate)
{
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	ListCell   *cur;
	uint64		processed = 0;
	bool *proj = NULL;

	if (cstate->rel)
		tupDesc = RelationGetDescr(cstate->rel);
	else
		tupDesc = cstate->queryDesc->tupDesc;
	num_phys_attrs = tupDesc->natts;
	cstate->null_print_client = cstate->null_print; /* default */

	/* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
	cstate->fe_msgbuf = makeStringInfo();

	/* Get info about the columns we need to process. */
	cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));

	proj = palloc0(sizeof(bool) * num_phys_attrs);
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		proj[attnum-1] = true;
		Oid			out_func_oid;
		bool		isvarlena;
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

		if (cstate->binary)
			getTypeBinaryOutputInfo(attr->atttypid,
									&out_func_oid,
									&isvarlena);
		else
			getTypeOutputInfo(attr->atttypid,
							  &out_func_oid,
							  &isvarlena);
		fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory.  This avoids any problems with leaks inside
	 * datatype output routines, and should be faster than retail pfree's
	 * anyway.  (We don't need a whole econtext as CopyFrom does.)
	 */
	cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
											   "COPY TO",
											   ALLOCSET_DEFAULT_SIZES);
	if (!cstate->binary)
	{
		/*
		 * For non-binary copy, we need to convert null_print to file
		 * encoding, because it will be sent directly with CopySendString.
		 */
		if (cstate->need_transcoding)
			cstate->null_print_client = pg_server_to_custom(cstate->null_print,
															cstate->null_print_len,
															cstate->file_encoding,
															cstate->enc_conversion_proc);
	}

	if (Gp_role == GP_ROLE_EXECUTE && !cstate->on_segment)
	{
		/* header should not be printed in execute mode. */
	}
	else if (cstate->binary)
	{
		/* Generate header for a binary copy */
		int32		tmp;

		/* Signature */
		CopySendData(cstate, BinarySignature, 11);
		/* Flags field */
		tmp = 0;
		CopySendInt32(cstate, tmp);
		/* No header extension */
		tmp = 0;
		CopySendInt32(cstate, tmp);
	}
	else
	{
		/* if a header has been requested send the line */
		if (cstate->header_line)
		{
			bool		hdr_delim = false;

			foreach(cur, cstate->attnumlist)
			{
				int			attnum = lfirst_int(cur);
				char	   *colname;

				if (hdr_delim)
					CopySendChar(cstate, cstate->delim[0]);
				hdr_delim = true;

				colname = NameStr(TupleDescAttr(tupDesc, attnum - 1)->attname);

				CopyAttributeOutCSV(cstate, colname, false,
									list_length(cstate->attnumlist) == 1);
			}

			CopySendEndOfRow(cstate);
		}
	}

	if (cstate->rel)
	{
		List				*relids;
		ListCell			*lc;
		TupleTableSlot		*slot;
		TableScanDesc		 scandesc;
		bool foreign_partition_was_skipped = false;

		relids = lappend_oid(NIL, cstate->rel->rd_rel->oid);
		while (relids != NIL)
		{
			List *inhRelIds = NIL;
			foreach(lc, relids)
			{
				Oid relid = lfirst_oid(lc);
				Relation rel;
				if (relid == cstate->rel->rd_rel->oid)
					rel = cstate->rel;
				else
					rel = relation_open(relid, AccessShareLock);

				/*
				 * Support `COPY partitioned_table TO file` in GPDB 7 for backwards-compatibility.
				 * In PostgreSQL, you get an error:
				 *
				 * ERROR:  cannot copy from partitioned table "foo"
				 * HINT:  Try the COPY (SELECT ...) TO variant.
				 */
				if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
				{
					for (int i = 0; i < RelationRetrievePartitionDesc(rel)->nparts; i++)
						inhRelIds = lappend_oid(inhRelIds, RelationRetrievePartitionDesc(rel)->oids[i]);
				}
				else if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
				{
					if (!cstate->skip_foreign_partitions)
					{
						ereport(ERROR,
								(errcode(ERRCODE_WRONG_OBJECT_TYPE),
										errmsg("cannot copy from relation \"%s\" which has external partition(s)",
											   RelationGetRelationName(cstate->rel)),
										errhint("Try the COPY (SELECT ...) TO variant.")));
					}
					if (!foreign_partition_was_skipped)
					{
						ereport(NOTICE,
								(errmsg("COPY ignores external partition(s)")));
						foreign_partition_was_skipped = true;
					}
				}
				else
				{
					/*
					 * We need to update attnumlist because different partition
					 * entries might have dropped tables.
					 */
					if (rel != cstate->rel)
					{
						tupDesc = RelationGetDescr(rel);
						num_phys_attrs = tupDesc->natts;

						/* Get info about the columns we need to process. */
						cstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
						cstate->attnumlist = CopyGetAttnums(tupDesc, rel, cstate->attnamelist);
						proj = palloc0(sizeof(bool) * num_phys_attrs);
						foreach(cur, cstate->attnumlist)
						{
							int			attnum = lfirst_int(cur);
							proj[attnum-1] = true;
							Oid			out_func_oid;
							bool		isvarlena;
							Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

							if (cstate->binary)
								getTypeBinaryOutputInfo(attr->atttypid,
														&out_func_oid,
														&isvarlena);
							else
								getTypeOutputInfo(attr->atttypid,
												  &out_func_oid,
												  &isvarlena);
							fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
						}
					}
					/*
					 * We specifically pass NULL proj if the table has no column, and leave it
					 * to the underlying CO AM layer to handle it - the behavior should be same
					 * as SELECT * which is to choose one column to scan.
					 */
					scandesc = table_beginscan_es(rel, GetActiveSnapshot(), 0, NULL, 
													cstate->attnumlist ? proj : NULL, 
													NULL);
					slot = table_slot_create(rel, NULL);

					while (table_scan_getnextslot(scandesc, ForwardScanDirection, slot))
					{
						CHECK_FOR_INTERRUPTS();

						/* Deconstruct the tuple ... */
						slot_getallattrs(slot);

						/* Format and send the data */
						CopyOneRowTo(cstate, slot);

						/*
						* Increment the number of processed tuples, and report the
						* progress.
						*/
						pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
													 ++processed);
#ifdef FAULT_INJECTOR
						if (processed == 2)
							SIMPLE_FAULT_INJECTOR("copy_processed_two_tuples");
#endif
					}
					ExecDropSingleTupleTableSlot(slot);
					table_endscan(scandesc);
					pfree(proj);
					pfree(cstate->out_functions);
				}
				if (rel != cstate->rel)
					relation_close(rel, AccessShareLock);
			}
			list_free(relids);
			relids = inhRelIds;
		}
	}
	else
	{
		Assert(Gp_role != GP_ROLE_EXECUTE);

		/* run the plan --- the dest receiver will send tuples */
		ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0L, true);
		processed = ((DR_copy *) cstate->queryDesc->dest)->processed;
	}

	if (Gp_role == GP_ROLE_EXECUTE && !cstate->on_segment)
	{
		/*
		 * Trailer should not be printed in execute mode. The dispatcher will
		 * write it once.
		 */
	}
	else if (cstate->binary)
	{
		/* Generate trailer for a binary copy */
		CopySendInt16(cstate, -1);

		/* Need to flush out the trailer */
		CopySendEndOfRow(cstate);
	}

	if (Gp_role == GP_ROLE_EXECUTE && cstate->on_segment)
		SendNumRows(0, processed);

	MemoryContextDelete(cstate->rowcontext);

	return processed;
}

void
CopyOneCustomRowTo(CopyState cstate, bytea *value)
{
	appendBinaryStringInfo(cstate->fe_msgbuf,
						   VARDATA_ANY((void *) value),
						   VARSIZE_ANY_EXHDR((void *) value));
}

/*
 * Emit one row during CopyTo().
 */
void
CopyOneRowTo(CopyState cstate, TupleTableSlot *slot)
{
	bool		need_delim = false;
	FmgrInfo   *out_functions = cstate->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;
	char	   *string;

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

	if (cstate->binary)
	{
		/* Binary per-tuple header */
		CopySendInt16(cstate, list_length(cstate->attnumlist));
	}

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = slot->tts_values[attnum - 1];
		bool		isnull = slot->tts_isnull[attnum - 1];

		if (!cstate->binary)
		{
			if (need_delim)
				CopySendChar(cstate, cstate->delim[0]);
			need_delim = true;
		}

		if (isnull)
		{
			if (!cstate->binary)
				CopySendString(cstate, cstate->null_print_client);
			else
				CopySendInt32(cstate, -1);
		}
		else
		{
			if (!cstate->binary)
			{
				char		quotec = cstate->quote ? cstate->quote[0] : '\0';

				/* int2out or int4out ? */
				if (out_functions[attnum -1].fn_oid == 39 ||  /* int2out or int4out */
					out_functions[attnum -1].fn_oid == 43 )
				{
					char tmp[33];
					/*
					 * The standard postgres way is to call the output function, but that involves one or more pallocs,
					 * and a call to sprintf, followed by a conversion to client charset.
					 * Do a fast conversion to string instead.
					 */

					if (out_functions[attnum -1].fn_oid ==  39)
						pg_itoa(DatumGetInt16(value),tmp);
					else
						pg_ltoa(DatumGetInt32(value),tmp);

					/*
					 * Integers don't need quoting, or transcoding to client char
					 * set. We still quote them if FORCE QUOTE was used, though.
					 */
					if (cstate->force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
					CopySendData(cstate, tmp, strlen(tmp));
					if (cstate->force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
				}
				else if (out_functions[attnum -1].fn_oid == 1702)   /* numeric_out */
				{
					string = OutputFunctionCall(&out_functions[attnum - 1],
												value);
					/*
					 * Numerics don't need quoting, or transcoding to client char
					 * set. We still quote them if FORCE QUOTE was used, though.
					 */
					if (cstate->force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
					CopySendData(cstate, string, strlen(string));
					if (cstate->force_quote_flags[attnum - 1])
						CopySendChar(cstate, quotec);
				}
				else
				{
					string = OutputFunctionCall(&out_functions[attnum - 1],
												value);
					if (cstate->csv_mode)
						CopyAttributeOutCSV(cstate, string,
											cstate->force_quote_flags[attnum - 1],
											list_length(cstate->attnumlist) == 1);
					else
						CopyAttributeOutText(cstate, string);
				}
			}
			else
			{
				bytea	   *outputbytes;

				outputbytes = SendFunctionCall(&out_functions[attnum - 1],
											   value);
				CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
				CopySendData(cstate, VARDATA(outputbytes),
							 VARSIZE(outputbytes) - VARHDRSZ);
			}
		}
	}

	/*
	 * Finish off the row: write it to the destination, and update the count.
	 * However, if we're in the context of a writable external table, we let
	 * the caller do it - send the data to its local external source (see
	 * external_insert() ).
	 */
	if (cstate->copy_dest != COPY_CALLBACK)
	{
		CopySendEndOfRow(cstate);
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyState.
 */
void
CopyFromErrorCallback(void *arg)
{
	CopyState	cstate = (CopyState) arg;
	char		curlineno_str[32];

	snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
			 cstate->cur_lineno);

	if (cstate->binary)
	{
		/* can't usefully display the data */
		if (cstate->cur_attname)
			errcontext("COPY %s, line %s, column %s",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		else
			errcontext("COPY %s, line %s",
					   cstate->cur_relname, curlineno_str);
	}
	else
	{
		if (cstate->cur_attname && cstate->cur_attval)
		{
			/* error is relevant to a particular column */
			char	   *attval;

			attval = limit_printout_length(cstate->cur_attval);
			errcontext("COPY %s, line %s, column %s: \"%s\"",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname, attval);
			pfree(attval);
		}
		else if (cstate->cur_attname)
		{
			/* error is relevant to a particular column, value is NULL */
			errcontext("COPY %s, line %s, column %s: null input",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		}
		else
		{
			/*
			 * Error is relevant to a particular line.
			 *
			 * If line_buf still contains the correct line, and it's already
			 * transcoded, print it. If it's still in a foreign encoding, it's
			 * quite likely that the error is precisely a failure to do
			 * encoding conversion (ie, bad data). We dare not try to convert
			 * it, and at present there's no way to regurgitate it without
			 * conversion. So we have to punt and just report the line number.
			 */
			if (cstate->line_buf_valid &&
				(cstate->line_buf_converted || !cstate->need_transcoding))
			{
				char	   *lineval;

				lineval = limit_printout_length(cstate->line_buf.data);
				errcontext("COPY %s, line %s: \"%s\"",
						   cstate->cur_relname, curlineno_str, lineval);
				pfree(lineval);
			}
			else
			{
				/*
				 * Here, the line buffer is still in a foreign encoding,
				 * and indeed it's quite likely that the error is precisely
				 * a failure to do encoding conversion (ie, bad data).	We
				 * dare not try to convert it, and at present there's no way
				 * to regurgitate it without conversion.  So we have to punt
				 * and just report the line number.
				 */
				errcontext("COPY %s, line %s",
						   cstate->cur_relname, curlineno_str);
			}
		}
	}
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * It would seem a lot easier to just use the sprintf "precision" limit to
 * truncate the string.  However, some versions of glibc have a bug/misfeature
 * that vsnprintf will always fail (return -1) if it is asked to truncate
 * a string that contains invalid byte sequences for the current encoding.
 * So, do our own truncation.  We return a pstrdup'd copy of the input.
 */
char *
limit_printout_length(const char *str)
{
#define MAX_COPY_DATA_DISPLAY 100

	int			slen = strlen(str);
	int			len;
	char	   *res;

	/* Fast path if definitely okay */
	if (slen <= MAX_COPY_DATA_DISPLAY)
		return pstrdup(str);

	/* Apply encoding-dependent truncation */
	len = pg_mbcliplen(str, slen, MAX_COPY_DATA_DISPLAY);

	/*
	 * Truncate, and add "..." to show we truncated the input.
	 */
	res = (char *) palloc(len + 4);
	memcpy(res, str, len);
	strcpy(res + len, "...");

	return res;
}

/*
 * Allocate memory and initialize a new CopyMultiInsertBuffer for this
 * ResultRelInfo.
 */
static CopyMultiInsertBuffer *
CopyMultiInsertBufferInit(ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = (CopyMultiInsertBuffer *) palloc(sizeof(CopyMultiInsertBuffer));
	memset(buffer->slots, 0, sizeof(TupleTableSlot *) * MAX_BUFFERED_TUPLES);
	buffer->resultRelInfo = rri;
	buffer->bistate = GetBulkInsertState();
	buffer->nused = 0;

	return buffer;
}

/*
 * Make a new buffer for this ResultRelInfo.
 */
static inline void
CopyMultiInsertInfoSetupBuffer(CopyMultiInsertInfo *miinfo,
							   ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = CopyMultiInsertBufferInit(rri);

	/* Setup back-link so we can easily find this buffer again */
	rri->ri_CopyMultiInsertBuffer = buffer;
	/* Record that we're tracking this buffer */
	miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
}

/*
 * Initialize an already allocated CopyMultiInsertInfo.
 *
 * If rri is a non-partitioned table then a CopyMultiInsertBuffer is set up
 * for that table.
 */
static void
CopyMultiInsertInfoInit(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						CopyState cstate, EState *estate, CommandId mycid,
						int ti_options)
{
	miinfo->multiInsertBuffers = NIL;
	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;
	miinfo->cstate = cstate;
	miinfo->estate = estate;
	miinfo->mycid = mycid;
	miinfo->ti_options = ti_options;

	/*
	 * Only setup the buffer when not dealing with a partitioned table.
	 * Buffers for partitioned tables will just be setup when we need to send
	 * tuples their way for the first time.
	 */
	if (rri->ri_RelationDesc->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		CopyMultiInsertInfoSetupBuffer(miinfo, rri);
}

/*
 * Returns true if the buffers are full
 */
static inline bool
CopyMultiInsertInfoIsFull(CopyMultiInsertInfo *miinfo)
{
	if (miinfo->bufferedTuples >= MAX_BUFFERED_TUPLES ||
		miinfo->bufferedBytes >= MAX_BUFFERED_BYTES)
		return true;
	return false;
}

/*
 * Returns true if we have no buffered tuples
 */
static inline bool
CopyMultiInsertInfoIsEmpty(CopyMultiInsertInfo *miinfo)
{
	return miinfo->bufferedTuples == 0;
}

/*
 * Write the tuples stored in 'buffer' out to the table.
 */
static inline void
CopyMultiInsertBufferFlush(CopyMultiInsertInfo *miinfo,
						   CopyMultiInsertBuffer *buffer)
{
	MemoryContext oldcontext;
	int			i;
	uint64		save_cur_lineno;
	CopyState	cstate = miinfo->cstate;
	EState	   *estate = miinfo->estate;
	CommandId	mycid = miinfo->mycid;
	int			ti_options = miinfo->ti_options;
	bool		line_buf_valid = cstate->line_buf_valid;
	int			nused = buffer->nused;
	ResultRelInfo *resultRelInfo = buffer->resultRelInfo;
	TupleTableSlot **slots = buffer->slots;

	/* Set es_result_relation_info to the ResultRelInfo we're flushing. */
	estate->es_result_relation_info = resultRelInfo;

	/*
	 * Print error context information correctly, if one of the operations
	 * below fails.
	 */
	cstate->line_buf_valid = false;
	save_cur_lineno = cstate->cur_lineno;

	/*
	 * table_multi_insert may leak memory, so switch to short-lived memory
	 * context before calling it.
	 */
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	table_multi_insert(resultRelInfo->ri_RelationDesc,
					   slots,
					   nused,
					   mycid,
					   ti_options,
					   buffer->bistate);
	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < nused; i++)
	{
		/*
		 * If there are any indexes, update them for all the inserted tuples,
		 * and run AFTER ROW INSERT triggers.
		 */
		if (resultRelInfo->ri_NumIndices > 0)
		{
			List	   *recheckIndexes;

			cstate->cur_lineno = buffer->linenos[i];
			recheckIndexes =
				ExecInsertIndexTuples(buffer->slots[i], estate, false, NULL,
									  NIL);
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], recheckIndexes,
								 cstate->transition_capture);
			list_free(recheckIndexes);
		}

		/*
		 * There's no indexes, but see if we need to run AFTER ROW INSERT
		 * triggers anyway.
		 */
		else if (resultRelInfo->ri_TrigDesc != NULL &&
				 (resultRelInfo->ri_TrigDesc->trig_insert_after_row ||
				  resultRelInfo->ri_TrigDesc->trig_insert_new_table))
		{
			cstate->cur_lineno = buffer->linenos[i];
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], NIL, cstate->transition_capture);
		}

		ExecClearTuple(slots[i]);
	}

	/* Mark that all slots are free */
	buffer->nused = 0;

	/* reset cur_lineno and line_buf_valid to what they were */
	cstate->line_buf_valid = line_buf_valid;
	cstate->cur_lineno = save_cur_lineno;
}

/*
 * Drop used slots and free member for this buffer.
 *
 * The buffer must be flushed before cleanup.
 */
static inline void
CopyMultiInsertBufferCleanup(CopyMultiInsertInfo *miinfo,
							 CopyMultiInsertBuffer *buffer)
{
	int			i;

	/* Ensure buffer was flushed */
	Assert(buffer->nused == 0);

	/* Remove back-link to ourself */
	buffer->resultRelInfo->ri_CopyMultiInsertBuffer = NULL;

	FreeBulkInsertState(buffer->bistate);

	/* Since we only create slots on demand, just drop the non-null ones. */
	for (i = 0; i < MAX_BUFFERED_TUPLES && buffer->slots[i] != NULL; i++)
		ExecDropSingleTupleTableSlot(buffer->slots[i]);

	table_finish_bulk_insert(buffer->resultRelInfo->ri_RelationDesc,
							 miinfo->ti_options);

	pfree(buffer);
}

/*
 * Write out all stored tuples in all buffers out to the tables.
 *
 * Once flushed we also trim the tracked buffers list down to size by removing
 * the buffers created earliest first.
 *
 * Callers should pass 'curr_rri' is the ResultRelInfo that's currently being
 * used.  When cleaning up old buffers we'll never remove the one for
 * 'curr_rri'.
 */
static inline void
CopyMultiInsertInfoFlush(CopyMultiInsertInfo *miinfo, ResultRelInfo *curr_rri)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
	{
		CopyMultiInsertBuffer *buffer = (CopyMultiInsertBuffer *) lfirst(lc);

		CopyMultiInsertBufferFlush(miinfo, buffer);
	}

	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;

	/*
	 * Trim the list of tracked buffers down if it exceeds the limit.  Here we
	 * remove buffers starting with the ones we created first.  It seems more
	 * likely that these older ones are less likely to be needed than ones
	 * that were just created.
	 */
	while (list_length(miinfo->multiInsertBuffers) > MAX_PARTITION_BUFFERS)
	{
		CopyMultiInsertBuffer *buffer;

		buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);

		/*
		 * We never want to remove the buffer that's currently being used, so
		 * if we happen to find that then move it to the end of the list.
		 */
		if (buffer->resultRelInfo == curr_rri)
		{
			miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
			miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
			buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);
		}

		CopyMultiInsertBufferCleanup(miinfo, buffer);
		miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
	}
}

/*
 * Cleanup allocated buffers and free memory
 */
static inline void
CopyMultiInsertInfoCleanup(CopyMultiInsertInfo *miinfo)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
		CopyMultiInsertBufferCleanup(miinfo, lfirst(lc));

	list_free(miinfo->multiInsertBuffers);
}

/*
 * Get the next TupleTableSlot that the next tuple should be stored in.
 *
 * Callers must ensure that the buffer is not full.
 */
static inline TupleTableSlot *
CopyMultiInsertInfoNextFreeSlot(CopyMultiInsertInfo *miinfo,
								ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;
	int			nused = buffer->nused;

	Assert(buffer != NULL);
	Assert(nused < MAX_BUFFERED_TUPLES);

	if (buffer->slots[nused] == NULL)
		buffer->slots[nused] = table_slot_create(rri->ri_RelationDesc, NULL);
	return buffer->slots[nused];
}

/*
 * Record the previously reserved TupleTableSlot that was reserved by
 * CopyMultiInsertInfoNextFreeSlot as being consumed.
 */
static inline void
CopyMultiInsertInfoStore(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						 TupleTableSlot *slot, int tuplen, uint64 lineno)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;

	Assert(buffer != NULL);
	Assert(slot == buffer->slots[buffer->nused]);

	/* Store the line number so we can properly report any errors later */
	buffer->linenos[buffer->nused] = lineno;

	/* Record this slot as being used */
	buffer->nused++;

	/* Update how many tuples are stored and their size */
	miinfo->bufferedTuples++;
	miinfo->bufferedBytes += tuplen;
}

/*
 * Copy FROM file to relation.
 */
uint64
CopyFrom(CopyState cstate)
{
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *target_resultRelInfo;
	ResultRelInfo *prevResultRelInfo = NULL;
	EState	   *estate = CreateExecutorState(); /* for ExecConstraints() */
	ModifyTableState *mtstate;
	ExprContext *econtext;
	TupleTableSlot *singleslot = NULL;
	MemoryContext oldcontext = CurrentMemoryContext;

	PartitionTupleRouting *proute = NULL;
	ErrorContextCallback errcallback;
	CommandId	mycid = GetCurrentCommandId(true);
	int			ti_options = 0; /* start with default options for insert */
	BulkInsertState bistate = NULL;
	CopyInsertMethod insertMethod;
	CopyMultiInsertInfo multiInsertInfo = {0};	/* pacify compiler */
	int64		processed = 0;
	int64		excluded = 0;
	bool		has_before_insert_row_trig;
	bool		has_instead_insert_row_trig;
	bool		leafpart_use_multi_insert = false;

	CdbCopy	   *cdbCopy = NULL;
	bool		is_check_distkey;
	GpDistributionData *distData = NULL; /* distribution data used to compute target seg */

	Assert(cstate->rel);

	/*
	 * The target must be a plain, foreign, or partitioned relation, or have
	 * an INSTEAD OF INSERT row trigger.  (Currently, such triggers are only
	 * allowed on views, so we only hint about them in the view case.)
	 */
	if (cstate->rel->rd_rel->relkind != RELKIND_RELATION &&
		cstate->rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
		cstate->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		!(cstate->rel->trigdesc &&
		  cstate->rel->trigdesc->trig_insert_instead_row))
	{
		if (cstate->rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"",
							RelationGetRelationName(cstate->rel)),
					 errhint("To enable copying to a view, provide an INSTEAD OF INSERT trigger.")));
		else if (cstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(cstate->rel))));
	}

	/*----------
	 * Check to see if we can avoid writing WAL
	 *
	 * If archive logging/streaming is not enabled *and* either
	 *	- table was created in same transaction as this COPY
	 *	- data is being written to relfilenode created in this transaction
	 * then we can skip writing WAL.  It's safe because if the transaction
	 * doesn't commit, we'll discard the table (or the new relfilenode file).
	 * If it does commit, we'll have done the table_finish_bulk_insert() at
	 * the bottom of this routine first.
	 *
	 * As mentioned in comments in utils/rel.h, the in-same-transaction test
	 * is not always set correctly, since in rare cases rd_newRelfilenodeSubid
	 * can be cleared before the end of the transaction. The exact case is
	 * when a relation sets a new relfilenode twice in same transaction, yet
	 * the second one fails in an aborted subtransaction, e.g.
	 *
	 * BEGIN;
	 * TRUNCATE t;
	 * SAVEPOINT save;
	 * TRUNCATE t;
	 * ROLLBACK TO save;
	 * COPY ...
	 *
	 * Also, if the target file is new-in-transaction, we assume that checking
	 * FSM for free space is a waste of time, even if we must use WAL because
	 * of archiving.  This could possibly be wrong, but it's unlikely.
	 *
	 * The comments for table_tuple_insert and RelationGetBufferForTuple
	 * specify that skipping WAL logging is only safe if we ensure that our
	 * tuples do not go into pages containing tuples from any other
	 * transactions --- but this must be the case if we have a new table or
	 * new relfilenode, so we need no additional work to enforce that.
	 *
	 * We currently don't support this optimization if the COPY target is a
	 * partitioned table as we currently only lazily initialize partition
	 * information when routing the first tuple to the partition.  We cannot
	 * know at this stage if we can perform this optimization.  It should be
	 * possible to improve on this, but it does mean maintaining heap insert
	 * option flags per partition and setting them when we first open the
	 * partition.
	 *
	 * This optimization is not supported for relation types which do not
	 * have any physical storage, with foreign tables and views using
	 * INSTEAD OF triggers entering in this category.  Partitioned tables
	 * are not supported as per the description above.
	 *----------
	 */
	/* createSubid is creation check, newRelfilenodeSubid is truncation check */
	if (RELKIND_HAS_STORAGE(cstate->rel->rd_rel->relkind) &&
		(cstate->rel->rd_createSubid != InvalidSubTransactionId ||
		 cstate->rel->rd_newRelfilenodeSubid != InvalidSubTransactionId))
	{
		ti_options |= TABLE_INSERT_SKIP_FSM;
		/*
		 * The optimization to skip WAL has been disabled in GPDB. wal_level
		 * is hardcoded to 'archive' in GPDB, so it wouldn't have any effect
		 * anyway.
		 */
#if 0
		if (!XLogIsNeeded())
			ti_options |= TABLE_INSERT_SKIP_WAL;
#endif
	}

	/*
	 * Optimize if new relfilenode was created in this subxact or one of its
	 * committed children and we won't see those rows later as part of an
	 * earlier scan or command. The subxact test ensures that if this subxact
	 * aborts then the frozen rows won't be visible after xact cleanup.  Note
	 * that the stronger test of exactly which subtransaction created it is
	 * crucial for correctness of this optimization. The test for an earlier
	 * scan or command tolerates false negatives. FREEZE causes other sessions
	 * to see rows they would not see under MVCC, and a false negative merely
	 * spreads that anomaly to the current session.
	 */
	if (cstate->freeze)
	{
		/*
		 * We currently disallow COPY FREEZE on partitioned tables.  The
		 * reason for this is that we've simply not yet opened the partitions
		 * to determine if the optimization can be applied to them.  We could
		 * go and open them all here, but doing so may be quite a costly
		 * overhead for small copies.  In any case, we may just end up routing
		 * tuples to a small number of partitions.  It seems better just to
		 * raise an ERROR for partitioned tables.
		 */
		if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot perform COPY FREEZE on a partitioned table")));
		}

		/*
		 * Tolerate one registration for the benefit of FirstXactSnapshot.
		 * Scan-bearing queries generally create at least two registrations,
		 * though relying on that is fragile, as is ignoring ActiveSnapshot.
		 * Clear CatalogSnapshot to avoid counting its registration.  We'll
		 * still detect ongoing catalog scans, each of which separately
		 * registers the snapshot it uses.
		 */
		InvalidateCatalogSnapshot();
		if (!ThereAreNoPriorRegisteredSnapshots() || !ThereAreNoReadyPortals())
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("cannot perform COPY FREEZE because of prior transaction activity")));

		if (cstate->rel->rd_createSubid != GetCurrentSubTransactionId() &&
			cstate->rel->rd_newRelfilenodeSubid != GetCurrentSubTransactionId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot perform COPY FREEZE because the table was not created or truncated in the current subtransaction")));

		ti_options |= TABLE_INSERT_FROZEN;
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo,
					  cstate->rel,
					  1,		/* must match rel's position in range_table */
					  NULL,
					  0);

	target_resultRelInfo = resultRelInfo;

	/* Verify the named relation is a valid target for INSERT */
	CheckValidResultRel(resultRelInfo, CMD_INSERT);

	ExecOpenIndices(resultRelInfo, false);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	ExecInitRangeTable(estate, cstate->range_table);

	/*
	 * Set up a ModifyTableState so we can let FDW(s) init themselves for
	 * foreign-table result relation(s).
	 */
	mtstate = makeNode(ModifyTableState);
	mtstate->ps.plan = NULL;
	mtstate->ps.state = estate;
	mtstate->operation = CMD_INSERT;
	mtstate->resultRelInfo = estate->es_result_relations;
	mtstate->rootResultRelInfo = estate->es_result_relations;

	if (resultRelInfo->ri_FdwRoutine != NULL &&
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert != NULL)
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert(mtstate,
														 resultRelInfo);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * If there are any triggers with transition tables on the named relation,
	 * we need to be prepared to capture transition tuples.
	 *
	 * Because partition tuple routing would like to know about whether
	 * transition capture is active, we also set it in mtstate, which is
	 * passed to ExecFindPartition() below.
	 */
	cstate->transition_capture = mtstate->mt_transition_capture =
		MakeTransitionCaptureState(cstate->rel->trigdesc,
								   RelationGetRelid(cstate->rel),
								   CMD_INSERT);

	/*
	 * If the named relation is a partitioned table, initialize state for
	 * CopyFrom tuple routing.
	 */
	if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		proute = ExecSetupPartitionTupleRouting(estate, NULL, cstate->rel);

	if (cstate->whereClause)
		cstate->qualexpr = ExecInitQual(castNode(List, cstate->whereClause),
										&mtstate->ps);

	/*
	 * It's generally more efficient to prepare a bunch of tuples for
	 * insertion, and insert them in one table_multi_insert() call, than call
	 * table_tuple_insert() separately for every tuple. However, there are a
	 * number of reasons why we might not be able to do this.  These are
	 * explained below.
	 */
	if (resultRelInfo->ri_TrigDesc != NULL &&
		(resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
		 resultRelInfo->ri_TrigDesc->trig_insert_instead_row))
	{
		/*
		 * Can't support multi-inserts when there are any BEFORE/INSTEAD OF
		 * triggers on the table. Such triggers might query the table we're
		 * inserting into and act differently if the tuples that have already
		 * been processed and prepared for insertion are not there.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (proute != NULL && resultRelInfo->ri_TrigDesc != NULL &&
			 resultRelInfo->ri_TrigDesc->trig_insert_new_table)
	{
		/*
		 * For partitioned tables we can't support multi-inserts when there
		 * are any statement level insert triggers. It might be possible to
		 * allow partitioned tables with such triggers in the future, but for
		 * now, CopyMultiInsertInfoFlush expects that any before row insert
		 * and statement level insert triggers are on the same relation.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (resultRelInfo->ri_FdwRoutine != NULL ||
			 cstate->volatile_defexprs)
	{
		/*
		 * Can't support multi-inserts to foreign tables or if there are any
		 * volatile default expressions in the table.  Similarly to the
		 * trigger case above, such expressions may query the table we're
		 * inserting into.
		 *
		 * Note: It does not matter if any partitions have any volatile
		 * default expressions as we use the defaults from the target of the
		 * COPY command.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (contain_volatile_functions(cstate->whereClause))
	{
		/*
		 * Can't support multi-inserts if there are any volatile function
		 * expressions in WHERE clause.  Similarly to the trigger case above,
		 * such expressions may query the table we're inserting into.
		 */
		insertMethod = CIM_SINGLE;
	}
	else
	{
		/*
		 * For partitioned tables, we may still be able to perform bulk
		 * inserts.  However, the possibility of this depends on which types
		 * of triggers exist on the partition.  We must disable bulk inserts
		 * if the partition is a foreign table or it has any before row insert
		 * or insert instead triggers (same as we checked above for the parent
		 * table).  Since the partition's resultRelInfos are initialized only
		 * when we actually need to insert the first tuple into them, we must
		 * have the intermediate insert method of CIM_MULTI_CONDITIONAL to
		 * flag that we must later determine if we can use bulk-inserts for
		 * the partition being inserted into.
		 */
		if (proute)
			insertMethod = CIM_MULTI_CONDITIONAL;
		else
			insertMethod = CIM_MULTI;

		CopyMultiInsertInfoInit(&multiInsertInfo, resultRelInfo, cstate,
								estate, mycid, ti_options);
	}

	/*
	 * If not using batch mode (which allocates slots as needed) set up a
	 * tuple slot too. When inserting into a partitioned table, we also need
	 * one, even if we might batch insert, to read the tuple in the root
	 * partition's form.
	 */
	if (insertMethod == CIM_SINGLE || insertMethod == CIM_MULTI_CONDITIONAL)
	{
		singleslot = table_slot_create(resultRelInfo->ri_RelationDesc,
									   &estate->es_tupleTable);
		bistate = GetBulkInsertState();
	}

	has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

	has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	econtext = GetPerTupleExprContext(estate);

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * Do we need to check the distribution keys? Normally, the QD computes the
	 * target segment and sends the data to the correct segment. We don't need to
	 * verify that in the QE anymore. But in ON SEGMENT, we're reading data
	 * directly from a file, and there's no guarantee on what it contains, so we
	 * need to do the checking in the QE.
	 */
	is_check_distkey = (cstate->on_segment && Gp_role == GP_ROLE_EXECUTE && gp_enable_segment_copy_checking) ? true : false;

	/*
	 * Initialize information about distribution keys, needed to compute target
	 * segment for each row.
	 */
	if (cstate->dispatch_mode == COPY_DISPATCH || is_check_distkey)
	{
		distData = InitDistributionData(cstate, estate);

		/*
		 * If this table is distributed randomly, there is nothing to check,
		 * after all.
		 */
		if (distData->policy == NULL || distData->policy->nattrs == 0)
			is_check_distkey = false;
	}

	/* Determine which fields we need to parse in the QD. */
	if (cstate->dispatch_mode == COPY_DISPATCH)
		InitCopyFromDispatchSplit(cstate, distData, estate);

	if (cstate->dispatch_mode == COPY_DISPATCH ||
		cstate->dispatch_mode == COPY_EXECUTOR)
	{
		/*
		 * Now split the attnumlist into the parts that are parsed in the QD, and
		 * in QE.
		 */
		ListCell   *lc;
		int			i = 0;
		List	   *qd_attnumlist = NIL;
		List	   *qe_attnumlist = NIL;
		int			first_qe_processed_field;

		first_qe_processed_field = cstate->first_qe_processed_field;

		foreach(lc, cstate->attnumlist)
		{
			int			attnum = lfirst_int(lc);

			if (i < first_qe_processed_field)
				qd_attnumlist = lappend_int(qd_attnumlist, attnum);
			else
				qe_attnumlist = lappend_int(qe_attnumlist, attnum);
			i++;
		}
		cstate->qd_attnumlist = qd_attnumlist;
		cstate->qe_attnumlist = qe_attnumlist;
	}

	if (cstate->dispatch_mode == COPY_DISPATCH)
	{
		/*
		 * We are the QD node, and we are receiving rows from client, or
		 * reading them from a file. We are not writing any data locally,
		 * instead, we determine the correct target segment for row,
		 * and forward each to the correct segment.
		 */

		/*
		 * pre-allocate buffer for constructing a message.
		 */
		cstate->dispatch_msgbuf = makeStringInfo();
		enlargeStringInfo(cstate->dispatch_msgbuf, SizeOfCopyFromDispatchRow);

		/*
		 * prepare to COPY data into segDBs:
		 * - set table partitioning information
		 * - set append only table relevant info for dispatch.
		 * - get the distribution policy for this table.
		 * - build a COPY command to dispatch to segdbs.
		 * - dispatch the modified COPY command to all segment databases.
		 * - prepare cdbhash for hashing on row values.
		 */
		cdbCopy = makeCdbCopy(cstate, true);

		/*
		 * Dispatch the COPY command.
		 *
		 * From this point in the code we need to be extra careful about error
		 * handling. ereport() must not be called until the COPY command sessions
		 * are closed on the executors. Calling ereport() will leave the executors
		 * hanging in COPY state.
		 *
		 * For errors detected by the dispatcher, we save the error message in
		 * cdbcopy_err StringInfo, move on to closing all COPY sessions on the
		 * executors and only then raise an error. We need to make sure to TRY/CATCH
		 * all other errors that may be raised from elsewhere in the backend. All
		 * error during COPY on the executors will be detected only when we end the
		 * COPY session there, so we are fine there.
		 */
		elog(DEBUG5, "COPY command sent to segdbs");

		cdbCopyStart(cdbCopy, glob_copystmt, cstate->file_encoding);

		/*
		 * Skip header processing if dummy file get from coordinator for COPY FROM ON
		 * SEGMENT
		 */
		if (!cstate->on_segment)
		{
			SendCopyFromForwardedHeader(cstate, cdbCopy);
		}
	}

	CopyInitDataParser(cstate);

	if (resultRelInfo->ri_RelationDesc->rd_tableam)
		table_dml_init(resultRelInfo->ri_RelationDesc);

	for (;;)
	{
		TupleTableSlot *myslot;
		bool		skip_tuple;
		unsigned int target_seg = 0;	/* result segment of cdbhash */

		CHECK_FOR_INTERRUPTS();

		/*
		 * Reset the per-tuple exprcontext. We do this after every tuple, to
		 * clean-up after expression evaluations etc.
		 */
		ResetPerTupleExprContext(estate);

		/* select slot to (initially) load row into */
		if (insertMethod == CIM_SINGLE || proute)
		{
			myslot = singleslot;
			Assert(myslot != NULL);
		}
		else
		{
			Assert(resultRelInfo == target_resultRelInfo);
			Assert(insertMethod == CIM_MULTI);

			myslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
													 resultRelInfo);
		}

		/*
		 * Switch to per-tuple context before calling NextCopyFrom, which does
		 * evaluate default expressions etc. and requires per-tuple context.
		 */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		ExecClearTuple(myslot);

		if (cstate->dispatch_mode == COPY_EXECUTOR)
		{
			if (!NextCopyFromExecute(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
				break;

			/*
			 * NextCopyFromExecute set up estate->es_result_relation_info,
			 * and stored the tuple in the correct slot.
			 */
			resultRelInfo = estate->es_result_relation_info;
		}
		else
		{
			/* Directly store the values/nulls array in the slot */
			if (cstate->dispatch_mode == COPY_DISPATCH)
			{
				if (!NextCopyFromDispatch(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
					break;
			}
			else
			{
				if (!NextCopyFrom(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
					break;
			}
		}
		ExecStoreVirtualTuple(myslot);

		/*
		 * Constraints and where clause might reference the tableoid column,
		 * so (re-)initialize tts_tableOid before evaluating them.
		 */
		myslot->tts_tableOid = RelationGetRelid(target_resultRelInfo->ri_RelationDesc);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		if (cstate->whereClause)
		{
			econtext->ecxt_scantuple = myslot;
			/* Skip items that don't match COPY's WHERE clause */
			if (!ExecQual(cstate->qualexpr, econtext))
			{
				/*
				 * Report that this tuple was filtered out by the WHERE
				 * clause.
				 */
				pgstat_progress_update_param(PROGRESS_COPY_TUPLES_EXCLUDED,
											 ++excluded);
				continue;
			}
		}

		if (cstate->dispatch_mode != COPY_DISPATCH && is_check_distkey)
		{
			/*
			 * In COPY FROM ON SEGMENT, check the distribution key in the
			 * QE.
			 * Note: For partitioned tables, the order of the root table's columns can be
			 * inconsistent with the order of the partition's columns and Greenplum/PostgreSQL
			 * allows such behavior. When they have different orders, we need to re-order the
			 * TupleTableSlot (myslot) to make it match the partition's columns (see execute_attr_map_slot()
			 * for details). We must perform this check before the re-ordering of TupleTableslot,
			 * or the value of target_seg will be incorrect.
			 */
			if (distData->policy->nattrs != 0)
			{
				target_seg = GetTargetSeg(distData, myslot);
				if (GpIdentity.segindex != target_seg)
				{
					PG_TRY();
					{
						ereport(ERROR,
								(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
								 errmsg("value of distribution key doesn't belong to segment with ID %d, it belongs to segment with ID %d",
										GpIdentity.segindex, target_seg)));
					}
					PG_CATCH();
					{
						HandleCopyError(cstate);
					}
					PG_END_TRY();
				}
			}
		}

		/* Determine the partition to insert the tuple into */
		if (proute && cstate->dispatch_mode != COPY_DISPATCH)
		{
			TupleConversionMap *map;
			bool		got_error = false;

			/*
			 * Attempt to find a partition suitable for this tuple.
			 * ExecFindPartition() will raise an error if none can be found or
			 * if the found partition is not suitable for INSERTs.
			 */
			PG_TRY();
			{
				resultRelInfo = ExecFindPartition(mtstate, target_resultRelInfo,
												  proute, myslot, estate);
			}
			PG_CATCH();
			{
				/* after all the prep work let cdbsreh do the real work */
				HandleCopyError(cstate);
				got_error = true;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_END_TRY();

			if (got_error)
				continue;

			if (prevResultRelInfo != resultRelInfo)
			{
				/* Determine which triggers exist on this partition */
				has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

				has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

				/*
				 * Disable multi-inserts when the partition has BEFORE/INSTEAD
				 * OF triggers, or if the partition is a foreign partition.
				 */
				leafpart_use_multi_insert = insertMethod == CIM_MULTI_CONDITIONAL &&
					!has_before_insert_row_trig &&
					!has_instead_insert_row_trig &&
					resultRelInfo->ri_FdwRoutine == NULL;

				/* Set the multi-insert buffer to use for this partition. */
				if (leafpart_use_multi_insert)
				{
					if (resultRelInfo->ri_CopyMultiInsertBuffer == NULL)
						CopyMultiInsertInfoSetupBuffer(&multiInsertInfo,
													   resultRelInfo);
				}
				else if (insertMethod == CIM_MULTI_CONDITIONAL &&
						 !CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
				{
					/*
					 * Flush pending inserts if this partition can't use
					 * batching, so rows are visible to triggers etc.
					 */
					CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}

				if (bistate != NULL)
					ReleaseBulkInsertStatePin(bistate);
				prevResultRelInfo = resultRelInfo;
			}

			/*
			 * For ExecInsertIndexTuples() to work on the partition's indexes
			 */
			estate->es_result_relation_info = resultRelInfo;

			/*
			 * If we're capturing transition tuples, we might need to convert
			 * from the partition rowtype to root rowtype.
			 */
			if (cstate->transition_capture != NULL)
			{
				if (has_before_insert_row_trig)
				{
					/*
					 * If there are any BEFORE triggers on the partition,
					 * we'll have to be ready to convert their result back to
					 * tuplestore format.
					 */
					cstate->transition_capture->tcs_original_insert_tuple = NULL;
					cstate->transition_capture->tcs_map =
						resultRelInfo->ri_PartitionInfo->pi_PartitionToRootMap;
				}
				else
				{
					/*
					 * Otherwise, just remember the original unconverted
					 * tuple, to avoid a needless round trip conversion.
					 */
					cstate->transition_capture->tcs_original_insert_tuple = myslot;
					cstate->transition_capture->tcs_map = NULL;
				}
			}

			/*
			 * We might need to convert from the root rowtype to the partition
			 * rowtype.
			 */
			map = resultRelInfo->ri_PartitionInfo->pi_RootToPartitionMap;
			if (insertMethod == CIM_SINGLE || !leafpart_use_multi_insert)
			{
				/* non batch insert */
				if (map != NULL)
				{
					TupleTableSlot *new_slot;

					new_slot = resultRelInfo->ri_PartitionInfo->pi_PartitionTupleSlot;
					myslot = execute_attr_map_slot(map->attrMap, myslot, new_slot);
				}
			}
			else
			{
				/*
				 * Prepare to queue up tuple for later batch insert into
				 * current partition.
				 */
				TupleTableSlot *batchslot;

				/* no other path available for partitioned table */
				Assert(insertMethod == CIM_MULTI_CONDITIONAL);

				batchslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
															resultRelInfo);

				if (map != NULL)
					myslot = execute_attr_map_slot(map->attrMap, myslot,
												   batchslot);
				else
				{
					/*
					 * This looks more expensive than it is (Believe me, I
					 * optimized it away. Twice.). The input is in virtual
					 * form, and we'll materialize the slot below - for most
					 * slot types the copy performs the work materialization
					 * would later require anyway.
					 */
					ExecCopySlot(batchslot, myslot);
					myslot = batchslot;
				}
			}

			/* ensure that triggers etc see the right relation  */
			myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
		}

		skip_tuple = false;

		/*
		 * Compute which segment this row belongs to.
		 */
		if (cstate->dispatch_mode == COPY_DISPATCH)
		{
			/* In QD, compute the target segment to send this row to. */
			target_seg = GetTargetSeg(distData, myslot);

			bool send_to_all = distData &&
							   GpPolicyIsReplicated(distData->policy);

			/* in the QD, forward the row to the correct segment(s). */
			SendCopyFromForwardedTuple(cstate, cdbCopy, send_to_all,
									   send_to_all ? 0 : target_seg,
									   resultRelInfo->ri_RelationDesc,
									   cstate->cur_lineno,
									   cstate->line_buf.data,
									   cstate->line_buf.len,
									   myslot->tts_values,
									   myslot->tts_isnull);
			skip_tuple = true;
			processed++;
		}

		/* BEFORE ROW INSERT Triggers */
		if (has_before_insert_row_trig)
		{
			/*
			 * If the tuple was dispatched to segments, do not execute trigger
			 * on coordinator.
			 */
			if (!skip_tuple && !ExecBRInsertTriggers(estate, resultRelInfo, myslot))
				skip_tuple = true;	/* "do nothing" */
		}

		if (!skip_tuple)
		{
			/*
			 * If there is an INSTEAD OF INSERT ROW trigger, let it handle the
			 * tuple.  Otherwise, proceed with inserting the tuple into the
			 * table or foreign table.
			 */
			if (has_instead_insert_row_trig)
			{
				ExecIRInsertTriggers(estate, resultRelInfo, myslot);
			}
			else
			{
				/* Compute stored generated columns */
				if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
					resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored)
					ExecComputeStoredGenerated(estate, myslot);

				/*
				 * If the target is a plain table, check the constraints of
				 * the tuple.
				 */
				if (resultRelInfo->ri_FdwRoutine == NULL &&
					resultRelInfo->ri_RelationDesc->rd_att->constr)
					ExecConstraints(resultRelInfo, myslot, estate);

				/*
				 * Also check the tuple against the partition constraint, if
				 * there is one; except that if we got here via tuple-routing,
				 * we don't need to if there's no BR trigger defined on the
				 * partition.
				 */
				if (resultRelInfo->ri_PartitionCheck &&
					(proute == NULL || has_before_insert_row_trig))
					ExecPartitionCheck(resultRelInfo, myslot, estate, true);

				/* Store the slot in the multi-insert buffer, when enabled. */
				if (insertMethod == CIM_MULTI || leafpart_use_multi_insert)
				{
					/*
					 * The slot previously might point into the per-tuple
					 * context. For batching it needs to be longer lived.
					 */
					ExecMaterializeSlot(myslot);

					/* Add this tuple to the tuple buffer */
					CopyMultiInsertInfoStore(&multiInsertInfo,
											 resultRelInfo, myslot,
											 cstate->line_buf.len,
											 cstate->cur_lineno);

					/*
					 * If enough inserts have queued up, then flush all
					 * buffers out to their tables.
					 */
					if (CopyMultiInsertInfoIsFull(&multiInsertInfo))
						CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}
				else
				{
					List	   *recheckIndexes = NIL;

					/* OK, store the tuple */
					if (resultRelInfo->ri_FdwRoutine != NULL)
					{
						myslot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate,
																				 resultRelInfo,
																				 myslot,
																				 NULL);

						if (myslot == NULL) /* "do nothing" */
							continue;	/* next tuple please */

						/*
						 * AFTER ROW Triggers might reference the tableoid
						 * column, so (re-)initialize tts_tableOid before
						 * evaluating them.
						 */
						myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
					}
					else
					{
						/* OK, store the tuple and create index entries for it */
						table_tuple_insert(resultRelInfo->ri_RelationDesc,
										   myslot, mycid, ti_options, bistate);

						if (resultRelInfo->ri_NumIndices > 0)
							recheckIndexes = ExecInsertIndexTuples(myslot,
																   estate,
																   false,
																   NULL,
																   NIL);
					}

					/* AFTER ROW INSERT Triggers */
					ExecARInsertTriggers(estate, resultRelInfo, myslot,
										 recheckIndexes, cstate->transition_capture);

					list_free(recheckIndexes);
				}
			}

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger
			 * or FDW; this is the same definition used by nodeModifyTable.c
			 * for counting tuples inserted by an INSERT command. Update
			 * progress of the COPY command as well.
			 *
			 * MPP: incrementing this counter here only matters for utility
			 * mode. in dispatch mode only the dispatcher COPY collects row
			 * count, so this counter is meaningless.
			 */
			pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
										 ++processed);
#ifdef FAULT_INJECTOR
			if (processed == 2)
				SIMPLE_FAULT_INJECTOR("copy_processed_two_tuples");
#endif
			if (cstate->cdbsreh)
				cstate->cdbsreh->processed++;
		}
	}

	/*
	 * After processed data from QD, which is empty and just for workflow, now
	 * to process the data on segment, only one shot if cstate->on_segment &&
	 * Gp_role == GP_ROLE_DISPATCH
	 */
	if (cstate->on_segment && Gp_role == GP_ROLE_EXECUTE)
	{
		CopyInitDataParser(cstate);
	}
	elog(DEBUG1, "Segment %u, Copied %lu rows.", GpIdentity.segindex, processed);
	/* Flush any remaining buffered tuples */
	if (insertMethod != CIM_SINGLE)
	{
		if (!CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
			CopyMultiInsertInfoFlush(&multiInsertInfo, NULL);
	}

	/* Done, clean up */
	error_context_stack = errcallback.previous;

	if (bistate != NULL)
		FreeBulkInsertState(bistate);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Done reading input data and sending it off to the segment
	 * databases Now we would like to end the copy command on
	 * all segment databases across the cluster.
	 */
	if (cstate->dispatch_mode == COPY_DISPATCH)
	{
		int64		total_completed_from_qes;
		int64		total_rejected_from_qes;

		cdbCopyEnd(cdbCopy,
				   &total_completed_from_qes,
				   &total_rejected_from_qes);

		/*
		 * Reset returned processed to total_completed_from_qes.
		 *
		 * processed above excludes only rejected rows on QD, it
		 * should also exclude rejected rows on QEs.
		 *
		 * NOTE:
		 *  total_completed_from_qes + total_rejected_from_qes <= # of
		 *  input file rows
		 *
		 * total_rejected_from_qes includes only rows rejected by
		 * SREH; however, total_completed_from_qes excludes both
		 * SREH-rejected rows and TRIGGER-rejected rows.
		 */
		processed = total_completed_from_qes;

		if (cstate->cdbsreh)
		{
			/* emit a NOTICE with number of rejected rows */
			uint64		total_rejected = 0;
			uint64		total_rejected_from_qd = cstate->cdbsreh->rejectcount;

			/*
			 * If error log has been requested, then we send the row to the segment
			 * so that it can be written in the error log file. The segment process
			 * counts it again as a rejected row. So we ignore the reject count
			 * from the coordinator and only consider the reject count from segments.
			 */
			if (IS_LOG_TO_FILE(cstate->cdbsreh->logerrors))
				total_rejected_from_qd = 0;

			total_rejected = total_rejected_from_qd + total_rejected_from_qes;

			ReportSrehResults(cstate->cdbsreh, total_rejected);
		}
	}

	/*
	 * In the old protocol, tell pqcomm that we can process normal protocol
	 * messages again.
	 */
	if (cstate->copy_dest == COPY_OLD_FE)
		pq_endmsgread();

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, target_resultRelInfo, cstate->transition_capture);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	/*
	 * In QE, send the number of rejected rows to the client (QD COPY) if
	 * SREH is on, always send the number of completed rows.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
	{
		SendNumRows((cstate->errMode != ALL_OR_NOTHING) ? cstate->cdbsreh->rejectcount : 0, processed);
	}

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Allow the FDW to shut down */
	if (target_resultRelInfo->ri_FdwRoutine != NULL &&
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert != NULL)
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert(estate,
															  target_resultRelInfo);

	/* Tear down the multi-insert buffer data */
	if (insertMethod != CIM_SINGLE)
		CopyMultiInsertInfoCleanup(&multiInsertInfo);

	if (target_resultRelInfo->ri_RelationDesc->rd_tableam)
		table_dml_finish(target_resultRelInfo->ri_RelationDesc);

	ExecCloseIndices(target_resultRelInfo);

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (proute)
		ExecCleanupTupleRouting(mtstate, proute);

	/* Close any trigger target relations */
	ExecCleanUpTriggerState(estate);

	FreeDistributionData(distData);

	FreeExecutorState(estate);

	return processed;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'filename': Name of server-local file to read
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyState, to be passed to NextCopyFrom and related functions.
 */
CopyState
BeginCopyFrom(ParseState *pstate,
			  Relation rel,
			  const char *filename,
			  bool is_program,
			  copy_data_source_cb data_source_cb,
			  void *data_source_cb_extra,
			  List *attnamelist,
			  List *options)
{
	CopyState	cstate;
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				num_defaults;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	int		   *defmap;
	ExprState **defexprs;
	MemoryContext oldcontext;
	bool		volatile_defexprs;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE,
		PROGRESS_COPY_BYTES_TOTAL
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_FROM,
		0,
		0
	};

	cstate = BeginCopy(pstate, true, rel, NULL, InvalidOid, attnamelist, options, NULL);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	if (cstate->on_segment)
		progress_vals[0] = PROGRESS_COPY_COMMAND_FROM_ON_SEGMENT;

	/*
	 * Determine the mode
	 */
	if (cstate->on_segment || data_source_cb)
		cstate->dispatch_mode = COPY_DIRECT;
	else if (Gp_role == GP_ROLE_DISPATCH &&
			 cstate->rel && cstate->rel->rd_cdbpolicy &&
			 cstate->rel->rd_cdbpolicy->ptype != POLICYTYPE_ENTRY)
		cstate->dispatch_mode = COPY_DISPATCH;
	/*
	 * Handle case where fdw executes on coordinator while it's acting as a segment
	 * This occurs when fdw is under a redistribute on the coordinator
	 */
	else if (Gp_role == GP_ROLE_EXECUTE &&
			 cstate->rel && cstate->rel->rd_cdbpolicy &&
			 cstate->rel->rd_cdbpolicy->ptype == POLICYTYPE_ENTRY)
		cstate->dispatch_mode = COPY_DIRECT;
	else if (Gp_role == GP_ROLE_EXECUTE)
		cstate->dispatch_mode = COPY_EXECUTOR;
	else
		cstate->dispatch_mode = COPY_DIRECT;

	/* Initialize state variables */
	cstate->reached_eof = false;
	// cstate->eol_type = EOL_UNKNOWN; /* GPDB: don't overwrite value set in ProcessCopyOptions */
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;

	/* Set up variables to avoid per-attribute overhead. */
	initStringInfo(&cstate->attribute_buf);
	initStringInfo(&cstate->line_buf);
	cstate->line_buf_converted = false;
	cstate->raw_buf = (char *) palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	num_defaults = 0;
	volatile_defexprs = false;

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* We don't need info for dropped attributes */
		if (att->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		if (cstate->binary)
			getTypeBinaryInputInfo(att->atttypid,
								   &in_func_oid, &typioparams[attnum - 1]);
		else
			getTypeInputInfo(att->atttypid,
							 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/* TODO: is force quote array necessary for default conversion */

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum) && !att->attgenerated)
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Expr	   *defexpr = (Expr *) build_column_default(cstate->rel,
																attnum);

			if (defexpr != NULL)
			{
				/* Run the expression through planner */
				defexpr = expression_planner(defexpr);

				/* Initialize executable expression in copycontext */
				defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
				defmap[num_defaults] = attnum - 1;
				num_defaults++;

				/*
				 * If a default expression looks at the table being loaded,
				 * then it could give the wrong answer when using
				 * multi-insert. Since database access can be dynamic this is
				 * hard to test for exactly, so we use the much wider test of
				 * whether the default expression is volatile. We allow for
				 * the special case of when the default expression is the
				 * nextval() of a sequence which in this specific case is
				 * known to be safe for use with the multi-insert
				 * optimization. Hence we use this special case function
				 * checker rather than the standard check for
				 * contain_volatile_functions().
				 */
				if (!volatile_defexprs)
					volatile_defexprs = contain_volatile_functions_not_nextval((Node *) defexpr);
			}
		}
	}

	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	cstate->bytes_processed = 0;

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->defmap = defmap;
	cstate->defexprs = defexprs;
	cstate->volatile_defexprs = volatile_defexprs;
	cstate->num_defaults = num_defaults;
	cstate->is_program = is_program;

	bool		pipe = (filename == NULL || cstate->dispatch_mode == COPY_EXECUTOR);

	if (cstate->on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* open nothing */

		if (filename == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("STDIN is not supported by 'COPY ON SEGMENT'")));
	}
	else if (data_source_cb)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_CALLBACK;
		cstate->copy_dest = COPY_CALLBACK;
		cstate->data_source_cb = data_source_cb;
		cstate->data_source_cb_extra = data_source_cb_extra;
	}
	else if (pipe)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;
		Assert(!is_program || cstate->dispatch_mode == COPY_EXECUTOR);	/* the grammar does not allow this */
		if (whereToSendOutput == DestRemote)
			ReceiveCopyBegin(cstate);
		else
			cstate->copy_file = stdin;
	}
	else
	{
		cstate->filename = pstrdup(filename);

		if (cstate->on_segment)
			MangleCopyFileName(cstate);

		if (cstate->is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->program_pipes = open_program_pipes(cstate, false);
			cstate->copy_file = fdopen(cstate->program_pipes->pipes[0], PG_BINARY_R);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			struct stat st;
			char	   *filename = cstate->filename;

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;
			cstate->copy_file = AllocateFile(filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int			save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for reading: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY FROM instructs the PostgreSQL server process to read a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));
			}

			// Increase buffer size to improve performance  (cmcdevitt)
			setvbuf(cstate->copy_file, NULL, _IOFBF, 393216); // 384 Kbytes

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", filename)));

			progress_vals[2] = st.st_size;
		}
	}

	pgstat_progress_update_multi_param(3, progress_cols, progress_vals);

	if (cstate->on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* nothing to do */
	}
	else if (cstate->dispatch_mode == COPY_EXECUTOR && cstate->copy_dest != COPY_CALLBACK)
	{
		/* Read special header from QD */
		static const size_t sigsize = sizeof(QDtoQESignature);
		char		readSig[sizeof(QDtoQESignature)];
		copy_from_dispatch_header header_frame;

		if (CopyGetData(cstate, &readSig, sigsize) != sigsize ||
			memcmp(readSig, QDtoQESignature, sigsize) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("QD->QE COPY communication signature not recognized")));

		if (CopyGetData(cstate, &header_frame, sizeof(header_frame)) != sizeof(header_frame))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					errmsg("invalid QD->QD COPY communication header")));

		cstate->first_qe_processed_field = header_frame.first_qe_processed_field;
	}
	else if (cstate->binary)
	{
		/* Read and verify binary header */
		char		readSig[11];
		int32		tmp;

		/* Signature */
		if (CopyGetData(cstate, readSig, 11) != 11 ||
			memcmp(readSig, BinarySignature, 11) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					errmsg("COPY file signature not recognized")));
		/* Flags field */
		if (!CopyGetInt32(cstate, &tmp))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (missing flags)")));
		if ((tmp & (1 << 16)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (WITH OIDS)")));
		tmp &= ~(1 << 16);
		if ((tmp >> 16) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unrecognized critical flags in COPY file header")));
		/* Header extension length */
		if (!CopyGetInt32(cstate, &tmp) ||
			tmp < 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid COPY file header (missing length)")));
		/* Skip extension header, if present */
		while (tmp-- > 0)
		{
			if (CopyGetData(cstate, readSig, 1) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid COPY file header (wrong length)")));
		}
	}

	/* create workspace for CopyReadAttributes results */
	if (!cstate->binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **) palloc(attr_count * sizeof(char *));
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Read raw fields in the next line for COPY FROM in text or csv mode.
 * Return false if no more lines.
 *
 * An internal temporary buffer is returned via 'fields'. It is valid until
 * the next call of the function. Since the function returns all raw fields
 * in the input file, 'nfields' could be different from the number of columns
 * in the relation.
 *
 * NOTE: force_not_null option are not applied to the returned fields.
 */
bool
NextCopyFromRawFields(CopyState cstate, char ***fields, int *nfields)
{
	return NextCopyFromRawFieldsX(cstate, fields, nfields, -1);
}

static bool
NextCopyFromRawFieldsX(CopyState cstate, char ***fields, int *nfields,
					   int stop_processing_at_field)
{
	int			fldct;
	bool		done;

	/* only available for text or csv input */
	Assert(!cstate->binary);

	/* on input just throw the header line away */
	if (cstate->cur_lineno == 0 && cstate->header_line)
	{
		cstate->cur_lineno++;
		if (CopyReadLine(cstate))
			return false;		/* done */
	}

	cstate->cur_lineno++;

	/* Actually read the line into memory here */
	done = CopyReadLine(cstate);

	/*
	 * EOF at start of line means we're done.  If we see EOF after some
	 * characters, we act as though it was newline followed by EOF, ie,
	 * process the line and then exit loop on next iteration.
	 */
	if (done && cstate->line_buf.len == 0)
		return false;

	/* Parse the line into de-escaped field values */
	if (cstate->csv_mode)
		fldct = CopyReadAttributesCSV(cstate, stop_processing_at_field);
	else
		fldct = CopyReadAttributesText(cstate, stop_processing_at_field);

	*fields = cstate->raw_fields;
	*nfields = fldct;
	return true;
}

bool
NextCopyFrom(CopyState cstate, ExprContext *econtext,
			   Datum *values, bool *nulls)
{
	if (!cstate->cdbsreh)
		return NextCopyFromX(cstate, econtext, values, nulls);
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;

		for (;;)
		{
			bool		got_error = false;
			bool		result = false;

			PG_TRY();
			{
				result = NextCopyFromX(cstate, econtext, values, nulls);
			}
			PG_CATCH();
			{
				HandleCopyError(cstate); /* cdbsreh->processed is updated inside here */
				got_error = true;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_END_TRY();

			if (result)
				cstate->cdbsreh->processed++;

			if (!got_error)
				return result;
		}
	}
}

/*
 * A data error happened. This code block will always be inside a PG_CATCH()
 * block right when a higher stack level produced an error. We handle the error
 * by checking which error mode is set (SREH or all-or-nothing) and do the right
 * thing accordingly. Note that we MUST have this code in a macro (as opposed
 * to a function) as elog_dismiss() has to be inlined with PG_CATCH in order to
 * access local error state variables.
 *
 * changing me? take a look at FILEAM_HANDLE_ERROR in fileam.c as well.
 */
void
HandleCopyError(CopyState cstate)
{
	if (cstate->errMode == ALL_OR_NOTHING)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	/* SREH must only handle data errors. all other errors must not be caught */
	if (ERRCODE_TO_CATEGORY(elog_geterrcode()) != ERRCODE_DATA_EXCEPTION)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	else
	{
		/* SREH - release error state and handle error */
		MemoryContext oldcontext;
		ErrorData	*edata;
		char	   *errormsg;
		CdbSreh	   *cdbsreh = cstate->cdbsreh;

		cdbsreh->processed++;

		oldcontext = MemoryContextSwitchTo(cstate->cdbsreh->badrowcontext);

		/* save a copy of the error info */
		edata = CopyErrorData();

		FlushErrorState();

		/*
		 * set the error message. Use original msg and add column name if available.
		 * We do this even if we're not logging the errors, because
		 * ErrorIfRejectLimit() below will use this information in the error message,
		 * if the error count is reached.
		 */
		cdbsreh->rawdata->cursor = 0;
		cdbsreh->rawdata->data = cstate->line_buf.data;
		cdbsreh->rawdata->len = cstate->line_buf.len;
		cdbsreh->is_server_enc = cstate->line_buf_converted;
		cdbsreh->linenumber = cstate->cur_lineno;
		if (cstate->cur_attname)
		{
			errormsg =  psprintf("%s, column %s",
								 edata->message, cstate->cur_attname);
		}
		else
		{
			errormsg = edata->message;
		}
		cstate->cdbsreh->errmsg = errormsg;

		if (IS_LOG_TO_FILE(cstate->cdbsreh->logerrors))
		{
			if (Gp_role == GP_ROLE_DISPATCH && !cstate->on_segment)
			{
				cstate->cdbsreh->rejectcount++;

				SendCopyFromForwardedError(cstate, cstate->cdbCopy, errormsg);
			}
			else
			{
				/* after all the prep work let cdbsreh do the real work */
				if (Gp_role == GP_ROLE_DISPATCH)
				{
					cstate->cdbsreh->rejectcount++;
				}
				else
				{
					HandleSingleRowError(cstate->cdbsreh);
				}
			}
		}
		else
			cstate->cdbsreh->rejectcount++;

		ErrorIfRejectLimitReached(cstate->cdbsreh);

		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(cstate->cdbsreh->badrowcontext);
	}
}


/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 */
bool
NextCopyFromX(CopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;
	List	   *attnumlist;
	int			stop_processing_at_field;

	/*
	 * Figure out what fields we're going to process in this process.
	 *
	 * In the QD, set 'stop_processing_at_field' so that we only those
	 * fields that are needed in the QD.
	 */
	switch (cstate->dispatch_mode)
	{
		case COPY_DIRECT:
			stop_processing_at_field = -1;
			attnumlist = cstate->attnumlist;
			break;

		case COPY_DISPATCH:
			stop_processing_at_field = cstate->first_qe_processed_field;
			attnumlist = cstate->qd_attnumlist;
			break;

		case COPY_EXECUTOR:
			stop_processing_at_field = -1;
			attnumlist = cstate->qe_attnumlist;
			break;

		default:
			elog(ERROR, "unexpected COPY dispatch mode %d", cstate->dispatch_mode);
	}

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(attnumlist);

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	if (!cstate->binary)
	{
		char	  **field_strings;
		ListCell   *cur;
		int			fldct;
		int			fieldno;
		char	   *string;

		/* read raw fields in the next line */
		if (cstate->dispatch_mode != COPY_EXECUTOR)
		{
			if (!NextCopyFromRawFieldsX(cstate, &field_strings, &fldct,
										stop_processing_at_field))
				return false;
		}
		else
		{
			/*
			 * We have received the raw line from the QD, and we just
			 * need to split it into raw fields.
			 */
			if (cstate->stopped_processing_at_delim &&
				cstate->line_buf.cursor <= cstate->line_buf.len)
			{
				if (cstate->csv_mode)
					fldct = CopyReadAttributesCSV(cstate, -1);
				else
					fldct = CopyReadAttributesText(cstate, -1);
			}
			else
				fldct = 0;
			field_strings = cstate->raw_fields;
		}

		/* 
		 * Check for overflowing fields.
		 * GPDB: Change below condition compared to upstream to 
		 * greater than or equal to 0 as in QE, 
		 * attr_count may be equal to 0, 
		 * when all fields are processed in the QD.
		 */
		if (fldct > attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));

		/*
		 * A completely empty line is not allowed with FILL MISSING FIELDS. Without
		 * FILL MISSING FIELDS, it's almost surely an error, but not always:
		 * a table with a single text column, for example, needs to accept empty
		 * lines.
		 */
		if (cstate->line_buf.len == 0 &&
			cstate->fill_missing &&
			list_length(cstate->attnumlist) > 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("missing data for column \"%s\", found empty data line",
							NameStr(TupleDescAttr(tupDesc, 1)->attname))));
		}

		fieldno = 0;

		/* Loop to read the user attributes on the line. */
		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			if (fieldno >= fldct)
			{
				/*
				 * Some attributes are missing. In FILL MISSING FIELDS mode,
				 * treat them as NULLs.
				 */
				if (!cstate->fill_missing)
					ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for column \"%s\"",
								NameStr(att->attname))));
				fieldno++;
				string = NULL;
			}
			else
				string = field_strings[fieldno++];

			if (cstate->convert_select_flags &&
				!cstate->convert_select_flags[m])
			{
				/* ignore input field, leaving column as NULL */
				continue;
			}

			if (cstate->csv_mode)
			{
				if (string == NULL &&
					cstate->force_notnull_flags[m])
				{
					/*
					 * FORCE_NOT_NULL option is set and column is NULL -
					 * convert it to the NULL string.
					 */
					string = cstate->null_print;
				}
				else if (string != NULL && cstate->force_null_flags[m]
						 && strcmp(string, cstate->null_print) == 0)
				{
					/*
					 * FORCE_NULL option is set and column matches the NULL
					 * string. It must have been quoted, or otherwise the
					 * string would already have been set to NULL. Convert it
					 * to NULL as specified.
					 */
					string = NULL;
				}
			}

			cstate->cur_attname = NameStr(att->attname);
			cstate->cur_attval = string;
			values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  att->atttypmod);
			if (string != NULL)
				nulls[m] = false;
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}

		Assert(fieldno == attr_count);
	}
	else if (attr_count)
	{
		/* binary */
		int16		fld_count;
		ListCell   *cur;

		cstate->cur_lineno++;

		if (!CopyGetInt16(cstate, &fld_count))
		{
			/* EOF detected (end of file, or protocol-level EOF) */
			return false;
		}

		if (fld_count == -1)
		{
			/*
			 * Received EOF marker.  In a V3-protocol copy, wait for the
			 * protocol-level EOF, and complain if it doesn't come
			 * immediately.  This ensures that we correctly handle CopyFail,
			 * if client chooses to send that now.
			 *
			 * Note that we MUST NOT try to read more data in an old-protocol
			 * copy, since there is no protocol-level EOF marker then.  We
			 * could go either way for copy from file, but choose to throw
			 * error if there's data after the EOF marker, for consistency
			 * with the new-protocol case.
			 */
			char		dummy;

			if (cstate->copy_dest != COPY_OLD_FE &&
				CopyGetData(cstate, &dummy, 1) > 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("received copy data after EOF marker")));
			return false;
		}

		if (fld_count != attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("row field count is %d, expected %d",
							(int) fld_count, attr_count)));

		i = 0;
		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			cstate->cur_attname = NameStr(att->attname);
			i++;
			values[m] = CopyReadBinaryAttribute(cstate,
												i,
												&in_functions[m],
												typioparams[m],
												att->atttypmod,
												&nulls[m]);
			cstate->cur_attname = NULL;
		}
	}

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.  Anything not processed here or above will
	 * remain NULL.
	 *
	 * GPDB: The defaults are always computed in the QD, and are included
	 * in the QD->QE stream as pre-computed Datums. Funny indentation, to
	 * keep the indentation of the code inside the same as in upstream.
	 * (We could improve this, and compute immutable defaults that don't
	 * affect which segment the row belongs to, in the QE.)
	 */
  if (cstate->dispatch_mode != COPY_EXECUTOR)
  {
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]]);
	}
  }

	return true;
}


/*
 * Like NextCopyFrom(), but used in the QD, when we want to parse the
 * input line only partially. We only want to parse enough fields needed
 * to determine which target segment to forward the row to.
 *
 * (The logic is actually within NextCopyFrom(). This is a separate
 * function just for symmetry with NextCopyFromExecute()).
 */
static bool
NextCopyFromDispatch(CopyState cstate, ExprContext *econtext,
					 Datum *values, bool *nulls)
{
	return NextCopyFrom(cstate, econtext, values, nulls);
}

/*
 * Like NextCopyFrom(), but used in the QE, when we're reading pre-processed
 * rows from the QD.
 */
static bool
NextCopyFromExecute(CopyState cstate, ExprContext *econtext, Datum *values, bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count;
	FormData_pg_attribute *attr;
	int			i;
	copy_from_dispatch_row frame;
	int			r;
	bool		got_error;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);

	/*
	 * The code below reads the 'copy_from_dispatch_row' struct, and only
	 * then checks if it was actually a 'copy_from_dispatch_error' struct.
	 * That only works when 'copy_from_dispatch_error' is larger than
	 *'copy_from_dispatch_row'.
	 */
	StaticAssertStmt(SizeOfCopyFromDispatchError >= SizeOfCopyFromDispatchRow,
					 "copy_from_dispatch_error must be larger than copy_from_dispatch_row");

	/*
	 * If we encounter an error while parsing the row (or we receive a row from
	 * the QD that was already marked as an erroneous row), we loop back here
	 * until we get a good row.
	 */
retry:
	got_error = false;

	r = CopyGetData(cstate, (char *) &frame, SizeOfCopyFromDispatchRow);
	if (r == 0)
		return false;
	if (r != SizeOfCopyFromDispatchRow)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	if (frame.lineno == -1)
	{
		HandleQDErrorFrame(cstate, (char *) &frame, SizeOfCopyFromDispatchRow);
		goto retry;
	}

	/* Prepare for parsing the input line */
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	/* check for overflowing fields */
	if (frame.fld_count < 0 || frame.fld_count > num_phys_attrs)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("extra data after last expected column")));

	/*
	 * Read the input line into 'line_buf'.
	 */
	resetStringInfo(&cstate->line_buf);
	enlargeStringInfo(&cstate->line_buf, frame.line_len);
	if (CopyGetData(cstate, cstate->line_buf.data, frame.line_len) != frame.line_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	cstate->line_buf.data[frame.line_len] = '\0';
	cstate->line_buf.len = frame.line_len;
	cstate->line_buf.cursor = frame.residual_off;
	cstate->line_buf_valid = true;
	cstate->line_buf_converted = true;
	cstate->cur_lineno = frame.lineno;
	cstate->stopped_processing_at_delim = frame.delim_seen_at_end;

	/*
	 * Parse any fields from the input line that were not processed in the
	 * QD already.
	 */
	if (!cstate->cdbsreh)
	{
		if (!NextCopyFromX(cstate, econtext, values, nulls))
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));
		}
	}
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;
		bool		result;

		PG_TRY();
		{
			result = NextCopyFromX(cstate, econtext, values, nulls);

			if (!result)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("unexpected EOF in COPY data")));
		}
		PG_CATCH();
		{
			HandleCopyError(cstate);
			got_error = true;
			MemoryContextSwitchTo(oldcontext);
		}
		PG_END_TRY();
	}

	/*
	 * Read any attributes that were processed in the QD already. The attribute
	 * numbers in the message are already in terms of the target partition, so
	 * we do this after remapping and switching to the partition slot.
	 */
	for (i = 0; i < frame.fld_count; i++)
	{
		int16		attnum;
		int			m;
		int32		len;
		Datum		value;

		if (CopyGetData(cstate, &attnum, sizeof(attnum)) != sizeof(attnum))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unexpected EOF in COPY data")));

		if (attnum < 1 || attnum > num_phys_attrs)
			elog(ERROR, "invalid attnum received from QD: %d (num physical attributes: %d)",
				 attnum, num_phys_attrs);
		m = attnum - 1;

		cstate->cur_attname = NameStr(attr[m].attname);

		if (attr[attnum - 1].attbyval)
		{
			if (CopyGetData(cstate, &value, sizeof(Datum)) != sizeof(Datum))
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("unexpected EOF in COPY data")));
		}
		else
		{
			char	   *p;

			if (attr[attnum - 1].attlen > 0)
			{
				len = attr[attnum - 1].attlen;

				p = palloc(len);
				if (CopyGetData(cstate, p, len) != len)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
			}
			else if (attr[attnum - 1].attlen == -1)
			{
				/* For simplicity, varlen's are always transmitted in "long" format */
				if (CopyGetData(cstate, &len, sizeof(len)) != sizeof(len))
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				if (len < VARHDRSZ)
					elog(ERROR, "invalid varlen length received from QD: %d", len);
				p = palloc(len);
				SET_VARSIZE(p, len);
				if (CopyGetData(cstate, p + VARHDRSZ, len - VARHDRSZ) != len - VARHDRSZ)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
			}
			else if (attr[attnum - 1].attlen == -2)
			{
				/*
				 * Like the varlen case above, cstrings are sent with a length
				 * prefix and no terminator, so we have to NULL-terminate in
				 * memory after reading them in.
				 */
				if (CopyGetData(cstate, &len, sizeof(len)) != sizeof(len))
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				p = palloc(len + 1);
				if (CopyGetData(cstate, p, len) != len)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unexpected EOF in COPY data")));
				p[len] = '\0';
			}
			else
			{
				elog(ERROR, "attribute %d has invalid length %d",
					 attnum, attr[attnum - 1].attlen);
			}
			value = PointerGetDatum(p);
		}

		cstate->cur_attname = NULL;

		values[m] = value;
		nulls[m] = false;
	}

	if (got_error)
		goto retry;

	/*
	 * Here we should compute defaults for any columns for which we didn't
	 * get a default from the QD. But at the moment, all defaults are evaluated
	 * in the QD.
	 */
	return true;
}

/*
 * Parse and handle an "error frame" from QD.
 *
 * The caller has already read part of the frame; 'p' points to that part,
 * of length 'len'.
 */
static void
HandleQDErrorFrame(CopyState cstate, char *p, int len)
{
	CdbSreh *cdbsreh = cstate->cdbsreh;
	MemoryContext oldcontext;
	copy_from_dispatch_error errframe;
	char	   *errormsg;
	char	   *line;
	int			r;

	Assert(len <= SizeOfCopyFromDispatchError);

	Assert(Gp_role == GP_ROLE_EXECUTE);

	oldcontext = MemoryContextSwitchTo(cdbsreh->badrowcontext);

	/*
	 * Copy the part of the struct that the caller had already read, and
	 * read the rest.
	 */
	memcpy(&errframe, p, len);

	r = CopyGetData(cstate, ((char *) &errframe) + len, SizeOfCopyFromDispatchError - len);
	if (r != SizeOfCopyFromDispatchError - len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	errormsg = palloc(errframe.errmsg_len + 1);
	line = palloc(errframe.line_len + 1);

	r = CopyGetData(cstate, errormsg, errframe.errmsg_len);
	if (r != errframe.errmsg_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	errormsg[errframe.errmsg_len] = '\0';

	r = CopyGetData(cstate, line, errframe.line_len);
	if (r != errframe.line_len)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	line[errframe.line_len] = '\0';

	cdbsreh->linenumber = errframe.lineno;
	cdbsreh->rawdata->cursor = 0;
	cdbsreh->rawdata->data = line;
	cdbsreh->rawdata->len = strlen(line);
	cdbsreh->errmsg = errormsg;
	cdbsreh->is_server_enc = errframe.line_buf_converted;

	HandleSingleRowError(cdbsreh);

	MemoryContextSwitchTo(oldcontext);

}

/*
 * Inlined versions of appendBinaryStringInfo and enlargeStringInfo, for
 * speed.
 *
 * NOTE: These versions don't NULL-terminate the string. We don't need
 * it here.
 */
#define APPEND_MSGBUF_NOCHECK(buf, ptr, datalen) \
	do { \
		memcpy((buf)->data + (buf)->len, ptr, (datalen)); \
		(buf)->len += (datalen); \
	} while(0)

#define APPEND_MSGBUF(buf, ptr, datalen) \
	do { \
		if ((buf)->len + (datalen) >= (buf)->maxlen) \
			enlargeStringInfo((buf), (datalen)); \
		memcpy((buf)->data + (buf)->len, ptr, (datalen)); \
		(buf)->len += (datalen); \
	} while(0)

#define ENLARGE_MSGBUF(buf, needed) \
	do { \
		if ((buf)->len + (needed) >= (buf)->maxlen) \
			enlargeStringInfo((buf), (needed)); \
	} while(0)

/*
 * This is the sending counterpart of NextCopyFromExecute. Used in the QD,
 * to send a row to a QE.
 */
static void
SendCopyFromForwardedTuple(CopyState cstate,
						   CdbCopy *cdbCopy,
						   bool toAll,
						   int target_seg,
						   Relation rel,
						   int64 lineno,
						   char *line,
						   int line_len,
						   Datum *values,
						   bool *nulls)
{
	TupleDesc	tupDesc;
	FormData_pg_attribute *attr;
	copy_from_dispatch_row *frame;
	StringInfo	msgbuf;
	int			num_sent_fields;
	AttrNumber	num_phys_attrs;
	int			i;

	if (!OidIsValid(RelationGetRelid(rel)))
		elog(ERROR, "invalid target table OID in COPY");

	tupDesc = RelationGetDescr(rel);
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;

	/*
	 * Reset the message buffer, and reserve enough space for the header,
	 * the OID if any, and the residual line.
	 */
	msgbuf = cstate->dispatch_msgbuf;
	ENLARGE_MSGBUF(msgbuf, SizeOfCopyFromDispatchRow + sizeof(Oid) + cstate->line_buf.len);

	/* the header goes to the beginning of the struct, but it will be filled in later. */
	msgbuf->len = SizeOfCopyFromDispatchRow;

	/*
	 * Next, any residual text that we didn't process in the QD.
	 */
	APPEND_MSGBUF_NOCHECK(msgbuf, cstate->line_buf.data, cstate->line_buf.len);

	/*
	 * Append attributes to the buffer.
	 */
	num_sent_fields = 0;
	for (i = 0; i < num_phys_attrs; i++)
	{
		int16		attnum = i + 1;

		/* NULLs are simply left out of the message. */
		if (nulls[i])
			continue;

		/*
		 * Make sure we have room for the attribute number. While we're at it,
		 * also reserve room for the Datum, if it's a by-value datatype, or for
		 * the length field, if it's a varlena. Allocating both in one call
		 * saves one size-check.
		 */
		ENLARGE_MSGBUF(msgbuf, sizeof(int16) + sizeof(Datum));

		/* attribute number comes first */
		APPEND_MSGBUF_NOCHECK(msgbuf, &attnum, sizeof(int16));

		if (attr[i].attbyval)
		{
			/* we already reserved space for this above, so we can just memcpy */
			APPEND_MSGBUF_NOCHECK(msgbuf, &values[i], sizeof(Datum));
		}
		else
		{
			if (attr[i].attlen > 0)
			{
				APPEND_MSGBUF(msgbuf, DatumGetPointer(values[i]), attr[i].attlen);
			}
			else if (attr[i].attlen == -1)
			{
				int32		len;
				char	   *ptr;

				/* For simplicity, varlen's are always transmitted in "long" format */
				Assert(!VARATT_IS_SHORT(values[i]));
				len = VARSIZE(values[i]);
				ptr = VARDATA(values[i]);

				/* we already reserved space for this int */
				APPEND_MSGBUF_NOCHECK(msgbuf, &len, sizeof(int32));
				APPEND_MSGBUF(msgbuf, ptr, len - VARHDRSZ);
			}
			else if (attr[i].attlen == -2)
			{
				/*
				 * These attrs are NULL-terminated in memory, but we send
				 * them length-prefixed (like the varlen case above) so that
				 * the receiver can preallocate a data buffer.
				 */
				int32		len;
				size_t		slen;
				char	   *ptr;

				ptr = DatumGetPointer(values[i]);
				slen = strlen(ptr);

				if (slen > PG_INT32_MAX)
				{
					elog(ERROR, "attribute %d is too long (%lld bytes)",
						 attnum, (long long) slen);
				}

				len = (int32) slen;

				APPEND_MSGBUF_NOCHECK(msgbuf, &len, sizeof(int32));
				APPEND_MSGBUF(msgbuf, ptr, len);
			}
			else
			{
				elog(ERROR, "attribute %d has invalid length %d",
					 attnum, attr[i].attlen);
			}
		}

		num_sent_fields++;
	}

	/*
	 * Fill in the header. We reserved room for this at the beginning of the
	 * buffer.
	 */
	frame = (copy_from_dispatch_row *) msgbuf->data;
	frame->lineno = lineno;
	frame->relid = RelationGetRelid(rel);
	frame->line_len = cstate->line_buf.len;
	frame->residual_off = cstate->line_buf.cursor;
	frame->fld_count = num_sent_fields;
	frame->delim_seen_at_end = cstate->stopped_processing_at_delim;

	if (toAll)
		cdbCopySendDataToAll(cdbCopy, msgbuf->data, msgbuf->len);
	else
		cdbCopySendData(cdbCopy, target_seg, msgbuf->data, msgbuf->len);
}

static void
SendCopyFromForwardedHeader(CopyState cstate, CdbCopy *cdbCopy)
{
	copy_from_dispatch_header header_frame;

	cdbCopySendDataToAll(cdbCopy, QDtoQESignature, sizeof(QDtoQESignature));

	memset(&header_frame, 0, sizeof(header_frame));
	header_frame.first_qe_processed_field = cstate->first_qe_processed_field;

	cdbCopySendDataToAll(cdbCopy, (char *) &header_frame, sizeof(header_frame));
}

static void
SendCopyFromForwardedError(CopyState cstate, CdbCopy *cdbCopy, char *errormsg)
{
	copy_from_dispatch_error *errframe;
	StringInfo	msgbuf;
	int			target_seg;
	int			errormsg_len = strlen(errormsg);

	msgbuf = cstate->dispatch_msgbuf;
	resetStringInfo(msgbuf);
	enlargeStringInfo(msgbuf, SizeOfCopyFromDispatchError);
	/* allocate space for the header (we'll fill it in last). */
	msgbuf->len = SizeOfCopyFromDispatchError;

	appendBinaryStringInfo(msgbuf, errormsg, errormsg_len);
	appendBinaryStringInfo(msgbuf, cstate->line_buf.data, cstate->line_buf.len);

	errframe = (copy_from_dispatch_error *) msgbuf->data;

	errframe->error_marker = -1;
	errframe->lineno = cstate->cur_lineno;
	errframe->line_len = cstate->line_buf.len;
	errframe->errmsg_len = errormsg_len;
	errframe->line_buf_converted = cstate->line_buf_converted;

	/* send the bad data row to a random QE (via roundrobin) */
	if (cstate->lastsegid == cdbCopy->total_segs)
		cstate->lastsegid = 0; /* start over from first segid */

	target_seg = (cstate->lastsegid++ % cdbCopy->total_segs);

	cdbCopySendData(cdbCopy, target_seg, msgbuf->data, msgbuf->len);
}

/*
 * Clean up storage and release resources for COPY FROM.
 */
void
EndCopyFrom(CopyState cstate)
{
	/* No COPY FROM related resources except memory. */

	/*
	 * We call pgstat_progress_end_command here even EndCopy does 
	 * the same because we want to be consistent with upstream.
	 * The upstream does that because it doesn't call EndCopy in 
	 * EndCopyFrom, and that's what GPDB would do when we merge with
	 * PG14. So calling it here in case we miss it when that happens.
	 * The second call of it should just be a no-op.
	 *
	 * GPDB_14_MERGE_FIXME: remove this comment. 
	 */
	pgstat_progress_end_command();

	EndCopy(cstate);
}

/*
 * Read the next input line and stash it in line_buf, with conversion to
 * server encoding.
 *
 * Result is true if read was terminated by EOF, false if terminated
 * by newline.  The terminating newline or EOF marker is not included
 * in the final value of line_buf.
 */
static bool
CopyReadLine(CopyState cstate)
{
	bool		result;

	resetStringInfo(&cstate->line_buf);
	cstate->line_buf_valid = true;

	/* Mark that encoding conversion hasn't occurred yet */
	cstate->line_buf_converted = false;

	/* Parse data and transfer into line_buf */
	result = CopyReadLineText(cstate);

	if (result)
	{
		/*
		 * Reached EOF.  In protocol version 3, we should ignore anything
		 * after \. up to the protocol end of copy data.  (XXX maybe better
		 * not to treat \. as special?)
		 */
		if (cstate->copy_dest == COPY_NEW_FE)
		{
			do
			{
				cstate->raw_buf_index = cstate->raw_buf_len;
			} while (CopyLoadRawBuf(cstate));
		}
	}
	else
	{
		/*
		 * If we didn't hit EOF, then we must have transferred the EOL marker
		 * to line_buf along with the data.  Get rid of it.
		 */
		switch (cstate->eol_type)
		{
			case EOL_NL:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_CR:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\r');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_CRNL:
				Assert(cstate->line_buf.len >= 2);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 2] == '\r');
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len -= 2;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case EOL_UNKNOWN:
				/* shouldn't get here */
				Assert(false);
				break;
		}
	}

	/* Done reading the line.  Convert it to server encoding. */
	if (cstate->need_transcoding)
	{
		char	   *cvt;

		cvt = pg_any_to_server(cstate->line_buf.data,
							   cstate->line_buf.len,
							   cstate->file_encoding);
		if (cvt != cstate->line_buf.data)
		{
			/* transfer converted data back to line_buf */
			resetStringInfo(&cstate->line_buf);
			appendBinaryStringInfo(&cstate->line_buf, cvt, strlen(cvt));
			pfree(cvt);
		}
	}

	/* Now it's safe to use the buffer in error messages */
	cstate->line_buf_converted = true;

	return result;
}

/*
 * CopyReadLineText - inner loop of CopyReadLine for text mode
 */
static bool
CopyReadLineText(CopyState cstate)
{
	char	   *copy_raw_buf;
	int			raw_buf_ptr;
	int			copy_buf_len;
	bool		need_data = false;
	bool		hit_eof = false;
	bool		result = false;
	char		mblen_str[2];

	/* CSV variables */
	bool		first_char_in_line = true;
	bool		in_quote = false,
				last_was_esc = false;
	char		quotec = '\0';
	char		escapec = '\0';

	if (cstate->csv_mode)
	{
		quotec = cstate->quote[0];
		escapec = cstate->escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';
	}

	mblen_str[1] = '\0';

	/*
	 * The objective of this loop is to transfer the entire next input line
	 * into line_buf.  Hence, we only care for detecting newlines (\r and/or
	 * \n) and the end-of-copy marker (\.).
	 *
	 * In CSV mode, \r and \n inside a quoted field are just part of the data
	 * value and are put in line_buf.  We keep just enough state to know if we
	 * are currently in a quoted field or not.
	 *
	 * These four characters, and the CSV escape and quote characters, are
	 * assumed the same in frontend and backend encodings.
	 *
	 * For speed, we try to move data from raw_buf to line_buf in chunks
	 * rather than one character at a time.  raw_buf_ptr points to the next
	 * character to examine; any characters from raw_buf_index to raw_buf_ptr
	 * have been determined to be part of the line, but not yet transferred to
	 * line_buf.
	 *
	 * For a little extra speed within the loop, we copy raw_buf and
	 * raw_buf_len into local variables.
	 */
	copy_raw_buf = cstate->raw_buf;
	raw_buf_ptr = cstate->raw_buf_index;
	copy_buf_len = cstate->raw_buf_len;

	for (;;)
	{
		int			prev_raw_ptr;
		char		c;

		/*
		 * Load more data if needed.  Ideally we would just force four bytes
		 * of read-ahead and avoid the many calls to
		 * IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(), but the COPY_OLD_FE protocol
		 * does not allow us to read too far ahead or we might read into the
		 * next data, so we read-ahead only as far we know we can.  One
		 * optimization would be to read-ahead four byte here if
		 * cstate->copy_dest != COPY_OLD_FE, but it hardly seems worth it,
		 * considering the size of the buffer.
		 */
		if (raw_buf_ptr >= copy_buf_len || need_data)
		{
			REFILL_LINEBUF;

			/*
			 * Try to read some more data.  This will certainly reset
			 * raw_buf_index to zero, and raw_buf_ptr must go with it.
			 */
			if (!CopyLoadRawBuf(cstate))
				hit_eof = true;
			raw_buf_ptr = 0;
			copy_buf_len = cstate->raw_buf_len;

			/*
			 * If we are completely out of data, break out of the loop,
			 * reporting EOF.
			 */
			if (copy_buf_len <= 0)
			{
				result = true;
				break;
			}
			need_data = false;
		}

		/* OK to fetch a character */
		prev_raw_ptr = raw_buf_ptr;
		c = copy_raw_buf[raw_buf_ptr++];

		if (cstate->csv_mode)
		{
			/*
			 * If character is '\\' or '\r', we may need to look ahead below.
			 * Force fetch of the next character if we don't already have it.
			 * We need to do this before changing CSV state, in case one of
			 * these characters is also the quote or escape character.
			 *
			 * Note: old-protocol does not like forced prefetch, but it's OK
			 * here since we cannot validly be at EOF.
			 */
			if (c == '\\' || c == '\r')
			{
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			}

			/*
			 * Dealing with quotes and escapes here is mildly tricky. If the
			 * quote char is also the escape char, there's no problem - we
			 * just use the char as a toggle. If they are different, we need
			 * to ensure that we only take account of an escape inside a
			 * quoted field and immediately preceding a quote char, and not
			 * the second in an escape-escape sequence.
			 */
			if (in_quote && c == escapec)
				last_was_esc = !last_was_esc;
			if (c == quotec && !last_was_esc)
				in_quote = !in_quote;
			if (c != escapec)
				last_was_esc = false;

			/*
			 * Updating the line count for embedded CR and/or LF chars is
			 * necessarily a little fragile - this test is probably about the
			 * best we can do.  (XXX it's arguable whether we should do this
			 * at all --- is cur_lineno a physical or logical count?)
			 */
			if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}

		/* Process \r */
		if (c == '\r' && (!cstate->csv_mode || !in_quote))
		{
			/* Check for \r\n on first line, _and_ handle \r\n. */
			if (cstate->eol_type == EOL_UNKNOWN ||
				cstate->eol_type == EOL_CRNL)
			{
				/*
				 * If need more data, go back to loop top to load it.
				 *
				 * Note that if we are at EOF, c will wind up as '\0' because
				 * of the guaranteed pad of raw_buf.
				 */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);

				/* get next char */
				c = copy_raw_buf[raw_buf_ptr];

				if (c == '\n')
				{
					raw_buf_ptr++;	/* eat newline */
					cstate->eol_type = EOL_CRNL;	/* in case not set yet */

					/*
					 * GPDB: end of line. Since we don't error out if we find a
					 * bare CR or LF in CRLF mode, break here instead.
					 */
					break;
				}
				else
				{
					/*
					 * GPDB_91_MERGE_FIXME: these commented-out blocks (as well
					 * as the restructured newline checks) are here because we
					 * allow the user to manually set the newline mode, and
					 * therefore don't error out on bare CR/LF in the middle of
					 * a column. Instead, they will be included verbatim.
					 *
					 * This probably has other fallout -- but so does changing
					 * the behavior. Discuss.
					 */
#if 0
					/* found \r, but no \n */
					if (cstate->eol_type == EOL_CRNL)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 !cstate->csv_mode ?
								 errmsg("literal carriage return found in data") :
								 errmsg("unquoted carriage return found in data"),
								 !cstate->csv_mode ?
								 errhint("Use \"\\r\" to represent carriage return.") :
								 errhint("Use quoted CSV field to represent carriage return.")));
#endif

					/* GPDB: only reset eol_type if it's currently unknown. */
					if (cstate->eol_type == EOL_UNKNOWN)
					{
						/*
						 * if we got here, it is the first line and we didn't find
						 * \n, so don't consume the peeked character
						 */
						cstate->eol_type = EOL_CR;
					}
				}
			}
#if 0 /* GPDB_91_MERGE_FIXME: see above. */
			else if (cstate->eol_type == EOL_NL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ?
						 errmsg("literal carriage return found in data") :
						 errmsg("unquoted carriage return found in data"),
						 !cstate->csv_mode ?
						 errhint("Use \"\\r\" to represent carriage return.") :
						 errhint("Use quoted CSV field to represent carriage return.")));
#endif
			/* GPDB: a CR only ends the line in CR mode. */
			if (cstate->eol_type == EOL_CR)
			{
				/* If reach here, we have found the line terminator */
				break;
			}
		}

		/* Process \n */
		if (c == '\n' && (!cstate->csv_mode || !in_quote))
		{
#if 0 /* GPDB_91_MERGE_FIXME: see above. */
			if (cstate->eol_type == EOL_CR || cstate->eol_type == EOL_CRNL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ?
						 errmsg("literal newline found in data") :
						 errmsg("unquoted newline found in data"),
						 !cstate->csv_mode ?
						 errhint("Use \"\\n\" to represent newline.") :
						 errhint("Use quoted CSV field to represent newline.")));
#endif
			/* GPDB: only reset eol_type if it's currently unknown. */
			if (cstate->eol_type == EOL_UNKNOWN)
				cstate->eol_type = EOL_NL;	/* in case not set yet */

			/* GPDB: a LF only ends the line in LF mode. */
			if (cstate->eol_type == EOL_NL)
			{
				/* If reach here, we have found the line terminator */
				break;
			}
		}

		/*
		 * In CSV mode, we only recognize \. alone on a line.  This is because
		 * \. is a valid CSV data value.
		 */
		if (c == '\\' && (!cstate->csv_mode || first_char_in_line))
		{
			char		c2;

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			IF_NEED_REFILL_AND_EOF_BREAK(0);

			/* -----
			 * get next character
			 * Note: we do not change c so if it isn't \., we can fall
			 * through and continue processing for file encoding.
			 * -----
			 */
			c2 = copy_raw_buf[raw_buf_ptr];

			/*
			* We need to recognize the EOL.
			* Github issue: https://github.com/greenplum-db/gpdb/issues/12454
			*/
			if(c2 == '\n')
			{
				if(cstate->eol_type == EOL_UNKNOWN)
				{
				/* We still do not found the first EOL.
				* The current '\n' will be recongnized as EOL
				* in next loop of c1.
				*/
					goto not_end_of_copy;
				}
				else if(cstate->eol_type == EOL_NL)
				{
					// found a new line with '\n'
					raw_buf_ptr++;
					break;
				}
			}
			if (c2 == '\r')
			{
				if(cstate->eol_type == EOL_UNKNOWN)
				{
					goto not_end_of_copy;
				}
				else if(cstate->eol_type == EOL_CR)
				{
					// found a new line wirh '\r'
					raw_buf_ptr++;
					break;
				}
				else if(cstate->eol_type == EOL_CRNL)
				{
					/*
					* Because the eol is '\r\n', we need another character c3
					* which comes after c2 if exists.
					*/
					char c3;
					raw_buf_ptr++;
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					IF_NEED_REFILL_AND_EOF_BREAK(0);
					c3 = copy_raw_buf[raw_buf_ptr];
					if(c3 == '\n')
					{
						// found a new line with '\r\n'
						raw_buf_ptr++;
						break;
					} else {
						NO_END_OF_COPY_GOTO;
					}
				}
			}
			if (c2 == '.')
			{
				raw_buf_ptr++;	/* consume the '.' */

				/*
				 * Note: if we loop back for more data here, it does not
				 * matter that the CSV state change checks are re-executed; we
				 * will come back here with no important state changed.
				 */
				if (cstate->eol_type == EOL_CRNL)
				{
					/* Get the next character */
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					/* if hit_eof, c2 will become '\0' */
					c2 = copy_raw_buf[raw_buf_ptr++];

					if (c2 == '\n')
					{
						if (!cstate->csv_mode)
						{
							cstate->raw_buf_index = raw_buf_ptr;
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker does not match previous newline style")));
						}
						else
							NO_END_OF_COPY_GOTO;
					}
					else if (c2 != '\r')
					{
						if (!cstate->csv_mode)
						{
							cstate->raw_buf_index = raw_buf_ptr;
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker corrupt")));
						}
						else
							NO_END_OF_COPY_GOTO;
					}
				}

				/* Get the next character */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
				/* if hit_eof, c2 will become '\0' */
				c2 = copy_raw_buf[raw_buf_ptr++];

				if (c2 != '\r' && c2 != '\n')
				{
					if (!cstate->csv_mode)
					{
						cstate->raw_buf_index = raw_buf_ptr;
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 errmsg("end-of-copy marker corrupt")));
					}
					else
						NO_END_OF_COPY_GOTO;
				}

				if ((cstate->eol_type == EOL_NL && c2 != '\n') ||
					(cstate->eol_type == EOL_CRNL && c2 != '\n') ||
					(cstate->eol_type == EOL_CR && c2 != '\r'))
				{
					cstate->raw_buf_index = raw_buf_ptr;
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker does not match previous newline style")));
				}

				/*
				 * Transfer only the data before the \. into line_buf, then
				 * discard the data and the \. sequence.
				 */
				if (prev_raw_ptr > cstate->raw_buf_index)
					appendBinaryStringInfo(&cstate->line_buf,
										   cstate->raw_buf + cstate->raw_buf_index,
										   prev_raw_ptr - cstate->raw_buf_index);
				cstate->raw_buf_index = raw_buf_ptr;
				result = true;	/* report EOF */
				break;
			}
			else if (!cstate->csv_mode)
			{
				/*
				 * If we are here, it means we found a backslash followed by
				 * something other than a period.  In non-CSV mode, anything
				 * after a backslash is special, so we skip over that second
				 * character too.  If we didn't do that \\. would be
				 * considered an eof-of copy, while in non-CSV mode it is a
				 * literal backslash followed by a period.  In CSV mode,
				 * backslashes are not special, so we want to process the
				 * character after the backslash just like a normal character,
				 * so we don't increment in those cases.
				 *
				 * Set 'c' to skip whole character correctly in multi-byte
				 * encodings.  If we don't have the whole character in the
				 * buffer yet, we might loop back to process it, after all,
				 * but that's OK because multi-byte characters cannot have any
				 * special meaning.
				 */
				raw_buf_ptr++;
				c = c2;
			}
		}
		/*
		 * This label is for CSV cases where \. appears at the start of a
		 * line, but there is more text after it, meaning it was a data value.
		 * We are more strict for \. in CSV mode because \. could be a data
		 * value, while in non-CSV mode, \. cannot be a data value.
		 */
not_end_of_copy:

		/*
		 * Process all bytes of a multi-byte character as a group.
		 *
		 * We only support multi-byte sequences where the first byte has the
		 * high-bit set, so as an optimization we can avoid this block
		 * entirely if it is not set.
		 */
		if (cstate->encoding_embeds_ascii && IS_HIGHBIT_SET(c))
		{
			int			mblen;

			/*
			 * It is enough to look at the first byte in all our encodings, to
			 * get the length.  (GB18030 is a bit special, but still works for
			 * our purposes; see comment in pg_gb18030_mblen())
			 */
			mblen_str[0] = c;
			mblen = pg_encoding_mblen(cstate->file_encoding, mblen_str);

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(mblen - 1);
			IF_NEED_REFILL_AND_EOF_BREAK(mblen - 1);
			raw_buf_ptr += mblen - 1;
		}
		first_char_in_line = false;
	}							/* end of outer loop */

	/*
	 * Transfer any still-uncopied data to line_buf.
	 */
	REFILL_LINEBUF;

	return result;
}

/*
 *	Return decimal value for a hexadecimal digit
 */
static int
GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char) hex))
		return hex - '0';
	else
		return tolower((unsigned char) hex) - 'a' + 10;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.
 *
 * The input is in line_buf.  We use attribute_buf to hold the result
 * strings.  cstate->raw_fields[k] is set to point to the k'th attribute
 * string, or NULL when the input matches the null marker string.
 * This array is expanded as necessary.
 *
 * (Note that the caller cannot check for nulls since the returned
 * string would be the post-de-escaping equivalent, which may look
 * the same as some valid data string.)
 *
 * delim is the column delimiter string (must be just one byte for now).
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string.
 *
 * The return value is the number of fields actually read.
 */
static int
CopyReadAttributesText(CopyState cstate, int stop_processing_at_field)
{
	char		delimc = cstate->delim[0];
	char		escapec = cstate->escape[0];
	bool		delim_off = cstate->delim_off;
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data + cstate->line_buf.cursor;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;
		bool		saw_non_ascii = false;

		/*
		 * In QD, stop once we have processed the last field we need in the QD.
		 */
		if (fieldno == stop_processing_at_field)
		{
			cstate->stopped_processing_at_delim = true;
			break;
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field.
		 *
		 * Note that in this loop, we are scanning to locate the end of field
		 * and also speculatively performing de-escaping.  Once we find the
		 * end-of-field, we can match the raw field contents against the null
		 * marker string.  Only after that comparison fails do we know that
		 * de-escaping is actually the right thing to do; therefore we *must
		 * not* throw any syntax errors before we've done the null-marker
		 * check.
		 */
		for (;;)
		{
			char		c;

			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (c == delimc && !delim_off)
			{
				found_delim = true;
				break;
			}
			if (c == escapec && !cstate->escape_off)
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							/* handle \013 */
							int			val;

							val = OCTVALUE(c);
							if (cur_ptr < line_end_ptr)
							{
								c = *cur_ptr;
								if (ISOCTAL(c))
								{
									cur_ptr++;
									val = (val << 3) + OCTVALUE(c);
									if (cur_ptr < line_end_ptr)
									{
										c = *cur_ptr;
										if (ISOCTAL(c))
										{
											cur_ptr++;
											val = (val << 3) + OCTVALUE(c);
										}
									}
								}
							}
							c = val & 0377;
							if (c == '\0' || IS_HIGHBIT_SET(c))
								saw_non_ascii = true;
						}
						break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char		hexchar = *cur_ptr;

							if (isxdigit((unsigned char) hexchar))
							{
								int			val = GetDecimalFromHex(hexchar);

								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char) hexchar))
									{
										cur_ptr++;
										val = (val << 4) + GetDecimalFromHex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;

						/*
						 * in all other cases, take the char after '\'
						 * literally
						 */
				}
			}

			/* Add c to output string */
			*output_ptr++ = c;
		}

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;
		else
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 */
			if (saw_non_ascii)
			{
				char	   *fld = cstate->raw_fields[fieldno];

				pg_verifymbstr(fld, output_ptr - fld, false);
			}
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
		{
			cstate->stopped_processing_at_delim = false;
			break;
		}
	}

	/*
	 * Make note of the stopping point in 'line_buf.cursor', so that we
	 * can send the rest to the QE later.
	 */
	cstate->line_buf.cursor = cur_ptr - cstate->line_buf.data;

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.  This has exactly the same API as
 * CopyReadAttributesText, except we parse the fields according to
 * "standard" (i.e. common) CSV usage.
 */
static int
CopyReadAttributesCSV(CopyState cstate, int stop_processing_at_field)
{
	char		delimc = cstate->delim[0];
	bool		delim_off = cstate->delim_off;
	char		quotec = cstate->quote[0];
	char		escapec = cstate->escape[0];
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data + cstate->line_buf.cursor;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		bool		saw_quote = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;

		/*
		 * In QD, stop once we have processed the last field we need in the QD.
		 */
		if (fieldno == stop_processing_at_field)
		{
			cstate->stopped_processing_at_delim = true;
			break;
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field,
		 *
		 * The loop starts in "not quote" mode and then toggles between that
		 * and "in quote" mode. The loop exits normally if it is in "not
		 * quote" mode and a delimiter or line end is seen.
		 */
		for (;;)
		{
			char		c;

			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if (c == delimc && !delim_off)
				{
					found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}

			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unterminated CSV quoted field")));

				c = *cur_ptr++;

				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char		nextc = *cur_ptr;

						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}

				/*
				 * end of quoted field. Must do this test after testing for
				 * escape in case quote char and escape char are the same
				 * (which is the common case).
				 */
				if (c == quotec)
					break;

				/* Add c to output string */
				*output_ptr++ = c;
			}
		}
endfield:

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
		{
			cstate->stopped_processing_at_delim = false;
			break;
		}
	}

	/*
	 * Make note of the stopping point in 'line_buf.cursor', so that we
	 * can send the rest to the QE later.
	 */
	cstate->line_buf.cursor = cur_ptr - cstate->line_buf.data;

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Read a binary attribute
 */
static Datum
CopyReadBinaryAttribute(CopyState cstate,
						int column_no, FmgrInfo *flinfo,
						Oid typioparam, int32 typmod,
						bool *isnull)
{
	int32		fld_size;
	Datum		result;

	if (!CopyGetInt32(cstate, &fld_size))
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	if (fld_size == -1)
	{
		*isnull = true;
		return ReceiveFunctionCall(flinfo, NULL, typioparam, typmod);
	}
	if (fld_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid field size")));

	/* reset attribute_buf to empty, and load raw data in it */
	resetStringInfo(&cstate->attribute_buf);

	enlargeStringInfo(&cstate->attribute_buf, fld_size);
	if (CopyGetData(cstate, cstate->attribute_buf.data,
					fld_size) != fld_size)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	cstate->attribute_buf.len = fld_size;
	cstate->attribute_buf.data[fld_size] = '\0';

	/* Call the column type's binary input converter */
	result = ReceiveFunctionCall(flinfo, &cstate->attribute_buf,
								 typioparam, typmod);

	/* Trouble if it didn't eat the whole buffer */
	if (cstate->attribute_buf.cursor != cstate->attribute_buf.len)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("incorrect binary data format")));

	*isnull = false;
	return result;
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define DUMPSOFAR() \
	do { \
		if (ptr > start) \
			CopySendData(cstate, start, ptr - start); \
	} while (0)

static void
CopyAttributeOutText(CopyState cstate, char *string)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];
	char		escapec = cstate->escape[0];

	if (cstate->need_transcoding)
		ptr = pg_server_to_custom(string,
								  strlen(string),
								  cstate->file_encoding,
								  cstate->enc_conversion_proc);
	else
		ptr = string;


	if (cstate->escape_off)
	{
		CopySendData(cstate, ptr, strlen(ptr));
		return;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (cstate->encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == escapec || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				CopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == escapec || c == delimc)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else
				ptr++;
		}
	}

	DUMPSOFAR();
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void
CopyAttributeOutCSV(CopyState cstate, char *string,
					bool use_quote, bool single_attr)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];
	char		quotec;
	char		escapec = cstate->escape[0];

	/*
	 * MPP-8075. We may get called with cstate->quote == NULL.
	 */
	if (cstate->quote == NULL)
	{
		quotec = '"';
	}
	else
	{
		quotec = cstate->quote[0];
	}

	/* force quoting if it matches null_print (before conversion!) */
	if (!use_quote && strcmp(string, cstate->null_print) == 0)
		use_quote = true;

	if (cstate->need_transcoding)
		ptr = pg_server_to_custom(string,
								  strlen(string),
								  cstate->file_encoding,
								  cstate->enc_conversion_proc);
	else
		ptr = string;

	/*
	 * Make a preliminary pass to discover if it needs quoting
	 */
	if (!use_quote)
	{
		/*
		 * Because '\.' can be a data value, quote it if it appears alone on a
		 * line so it is not interpreted as the end-of-data marker.
		 */
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			char	   *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if (c == delimc || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (use_quote)
	{
		CopySendChar(cstate, quotec);

		/*
		 * We adopt the same optimization strategy as in CopyAttributeOutText
		 */
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if (c == quotec || c == escapec)
			{
				DUMPSOFAR();
				CopySendChar(cstate, escapec);
				start = ptr;	/* we include char in next run */
			}
			if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
		DUMPSOFAR();

		CopySendChar(cstate, quotec);
	}
	else
	{
		/* If it doesn't need quoting, we can just dump it as-is */
		CopySendString(cstate, ptr);
	}
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * We don't include generated columns in the generated full list and we don't
 * allow them to be specified explicitly.  They don't make sense for COPY
 * FROM, but we could possibly allow them for COPY TO.  But this way it's at
 * least ensured that whatever we copy out can be copied back in.
 *
 * rel can be NULL ... it's only used for error reports.
 */
List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			if (TupleDescAttr(tupDesc, i)->attgenerated)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					if (att->attgenerated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
								 errmsg("column \"%s\" is a generated column",
										name),
								 errdetail("Generated columns cannot be used in COPY.")));
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

/* remove end of line chars from end of a buffer */
void truncateEol(StringInfo buf, EolType eol_type)
{
	int one_back = buf->len - 1;
	int two_back = buf->len - 2;

	if(eol_type == EOL_CRNL)
	{
		if(buf->len < 2)
			return;

		if(buf->data[two_back] == '\r' &&
		   buf->data[one_back] == '\n')
		{
			buf->data[two_back] = '\0';
			buf->data[one_back] = '\0';
			buf->len -= 2;
		}
	}
	else
	{
		if(buf->len < 1)
			return;

		if(buf->data[one_back] == '\r' ||
		   buf->data[one_back] == '\n')
		{
			buf->data[one_back] = '\0';
			buf->len--;
		}
	}
}

/* wrapper for truncateEol */
void
truncateEolStr(char *str, EolType eol_type)
{
	StringInfoData buf;

	buf.data = str;
	buf.len = strlen(str);
	buf.maxlen = buf.len;
	truncateEol(&buf, eol_type);
}

/*
 * copy_dest_startup --- executor startup
 */
static void
copy_dest_startup(DestReceiver *self pg_attribute_unused(), int operation pg_attribute_unused(), TupleDesc typeinfo pg_attribute_unused())
{
	if (Gp_role != GP_ROLE_EXECUTE)
		return;
	DR_copy    *myState = (DR_copy *) self;
	myState->cstate = BeginCopyToOnSegment(myState->queryDesc);
}

/*
 * copy_dest_receive --- receive one tuple
 */
static bool
copy_dest_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_copy    *myState = (DR_copy *) self;
	CopyState	cstate = myState->cstate;

	/* Send the data */
	CopyOneRowTo(cstate, slot);

	/* Increment the number of processed tuples, and report the progress */
	pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
								 ++myState->processed);

	return true;
}

/*
 * copy_dest_shutdown --- executor end
 */
static void
copy_dest_shutdown(DestReceiver *self pg_attribute_unused())
{
	if (Gp_role != GP_ROLE_EXECUTE)
		return;
	DR_copy    *myState = (DR_copy *) self;
	EndCopyToOnSegment(myState->cstate);
}

/*
 * copy_dest_destroy --- release DestReceiver object
 */
static void
copy_dest_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * CreateCopyDestReceiver -- create a suitable DestReceiver object
 */
DestReceiver *
CreateCopyDestReceiver(void)
{
	DR_copy    *self = (DR_copy *) palloc(sizeof(DR_copy));

	self->pub.receiveSlot = copy_dest_receive;
	self->pub.rStartup = copy_dest_startup;
	self->pub.rShutdown = copy_dest_shutdown;
	self->pub.rDestroy = copy_dest_destroy;
	self->pub.mydest = DestCopyOut;

	self->cstate = NULL;		/* need to be set later */
	self->queryDesc = NULL;		/* need to be set later */
	self->processed = 0;

	return (DestReceiver *) self;
}

/*
 * Initialize data loader parsing state
 */
static void CopyInitDataParser(CopyState cstate)
{
	cstate->reached_eof = false;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->null_print_len = strlen(cstate->null_print);

	/* Set up data buffer to hold a chunk of data */
	MemSet(cstate->raw_buf, ' ', RAW_BUF_SIZE * sizeof(char));
	cstate->raw_buf[RAW_BUF_SIZE] = '\0';
}

/*
 * setEncodingConversionProc
 *
 * COPY and External tables use a custom path to the encoding conversion
 * API because external tables have their own encoding (which is not
 * necessarily client_encoding). We therefore have to set the correct
 * encoding conversion function pointer ourselves, to be later used in
 * the conversion engine.
 *
 * The code here mimics a part of SetClientEncoding() in mbutils.c
 */
static void
setEncodingConversionProc(CopyState cstate, int encoding, bool iswritable)
{
	Oid		conversion_proc;
	
	/*
	 * COPY FROM and RET: convert from file to server
	 * COPY TO   and WET: convert from server to file
	 */
	if (iswritable)
		conversion_proc = FindDefaultConversionProc(GetDatabaseEncoding(), encoding);
	else		
		conversion_proc = FindDefaultConversionProc(encoding, GetDatabaseEncoding());
	
	if (OidIsValid(conversion_proc))
	{
		/* conversion proc found */
		cstate->enc_conversion_proc = palloc(sizeof(FmgrInfo));
		fmgr_info(conversion_proc, cstate->enc_conversion_proc);
	}
	else
	{
		/* no conversion function (both encodings are probably the same) */
		cstate->enc_conversion_proc = NULL;
	}
}

static GpDistributionData *
InitDistributionData(CopyState cstate, EState *estate)
{
	GpDistributionData *distData;
	GpPolicy   *policy;
	CdbHash	   *cdbHash;

	/*
	 * A non-partitioned table, or all the partitions have identical
	 * distribution policies.
	 */
	policy = GpPolicyCopy(cstate->rel->rd_cdbpolicy);
	cdbHash = makeCdbHashForRelation(cstate->rel);

	distData = palloc(sizeof(GpDistributionData));
	distData->policy = policy;
	distData->cdbHash = cdbHash;

	return distData;
}

static void
FreeDistributionData(GpDistributionData *distData)
{
	if (distData)
	{
		if (distData->policy)
			pfree(distData->policy);
		if (distData->cdbHash)
			pfree(distData->cdbHash);
		pfree(distData);
	}
}

/*
 * Compute which fields need to be processed in the QD, and which ones can
 * be delayed to the QE.
 */
static void
InitCopyFromDispatchSplit(CopyState cstate, GpDistributionData *distData,
						  EState *estate)
{
	int			first_qe_processed_field = 0;
	Bitmapset  *needed_cols = NULL;
	ListCell   *lc;

	if (cstate->binary)
	{
		foreach(lc, cstate->attnumlist)
		{
			AttrNumber attnum = lfirst_int(lc);
			needed_cols = bms_add_member(needed_cols, attnum);
			first_qe_processed_field++;
		}
	}
	else
	{
		int			fieldno;
		/*
		 * We need all the columns that form the distribution key.
		 */
		if (distData->policy)
		{
			for (int i = 0; i < distData->policy->nattrs; i++)
				needed_cols = bms_add_member(needed_cols, distData->policy->attrs[i]);
		}

		/* Get the max fieldno that contains one of the needed attributes. */
		fieldno = 0;
		foreach(lc, cstate->attnumlist)
		{
			AttrNumber attnum = lfirst_int(lc);

			if (bms_is_member(attnum, needed_cols))
				first_qe_processed_field = fieldno + 1;
			fieldno++;
		}
	}

	cstate->first_qe_processed_field = first_qe_processed_field;

	if (Test_copy_qd_qe_split)
	{
		if (first_qe_processed_field == list_length(cstate->attnumlist))
			elog(INFO, "all fields will be processed in the QD");
		else
			elog(INFO, "first field processed in the QE: %d", first_qe_processed_field);
	}
}

static unsigned int
GetTargetSeg(GpDistributionData *distData, TupleTableSlot *slot)
{
	unsigned int target_seg;
	CdbHash	   *cdbHash = distData->cdbHash;
	GpPolicy   *policy = distData->policy; /* the partitioning policy for this table */
	AttrNumber	p_nattrs;	/* num of attributes in the distribution policy */

	/*
	 * These might be NULL, if we're called with a "main" GpDistributionData,
	 * for a partitioned table with heterogenous partitions. The caller
	 * should've used GetDistributionPolicyForPartition() to get the right
	 * distdata object for the partition.
	 */
	if (!policy)
		elog(ERROR, "missing distribution policy.");
	if (!cdbHash)
		elog(ERROR, "missing cdbhash");

	/*
	 * At this point in the code, baseValues[x] is final for this
	 * data row -- either the input data, a null or a default
	 * value is in there, and constraints applied.
	 *
	 * Perform a cdbhash on this data row. Perform a hash operation
	 * on each attribute.
	 */
	p_nattrs = policy->nattrs;
	if (p_nattrs > 0)
	{
		cdbhashinit(cdbHash);

		for (int i = 0; i < p_nattrs; i++)
		{
			/* current attno from the policy */
			AttrNumber	h_attnum = policy->attrs[i];
			Datum		d;
			bool		isnull;

			d = slot_getattr(slot, h_attnum, &isnull);

			cdbhash(cdbHash, i + 1, d, isnull);
		}

		target_seg = cdbhashreduce(cdbHash); /* hash result segment */
	}
	else
	{
		/*
		 * Randomly distributed. Pick a segment at random.
		 */
		target_seg = cdbhashrandomseg(policy->numsegments);
	}

	return target_seg;
}

static void
close_program_pipes_on_reset(void *arg)
{
	if (!IsAbortInProgress())
		return;

	CopyState	cstate = arg;

	close_program_pipes(cstate, false);
}

static ProgramPipes*
open_program_pipes(CopyState cstate, bool forwrite)
{
	char	   *command = cstate->filename;
	int save_errno;
	pqsigfunc save_SIGPIPE;
	/* set up extvar */
	extvar_t extvar;
	memset(&extvar, 0, sizeof(extvar));

	external_set_env_vars(&extvar, command, false, NULL, NULL, false, 0);

	ProgramPipes *program_pipes = palloc(sizeof(ProgramPipes));
	program_pipes->pid = -1;
	program_pipes->pipes[0] = -1;
	program_pipes->pipes[1] = -1;
	program_pipes->shexec = make_command(command, &extvar);

	/*
	 * Preserve the SIGPIPE handler and set to default handling.  This
	 * allows "normal" SIGPIPE handling in the command pipeline.  Normal
	 * for PG is to *ignore* SIGPIPE.
	 */
	save_SIGPIPE = pqsignal(SIGPIPE, SIG_DFL);

	program_pipes->pid = popen_with_stderr(program_pipes->pipes, program_pipes->shexec, forwrite);

	save_errno = errno;

	/* Restore the SIGPIPE handler */
	pqsignal(SIGPIPE, save_SIGPIPE);

	elog(DEBUG5, "COPY ... PROGRAM command: %s", program_pipes->shexec);
	if (program_pipes->pid == -1)
	{
		errno = save_errno;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("can not start command: %s", command)));
	}

	MemoryContextCallback *callback = MemoryContextAlloc(cstate->copycontext, sizeof(MemoryContextCallback));

	callback->arg = cstate;
	callback->func = close_program_pipes_on_reset;
	MemoryContextRegisterResetCallback(cstate->copycontext, callback);

	return program_pipes;
}

static void
close_program_pipes(CopyState cstate, bool ifThrow)
{
	Assert(cstate->is_program);

	int ret = 0;
	StringInfoData sinfo = {0};

	if (cstate->copy_file)
	{
		fclose(cstate->copy_file);
		cstate->copy_file = NULL;
	}

	/* just return if pipes not created, like when relation does not exist */
	if (!cstate->program_pipes)
	{
		return;
	}

	if (ifThrow)
		initStringInfo(&sinfo);
	ret = pclose_with_stderr(cstate->program_pipes->pid, cstate->program_pipes->pipes, &sinfo);
	cstate->program_pipes = NULL;

	if (ret == 0 || !ifThrow)
	{
		return;
	}

	if (ret == -1)
	{
		/* pclose()/wait4() ended with an error; errno should be valid */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("can not close pipe: %m")));
	}
	else if (!WIFSIGNALED(ret))
	{
		/*
		 * pclose() returned the process termination state.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
				 errmsg("command error message: %s", sinfo.data)));
	}
}

static List *
parse_joined_option_list(char *str, char *delimiter)
{
	char	   *token;
	char	   *comma;
	const char *whitespace = " \t\n\r";
	List	   *cols = NIL;
	int			encoding = GetDatabaseEncoding();

	token = strtokx2(str, whitespace, delimiter, "\"",
					 0, false, false, encoding);

	while (token)
	{
		if (token[0] == ',')
			break;

		cols = lappend(cols, makeString(pstrdup(token)));

		/* consume the comma if any */
		comma = strtokx2(NULL, whitespace, delimiter, "\"",
						 0, false, false, encoding);
		if (!comma || comma[0] != ',')
			break;

		token = strtokx2(NULL, whitespace, delimiter, "\"",
						 0, false, false, encoding);
	}

	return cols;
}
