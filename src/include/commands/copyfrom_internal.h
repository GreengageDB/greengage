/*-------------------------------------------------------------------------
 *
 * copyfrom_internal.h
 *	  Internal definitions for COPY FROM command.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copyfrom_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPYFROM_INTERNAL_H
#define COPYFROM_INTERNAL_H

#include "commands/copy.h"
#include "commands/trigger.h"

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
 * Holds the target's distribution policy during COPY FROM.
 */
typedef struct GpDistributionData
{
	GpPolicy   *policy;		/* partitioning policy for this table */
	CdbHash	   *cdbHash;	/* corresponding CdbHash object */
} GpDistributionData;

/*
 * This struct contains all the state variables used throughout a COPY FROM
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
typedef struct CopyFromStateData
{
	/* low-level state data */
	CopySrcDest	copy_src;		/* type of copy source */
	FILE	   *copy_file;		/* used if copy_src == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used if copy_src == COPY_NEW_FE */
	bool		reached_eof;	/* true if we read to end of copy data (not
								 * all copy_src types maintain this) */

	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_source_cb data_source_cb; /* function for reading data */
	void	   *data_source_cb_extra;

	CopyFormatOptions opts;
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */
	Node	   *whereClause;	/* WHERE condition (or NULL) */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	uint64		cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state
	 */
	CopyDispatchMode dispatch_mode;
	MemoryContext copycontext;	/* per-copy execution context */

	AttrNumber	num_defaults;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;	/* is any of defexprs volatile? */
	List	   *range_table;
	ExprState  *qualexpr;

	TransitionCaptureState *transition_capture;

	StringInfo	dispatch_msgbuf; /* used in COPY_DISPATCH mode, to construct message
								  * to send to QE. */
	
	/* Error handling options */
	CopyErrMode	errMode;
	struct CdbSreh *cdbsreh; /* single row error handler */
	int			lastsegid;


	/*
	 * These variables are used to reduce overhead in COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 *
	 * In binary COPY FROM, attribute_buf holds the binary data for the
	 * current field, but the usage is otherwise similar.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.  (In binary mode,
	 * line_buf is not used.)
	 */
	StringInfoData line_buf;
	bool		line_buf_converted; /* converted to server encoding? */
	bool		line_buf_valid; /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).  In text mode, CopyReadLine parses this data
	 * sufficiently to locate line boundaries, then transfers the data to
	 * line_buf and converts it.  In binary mode, CopyReadBinaryData fetches
	 * appropriate amounts of data from this buffer.  In both modes, we
	 * guarantee that there is a \0 at raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
	uint64		bytes_processed;/* number of bytes processed so far */
	/* Shorthand for number of unconsumed bytes available in raw_buf */
#define RAW_BUF_BYTES(cstate) ((cstate)->raw_buf_len - (cstate)->raw_buf_index)

	/* GPDB specific variables */
	FmgrInfo   *enc_conversion_proc; /* conv proc from exttbl encoding to
										server or the other way around */
	int			first_qe_processed_field;
	List	   *qd_attnumlist;
	List	   *qe_attnumlist;
	bool		stopped_processing_at_delim;

	ProgramPipes	*program_pipes; /* COPY PROGRAM pipes for data and stderr */
	MemoryContext rowcontext;	/* per-row evaluation context */

	/* Information on the connections to QEs. */
	CdbCopy    *cdbCopy;

	/* end GPDB specific variables */
} CopyFromStateData;

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

extern void ReceiveCopyBegin(CopyFromState cstate);
extern void ReceiveCopyBinaryHeader(CopyFromState cstate);

/* in copyfromparse.c, shared with copyfrom.c */
extern int	CopyGetData(CopyFromState cstate, void *databuf, int datasize);
extern bool NextCopyFromX(CopyFromState cstate, ExprContext *econtext,
						  Datum *values, bool *nulls);
extern bool NextCopyFromDispatch(CopyFromState cstate, ExprContext *econtext,
								 Datum *values, bool *nulls);
extern bool NextCopyFromExecute(CopyFromState cstate, ExprContext *econtext,
								Datum *values, bool *nulls);
extern void HandleCopyError(CopyFromState cstate);

/* in copyfrom.c, shared with copyfromparse.c */
extern void SendCopyFromForwardedError(CopyFromState cstate, CdbCopy *cdbCopy,
									   char *errormsg);

#endif							/* COPYFROM_INTERNAL_H */
