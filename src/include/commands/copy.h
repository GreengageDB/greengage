/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPY_H
#define COPY_H

#include "commands/trigger.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"
#include "executor/executor.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbcopy.h"

/*
 * Represents whether a header line should be present, and whether it must
 * match the actual names (which implies "true").
 */
typedef enum CopyHeaderChoice
{
	COPY_HEADER_FALSE = 0,
	COPY_HEADER_TRUE,
	COPY_HEADER_MATCH,
} CopyHeaderChoice;

/*
 * Represents the different source/dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_FILE,					/* to/from file (or a piped program) */
	COPY_FRONTEND,				/* to/from frontend (3.0 protocol) */
	COPY_CALLBACK				/* to/from callback function (used for external tables) */
} CopyDest;

#define COPY_OLD_FE		COPY_FRONTEND
#define COPY_NEW_FE		COPY_FRONTEND

/* CopyStateData is private in commands/copy.c */
typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread, void *extra);

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;

/*
 * The error handling mode for this data load.
 */
typedef enum CopyErrMode
{
	ALL_OR_NOTHING,	/* Either all rows or no rows get loaded (the default) */
	SREH_IGNORE,	/* Sreh - ignore errors (REJECT, but don't log errors) */
	SREH_LOG		/* Sreh - log errors */
} CopyErrMode;

typedef struct ProgramPipes
{
	char *shexec;
	int pipes[2];
	int pid;
} ProgramPipes;

/*
 *
 * COPY FROM modes (from file/client to table)
 *
 * 1. "normal", direct, mode. This means ON SEGMENT running on a segment, or
 *    utility mode, or non-distributed table in QD.
 * 2. Dispatcher mode. We are reading from file/client, and forwarding all data to QEs,
 *    or vice versa.
 * 3. Executor mode. We are receiving pre-processed data from QD, and inserting to table.
 *
 * COPY TO modes (table/query to file/client)
 *
 * 1. Direct. This can mean ON SEGMENT running on segment, or utility mode, or
 *    non-distributed table in QD. Or COPY TO running on segment.
 * 2. Dispatcher mode. We are receiving pre-formatted data from segments, and forwarding
 *    it all to to the client.
 * 3. Executor mode. Not used.
 */

typedef enum
{
	COPY_DIRECT,
	COPY_DISPATCH,
	COPY_EXECUTOR
} CopyDispatchMode;

struct PartitionNode;
struct CdbCopy;

/*
 * This struct contains all the state variables used throughout a COPY
 * operation. For simplicity, we use the same struct for all variants of COPY,
 * even though some fields are used in only some cases.
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
typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		is_copy_from;	/* COPY TO, or COPY FROM? */
	bool		fe_eof;			/* true if detected end of copy data */
	bool		reached_eof;	/* true if we read to end of copy data */
	EolType		eol_type;		/* EOL type of input */
	char	   *eol_str;		/* optional NEWLINE from command */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */
	FmgrInfo   *enc_conversion_proc;

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to or from */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	List	   *attnamelist;	/* list of attributes by name */
	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_source_cb data_source_cb;
	void	   *data_source_cb_extra;
	bool		freeze;			/* freeze rows on loading? */
	bool		binary;			/* binary format */
	bool		csv_mode;		/* Comma Separated Value format? */
	CopyHeaderChoice header_line;	/* header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len;	/* length of same */
	char	   *null_print_client;	/* same converted to file encoding */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE QUOTE *? */
	bool	   *force_quote_flags;	/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool	   *force_null_flags;	/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select;	/* list of column names (can be NIL) */
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */
	bool		fill_missing;	/* missing attrs at end of line are NULL */

	SingleRowErrorDesc *sreh;

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	int64		cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/* Working state */
	CopyDispatchMode dispatch_mode;
	MemoryContext copycontext;	/* per-copy execution context */

	/* Working state for COPY TO */
	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */

	/* Working state for COPY FROM */
	AttrNumber	num_defaults;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;	/* is any of defexprs volatile? */
	List	   *range_table;
	ExprState  *qualexpr;

	TransitionCaptureState *transition_capture;

	StringInfo	dispatch_msgbuf;

	/* Error handling options */
	CopyErrMode errMode;
	struct CdbSreh *cdbsreh;	/* single row error handler */
	int			lastsegid;

	StringInfoData attribute_buf;

	int			max_fields;
	char	  **raw_fields;

	StringInfoData line_buf;
	bool		line_buf_converted;
	bool		line_buf_valid;

#define RAW_BUF_SIZE 65536
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */

	/* Greenplum specific variables */
	bool		escape_off;
	int			first_qe_processed_field;
	List	   *qd_attnumlist;
	List	   *qe_attnumlist;
	bool		stopped_processing_at_delim;

	Node	   *whereClause;	/* WHERE condition (or NULL) */

	struct PartitionNode *partitions;
	List	   *ao_segnos;
	bool		skip_ext_partition;
	bool		skip_foreign_partitions;

	bool		on_segment;
	bool		ignore_extra_line;
	ProgramPipes *program_pipes;

	struct CdbCopy *cdbCopy;

	bool		delim_off;		/* delimiter is set to OFF? */
} CopyStateData;

typedef CopyStateData *CopyState;


/* CopyFormatOptions: PG14 type alias for compatibility */
typedef CopyStateData CopyFormatOptions;

/* DestReceiver for COPY (query) TO */
typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	CopyState	cstate;			/* CopyStateData for the command */
	QueryDesc  *queryDesc;		/* QueryDesc for the copy */
	uint64		processed;		/* # of tuples processed */
} DR_copy;

#ifndef htonll
#define htonll(x) ((1==htonl(1)) ? (x) : ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#endif
#ifndef ntohll
#define ntohll(x) ((1==ntohl(1)) ? (x) : ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#endif

/* These are private in commands/copy[from|to].c */
typedef struct CopyFromStateData *CopyFromState;
typedef struct CopyToStateData *CopyToState;

extern void DoCopy(ParseState *state, const CopyStmt *stmt,
				   int stmt_location, int stmt_len,
				   uint64 *processed);

extern void ProcessCopyOptions(ParseState *pstate, CopyFormatOptions *ops_out, bool is_from, List *options);

extern CopyState BeginCopy(ParseState *pstate, bool is_from, Relation rel,
						   RawStmt *raw_query, Oid queryRelId,
						   List *attnamelist, List *options,
						   TupleDesc tupDesc);
extern CopyState BeginCopyToOnSegment(QueryDesc *queryDesc);
extern void EndCopyToOnSegment(CopyState cstate);
extern CopyState BeginCopyToForeignTable(Relation forrel, List *options);
extern CopyState BeginCopyFrom(ParseState *pstate, Relation rel,
							   const char *filename,
							   bool is_program, copy_data_source_cb data_source_cb,
							   void *data_source_cb_extra,
							   List *attnamelist, List *options);
extern void EndCopyFrom(CopyState cstate);
extern bool NextCopyFrom(CopyState cstate, ExprContext *econtext,
						 Datum *values, bool *nulls);
extern bool NextCopyFromRawFields(CopyState cstate,
								  char ***fields, int *nfields);
extern void CopyFromErrorCallback(void *arg);

extern uint64 CopyFrom(CopyState cstate);

extern DestReceiver *CreateCopyDestReceiver(void);

extern List *CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist);

extern void CopyOneRowTo(CopyState cstate, TupleTableSlot *slot);
extern void CopyOneCustomRowTo(CopyState cstate, bytea *value);
extern void CopySendEndOfRow(CopyState cstate);
extern char *limit_printout_length(const char *str);
extern void truncateEol(StringInfo buf, EolType	eol_type);
extern void truncateEolStr(char *str, EolType eol_type);

/*
 * This is used to hold information about the target's distribution policy,
 * during COPY FROM.
 */
typedef struct GpDistributionData
{
	GpPolicy   *policy;		/* partitioning policy for this table */
	CdbHash	   *cdbHash;	/* corresponding CdbHash object */
} GpDistributionData;
/*
 * internal prototypes
 */
extern CopyState BeginCopyTo(ParseState *pstate, Relation rel, RawStmt *query,
							 Oid queryRelId, const char *filename, bool is_program,
							 List *attnamelist, List *options);
extern void EndCopyTo(CopyState cstate, uint64 *processed);
extern uint64 DoCopyTo(CopyState cstate);
extern List *CopyGetAttnums(TupleDesc tupDesc, Relation rel,
							List *attnamelist);

#endif							/* COPY_H */
