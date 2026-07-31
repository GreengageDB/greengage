/*-------------------------------------------------------------------------
 *
 * copyfrom.c
 *		COPY <table> FROM file/program/client
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyfrom.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "commands/progress.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "libpq-fe.h"

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

static const char QDtoQESignature[] = "PGCOPY-QD-TO-QE\n\377\r\n";

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
	CopyFromState	cstate;			/* Copy state for this CopyMultiInsertInfo */
	EState	   *estate;			/* Executor state used for COPY */
	CommandId	mycid;			/* Command Id used for COPY */
	int			ti_options;		/* table insert options */
} CopyMultiInsertInfo;


/*
 * Testing GUC: When enabled, COPY FROM prints an INFO line to indicate which
 * fields are processed in the QD, and which in the QE.
 */
extern bool Test_copy_qd_qe_split;

/* non-export function prototypes */
char *limit_printout_length(const char *str);

static void ClosePipeFromProgram(CopyFromState cstate);

static void SendCopyFromForwardedTuple(CopyFromState cstate,
									   CdbCopy *cdbCopy,
									   bool toAll,
									   int target_seg,
									   Relation rel,
									   int64 lineno,
									   char *line,
									   int line_len,
									   Datum *values,
									   bool *nulls);
static void SendCopyFromForwardedHeader(CopyFromState cstate,
										CdbCopy *cdbCopy);
static GpDistributionData *InitDistributionData(CopyFromState cstate,
												EState *estate);
static void FreeDistributionData(GpDistributionData *distData);
static void InitCopyFromDispatchSplit(CopyFromState cstate,
									  GpDistributionData *distData,
									  EState *estate);
static unsigned int GetTargetSeg(GpDistributionData *distData,
								 TupleTableSlot *slot);

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyFromState.
 */
void
CopyFromErrorCallback(void *arg)
{
	CopyFromState	cstate = (CopyFromState) arg;
	char		curlineno_str[32];

	snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
			 cstate->cur_lineno);

	if (cstate->opts.binary)
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
				errcontext("COPY %s, line %s",
						   cstate->cur_relname, curlineno_str);
			}
		}
	}
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * Returns a pstrdup'd copy of the input.
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
						CopyFromState cstate, EState *estate, CommandId mycid,
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
	CopyFromState	cstate = miinfo->cstate;
	EState	   *estate = miinfo->estate;
	CommandId	mycid = miinfo->mycid;
	int			ti_options = miinfo->ti_options;
	bool		line_buf_valid = cstate->line_buf_valid;
	int			nused = buffer->nused;
	ResultRelInfo *resultRelInfo = buffer->resultRelInfo;
	TupleTableSlot **slots = buffer->slots;

	/*
	 * Print error context information correctly, if one of the operations
	 * below fail.
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
				ExecInsertIndexTuples(resultRelInfo,
									  buffer->slots[i], estate, false, false,
									  NULL, NIL);
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
	 * remove buffers starting with the ones we created first.  It seems less
	 * likely that these older ones will be needed than the ones that were
	 * just created.
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
 *
 * Note: 'miinfo' is unused but has been included for consistency with the
 * other functions in this area.
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
CopyFrom(CopyFromState cstate)
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
	Assert(list_length(cstate->range_table) == 1);

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

	/*
	 * If the target file is new-in-transaction, we assume that checking FSM
	 * for free space is a waste of time.  This could possibly be wrong, but
	 * it's unlikely.
	 */
	if (RELKIND_HAS_STORAGE(cstate->rel->rd_rel->relkind) &&
		(cstate->rel->rd_createSubid != InvalidSubTransactionId ||
		 cstate->rel->rd_firstRelfilenodeSubid != InvalidSubTransactionId))
		ti_options |= TABLE_INSERT_SKIP_FSM;

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
	if (cstate->opts.freeze)
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
	ExecInitRangeTable(estate, cstate->range_table);
	resultRelInfo = target_resultRelInfo = makeNode(ResultRelInfo);
	ExecInitResultRelation(estate, resultRelInfo, 1);

	/* Verify the named relation is a valid target for INSERT */
	CheckValidResultRel(resultRelInfo, CMD_INSERT);

	ExecOpenIndices(resultRelInfo, false);

	/*
	 * Set up a ModifyTableState so we can let FDW(s) init themselves for
	 * foreign-table result relation(s).
	 */
	mtstate = makeNode(ModifyTableState);
	mtstate->ps.plan = NULL;
	mtstate->ps.state = estate;
	mtstate->operation = CMD_INSERT;
	mtstate->resultRelInfo = resultRelInfo;
	mtstate->rootResultRelInfo = resultRelInfo;

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
	is_check_distkey = (cstate->opts.on_segment && Gp_role == GP_ROLE_EXECUTE && gp_enable_segment_copy_checking) ? true : false;

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
		cdbCopy = makeCdbCopy(cstate->rel->rd_cdbpolicy,
							  cstate->opts.on_segment, true);
		cstate->cdbCopy = cdbCopy;

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
		 * Skip header processing if dummy file get from master for COPY FROM ON
		 * SEGMENT
		 */
		if (!cstate->opts.on_segment)
		{
			SendCopyFromForwardedHeader(cstate, cdbCopy);
		}
	}

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

#if 0 /* GGDB: replaced by the dispatch_mode-aware calls below */
		/* Directly store the values/nulls array in the slot */
		if (!NextCopyFrom(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
			break;
#endif

		if (cstate->dispatch_mode == COPY_EXECUTOR)
		{
			if (!NextCopyFromExecute(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
				break;
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
			 * inconsistent with the order of the partition's columns and GPDB/PostgreSQL
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
			 * If we're capturing transition tuples, we might need to convert
			 * from the partition rowtype to root rowtype. But if there are no
			 * BEFORE triggers on the partition that could change the tuple,
			 * we can just remember the original unconverted tuple to avoid a
			 * needless round trip conversion.
			 */
			if (cstate->transition_capture != NULL)
				cstate->transition_capture->tcs_original_insert_tuple =
					!has_before_insert_row_trig ? myslot : NULL;

			/*
			 * We might need to convert from the root rowtype to the partition
			 * rowtype.
			 */
			map = resultRelInfo->ri_RootToPartitionMap;
			if (insertMethod == CIM_SINGLE || !leafpart_use_multi_insert)
			{
				/* non batch insert */
				if (map != NULL)
				{
					TupleTableSlot *new_slot;

					new_slot = resultRelInfo->ri_PartitionTupleSlot;
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
			 * on master.
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
					ExecComputeStoredGenerated(resultRelInfo, estate, myslot,
											   CMD_INSERT);

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
				if (resultRelInfo->ri_RelationDesc->rd_rel->relispartition &&
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
							recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
																   myslot,
																   estate,
																   false,
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
			 * for counting tuples inserted by an INSERT command.  Update
			 * progress of the COPY command as well.
			 *
			 * MPP: incrementing this counter here only matters for utility
			 * mode. in dispatch mode only the dispatcher COPY collects row
			 * count, so this counter is meaningless.
			 *
			 * Note that NextCopyFrom() also increments cdbsreh->processed on
			 * every successfully parsed row.  That covers the QD, which
			 * dispatches rows without inserting them and so never reaches
			 * this spot; the QE receives pre-parsed rows through
			 * NextCopyFromExecute() and counts them only here.
			 */
			pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
										 ++processed);
			if (cstate->cdbsreh)
				cstate->cdbsreh->processed++;
		}
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
			 * from the master and only consider the reject count from segments.
			 */
			if (IS_LOG_TO_FILE(cstate->cdbsreh->logerrors))
				total_rejected_from_qd = 0;

			total_rejected = total_rejected_from_qd + total_rejected_from_qes;

			ReportSrehResults(cstate->cdbsreh, total_rejected);
		}
	}

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

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (proute)
		ExecCleanupTupleRouting(mtstate, proute);

	/* Close the result relations, including any trigger target relations */
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);

	FreeDistributionData(distData);

	FreeExecutorState(estate);

	return processed;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'whereClause': WHERE clause from the COPY FROM command
 * 'filename': Name of server-local file to read, NULL for STDIN
 * 'is_program': true if 'filename' is program to execute
 * 'data_source_cb': callback that provides the input data
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyFromState, to be passed to NextCopyFrom and related functions.
 */
CopyFromState
BeginCopyFrom(ParseState *pstate,
			  Relation rel,
			  Node *whereClause,
			  const char *filename,
			  bool is_program,
			  copy_data_source_cb data_source_cb,
			  void *data_source_cb_extra,
			  List *attnamelist,
			  List *options)
{
	CopyFromState	cstate;
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
	bool		is_external_table;
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

	/* Allocate workspace and zero all fields */
	cstate = (CopyFromStateData *) palloc0(sizeof(CopyFromStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* GPDB needs this to detect custom protocol */
	if (rel)
		cstate->rel = rel;

	is_external_table = rel != NULL && rel_is_external_table(rel->rd_id);

	/* Extract options from the statement node tree */
	ProcessCopyOptions(pstate, &cstate->opts, true /* is_from */, options, is_external_table);

	if (cstate->opts.delim_off && !is_external_table)
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
	/*
	 * Determine the mode
	 */
	if (cstate->opts.on_segment || data_source_cb)
		cstate->dispatch_mode = COPY_DIRECT;
	else if (Gp_role == GP_ROLE_DISPATCH &&
			 cstate->rel && cstate->rel->rd_cdbpolicy &&
			 cstate->rel->rd_cdbpolicy->ptype != POLICYTYPE_ENTRY)
		cstate->dispatch_mode = COPY_DISPATCH;
	else if (Gp_role == GP_ROLE_EXECUTE)
		cstate->dispatch_mode = COPY_EXECUTOR;
	else
		cstate->dispatch_mode = COPY_DIRECT;

	/* Process the target relation */
	cstate->rel = rel;

	tupDesc = RelationGetDescr(cstate->rel);

	/* process commmon options or initialization */

	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
	cstate->opts.force_notnull_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_notnull)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_notnull);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NOT_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_notnull_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NULL name list to per-column flags, check validity */
	cstate->opts.force_null_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_null)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_null);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_null_flags[attnum - 1] = true;
		}
	}

	/* Convert convert_selectively name list to per-column flags */
	if (cstate->opts.convert_selectively)
	{
		List	   *attnums;
		ListCell   *cur;

		cstate->convert_select_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.convert_select);

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
	if (cstate->opts.file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();
	else
		cstate->file_encoding = cstate->opts.file_encoding;

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
		setEncodingConversionProc(&cstate->enc_conversion_proc,
								  cstate->file_encoding, false);
	}
	else
	{
		cstate->need_transcoding = false;
		cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
	}

	cstate->copy_src = COPY_FILE;	/* default */

	cstate->whereClause = whereClause;

	cstate->eol_type = cstate->opts.eol_type;

	MemoryContextSwitchTo(oldcontext);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Initialize state variables */
	cstate->reached_eof = false;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;

	/*
	 * Set up variables to avoid per-attribute overhead.  attribute_buf and
	 * raw_buf are used in both text and binary modes, but we use line_buf
	 * only in text mode.
	 */
	initStringInfo(&cstate->attribute_buf);
	cstate->raw_buf = (char *) palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;
	if (!cstate->opts.binary || cstate->dispatch_mode == COPY_EXECUTOR)
	{
		initStringInfo(&cstate->line_buf);
		cstate->line_buf_converted = false;
	}

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
		if (cstate->opts.binary)
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

	if (cstate->opts.on_segment && Gp_role == GP_ROLE_DISPATCH)
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
		cstate->copy_src = COPY_CALLBACK;
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

		if (cstate->opts.on_segment)
			MangleCopyFileName(&cstate->filename, cstate->cdbsreh);

		if (cstate->is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->program_pipes = open_program_pipes(cstate->filename, false);
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

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;
			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);
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
						 errmsg("\"%s\" is a directory", cstate->filename)));

			progress_vals[2] = st.st_size;
		}
	}

	pgstat_progress_update_multi_param(3, progress_cols, progress_vals);

	if (cstate->opts.on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* nothing to do */
	}
	else if (cstate->dispatch_mode == COPY_EXECUTOR && cstate->copy_src != COPY_CALLBACK)
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
	else if (cstate->opts.binary)
	{
		/* Read and verify binary header */
		ReceiveCopyBinaryHeader(cstate);
	}

	/* create workspace for CopyReadAttributes results */
	if (!cstate->opts.binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **) palloc(attr_count * sizeof(char *));
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Clean up storage and release resources for COPY FROM.
 */
void
EndCopyFrom(CopyFromState cstate)
{
	/* No COPY FROM related resources except memory. */
	if (cstate->is_program)
	{
		ClosePipeFromProgram(cstate);
	}
	else
	{
		if (cstate->filename != NULL && FreeFile(cstate->copy_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							cstate->filename)));
	}

	pgstat_progress_end_command();

	/* Clean up single row error handling related memory */
	if (cstate->cdbsreh)
		destroyCdbSreh(cstate->cdbsreh);

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Closes the pipe from an external program, checking the pclose() return code.
 */
static void
ClosePipeFromProgram(CopyFromState cstate)
{
	Assert(cstate->is_program);

#if 0 /* GGDB: replaced by the close_program_pipes() call below */
	int			pclose_rc;

	pclose_rc = ClosePipeStream(cstate->copy_file);
	if (pclose_rc == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
	else if (pclose_rc != 0)
	{
		/*
		 * If we ended a COPY FROM PROGRAM before reaching EOF, then it's
		 * expectable for the called program to fail with SIGPIPE, and we
		 * should not report that as an error.  Otherwise, SIGPIPE indicates a
		 * problem.
		 */
		if (!cstate->reached_eof &&
			wait_result_is_signal(pclose_rc, SIGPIPE))
			return;

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("program \"%s\" failed",
						cstate->filename),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
	}
#endif

	/*
	 * GGDB: the pipe was set up with open_program_pipes(), not upstream's
	 * OpenPipeStream(), so it must be torn down with close_program_pipes(),
	 * which fclose()s the stream, reaps the child and reports the program's
	 * stderr on failure.  Signal-terminated programs (including SIGPIPE
	 * when we stop reading before EOF) are tolerated there, as in the
	 * pre-split GGDB code.
	 */
	close_program_pipes(cstate->program_pipes, &cstate->copy_file, true);
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
SendCopyFromForwardedTuple(CopyFromState cstate,
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
SendCopyFromForwardedHeader(CopyFromState cstate, CdbCopy *cdbCopy)
{
	copy_from_dispatch_header header_frame;

	cdbCopySendDataToAll(cdbCopy, QDtoQESignature, sizeof(QDtoQESignature));

	memset(&header_frame, 0, sizeof(header_frame));
	header_frame.first_qe_processed_field = cstate->first_qe_processed_field;

	cdbCopySendDataToAll(cdbCopy, (char *) &header_frame, sizeof(header_frame));
}

void
SendCopyFromForwardedError(CopyFromState cstate, CdbCopy *cdbCopy, char *errormsg)
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

static GpDistributionData *
InitDistributionData(CopyFromState cstate, EState *estate)
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
InitCopyFromDispatchSplit(CopyFromState cstate, GpDistributionData *distData,
						  EState *estate)
{
	int			first_qe_processed_field = 0;
	Bitmapset  *needed_cols = NULL;
	ListCell   *lc;

	if (cstate->opts.binary)
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

		/*
		 * We also need every column referenced by the COPY WHERE clause,
		 * since it's evaluated against the row in the QD before the row
		 * is dispatched to the QE. Otherwise columns past the last one
		 * needed for distribution would still be unparsed (NULL) at that
		 * point, and the WHERE clause would spuriously exclude every row.
		 */
		if (cstate->whereClause)
		{
			Bitmapset  *where_cols = NULL;
			int			attnum;

			pull_varattnos(cstate->whereClause, 1, &where_cols);
			attnum = -1;
			while ((attnum = bms_next_member(where_cols, attnum)) >= 0)
				needed_cols = bms_add_member(needed_cols,
											 attnum + FirstLowInvalidHeapAttributeNumber);
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
