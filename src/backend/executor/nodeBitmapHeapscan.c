/*-------------------------------------------------------------------------
 *
 * nodeBitmapHeapscan.c
 *	  Routines to support bitmapped scans of relations
 *
 * NOTE: it is critical that this plan type only be used with MVCC-compliant
 * snapshots (ie, regular snapshots, not SnapshotAny or one of the other
 * special snapshots).  The reason is that since index and heap scans are
 * decoupled, there can be no assurance that the index tuple prompting a
 * visit to a particular heap TID still exists when the visit is made.
 * Therefore the tuple might not exist anymore either (which is OK because
 * heap_fetch will cope) --- but worse, the tuple slot could have been
 * re-used for a newer tuple.  With an MVCC snapshot the newer tuple is
 * certain to fail the time qual and so it will not be mistakenly returned,
 * but with anything else we might return a tuple that doesn't meet the
 * required index qual conditions.
 *
 * In GPDB, this also deals with AppendOnly and AOCS tables. The prefetching
 * hasn't been implemented for them, though.
 *
 * This can also be used in "Dynamic" mode.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2008-2009, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBitmapHeapscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBitmapHeapScan			scans a relation using bitmap info
 *		ExecBitmapHeapNext			workhorse for above
 *		ExecInitBitmapHeapScan		creates and initializes state info.
 *		ExecReScanBitmapHeapScan	prepares to rescan the plan.
 *		ExecEndBitmapHeapScan		releases all storage.
 */
#include "postgres.h"

#include <math.h>

#include "access/relscan.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "executor/executor.h"
#include "executor/nodeBitmapHeapscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "parser/parsetree.h"
#include "nodes/tidbitmap.h"
#include "utils/rel.h"
#include "utils/spccache.h"

#include "cdb/cdbvars.h" /* gp_select_invisible */

static void BitmapTableScanSetup(BitmapHeapScanState *node);
static TupleTableSlot *BitmapHeapNext(BitmapHeapScanState *node);
static inline void BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate);
static bool BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate);
static void ExecEagerFreeBitmapHeapScan(BitmapHeapScanState *node);


/*
 * Free the state relevant to bitmaps.
 *
 * GPDB: this is used by ExecSquelchBitmapHeapScan() to release the bitmap and
 * its iterator eagerly (e.g. when a Motion above us stops pulling tuples),
 * before ExecEndBitmapHeapScan() runs.  In the PG18 table-AM model the actual
 * bitmap iteration lives in the scan descriptor (st.rs_tbmiterator), so we end
 * that here rather than the node-local iterator fields.
 */
static void
freeBitmapState(BitmapHeapScanState *scanstate)
{
	TableScanDesc scan = scanstate->ss.ss_currentScanDesc;

	/*
	 * End iteration on the iterator saved in the scan descriptor if it has not
	 * already been cleaned up.  tbm_end_iterate() is idempotent w.r.t.
	 * tbm_exhausted(), so ExecEndBitmapHeapScan() will not double-free.
	 */
	if (scan && !tbm_exhausted(&scan->st.rs_tbmiterator))
		tbm_end_iterate(&scan->st.rs_tbmiterator);

	/* The bitmap (TIDBitmap or StreamBitmap) is owned by us; release it. */
	if (scanstate->tbm)
		tbm_generic_free(scanstate->tbm);
	scanstate->tbm = NULL;
}

/*
 * Do the underlying index scan, build the bitmap, set up the parallel state
 * needed for parallel workers to iterate through the bitmap, and set up the
 * underlying table scan descriptor.
 */
static void
BitmapTableScanSetup(BitmapHeapScanState *node)
{
	TBMIterator tbmiterator = {0};
	ParallelBitmapHeapState *pstate = node->pstate;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	if (!pstate)
	{
		node->tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));

		if (!node->tbm || !IsA(node->tbm, TIDBitmap))
			elog(ERROR, "unrecognized result from subplan");
	}
	else if (BitmapShouldInitializeSharedState(pstate))
	{
		/*
		 * The leader will immediately come out of the function, but others
		 * will be blocked until leader populates the TBM and wakes them up.
		 */
		node->tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));
		if (!node->tbm || !IsA(node->tbm, TIDBitmap))
			elog(ERROR, "unrecognized result from subplan");

		/*
		 * Prepare to iterate over the TBM. This will return the dsa_pointer
		 * of the iterator state which will be used by multiple processes to
		 * iterate jointly.
		 */
		pstate->tbmiterator = tbm_prepare_shared_iterate(node->tbm);

		/* We have initialized the shared state so wake up others. */
		BitmapDoneInitializingSharedState(pstate);
	}

	tbmiterator = tbm_begin_iterate(node->tbm, dsa,
									pstate ?
									pstate->tbmiterator :
									InvalidDsaPointer);

	/*
	 * If this is the first scan of the underlying table, create the table
	 * scan descriptor and begin the scan.
	 */
	if (!node->ss.ss_currentScanDesc)
	{
		node->ss.ss_currentScanDesc =
			table_beginscan_bm(node->ss.ss_currentRelation,
							   node->ss.ps.state->es_snapshot,
							   0,
							   NULL);
	}

	node->ss.ss_currentScanDesc->st.rs_tbmiterator = tbmiterator;
	node->initialized = true;
}

/* ----------------------------------------------------------------
 *		BitmapHeapNext
 *
 *		Retrieve next tuple from the BitmapHeapScan node's currentRelation
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
BitmapHeapNext(BitmapHeapScanState *node)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * If we haven't yet performed the underlying index scan, do it, and begin
	 * the iteration over the bitmap.
	 */
	if (!node->initialized)
		BitmapTableScanSetup(node);

	while (table_scan_bitmap_next_tuple(node->ss.ss_currentScanDesc,
										slot, &node->recheck,
										&node->stats.lossy_pages,
										&node->stats.exact_pages))
	{
		/*
		 * Continuing in previously obtained page.
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * If we are using lossy info, we have to recheck the qual conditions
		 * at every tuple.
		 */
		if (node->recheck)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQualAndReset(node->bitmapqualorig, econtext))
			{
				/* Fails recheck, so drop it and loop back for another */
				InstrCountFiltered2(node, 1);
				ExecClearTuple(slot);
				continue;
			}
		}

		/* OK to return this tuple */
		return slot;
	}

	ExecEagerFreeBitmapHeapScan(node);

	/*
	 * if we get here it means we are at the end of the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 *	BitmapDoneInitializingSharedState - Shared state is initialized
 *
 *	By this time the leader has already populated the TBM and initialized the
 *	shared state so wake up other processes.
 */
static inline void
BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate)
{
	SpinLockAcquire(&pstate->mutex);
	pstate->state = BM_FINISHED;
	SpinLockRelease(&pstate->mutex);
	ConditionVariableBroadcast(&pstate->cv);
}

/*
 * BitmapHeapRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
BitmapHeapRecheck(BitmapHeapScanState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the original qual conditions? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->bitmapqualorig, econtext);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapHeapScan(PlanState *pstate)
{
	BitmapHeapScanState *node = castNode(BitmapHeapScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) BitmapHeapNext,
					(ExecScanRecheckMtd) BitmapHeapRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
void
ExecReScanBitmapHeapScan(BitmapHeapScanState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	TableScanDesc scan = node->ss.ss_currentScanDesc;

	if (scan)
	{
		/*
		 * End iteration on iterators saved in scan descriptor if they have
		 * not already been cleaned up.
		 */
		if (!tbm_exhausted(&scan->st.rs_tbmiterator))
			tbm_end_iterate(&scan->st.rs_tbmiterator);

		/* rescan to release any page pin */
		table_rescan(node->ss.ss_currentScanDesc, NULL);
	}

	/* release bitmaps and buffers if any */
	if (node->tbm)
		tbm_generic_free(node->tbm);
	node->tbm = NULL;
	node->initialized = false;
	node->recheck = true;

	ExecScanReScan(&node->ss);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapHeapScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapHeapScan(BitmapHeapScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * When ending a parallel worker, copy the statistics gathered by the
	 * worker back into shared memory so that it can be picked up by the main
	 * process to report in EXPLAIN ANALYZE.
	 */
	if (node->sinstrument != NULL && IsParallelWorker())
	{
		BitmapHeapScanInstrumentation *si;

		Assert(ParallelWorkerNumber <= node->sinstrument->num_workers);
		si = &node->sinstrument->sinstrument[ParallelWorkerNumber];

		/*
		 * Here we accumulate the stats rather than performing memcpy on
		 * node->stats into si.  When a Gather/GatherMerge node finishes it
		 * will perform planner shutdown on the workers.  On rescan it will
		 * spin up new workers which will have a new BitmapHeapScanState and
		 * zeroed stats.
		 */
		si->exact_pages += node->stats.exact_pages;
		si->lossy_pages += node->stats.lossy_pages;
	}

	/*
	 * extract information from the node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));

	if (scanDesc)
	{
		/*
		 * End iteration on iterators saved in scan descriptor if they have
		 * not already been cleaned up.
		 */
		if (!tbm_exhausted(&scanDesc->st.rs_tbmiterator))
			tbm_end_iterate(&scanDesc->st.rs_tbmiterator);

		/*
		 * close table scan
		 */
		table_endscan(scanDesc);
	}

	/*
	 * release bitmaps and buffers if any
	 */
	if (node->tbm)
		tbm_generic_free(node->tbm);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapHeapScan
 *
 *		Initializes the scan's state information.
 * ----------------------------------------------------------------
 */
BitmapHeapScanState *
ExecInitBitmapHeapScan(BitmapHeapScan *node, EState *estate, int eflags)
{
	Relation	currentRelation;
	BitmapHeapScanState *bhsState;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * open the scan relation
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	bhsState =  ExecInitBitmapHeapScanForPartition(node, estate, eflags,
												   currentRelation);

	/*
	 * initialize child nodes
	 *
	 * We do this last because the child nodes will open indexscans on our
	 * relation's indexes, and we want to be sure we have acquired a lock on
	 * the relation first.
	 */
	outerPlanState(bhsState) = ExecInitNode(outerPlan(node), estate, eflags);

	return bhsState;
}

BitmapHeapScanState *
ExecInitBitmapHeapScanForPartition(BitmapHeapScan *node, EState *estate, int eflags,
								   Relation currentRelation)
{
	BitmapHeapScanState *scanstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * Assert caller didn't ask for an unsafe snapshot --- see comments at
	 * head of file.
	 *
	 * MPP-4703: the MVCC-snapshot restriction is required for correct results.
	 * our test-mode may deliberately return incorrect results, but that's OK.
	 */
	Assert(IsMVCCSnapshot(estate->es_snapshot) || gp_select_invisible);

	/*
	 * create state structure
	 */
	scanstate = makeNode(BitmapHeapScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecBitmapHeapScan;

	scanstate->tbm = NULL;

	/* Zero the statistics counters */
	memset(&scanstate->stats, 0, sizeof(BitmapHeapScanInstrumentation));

	scanstate->initialized = false;
	scanstate->pstate = NULL;
	scanstate->recheck = true;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(currentRelation),
						  table_slot_callbacks(currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
	scanstate->bitmapqualorig =
		ExecInitQual(node->bitmapqualorig, (PlanState *) scanstate);

	scanstate->ss.ss_currentRelation = currentRelation;

	/*
	 * GPDB: For append-optimized (AO/AOCS) relations we begin the scan here
	 * (rather than lazily in BitmapHeapNext) so that the scan state is set up
	 * with the column info needed for AOCO relations. AO tables are not
	 * block/visibility-map based, so upstream's skip-fetch-in-AM logic (which
	 * keys on SO_NEED_TUPLES) does not apply to them. For heap relations we
	 * leave ss_currentScanDesc NULL and let BitmapHeapNext create the scan via
	 * table_beginscan_bm(), which sets SO_NEED_TUPLES correctly so the table AM
	 * can decide whether to skip heap fetches.
	 */
	if (RelationIsAppendOptimized(currentRelation))
		scanstate->ss.ss_currentScanDesc = table_beginscan_bm_ecs(currentRelation,
																  estate->es_snapshot,
																  node->scan.plan.targetlist,
																  node->scan.plan.qual,
																  node->bitmapqualorig);

	/*
	 * all done.
	 */
	return scanstate;
}

static void
ExecEagerFreeBitmapHeapScan(BitmapHeapScanState *node)
{
	freeBitmapState(node);
}

/*----------------
 *		BitmapShouldInitializeSharedState
 *
 *		The first process to come here and see the state to the BM_INITIAL
 *		will become the leader for the parallel bitmap scan and will be
 *		responsible for populating the TIDBitmap.  The other processes will
 *		be blocked by the condition variable until the leader wakes them up.
 * ---------------
 */
static bool
BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate)
{
	SharedBitmapState state;

	while (1)
	{
		SpinLockAcquire(&pstate->mutex);
		state = pstate->state;
		if (pstate->state == BM_INITIAL)
			pstate->state = BM_INPROGRESS;
		SpinLockRelease(&pstate->mutex);

		/* Exit if bitmap is done, or if we're the leader. */
		if (state != BM_INPROGRESS)
			break;

		/* Wait for the leader to wake us up. */
		ConditionVariableSleep(&pstate->cv, WAIT_EVENT_PARALLEL_BITMAP_SCAN);
	}

	ConditionVariableCancelSleep();

	return (state == BM_INITIAL);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapEstimate(BitmapHeapScanState *node,
					   ParallelContext *pcxt)
{
	Size		size;

	size = MAXALIGN(sizeof(ParallelBitmapHeapState));

	/* account for instrumentation, if required */
	if (node->ss.ps.instrument && pcxt->nworkers > 0)
	{
		size = add_size(size, offsetof(SharedBitmapHeapInstrumentation, sinstrument));
		size = add_size(size, mul_size(pcxt->nworkers, sizeof(BitmapHeapScanInstrumentation)));
	}

	shm_toc_estimate_chunk(&pcxt->estimator, size);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeDSM
 *
 *		Set up a parallel bitmap heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapInitializeDSM(BitmapHeapScanState *node,
							ParallelContext *pcxt)
{
	ParallelBitmapHeapState *pstate;
	SharedBitmapHeapInstrumentation *sinstrument = NULL;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;
	char	   *ptr;
	Size		size;

	/* If there's no DSA, there are no workers; initialize nothing. */
	if (dsa == NULL)
		return;

	size = MAXALIGN(sizeof(ParallelBitmapHeapState));
	if (node->ss.ps.instrument && pcxt->nworkers > 0)
	{
		size = add_size(size, offsetof(SharedBitmapHeapInstrumentation, sinstrument));
		size = add_size(size, mul_size(pcxt->nworkers, sizeof(BitmapHeapScanInstrumentation)));
	}

	ptr = shm_toc_allocate(pcxt->toc, size);
	pstate = (ParallelBitmapHeapState *) ptr;
	ptr += MAXALIGN(sizeof(ParallelBitmapHeapState));
	if (node->ss.ps.instrument && pcxt->nworkers > 0)
		sinstrument = (SharedBitmapHeapInstrumentation *) ptr;

	pstate->tbmiterator = 0;

	/* Initialize the mutex */
	SpinLockInit(&pstate->mutex);
	pstate->state = BM_INITIAL;

	ConditionVariableInit(&pstate->cv);

	if (sinstrument)
	{
		sinstrument->num_workers = pcxt->nworkers;

		/* ensure any unfilled slots will contain zeroes */
		memset(sinstrument->sinstrument, 0,
			   pcxt->nworkers * sizeof(BitmapHeapScanInstrumentation));
	}

	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pstate);
	node->pstate = pstate;
	node->sinstrument = sinstrument;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapReInitializeDSM(BitmapHeapScanState *node,
							  ParallelContext *pcxt)
{
	ParallelBitmapHeapState *pstate = node->pstate;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	/* If there's no DSA, there are no workers; do nothing. */
	if (dsa == NULL)
		return;

	pstate->state = BM_INITIAL;

	if (DsaPointerIsValid(pstate->tbmiterator))
		tbm_free_shared_area(dsa, pstate->tbmiterator);

	pstate->tbmiterator = InvalidDsaPointer;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapInitializeWorker(BitmapHeapScanState *node,
							   ParallelWorkerContext *pwcxt)
{
	char	   *ptr;

	Assert(node->ss.ps.state->es_query_dsa != NULL);

	ptr = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);

	node->pstate = (ParallelBitmapHeapState *) ptr;
	ptr += MAXALIGN(sizeof(ParallelBitmapHeapState));

	if (node->ss.ps.instrument)
		node->sinstrument = (SharedBitmapHeapInstrumentation *) ptr;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapRetrieveInstrumentation
 *
 *		Transfer bitmap heap scan statistics from DSM to private memory.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapRetrieveInstrumentation(BitmapHeapScanState *node)
{
	SharedBitmapHeapInstrumentation *sinstrument = node->sinstrument;
	Size		size;

	if (sinstrument == NULL)
		return;

	size = offsetof(SharedBitmapHeapInstrumentation, sinstrument)
		+ sinstrument->num_workers * sizeof(BitmapHeapScanInstrumentation);

	node->sinstrument = palloc(size);
	memcpy(node->sinstrument, sinstrument, size);
}

void
ExecSquelchBitmapHeapScan(BitmapHeapScanState *node)
{
	ExecEagerFreeBitmapHeapScan(node);
}
