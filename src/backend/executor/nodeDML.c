/*-------------------------------------------------------------------------
 *
 * nodeDML.c
 *	  Implementation of nodeDML.
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/executor/nodeDML.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "cdb/cdbpartition.h"
#include "cdb/cdbvars.h"
#include "commands/tablecmds.h"
#include "executor/execDML.h"
#include "executor/instrument.h"
#include "executor/nodeDML.h"
#include "utils/memutils.h"

/* DML default memory */
#define DML_MEM 1

/*
 * Estimated Memory Usage of DML Node.
 * */
void
ExecDMLExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
	planstate->instrument->execmemused += DML_MEM;
}

/*
 * Executes INSERT and DELETE DML operations. The
 * action is specified within the TupleTableSlot at
 * plannode->actionColIdx.The ctid of the tuple to delete
 * is in position plannode->ctidColIdx in the current slot.
 * */
TupleTableSlot*
ExecDML(DMLState *node)
{

	PlanState *outerNode = outerPlanState(node);
	DML *plannode = (DML *) node->ps.plan;

	Assert(outerNode != NULL);

	TupleTableSlot *slot = ExecProcNode(outerNode);

	if (TupIsNull(slot))
	{
		return NULL;
	}

	bool isnull = false;
	int action = DatumGetUInt32(slot_getattr(slot, plannode->actionColIdx, &isnull));
	Assert(!isnull);

	bool isUpdate = false;
	if (node->ps.state->es_plannedstmt->commandType == CMD_UPDATE)
	{
		isUpdate = true;
	}

	Assert(action == DML_INSERT || action == DML_DELETE);


	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ExprContext *econtext = node->ps.ps_ExprContext;
	ResetExprContext(econtext);

	/* Prepare cleaned-up tuple by projecting it and filtering junk columns */
	econtext->ecxt_outertuple = slot;
	TupleTableSlot *projectedSlot = ExecProject(node->ps.ps_ProjInfo, NULL);

	/* remove 'junk' columns from tuple */
	node->cleanedUpSlot = ExecFilterJunk(node->junkfilter, projectedSlot);

	/*
	 * If we are modifying a leaf partition we have to ensure that partition
	 * selection operation will consider leaf partition's attributes as
	 * coherent with root partition's attribute numbers, because partition
	 * selection is performed using root's attribute numbers (all partition
	 * rules are based on the parent relation's tuple descriptor). In case
	 * when child partition has different attribute numbers from root's due to
	 * dropped columns, the partition selection may go wrong without extra
	 * validation.
	 */
	if (node->ps.state->es_result_partitions)
	{
		ResultRelInfo *relInfo = node->ps.state->es_result_relations;

		/*
		 * The DML is done on a leaf partition. In order to reuse the map,
		 * it will be allocated at es_result_relations.
		 */
		if (RelationGetRelid(relInfo->ri_RelationDesc) !=
			node->ps.state->es_result_partitions->part->parrelid &&
			action != DML_DELETE)
			makePartitionCheckMap(node->ps.state, relInfo);

		/*
		 * DML node always performs partition selection, and if we want to
		 * reuse the map built in makePartitionCheckMap, we are allowed to
		 * reassign es_result_relation_info, because ExecInsert, ExecDelete
		 * changes it with target partition anyway. Moreover, without
		 * inheritance plan (ORCA never builds such plans) the
		 * es_result_relations will contain the only relation.
		 */
		node->ps.state->es_result_relation_info = relInfo;
	}
	/* GPDB_91_MERGE_FIXME:
	 * This kind of node is used by ORCA only. If in the future ORCA still uses
	 * DML node, canSetTag should be saved in DML plan node and init-ed by
	 * copying canSetTag value from the parse tree.
	 *
	 * For !isUpdate case, ExecInsert() and ExecDelete() should use canSetTag
	 * value from parse tree, however for isUpdate case, it seems that
	 * ExecInsert() is run after ExecDelete() so canSetTag should be set
	 * properly in ExecInsert().
	 */
	if (DML_INSERT == action)
	{
		/* Respect any given tuple Oid when updating a tuple. */
		if (isUpdate && plannode->tupleoidColIdx != 0)
		{
			Oid			oid;
			HeapTuple	htuple;

			isnull = false;
			oid = slot_getattr(slot, plannode->tupleoidColIdx, &isnull);
			htuple = ExecFetchSlotHeapTuple(node->cleanedUpSlot);
			Assert(htuple == node->cleanedUpSlot->PRIVATE_tts_heaptuple);
			HeapTupleSetOid(htuple, oid);
		}

		/*
		 * The plan origin is required since ExecInsert performs different
		 * actions depending on the type of plan (constraint enforcement and
		 * triggers.)
		 */
		ExecInsert(node->cleanedUpSlot,
				   NULL,
				   node->ps.state,
				   true, /* GPDB_91_MERGE_FIXME: canSetTag, where to get this? */
				   PLANGEN_OPTIMIZER /* Plan origin */,
				   isUpdate,
				   InvalidOid);
	}
	else /* DML_DELETE */
	{
		int32 segid = GpIdentity.segindex;
		Datum ctid = slot_getattr(slot, plannode->ctidColIdx, &isnull);
		Oid tableoid = InvalidOid;

		Assert(!isnull);

		if (AttributeNumberIsValid(plannode->tableoidColIdx))
		{
			Datum dtableoid = slot_getattr(slot, plannode->tableoidColIdx, &isnull);
			tableoid = isnull ? InvalidOid : DatumGetObjectId(dtableoid);
		}

		/*
		 * If tableoid is valid, it means that we are executing UPDATE/DELETE
		 * on partitioned table (root partition). In order to avoid partition
		 * pruning in ExecDelete one can use tableoid to build target
		 * ResultRelInfo for the leaf partition.
		 */
		if (OidIsValid(tableoid) && node->ps.state->es_result_partitions)
			node->ps.state->es_result_relation_info =
				targetid_get_partition(tableoid, node->ps.state, true);

		ItemPointer  tupleid = (ItemPointer) DatumGetPointer(ctid);
		ItemPointerData tuple_ctid = *tupleid;
		tupleid = &tuple_ctid;

		if (AttributeNumberIsValid(node->segid_attno))
		{
			segid = DatumGetInt32(slot_getattr(slot, node->segid_attno, &isnull));
			Assert(!isnull);
		}

		/* Correct tuple count by ignoring deletes when splitting tuples. */
		ExecDelete(tupleid,
				   segid,
				   NULL, /* GPDB_91_MERGE_FIXME: oldTuple? */
				   node->cleanedUpSlot,
				   NULL /* DestReceiver */,
				   node->ps.state,
				   !isUpdate, /* GPDB_91_MERGE_FIXME: where to get canSetTag? */
				   PLANGEN_OPTIMIZER /* Plan origin */,
				   isUpdate);
	}

	return slot;
}

/**
 * Init nodeDML, which initializes the insert TupleTableSlot.
 * */
DMLState*
ExecInitDML(DML *node, EState *estate, int eflags)
{
	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));

	DMLState *dmlstate = makeNode(DMLState);
	dmlstate->ps.plan = (Plan *)node;
	dmlstate->ps.state = estate;
	/*
	 * Initialize es_result_relation_info, just like ModifyTable.
	 * GPDB_90_MERGE_FIXME: do we need to consolidate the ModifyTable and DML
	 * logic?
	 */
	estate->es_result_relation_info = estate->es_result_relations;

	CmdType operation = estate->es_plannedstmt->commandType;
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;

	ExecInitResultTupleSlot(estate, &dmlstate->ps);

	dmlstate->ps.targetlist = (List *)
						ExecInitExpr((Expr *) node->plan.targetlist,
						(PlanState *) dmlstate);

	Plan *outerPlan  = outerPlan(node);
	outerPlanState(dmlstate) = ExecInitNode(outerPlan, estate, eflags);

	/*
	 * ORCA Plan does not seem to set junk attribute for "gp_segment_id", else we
	 * could call the simple code below.
	 * resultRelInfo->ri_segid_attno = ExecFindJunkAttributeInTlist(outerPlanState(dmlstate)->plan->targetlist, "gp_segment_id");
	 */
	ListCell   *t;
	dmlstate->segid_attno = InvalidAttrNumber;
	foreach(t, outerPlanState(dmlstate)->plan->targetlist)
	{
		TargetEntry *tle = lfirst(t);

		if (tle->resname && (strcmp(tle->resname, "gp_segment_id") == 0))
		{
			dmlstate->segid_attno = tle->resno;
			break;
		}
	}

	ExecAssignResultTypeFromTL(&dmlstate->ps);

	/* Create expression evaluation context. This will be used for projections */
	ExecAssignExprContext(estate, &dmlstate->ps);

	/*
	 * Create projection info from the child tuple descriptor and our target list
	 * Projection will be placed in the ResultSlot
	 */
	TupleTableSlot *childResultSlot = outerPlanState(dmlstate)->ps_ResultTupleSlot;
	ExecAssignProjectionInfo(&dmlstate->ps, childResultSlot->tts_tupleDescriptor);

	/*
	 * Initialize slot to insert/delete using output relation descriptor.
	 */
	dmlstate->cleanedUpSlot = ExecInitExtraTupleSlot(estate);

	/*
	 * Both input and output of the junk filter include dropped attributes, so
	 * the junk filter doesn't need to do anything special there about them
	 */

	dmlstate->junkfilter = ExecInitJunkFilter(node->plan.targetlist,
			dmlstate->ps.state->es_result_relation_info->ri_RelationDesc->rd_att->tdhasoid,
			dmlstate->cleanedUpSlot);

	/*
	 * We don't maintain typmod in the targetlist, so we should fixup the
	 * junkfilter to use the same tuple descriptor as the result relation.
	 * Otherwise the mismatch of tuple descriptor will cause a break in
	 * ExecInsert()->reconstructMatchingTupleSlot().
	 */
	TupleDesc cleanTupType = CreateTupleDescCopy(dmlstate->ps.state->es_result_relation_info->ri_RelationDesc->rd_att);

	ExecSetSlotDescriptor(dmlstate->junkfilter->jf_resultSlot, cleanTupType);

	ReleaseTupleDesc(dmlstate->junkfilter->jf_cleanTupType);
	dmlstate->junkfilter->jf_cleanTupType = cleanTupType;

	if (estate->es_instrument && (estate->es_instrument & INSTRUMENT_CDB))
	{
	        dmlstate->ps.cdbexplainbuf = makeStringInfo();

	        /* Request a callback at end of query. */
	        dmlstate->ps.cdbexplainfun = ExecDMLExplainEnd;
	}

	/*
	 * If there are indices on the result relation, open them and save
	 * descriptors in the result relation info, so that we can add new index
	 * entries for the tuples we add/update.  We need not do this for a
	 * DELETE, however, since deletion doesn't affect indexes.
	 */
	if (Gp_role != GP_ROLE_EXECUTE || Gp_is_writer) /* only needed by the root slice who will do the actual updating */
	{
		if (resultRelInfo->ri_RelationDesc->rd_rel->relhasindex  &&
			operation != CMD_DELETE)
		{
			ExecOpenIndices(resultRelInfo);
		}
	}

	return dmlstate;
}

/* Release Resources Requested by nodeDML. */
void
ExecEndDML(DMLState *node)
{
	/* Release explicitly the TupleDesc for result relation */
	ReleaseTupleDesc(node->junkfilter->jf_cleanTupType);

	ExecFreeExprContext(&node->ps);
	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	ExecClearTuple(node->cleanedUpSlot);
	ExecEndNode(outerPlanState(node));
	EndPlanStateGpmonPkt(&node->ps);
}
/* EOF */
