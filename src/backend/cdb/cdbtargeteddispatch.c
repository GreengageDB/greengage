/*-------------------------------------------------------------------------
 *
 * cdbtargeteddispatch.c
 *
 * Portions Copyright (c) 2009, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbtargeteddispatch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "cdb/cdbtargeteddispatch.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"	/* for rt_fetch() */
#include "nodes/makefuncs.h"	/* for makeVar() */
#include "utils/relcache.h"		/* RelationGetPartitioningKey() */
#include "optimizer/optimizer.h"
#include "optimizer/predtest_valueset.h"
#include "optimizer/restrictinfo.h"

#include "catalog/gp_distribution_policy.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"

#include "catalog/pg_proc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "cdb/cdbdisp_query.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbmutate.h"
#include "cdb/cdbpathlocus.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"

#include "executor/executor.h"
#include "commands/defrem.h"

#define PRINT_DISPATCH_DECISIONS_STRING ("print_dispatch_decisions")

/**
 *  The attributes of GpSegmentId,
 *  you can find its static definition in heap.c (static FormData_pg_attribute a8;)
 */
#define GP_SEGMENTID_TYPID INT4OID
#define GP_SEGMENTID_TYPMOD -1
#define GP_SEGMENTID_TYPCOLL 0
#define GP_SEGMENTID_OPFAMILY 1977	/* integer_ops */

/* PRINT_DISPATCH_DECISIONS_STRING; */

/**
 * Used when building up all of the composite distribution keys.
 */
typedef struct PartitionKeyInfo
{
	Form_pg_attribute attr;
	Node	  **values;
	int			counter;
	int			numValues;
} PartitionKeyInfo;

/**
 * Initialize a DirectDispatchCalculationInfo.
 */
static void
InitDirectDispatchCalculationInfo(DirectDispatchInfo *data)
{
	data->isDirectDispatch = false;
	data->contentIds = NULL;
	data->haveProcessedAnyCalculations = false;
}

/**
 * Mark a DirectDispatchCalculationInfo as having targeted dispatch disabled
 */
static void
DisableTargetedDispatch(DirectDispatchInfo *data)
{
	data->isDirectDispatch = false;
	data->contentIds = NULL;
	data->haveProcessedAnyCalculations = true;
}

/**
 * helper function for AssignContentIdsFromUpdateDeleteQualification
 */
static DirectDispatchInfo
GetContentIdsFromPlanForSingleRelation(PlannerInfo *root, Plan *plan, int rangeTableIndex, List *qualification)
{
	GpPolicy   *policy = NULL;
	PartitionKeyInfo *parts = NULL;
	int			i;

	DirectDispatchInfo result;
	RangeTblEntry *rte;
	Relation	relation = NULL;

	InitDirectDispatchCalculationInfo(&result);

	if (nodeTag((Node *) plan) == T_BitmapHeapScan)
	{
		/*
		 * do not assert for bitmap heap scan --> it can have a child which is
		 * an index scan
		 */
		/*
		 * in fact, checking the quals for the bitmap heap scan are redundant
		 * with checking them on the child scan.  But it won't cause any harm
		 * since we will recurse to the child scan.
		 */
	}
	else if (!IsA(plan, CustomScan))
	{
		/* A custom scan may keep children in its trees too. */
		Assert(plan->lefttree == NULL);
	}
	Assert(plan->righttree == NULL || IsA(plan, CustomScan));

	/* open and get relation info */
	rte = planner_rt_fetch(rangeTableIndex, root);
	if (rte->rtekind == RTE_RELATION)
	{
		/* Get a copy of the rel's GpPolicy from the relcache. */
		relation = relation_open(rte->relid, NoLock);
		policy = relation->rd_cdbpolicy;

		if (policy != NULL)
		{
			parts = (PartitionKeyInfo *) palloc(policy->nattrs * sizeof(PartitionKeyInfo));
			for (i = 0; i < policy->nattrs; i++)
			{
				parts[i].attr = TupleDescAttr(relation->rd_att, policy->attrs[i] - 1);
				parts[i].values = NULL;
				parts[i].numValues = 0;
				parts[i].counter = 0;
			}
		}
	}
	else
	{
		/* fall through, policy will be NULL so we won't direct dispatch */
	}

	if (rte->forceDistRandom ||	policy == NULL)
	{
		/*  we won't direct dispatch  */
		if (rte->rtekind == RTE_RELATION)
			relation_close(relation, NoLock);
		result.haveProcessedAnyCalculations = true;
		return result;
	}

	/*
	 * We first test if the predict contains a qual like
	 * gp_segment_id = some_const. This is very suitable
	 * for a direct dispatch. If this leads to direct dispatch
	 * then we just return because it does not need to
	 * eval distkey.
	 */
	if (GpPolicyIsPartitioned(policy))
	{
		PossibleValueSet    pvs_segids;
		Node              **seg_ids;
		int                 len;
		int                 i;
		List               *contentIds = NULL;

		/**
		 * In previous code, we get the attributes of GpSegmentId dynamically:
		 * 	get_atttypetypmodcoll(rte->relid, GpSegmentIdAttributeNumber,
		 *					      &vartypeid, &type_mod, &type_coll);
		 *  Oid opclass = GetDefaultOpClass(vartypeid, HASH_AM_OID);
		 *	Oid opfamily = get_opclass_family(opclass);
		 *
		 * But they caused the perf regression of pgbench. After test, the bottleneck is
		 * the indexscan on systable.
		 *
		 * So, let's simply introduce GP_SEGMENTID_XXX macros to hardcord them here
		 * since GpSegmentId is a stable type in GP: you can find its static definition
		 * in heap.c (static FormData_pg_attribute a8;)
		 */
		Var *seg_id_var = makeVar(rangeTableIndex,
								  GpSegmentIdAttributeNumber,
								  GP_SEGMENTID_TYPID, GP_SEGMENTID_TYPMOD, GP_SEGMENTID_TYPCOLL,
								  0);
		Oid opfamily = GP_SEGMENTID_OPFAMILY;

		pvs_segids = DeterminePossibleValueSet((Node *) qualification,
											   (Node *) seg_id_var, opfamily);
		if (!pvs_segids.isAnyValuePossible)
		{
			seg_ids = GetPossibleValuesAsArray(&pvs_segids, &len);
			if (len > 0 && len < policy->numsegments)
			{
				result.isDirectDispatch = true;
				for (i = 0; i < len; i++)
				{
					Node *val = seg_ids[i];
					if (IsA(val, Const))
					{
						int32 segid = DatumGetInt32(((Const *) val)->constvalue);
						contentIds = list_append_unique_int(contentIds, segid);
					}
					else
					{
						result.isDirectDispatch = false;
						break;
					}
				}
			}
		}

		DeletePossibleValueSetData(&pvs_segids);
		if (result.isDirectDispatch)
		{
			result.contentIds = contentIds;
			result.haveProcessedAnyCalculations = true;
			if (rte->rtekind == RTE_RELATION)
				relation_close(relation, NoLock);
			return result;
		}
	}

	if (GpPolicyIsHashPartitioned(policy))
	{
		long		totalCombinations = 1;

		Assert(parts != NULL);

		/* calculate possible value set for each partitioning attribute */
		for (i = 0; i < policy->nattrs; i++)
		{
			Var		   *var;
			PossibleValueSet pvs;
			Oid policy_opclass = policy->opclasses[i];
			Oid policy_opfamily = get_opclass_family(policy_opclass);

			var = makeVar(rangeTableIndex,
						  policy->attrs[i],
						  parts[i].attr->atttypid,
						  parts[i].attr->atttypmod,
						  parts[i].attr->attcollation,
						  0);
			/**
			 * Note that right now we only examine the given qual.  This is okay because if there are other
			 *   quals on the plan then those would be ANDed with the qual, which can only narrow our choice
			 *   of segment and not expand it.
			 */
			pvs = DeterminePossibleValueSet((Node *) qualification, (Node *) var, policy_opfamily);

			if (pvs.isAnyValuePossible)
			{
				/*
				 * can't isolate to single statement -- totalCombinations = -1
				 * will signal this
				 */
				DeletePossibleValueSetData(&pvs);
				totalCombinations = -1;
				break;
			}
			else
			{
				parts[i].values = GetPossibleValuesAsArray(&pvs, &parts[i].numValues);
				totalCombinations *= parts[i].numValues;
				DeletePossibleValueSetData(&pvs);
			}
		}

		/*
		 * calculate possible target content ids from the combinations of
		 * partitioning attributes
		 */
		if (totalCombinations == 0)
		{
			/*
			 * one of the possible sets was empty and so we don't care where
			 * we run this
			 */
			result.isDirectDispatch = true;
			Assert(result.contentIds == NULL);		/* direct dispatch but no
													 * specific content at
													 * all! */
		}
		else if (totalCombinations > 0 &&
			/* don't bother for ones which will likely hash to many segments */
				 totalCombinations < policy->numsegments * 3)
		{
			CdbHash    *h;
			long		index;

			h = makeCdbHashForRelation(relation);

			result.isDirectDispatch = true;
			result.contentIds = NULL;

			/* for each combination of keys calculate target segment */
			for (index = 0; index < totalCombinations; index++)
			{
				long		curIndex = index;
				int			hashCode;

				/* hash the attribute values */
				cdbhashinit(h);

				for (i = 0; i < policy->nattrs; i++)
				{
					int			numValues = parts[i].numValues;
					int			which = curIndex % numValues;
					Node	   *val = parts[i].values[which];

					if (IsA(val, Const))
					{
						Const		*c = (Const *) val;

						cdbhash(h, i + 1, c->constvalue, c->constisnull);
					}
					else
					{
						result.isDirectDispatch = false;
						break;
					}
					curIndex /= numValues;
				}

				if (!result.isDirectDispatch)
					break;

				hashCode = cdbhashreduce(h);

				result.contentIds = list_append_unique_int(result.contentIds, hashCode);
			}
		}
		else
		{
			/* know nothing, can't do directed dispatch */
			result.isDirectDispatch = false;
		}
	}

	if (rte->rtekind == RTE_RELATION)
	{
		relation_close(relation, NoLock);
	}

	result.haveProcessedAnyCalculations = true;
	return result;
}

void
MergeDirectDispatchCalculationInfo(DirectDispatchInfo *to, DirectDispatchInfo *from)
{
	if (!from->isDirectDispatch)
	{
		/* from eliminates all options so take it */
		to->isDirectDispatch = false;
	}
	else if (!to->haveProcessedAnyCalculations)
	{
		/* to has no data, so just take from */
		*to = *from;
	}
	else if (!to->isDirectDispatch)
	{
		/* to cannot get better -- leave it alone */
	}
	else if (from->contentIds == NULL)
	{
		/* from says that it doesn't need to run anywhere -- so we accept to */
	}
	else if (to->contentIds == NULL)
	{
		/* to didn't even think it needed to run so accept from */
		to->contentIds = from->contentIds;
	}
	else
	{
		/* union to with from */
		to->contentIds = list_union_int(to->contentIds, from->contentIds);
	}

	to->haveProcessedAnyCalculations = true;
}

void
DirectDispatchUpdateContentIdsFromPlan(PlannerInfo *root, Plan *plan)
{
	DirectDispatchInfo dispatchInfo;

	InitDirectDispatchCalculationInfo(&dispatchInfo);

	switch (nodeTag(plan))
	{
		case T_Result:
		case T_Append:
		case T_MergeAppend:
		case T_LockRows:
		case T_ModifyTable:
		case T_BitmapAnd:
		case T_BitmapOr:
		case T_BitmapHeapScan:
			/* no change to dispatchInfo */
			break;
		case T_SampleScan:
			DisableTargetedDispatch(&dispatchInfo);
			break;
		case T_SeqScan:
			/*
			 * we can determine the dispatch data to merge by looking at
			 * the relation begin scanned
			 */
			dispatchInfo = GetContentIdsFromPlanForSingleRelation(root,
																  plan,
																  ((Scan *) plan)->scanrelid,
																  plan->qual);
			break;

		case T_IndexScan:
			{
				IndexScan  *indexScan = (IndexScan *) plan;

				/*
				 * we can determine the dispatch data to merge by looking
				 * at the relation begin scanned
				 */
				dispatchInfo = GetContentIdsFromPlanForSingleRelation(root,
																	  plan,
																	  ((Scan *) plan)->scanrelid,
																	  indexScan->indexqualorig);
				/* must use _orig_ qual ! */
			}
			break;

		case T_IndexOnlyScan:
			{
				IndexOnlyScan  *indexOnlyScan = (IndexOnlyScan *) plan;

				/*
				 * we can determine the dispatch data to merge by looking
				 * at the relation begin scanned
				 */
				dispatchInfo = GetContentIdsFromPlanForSingleRelation(root,
																	  plan,
																	  ((Scan *) plan)->scanrelid,
																	  indexOnlyScan->recheckqual);
				/* must use _orig_ qual ! Upstream's recheckqual is the orig qual, just use it. */
			}
			break;

		case T_BitmapIndexScan:
			{
				BitmapIndexScan *bitmapScan = (BitmapIndexScan *) plan;

				/*
				 * we can determine the dispatch data to merge by looking
				 * at the relation begin scanned
				 */
				dispatchInfo = GetContentIdsFromPlanForSingleRelation(root,
																	  plan,
																	  ((Scan *) plan)->scanrelid,
																	  bitmapScan->indexqualorig);
				/* must use original qual ! */
			}
			break;
		case T_SubqueryScan:
		case T_NamedTuplestoreScan:
			/* no change to dispatchInfo */
			break;
		case T_TidScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_WorkTableScan:
			DisableTargetedDispatch(&dispatchInfo);
			break;
		case T_ValuesScan:
			/* no change to dispatchInfo */
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* join: no change to dispatchInfo */

			/*
			 * note that we could want to look at the join qual but
			 * constant checks should have been pushed down to the
			 * underlying scans so we shouldn't learn anything
			 */
			break;
		case T_Material:
		case T_Sort:
		case T_Agg:
		case T_TupleSplit:
		case T_Unique:
		case T_Gather:
		case T_Hash:
		case T_SetOp:
		case T_Limit:
		case T_PartitionSelector:
			break;
		case T_Motion:
			/*
			 * It's currently not allowed to direct-dispatch a slice that
			 * has a Motion that sends tuples to it. It would be possible
			 * in principle, but the interconnect initialization code gets
			 * confused.
			 */
			DisableTargetedDispatch(&dispatchInfo);
			break;

		case T_ShareInputScan:

			/*
			 * note: could try to peek into the building slice to get its
			 * direct dispatch values but we don't
			 */
			DisableTargetedDispatch(&dispatchInfo);
			break;
		case T_WindowAgg:
		case T_TableFunctionScan:
		case T_RecursiveUnion:
			/* no change to dispatchInfo */
			break;
		case T_ForeignScan:
			DisableTargetedDispatch(&dispatchInfo); /* not sure about
													 * foreign tables ...
													 * so disable */
			break;
		case T_CustomScan:

			/*
			 * Only the custom path tells what the scan reads: see
			 * DirectDispatchUpdateContentIdsFromCustomPath.
			 */
			DisableTargetedDispatch(&dispatchInfo);
			break;
		case T_SplitUpdate:
			break;
		default:
			elog(ERROR, "unknown plan node %d", nodeTag(plan));
			break;
	}

	/*
	 * analyzed node type, now do the work (for all except subquery scan,
	 * which do work in the switch above and return
	 */
	if (dispatchInfo.haveProcessedAnyCalculations)
	{
		/* learned new info: merge it in */
		MergeDirectDispatchCalculationInfo(&root->curSlice->directDispatch, &dispatchInfo);
	}
}

/*
 * The relations whose rows a rel holds. An upper rel lists none of its own:
 * it holds rows of every base relation of the query level. They are
 * collected here, as root->all_baserels stays empty for a set operation.
 */
static Relids
RelidsHeldBy(PlannerInfo *root, RelOptInfo *rel)
{
	Relids		relids = NULL;
	int			i;

	if (!IS_UPPER_REL(rel) || !bms_is_empty(rel->relids))
		return rel->relids;

	for (i = 1; i < root->simple_rel_array_size; i++)
	{
		RelOptInfo *base = root->simple_rel_array[i];

		if (base != NULL && base->reloptkind == RELOPT_BASEREL)
			relids = bms_add_member(relids, i);
	}

	return relids;
}

/*
 * The relations whose rows a child path brings. A path of an upper rel that
 * lists no relations brings what its inputs bring, which here a custom path
 * or a projection tells; of any other such path the core cannot tell, so it
 * counts as bringing none.
 */
static Relids
RelidsBroughtBy(Path *path)
{
	Relids		relids = NULL;
	ListCell   *lc;

	if (!IS_UPPER_REL(path->parent) || !bms_is_empty(path->parent->relids))
		return path->parent->relids;

	if (IsA(path, ProjectionPath))
		return RelidsBroughtBy(((ProjectionPath *) path)->subpath);
	if (IsA(path, CustomPath))
	{
		foreach(lc, ((CustomPath *) path)->custom_paths)
			relids = bms_add_members(relids, RelidsBroughtBy((Path *) lfirst(lc)));
	}

	return relids;
}

/*
 * Whether the rows of a relation reach a custom scan through its children:
 * a child holds the relation, or, for the parent of an appendrel (a
 * partitioned table, an inheritance tree, a UNION ALL), the children reach
 * every member the planner kept.
 */
static bool
RelidReachedByChildren(PlannerInfo *root, Index relid, Relids children)
{
	bool		members = false;
	ListCell   *lc;

	if (bms_is_member(relid, children))
		return true;

	foreach(lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, lc);
		RelOptInfo *member;

		if (appinfo->parent_relid != relid)
			continue;
		member = root->simple_rel_array[appinfo->child_relid];
		if (member == NULL || IS_DUMMY_REL(member))
			continue;
		if (!RelidReachedByChildren(root, appinfo->child_relid, children))
			return false;
		members = true;
	}

	return members;
}

/*
 * Targeted dispatch for a custom scan, from its path: the plan alone does
 * not tell which relations the provider reads itself.
 */
void
DirectDispatchUpdateContentIdsFromCustomPath(PlannerInfo *root,
											 CustomPath *path, Plan *plan)
{
	DirectDispatchInfo dispatchInfo;
	RelOptInfo *rel = path->path.parent;
	Index		scanrelid = ((Scan *) plan)->scanrelid;
	RangeTblEntry *rte = scanrelid > 0 ? planner_rt_fetch(scanrelid, root) : NULL;

	InitDirectDispatchCalculationInfo(&dispatchInfo);

	if (rte != NULL && rel->relid == scanrelid &&
		rte->rtekind == RTE_RELATION && !rte->inh &&
		(rte->relkind == RELKIND_RELATION ||
		 rte->relkind == RELKIND_MATVIEW))
	{
		/*
		 * A scan of a table returns only rows that pass the table's
		 * restrictions, wherever the provider tests them, so its segments
		 * follow from them as for a Seq Scan, where its rows lie as the
		 * table's do. A provider that reads them from elsewhere, laid out
		 * otherwise, says so in the path's locus. A Strewn locus, as of a
		 * randomly distributed table, cannot tell, so the scan of such a
		 * table goes to every segment. Not so for the parent of an
		 * inheritance tree or a partitioned table: its members may be
		 * distributed otherwise, so its children must reach them.
		 */
		if (cdbpathlocus_equal(path->path.locus,
							   cdbpathlocus_from_baserel(root, rel)))
			dispatchInfo = GetContentIdsFromPlanForSingleRelation(root, plan, scanrelid,
																  extract_actual_clauses(rel->baserestrictinfo,
																						 false));
		else
			DisableTargetedDispatch(&dispatchInfo);
	}
	else if (scanrelid > 0 && rel->relid != scanrelid)
	{
		/*
		 * A scan of a join or an upper rel that reads a table itself
		 * (scanrelid), which PostgreSQL expects only of a scan of that
		 * table. Following its children could leave out the table's rows
		 * on other segments, whatever the children reach.
		 */
		DisableTargetedDispatch(&dispatchInfo);
	}
	else
	{
		Relids		needed = RelidsHeldBy(root, rel);
		Relids		children = NULL;
		int			relid = -1;
		ListCell   *lc;

		foreach(lc, path->custom_paths)
			children = bms_add_members(children, RelidsBroughtBy((Path *) lfirst(lc)));

		/*
		 * The children set the segments of the rows they bring. A relation
		 * they do not reach, the provider reads itself, from segments the
		 * core cannot tell: dispatch to every segment.
		 */
		if (path->custom_paths == NIL)
			DisableTargetedDispatch(&dispatchInfo);
		while ((relid = bms_next_member(needed, relid)) >= 0)
		{
			if (!RelidReachedByChildren(root, relid, children))
			{
				DisableTargetedDispatch(&dispatchInfo);
				break;
			}
		}
	}

	if (dispatchInfo.haveProcessedAnyCalculations)
		MergeDirectDispatchCalculationInfo(&root->curSlice->directDispatch, &dispatchInfo);
}

void
DirectDispatchUpdateContentIdsForInsert(PlannerInfo *root, Plan *plan,
										GpPolicy *targetPolicy, Oid *hashfuncs)
{
	DirectDispatchInfo dispatchInfo;
	int			i;
	ListCell   *cell = NULL;
	bool		isDirectDispatch;
	Datum	   *values;
	bool	   *nulls;

	InitDirectDispatchCalculationInfo(&dispatchInfo);

	values = (Datum *) palloc(targetPolicy->nattrs * sizeof(Datum));
	nulls = (bool *) palloc(targetPolicy->nattrs * sizeof(bool));

	/*
	 * the nested loops here seem scary -- especially since we've already
	 * walked them before -- but I think this is still required since they may
	 * not be in the same order. (also typically we don't distribute by more
	 * than a handful of attributes).
	 */
	isDirectDispatch = true;
	for (i = 0; i < targetPolicy->nattrs; i++)
	{
		foreach(cell, plan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(cell);
			Const	   *c;

			Assert(tle->expr);

			if (tle->resno != targetPolicy->attrs[i])
				continue;

			if (!IsA(tle->expr, Const))
			{
				/* the planner could not simplify this */
				isDirectDispatch = false;
				break;
			}

			c = (Const *) tle->expr;

			values[i] = c->constvalue;
			nulls[i] = c->constisnull;
			break;
		}

		if (!isDirectDispatch)
			break;
	}

	if (isDirectDispatch)
	{
		uint32		hashcode;
		CdbHash    *h;

		h = makeCdbHash(targetPolicy->numsegments, targetPolicy->nattrs, hashfuncs);

		cdbhashinit(h);
		for (i = 0; i < targetPolicy->nattrs; i++)
		{
			cdbhash(h, i + 1, values[i], nulls[i]);
		}

		/* We now have the hash-partition that this row belong to */
		hashcode = cdbhashreduce(h);
		dispatchInfo.isDirectDispatch = true;
		dispatchInfo.contentIds = list_make1_int(hashcode);
		dispatchInfo.haveProcessedAnyCalculations = true;

		/* learned new info: merge it in */
		MergeDirectDispatchCalculationInfo(&root->curSlice->directDispatch, &dispatchInfo);

		elog(DEBUG1, "sending single row constant insert to content %d", hashcode);
	}
	pfree(values);
	pfree(nulls);
}
