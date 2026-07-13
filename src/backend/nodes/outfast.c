/*-------------------------------------------------------------------------
 *
 * outfast.c
 *	  Fast serialization functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * NOTES
 *	  Every node type that can appear in an Greenplum Database serialized query or plan
 *    tree must have an output function defined here.
 *
 * 	  There *MUST* be a one-to-one correspondence between this routine
 *    and readfast.c.  If not, you will likely crash the system.
 *
 *     By design, the only user of these routines is the function
 *     serializeNode in cdbsrlz.c.  Other callers beware.
 *
 *    Like readfast.c, this file borrows the definitions of most functions
 *    from outfuncs.c.
 *
 * 	  Rather than serialize to a (somewhat human-readable) string, these
 *    routines create a binary serialization via a simple depth-first walk
 *    of the tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "utils/datum.h"
#include "catalog/heap.h"
#include "cdb/cdbgang.h"
#include "utils/expandeddatum.h"
#include "utils/workfile_mgr.h"
#include "parser/parsetree.h"

/*
 * Macros to simplify output of different kinds of fields.	Use these
 * wherever possible to reduce the chance for silly typos.	Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/*
 * Write the label for the node type.  nodelabel is accepted for
 * compatibility with outfuncs.c, but is ignored
 */
#define WRITE_NODE_TYPE(nodelabel) \
	{ int16 nt =nodeTag(node); appendBinaryStringInfo(str, (const char *)&nt, sizeof(int16)); }

/* Write an integer field  */
#define WRITE_INT_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/* Write an integer field  */
#define WRITE_INT8_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int8)); }

/* Write an integer field  */
#define WRITE_INT16_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int16)); }

/* Write an unsigned integer field */
#define WRITE_UINT_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int))

/* Write an uint64 field */
#define WRITE_UINT64_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(uint64))

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(Oid))

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(long))

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	appendBinaryStringInfo(str, &node->fldname, 1)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	{ int16 en=node->fldname; appendBinaryStringInfo(str, (const char *)&en, sizeof(int16)); }

/* Write a float field --- the format is accepted but ignored (for compat with outfuncs.c)  */
#define WRITE_FLOAT_FIELD(fldname,format) \
	appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(double))

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	{ \
		char b = node->fldname ? 1 : 0; \
		appendBinaryStringInfo(str, (const char *)&b, 1); }

/* Write a character-string (possibly NULL) varable */
#define WRITE_STRING_VAR(var) \
	{ int slen = var != NULL ? strlen(var) : 0; \
		appendBinaryStringInfo(str, (const char *)&slen, sizeof(int)); \
		if (slen>0) appendBinaryStringInfo(str, var, slen);}

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname)  WRITE_STRING_VAR(node->fldname)

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	{ appendBinaryStringInfo(str, (const char *)&node->fldname, sizeof(int)); }

/*
 * Write a Node field
 *
 * If compiled with GP_SERIALIZATION_DEBUG, write the field name in the
 * serialized form, and check that it matches in the read function
 * (READ_NODE_FIELD in readfast.c). That makes it much easier to debug bugs
 * where the out and read functions are not in sync, as you get an error
 * much earlier, and it can print the field name where the mismatch occurred.
 * It makes the serialized plans much larger, though, so we don't want to do
 * it production.
 */
#ifdef GP_SERIALIZATION_DEBUG
#define WRITE_NODE_FIELD(fldname) \
	do { \
		const char *xx = CppAsString(fldname); \
		appendBinaryStringInfo(str, xx, strlen(xx) + 1); \
		(_outNode(str, node->fldname)); \
	} while (0)
#else
#define WRITE_NODE_FIELD(fldname) \
	(_outNode(str, node->fldname))
#endif


/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	 _outBitmapset(str, node->fldname)

/* Write a binary field */
#define WRITE_BINARY_FIELD(fldname, sz) \
{ appendBinaryStringInfo(str, (const char *) &node->fldname, (sz)); }

/* Write a bytea field */
#define WRITE_BYTEA_FIELD(fldname) \
	(_outDatum(str, PointerGetDatum(node->fldname), -1, false))

/* Write a dummy field -- value not displayable or copyable */
#define WRITE_DUMMY_FIELD(fldname) \
	{ /*int * dummy = 0; appendBinaryStringInfo(str,(const char *)&dummy, sizeof(int *)) ;*/ }

	/* Read an integer array */
#define WRITE_INT_ARRAY(fldname, count) \
	StaticAssertStmt(sizeof(node->fldname[0]) == sizeof(int32), \
					 "WRITE_INT_ARRAY() used on wrong array type"); \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(int32)); \
		} \
	}

/* Write a boolean array  */
#define WRITE_BOOL_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			char b = node->fldname[i] ? 1 : 0;								\
			appendBinaryStringInfo(str, (const char *)&b, 1); \
		} \
	}

/* Write an Trasnaction ID array  */
#define WRITE_XID_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(TransactionId)); \
		} \
	}

/* Write an AttrNumber array  */
#define WRITE_ATTRNUMBER_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(AttrNumber)); \
		} \
	}

/* Write an Oid array  */
#define WRITE_OID_ARRAY(fldname, count) \
	if ( (count) > 0 ) \
	{ \
		int i; \
		for(i = 0; i < (count); i++) \
		{ \
			appendBinaryStringInfo(str, (const char *)&node->fldname[i], sizeof(Oid)); \
		} \
	}

static void _outNode(StringInfo str, void *obj);

#define outDatum(str, value, typlen, typbyval) _outDatum(str, value, typlen, typbyval)

static void
_outList(StringInfo str, List *node)
{
	ListCell   *lc;

	if (node == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
		return;
	}

	WRITE_NODE_TYPE("");
    WRITE_INT_FIELD(length);

	foreach(lc, node)
	{

		if (IsA(node, List))
		{
			_outNode(str, lfirst(lc));
		}
		else if (IsA(node, IntList))
		{
			int n = lfirst_int(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(int));
		}
		else if (IsA(node, OidList))
		{
			Oid n = lfirst_oid(lc);
			appendBinaryStringInfo(str, (const char *)&n, sizeof(Oid));
		}
	}
}

/*
 * _outBitmapset -
 *	   converts a bitmap set of integers
 *
 * Currently bitmapsets do not appear in any node type that is stored in
 * rules, so there is no support in readfast.c for reading this format.
 */
static void
_outBitmapset(StringInfo str, const Bitmapset *bms)
{
	int			i;
	int			nwords = 0;

	if (bms)
		nwords = bms->nwords;

	appendBinaryStringInfo(str, (char *) &nwords, sizeof(int));
	for (i = 0; i < nwords; i++)
	{
		appendBinaryStringInfo(str, (char *) &bms->words[i], sizeof(bitmapword));
	}

}

/*
 * Print the value of a Datum given its type.
 */
static void
_outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
	Size		length;
	char	   *s;

	if (typbyval)
	{
		s = (char *) (&value);
		appendBinaryStringInfo(str, s, sizeof(Datum));
	}
	else
	{
		s = (char *) DatumGetPointer(value);
		if (!PointerIsValid(s))
		{
			length = 0;
			appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
		}
		else
		{
			if (typlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(s))
			{
				ExpandedObjectHeader *eoh = DatumGetEOHP(value);
				Size		resultsize;
				char		*resultptr;

				resultsize = EOH_get_flat_size(eoh);
				resultptr = (char *) palloc(resultsize);
				EOH_flatten_into(eoh, (void *) resultptr, resultsize);
				appendBinaryStringInfo(str, (char *)&resultsize, sizeof(Size));
				appendBinaryStringInfo(str, resultptr, resultsize);
			}
			else
			{
				length = datumGetSize(value, typbyval, typlen);
				appendBinaryStringInfo(str, (char *)&length, sizeof(Size));
				appendBinaryStringInfo(str, s, length);
			}
		}
	}
}

/*
 * GGDB binary node serializers.
 *
 * These were formerly obtained by "#define COMPILING_BINARY_FUNCS" +
 * "#include \"outfuncs.c\"".  PG16 turned outfuncs.c into a thin stub that
 * #includes the generated outfuncs.funcs.c, so it can no longer be re-included
 * under a macro.  The binary (COMPILING_BINARY_FUNCS) variant of every shared
 * serializer is therefore inlined here (faithful snapshot of the prior binary
 * compilation).  outfast.c is now standalone; keep it in sync with the node
 * definitions by hand (gen_node_support.pl does NOT produce the fast variant).
 */
#include "postgres.h"

#include <ctype.h>

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "utils/datum.h"
#include "utils/rel.h"

#include "cdb/cdbgang.h"
#include "nodes/altertablenodes.h"
#include "catalog/gp_distribution_policy.h"


/*
 * outfuncs.c is compiled normally into outfuncs.o, but it's also
 * #included from outfast.c. When #included, outfast.c defines
 * COMPILING_BINARY_FUNCS, and provides replacements WRITE_* macros. See
 * comments at top of readfast.c.
 */

#define booltostr(x)  ((x) ? "true" : "false")




/*
 *	Stuff from plannodes.h
 */

static void
_outPlannedStmt(StringInfo str, const PlannedStmt *node)
{
	WRITE_NODE_TYPE("PLANNEDSTMT");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(planGen, PlanGenerator);
	WRITE_UINT64_FIELD(queryId);
	WRITE_BOOL_FIELD(hasReturning);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_BOOL_FIELD(transientPlan);
	WRITE_BOOL_FIELD(oneoffPlan);
	WRITE_OID_FIELD(simplyUpdatableRel);
	WRITE_BOOL_FIELD(dependsOnRole);
	WRITE_BOOL_FIELD(parallelModeNeeded);
	WRITE_INT_FIELD(jitFlags);
	WRITE_NODE_FIELD(planTree);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(permInfos);
	/*
	 * PG18 centralized runtime partition pruning onto PlannedStmt: the QE
	 * needs partPruneInfos (indexed by Append/MergeAppend.part_prune_index in
	 * ExecDoInitialPruning) and unprunableRelids, which seeds
	 * EState.es_unpruned_relids.  Without the latter every scanned relation
	 * looks pruned ("trying to open a pruned relation") on the segment.
	 */
	WRITE_NODE_FIELD(partPruneInfos);
	WRITE_BITMAPSET_FIELD(unprunableRelids);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(appendRelations);
	WRITE_NODE_FIELD(subplans);
	WRITE_BITMAPSET_FIELD(rewindPlanIDs);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(relationOids);
	/*
	 * Don't serialize invalItems when dispatching. The TIDs of the invalidated items wouldn't
	 * make sense in segments.
	 */
	WRITE_NODE_FIELD(paramExecTypes);
	WRITE_NODE_FIELD(utilityStmt);
	WRITE_LOCATION_FIELD(stmt_location);
	WRITE_INT_FIELD(stmt_len);

	WRITE_INT_ARRAY(subplan_sliceIds, list_length(node->subplans));

	WRITE_INT_FIELD(numSlices);
	for (int i = 0; i < node->numSlices; i++)
	{
		WRITE_INT_FIELD(slices[i].sliceIndex);
		WRITE_INT_FIELD(slices[i].parentIndex);
		WRITE_INT_FIELD(slices[i].gangType);
		WRITE_INT_FIELD(slices[i].numsegments);
		WRITE_INT_FIELD(slices[i].segindex);
		WRITE_BOOL_FIELD(slices[i].directDispatch.isDirectDispatch);
		WRITE_NODE_FIELD(slices[i].directDispatch.contentIds);
	}

	WRITE_BITMAPSET_FIELD(rewindPlanIDs);

	WRITE_NODE_FIELD(intoPolicy);

	WRITE_UINT64_FIELD(query_mem);

	WRITE_INT_FIELD(total_memory_coordinator);
	WRITE_INT_FIELD(nsegments_coordinator);

	WRITE_NODE_FIELD(intoClause);
	WRITE_NODE_FIELD(copyIntoClause);
	WRITE_NODE_FIELD(refreshClause);
	WRITE_INT_FIELD(metricsQueryType);
}


static void
_outQueryDispatchDesc(StringInfo str, const QueryDispatchDesc *node)
{
	WRITE_NODE_TYPE("QUERYDISPATCHDESC");

	WRITE_NODE_FIELD(intoCreateStmt);
	WRITE_NODE_FIELD(paramInfo);
	WRITE_NODE_FIELD(oidAssignments);
	WRITE_NODE_FIELD(sliceTable);
	WRITE_NODE_FIELD(cursorPositions);
	WRITE_STRING_FIELD(parallelCursorName);
	WRITE_BOOL_FIELD(useChangedAOOpts);
	WRITE_INT_FIELD(secContext);
}

static void
_outSerializedParams(StringInfo str, const SerializedParams *node)
{
	WRITE_NODE_TYPE("SERIALIZEDPARAMS");

	WRITE_INT_FIELD(nExternParams);
	for (int i = 0; i < node->nExternParams; i++)
	{
		WRITE_BOOL_FIELD(externParams[i].isnull);
		WRITE_INT_FIELD(externParams[i].pflags);
		WRITE_OID_FIELD(externParams[i].ptype);
		WRITE_INT_FIELD(externParams[i].plen);
		WRITE_BOOL_FIELD(externParams[i].pbyval);

		if (!node->externParams[i].isnull)
			outDatum(str,
					 node->externParams[i].value,
					 node->externParams[i].plen,
					 node->externParams[i].pbyval);
	}

	WRITE_INT_FIELD(nExecParams);
	for (int i = 0; i < node->nExecParams; i++)
	{
		WRITE_BOOL_FIELD(execParams[i].isnull);
		WRITE_BOOL_FIELD(execParams[i].isvalid);
		WRITE_INT_FIELD(execParams[i].plen);
		WRITE_BOOL_FIELD(execParams[i].pbyval);

		if (node->execParams[i].isvalid && !node->execParams[i].isnull)
			outDatum(str,
					 node->execParams[i].value,
					 node->execParams[i].plen,
					 node->execParams[i].pbyval);
		WRITE_BOOL_FIELD(execParams[i].pbyval);
	}

	/*
	 * No text output function for TupleDescNodes. But that's OK, we
	 * only support text output for debugging purposes.
	 */
	WRITE_NODE_FIELD(transientTypes);
}

static void
_outOidAssignment(StringInfo str, const OidAssignment *node)
{
	WRITE_NODE_TYPE("OIDASSIGNMENT");

	WRITE_OID_FIELD(catalog);
	WRITE_STRING_FIELD(objname);
	WRITE_OID_FIELD(namespaceOid);
	WRITE_OID_FIELD(keyOid1);
	WRITE_OID_FIELD(keyOid2);
	WRITE_OID_FIELD(oid);
}

/*
 * print the basic stuff of all nodes that inherit from Plan
 */
static void
_outPlanInfo(StringInfo str, const Plan *node)
{
	WRITE_FLOAT_FIELD(startup_cost, "%.2f");
	WRITE_FLOAT_FIELD(total_cost, "%.2f");
	WRITE_FLOAT_FIELD(plan_rows, "%.0f");
	WRITE_INT_FIELD(plan_width);
	WRITE_BOOL_FIELD(parallel_aware);
	WRITE_BOOL_FIELD(parallel_safe);
	WRITE_BOOL_FIELD(async_capable);
	WRITE_INT_FIELD(plan_node_id);
	WRITE_NODE_FIELD(targetlist);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(lefttree);
	WRITE_NODE_FIELD(righttree);
	WRITE_NODE_FIELD(initPlan);

	WRITE_BITMAPSET_FIELD(extParam);
	WRITE_BITMAPSET_FIELD(allParam);

	/* 'flow' is only needed during planning. */

	WRITE_UINT64_FIELD(operatorMemKB);
}

/*
 * print the basic stuff of all nodes that inherit from Scan
 */
static void
_outScanInfo(StringInfo str, const Scan *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_UINT_FIELD(scanrelid);
}

/*
 * print the basic stuff of all nodes that inherit from Join
 */
static void
_outJoinPlanInfo(StringInfo str, const Join *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_BOOL_FIELD(prefetch_inner);
	WRITE_BOOL_FIELD(prefetch_joinqual);
	WRITE_BOOL_FIELD(prefetch_qual);

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(inner_unique);
	WRITE_NODE_FIELD(joinqual);
}

static void
_outPlan(StringInfo str, const Plan *node)
{
	WRITE_NODE_TYPE("PLAN");

	_outPlanInfo(str, (const Plan *) node);
}

static void
_outResult(StringInfo str, const Result *node)
{
	WRITE_NODE_TYPE("RESULT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(resconstantqual);

	WRITE_INT_FIELD(numHashFilterCols);
	WRITE_ATTRNUMBER_ARRAY(hashFilterColIdx, node->numHashFilterCols);
	WRITE_OID_ARRAY(hashFilterFuncs, node->numHashFilterCols);
}

static void
_outProjectSet(StringInfo str, const ProjectSet *node)
{
	WRITE_NODE_TYPE("PROJECTSET");

	_outPlanInfo(str, (const Plan *) node);
}

static void
_outModifyTable(StringInfo str, const ModifyTable *node)
{
	WRITE_NODE_TYPE("MODIFYTABLE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(operation, CmdType);
	WRITE_BOOL_FIELD(canSetTag);
	WRITE_UINT_FIELD(nominalRelation);
	WRITE_UINT_FIELD(rootRelation);
	WRITE_BOOL_FIELD(partColsUpdated);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(updateColnosLists);
	WRITE_NODE_FIELD(withCheckOptionLists);
	WRITE_NODE_FIELD(returningLists);
	WRITE_NODE_FIELD(fdwPrivLists);
	WRITE_BITMAPSET_FIELD(fdwDirectModifyPlans);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_INT_FIELD(epqParam);
	WRITE_ENUM_FIELD(onConflictAction, OnConflictAction);
	WRITE_NODE_FIELD(arbiterIndexes);
	WRITE_NODE_FIELD(onConflictSet);
	WRITE_NODE_FIELD(onConflictCols);
	WRITE_NODE_FIELD(onConflictWhere);
	WRITE_UINT_FIELD(exclRelRTI);
	WRITE_NODE_FIELD(exclRelTlist);
	WRITE_NODE_FIELD(isSplitUpdates);
	WRITE_BOOL_FIELD(forceTupleRouting);
	WRITE_NODE_FIELD(mergeActionLists);
	WRITE_NODE_FIELD(mergeJoinConditions);
}

static void
_outAppend(StringInfo str, const Append *node)
{
	WRITE_NODE_TYPE("APPEND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BITMAPSET_FIELD(apprelids);
	WRITE_NODE_FIELD(appendplans);
	WRITE_INT_FIELD(nasyncplans);
	WRITE_INT_FIELD(first_partial_plan);
	WRITE_INT_FIELD(part_prune_index);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outSequence(StringInfo str, const Sequence *node)
{
	WRITE_NODE_TYPE("SEQUENCE");
	_outPlanInfo(str, (Plan *)node);
	WRITE_NODE_FIELD(subplans);
}

static void
_outMergeAppend(StringInfo str, const MergeAppend *node)
{
	WRITE_NODE_TYPE("MERGEAPPEND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BITMAPSET_FIELD(apprelids);
	WRITE_NODE_FIELD(mergeplans);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
	WRITE_INT_FIELD(part_prune_index);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outRecursiveUnion(StringInfo str, const RecursiveUnion *node)
{
	WRITE_NODE_TYPE("RECURSIVEUNION");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(wtParam);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(dupColIdx, node->numCols);
	WRITE_OID_ARRAY(dupOperators, node->numCols);
	WRITE_OID_ARRAY(dupCollations, node->numCols);
	WRITE_LONG_FIELD(numGroups);
}

static void
_outBitmapAnd(StringInfo str, const BitmapAnd *node)
{
	WRITE_NODE_TYPE("BITMAPAND");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(bitmapplans);
}

static void
_outBitmapOr(StringInfo str, const BitmapOr *node)
{
	WRITE_NODE_TYPE("BITMAPOR");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BOOL_FIELD(isshared);
	WRITE_NODE_FIELD(bitmapplans);
}

static void
_outGather(StringInfo str, const Gather *node)
{
	WRITE_NODE_TYPE("GATHER");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(num_workers);
	WRITE_INT_FIELD(rescan_param);
	WRITE_BOOL_FIELD(single_copy);
	WRITE_BOOL_FIELD(invisible);
	WRITE_BITMAPSET_FIELD(initParam);
}

static void
_outGatherMerge(StringInfo str, const GatherMerge *node)
{
	WRITE_NODE_TYPE("GATHERMERGE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(num_workers);
	WRITE_INT_FIELD(rescan_param);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);
	WRITE_OID_ARRAY(sortOperators, node->numCols);
	WRITE_OID_ARRAY(collations, node->numCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
	WRITE_BITMAPSET_FIELD(initParam);
}

static void
_outScan(StringInfo str, const Scan *node)
{
	WRITE_NODE_TYPE("SCAN");

	_outScanInfo(str, node);
}

static void
_outSeqScan(StringInfo str, const SeqScan *node)
{
	WRITE_NODE_TYPE("SEQSCAN");

	_outScanInfo(str, (const Scan *) node);
}

static void
_outDynamicSeqScan(StringInfo str, const DynamicSeqScan *node)
{
	WRITE_NODE_TYPE("DYNAMICSEQSCAN");

	_outScanInfo(str, (Scan *)node);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outSampleScan(StringInfo str, const SampleScan *node)
{
	WRITE_NODE_TYPE("SAMPLESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tablesample);
}

static void
_outExternalScanInfo(StringInfo str, const ExternalScanInfo *node)
{
	WRITE_NODE_TYPE("EXTERNALSCANINFO");

	WRITE_NODE_FIELD(uriList);
	WRITE_CHAR_FIELD(fmtType);
	WRITE_BOOL_FIELD(isMasterOnly);
	WRITE_INT_FIELD(rejLimit);
	WRITE_BOOL_FIELD(rejLimitInRows);
	WRITE_CHAR_FIELD(logErrors);
	WRITE_INT_FIELD(encoding);
	WRITE_INT_FIELD(scancounter);
	WRITE_NODE_FIELD(extOptions);
}

static void
outIndexScanFields(StringInfo str, const IndexScan *node)
{
	_outScanInfo(str, (const Scan *) node);

	WRITE_OID_FIELD(indexid);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(indexqualorig);
	WRITE_NODE_FIELD(indexorderby);
	WRITE_NODE_FIELD(indexorderbyorig);
	WRITE_NODE_FIELD(indexorderbyops);
	WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void
_outIndexScan(StringInfo str, const IndexScan *node)
{
	WRITE_NODE_TYPE("INDEXSCAN");

	outIndexScanFields(str, node);
}

static void
_outIndexOnlyScan(StringInfo str, const IndexOnlyScan *node)
{
	WRITE_NODE_TYPE("INDEXONLYSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_OID_FIELD(indexid);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(recheckqual);
	WRITE_NODE_FIELD(indexorderby);
	WRITE_NODE_FIELD(indextlist);
	WRITE_ENUM_FIELD(indexorderdir, ScanDirection);
}

static void
_outDynamicIndexScan(StringInfo str, const DynamicIndexScan *node)
{
	WRITE_NODE_TYPE("DYNAMICINDEXSCAN");

	outIndexScanFields(str, &node->indexscan);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outBitmapIndexScanFields(StringInfo str, const BitmapIndexScan *node)
{
	_outScanInfo(str, (Scan *) node);

	WRITE_OID_FIELD(indexid);
	WRITE_BOOL_FIELD(isshared);
	WRITE_NODE_FIELD(indexqual);
	WRITE_NODE_FIELD(indexqualorig);
}

static void
_outBitmapIndexScan(StringInfo str, const BitmapIndexScan *node)
{
	WRITE_NODE_TYPE("BITMAPINDEXSCAN");

	_outBitmapIndexScanFields(str, node);
}

static void
_outDynamicBitmapIndexScan(StringInfo str, const DynamicBitmapIndexScan *node)
{
	WRITE_NODE_TYPE("DYNAMICBITMAPINDEXSCAN");

	_outBitmapIndexScanFields(str, &node->biscan);
}

static void
outBitmapHeapScanFields(StringInfo str, const BitmapHeapScan *node)
{
	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(bitmapqualorig);
}

static void
_outBitmapHeapScan(StringInfo str, const BitmapHeapScan *node)
{
	WRITE_NODE_TYPE("BITMAPHEAPSCAN");

	outBitmapHeapScanFields(str, node);
}

static void
_outDynamicBitmapHeapScan(StringInfo str, const DynamicBitmapHeapScan *node)
{
	WRITE_NODE_TYPE("DYNAMICBITMAPHEAPSCAN");

	outBitmapHeapScanFields(str, &node->bitmapheapscan);
	WRITE_NODE_FIELD(partOids);
	WRITE_NODE_FIELD(part_prune_info);
	WRITE_NODE_FIELD(join_prune_paramids);
}

static void
_outTidScan(StringInfo str, const TidScan *node)
{
	WRITE_NODE_TYPE("TIDSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tidquals);
}

static void
_outTidRangeScan(StringInfo str, const TidRangeScan *node)
{
	WRITE_NODE_TYPE("TIDRANGESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tidrangequals);
}

static void
_outSubqueryScan(StringInfo str, const SubqueryScan *node)
{
	WRITE_NODE_TYPE("SUBQUERYSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(subplan);
	WRITE_ENUM_FIELD(scanstatus, SubqueryScanStatus);
}

static void
_outFunctionScan(StringInfo str, const FunctionScan *node)
{
	WRITE_NODE_TYPE("FUNCTIONSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(functions);
	WRITE_BOOL_FIELD(funcordinality);
	WRITE_NODE_FIELD(param);
	WRITE_BOOL_FIELD(resultInTupleStore);
	WRITE_INT_FIELD(initplanId);
}

static void
_outTableFuncScan(StringInfo str, const TableFuncScan *node)
{
	WRITE_NODE_TYPE("TABLEFUNCSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tablefunc);
}

static void
_outValuesScan(StringInfo str, const ValuesScan *node)
{
	WRITE_NODE_TYPE("VALUESSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(values_lists);
}

static void
_outCteScan(StringInfo str, const CteScan *node)
{
	WRITE_NODE_TYPE("CTESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_INT_FIELD(ctePlanId);
	WRITE_INT_FIELD(cteParam);
}

static void
_outNamedTuplestoreScan(StringInfo str, const NamedTuplestoreScan *node)
{
	WRITE_NODE_TYPE("NAMEDTUPLESTORESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_STRING_FIELD(enrname);
}

static void
_outWorkTableScan(StringInfo str, const WorkTableScan *node)
{
	WRITE_NODE_TYPE("WORKTABLESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_INT_FIELD(wtParam);
}

static void
_outForeignScan(StringInfo str, const ForeignScan *node)
{
	WRITE_NODE_TYPE("FOREIGNSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_ENUM_FIELD(operation, CmdType);
	WRITE_UINT_FIELD(resultRelation);
	WRITE_OID_FIELD(fs_server);
	WRITE_NODE_FIELD(fdw_exprs);
	WRITE_NODE_FIELD(fdw_private);
	WRITE_NODE_FIELD(fdw_scan_tlist);
	WRITE_NODE_FIELD(fdw_recheck_quals);
	WRITE_BITMAPSET_FIELD(fs_relids);
	WRITE_BOOL_FIELD(fsSystemCol);
}


static void
_outJoin(StringInfo str, const Join *node)
{
	WRITE_NODE_TYPE("JOIN");

	_outJoinPlanInfo(str, (const Join *) node);
}

static void
_outNestLoop(StringInfo str, const NestLoop *node)
{
	WRITE_NODE_TYPE("NESTLOOP");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_NODE_FIELD(nestParams);

	WRITE_BOOL_FIELD(shared_outer);
	WRITE_BOOL_FIELD(singleton_outer); /*CDB-OLAP*/
}

static void
_outMergeJoin(StringInfo str, const MergeJoin *node)
{
	int			numCols;

	WRITE_NODE_TYPE("MERGEJOIN");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_BOOL_FIELD(skip_mark_restore);
	WRITE_NODE_FIELD(mergeclauses);

	numCols = list_length(node->mergeclauses);

	WRITE_OID_ARRAY(mergeFamilies, numCols);
	WRITE_OID_ARRAY(mergeCollations, numCols);
	WRITE_BOOL_ARRAY(mergeReversals, numCols);
	WRITE_BOOL_ARRAY(mergeNullsFirst, numCols);
    WRITE_BOOL_FIELD(unique_outer);
}

static void
_outHashJoin(StringInfo str, const HashJoin *node)
{
	WRITE_NODE_TYPE("HASHJOIN");

	_outJoinPlanInfo(str, (const Join *) node);

	WRITE_NODE_FIELD(hashclauses);
	WRITE_NODE_FIELD(hashqualclauses);
	WRITE_NODE_FIELD(hashoperators);
	WRITE_NODE_FIELD(hashcollations);
	WRITE_NODE_FIELD(hashkeys);
}

static void
_outAgg(StringInfo str, const Agg *node)
{
	WRITE_NODE_TYPE("AGG");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(aggstrategy, AggStrategy);
	WRITE_ENUM_FIELD(aggsplit, AggSplit);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);
	WRITE_OID_ARRAY(grpOperators, node->numCols);
	WRITE_OID_ARRAY(grpCollations, node->numCols);
	WRITE_LONG_FIELD(numGroups);
	WRITE_UINT64_FIELD(transitionSpace);
	WRITE_BITMAPSET_FIELD(aggParams);
	WRITE_NODE_FIELD(groupingSets);
	WRITE_NODE_FIELD(chain);
	WRITE_BOOL_FIELD(streaming);

	WRITE_UINT_FIELD(agg_expr_id);
}

static void
_outDQAExpr(StringInfo str, const DQAExpr *node)
{
    WRITE_NODE_TYPE("DQAExpr");

    WRITE_INT_FIELD(agg_expr_id);
    WRITE_BITMAPSET_FIELD(agg_args_id_bms);
    WRITE_NODE_FIELD(agg_filter);
}

static void
_outTupleSplit(StringInfo str, const TupleSplit *node)
{
	WRITE_NODE_TYPE("TupleSplit");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);
	WRITE_NODE_FIELD(dqa_expr_lst);
}

static void
_outWindowAgg(StringInfo str, const WindowAgg *node)
{
	WRITE_NODE_TYPE("WINDOWAGG");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_UINT_FIELD(winref);
	WRITE_INT_FIELD(partNumCols);
	WRITE_ATTRNUMBER_ARRAY(partColIdx, node->partNumCols);
	WRITE_OID_ARRAY(partOperators, node->partNumCols);
	WRITE_OID_ARRAY(partCollations, node->partNumCols);
	WRITE_INT_FIELD(ordNumCols);
	WRITE_ATTRNUMBER_ARRAY(ordColIdx, node->ordNumCols);
	WRITE_OID_ARRAY(ordOperators, node->ordNumCols);
	WRITE_OID_ARRAY(ordCollations, node->ordNumCols);
	WRITE_INT_FIELD(firstOrderCol);
	WRITE_OID_FIELD(firstOrderCmpOperator);
	WRITE_BOOL_FIELD(firstOrderNullsFirst);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	WRITE_NODE_FIELD(runCondition);
	WRITE_NODE_FIELD(runConditionOrig);
	WRITE_OID_FIELD(startInRangeFunc);
	WRITE_OID_FIELD(endInRangeFunc);
	WRITE_OID_FIELD(inRangeColl);
	WRITE_BOOL_FIELD(inRangeAsc);
	WRITE_BOOL_FIELD(inRangeNullsFirst);
	WRITE_BOOL_FIELD(topWindow);
}

static void
_outTableFunctionScan(StringInfo str, const TableFunctionScan *node)
{
	WRITE_NODE_TYPE("TABLEFUNCTIONSCAN");

	_outScanInfo(str, (Scan *) node);

	WRITE_NODE_FIELD(function);
}

static void
_outMaterial(StringInfo str, const Material *node)
{
	WRITE_NODE_TYPE("MATERIAL");

	_outPlanInfo(str, (Plan *) node);

	WRITE_BOOL_FIELD(cdb_strict);
	WRITE_BOOL_FIELD(cdb_shield_child_from_rescans);
}

static void
_outShareInputScan(StringInfo str, const ShareInputScan *node)
{
	WRITE_NODE_TYPE("SHAREINPUTSCAN");

	WRITE_BOOL_FIELD(cross_slice);
	WRITE_INT_FIELD(share_id);
	WRITE_INT_FIELD(producer_slice_id);
	WRITE_INT_FIELD(this_slice_id);
	WRITE_INT_FIELD(nconsumers);

	_outPlanInfo(str, (Plan *) node);
}

static void
_outMemoize(StringInfo str, const Memoize *node)
{
	WRITE_NODE_TYPE("MEMOIZE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numKeys);
	WRITE_OID_ARRAY(hashOperators, node->numKeys);
	WRITE_OID_ARRAY(collations, node->numKeys);
	WRITE_NODE_FIELD(param_exprs);
	WRITE_BOOL_FIELD(singlerow);
	WRITE_BOOL_FIELD(binary_mode);
	WRITE_UINT_FIELD(est_entries);
	WRITE_BITMAPSET_FIELD(keyparamids);
}

static void
_outSortInfo(StringInfo str, const Sort *node)
{
	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
    WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numCols);
    WRITE_OID_ARRAY(sortOperators, node->numCols);
    WRITE_OID_ARRAY(collations, node->numCols);
    WRITE_BOOL_ARRAY(nullsFirst, node->numCols);
}

static void
_outSort(StringInfo str, const Sort *node)
{
	WRITE_NODE_TYPE("SORT");

	_outSortInfo(str, node);
}

static void
_outIncrementalSort(StringInfo str, const IncrementalSort *node)
{
	WRITE_NODE_TYPE("INCREMENTALSORT");

	_outSortInfo(str, (const Sort *) node);

	WRITE_INT_FIELD(nPresortedCols);
}

static void
_outUnique(StringInfo str, const Unique *node)
{
	WRITE_NODE_TYPE("UNIQUE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(uniqColIdx, node->numCols);
	WRITE_OID_ARRAY(uniqOperators, node->numCols);
	WRITE_OID_ARRAY(uniqCollations, node->numCols);
}

static void
_outHash(StringInfo str, const Hash *node)
{
	WRITE_NODE_TYPE("HASH");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_BOOL_FIELD(rescannable);          /*CDB*/
	WRITE_NODE_FIELD(hashkeys);
	WRITE_OID_FIELD(skewTable);
	WRITE_INT_FIELD(skewColumn);
	WRITE_BOOL_FIELD(skewInherit);

	WRITE_FLOAT_FIELD(rows_total, "%.0f");
}

static void
_outSetOp(StringInfo str, const SetOp *node)
{
	WRITE_NODE_TYPE("SETOP");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_ENUM_FIELD(cmd, SetOpCmd);
	WRITE_ENUM_FIELD(strategy, SetOpStrategy);
	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(cmpColIdx, node->numCols);
	WRITE_OID_ARRAY(cmpOperators, node->numCols);
	WRITE_OID_ARRAY(cmpCollations, node->numCols);
	WRITE_BOOL_ARRAY(cmpNullsFirst, node->numCols);
	WRITE_LONG_FIELD(numGroups);
}

static void
_outLockRows(StringInfo str, const LockRows *node)
{
	WRITE_NODE_TYPE("LOCKROWS");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(rowMarks);
	WRITE_INT_FIELD(epqParam);
}

static void
_outLimit(StringInfo str, const Limit *node)
{
	WRITE_NODE_TYPE("LIMIT");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_ENUM_FIELD(limitOption, LimitOption);
	WRITE_INT_FIELD(uniqNumCols);
	WRITE_ATTRNUMBER_ARRAY(uniqColIdx, node->uniqNumCols);
	WRITE_OID_ARRAY(uniqOperators, node->uniqNumCols);
	WRITE_OID_ARRAY(uniqCollations, node->uniqNumCols);
}

static void
_outNestLoopParam(StringInfo str, const NestLoopParam *node)
{
	WRITE_NODE_TYPE("NESTLOOPPARAM");

	WRITE_INT_FIELD(paramno);
	WRITE_NODE_FIELD(paramval);
}

static void
_outPlanRowMark(StringInfo str, const PlanRowMark *node)
{
	WRITE_NODE_TYPE("PLANROWMARK");

	WRITE_UINT_FIELD(rti);
	WRITE_UINT_FIELD(prti);
	WRITE_UINT_FIELD(rowmarkId);
	WRITE_ENUM_FIELD(markType, RowMarkType);
	WRITE_INT_FIELD(allMarkTypes);
	WRITE_ENUM_FIELD(strength, LockClauseStrength);
	WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	WRITE_BOOL_FIELD(isParent);
	WRITE_BOOL_FIELD(canOptSelectLockingClause);
}

static void
_outPartitionPruneInfo(StringInfo str, const PartitionPruneInfo *node)
{
	WRITE_NODE_TYPE("PARTITIONPRUNEINFO");

	WRITE_BITMAPSET_FIELD(relids);
	WRITE_NODE_FIELD(prune_infos);
	WRITE_BITMAPSET_FIELD(other_subplans);
}

static void
_outPartitionedRelPruneInfo(StringInfo str, const PartitionedRelPruneInfo *node)
{
	WRITE_NODE_TYPE("PARTITIONEDRELPRUNEINFO");

	WRITE_UINT_FIELD(rtindex);
	WRITE_BITMAPSET_FIELD(present_parts);
	WRITE_INT_FIELD(nparts);
	WRITE_INT_ARRAY(subplan_map, node->nparts);
	WRITE_INT_ARRAY(subpart_map, node->nparts);
	WRITE_OID_ARRAY(relid_map, node->nparts);
	WRITE_NODE_FIELD(initial_pruning_steps);
	WRITE_NODE_FIELD(exec_pruning_steps);
	WRITE_BITMAPSET_FIELD(execparamids);
}

static void
_outPartitionPruneStepOp(StringInfo str, const PartitionPruneStepOp *node)
{
	WRITE_NODE_TYPE("PARTITIONPRUNESTEPOP");

	WRITE_INT_FIELD(step.step_id);
	WRITE_INT_FIELD(opstrategy);
	WRITE_NODE_FIELD(exprs);
	WRITE_NODE_FIELD(cmpfns);
	WRITE_BITMAPSET_FIELD(nullkeys);
}

static void
_outPartitionPruneStepCombine(StringInfo str, const PartitionPruneStepCombine *node)
{
	WRITE_NODE_TYPE("PARTITIONPRUNESTEPCOMBINE");

	WRITE_INT_FIELD(step.step_id);
	WRITE_ENUM_FIELD(combineOp, PartitionPruneCombineOp);
	WRITE_NODE_FIELD(source_stepids);
}


static void
_outMotion(StringInfo str, const Motion *node)
{
	WRITE_NODE_TYPE("MOTION");

	WRITE_INT_FIELD(motionID);
	WRITE_ENUM_FIELD(motionType, MotionType);

	WRITE_BOOL_FIELD(sendSorted);

	WRITE_NODE_FIELD(hashExprs);
	WRITE_OID_ARRAY(hashFuncs, list_length(node->hashExprs));
	WRITE_INT_FIELD(numSortCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numSortCols);
	WRITE_INT_ARRAY(sortOperators, node->numSortCols);
	WRITE_INT_ARRAY(collations, node->numSortCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numSortCols);
	WRITE_INT_FIELD(segidColIdx);

	WRITE_INT_FIELD(numHashSegments);

	/* senderSliceInfo is intentionally omitted. It's only used during planning */

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outSplitUpdate
 */
static void
_outSplitUpdate(StringInfo str, const SplitUpdate *node)
{
	WRITE_NODE_TYPE("SplitUpdate");

	WRITE_INT_FIELD(actionColIdx);
	WRITE_NODE_FIELD(insertColIdx);
	WRITE_NODE_FIELD(deleteColIdx);

	WRITE_INT_FIELD(numHashSegments);
	WRITE_INT_FIELD(numHashAttrs);
	WRITE_ATTRNUMBER_ARRAY(hashAttnos, node->numHashAttrs);
	WRITE_OID_ARRAY(hashFuncs, node->numHashAttrs);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outAssertOp
 */
static void
_outAssertOp(StringInfo str, const AssertOp *node)
{
	WRITE_NODE_TYPE("AssertOp");

	WRITE_NODE_FIELD(errmessage);
	WRITE_INT_FIELD(errcode);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outPartitionSelector
 */
static void
_outPartitionSelector(StringInfo str, const PartitionSelector *node)
{
	WRITE_NODE_TYPE("PartitionSelector");

	WRITE_INT_FIELD(paramid);
	WRITE_NODE_FIELD(part_prune_info);

	_outPlanInfo(str, (Plan *) node);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outAlias(StringInfo str, const Alias *node)
{
	WRITE_NODE_TYPE("ALIAS");

	WRITE_STRING_FIELD(aliasname);
	WRITE_NODE_FIELD(colnames);
}

static void
_outRangeVar(StringInfo str, const RangeVar *node)
{
	WRITE_NODE_TYPE("RANGEVAR");

	/*
	 * we deliberately ignore catalogname here, since it is presently not
	 * semantically meaningful
	 */
	WRITE_STRING_FIELD(catalogname);
	WRITE_STRING_FIELD(schemaname);
	WRITE_STRING_FIELD(relname);
	WRITE_BOOL_FIELD(inh);
	WRITE_CHAR_FIELD(relpersistence);
	WRITE_NODE_FIELD(alias);
	WRITE_LOCATION_FIELD(location);
}

static void
_outTableFunc(StringInfo str, const TableFunc *node)
{
	WRITE_NODE_TYPE("TABLEFUNC");

	WRITE_ENUM_FIELD(functype, TableFuncType);
	WRITE_NODE_FIELD(ns_uris);
	WRITE_NODE_FIELD(ns_names);
	WRITE_NODE_FIELD(docexpr);
	WRITE_NODE_FIELD(rowexpr);
	WRITE_NODE_FIELD(colnames);
	WRITE_NODE_FIELD(coltypes);
	WRITE_NODE_FIELD(coltypmods);
	WRITE_NODE_FIELD(colcollations);
	WRITE_NODE_FIELD(colexprs);
	WRITE_NODE_FIELD(coldefexprs);
	WRITE_NODE_FIELD(colvalexprs);
	WRITE_NODE_FIELD(passingvalexprs);
	WRITE_BITMAPSET_FIELD(notnulls);
	WRITE_NODE_FIELD(plan);
	WRITE_INT_FIELD(ordinalitycol);
	WRITE_LOCATION_FIELD(location);
}

static void
_outIntoClause(StringInfo str, const IntoClause *node)
{
	WRITE_NODE_TYPE("INTOCLAUSE");

	WRITE_NODE_FIELD(rel);
	WRITE_NODE_FIELD(colNames);
	WRITE_STRING_FIELD(accessMethod);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(onCommit, OnCommitAction);
	WRITE_STRING_FIELD(tableSpaceName);
	WRITE_NODE_FIELD(viewQuery);
	WRITE_BOOL_FIELD(skipData);
	WRITE_NODE_FIELD(distributedBy);
}

static void
_outCopyIntoClause(StringInfo str, const CopyIntoClause *node)
{
	WRITE_NODE_TYPE("COPYINTOCLAUSE");

	WRITE_NODE_FIELD(attlist);
	WRITE_BOOL_FIELD(is_program);
	WRITE_STRING_FIELD(filename);
	WRITE_NODE_FIELD(options);
}

static void
_outRefreshClause(StringInfo str, const RefreshClause *node)
{
	WRITE_NODE_TYPE("REFRESHCLAUSE");

	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(skipData);
	WRITE_NODE_FIELD(relation);
}

static void
_outVar(StringInfo str, const Var *node)
{
	WRITE_NODE_TYPE("VAR");

	WRITE_INT_FIELD(varno);
	WRITE_INT_FIELD(varattno);
	WRITE_OID_FIELD(vartype);
	WRITE_INT_FIELD(vartypmod);
	WRITE_OID_FIELD(varcollid);
	WRITE_UINT_FIELD(varlevelsup);
	WRITE_ENUM_FIELD(varreturningtype, VarReturningType);	/* PG18: OLD/NEW ref */
	WRITE_UINT_FIELD(varnosyn);
	WRITE_INT_FIELD(varattnosyn);
	WRITE_LOCATION_FIELD(location);
}


static void
_outParam(StringInfo str, const Param *node)
{
	WRITE_NODE_TYPE("PARAM");

	WRITE_ENUM_FIELD(paramkind, ParamKind);
	WRITE_INT_FIELD(paramid);
	WRITE_OID_FIELD(paramtype);
	WRITE_INT_FIELD(paramtypmod);
	WRITE_OID_FIELD(paramcollid);
	WRITE_LOCATION_FIELD(location);
}

static void
_outAggref(StringInfo str, const Aggref *node)
{
	WRITE_NODE_TYPE("AGGREF");

	WRITE_OID_FIELD(aggfnoid);
	WRITE_OID_FIELD(aggtype);
	WRITE_OID_FIELD(aggcollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_OID_FIELD(aggtranstype);
	WRITE_NODE_FIELD(aggargtypes);
	WRITE_NODE_FIELD(aggdirectargs);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(aggorder);
	WRITE_NODE_FIELD(aggdistinct);
	WRITE_NODE_FIELD(aggfilter);
	WRITE_BOOL_FIELD(aggstar);
	WRITE_BOOL_FIELD(aggvariadic);
	WRITE_CHAR_FIELD(aggkind);
	WRITE_BOOL_FIELD(aggpresorted);
	WRITE_UINT_FIELD(agglevelsup);
	WRITE_ENUM_FIELD(aggsplit, AggSplit);
	WRITE_INT_FIELD(aggno);
	WRITE_INT_FIELD(aggtransno);
	WRITE_LOCATION_FIELD(location);
    WRITE_INT_FIELD(agg_expr_id);
}

static void
_outGroupingFunc(StringInfo str, const GroupingFunc *node)
{
	WRITE_NODE_TYPE("GROUPINGFUNC");

	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(refs);
	WRITE_NODE_FIELD(cols);
	WRITE_UINT_FIELD(agglevelsup);
	WRITE_LOCATION_FIELD(location);
}

static void
_outGroupId(StringInfo str, const GroupId *node)
{
	WRITE_NODE_TYPE("GROUPID");

	WRITE_INT_FIELD(agglevelsup);
	WRITE_LOCATION_FIELD(location);
}

static void
_outGroupingSetId(StringInfo str, const GroupingSetId *node)
{
	WRITE_NODE_TYPE("GROUPINGSETID");

	WRITE_LOCATION_FIELD(location);
}

static void
_outWindowFunc(StringInfo str, const WindowFunc *node)
{
	WRITE_NODE_TYPE("WINDOWFUNC");

	WRITE_OID_FIELD(winfnoid);
	WRITE_OID_FIELD(wintype);
	WRITE_OID_FIELD(wincollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(aggfilter);
	WRITE_NODE_FIELD(runCondition);
	WRITE_UINT_FIELD(winref);
	WRITE_BOOL_FIELD(winstar);
	WRITE_BOOL_FIELD(winagg);
	WRITE_BOOL_FIELD(windistinct);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSubscriptingRef(StringInfo str, const SubscriptingRef *node)
{
	WRITE_NODE_TYPE("SUBSCRIPTINGREF");

	WRITE_OID_FIELD(refcontainertype);
	WRITE_OID_FIELD(refelemtype);
	WRITE_OID_FIELD(refrestype);
	WRITE_INT_FIELD(reftypmod);
	WRITE_OID_FIELD(refcollid);
	WRITE_NODE_FIELD(refupperindexpr);
	WRITE_NODE_FIELD(reflowerindexpr);
	WRITE_NODE_FIELD(refexpr);
	WRITE_NODE_FIELD(refassgnexpr);
}

static void
_outFuncExpr(StringInfo str, const FuncExpr *node)
{
	WRITE_NODE_TYPE("FUNCEXPR");

	WRITE_OID_FIELD(funcid);
	WRITE_OID_FIELD(funcresulttype);
	WRITE_BOOL_FIELD(funcretset);
	WRITE_BOOL_FIELD(funcvariadic);
	WRITE_ENUM_FIELD(funcformat, CoercionForm);
	WRITE_OID_FIELD(funccollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_BOOL_FIELD(is_tablefunc);  /* GPDB */
	WRITE_LOCATION_FIELD(location);
}

static void
_outNamedArgExpr(StringInfo str, const NamedArgExpr *node)
{
	WRITE_NODE_TYPE("NAMEDARGEXPR");

	WRITE_NODE_FIELD(arg);
	WRITE_STRING_FIELD(name);
	WRITE_INT_FIELD(argnumber);
	WRITE_LOCATION_FIELD(location);
}

static void
_outOpExpr(StringInfo str, const OpExpr *node)
{
	WRITE_NODE_TYPE("OPEXPR");

	WRITE_OID_FIELD(opno);
	WRITE_OID_FIELD(opfuncid);
	WRITE_OID_FIELD(opresulttype);
	WRITE_BOOL_FIELD(opretset);
	WRITE_OID_FIELD(opcollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outDistinctExpr(StringInfo str, const DistinctExpr *node)
{
	WRITE_NODE_TYPE("DISTINCTEXPR");

	WRITE_OID_FIELD(opno);
	WRITE_OID_FIELD(opfuncid);
	WRITE_OID_FIELD(opresulttype);
	WRITE_BOOL_FIELD(opretset);
	WRITE_OID_FIELD(opcollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outNullIfExpr(StringInfo str, const NullIfExpr *node)
{
	WRITE_NODE_TYPE("NULLIFEXPR");

	WRITE_OID_FIELD(opno);
	WRITE_OID_FIELD(opfuncid);
	WRITE_OID_FIELD(opresulttype);
	WRITE_BOOL_FIELD(opretset);
	WRITE_OID_FIELD(opcollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outScalarArrayOpExpr(StringInfo str, const ScalarArrayOpExpr *node)
{
	WRITE_NODE_TYPE("SCALARARRAYOPEXPR");

	WRITE_OID_FIELD(opno);
	WRITE_OID_FIELD(opfuncid);
	WRITE_OID_FIELD(hashfuncid);
	WRITE_OID_FIELD(negfuncid);
	WRITE_BOOL_FIELD(useOr);
	WRITE_OID_FIELD(inputcollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}


static void
_outSubLink(StringInfo str, const SubLink *node)
{
	WRITE_NODE_TYPE("SUBLINK");

	WRITE_ENUM_FIELD(subLinkType, SubLinkType);
	WRITE_INT_FIELD(subLinkId);
	WRITE_NODE_FIELD(testexpr);
	WRITE_NODE_FIELD(operName);
	WRITE_NODE_FIELD(subselect);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSubPlan(StringInfo str, const SubPlan *node)
{
	WRITE_NODE_TYPE("SUBPLAN");

	WRITE_ENUM_FIELD(subLinkType, SubLinkType);
	WRITE_NODE_FIELD(testexpr);
	WRITE_NODE_FIELD(paramIds);
	WRITE_INT_FIELD(plan_id);
	WRITE_STRING_FIELD(plan_name);
	WRITE_OID_FIELD(firstColType);
	WRITE_INT_FIELD(firstColTypmod);
	WRITE_OID_FIELD(firstColCollation);
	WRITE_BOOL_FIELD(useHashTable);
	WRITE_BOOL_FIELD(unknownEqFalse);
    WRITE_BOOL_FIELD(parallel_safe);
	WRITE_BOOL_FIELD(is_initplan); /*CDB*/
	WRITE_BOOL_FIELD(is_multirow); /*CDB*/
	WRITE_NODE_FIELD(setParam);
	WRITE_NODE_FIELD(parParam);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(extParam);
	WRITE_FLOAT_FIELD(startup_cost, "%.2f");
	WRITE_FLOAT_FIELD(per_call_cost, "%.2f");
}

static void
_outAlternativeSubPlan(StringInfo str, const AlternativeSubPlan *node)
{
	WRITE_NODE_TYPE("ALTERNATIVESUBPLAN");

	WRITE_NODE_FIELD(subplans);
}

static void
_outFieldSelect(StringInfo str, const FieldSelect *node)
{
	WRITE_NODE_TYPE("FIELDSELECT");

	WRITE_NODE_FIELD(arg);
	WRITE_INT_FIELD(fieldnum);
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
	WRITE_OID_FIELD(resultcollid);
}

static void
_outFieldStore(StringInfo str, const FieldStore *node)
{
	WRITE_NODE_TYPE("FIELDSTORE");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(newvals);
	WRITE_NODE_FIELD(fieldnums);
	WRITE_OID_FIELD(resulttype);
}

static void
_outRelabelType(StringInfo str, const RelabelType *node)
{
	WRITE_NODE_TYPE("RELABELTYPE");

	WRITE_NODE_FIELD(arg);
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(relabelformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceViaIO(StringInfo str, const CoerceViaIO *node)
{
	WRITE_NODE_TYPE("COERCEVIAIO");

	WRITE_NODE_FIELD(arg);
	WRITE_OID_FIELD(resulttype);
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(coerceformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outArrayCoerceExpr(StringInfo str, const ArrayCoerceExpr *node)
{
	WRITE_NODE_TYPE("ARRAYCOERCEEXPR");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(elemexpr);
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(coerceformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outConvertRowtypeExpr(StringInfo str, const ConvertRowtypeExpr *node)
{
	WRITE_NODE_TYPE("CONVERTROWTYPEEXPR");

	WRITE_NODE_FIELD(arg);
	WRITE_OID_FIELD(resulttype);
	WRITE_ENUM_FIELD(convertformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCollateExpr(StringInfo str, const CollateExpr *node)
{
	WRITE_NODE_TYPE("COLLATEEXPR");

	WRITE_NODE_FIELD(arg);
	WRITE_OID_FIELD(collOid);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCaseExpr(StringInfo str, const CaseExpr *node)
{
	WRITE_NODE_TYPE("CASEEXPR");

	WRITE_OID_FIELD(casetype);
	WRITE_OID_FIELD(casecollid);
	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(defresult);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCaseWhen(StringInfo str, const CaseWhen *node)
{
	WRITE_NODE_TYPE("CASEWHEN");

	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(result);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCaseTestExpr(StringInfo str, const CaseTestExpr *node)
{
	WRITE_NODE_TYPE("CASETESTEXPR");

	WRITE_OID_FIELD(typeId);
	WRITE_INT_FIELD(typeMod);
	WRITE_OID_FIELD(collation);
}

static void
_outArrayExpr(StringInfo str, const ArrayExpr *node)
{
	WRITE_NODE_TYPE("ARRAYEXPR");

	WRITE_OID_FIELD(array_typeid);
	WRITE_OID_FIELD(array_collid);
	WRITE_OID_FIELD(element_typeid);
	WRITE_NODE_FIELD(elements);
	WRITE_BOOL_FIELD(multidims);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRowExpr(StringInfo str, const RowExpr *node)
{
	WRITE_NODE_TYPE("ROWEXPR");

	WRITE_NODE_FIELD(args);
	WRITE_OID_FIELD(row_typeid);
	WRITE_ENUM_FIELD(row_format, CoercionForm);
	WRITE_NODE_FIELD(colnames);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRowCompareExpr(StringInfo str, const RowCompareExpr *node)
{
	WRITE_NODE_TYPE("ROWCOMPAREEXPR");

	WRITE_ENUM_FIELD(cmptype, CompareType);
	WRITE_NODE_FIELD(opnos);
	WRITE_NODE_FIELD(opfamilies);
	WRITE_NODE_FIELD(inputcollids);
	WRITE_NODE_FIELD(largs);
	WRITE_NODE_FIELD(rargs);
}

static void
_outCoalesceExpr(StringInfo str, const CoalesceExpr *node)
{
	WRITE_NODE_TYPE("COALESCEEXPR");

	WRITE_OID_FIELD(coalescetype);
	WRITE_OID_FIELD(coalescecollid);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outMinMaxExpr(StringInfo str, const MinMaxExpr *node)
{
	WRITE_NODE_TYPE("MINMAXEXPR");

	WRITE_OID_FIELD(minmaxtype);
	WRITE_OID_FIELD(minmaxcollid);
	WRITE_OID_FIELD(inputcollid);
	WRITE_ENUM_FIELD(op, MinMaxOp);
	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSQLValueFunction(StringInfo str, const SQLValueFunction *node)
{
	WRITE_NODE_TYPE("SQLVALUEFUNCTION");

	WRITE_ENUM_FIELD(op, SQLValueFunctionOp);
	WRITE_OID_FIELD(type);
	WRITE_INT_FIELD(typmod);
	WRITE_LOCATION_FIELD(location);
}

static void
_outXmlExpr(StringInfo str, const XmlExpr *node)
{
	WRITE_NODE_TYPE("XMLEXPR");

	WRITE_ENUM_FIELD(op, XmlExprOp);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(named_args);
	WRITE_NODE_FIELD(arg_names);
	WRITE_NODE_FIELD(args);
	WRITE_ENUM_FIELD(xmloption, XmlOptionType);
	WRITE_OID_FIELD(type);
	WRITE_INT_FIELD(typmod);
	WRITE_LOCATION_FIELD(location);
}

static void
_outNullTest(StringInfo str, const NullTest *node)
{
	WRITE_NODE_TYPE("NULLTEST");

	WRITE_NODE_FIELD(arg);
	WRITE_ENUM_FIELD(nulltesttype, NullTestType);
	WRITE_BOOL_FIELD(argisrow);
	WRITE_LOCATION_FIELD(location);
}

static void
_outBooleanTest(StringInfo str, const BooleanTest *node)
{
	WRITE_NODE_TYPE("BOOLEANTEST");

	WRITE_NODE_FIELD(arg);
	WRITE_ENUM_FIELD(booltesttype, BoolTestType);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceToDomain(StringInfo str, const CoerceToDomain *node)
{
	WRITE_NODE_TYPE("COERCETODOMAIN");

	WRITE_NODE_FIELD(arg);
	WRITE_OID_FIELD(resulttype);
	WRITE_INT_FIELD(resulttypmod);
	WRITE_OID_FIELD(resultcollid);
	WRITE_ENUM_FIELD(coercionformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCoerceToDomainValue(StringInfo str, const CoerceToDomainValue *node)
{
	WRITE_NODE_TYPE("COERCETODOMAINVALUE");

	WRITE_OID_FIELD(typeId);
	WRITE_INT_FIELD(typeMod);
	WRITE_OID_FIELD(collation);
	WRITE_LOCATION_FIELD(location);
}

static void
_outSetToDefault(StringInfo str, const SetToDefault *node)
{
	WRITE_NODE_TYPE("SETTODEFAULT");

	WRITE_OID_FIELD(typeId);
	WRITE_INT_FIELD(typeMod);
	WRITE_OID_FIELD(collation);
	WRITE_LOCATION_FIELD(location);
}


static void
_outNextValueExpr(StringInfo str, const NextValueExpr *node)
{
	WRITE_NODE_TYPE("NEXTVALUEEXPR");

	WRITE_OID_FIELD(seqid);
	WRITE_OID_FIELD(typeId);
}

static void
_outInferenceElem(StringInfo str, const InferenceElem *node)
{
	WRITE_NODE_TYPE("INFERENCEELEM");

	WRITE_NODE_FIELD(expr);
	WRITE_OID_FIELD(infercollid);
	WRITE_OID_FIELD(inferopclass);
}

static void
_outTargetEntry(StringInfo str, const TargetEntry *node)
{
	WRITE_NODE_TYPE("TARGETENTRY");

	WRITE_NODE_FIELD(expr);
	WRITE_INT_FIELD(resno);
	WRITE_STRING_FIELD(resname);
	WRITE_UINT_FIELD(ressortgroupref);
	WRITE_OID_FIELD(resorigtbl);
	WRITE_INT_FIELD(resorigcol);
	WRITE_BOOL_FIELD(resjunk);
}

static void
_outRangeTblRef(StringInfo str, const RangeTblRef *node)
{
	WRITE_NODE_TYPE("RANGETBLREF");

	WRITE_INT_FIELD(rtindex);
}


static void
_outFromExpr(StringInfo str, const FromExpr *node)
{
	WRITE_NODE_TYPE("FROMEXPR");

	WRITE_NODE_FIELD(fromlist);
	WRITE_NODE_FIELD(quals);
}

static void
_outOnConflictExpr(StringInfo str, const OnConflictExpr *node)
{
	WRITE_NODE_TYPE("ONCONFLICTEXPR");

	WRITE_ENUM_FIELD(action, OnConflictAction);
	WRITE_NODE_FIELD(arbiterElems);
	WRITE_NODE_FIELD(arbiterWhere);
	WRITE_OID_FIELD(constraint);
	WRITE_NODE_FIELD(onConflictSet);
	WRITE_NODE_FIELD(onConflictWhere);
	WRITE_INT_FIELD(exclRelIndex);
	WRITE_NODE_FIELD(exclRelTlist);
}

/* 'flow' is only needed during planning. */

/*****************************************************************************
 *
 *	Stuff from cdbpathlocus.h.
 *
 *****************************************************************************/

/*
 * _outCdbPathLocus
 */

static void
_outJsonFormat(StringInfo str, const JsonFormat *node)
{
	WRITE_NODE_TYPE("JSONFORMAT");

	WRITE_ENUM_FIELD(format_type, JsonFormatType);
	WRITE_ENUM_FIELD(encoding, JsonEncoding);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonReturning(StringInfo str, const JsonReturning *node)
{
	WRITE_NODE_TYPE("JSONRETURNING");

	WRITE_NODE_FIELD(format);
	WRITE_OID_FIELD(typid);
	WRITE_INT_FIELD(typmod);
}

static void
_outJsonValueExpr(StringInfo str, const JsonValueExpr *node)
{
	WRITE_NODE_TYPE("JSONVALUEEXPR");

	WRITE_NODE_FIELD(raw_expr);
	WRITE_NODE_FIELD(formatted_expr);
	WRITE_NODE_FIELD(format);
}

static void
_outJsonOutput(StringInfo str, const JsonOutput *node)
{
	WRITE_NODE_TYPE("JSONOUTPUT");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(returning);
}

/*
 * GPDB: CREATE/ALTER PUBLICATION is dispatched to the segments as a parsed
 * statement, so its publication-object specifications must be serializable.
 */
static void
_outPublicationObjSpec(StringInfo str, const PublicationObjSpec *node)
{
	WRITE_NODE_TYPE("PUBLICATIONOBJSPEC");

	WRITE_ENUM_FIELD(pubobjtype, PublicationObjSpecType);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(pubtable);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPublicationTable(StringInfo str, const PublicationTable *node)
{
	WRITE_NODE_TYPE("PUBLICATIONTABLE");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(columns);
}

static void
_outJsonConstructorExpr(StringInfo str, const JsonConstructorExpr *node)
{
	WRITE_NODE_TYPE("JSONCONSTRUCTOREXPR");

	WRITE_ENUM_FIELD(type, JsonConstructorType);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(func);
	WRITE_NODE_FIELD(coercion);
	WRITE_NODE_FIELD(returning);
	WRITE_BOOL_FIELD(absent_on_null);
	WRITE_BOOL_FIELD(unique);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonIsPredicate(StringInfo str, const JsonIsPredicate *node)
{
	WRITE_NODE_TYPE("JSONISPREDICATE");

	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(format);
	WRITE_ENUM_FIELD(item_type, JsonValueType);
	WRITE_BOOL_FIELD(unique_keys);
	WRITE_LOCATION_FIELD(location);
}

/*
 * GPDB: PG17 SQL/JSON query (JSON_EXISTS/JSON_QUERY/JSON_VALUE), JSON_TABLE,
 * JSON()/JSON_SCALAR()/JSON_SERIALIZE(), plus the MERGE and window run-condition
 * expression nodes. These can appear in dispatched plans/expressions, so the
 * hand-maintained binary serializers must handle them. Field order and types
 * mirror the auto-generated _out* functions in outfuncs.funcs.c exactly; the
 * matching readers live in readfast.c and MUST stay in lockstep.
 */
static void
_outWindowFuncRunCondition(StringInfo str, const WindowFuncRunCondition *node)
{
	WRITE_NODE_TYPE("WINDOWFUNCRUNCONDITION");

	WRITE_OID_FIELD(opno);
	WRITE_OID_FIELD(inputcollid);
	WRITE_BOOL_FIELD(wfunc_left);
	WRITE_NODE_FIELD(arg);
}

static void
_outMergeSupportFunc(StringInfo str, const MergeSupportFunc *node)
{
	WRITE_NODE_TYPE("MERGESUPPORTFUNC");

	WRITE_OID_FIELD(msftype);
	WRITE_OID_FIELD(msfcollid);
	WRITE_LOCATION_FIELD(location);
}

static void
_outReturningExpr(StringInfo str, const ReturningExpr *node)
{
	WRITE_NODE_TYPE("RETURNINGEXPR");

	WRITE_INT_FIELD(retlevelsup);
	WRITE_BOOL_FIELD(retold);
	WRITE_NODE_FIELD(retexpr);
}

static void
_outJsonBehavior(StringInfo str, const JsonBehavior *node)
{
	WRITE_NODE_TYPE("JSONBEHAVIOR");

	WRITE_ENUM_FIELD(btype, JsonBehaviorType);
	WRITE_NODE_FIELD(expr);
	WRITE_BOOL_FIELD(coerce);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonExpr(StringInfo str, const JsonExpr *node)
{
	WRITE_NODE_TYPE("JSONEXPR");

	WRITE_ENUM_FIELD(op, JsonExprOp);
	WRITE_STRING_FIELD(column_name);
	WRITE_NODE_FIELD(formatted_expr);
	WRITE_NODE_FIELD(format);
	WRITE_NODE_FIELD(path_spec);
	WRITE_NODE_FIELD(returning);
	WRITE_NODE_FIELD(passing_names);
	WRITE_NODE_FIELD(passing_values);
	WRITE_NODE_FIELD(on_empty);
	WRITE_NODE_FIELD(on_error);
	WRITE_BOOL_FIELD(use_io_coercion);
	WRITE_BOOL_FIELD(use_json_coercion);
	WRITE_ENUM_FIELD(wrapper, JsonWrapper);
	WRITE_BOOL_FIELD(omit_quotes);
	WRITE_OID_FIELD(collation);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonTablePath(StringInfo str, const JsonTablePath *node)
{
	WRITE_NODE_TYPE("JSONTABLEPATH");

	WRITE_NODE_FIELD(value);
	WRITE_STRING_FIELD(name);
}

static void
_outJsonTablePathScan(StringInfo str, const JsonTablePathScan *node)
{
	WRITE_NODE_TYPE("JSONTABLEPATHSCAN");

	WRITE_NODE_FIELD(path);
	WRITE_BOOL_FIELD(errorOnError);
	WRITE_NODE_FIELD(child);
	WRITE_INT_FIELD(colMin);
	WRITE_INT_FIELD(colMax);
}

static void
_outJsonTableSiblingJoin(StringInfo str, const JsonTableSiblingJoin *node)
{
	WRITE_NODE_TYPE("JSONTABLESIBLINGJOIN");

	WRITE_NODE_FIELD(lplan);
	WRITE_NODE_FIELD(rplan);
}

static void
_outJsonArgument(StringInfo str, const JsonArgument *node)
{
	WRITE_NODE_TYPE("JSONARGUMENT");

	WRITE_NODE_FIELD(val);
	WRITE_STRING_FIELD(name);
}

static void
_outJsonFuncExpr(StringInfo str, const JsonFuncExpr *node)
{
	WRITE_NODE_TYPE("JSONFUNCEXPR");

	WRITE_ENUM_FIELD(op, JsonExprOp);
	WRITE_STRING_FIELD(column_name);
	WRITE_NODE_FIELD(context_item);
	WRITE_NODE_FIELD(pathspec);
	WRITE_NODE_FIELD(passing);
	WRITE_NODE_FIELD(output);
	WRITE_NODE_FIELD(on_empty);
	WRITE_NODE_FIELD(on_error);
	WRITE_ENUM_FIELD(wrapper, JsonWrapper);
	WRITE_ENUM_FIELD(quotes, JsonQuotes);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonTablePathSpec(StringInfo str, const JsonTablePathSpec *node)
{
	WRITE_NODE_TYPE("JSONTABLEPATHSPEC");

	WRITE_NODE_FIELD(string);
	WRITE_STRING_FIELD(name);
	WRITE_LOCATION_FIELD(name_location);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonTable(StringInfo str, const JsonTable *node)
{
	WRITE_NODE_TYPE("JSONTABLE");

	WRITE_NODE_FIELD(context_item);
	WRITE_NODE_FIELD(pathspec);
	WRITE_NODE_FIELD(passing);
	WRITE_NODE_FIELD(columns);
	WRITE_NODE_FIELD(on_error);
	WRITE_NODE_FIELD(alias);
	WRITE_BOOL_FIELD(lateral);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonTableColumn(StringInfo str, const JsonTableColumn *node)
{
	WRITE_NODE_TYPE("JSONTABLECOLUMN");

	WRITE_ENUM_FIELD(coltype, JsonTableColumnType);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(pathspec);
	WRITE_NODE_FIELD(format);
	WRITE_ENUM_FIELD(wrapper, JsonWrapper);
	WRITE_ENUM_FIELD(quotes, JsonQuotes);
	WRITE_NODE_FIELD(columns);
	WRITE_NODE_FIELD(on_empty);
	WRITE_NODE_FIELD(on_error);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonParseExpr(StringInfo str, const JsonParseExpr *node)
{
	WRITE_NODE_TYPE("JSONPARSEEXPR");

	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(output);
	WRITE_BOOL_FIELD(unique_keys);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonScalarExpr(StringInfo str, const JsonScalarExpr *node)
{
	WRITE_NODE_TYPE("JSONSCALAREXPR");

	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(output);
	WRITE_LOCATION_FIELD(location);
}

static void
_outJsonSerializeExpr(StringInfo str, const JsonSerializeExpr *node)
{
	WRITE_NODE_TYPE("JSONSERIALIZEEXPR");

	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(output);
	WRITE_LOCATION_FIELD(location);
}

/*****************************************************************************
 *
 *	Stuff from pathnodes.h.
 *
 *****************************************************************************/

/*
 * None of this stuff is needed after planning, and doesn't need to be
 * dispatched to QEs.
 */


static void
_outRestrictInfo(StringInfo str, const RestrictInfo *node)
{
	WRITE_NODE_TYPE("RESTRICTINFO");

	/* NB: this isn't a complete set of fields */
	WRITE_NODE_FIELD(clause);
	WRITE_BOOL_FIELD(is_pushed_down);
	WRITE_BOOL_FIELD(can_join);
	WRITE_BOOL_FIELD(pseudoconstant);
	WRITE_BOOL_FIELD(has_clone);
	WRITE_BOOL_FIELD(is_clone);
	WRITE_BOOL_FIELD(leakproof);
	WRITE_ENUM_FIELD(has_volatile, VolatileFunctionStatus);
	WRITE_UINT_FIELD(security_level);
	WRITE_BOOL_FIELD(contain_outer_query_references);
	WRITE_BITMAPSET_FIELD(clause_relids);
	WRITE_BITMAPSET_FIELD(required_relids);
	WRITE_BITMAPSET_FIELD(outer_relids);
	WRITE_BITMAPSET_FIELD(incompatible_relids);
	WRITE_BITMAPSET_FIELD(left_relids);
	WRITE_BITMAPSET_FIELD(right_relids);
	WRITE_NODE_FIELD(orclause);
	/* don't write parent_ec, leads to infinite recursion in plan tree dump */
	WRITE_FLOAT_FIELD(norm_selec, "%.4f");
	WRITE_FLOAT_FIELD(outer_selec, "%.4f");
	WRITE_NODE_FIELD(mergeopfamilies);
	/* don't write left_ec, leads to infinite recursion in plan tree dump */
	/* don't write right_ec, leads to infinite recursion in plan tree dump */
	WRITE_NODE_FIELD(left_em);
	WRITE_NODE_FIELD(right_em);
	WRITE_BOOL_FIELD(outer_is_left);
	WRITE_OID_FIELD(hashjoinoperator);
	WRITE_OID_FIELD(left_hasheqoperator);
	WRITE_OID_FIELD(right_hasheqoperator);
}


static void
_outAppendRelInfo(StringInfo str, const AppendRelInfo *node)
{
	WRITE_NODE_TYPE("APPENDRELINFO");

	WRITE_UINT_FIELD(parent_relid);
	WRITE_UINT_FIELD(child_relid);
	WRITE_OID_FIELD(parent_reltype);
	WRITE_OID_FIELD(child_reltype);
	WRITE_NODE_FIELD(translated_vars);
	WRITE_INT_FIELD(num_child_cols);
	WRITE_ATTRNUMBER_ARRAY(parent_colnos, node->num_child_cols);
	WRITE_OID_FIELD(parent_reloid);
}


/*****************************************************************************
 *
 *	Stuff from extensible.h
 *
 *****************************************************************************/


/*****************************************************************************
 *
 *	Stuff from parsenodes.h.
 *
 *****************************************************************************/

/*
 * print the basic stuff of all nodes that inherit from CreateStmt
 */
static void
_outCreateStmtInfo(StringInfo str, const CreateStmt *node)
{
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(inhRelations);
    WRITE_NODE_FIELD(partspec);
    WRITE_NODE_FIELD(partbound);
	WRITE_NODE_FIELD(ofTypename);
	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(oncommit, OnCommitAction);
	WRITE_STRING_FIELD(tablespacename);
	WRITE_STRING_FIELD(accessMethod);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_BOOL_FIELD(gp_style_alter_part);

	WRITE_NODE_FIELD(distributedBy);
	WRITE_NODE_FIELD(partitionBy);
	WRITE_CHAR_FIELD(relKind);
	WRITE_OID_FIELD(ownerid);
	WRITE_BOOL_FIELD(buildAoBlkdir);
	WRITE_NODE_FIELD(attr_encodings);
	WRITE_BOOL_FIELD(isCtas);
	WRITE_NODE_FIELD(intoQuery);
	WRITE_NODE_FIELD(intoPolicy);

	WRITE_NODE_FIELD(part_idx_oids);
	WRITE_NODE_FIELD(part_idx_names);

	/*
	 * Some extra checks to make sure we didn't get lost
	 * during serialization/deserialization
	 */
	Assert(node->relKind != 0);
	Assert(node->oncommit <= ONCOMMIT_DROP);
}

static void
_outCreateStmt(StringInfo str, const CreateStmt *node)
{
	WRITE_NODE_TYPE("CREATESTMT");

	_outCreateStmtInfo(str, (const CreateStmt *) node);
}

static void
_outCreateForeignTableStmt(StringInfo str, const CreateForeignTableStmt *node)
{
	WRITE_NODE_TYPE("CREATEFOREIGNTABLESTMT");

	_outCreateStmtInfo(str, (const CreateStmt *) node);

	WRITE_STRING_FIELD(servername);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(distributedBy);
}

static void
_outDistributionKeyElem(StringInfo str, const DistributionKeyElem *node)
{
	WRITE_NODE_TYPE("DISTRIBUTIONKEYELEM");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(opclass);
	WRITE_LOCATION_FIELD(location);
}

static void
_outColumnReferenceStorageDirective(StringInfo str, const ColumnReferenceStorageDirective *node)
{
	WRITE_NODE_TYPE("COLUMNREFERENCESTORAGEDIRECTIVE");

	WRITE_STRING_FIELD(column);
	WRITE_BOOL_FIELD(deflt);
	WRITE_NODE_FIELD(encoding);
}

static void
_outExtTableTypeDesc(StringInfo str, const ExtTableTypeDesc *node)
{
	WRITE_NODE_TYPE("EXTTABLETYPEDESC");

	WRITE_ENUM_FIELD(exttabletype, ExtTableType);
	WRITE_NODE_FIELD(location_list);
	WRITE_NODE_FIELD(on_clause);
	WRITE_STRING_FIELD(command_string);
}

static void
_outCreateExternalStmt(StringInfo str, const CreateExternalStmt *node)
{
	WRITE_NODE_TYPE("CREATEEXTERNALSTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(exttypedesc);
	WRITE_STRING_FIELD(format);
	WRITE_NODE_FIELD(formatOpts);
	WRITE_BOOL_FIELD(isweb);
	WRITE_BOOL_FIELD(iswritable);
	WRITE_NODE_FIELD(sreh);
	WRITE_NODE_FIELD(extOptions);
	WRITE_NODE_FIELD(encoding);
	WRITE_NODE_FIELD(distributedBy);
}

static void
_outDistributedBy(StringInfo str, const DistributedBy *node)
{
	WRITE_NODE_TYPE("DISTRIBUTEDBY");

	WRITE_ENUM_FIELD(ptype, GpPolicyType);
	WRITE_INT_FIELD(numsegments);
	WRITE_NODE_FIELD(keyCols);
}


static void
_outImportForeignSchemaStmt(StringInfo str, const ImportForeignSchemaStmt *node)
{
	WRITE_NODE_TYPE("IMPORTFOREIGNSCHEMASTMT");

	WRITE_STRING_FIELD(server_name);
	WRITE_STRING_FIELD(remote_schema);
	WRITE_STRING_FIELD(local_schema);
	WRITE_ENUM_FIELD(list_type, ImportForeignSchemaType);
	WRITE_NODE_FIELD(table_list);
	WRITE_NODE_FIELD(options);
}

static void
_outIndexStmt(StringInfo str, const IndexStmt *node)
{
	WRITE_NODE_TYPE("INDEXSTMT");

	WRITE_STRING_FIELD(idxname);
	WRITE_NODE_FIELD(relation);
	WRITE_OID_FIELD(relationOid);
	WRITE_STRING_FIELD(accessMethod);
	WRITE_STRING_FIELD(tableSpace);
	WRITE_NODE_FIELD(indexParams);
	WRITE_NODE_FIELD(indexIncludingParams);
	WRITE_NODE_FIELD(options);

	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(excludeOpNames);
	WRITE_STRING_FIELD(idxcomment);
	WRITE_OID_FIELD(indexOid);
	WRITE_OID_FIELD(oldNumber);
	WRITE_UINT_FIELD(oldCreateSubid);
	WRITE_UINT_FIELD(oldFirstRelfilelocatorSubid);
	WRITE_BOOL_FIELD(unique);
	WRITE_BOOL_FIELD(nulls_not_distinct);
	WRITE_BOOL_FIELD(primary);
	WRITE_BOOL_FIELD(isconstraint);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_BOOL_FIELD(transformed);
	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_BOOL_FIELD(reset_default_tblspc);
}

static void
_outReindexStmt(StringInfo str, const ReindexStmt *node)
{
	WRITE_NODE_TYPE("REINDEXSTMT");

	WRITE_ENUM_FIELD(kind,ReindexObjectType);
	WRITE_NODE_FIELD(relation);
	WRITE_STRING_FIELD(name);
	WRITE_OID_FIELD(relid);
}


static void
_outViewStmt(StringInfo str, const ViewStmt *node)
{
	WRITE_NODE_TYPE("VIEWSTMT");

	WRITE_NODE_FIELD(view);
	WRITE_NODE_FIELD(aliases);
	WRITE_NODE_FIELD(query);
	WRITE_BOOL_FIELD(replace);
	WRITE_NODE_FIELD(options);
}

static void
_outRuleStmt(StringInfo str, const RuleStmt *node)
{
	WRITE_NODE_TYPE("RULESTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_STRING_FIELD(rulename);
	WRITE_NODE_FIELD(whereClause);
	WRITE_ENUM_FIELD(event, CmdType);
	WRITE_BOOL_FIELD(instead);
	WRITE_NODE_FIELD(actions);
	WRITE_BOOL_FIELD(replace);
}

static void
_outDropStmt(StringInfo str, const DropStmt *node)
{
	WRITE_NODE_TYPE("DROPSTMT");

	WRITE_NODE_FIELD(objects);
	WRITE_ENUM_FIELD(removeType, ObjectType);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_BOOL_FIELD(concurrent);
}

static void
_outDropOwnedStmt(StringInfo str, const DropOwnedStmt *node)
{
	WRITE_NODE_TYPE("DROPOWNEDSTMT");

	WRITE_NODE_FIELD(roles);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outReassignOwnedStmt(StringInfo str, const ReassignOwnedStmt *node)
{
	WRITE_NODE_TYPE("REASSIGNOWNEDSTMT");

	WRITE_NODE_FIELD(roles);
	WRITE_NODE_FIELD(newrole);
}

static void
_outTruncateStmt(StringInfo str, const TruncateStmt *node)
{
	WRITE_NODE_TYPE("TRUNCATESTMT");

	WRITE_NODE_FIELD(relations);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outReplicaIdentityStmt(StringInfo str, const ReplicaIdentityStmt *node)
{
	WRITE_NODE_TYPE("REPLICAIDENTITYSTMT");

	WRITE_CHAR_FIELD(identity_type);
	WRITE_STRING_FIELD(name);
}

static void
_outAlterTableStmt(StringInfo str, const AlterTableStmt *node)
{
	WRITE_NODE_TYPE("ALTERTABLESTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(cmds);
	WRITE_ENUM_FIELD(objtype, ObjectType);

	WRITE_INT_FIELD(lockmode);
	/*
	 * AlteredTableInfos are not Nodes in upstream, so make sure the node tags
	 * are set correctly before trying to serialize them.
	 */
	ListCell   *lc;
	foreach(lc, node->wqueue)
	{
		AlteredTableInfo *e = (AlteredTableInfo *) lfirst(lc);
		e->type = T_AlteredTableInfo;
	}
	WRITE_NODE_FIELD(wqueue);
}

static void
_outAlterTableCmd(StringInfo str, const AlterTableCmd *node)
{
	WRITE_NODE_TYPE("ALTERTABLECMD");

	WRITE_ENUM_FIELD(subtype, AlterTableType);
	WRITE_STRING_FIELD(name);
	WRITE_INT_FIELD(num);
	WRITE_NODE_FIELD(newowner);
	WRITE_NODE_FIELD(def);
	WRITE_NODE_FIELD(transform);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_BOOL_FIELD(recurse);

	WRITE_INT_FIELD(backendId);
	WRITE_NODE_FIELD(policy);
	WRITE_NODE_FIELD(prepStmts);
	WRITE_NODE_FIELD(execStmts);
	WRITE_BOOL_FIELD(isNULL);
}

static void
wrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		char	   *str = (char *) lfirst(lc);

		lfirst(lc) = makeString(str);
	}
}

static void
unwrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		String	   *val = (String *) lfirst(lc);

		lfirst(lc) = strVal(val);
		pfree(val);
	}
}

static void
_outAlteredTableInfo(StringInfo str, const AlteredTableInfo *node)
{
	ListCell   *lc;

	WRITE_NODE_TYPE("ALTEREDTABLEINFO");

	WRITE_OID_FIELD(relid);
	WRITE_CHAR_FIELD(relkind);
	/* oldDesc is omitted */

	for (int i = 0; i < AT_NUM_PASSES; i++)
		WRITE_NODE_FIELD(subcmds[i]);

	/*
	 * These aren't Nodes in upstream, so make sure the node tags
	 * are set correctly before trying to serialize them.
	 */
	foreach(lc, node->constraints)
	{
		NewConstraint *e = (NewConstraint *) lfirst(lc);
		e->type = T_NewConstraint;
	}
	foreach(lc, node->newvals)
	{
		NewColumnValue *e = (NewColumnValue *) lfirst(lc);
		e->type = T_NewColumnValue;
	}

	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(newvals);
	WRITE_NODE_FIELD(afterStmts);
	WRITE_BOOL_FIELD(verify_new_notnull);
	WRITE_INT_FIELD(rewrite);
	WRITE_OID_FIELD(newAccessMethod);
	WRITE_BOOL_FIELD(dist_opfamily_changed);
	WRITE_OID_FIELD(new_opclass);
	WRITE_OID_FIELD(newTableSpace);
	WRITE_BOOL_FIELD(chgPersistence);
	WRITE_CHAR_FIELD(newrelpersistence);
	WRITE_NODE_FIELD(partition_constraint);
	WRITE_BOOL_FIELD(validate_default);
	WRITE_NODE_FIELD(changedConstraintOids);

	/* node->changedConstraintDefs is a list of naked strings, so
	 * we can't use WRITE_NODE_FIELD on it. Temporarily wrap them in Values.
	 */
	wrapStringList(node->changedConstraintDefs);
	WRITE_NODE_FIELD(changedConstraintDefs);
	/* unwrap them again */
	unwrapStringList(node->changedConstraintDefs);

	WRITE_NODE_FIELD(changedIndexOids);
	wrapStringList(node->changedIndexDefs);
	WRITE_NODE_FIELD(changedIndexDefs);
	unwrapStringList(node->changedIndexDefs);
}

static void
_outNewConstraint(StringInfo str, const NewConstraint *node)
{
	WRITE_NODE_TYPE("NEWCONSTRAINT");

	WRITE_STRING_FIELD(name);
	WRITE_ENUM_FIELD(contype, ConstrType);
	WRITE_OID_FIELD(refrelid);
	WRITE_OID_FIELD(refindid);
	WRITE_OID_FIELD(conid);
	WRITE_NODE_FIELD(qual);
	/* can't serialize qualstate */
}

static void
_outNewColumnValue(StringInfo str, const NewColumnValue *node)
{
	WRITE_NODE_TYPE("NEWCOLUMNVALUE");

	WRITE_INT_FIELD(attnum);
	WRITE_NODE_FIELD(expr);
	/* can't serialize exprstate */
	WRITE_BOOL_FIELD(is_generated);
}

static void
_outCreateRoleStmt(StringInfo str, const CreateRoleStmt *node)
{
	WRITE_NODE_TYPE("CREATEROLESTMT");

	WRITE_ENUM_FIELD(stmt_type, RoleStmtType);
	WRITE_STRING_FIELD(role);
	WRITE_NODE_FIELD(options);
}

static void
_outDenyLoginInterval(StringInfo str, const DenyLoginInterval *node)
{
	WRITE_NODE_TYPE("DENYLOGININTERVAL");

	WRITE_NODE_FIELD(start);
	WRITE_NODE_FIELD(end);
}

static void
_outDenyLoginPoint(StringInfo str, const DenyLoginPoint *node)
{
	WRITE_NODE_TYPE("DENYLOGINPOINT");

	WRITE_NODE_FIELD(day);
	WRITE_NODE_FIELD(time);
}

static  void
_outDropRoleStmt(StringInfo str, const DropRoleStmt *node)
{
	WRITE_NODE_TYPE("DROPROLESTMT");

	WRITE_NODE_FIELD(roles);
	WRITE_BOOL_FIELD(missing_ok);
}

static  void
_outAlterRoleStmt(StringInfo str, const AlterRoleStmt *node)
{
	WRITE_NODE_TYPE("ALTERROLESTMT");

	WRITE_NODE_FIELD(role);
	WRITE_NODE_FIELD(options);
	WRITE_INT_FIELD(action);
}

static  void
_outAlterRoleSetStmt(StringInfo str, const AlterRoleSetStmt *node)
{
	WRITE_NODE_TYPE("ALTERROLESETSTMT");

	WRITE_NODE_FIELD(role);
	WRITE_NODE_FIELD(setstmt);
}

static  void
_outAlterSystemStmt(StringInfo str, const AlterSystemStmt *node)
{
	WRITE_NODE_TYPE("ALTERSYSTEMSTMT");

	WRITE_NODE_FIELD(setstmt);
}

static  void
_outAlterOwnerStmt(StringInfo str, const AlterOwnerStmt *node)
{
	WRITE_NODE_TYPE("ALTEROWNERSTMT");

	WRITE_ENUM_FIELD(objectType,ObjectType);
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(object);
	WRITE_NODE_FIELD(newowner);
}


static void
_outRenameStmt(StringInfo str, const RenameStmt *node)
{
	WRITE_NODE_TYPE("RENAMESTMT");

	WRITE_ENUM_FIELD(renameType, ObjectType);
	WRITE_ENUM_FIELD(relationType, ObjectType);
	WRITE_NODE_FIELD(relation);
	WRITE_OID_FIELD(objid);
	WRITE_NODE_FIELD(object);
	WRITE_STRING_FIELD(subname);
	WRITE_STRING_FIELD(newname);
	WRITE_ENUM_FIELD(behavior,DropBehavior);

	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterObjectSchemaStmt(StringInfo str, const AlterObjectSchemaStmt *node)
{
	WRITE_NODE_TYPE("ALTEROBJECTSCHEMASTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(object);
	WRITE_STRING_FIELD(newschema);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_ENUM_FIELD(objectType,ObjectType);
}

static void
_outCreateSeqStmt(StringInfo str, const CreateSeqStmt *node)
{
	WRITE_NODE_TYPE("CREATESEQSTMT");
	WRITE_NODE_FIELD(sequence);
	WRITE_NODE_FIELD(options);
	WRITE_OID_FIELD(ownerId);
	WRITE_BOOL_FIELD(for_identity);
	WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outAlterSeqStmt(StringInfo str, const AlterSeqStmt *node)
{
	WRITE_NODE_TYPE("ALTERSEQSTMT");
	WRITE_NODE_FIELD(sequence);
	WRITE_NODE_FIELD(options);
	WRITE_BOOL_FIELD(for_identity);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outClusterStmt(StringInfo str, const ClusterStmt *node)
{
	WRITE_NODE_TYPE("CLUSTERSTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_STRING_FIELD(indexname);
}

static void
_outCreatedbStmt(StringInfo str, const CreatedbStmt *node)
{
	WRITE_NODE_TYPE("CREATEDBSTMT");
	WRITE_STRING_FIELD(dbname);
	WRITE_NODE_FIELD(options);
}

static void
_outDropdbStmt(StringInfo str, const DropdbStmt *node)
{
	WRITE_NODE_TYPE("DROPDBSTMT");
	WRITE_STRING_FIELD(dbname);
	WRITE_BOOL_FIELD(missing_ok);
}



static void
_outCreateFunctionStmt(StringInfo str, const CreateFunctionStmt *node)
{
	WRITE_NODE_TYPE("CREATEFUNCSTMT");
	WRITE_BOOL_FIELD(is_procedure);
	WRITE_BOOL_FIELD(replace);
	WRITE_NODE_FIELD(funcname);
	WRITE_NODE_FIELD(parameters);
	WRITE_NODE_FIELD(returnType);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(sql_body);
}

static void
_outFunctionParameter(StringInfo str, const FunctionParameter *node)
{
	WRITE_NODE_TYPE("FUNCTIONPARAMETER");
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(argType);
	WRITE_ENUM_FIELD(mode, FunctionParameterMode);
	WRITE_NODE_FIELD(defexpr);
}

static void
_outAlterFunctionStmt(StringInfo str, const AlterFunctionStmt *node)
{
	WRITE_NODE_TYPE("ALTERFUNCTIONSTMT");
	WRITE_ENUM_FIELD(objtype,ObjectType);
	WRITE_NODE_FIELD(func);
	WRITE_NODE_FIELD(actions);
}

static void
_outSegfileMapNode(StringInfo str, const SegfileMapNode *node)
{
	WRITE_NODE_TYPE("SEGFILEMAPNODE");

	WRITE_OID_FIELD(relid);
	WRITE_INT_FIELD(segno);
}


static void
_outDefineStmt(StringInfo str, const DefineStmt *node)
{
	WRITE_NODE_TYPE("DEFINESTMT");
	WRITE_ENUM_FIELD(kind, ObjectType);
	WRITE_BOOL_FIELD(oldstyle);
	WRITE_NODE_FIELD(defnames);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(definition);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_BOOL_FIELD(replace);
	WRITE_BOOL_FIELD(trusted);  /* CDB */
}

static void
_outCompositeTypeStmt(StringInfo str, const CompositeTypeStmt *node)
{
	WRITE_NODE_TYPE("COMPTYPESTMT");

	WRITE_NODE_FIELD(typevar);
	WRITE_NODE_FIELD(coldeflist);
}

static void
_outCreateEnumStmt(StringInfo str, const CreateEnumStmt *node)
{
	WRITE_NODE_TYPE("CREATEENUMSTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(vals);
}

static void
_outCreateRangeStmt(StringInfo str, const CreateRangeStmt *node)
{
	WRITE_NODE_TYPE("CREATERANGESTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(params);
}

static void
_outCreateCastStmt(StringInfo str, const CreateCastStmt *node)
{
	WRITE_NODE_TYPE("CREATECAST");
	WRITE_NODE_FIELD(sourcetype);
	WRITE_NODE_FIELD(targettype);
	WRITE_NODE_FIELD(func);
	WRITE_ENUM_FIELD(context, CoercionContext);
	WRITE_BOOL_FIELD(inout);
}

static void
_outCreateOpClassStmt(StringInfo str, const CreateOpClassStmt *node)
{
	WRITE_NODE_TYPE("CREATEOPCLASS");
	WRITE_NODE_FIELD(opclassname);
	WRITE_NODE_FIELD(opfamilyname);
	WRITE_STRING_FIELD(amname);
	WRITE_NODE_FIELD(datatype);
	WRITE_NODE_FIELD(items);
	WRITE_BOOL_FIELD(isDefault);
}

static void
_outCreateOpClassItem(StringInfo str, const CreateOpClassItem *node)
{
	WRITE_NODE_TYPE("CREATEOPCLASSITEM");
	WRITE_INT_FIELD(itemtype);
	WRITE_NODE_FIELD(name);
	WRITE_INT_FIELD(number);
	WRITE_NODE_FIELD(order_family);
	WRITE_NODE_FIELD(class_args);
	WRITE_NODE_FIELD(storedtype);
}

static void
_outCreateOpFamilyStmt(StringInfo str, const CreateOpFamilyStmt *node)
{
	WRITE_NODE_TYPE("CREATEOPFAMILY");
	WRITE_NODE_FIELD(opfamilyname);
	WRITE_STRING_FIELD(amname);
}

static void
_outAlterOpFamilyStmt(StringInfo str, const AlterOpFamilyStmt *node)
{
	WRITE_NODE_TYPE("ALTEROPFAMILY");
	WRITE_NODE_FIELD(opfamilyname);
	WRITE_STRING_FIELD(amname);
	WRITE_BOOL_FIELD(isDrop);
	WRITE_NODE_FIELD(items);
}

static void
_outCreatePolicyStmt(StringInfo str, const CreatePolicyStmt *node)
{
	WRITE_NODE_TYPE("CREATEPOLICYSTMT");

	WRITE_STRING_FIELD(policy_name);
	WRITE_NODE_FIELD(table);
	WRITE_STRING_FIELD(cmd_name);
	WRITE_BOOL_FIELD(permissive);
	WRITE_NODE_FIELD(roles);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(with_check);
}

static void
_outAlterPolicyStmt(StringInfo str, const AlterPolicyStmt *node)
{
	WRITE_NODE_TYPE("ALTERPOLICYSTMT");

	WRITE_STRING_FIELD(policy_name);
	WRITE_NODE_FIELD(table);
	WRITE_NODE_FIELD(roles);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(with_check);
}

static void
_outCreateTransformStmt(StringInfo str, const CreateTransformStmt *node)
{
	WRITE_NODE_TYPE("CREATETRANSFORMSTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_NODE_FIELD(type_name);
	WRITE_STRING_FIELD(lang);
	WRITE_NODE_FIELD(fromsql);
	WRITE_NODE_FIELD(tosql);
}

static void
_outCreateConversionStmt(StringInfo str, const CreateConversionStmt *node)
{
	WRITE_NODE_TYPE("CREATECONVERSION");
	WRITE_NODE_FIELD(conversion_name);
	WRITE_STRING_FIELD(for_encoding_name);
	WRITE_STRING_FIELD(to_encoding_name);
	WRITE_NODE_FIELD(func_name);
	WRITE_BOOL_FIELD(def);
}

static void
_outTransactionStmt(StringInfo str, const TransactionStmt *node)
{
	WRITE_NODE_TYPE("TRANSACTIONSTMT");

	WRITE_ENUM_FIELD(kind, TransactionStmtKind);
	WRITE_NODE_FIELD(options);
}

static void
_outCreateStatsStmt(StringInfo str, const CreateStatsStmt *node)
{
	WRITE_NODE_TYPE("CREATESTATSSTMT");

	WRITE_NODE_FIELD(defnames);
	WRITE_NODE_FIELD(stat_types);
	WRITE_NODE_FIELD(exprs);
	WRITE_NODE_FIELD(relations);
	WRITE_STRING_FIELD(stxcomment);
	WRITE_BOOL_FIELD(transformed);
	WRITE_BOOL_FIELD(if_not_exists);
}

static void
_outAlterStatsStmt(StringInfo str, const AlterStatsStmt *node)
{
	WRITE_NODE_TYPE("ALTERSTATSSTMT");

	WRITE_NODE_FIELD(defnames);
	WRITE_INT_FIELD(stxstattarget);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outNotifyStmt(StringInfo str, const NotifyStmt *node)
{
	WRITE_NODE_TYPE("NOTIFYSTMT");

	WRITE_STRING_FIELD(conditionname);
	WRITE_STRING_FIELD(payload);
}

static void
_outDeclareCursorStmt(StringInfo str, const DeclareCursorStmt *node)
{
	WRITE_NODE_TYPE("DECLARECURSORSTMT");

	WRITE_STRING_FIELD(portalname);
	WRITE_INT_FIELD(options);
	WRITE_NODE_FIELD(query);
}

static void
_outSingleRowErrorDesc(StringInfo str, const SingleRowErrorDesc *node)
{
	WRITE_NODE_TYPE("SINGLEROWERRORDESC");
	WRITE_INT_FIELD(rejectlimit);
	WRITE_BOOL_FIELD(is_limit_in_rows);
	WRITE_CHAR_FIELD(log_error_type);
}



static void
_outGrantStmt(StringInfo str, const GrantStmt *node)
{
	WRITE_NODE_TYPE("GRANTSTMT");
	WRITE_BOOL_FIELD(is_grant);
	WRITE_ENUM_FIELD(targtype,GrantTargetType);
	WRITE_ENUM_FIELD(objtype,ObjectType);
	WRITE_NODE_FIELD(objects);
	WRITE_NODE_FIELD(privileges);
	WRITE_NODE_FIELD(grantees);
	WRITE_BOOL_FIELD(grant_option);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outObjectWithArgs(StringInfo str, const ObjectWithArgs *node)
{
	WRITE_NODE_TYPE("OBJECTWITHARGS");
	WRITE_NODE_FIELD(objname);
	WRITE_NODE_FIELD(objargs);
	WRITE_BOOL_FIELD(args_unspecified);
}

static void
_outGrantRoleStmt(StringInfo str, const GrantRoleStmt *node)
{
	WRITE_NODE_TYPE("GRANTROLESTMT");
	WRITE_NODE_FIELD(granted_roles);
	WRITE_NODE_FIELD(grantee_roles);
	WRITE_BOOL_FIELD(is_grant);
	WRITE_NODE_FIELD(opt);
	WRITE_NODE_FIELD(grantor);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outLockStmt(StringInfo str, const LockStmt *node)
{
	WRITE_NODE_TYPE("LOCKSTMT");
	WRITE_NODE_FIELD(relations);
	WRITE_INT_FIELD(mode);
	WRITE_BOOL_FIELD(nowait);
}

static void
_outConstraintsSetStmt(StringInfo str, const ConstraintsSetStmt *node)
{
	WRITE_NODE_TYPE("CONSTRAINTSSETSTMT");
	WRITE_NODE_FIELD(constraints);
	WRITE_BOOL_FIELD(deferred);
}

/*
 * SelectStmt's are never written to the catalog, they only exist
 * between parse and parseTransform.  The only use of this function
 * is for debugging purposes.
 *
 * In GPDB, these are also dispatched from QD to QEs, so we need full
 * out/read support.
 *
 * If the Nodes Struct changed, we need to maintain these funtions.
 */
static void
_outSelectStmt(StringInfo str, const SelectStmt *node)
{
	WRITE_NODE_TYPE("SELECT");

	WRITE_NODE_FIELD(distinctClause);
	WRITE_NODE_FIELD(intoClause);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(fromClause);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(groupClause);
	WRITE_BOOL_FIELD(groupDistinct);
	WRITE_NODE_FIELD(havingClause);
	WRITE_NODE_FIELD(windowClause);
	WRITE_NODE_FIELD(valuesLists);
	WRITE_NODE_FIELD(sortClause);
	WRITE_NODE_FIELD(scatterClause);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_ENUM_FIELD(limitOption, LimitOption);
	WRITE_NODE_FIELD(lockingClause);
	WRITE_NODE_FIELD(withClause);
	WRITE_ENUM_FIELD(op, SetOperation);
	WRITE_BOOL_FIELD(all);
	WRITE_NODE_FIELD(larg);
	WRITE_NODE_FIELD(rarg);
	WRITE_BOOL_FIELD(disableLockingOptimization);
}

static void
_outInsertStmt(StringInfo str, const InsertStmt *node)
{
	WRITE_NODE_TYPE("INSERT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(cols);
	WRITE_NODE_FIELD(selectStmt);
	WRITE_NODE_FIELD(onConflictClause);
	WRITE_NODE_FIELD(returningClause);
	WRITE_NODE_FIELD(withClause);
	WRITE_ENUM_FIELD(override, OverridingKind);
}

static void
_outDeleteStmt(StringInfo str, const DeleteStmt *node)
{
	WRITE_NODE_TYPE("DELETE");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(usingClause);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(returningClause);
	WRITE_NODE_FIELD(withClause);
}

static void
_outUpdateStmt(StringInfo str, const UpdateStmt *node)
{
	WRITE_NODE_TYPE("UPDATE");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(fromClause);
	WRITE_NODE_FIELD(returningClause);
	WRITE_NODE_FIELD(withClause);
}

static void
_outReturnStmt(StringInfo str, const ReturnStmt *node)
{
	WRITE_NODE_TYPE("RETURN");

	WRITE_NODE_FIELD(returnval);
}

static void
_outPLAssignStmt(StringInfo str, const PLAssignStmt *node)
{
	WRITE_NODE_TYPE("PLASSIGN");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(indirection);
	WRITE_INT_FIELD(nnames);
	WRITE_NODE_FIELD(val);
	WRITE_LOCATION_FIELD(location);
}

static void
_outFuncCall(StringInfo str, const FuncCall *node)
{
	WRITE_NODE_TYPE("FUNCCALL");

	WRITE_NODE_FIELD(funcname);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(agg_order);
	WRITE_NODE_FIELD(agg_filter);
	WRITE_NODE_FIELD(over);
	WRITE_BOOL_FIELD(agg_within_group);
	WRITE_BOOL_FIELD(agg_star);
	WRITE_BOOL_FIELD(agg_distinct);
	WRITE_BOOL_FIELD(func_variadic);
	WRITE_ENUM_FIELD(funcformat, CoercionForm);
	WRITE_LOCATION_FIELD(location);
}

static void
_outDefElem(StringInfo str, const DefElem *node)
{
	WRITE_NODE_TYPE("DEFELEM");

	WRITE_STRING_FIELD(defnamespace);
	WRITE_STRING_FIELD(defname);
	WRITE_NODE_FIELD(arg);
	WRITE_ENUM_FIELD(defaction, DefElemAction);
	WRITE_LOCATION_FIELD(location);
}

static void
_outTableLikeClause(StringInfo str, const TableLikeClause *node)
{
	WRITE_NODE_TYPE("TABLELIKECLAUSE");

	WRITE_NODE_FIELD(relation);
	WRITE_UINT_FIELD(options);
	WRITE_OID_FIELD(relationOid);
}

static void
_outLockingClause(StringInfo str, const LockingClause *node)
{
	WRITE_NODE_TYPE("LOCKINGCLAUSE");

	WRITE_NODE_FIELD(lockedRels);
	WRITE_ENUM_FIELD(strength, LockClauseStrength);
	WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
}

static void
_outXmlSerialize(StringInfo str, const XmlSerialize *node)
{
	WRITE_NODE_TYPE("XMLSERIALIZE");

	WRITE_ENUM_FIELD(xmloption, XmlOptionType);
	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(typeName);
	WRITE_LOCATION_FIELD(location);
}

static void
_outDMLActionExpr(StringInfo str, const DMLActionExpr *node)
{
	WRITE_NODE_TYPE("DMLACTIONEXPR");
}

static void
_outTriggerTransition(StringInfo str, const TriggerTransition *node)
{
	WRITE_NODE_TYPE("TRIGGERTRANSITION");

	WRITE_STRING_FIELD(name);
	WRITE_BOOL_FIELD(isNew);
	WRITE_BOOL_FIELD(isTable);
}

static void
_outColumnDef(StringInfo str, const ColumnDef *node)
{
	WRITE_NODE_TYPE("COLUMNDEF");

	WRITE_STRING_FIELD(colname);
	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(compression);
	WRITE_INT_FIELD(inhcount);
	WRITE_BOOL_FIELD(is_local);
	WRITE_BOOL_FIELD(is_not_null);
	WRITE_BOOL_FIELD(is_from_type);
	WRITE_INT_FIELD(attnum);
	WRITE_INT_FIELD(storage);
	/*
	 * GPDB: the CREATE TABLE column STORAGE clause (new in PG16) sets
	 * storage_name (the string), which DefineRelation resolves to attstorage;
	 * dispatch it so segments apply the same storage (else a text column stays
	 * extended on the QE, diverging from the QD and later mis-deciding TOAST).
	 */
	WRITE_STRING_FIELD(storage_name);
	WRITE_NODE_FIELD(raw_default);
	WRITE_NODE_FIELD(cooked_default);

	WRITE_BOOL_FIELD(hasCookedMissingVal);
	WRITE_BOOL_FIELD(missingIsNull);
	if (node->hasCookedMissingVal && !node->missingIsNull)
		outDatum(str, node->missingVal, -1, false);

	WRITE_CHAR_FIELD(identity);
	WRITE_NODE_FIELD(identitySequence);
	WRITE_CHAR_FIELD(generated);
	WRITE_NODE_FIELD(collClause);
	WRITE_OID_FIELD(collOid);
	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(encoding);
	WRITE_NODE_FIELD(fdwoptions);
	WRITE_LOCATION_FIELD(location);
}

static void
_outTypeName(StringInfo str, const TypeName *node)
{
	WRITE_NODE_TYPE("TYPENAME");

	WRITE_NODE_FIELD(names);
	WRITE_OID_FIELD(typeOid);
	WRITE_BOOL_FIELD(setof);
	WRITE_BOOL_FIELD(pct_type);
	WRITE_NODE_FIELD(typmods);
	WRITE_INT_FIELD(typemod);
	WRITE_NODE_FIELD(arrayBounds);
	WRITE_LOCATION_FIELD(location);
}

static void
_outTypeCast(StringInfo str, const TypeCast *node)
{
	WRITE_NODE_TYPE("TYPECAST");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(typeName);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCollateClause(StringInfo str, const CollateClause *node)
{
	WRITE_NODE_TYPE("COLLATECLAUSE");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(collname);
	WRITE_LOCATION_FIELD(location);
}

static void
_outIndexElem(StringInfo str, const IndexElem *node)
{
	WRITE_NODE_TYPE("INDEXELEM");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(expr);
	WRITE_STRING_FIELD(indexcolname);
	WRITE_NODE_FIELD(collation);
	WRITE_NODE_FIELD(opclass);
	WRITE_NODE_FIELD(opclassopts);
	WRITE_ENUM_FIELD(ordering, SortByDir);
	WRITE_ENUM_FIELD(nulls_ordering, SortByNulls);
}

static void
_outStatsElem(StringInfo str, const StatsElem *node)
{
	WRITE_NODE_TYPE("STATSELEM");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(expr);
}

static void
_outVariableSetStmt(StringInfo str, const VariableSetStmt *node)
{
	WRITE_NODE_TYPE("VARIABLESETSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_ENUM_FIELD(kind, VariableSetKind);
	WRITE_NODE_FIELD(args);
	WRITE_BOOL_FIELD(is_local);
}


static void
_outWithCheckOption(StringInfo str, const WithCheckOption *node)
{
	WRITE_NODE_TYPE("WITHCHECKOPTION");

	WRITE_ENUM_FIELD(kind, WCOKind);
	WRITE_STRING_FIELD(relname);
	WRITE_STRING_FIELD(polname);
	WRITE_NODE_FIELD(qual);
	WRITE_BOOL_FIELD(cascaded);
}

static void
_outSortGroupClause(StringInfo str, const SortGroupClause *node)
{
	WRITE_NODE_TYPE("SORTGROUPCLAUSE");

	WRITE_UINT_FIELD(tleSortGroupRef);
	WRITE_OID_FIELD(eqop);
	WRITE_OID_FIELD(sortop);
	WRITE_BOOL_FIELD(nulls_first);
	WRITE_BOOL_FIELD(hashable);
}

static void
_outGroupingSet(StringInfo str, const GroupingSet *node)
{
	WRITE_NODE_TYPE("GROUPINGSET");

	WRITE_ENUM_FIELD(kind, GroupingSetKind);
	WRITE_NODE_FIELD(content);
	WRITE_LOCATION_FIELD(location);
}

static void
_outWindowClause(StringInfo str, const WindowClause *node)
{
	WRITE_NODE_TYPE("WINDOWCLAUSE");

	WRITE_STRING_FIELD(name);
	WRITE_STRING_FIELD(refname);
	WRITE_NODE_FIELD(partitionClause);
	WRITE_NODE_FIELD(orderClause);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	WRITE_OID_FIELD(startInRangeFunc);
	WRITE_OID_FIELD(endInRangeFunc);
	WRITE_OID_FIELD(inRangeColl);
	WRITE_BOOL_FIELD(inRangeAsc);
	WRITE_BOOL_FIELD(inRangeNullsFirst);
	WRITE_UINT_FIELD(winref);
	WRITE_BOOL_FIELD(copiedOrder);
}

static void
_outRowMarkClause(StringInfo str, const RowMarkClause *node)
{
	WRITE_NODE_TYPE("ROWMARKCLAUSE");

	WRITE_UINT_FIELD(rti);
	WRITE_ENUM_FIELD(strength, LockClauseStrength);
	WRITE_ENUM_FIELD(waitPolicy, LockWaitPolicy);
	WRITE_BOOL_FIELD(pushedDown);
}

static void
_outWithClause(StringInfo str, const WithClause *node)
{
	WRITE_NODE_TYPE("WITHCLAUSE");

	WRITE_NODE_FIELD(ctes);
	WRITE_BOOL_FIELD(recursive);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCTESearchClause(StringInfo str, const CTESearchClause *node)
{
	WRITE_NODE_TYPE("CTESEARCHCLAUSE");

	WRITE_NODE_FIELD(search_col_list);
	WRITE_BOOL_FIELD(search_breadth_first);
	WRITE_STRING_FIELD(search_seq_column);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCTECycleClause(StringInfo str, const CTECycleClause *node)
{
	WRITE_NODE_TYPE("CTECYCLECLAUSE");

	WRITE_NODE_FIELD(cycle_col_list);
	WRITE_STRING_FIELD(cycle_mark_column);
	WRITE_NODE_FIELD(cycle_mark_value);
	WRITE_NODE_FIELD(cycle_mark_default);
	WRITE_STRING_FIELD(cycle_path_column);
	WRITE_LOCATION_FIELD(location);
	WRITE_OID_FIELD(cycle_mark_type);
	WRITE_INT_FIELD(cycle_mark_typmod);
	WRITE_OID_FIELD(cycle_mark_collation);
	WRITE_OID_FIELD(cycle_mark_neop);
}

static void
_outCommonTableExpr(StringInfo str, const CommonTableExpr *node)
{
	WRITE_NODE_TYPE("COMMONTABLEEXPR");

	WRITE_STRING_FIELD(ctename);
	WRITE_NODE_FIELD(aliascolnames);
	WRITE_ENUM_FIELD(ctematerialized, CTEMaterialize);
	WRITE_NODE_FIELD(ctequery);
	WRITE_NODE_FIELD(search_clause);
	WRITE_NODE_FIELD(cycle_clause);
	WRITE_LOCATION_FIELD(location);
	WRITE_BOOL_FIELD(cterecursive);
	WRITE_INT_FIELD(cterefcount);
	WRITE_NODE_FIELD(ctecolnames);
	WRITE_NODE_FIELD(ctecoltypes);
	WRITE_NODE_FIELD(ctecoltypmods);
	WRITE_NODE_FIELD(ctecolcollations);
}

static void
_outMergeWhenClause(StringInfo str, const MergeWhenClause *node)
{
	WRITE_NODE_TYPE("MERGEWHENCLAUSE");

	WRITE_ENUM_FIELD(matchKind, MergeMatchKind);
	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(override, OverridingKind);
	WRITE_NODE_FIELD(condition);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(values);
}

static void
_outMergeAction(StringInfo str, const MergeAction *node)
{
	WRITE_NODE_TYPE("MERGEACTION");

	WRITE_ENUM_FIELD(matchKind, MergeMatchKind);
	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(override, OverridingKind);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(updateColnos);
}

static void
_outSetOperationStmt(StringInfo str, const SetOperationStmt *node)
{
	WRITE_NODE_TYPE("SETOPERATIONSTMT");

	WRITE_ENUM_FIELD(op, SetOperation);
	WRITE_BOOL_FIELD(all);
	WRITE_NODE_FIELD(larg);
	WRITE_NODE_FIELD(rarg);
	WRITE_NODE_FIELD(colTypes);
	WRITE_NODE_FIELD(colTypmods);
	WRITE_NODE_FIELD(colCollations);
	WRITE_NODE_FIELD(groupClauses);
}

static void
_outRangeTblEntry(StringInfo str, const RangeTblEntry *node)
{
	WRITE_NODE_TYPE("RANGETBLENTRY");

	/* put alias + eref first to make dump more legible */
	WRITE_NODE_FIELD(alias);
	WRITE_NODE_FIELD(eref);
	WRITE_ENUM_FIELD(rtekind, RTEKind);

	switch (node->rtekind)
	{
		case RTE_RELATION:
			WRITE_OID_FIELD(relid);
			WRITE_CHAR_FIELD(relkind);
			WRITE_INT_FIELD(rellockmode);
			WRITE_NODE_FIELD(tablesample);
			break;
		case RTE_SUBQUERY:
			WRITE_NODE_FIELD(subquery);
			WRITE_BOOL_FIELD(security_barrier);
			/*
			 * GPDB: view-derived subquery RTEs re-use these RELATION fields
			 * (PG16 keeps relkind == RELKIND_VIEW so the view's ACL is still
			 * checked via the RTEPermissionInfo).  Must be dispatched to QEs.
			 */
			WRITE_OID_FIELD(relid);
			WRITE_CHAR_FIELD(relkind);
			WRITE_INT_FIELD(rellockmode);
			break;
		case RTE_JOIN:
			WRITE_ENUM_FIELD(jointype, JoinType);
			WRITE_INT_FIELD(joinmergedcols);
			WRITE_NODE_FIELD(joinaliasvars);
			WRITE_NODE_FIELD(joinleftcols);
			WRITE_NODE_FIELD(joinrightcols);
			WRITE_NODE_FIELD(join_using_alias);
			break;
		case RTE_FUNCTION:
			WRITE_NODE_FIELD(functions);
			WRITE_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNCTION:
			WRITE_NODE_FIELD(subquery);
			WRITE_NODE_FIELD(functions);
			WRITE_BOOL_FIELD(funcordinality);
			break;
		case RTE_TABLEFUNC:
			WRITE_NODE_FIELD(tablefunc);
			break;
		case RTE_VALUES:
			WRITE_NODE_FIELD(values_lists);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_CTE:
			WRITE_STRING_FIELD(ctename);
			WRITE_UINT_FIELD(ctelevelsup);
			WRITE_BOOL_FIELD(self_reference);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_NAMEDTUPLESTORE:
			WRITE_STRING_FIELD(enrname);
			WRITE_FLOAT_FIELD(enrtuples, "%.0f");
			WRITE_OID_FIELD(relid);
			WRITE_NODE_FIELD(coltypes);
			WRITE_NODE_FIELD(coltypmods);
			WRITE_NODE_FIELD(colcollations);
			break;
		case RTE_RESULT:
			/* no extra fields */
			break;
		case RTE_GROUP:				/* PG18: the grouping-step RTE */
			WRITE_NODE_FIELD(groupexprs);
			break;
        case RTE_VOID:                                                  /*CDB*/
			/*
			 * GPDB: an RTE the planner pulled up is marked RTE_VOID but keeps
			 * its relid/relkind (and perminfoindex, written below) so the
			 * dispatched RTE still matches its RTEPermissionInfo on the QE
			 * (ExecCheckPermissions asserts perminfo->relid == rte->relid).
			 * A pulled-up view RTE has a valid relid; a plain subquery has 0.
			 */
			WRITE_OID_FIELD(relid);
			WRITE_CHAR_FIELD(relkind);
			WRITE_INT_FIELD(rellockmode);
            break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int) node->rtekind);
			break;
	}

	WRITE_UINT_FIELD(perminfoindex);
	WRITE_BOOL_FIELD(lateral);
	WRITE_BOOL_FIELD(inh);
	WRITE_BOOL_FIELD(inFromCl);
	WRITE_NODE_FIELD(securityQuals);

	WRITE_BOOL_FIELD(forceDistRandom);
}

static void
_outRTEPermissionInfo(StringInfo str, const RTEPermissionInfo *node)
{
	WRITE_NODE_TYPE("RTEPERMISSIONINFO");

	WRITE_OID_FIELD(relid);
	WRITE_BOOL_FIELD(inh);
	WRITE_UINT64_FIELD(requiredPerms);
	WRITE_OID_FIELD(checkAsUser);
	WRITE_BITMAPSET_FIELD(selectedCols);
	WRITE_BITMAPSET_FIELD(insertedCols);
	WRITE_BITMAPSET_FIELD(updatedCols);
}

static void
_outRangeTblFunction(StringInfo str, const RangeTblFunction *node)
{
	WRITE_NODE_TYPE("RANGETBLFUNCTION");

	WRITE_NODE_FIELD(funcexpr);
	WRITE_INT_FIELD(funccolcount);
	WRITE_NODE_FIELD(funccolnames);
	WRITE_NODE_FIELD(funccoltypes);
	WRITE_NODE_FIELD(funccoltypmods);
	WRITE_NODE_FIELD(funccolcollations);
	/* funcuserdata is only serialized in binary out/read functions */
	WRITE_BYTEA_FIELD(funcuserdata);
	WRITE_BITMAPSET_FIELD(funcparams);
}

static void
_outTableSampleClause(StringInfo str, const TableSampleClause *node)
{
	WRITE_NODE_TYPE("TABLESAMPLECLAUSE");

	WRITE_OID_FIELD(tsmhandler);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(repeatable);
}




static void
_outColumnRef(StringInfo str, const ColumnRef *node)
{
	WRITE_NODE_TYPE("COLUMNREF");

	WRITE_NODE_FIELD(fields);
	WRITE_LOCATION_FIELD(location);
}

static void
_outParamRef(StringInfo str, const ParamRef *node)
{
	WRITE_NODE_TYPE("PARAMREF");

	WRITE_INT_FIELD(number);
	WRITE_LOCATION_FIELD(location);
}

/*
 * Node types found in raw parse trees (supported for debug purposes)
 */

static void
_outRawStmt(StringInfo str, const RawStmt *node)
{
	WRITE_NODE_TYPE("RAWSTMT");

	WRITE_NODE_FIELD(stmt);
	WRITE_LOCATION_FIELD(stmt_location);
	WRITE_INT_FIELD(stmt_len);
}


static void
_outA_Star(StringInfo str, const A_Star *node)
{
	WRITE_NODE_TYPE("A_STAR");
}

static void
_outA_Indices(StringInfo str, const A_Indices *node)
{
	WRITE_NODE_TYPE("A_INDICES");

	WRITE_BOOL_FIELD(is_slice);
	WRITE_NODE_FIELD(lidx);
	WRITE_NODE_FIELD(uidx);
}

static void
_outA_Indirection(StringInfo str, const A_Indirection *node)
{
	WRITE_NODE_TYPE("A_INDIRECTION");

	WRITE_NODE_FIELD(arg);
	WRITE_NODE_FIELD(indirection);
}

static void
_outA_ArrayExpr(StringInfo str, const A_ArrayExpr *node)
{
	WRITE_NODE_TYPE("A_ARRAYEXPR");

	WRITE_NODE_FIELD(elements);
	WRITE_LOCATION_FIELD(location);
}

static void
_outResTarget(StringInfo str, const ResTarget *node)
{
	WRITE_NODE_TYPE("RESTARGET");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(indirection);
	WRITE_NODE_FIELD(val);
	WRITE_LOCATION_FIELD(location);
}

static void
_outMultiAssignRef(StringInfo str, const MultiAssignRef *node)
{
	WRITE_NODE_TYPE("MULTIASSIGNREF");

	WRITE_NODE_FIELD(source);
	WRITE_INT_FIELD(colno);
	WRITE_INT_FIELD(ncolumns);
}

static void
_outSortBy(StringInfo str, const SortBy *node)
{
	WRITE_NODE_TYPE("SORTBY");

	WRITE_NODE_FIELD(node);
	WRITE_ENUM_FIELD(sortby_dir, SortByDir);
	WRITE_ENUM_FIELD(sortby_nulls, SortByNulls);
	WRITE_NODE_FIELD(useOp);
	WRITE_LOCATION_FIELD(location);
}

static void
_outWindowDef(StringInfo str, const WindowDef *node)
{
	WRITE_NODE_TYPE("WINDOWDEF");

	WRITE_STRING_FIELD(name);
	WRITE_STRING_FIELD(refname);
	WRITE_NODE_FIELD(partitionClause);
	WRITE_NODE_FIELD(orderClause);
	WRITE_INT_FIELD(frameOptions);
	WRITE_NODE_FIELD(startOffset);
	WRITE_NODE_FIELD(endOffset);
	WRITE_LOCATION_FIELD(location);
}

static void
_outRangeSubselect(StringInfo str, const RangeSubselect *node)
{
	WRITE_NODE_TYPE("RANGESUBSELECT");

	WRITE_BOOL_FIELD(lateral);
	WRITE_NODE_FIELD(subquery);
	WRITE_NODE_FIELD(alias);
}

static void
_outInferClause(StringInfo str, const InferClause *node)
{
	WRITE_NODE_TYPE("INFERCLAUSE");

	WRITE_NODE_FIELD(indexElems);
	WRITE_NODE_FIELD(whereClause);
	WRITE_STRING_FIELD(conname);
	WRITE_LOCATION_FIELD(location);
}

static void
_outOnConflictClause(StringInfo str, const OnConflictClause *node)
{
	WRITE_NODE_TYPE("ONCONFLICTCLAUSE");

	WRITE_ENUM_FIELD(action, OnConflictAction);
	WRITE_NODE_FIELD(infer);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(whereClause);
	WRITE_LOCATION_FIELD(location);
}



static void
_outConstraint(StringInfo str, const Constraint *node)
{
	WRITE_NODE_TYPE("CONSTRAINT");

	WRITE_ENUM_FIELD(contype, ConstrType);
	WRITE_STRING_FIELD(conname);			/* name, or NULL if unnamed */
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_LOCATION_FIELD(location);

	WRITE_BOOL_FIELD(is_no_inherit);
	WRITE_NODE_FIELD(raw_expr);
	WRITE_STRING_FIELD(cooked_expr);
	WRITE_CHAR_FIELD(generated_when);
	/*
	 * Other Constraint fields that must round-trip to the QE:
	 * generated_kind is PG18 (STORED vs VIRTUAL generated columns);
	 * without_overlaps/fk_with_period/pk_with_period are the PG17 temporal
	 * (PERIOD / WITHOUT OVERLAPS) flags.  Kept adjacent so _outConstraint and
	 * _readConstraint stay in lockstep.
	 */
	WRITE_CHAR_FIELD(generated_kind);
	WRITE_BOOL_FIELD(without_overlaps);
	WRITE_BOOL_FIELD(fk_with_period);
	WRITE_BOOL_FIELD(pk_with_period);

	WRITE_NODE_FIELD(keys);
	WRITE_NODE_FIELD(including);

	WRITE_NODE_FIELD(exclusions);

	WRITE_NODE_FIELD(options);
	WRITE_STRING_FIELD(indexname);
	WRITE_STRING_FIELD(indexspace);
	WRITE_BOOL_FIELD(reset_default_tblspc);

	WRITE_STRING_FIELD(access_method);
	WRITE_NODE_FIELD(where_clause);

	WRITE_NODE_FIELD(pktable);
	WRITE_NODE_FIELD(fk_attrs);
	WRITE_NODE_FIELD(pk_attrs);
	WRITE_CHAR_FIELD(fk_matchtype);
	WRITE_CHAR_FIELD(fk_upd_action);
	WRITE_CHAR_FIELD(fk_del_action);
	WRITE_NODE_FIELD(old_conpfeqop);
	WRITE_OID_FIELD(old_pktable_oid);

	WRITE_BOOL_FIELD(nulls_not_distinct);
	WRITE_NODE_FIELD(fk_del_set_cols);

	/*
	 * PG18: ENFORCED/NOT ENFORCED flag.  Must reach the QE, else the
	 * dispatched CHECK/FK constraint deserializes as is_enforced=false while
	 * initially_valid stays true, tripping CreateConstraintEntry's
	 * "isEnforced || !isValidated" assertion (pg_constraint.c).
	 */
	WRITE_BOOL_FIELD(is_enforced);
	WRITE_BOOL_FIELD(skip_validation);
	WRITE_BOOL_FIELD(initially_valid);
}


static void
_outPartitionElem(StringInfo str, const PartitionElem *node)
{
	WRITE_NODE_TYPE("PARTITIONELEM");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(expr);
	WRITE_NODE_FIELD(collation);
	WRITE_NODE_FIELD(opclass);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartitionSpec(StringInfo str, const PartitionSpec *node)
{
	WRITE_NODE_TYPE("PARTITIONSPEC");

	WRITE_CHAR_FIELD(strategy);
	WRITE_NODE_FIELD(partParams);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartitionBoundSpec(StringInfo str, const PartitionBoundSpec *node)
{
	WRITE_NODE_TYPE("PARTITIONBOUNDSPEC");

	WRITE_CHAR_FIELD(strategy);
	WRITE_BOOL_FIELD(is_default);
	WRITE_INT_FIELD(modulus);
	WRITE_INT_FIELD(remainder);
	WRITE_NODE_FIELD(listdatums);
	WRITE_NODE_FIELD(lowerdatums);
	WRITE_NODE_FIELD(upperdatums);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartitionRangeDatum(StringInfo str, const PartitionRangeDatum *node)
{
	WRITE_NODE_TYPE("PARTITIONRANGEDATUM");

	WRITE_ENUM_FIELD(kind, PartitionRangeDatumKind);
	WRITE_NODE_FIELD(value);
	WRITE_LOCATION_FIELD(location);
}

static void
_outPartitionCmd(StringInfo str, const PartitionCmd *node)
{
	WRITE_NODE_TYPE("PARTITIONCMD");

	WRITE_NODE_FIELD(name);
	WRITE_NODE_FIELD(bound);
	WRITE_BOOL_FIELD(concurrent);
}

static void
_outGpAlterPartitionId(StringInfo str, const GpAlterPartitionId *node)
{
	WRITE_NODE_TYPE("GPALTERPARTITIONID");

	WRITE_ENUM_FIELD(idtype, GpAlterPartitionIdType);
	WRITE_NODE_FIELD(partiddef);
}

static void
_outGpDropPartitionCmd(StringInfo str, const GpDropPartitionCmd *node)
{
	WRITE_NODE_TYPE("GPDROPPARTITIONCMD");

	WRITE_NODE_FIELD(partid);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outGpAlterPartitionCmd(StringInfo str, const GpAlterPartitionCmd *node)
{
	WRITE_NODE_TYPE("GPALTERPARTITIONCMD");

	WRITE_NODE_FIELD(partid);
	WRITE_NODE_FIELD(arg);
}

static void
_outCreateSchemaStmt(StringInfo str, const CreateSchemaStmt *node)
{
	WRITE_NODE_TYPE("CREATESCHEMASTMT");

	WRITE_STRING_FIELD(schemaname);
	WRITE_NODE_FIELD(authrole);
	WRITE_BOOL_FIELD(istemp);
}

static void
_outCreatePLangStmt(StringInfo str, const CreatePLangStmt *node)
{
	WRITE_NODE_TYPE("CREATEPLANGSTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_STRING_FIELD(plname);
	WRITE_NODE_FIELD(plhandler);
	WRITE_NODE_FIELD(plinline);
	WRITE_NODE_FIELD(plvalidator);
	WRITE_BOOL_FIELD(pltrusted);
}

static void
_outVacuumStmt(StringInfo str, const VacuumStmt *node)
{
	WRITE_NODE_TYPE("VACUUMSTMT");

	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(rels);
	WRITE_BOOL_FIELD(is_vacuumcmd);
}

static void
_outVacuumRelation(StringInfo str, const VacuumRelation *node)
{
	WRITE_NODE_TYPE("VACUUMRELATION");

	WRITE_NODE_FIELD(relation);
	WRITE_OID_FIELD(oid);
	WRITE_NODE_FIELD(va_cols);
}

static void
_outCdbProcess(StringInfo str, const CdbProcess *node)
{
	WRITE_NODE_TYPE("CDBPROCESS");
	WRITE_STRING_FIELD(listenerAddr);
	WRITE_INT_FIELD(listenerPort);
	WRITE_INT_FIELD(pid);
	WRITE_INT_FIELD(contentid);
	WRITE_INT_FIELD(dbid);
}

static void
_outSliceTable(StringInfo str, const SliceTable *node)
{
	WRITE_NODE_TYPE("SLICETABLE");

	WRITE_INT_FIELD(localSlice);
	WRITE_INT_FIELD(numSlices);
	for (int i = 0; i < node->numSlices; i++)
	{
		WRITE_INT_FIELD(slices[i].sliceIndex);
		WRITE_INT_FIELD(slices[i].rootIndex);
		WRITE_INT_FIELD(slices[i].parentIndex);
		WRITE_INT_FIELD(slices[i].planNumSegments);
		WRITE_NODE_FIELD(slices[i].children); /* List of int index */
		WRITE_ENUM_FIELD(slices[i].gangType, GangType);
		WRITE_NODE_FIELD(slices[i].segments); /* List of int */
		WRITE_DUMMY_FIELD(slices[i].primaryGang);
		WRITE_NODE_FIELD(slices[i].primaryProcesses); /* List of (CDBProcess *) */
		WRITE_BITMAPSET_FIELD(slices[i].processesMap);
	}
	WRITE_BOOL_FIELD(hasMotions);
	WRITE_INT_FIELD(instrument_options);
	WRITE_INT_FIELD(ic_instance_id);
}

static void
_outCursorPosInfo(StringInfo str, const CursorPosInfo *node)
{
	WRITE_NODE_TYPE("CURSORPOSINFO");

	WRITE_STRING_FIELD(cursor_name);
	WRITE_INT_FIELD(gp_segment_id);
	WRITE_UINT_FIELD(ctid.ip_blkid.bi_hi);
	WRITE_UINT_FIELD(ctid.ip_blkid.bi_lo);
	WRITE_UINT_FIELD(ctid.ip_posid);
	WRITE_OID_FIELD(table_oid);
}

static void
_outCreateTrigStmt(StringInfo str, const CreateTrigStmt *node)
{
	WRITE_NODE_TYPE("CREATETRIGSTMT");

	WRITE_STRING_FIELD(trigname);
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(funcname);
	WRITE_NODE_FIELD(args);
	WRITE_BOOL_FIELD(row);
	WRITE_INT_FIELD(timing);
	WRITE_INT_FIELD(events);
	WRITE_NODE_FIELD(columns);
	WRITE_NODE_FIELD(whenClause);
	WRITE_BOOL_FIELD(isconstraint);
	WRITE_NODE_FIELD(transitionRels);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_NODE_FIELD(constrrel);
}

static void
_outCreateTableSpaceStmt(StringInfo str, const CreateTableSpaceStmt *node)
{
	WRITE_NODE_TYPE("CREATETABLESPACESTMT");

	WRITE_STRING_FIELD(tablespacename);
	WRITE_NODE_FIELD(owner);
	WRITE_STRING_FIELD(location);
	WRITE_NODE_FIELD(options);
}

static void
_outDropTableSpaceStmt(StringInfo str, const DropTableSpaceStmt *node)
{
	WRITE_NODE_TYPE("DROPTABLESPACESTMT");

	WRITE_STRING_FIELD(tablespacename);
	WRITE_BOOL_FIELD(missing_ok);
}



static void
_outDropQueueStmt(StringInfo str, const DropQueueStmt *node)
{
	WRITE_NODE_TYPE("DROPQUEUESTMT");

	WRITE_STRING_FIELD(queue);
}


static void
_outDropResourceGroupStmt(StringInfo str, const DropResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("DROPRESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
}



static void
_outCommentStmt(StringInfo str, const CommentStmt *node)
{
	WRITE_NODE_TYPE("COMMENTSTMT");

	WRITE_ENUM_FIELD(objtype, ObjectType);
	WRITE_NODE_FIELD(object);
	WRITE_STRING_FIELD(comment);
}

static void
_outTableValueExpr(StringInfo str, const TableValueExpr *node)
{
	WRITE_NODE_TYPE("TABLEVALUEEXPR");

	WRITE_NODE_FIELD(subquery);
}

static void
_outAlterTypeStmtSetDefaultEnc(StringInfo str, const AlterTypeStmtSetDefaultEnc *node)
{
	WRITE_NODE_TYPE("ALTERTYPESTMTSETDEFAULTENC");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(encoding);
}

static void
_outAlterTypeStmt(StringInfo str, const AlterTypeStmt *node)
{
	WRITE_NODE_TYPE("ALTERTYPESTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterExtensionStmt(StringInfo str, const AlterExtensionStmt *node)
{
	WRITE_NODE_TYPE("ALTEREXTENSIONSTMT");

	WRITE_STRING_FIELD(extname);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(update_ext_state, UpdateExtensionState);
}

static void
_outAlterExtensionContentsStmt(StringInfo str, const AlterExtensionContentsStmt *node)
{
	WRITE_NODE_TYPE("ALTEREXTENSIONCONTENTSSTMT");

	WRITE_STRING_FIELD(extname);
	WRITE_INT_FIELD(action);
	WRITE_ENUM_FIELD(objtype, ObjectType);
	WRITE_NODE_FIELD(object);
}

static void
_outAlterTSConfigurationStmt(StringInfo str, const AlterTSConfigurationStmt *node)
{
	WRITE_NODE_TYPE("ALTERTSCONFIGURATIONSTMT");

	WRITE_NODE_FIELD(cfgname);
	WRITE_NODE_FIELD(tokentype);
	WRITE_NODE_FIELD(dicts);
	WRITE_BOOL_FIELD(override);
	WRITE_BOOL_FIELD(replace);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterTSDictionaryStmt(StringInfo str, const AlterTSDictionaryStmt *node)
{
	WRITE_NODE_TYPE("ALTERTSDICTIONARYSTMT");

	WRITE_NODE_FIELD(dictname);
	WRITE_NODE_FIELD(options);
}

static void
_outCreatePublicationStmt(StringInfo str, const CreatePublicationStmt *node)
{
	WRITE_NODE_TYPE("CREATEPUBLICATIONSTMT");

	WRITE_STRING_FIELD(pubname);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(pubobjects);
	WRITE_BOOL_FIELD(for_all_tables);
}

static void
_outAlterPublicationStmt(StringInfo str, const AlterPublicationStmt *node)
{
	WRITE_NODE_TYPE("ALTERPUBLICATIONSTMT");

	WRITE_STRING_FIELD(pubname);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(pubobjects);
	WRITE_BOOL_FIELD(for_all_tables);
	WRITE_ENUM_FIELD(action, AlterPublicationAction);
}

static void
_outCreateSubscriptionStmt(StringInfo str, const CreateSubscriptionStmt *node)
{
	WRITE_NODE_TYPE("CREATESUBSCRIPTIONSTMT");

	WRITE_STRING_FIELD(subname);
	WRITE_STRING_FIELD(conninfo);
	WRITE_NODE_FIELD(publication);
	WRITE_NODE_FIELD(options);
}

static void
_outDropSubscriptionStmt(StringInfo str, const DropSubscriptionStmt *node)
{
	WRITE_NODE_TYPE("DROPSUBSCRIPTIONSTMT");

	WRITE_STRING_FIELD(subname);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outAlterSubscriptionStmt(StringInfo str, const AlterSubscriptionStmt *node)
{
	WRITE_NODE_TYPE("ALTERSUBSCRIPTIONSTMT");

	WRITE_ENUM_FIELD(kind, AlterSubscriptionType);
	WRITE_STRING_FIELD(subname);
	WRITE_STRING_FIELD(conninfo);
	WRITE_NODE_FIELD(publication);
	WRITE_NODE_FIELD(options);
}




static void
_outCopyStmt(StringInfo str, CopyStmt *node)
{
	WRITE_NODE_TYPE("COPYSTMT");
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(attlist);
	WRITE_BOOL_FIELD(is_from);
	WRITE_BOOL_FIELD(is_program);
	WRITE_STRING_FIELD(filename);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(sreh);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outConst(StringInfo str, Const *node)
{
	WRITE_NODE_TYPE("CONST");

	WRITE_OID_FIELD(consttype);
	WRITE_INT_FIELD(consttypmod);
	WRITE_OID_FIELD(constcollid);
	WRITE_INT_FIELD(constlen);
	WRITE_BOOL_FIELD(constbyval);
	WRITE_BOOL_FIELD(constisnull);
	WRITE_LOCATION_FIELD(location);

	if (!node->constisnull)
		_outDatum(str, node->constvalue, node->constlen, node->constbyval);
}

static void
_outBoolExpr(StringInfo str, BoolExpr *node)
{
	WRITE_NODE_TYPE("BOOLEXPR");
	WRITE_ENUM_FIELD(boolop, BoolExprType);

	WRITE_NODE_FIELD(args);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCurrentOfExpr(StringInfo str, CurrentOfExpr *node)
{
	WRITE_NODE_TYPE("CURRENTOFEXPR");

	WRITE_UINT_FIELD(cvarno);
	WRITE_STRING_FIELD(cursor_name);
	WRITE_INT_FIELD(cursor_param);
	WRITE_OID_FIELD(target_relid);
}

static void
_outJoinExpr(StringInfo str, JoinExpr *node)
{
	WRITE_NODE_TYPE("JOINEXPR");

	WRITE_ENUM_FIELD(jointype, JoinType);
	WRITE_BOOL_FIELD(isNatural);
	WRITE_NODE_FIELD(larg);
	WRITE_NODE_FIELD(rarg);
	WRITE_NODE_FIELD(usingClause);
	WRITE_NODE_FIELD(join_using_alias);
	WRITE_NODE_FIELD(quals);
	WRITE_NODE_FIELD(alias);
	WRITE_INT_FIELD(rtindex);
}

/*****************************************************************************
 *
 *	Stuff from extensible.h
 *
 *****************************************************************************/

static void
_outExtensibleNode(StringInfo str, const ExtensibleNode *node)
{
	const ExtensibleNodeMethods *methods;
	StringInfoData buf;
	initStringInfo(&buf);

	methods = GetExtensibleNodeMethods(node->extnodename, false);

	WRITE_NODE_TYPE("EXTENSIBLENODE");

	WRITE_STRING_FIELD(extnodename);

	/* serialize the private fields */
	methods->nodeOut(&buf, node);

	WRITE_STRING_VAR(buf.data);
}

/*****************************************************************************
 *
 *	Stuff from parsenodes.h.
 *
 *****************************************************************************/

static void
_outCreateExtensionStmt(StringInfo str, CreateExtensionStmt *node)
{
	WRITE_NODE_TYPE("CREATEEXTENSIONSTMT");
	WRITE_STRING_FIELD(extname);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(create_ext_state, CreateExtensionState);
}

static void
_outRoleSpec(StringInfo str, const RoleSpec *node)
{
	WRITE_NODE_TYPE("ROLESPEC");

	WRITE_ENUM_FIELD(roletype, RoleSpecType);
	WRITE_STRING_FIELD(rolename);
	WRITE_LOCATION_FIELD(location);
}

static void
_outCreateDomainStmt(StringInfo str, CreateDomainStmt *node)
{
	WRITE_NODE_TYPE("CREATEDOMAINSTMT");
	WRITE_NODE_FIELD(domainname);
	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(collClause);
	WRITE_NODE_FIELD(constraints);
}

static void
_outAlterDomainStmt(StringInfo str, AlterDomainStmt *node)
{
	WRITE_NODE_TYPE("ALTERDOMAINSTMT");
	WRITE_CHAR_FIELD(subtype);
	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(def);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterDatabaseStmt(StringInfo str, AlterDatabaseStmt *node)
{
	WRITE_NODE_TYPE("ALTERDATABASESTMT");
	WRITE_STRING_FIELD(dbname);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterDefaultPrivilegesStmt(StringInfo str, AlterDefaultPrivilegesStmt *node)
{
	WRITE_NODE_TYPE("ALTERDEFAULTPRIVILEGESSTMT");
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(action);
}

static void
_outQuery(StringInfo str, Query *node)
{
	WRITE_NODE_TYPE("QUERY");

	WRITE_ENUM_FIELD(commandType, CmdType);
	WRITE_ENUM_FIELD(querySource, QuerySource);
	WRITE_BOOL_FIELD(canSetTag);

	WRITE_NODE_FIELD(utilityStmt);
	WRITE_INT_FIELD(resultRelation);
	WRITE_BOOL_FIELD(hasAggs);
	WRITE_BOOL_FIELD(hasWindowFuncs);
	WRITE_BOOL_FIELD(hasSubLinks);
	WRITE_BOOL_FIELD(hasDynamicFunctions);
	WRITE_BOOL_FIELD(hasFuncsWithExecRestrictions);
	WRITE_BOOL_FIELD(hasDistinctOn);
	WRITE_BOOL_FIELD(hasRecursive);
	WRITE_BOOL_FIELD(hasModifyingCTE);
	WRITE_BOOL_FIELD(hasForUpdate);
	WRITE_BOOL_FIELD(hasRowSecurity);
	WRITE_BOOL_FIELD(canOptSelectLockingClause);
	WRITE_NODE_FIELD(cteList);
	WRITE_NODE_FIELD(rtable);
	WRITE_NODE_FIELD(rteperminfos);
	WRITE_NODE_FIELD(jointree);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(withCheckOptions);
	WRITE_NODE_FIELD(onConflict);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(groupClause);
	WRITE_NODE_FIELD(groupingSets);
	WRITE_NODE_FIELD(havingQual);
	WRITE_NODE_FIELD(windowClause);
	WRITE_NODE_FIELD(distinctClause);
	WRITE_NODE_FIELD(sortClause);
	WRITE_NODE_FIELD(scatterClause);
	WRITE_BOOL_FIELD(isTableValueSelect);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(limitCount);
	WRITE_NODE_FIELD(rowMarks);
	WRITE_NODE_FIELD(setOperations);
	WRITE_NODE_FIELD(constraintDeps);
	WRITE_BOOL_FIELD(parentStmtType);

	/* Don't serialize policy */
}

static void
_outAExpr(StringInfo str, A_Expr *node)
{
	WRITE_NODE_TYPE("AEXPR");
	WRITE_ENUM_FIELD(kind, A_Expr_Kind);

	switch (node->kind)
	{
		case AEXPR_OP:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OP_ANY:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_OP_ALL:

			WRITE_NODE_FIELD(name);

			break;
		case AEXPR_DISTINCT:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_DISTINCT:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NULLIF:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_OF:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_IN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_LIKE:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_ILIKE:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_SIMILAR:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_BETWEEN_SYM:

			WRITE_NODE_FIELD(name);
			break;
		case AEXPR_NOT_BETWEEN_SYM:

			WRITE_NODE_FIELD(name);
			break;
		default:

			break;
	}

	WRITE_NODE_FIELD(lexpr);
	WRITE_NODE_FIELD(rexpr);
	WRITE_LOCATION_FIELD(location);
}

static void
_outValue(StringInfo str, const Node *value)
{
	int16 vt = value->type;
	appendBinaryStringInfo(str, (const char *)&vt, sizeof(int16));
	switch (nodeTag(value))
	{
		case T_Integer:
			{
				long ival = ((const Integer *) value)->ival;
				appendBinaryStringInfo(str, (const char *)&ival, sizeof(long));
			}
			break;
		case T_Float:
		case T_String:
		case T_BitString:
			{
				/* Float.fval / String.sval / BitString.bsval all alias a char * */
				const char *sval = ((const String *) value)->sval;
				int slen = (sval != NULL ? strlen(sval) : 0);
				appendBinaryStringInfo(str, (const char *)&slen, sizeof(int));
				if (slen > 0)
					appendBinaryStringInfo(str, sval, slen);
			}
			break;
		case T_Boolean:
			{
				bool bval = ((const Boolean *) value)->boolval;
				appendBinaryStringInfo(str, (const char *)&bval, sizeof(bool));
			}
			break;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) value->type);
			break;
	}
}

static void
_outAConst(StringInfo str, A_Const *node)
{
	bool isnull = node->isnull;

	WRITE_NODE_TYPE("A_CONST");
	appendBinaryStringInfo(str, (const char *)&isnull, sizeof(bool));
	if (!node->isnull)
		_outValue(str, (const Node *) &node->val);
	WRITE_LOCATION_FIELD(location);  /*CDB*/
}

static void
_outCreateQueueStmt(StringInfo str, CreateQueueStmt *node)
{
	WRITE_NODE_TYPE("CREATEQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outAlterQueueStmt(StringInfo str, AlterQueueStmt *node)
{
	WRITE_NODE_TYPE("ALTERQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outCreateResourceGroupStmt(StringInfo str, CreateResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("CREATERESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outAlterResourceGroupStmt(StringInfo str, AlterResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("ALTERRESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

/*
 * Support for serializing TupleDescs and ParamListInfos.
 *
 * TupleDescs and ParamListInfos are not Nodes as such, but if you wrap them
 * in TupleDescNode and ParamListInfoNode structs, we allow serializing them.
 */
static void
_outTupleDescNode(StringInfo str, TupleDescNode *node)
{
	int			i;

	Assert(node->tuple->tdtypeid == RECORDOID);

	WRITE_NODE_TYPE("TUPLEDESCNODE");
	WRITE_INT_FIELD(natts);
	WRITE_INT_FIELD(tuple->natts);

	for (i = 0; i < node->tuple->natts; i++)
		appendBinaryStringInfo(str, (char *) TupleDescAttr(node->tuple, i), ATTRIBUTE_FIXED_PART_SIZE);

	Assert(node->tuple->constr == NULL);

	WRITE_OID_FIELD(tuple->tdtypeid);
	WRITE_INT_FIELD(tuple->tdtypmod);
	WRITE_INT_FIELD(tuple->tdrefcount);
}

static void
_outCookedConstraint(StringInfo str, CookedConstraint *node)
{
	WRITE_NODE_TYPE("COOKEDCONSTRAINT");

	WRITE_ENUM_FIELD(contype,ConstrType);
	WRITE_STRING_FIELD(name);
	WRITE_INT_FIELD(attnum);
	WRITE_NODE_FIELD(expr);
	/*
	 * GPDB dispatches pre-cooked CookedConstraints to the QEs.  is_enforced is
	 * new in PG18 (unset it deserializes as false, tripping
	 * CreateConstraintEntry's "isEnforced || !isValidated" assert for a normal
	 * validated CHECK).  skip_validation must also round-trip so the QE records
	 * the same convalidated as the coordinator (the QD sets it to
	 * !initially_valid; see AddRelationNewConstraints); without it a NOT
	 * ENFORCED / NOT VALID constraint deserializes as validated and asserts.
	 */
	WRITE_BOOL_FIELD(is_enforced);
	WRITE_BOOL_FIELD(skip_validation);
	WRITE_BOOL_FIELD(is_local);
	WRITE_INT_FIELD(inhcount);
	WRITE_BOOL_FIELD(is_no_inherit);
}

static void
_outAlterEnumStmt(StringInfo str, AlterEnumStmt *node)
{
	WRITE_NODE_TYPE("ALTERENUMSTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(oldVal);
	WRITE_STRING_FIELD(newVal);
	WRITE_STRING_FIELD(newValNeighbor);
	WRITE_BOOL_FIELD(newValIsAfter);
	WRITE_BOOL_FIELD(skipIfNewValExists);
}

static void
_outCreateFdwStmt(StringInfo str, CreateFdwStmt *node)
{
	WRITE_NODE_TYPE("CREATEFDWSTMT");

	WRITE_STRING_FIELD(fdwname);
	WRITE_NODE_FIELD(func_options);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterFdwStmt(StringInfo str, AlterFdwStmt *node)
{
	WRITE_NODE_TYPE("ALTERFDWSTMT");

	WRITE_STRING_FIELD(fdwname);
	WRITE_NODE_FIELD(func_options);
	WRITE_NODE_FIELD(options);
}

static void
_outCreateForeignServerStmt(StringInfo str, CreateForeignServerStmt *node)
{
	WRITE_NODE_TYPE("CREATEFOREIGNSERVERSTMT");

	WRITE_STRING_FIELD(servername);
	WRITE_STRING_FIELD(servertype);
	WRITE_STRING_FIELD(version);
	WRITE_STRING_FIELD(fdwname);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterForeignServerStmt(StringInfo str, AlterForeignServerStmt *node)
{
	WRITE_NODE_TYPE("ALTERFOREIGNSERVERSTMT");

	WRITE_STRING_FIELD(servername);
	WRITE_STRING_FIELD(version);
	WRITE_NODE_FIELD(options);
	WRITE_BOOL_FIELD(has_version);
}

static void
_outCreateUserMappingStmt(StringInfo str, CreateUserMappingStmt *node)
{
	WRITE_NODE_TYPE("CREATEUSERMAPPINGSTMT");

	WRITE_NODE_FIELD(user);
	WRITE_STRING_FIELD(servername);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterUserMappingStmt(StringInfo str, AlterUserMappingStmt *node)
{
	WRITE_NODE_TYPE("ALTERUSERMAPPINGSTMT");

	WRITE_NODE_FIELD(user);
	WRITE_STRING_FIELD(servername);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterObjectDependsStmt(StringInfo str, const AlterObjectDependsStmt *node)
{
	WRITE_NODE_TYPE("ALTEROBJECTDEPENDSSTMT");

	WRITE_ENUM_FIELD(objectType,ObjectType);
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(object);
	WRITE_NODE_FIELD(extname);
}

static void
_outCustomScan(StringInfo str, const CustomScan *node)
{
	WRITE_NODE_TYPE("CUSTOMSCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_UINT_FIELD(flags);
	WRITE_NODE_FIELD(custom_plans);
	WRITE_NODE_FIELD(custom_exprs);
	WRITE_NODE_FIELD(custom_private);
	WRITE_NODE_FIELD(custom_scan_tlist);
	WRITE_BITMAPSET_FIELD(custom_relids);
	WRITE_STRING_FIELD(methods->CustomName);	
}

static void
_outDropUserMappingStmt(StringInfo str, DropUserMappingStmt *node)
{
	WRITE_NODE_TYPE("DROPUSERMAPPINGSTMT");

	WRITE_NODE_FIELD(user);
	WRITE_STRING_FIELD(servername);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAccessPriv(StringInfo str, AccessPriv *node)
{
	WRITE_NODE_TYPE("ACCESSPRIV");

	WRITE_STRING_FIELD(priv_name);
	WRITE_NODE_FIELD(cols);
}

static void
_outGpPolicy(StringInfo str, GpPolicy *node)
{
	WRITE_NODE_TYPE("GPPOLICY");

	WRITE_ENUM_FIELD(ptype, GpPolicyType);
	WRITE_INT_FIELD(numsegments);
	WRITE_INT_FIELD(nattrs);
	WRITE_ATTRNUMBER_ARRAY(attrs, node->nattrs);
	WRITE_OID_ARRAY(opclasses, node->nattrs);
}

static void
_outAlterTableMoveAllStmt(StringInfo str, AlterTableMoveAllStmt *node)
{
	WRITE_NODE_TYPE("ALTERTABLESPACEMOVESTMT");

	WRITE_STRING_FIELD(orig_tablespacename);
	WRITE_ENUM_FIELD(objtype, ObjectType);
	WRITE_NODE_FIELD(roles);
	WRITE_STRING_FIELD(new_tablespacename);
	WRITE_BOOL_FIELD(nowait);
}

static void
_outAlterTableSpaceOptionsStmt(StringInfo str, AlterTableSpaceOptionsStmt *node)
{
	WRITE_NODE_TYPE("ALTERTABLESPACEOPTIONS");

	WRITE_STRING_FIELD(tablespacename);
	WRITE_NODE_FIELD(options);
	WRITE_BOOL_FIELD(isReset);
}

static void
_outCreateAmStmt(StringInfo str, const CreateAmStmt *node)
{
	WRITE_NODE_TYPE("CREATEAMSTMT");

	WRITE_STRING_FIELD(amname);
	WRITE_NODE_FIELD(handler_name);
	WRITE_INT_FIELD(amtype);
}
static void
_outAggExprId(StringInfo str, const AggExprId *node)
{
	WRITE_NODE_TYPE("AGGEXPRID");
}
static void
_outRowIdExpr(StringInfo str, const RowIdExpr *node)
{
	WRITE_NODE_TYPE("ROWIDEXPR");

	WRITE_INT_FIELD(rowidexpr_id);
}

/*
 * _outNode -
 *	  converts a Node into binary string and append it to 'str'
 */
static void
_outNode(StringInfo str, void *obj)
{
	if (obj == NULL)
	{
		int16 tg = 0;
		appendBinaryStringInfo(str, (const char *)&tg, sizeof(int16));
	}
	else if (IsA(obj, List) ||IsA(obj, IntList) || IsA(obj, OidList))
		_outList(str, obj);
	else if (IsA(obj, Integer) ||
			 IsA(obj, Float) ||
			 IsA(obj, String) ||
			 IsA(obj, Boolean) ||
			 IsA(obj, BitString))
	{
		_outValue(str, obj);
	}
	else
	{
		switch (nodeTag(obj))
		{
			case T_PlannedStmt:
				_outPlannedStmt(str,obj);
				break;
			case T_QueryDispatchDesc:
				_outQueryDispatchDesc(str,obj);
				break;
			case T_OidAssignment:
				_outOidAssignment(str,obj);
				break;
			case T_Plan:
				_outPlan(str, obj);
				break;
			case T_Result:
				_outResult(str, obj);
				break;
			case T_ProjectSet:
				_outProjectSet(str, obj);
				break;
			case T_ModifyTable:
				_outModifyTable(str, obj);
				break;
			case T_Append:
				_outAppend(str, obj);
				break;
			case T_MergeAppend:
				_outMergeAppend(str, obj);
				break;
			case T_Sequence:
				_outSequence(str, obj);
				break;
			case T_RecursiveUnion:
				_outRecursiveUnion(str, obj);
				break;
			case T_BitmapAnd:
				_outBitmapAnd(str, obj);
				break;
			case T_BitmapOr:
				_outBitmapOr(str, obj);
				break;
			case T_Gather:
				_outGather(str, obj);
				break;
			case T_GatherMerge:
				_outGatherMerge(str, obj);
				break;
			case T_Scan:
				_outScan(str, obj);
				break;
			case T_SeqScan:
				_outSeqScan(str, obj);
				break;
			case T_DynamicSeqScan:
				_outDynamicSeqScan(str, obj);
				break;
			case T_SampleScan:
				_outSampleScan(str, obj);
				break;
			case T_CteScan:
				_outCteScan(str, obj);
				break;
			case T_NamedTuplestoreScan:
				_outNamedTuplestoreScan(str, obj);
				break;
			case T_WorkTableScan:
				_outWorkTableScan(str, obj);
				break;
			case T_ForeignScan:
				_outForeignScan(str, obj);
				break;
			case T_CustomScan:
				_outCustomScan(str, obj);
				break;
			case T_ExternalScanInfo:
				_outExternalScanInfo(str, obj);
				break;
			case T_IndexScan:
				_outIndexScan(str, obj);
				break;
			case T_IndexOnlyScan:
				_outIndexOnlyScan(str, obj);
				break;
			case T_DynamicIndexScan:
				_outDynamicIndexScan(str, obj);
				break;
			case T_BitmapIndexScan:
				_outBitmapIndexScan(str, obj);
				break;
			case T_DynamicBitmapIndexScan:
				_outDynamicBitmapIndexScan(str, obj);
				break;
			case T_BitmapHeapScan:
				_outBitmapHeapScan(str, obj);
				break;
			case T_DynamicBitmapHeapScan:
				_outDynamicBitmapHeapScan(str, obj);
				break;
			case T_TidScan:
				_outTidScan(str, obj);
				break;
			case T_SubqueryScan:
				_outSubqueryScan(str, obj);
				break;
			case T_FunctionScan:
				_outFunctionScan(str, obj);
				break;
			case T_TableFuncScan:
				_outTableFuncScan(str, obj);
				break;
			case T_ValuesScan:
				_outValuesScan(str, obj);
				break;
			case T_Join:
				_outJoin(str, obj);
				break;
			case T_NestLoop:
				_outNestLoop(str, obj);
				break;
			case T_MergeJoin:
				_outMergeJoin(str, obj);
				break;
			case T_HashJoin:
				_outHashJoin(str, obj);
				break;
			case T_Agg:
				_outAgg(str, obj);
				break;
			case T_TupleSplit:
				_outTupleSplit(str, obj);
				break;
			case T_DQAExpr:
				_outDQAExpr(str, obj);
				break;
			case T_WindowAgg:
				_outWindowAgg(str, obj);
				break;
			case T_TableFunctionScan:
				_outTableFunctionScan(str, obj);
				break;
			case T_Material:
				_outMaterial(str, obj);
				break;
			case T_ShareInputScan:
				_outShareInputScan(str, obj);
				break;
			case T_Sort:
				_outSort(str, obj);
				break;
			case T_IncrementalSort:
				_outIncrementalSort(str, obj);
				break;
			case T_Unique:
				_outUnique(str, obj);
				break;
			case T_SetOp:
				_outSetOp(str, obj);
				break;
			case T_LockRows:
				_outLockRows(str, obj);
				break;
			case T_Limit:
				_outLimit(str, obj);
				break;
			case T_NestLoopParam:
				_outNestLoopParam(str, obj);
				break;
			case T_PlanRowMark:
				_outPlanRowMark(str, obj);
				break;
			case T_PartitionPruneInfo:
				_outPartitionPruneInfo(str, obj);
				break;
			case T_PartitionedRelPruneInfo:
				_outPartitionedRelPruneInfo(str, obj);
				break;
			case T_PartitionPruneStepOp:
				_outPartitionPruneStepOp(str, obj);
				break;
			case T_PartitionPruneStepCombine:
				_outPartitionPruneStepCombine(str, obj);
				break;
			case T_Hash:
				_outHash(str, obj);
				break;
			case T_Motion:
				_outMotion(str, obj);
				break;
			case T_SplitUpdate:
				_outSplitUpdate(str, obj);
				break;
			case T_AssertOp:
				_outAssertOp(str, obj);
				break;
			case T_PartitionSelector:
				_outPartitionSelector(str, obj);
				break;
			case T_Alias:
				_outAlias(str, obj);
				break;
			case T_RangeVar:
				_outRangeVar(str, obj);
				break;
			case T_TableFunc:
				_outTableFunc(str, obj);
				break;
			case T_IntoClause:
				_outIntoClause(str, obj);
				break;
			case T_CopyIntoClause:
				_outCopyIntoClause(str, obj);
				break;
			case T_RefreshClause:
				_outRefreshClause(str, obj);
				break;
			case T_Var:
				_outVar(str, obj);
				break;
			case T_Const:
				_outConst(str, obj);
				break;
			case T_Param:
				_outParam(str, obj);
				break;
			case T_Aggref:
				_outAggref(str, obj);
				break;
			case T_GroupingFunc:
				_outGroupingFunc(str, obj);
				break;
			case T_GroupId:
				_outGroupId(str, obj);
				break;
			case T_GroupingSetId:
				_outGroupingSetId(str, obj);
				break;
			case T_WindowFunc:
				_outWindowFunc(str, obj);
				break;
			case T_SubscriptingRef:
				_outSubscriptingRef(str, obj);
				break;
			case T_FuncExpr:
				_outFuncExpr(str, obj);
				break;
			case T_NamedArgExpr:
				_outNamedArgExpr(str, obj);
				break;
			case T_OpExpr:
				_outOpExpr(str, obj);
				break;
			case T_DistinctExpr:
				_outDistinctExpr(str, obj);
				break;
			case T_ScalarArrayOpExpr:
				_outScalarArrayOpExpr(str, obj);
				break;
			case T_BoolExpr:
				_outBoolExpr(str, obj);
				break;
			case T_SubLink:
				_outSubLink(str, obj);
				break;
			case T_SubPlan:
				_outSubPlan(str, obj);
				break;
			case T_AlternativeSubPlan:
				_outAlternativeSubPlan(str, obj);
				break;
			case T_FieldSelect:
				_outFieldSelect(str, obj);
				break;
			case T_FieldStore:
				_outFieldStore(str, obj);
				break;
			case T_RelabelType:
				_outRelabelType(str, obj);
				break;
			case T_CoerceViaIO:
				_outCoerceViaIO(str, obj);
				break;
			case T_ArrayCoerceExpr:
				_outArrayCoerceExpr(str, obj);
				break;
			case T_ConvertRowtypeExpr:
				_outConvertRowtypeExpr(str, obj);
				break;
			case T_CollateExpr:
				_outCollateExpr(str, obj);
				break;
			case T_CaseExpr:
				_outCaseExpr(str, obj);
				break;
			case T_CaseWhen:
				_outCaseWhen(str, obj);
				break;
			case T_CaseTestExpr:
				_outCaseTestExpr(str, obj);
				break;
			case T_ArrayExpr:
				_outArrayExpr(str, obj);
				break;
			case T_RowExpr:
				_outRowExpr(str, obj);
				break;
			case T_RowCompareExpr:
				_outRowCompareExpr(str, obj);
				break;
			case T_CoalesceExpr:
				_outCoalesceExpr(str, obj);
				break;
			case T_MinMaxExpr:
				_outMinMaxExpr(str, obj);
				break;
			case T_NullIfExpr:
				_outNullIfExpr(str, obj);
				break;
			case T_NullTest:
				_outNullTest(str, obj);
				break;
			case T_BooleanTest:
				_outBooleanTest(str, obj);
				break;
			case T_SQLValueFunction:
				_outSQLValueFunction(str, obj);
				break;
			case T_XmlExpr:
				_outXmlExpr(str, obj);
				break;
			case T_CoerceToDomain:
				_outCoerceToDomain(str, obj);
				break;
			case T_CoerceToDomainValue:
				_outCoerceToDomainValue(str, obj);
				break;
			case T_SetToDefault:
				_outSetToDefault(str, obj);
				break;
			case T_CurrentOfExpr:
				_outCurrentOfExpr(str, obj);
				break;
			case T_NextValueExpr:
				_outNextValueExpr(str, obj);
				break;
			case T_InferenceElem:
				_outInferenceElem(str, obj);
				break;
			case T_TargetEntry:
				_outTargetEntry(str, obj);
				break;
			case T_RangeTblRef:
				_outRangeTblRef(str, obj);
				break;
			case T_JoinExpr:
				_outJoinExpr(str, obj);
				break;
			case T_FromExpr:
				_outFromExpr(str, obj);
				break;
			case T_OnConflictExpr:
				_outOnConflictExpr(str, obj);
				break;
			case T_CreateExtensionStmt:
				_outCreateExtensionStmt(str, obj);
				break;
			case T_GrantStmt:
				_outGrantStmt(str, obj);
				break;
			case T_AccessPriv:
				_outAccessPriv(str, obj);
				break;
			case T_ObjectWithArgs:
				_outObjectWithArgs(str, obj);
				break;
			case T_GrantRoleStmt:
				_outGrantRoleStmt(str, obj);
				break;
			case T_LockStmt:
				_outLockStmt(str, obj);
				break;

			case T_ExtensibleNode:
				_outExtensibleNode(str, obj);
				break;

			case T_CreateStmt:
				_outCreateStmt(str, obj);
				break;
			case T_CreateForeignTableStmt:
				_outCreateForeignTableStmt(str, obj);
				break;
			case T_DistributionKeyElem:
				_outDistributionKeyElem(str, obj);
				break;
			case T_ColumnReferenceStorageDirective:
				_outColumnReferenceStorageDirective(str, obj);
				break;
			case T_RoleSpec:
				_outRoleSpec(str, obj);
				break;

			case T_SegfileMapNode:
				_outSegfileMapNode(str, obj);
				break;

			case T_ExtTableTypeDesc:
				_outExtTableTypeDesc(str, obj);
				break;
            case T_CreateExternalStmt:
				_outCreateExternalStmt(str, obj);
				break;

			case T_IndexStmt:
				_outIndexStmt(str, obj);
				break;
			case T_ReindexStmt:
				_outReindexStmt(str, obj);
				break;

			case T_ConstraintsSetStmt:
				_outConstraintsSetStmt(str, obj);
				break;

			case T_CreateFunctionStmt:
				_outCreateFunctionStmt(str, obj);
				break;
			case T_ReturnStmt:
				_outReturnStmt(str, obj);
				break;
			case T_FunctionParameter:
				_outFunctionParameter(str, obj);
				break;
			case T_AlterFunctionStmt:
				_outAlterFunctionStmt(str, obj);
				break;

			case T_AlterObjectDependsStmt:
				_outAlterObjectDependsStmt(str, obj);
				break;

			case T_DefineStmt:
				_outDefineStmt(str,obj);
				break;

			case T_CompositeTypeStmt:
				_outCompositeTypeStmt(str,obj);
				break;
			case T_CreateEnumStmt:
				_outCreateEnumStmt(str,obj);
				break;
			case T_CreateRangeStmt:
				_outCreateRangeStmt(str, obj);
				break;
			case T_AlterEnumStmt:
				_outAlterEnumStmt(str, obj);
				break;

			case T_CreateCastStmt:
				_outCreateCastStmt(str,obj);
				break;
			case T_CreateOpClassStmt:
				_outCreateOpClassStmt(str,obj);
				break;
			case T_CreateOpClassItem:
				_outCreateOpClassItem(str,obj);
				break;
			case T_CreateOpFamilyStmt:
				_outCreateOpFamilyStmt(str,obj);
				break;
			case T_AlterOpFamilyStmt:
				_outAlterOpFamilyStmt(str,obj);
				break;
			case T_CreateConversionStmt:
				_outCreateConversionStmt(str,obj);
				break;


			case T_ViewStmt:
				_outViewStmt(str, obj);
				break;
			case T_RuleStmt:
				_outRuleStmt(str, obj);
				break;
			case T_DropStmt:
				_outDropStmt(str, obj);
				break;
			case T_DropOwnedStmt:
				_outDropOwnedStmt(str, obj);
				break;
			case T_ReassignOwnedStmt:
				_outReassignOwnedStmt(str, obj);
				break;
			case T_TruncateStmt:
				_outTruncateStmt(str, obj);
				break;
			case T_ReplicaIdentityStmt:
				_outReplicaIdentityStmt(str, obj);
				break;
			case T_AlterTableStmt:
				_outAlterTableStmt(str, obj);
				break;
			case T_AlterTableCmd:
				_outAlterTableCmd(str, obj);
				break;
			case T_AlteredTableInfo:
				_outAlteredTableInfo(str, obj);
				break;
			case T_NewConstraint:
				_outNewConstraint(str, obj);
				break;
			case T_NewColumnValue:
				_outNewColumnValue(str, obj);
				break;

			case T_CreateRoleStmt:
				_outCreateRoleStmt(str, obj);
				break;
			case T_DropRoleStmt:
				_outDropRoleStmt(str, obj);
				break;
			case T_AlterRoleStmt:
				_outAlterRoleStmt(str, obj);
				break;
			case T_AlterRoleSetStmt:
				_outAlterRoleSetStmt(str, obj);
				break;
			case T_AlterSystemStmt:
				_outAlterSystemStmt(str, obj);
				break;

			case T_AlterObjectSchemaStmt:
				_outAlterObjectSchemaStmt(str, obj);
				break;

			case T_AlterOwnerStmt:
				_outAlterOwnerStmt(str, obj);
				break;

			case T_RenameStmt:
				_outRenameStmt(str, obj);
				break;

			case T_CreateSeqStmt:
				_outCreateSeqStmt(str, obj);
				break;
			case T_AlterSeqStmt:
				_outAlterSeqStmt(str, obj);
				break;
			case T_ClusterStmt:
				_outClusterStmt(str, obj);
				break;
			case T_CreatedbStmt:
				_outCreatedbStmt(str, obj);
				break;
			case T_DropdbStmt:
				_outDropdbStmt(str, obj);
				break;
			case T_CreateDomainStmt:
				_outCreateDomainStmt(str, obj);
				break;
			case T_AlterDatabaseStmt:
				_outAlterDatabaseStmt(str, obj);
				break;
			case T_AlterDomainStmt:
				_outAlterDomainStmt(str, obj);
				break;
			case T_AlterDefaultPrivilegesStmt:
				_outAlterDefaultPrivilegesStmt(str, obj);
				break;
			case T_TransactionStmt:
				_outTransactionStmt(str, obj);
				break;
			case T_CreateStatsStmt:
				_outCreateStatsStmt(str, obj);
				break;
			case T_NotifyStmt:
				_outNotifyStmt(str, obj);
				break;
			case T_DeclareCursorStmt:
				_outDeclareCursorStmt(str, obj);
				break;
			case T_SingleRowErrorDesc:
				_outSingleRowErrorDesc(str, obj);
				break;
			case T_CopyStmt:
				_outCopyStmt(str, obj);
				break;
			case T_SelectStmt:
				_outSelectStmt(str, obj);
				break;
			case T_InsertStmt:
				_outInsertStmt(str, obj);
				break;
			case T_DeleteStmt:
				_outDeleteStmt(str, obj);
				break;
			case T_UpdateStmt:
				_outUpdateStmt(str, obj);
				break;
			case T_ColumnDef:
				_outColumnDef(str, obj);
				break;
			case T_TypeName:
				_outTypeName(str, obj);
				break;
			case T_SortBy:
				_outSortBy(str, obj);
				break;
			case T_TypeCast:
				_outTypeCast(str, obj);
				break;
			case T_CollateClause:
				_outCollateClause(str, obj);
				break;
			case T_IndexElem:
				_outIndexElem(str, obj);
				break;
			case T_StatsElem:
				_outStatsElem(str, obj);
				break;
			case T_WindowDef:
				_outWindowDef(str, obj);
				break;
			case T_RangeSubselect:
				_outRangeSubselect(str, obj);
				break;
			case T_InferClause:
				_outInferClause(str, obj);
				break;
			case T_OnConflictClause:
				_outOnConflictClause(str, obj);
				break;
			case T_Query:
				_outQuery(str, obj);
				break;
			case T_WithCheckOption:
				_outWithCheckOption(str, obj);
				break;
			case T_SortGroupClause:
				_outSortGroupClause(str, obj);
				break;
			case T_GroupingSet:
				_outGroupingSet(str, obj);
				break;
			case T_WindowClause:
				_outWindowClause(str, obj);
				break;
			case T_RowMarkClause:
				_outRowMarkClause(str, obj);
				break;
			case T_WithClause:
				_outWithClause(str, obj);
				break;
			case T_CTESearchClause:
				_outCTESearchClause(str, obj);
				break;
			case T_CTECycleClause:
				_outCTECycleClause(str, obj);
				break;
			case T_CommonTableExpr:
				_outCommonTableExpr(str, obj);
				break;
			case T_SetOperationStmt:
				_outSetOperationStmt(str, obj);
				break;
			case T_RangeTblEntry:
				_outRangeTblEntry(str, obj);
				break;
			case T_RTEPermissionInfo:
				_outRTEPermissionInfo(str, obj);
				break;
			case T_RangeTblFunction:
				_outRangeTblFunction(str, obj);
				break;
			case T_TableSampleClause:
				_outTableSampleClause(str, obj);
				break;
			case T_A_Expr:
				_outAExpr(str, obj);
				break;
			case T_ColumnRef:
				_outColumnRef(str, obj);
				break;
			case T_ParamRef:
				_outParamRef(str, obj);
				break;
			case T_RawStmt:
				_outRawStmt(str, obj);
				break;
			case T_A_Const:
				_outAConst(str, obj);
				break;
			case T_A_Star:
				_outA_Star(str, obj);
				break;
			case T_A_Indices:
				_outA_Indices(str, obj);
				break;
			case T_A_Indirection:
				_outA_Indirection(str, obj);
				break;
			case T_A_ArrayExpr:
				_outA_ArrayExpr(str,obj);
				break;
			case T_ResTarget:
				_outResTarget(str, obj);
				break;
			case T_MultiAssignRef:
				_outMultiAssignRef(str, obj);
				break;
			case T_Constraint:
				_outConstraint(str, obj);
				break;
			case T_FuncCall:
				_outFuncCall(str, obj);
				break;
			case T_DefElem:
				_outDefElem(str, obj);
				break;
			case T_TableLikeClause:
				_outTableLikeClause(str, obj);
				break;
			case T_LockingClause:
				_outLockingClause(str, obj);
				break;
			case T_XmlSerialize:
				_outXmlSerialize(str, obj);
				break;
			case T_TriggerTransition:
				_outTriggerTransition(str, obj);
				break;
			case T_PartitionElem:
				_outPartitionElem(str, obj);
				break;
			case T_PartitionSpec:
				_outPartitionSpec(str, obj);
				break;
			case T_PartitionBoundSpec:
				_outPartitionBoundSpec(str, obj);
				break;
			case T_PartitionRangeDatum:
				_outPartitionRangeDatum(str, obj);
				break;
			case T_PartitionCmd:
				_outPartitionCmd(str, obj);
				break;
			case T_GpAlterPartitionId:
				_outGpAlterPartitionId(str, obj);
				break;
			case T_GpDropPartitionCmd:
				_outGpDropPartitionCmd(str, obj);
				break;
			case T_GpAlterPartitionCmd:
				_outGpAlterPartitionCmd(str, obj);
				break;

			case T_CreateSchemaStmt:
				_outCreateSchemaStmt(str, obj);
				break;
			case T_CreatePLangStmt:
				_outCreatePLangStmt(str, obj);
				break;
			case T_VacuumStmt:
				_outVacuumStmt(str, obj);
				break;
			case T_VacuumRelation:
				_outVacuumRelation(str, obj);
				break;
			case T_CdbProcess:
				_outCdbProcess(str, obj);
				break;
			case T_SliceTable:
				_outSliceTable(str, obj);
				break;
			case T_CursorPosInfo:
				_outCursorPosInfo(str, obj);
				break;
			case T_VariableSetStmt:
				_outVariableSetStmt(str, obj);
				break;

			case T_DMLActionExpr:
				_outDMLActionExpr(str, obj);
				break;

			case T_CreateTrigStmt:
				_outCreateTrigStmt(str, obj);
				break;

			case T_CreateTableSpaceStmt:
				_outCreateTableSpaceStmt(str, obj);
				break;

			case T_DropTableSpaceStmt:
				_outDropTableSpaceStmt(str, obj);
				break;

			case T_CreateQueueStmt:
				_outCreateQueueStmt(str, obj);
				break;
			case T_AlterQueueStmt:
				_outAlterQueueStmt(str, obj);
				break;
			case T_DropQueueStmt:
				_outDropQueueStmt(str, obj);
				break;

			case T_CreateResourceGroupStmt:
				_outCreateResourceGroupStmt(str, obj);
				break;
			case T_DropResourceGroupStmt:
				_outDropResourceGroupStmt(str, obj);
				break;
			case T_AlterResourceGroupStmt:
				_outAlterResourceGroupStmt(str, obj);
				break;

            case T_CommentStmt:
                _outCommentStmt(str, obj);
                break;
			case T_TableValueExpr:
				_outTableValueExpr(str, obj);
                break;
			case T_DenyLoginInterval:
				_outDenyLoginInterval(str, obj);
				break;
			case T_DenyLoginPoint:
				_outDenyLoginPoint(str, obj);
				break;

			case T_AlterTypeStmtSetDefaultEnc:
				_outAlterTypeStmtSetDefaultEnc(str, obj);
				break;
			case T_AlterTypeStmt:
				_outAlterTypeStmt(str, obj);
				break;
			case T_AlterExtensionStmt:
				_outAlterExtensionStmt(str, obj);
				break;
			case T_AlterExtensionContentsStmt:
				_outAlterExtensionContentsStmt(str, obj);
				break;

			case T_TupleDescNode:
				_outTupleDescNode(str, obj);
				break;
			case T_SerializedParams:
				_outSerializedParams(str, obj);
				break;

			case T_AlterTSConfigurationStmt:
				_outAlterTSConfigurationStmt(str, obj);
				break;
			case T_AlterTSDictionaryStmt:
				_outAlterTSDictionaryStmt(str, obj);
				break;

			case T_CookedConstraint:
				_outCookedConstraint(str, obj);
				break;

			case T_DropUserMappingStmt:
				_outDropUserMappingStmt(str, obj);
				break;
			case T_AlterUserMappingStmt:
				_outAlterUserMappingStmt(str, obj);
				break;
			case T_CreateUserMappingStmt:
				_outCreateUserMappingStmt(str, obj);
				break;
			case T_AlterForeignServerStmt:
				_outAlterForeignServerStmt(str, obj);
				break;
			case T_CreateForeignServerStmt:
				_outCreateForeignServerStmt(str, obj);
				break;
			case T_AlterFdwStmt:
				_outAlterFdwStmt(str, obj);
				break;
			case T_CreateFdwStmt:
				_outCreateFdwStmt(str, obj);
				break;
			case T_GpPolicy:
				_outGpPolicy(str, obj);
				break;
			case T_DistributedBy:
				_outDistributedBy(str, obj);
				break;
			case T_ImportForeignSchemaStmt:
				_outImportForeignSchemaStmt(str, obj);
				break;
			case T_AlterTableMoveAllStmt:
				_outAlterTableMoveAllStmt(str, obj);
				break;
			case T_AlterTableSpaceOptionsStmt:
				_outAlterTableSpaceOptionsStmt(str, obj);
				break;

			case T_CreatePublicationStmt:
				_outCreatePublicationStmt(str, obj);
				break;
			case T_AlterPublicationStmt:
				_outAlterPublicationStmt(str, obj);
				break;
			case T_CreateSubscriptionStmt:
				_outCreateSubscriptionStmt(str, obj);
				break;
			case T_DropSubscriptionStmt:
				_outDropSubscriptionStmt(str, obj);
				break;
			case T_AlterSubscriptionStmt:
				_outAlterSubscriptionStmt(str, obj);
				break;

			case T_CreatePolicyStmt:
				_outCreatePolicyStmt(str, obj);
				break;
			case T_AlterPolicyStmt:
				_outAlterPolicyStmt(str, obj);
				break;
			case T_CreateTransformStmt:
				_outCreateTransformStmt(str, obj);
				break;
			case T_CreateAmStmt:
				_outCreateAmStmt(str, obj);
				break;
			case T_AggExprId:
				_outAggExprId(str, obj);
				break;
			case T_RowIdExpr:
				_outRowIdExpr(str, obj);
				break;
			case T_RestrictInfo:
				_outRestrictInfo(str, obj);
				break;
			case T_AppendRelInfo:
				_outAppendRelInfo(str, obj);
				break;
			case T_MergeAction:
				_outMergeAction(str, obj);
				break;

			/*
			 * GPDB: PG15 SQL/JSON nodes — needed to dispatch JSON_TABLE /
			 * JSON_VALUE / JSON_QUERY / IS JSON / JSON constructors over
			 * distributed tables. The _out* bodies come from outfuncs.c.
			 */
			case T_JsonFormat:
				_outJsonFormat(str, obj);
				break;
			case T_JsonReturning:
				_outJsonReturning(str, obj);
				break;
			case T_JsonValueExpr:
				_outJsonValueExpr(str, obj);
				break;
			case T_JsonConstructorExpr:
				_outJsonConstructorExpr(str, obj);
				break;
			case T_JsonIsPredicate:
				_outJsonIsPredicate(str, obj);
				break;
			case T_JsonOutput:
				_outJsonOutput(str, obj);
				break;

			/* GPDB: PG17 SQL/JSON and MERGE/window expression nodes */
			case T_WindowFuncRunCondition:
				_outWindowFuncRunCondition(str, obj);
				break;
			case T_MergeSupportFunc:
				_outMergeSupportFunc(str, obj);
				break;
			case T_ReturningExpr:
				_outReturningExpr(str, obj);
				break;
			case T_JsonBehavior:
				_outJsonBehavior(str, obj);
				break;
			case T_JsonExpr:
				_outJsonExpr(str, obj);
				break;
			case T_JsonTablePath:
				_outJsonTablePath(str, obj);
				break;
			case T_JsonTablePathScan:
				_outJsonTablePathScan(str, obj);
				break;
			case T_JsonTableSiblingJoin:
				_outJsonTableSiblingJoin(str, obj);
				break;
			case T_JsonArgument:
				_outJsonArgument(str, obj);
				break;
			case T_JsonFuncExpr:
				_outJsonFuncExpr(str, obj);
				break;
			case T_JsonTablePathSpec:
				_outJsonTablePathSpec(str, obj);
				break;
			case T_JsonTable:
				_outJsonTable(str, obj);
				break;
			case T_JsonTableColumn:
				_outJsonTableColumn(str, obj);
				break;
			case T_JsonParseExpr:
				_outJsonParseExpr(str, obj);
				break;
			case T_JsonScalarExpr:
				_outJsonScalarExpr(str, obj);
				break;
			case T_JsonSerializeExpr:
				_outJsonSerializeExpr(str, obj);
				break;

			case T_PublicationObjSpec:
				_outPublicationObjSpec(str, obj);
				break;
			case T_PublicationTable:
				_outPublicationTable(str, obj);
				break;

			default:
				elog(ERROR, "could not serialize unrecognized node type: %d",
						 (int) nodeTag(obj));
				break;
		}
	}
}

/*
 * nodeToBinaryStringFast -
 *	   returns a binary representation of the Node as a palloc'd string
 */
char *
nodeToBinaryStringFast(void *obj, int *length)
{
	StringInfoData str;
	int16 tg = (int16) 0xDEAD;

	/* see stringinfo.h for an explanation of this maneuver */
	initStringInfoOfSize(&str, 4096);

	_outNode(&str, obj);

	/* Add something special at the end that we can check in readfast.c */
	appendBinaryStringInfo(&str, (const char *)&tg, sizeof(int16));

	*length = str.len;
	return str.data;
}
