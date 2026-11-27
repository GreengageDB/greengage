/*--------------------------------------------------------------------------
 *
 * test_custom_scan.c
 *		A custom scan provider for tests: a scan that passes the rows of
 *		its children through.
 *
 * The provider wraps every subquery in FROM whose alias is "cscan". The
 * cheapest path of the subquery becomes the one child of a CustomScan,
 * or, for a UNION ALL, each branch becomes a child, and the scan returns
 * the rows of its children unchanged, one child after another. A query
 * gives the same rows with the provider as without it, so a test can put
 * any plan under a custom scan: a Motion, a shared scan of a CTE, a
 * SubPlan, and run the query.
 *
 * A subquery aliased "cscan_plain" is wrapped the same way, in a scan
 * without the squelch callback, whose children the core squelches.
 *
 * A table aliased "cscan" is wrapped too, as a provider's scan of the
 * table: the scan reads the table (scanrelid), and keeps its one child,
 * the table's own path, in lefttree rather than custom_plans, as some
 * providers do. For the parent of an inheritance tree or a partitioned
 * table that child is the Append of its members, and the scan takes its
 * locus. A scan of a table that returns no columns is left alone.
 * With test_custom_scan.strewn, the scan of a table says its rows are
 * strewn over the segments, as if the provider read a copy of the table
 * laid out otherwise; it still reads the table. With
 * test_custom_scan.outer_join_locus, it says its rows are hashed on the
 * table's key but a NULL key may lie on any segment (HashedOJ).
 *
 * DML and row-locking queries are left alone: EvalPlanQual rechecks their
 * rows through the scan, which this provider does not implement.
 *
 * The scan keeps a value node in custom_private, as providers commonly
 * do. Four settings choose its flags: test_custom_scan.child_rows_only
 * (CUSTOMPATH_GP_CHILD_ROWS_ONLY), test_custom_scan.projection
 * (CUSTOMPATH_SUPPORT_PROJECTION), test_custom_scan.memory_intensive
 * (CUSTOMPATH_GP_MEMORY_INTENSIVE) and
 * test_custom_scan.memory_overlaps_children
 * (CUSTOMPATH_GP_MEMORY_OVERLAPS_CHILDREN). With
 * test_custom_scan.last_branch_first, the scan reads the branches of a
 * UNION ALL from the last to the first, and lists its children in that
 * order, as extensible.h asks. With test_custom_scan.buffer, the scan
 * reads all the rows of its children into a buffer first, as a Material
 * does, releases the buffer on a squelch, and fills it again after a
 * rescan.
 *
 * A plan with the scan reaches the segments, which must know the scan's
 * methods to read it: the library is loaded in every backend of the
 * cluster, through session_preload_libraries (see sql/setup.sql).
 *
 * IDENTIFICATION
 *		src/test/modules/test_custom_scan/test_custom_scan.c
 *
 *--------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/value.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* The alias of the subqueries the provider wraps. */
#define WRAPPED_ALIAS "cscan"
/* The alias of the subqueries it wraps in a scan without squelch callback. */
#define PLAIN_ALIAS "cscan_plain"

#define PASS_NAME "TestPassThrough"

static bool child_rows_only = false;
static bool projection = false;
static bool memory_intensive = false;
static bool memory_overlaps_children = false;
static bool last_branch_first = false;
static bool strewn = false;
static bool outer_join_locus = false;
static bool buffer = false;

typedef struct PassState
{
	CustomScanState css;
	List	   *children;		/* custom_ps, or the child in lefttree */
	int			done;			/* the children read to their end */
	int			shutdowns;		/* calls of the shutdown callback */
	int			squelches;		/* calls of the squelch callback */
	bool		buffered;		/* return the rows from a buffer */
	Tuplestorestate *buffer;	/* the children's rows, or NULL */
	TupleTableSlot *buffer_slot;	/* a row read from the buffer */
	int			builds;			/* buffers filled from the children */
	int			releases;		/* buffers released on squelch */
} PassState;

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

static Plan *plan_pass(PlannerInfo *root, RelOptInfo *rel,
					   CustomPath *best_path, List *tlist, List *clauses,
					   List *custom_plans);
static Node *create_pass_state(CustomScan *cscan);
static void begin_pass(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *exec_pass(CustomScanState *node);
static void end_pass(CustomScanState *node);
static void rescan_pass(CustomScanState *node);
static void shutdown_pass(CustomScanState *node);
static void squelch_pass(CustomScanState *node);
static void explain_pass(CustomScanState *node, List *ancestors,
						 ExplainState *es);

static const CustomPathMethods pass_path_methods = {
	.CustomName = PASS_NAME,
	.PlanCustomPath = plan_pass,
};

static const CustomScanMethods pass_scan_methods = {
	.CustomName = PASS_NAME,
	.CreateCustomScanState = create_pass_state,
};

static const CustomExecMethods pass_exec_methods = {
	.CustomName = PASS_NAME,
	.BeginCustomScan = begin_pass,
	.ExecCustomScan = exec_pass,
	.EndCustomScan = end_pass,
	.ReScanCustomScan = rescan_pass,
	.ShutdownCustomScan = shutdown_pass,
	.ExplainCustomScan = explain_pass,
	.SquelchCustomScan = squelch_pass,
};

/* The same methods without SquelchCustomScan, set in _PG_init. */
static CustomExecMethods plain_exec_methods;

/*
 * set_rel_pathlist_hook: a subquery or a table aliased "cscan" gets a
 * custom path over its cheapest path.
 */
static void
wrap_rel(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Path	   *best = NULL;
	List	   *children;
	CustomPath *cpath;
	ListCell   *lc;

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	/*
	 * EvalPlanQual rechecks a row of a DML or a row-locking query through
	 * the scan, which must then test the row itself and put it in its slot.
	 */
	if (root->parse->commandType != CMD_SELECT || root->rowMarks != NIL)
		return;

	if (rel->reloptkind != RELOPT_BASEREL ||
		!(rte->rtekind == RTE_SUBQUERY ||
		  (rte->rtekind == RTE_RELATION &&
		   (rte->relkind == RELKIND_RELATION ||
			rte->relkind == RELKIND_PARTITIONED_TABLE))) ||
		(strcmp(rte->eref->aliasname, WRAPPED_ALIAS) != 0 &&
		 (rte->rtekind != RTE_SUBQUERY ||
		  strcmp(rte->eref->aliasname, PLAIN_ALIAS) != 0)))
		return;

	foreach(lc, rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);

		if (path->param_info == NULL &&
			(best == NULL || path->total_cost < best->total_cost))
			best = path;
	}
	if (best == NULL)
		return;

	/*
	 * The custom_scan_tlist of a scan of a table that returns no columns
	 * would be empty, which the core takes for none: the scan's tuple would
	 * be the table's row rather than the child's.
	 */
	if (rte->rtekind == RTE_RELATION && best->pathtarget->exprs == NIL)
		return;

	/*
	 * The branches of a UNION ALL become the children, in the order the scan
	 * reads them. The Append of an inheritance tree or a partitioned table
	 * stays the one child of the scan of the table.
	 */
	children = NIL;
	if (IsA(best, AppendPath) && rte->rtekind == RTE_SUBQUERY)
	{
		foreach(lc, ((AppendPath *) best)->subpaths)
			children = last_branch_first ? lcons(lfirst(lc), children) :
				lappend(children, lfirst(lc));
	}
	else
		children = list_make1(best);
	if (children == NIL)
		return;

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.rows = best->rows;
	cpath->path.startup_cost = best->startup_cost;
	cpath->path.total_cost = best->total_cost;
	cpath->path.pathkeys = list_length(children) == 1 ? best->pathkeys : NIL;
	cpath->path.locus = best->locus;
	if (strewn && rte->rtekind == RTE_RELATION)
		CdbPathLocus_MakeStrewn(&cpath->path.locus, best->locus.numsegments);
	if (outer_join_locus && rte->rtekind == RTE_RELATION &&
		CdbPathLocus_IsHashed(cpath->path.locus))
		cpath->path.locus.locustype = CdbLocusType_HashedOJ;
	cpath->path.motionHazard = best->motionHazard;
	cpath->path.rescannable = best->rescannable;
	cpath->path.sameslice_relids = best->sameslice_relids;
	cpath->flags = 0;
	if (child_rows_only)
		cpath->flags |= CUSTOMPATH_GP_CHILD_ROWS_ONLY;
	if (projection)
		cpath->flags |= CUSTOMPATH_SUPPORT_PROJECTION;
	if (memory_intensive)
		cpath->flags |= CUSTOMPATH_GP_MEMORY_INTENSIVE;
	if (memory_overlaps_children)
		cpath->flags |= CUSTOMPATH_GP_MEMORY_OVERLAPS_CHILDREN;
	cpath->custom_paths = children;

	/*
	 * The columns of the children's rows stay with the path. The planner may
	 * later change rel->reltarget, and the custom path's own target, to the
	 * expressions above the scan.
	 */
	cpath->custom_private = list_make1(best->pathtarget);
	cpath->methods = &pass_path_methods;

	/*
	 * The custom path replaces the rel's paths: add_path would free the
	 * paths it beats, the child among them.
	 */
	rel->pathlist = NIL;
	add_path(rel, &cpath->path);
}

static Plan *
plan_pass(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
		  List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

	cscan->scan.plan.targetlist = tlist;

	/*
	 * The children check the restrictions themselves. A scan of a subquery
	 * reads no relation: its tuple is a row of the subquery.
	 */
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = 0;
	if (rel->rtekind == RTE_RELATION)
	{
		/*
		 * A scan of the table, whose one child, the table's own scan, sits
		 * in lefttree.
		 */
		cscan->scan.scanrelid = rel->relid;
		outerPlan(cscan) = linitial(custom_plans);
		custom_plans = NIL;
	}
	cscan->flags = best_path->flags;
	cscan->custom_plans = custom_plans;
	cscan->custom_exprs = NIL;
	/*
	 * Value nodes: the number of children in custom_plans, whether the scan
	 * returns the rows from a buffer, and whether it has no squelch
	 * callback.
	 */
	cscan->custom_private =
		list_make3(makeInteger(list_length(custom_plans)),
				   makeInteger(buffer),
				   makeInteger(strcmp(rte->eref->aliasname, PLAIN_ALIAS) == 0));
	cscan->custom_scan_tlist =
		make_tlist_from_pathtarget(linitial_node(PathTarget,
												 best_path->custom_private));
	cscan->methods = &pass_scan_methods;

	return &cscan->scan.plan;
}

static Node *
create_pass_state(CustomScan *cscan)
{
	PassState  *state = (PassState *) newNode(sizeof(PassState),
											  T_CustomScanState);

	state->css.methods = intVal(lthird(cscan->custom_private)) ?
		&plain_exec_methods : &pass_exec_methods;
	return (Node *) state;
}

static void
begin_pass(CustomScanState *node, EState *estate, int eflags)
{
	PassState  *state = (PassState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	ListCell   *lc;

	Assert(list_length(cscan->custom_plans) ==
		   intVal(linitial(cscan->custom_private)));
	foreach(lc, cscan->custom_plans)
		node->custom_ps = lappend(node->custom_ps,
								  ExecInitNode((Plan *) lfirst(lc), estate,
											   eflags));
	if (outerPlan(cscan) != NULL)
	{
		outerPlanState(node) = ExecInitNode(outerPlan(cscan), estate, eflags);
		state->children = list_make1(outerPlanState(node));
	}
	else
		state->children = node->custom_ps;
	state->buffered = intVal(lsecond(cscan->custom_private)) != 0;
	if (state->buffered)
		state->buffer_slot =
			ExecInitExtraTupleSlot(estate,
								   node->ss.ss_ScanTupleSlot->tts_tupleDescriptor,
								   &TTSOpsMinimalTuple);
}

/* The next row of the children, or NULL after the last. */
static TupleTableSlot *
child_row(PassState *state)
{
	int			nchildren = list_length(state->children);

	while (state->done < nchildren)
	{
		TupleTableSlot *slot;

		slot = ExecProcNode((PlanState *) list_nth(state->children,
												   state->done));
		if (!TupIsNull(slot))
			return slot;
		state->done++;
	}
	return NULL;
}

/*
 * The next row of the scan, or an empty slot after the last. A buffered
 * scan reads all the rows of its children into the buffer first, as a
 * Material does, and returns them from there.
 *
 * The buffer tests squelch and rescan; a provider should not copy it.
 * Unlike a Material, it keeps its rows after the last one until a squelch,
 * a rescan or the end of the plan, and it passes EXEC_FLAG_REWIND on to
 * the children, which may then keep the same rows for a rescan too.
 */
static TupleTableSlot *
next_row(ScanState *ss)
{
	PassState  *state = (PassState *) ss;
	TupleTableSlot *slot;

	if (!state->buffered)
	{
		slot = child_row(state);
		if (slot == NULL)
			return ExecClearTuple(ss->ss_ScanTupleSlot);
		return ExecCopySlot(ss->ss_ScanTupleSlot, slot);
	}
	if (state->buffer == NULL)
	{
		state->buffer = tuplestore_begin_heap(false, false,
											  PlanStateOperatorMemKB(&ss->ps));
		state->builds++;
		while ((slot = child_row(state)) != NULL)
			tuplestore_puttupleslot(state->buffer, slot);
	}
	if (!tuplestore_gettupleslot(state->buffer, true, false,
								 state->buffer_slot))
		return ExecClearTuple(ss->ss_ScanTupleSlot);
	return ExecCopySlot(ss->ss_ScanTupleSlot, state->buffer_slot);
}

static bool
recheck_row(ScanState *ss, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
exec_pass(CustomScanState *node)
{
	return ExecScan(&node->ss, next_row, recheck_row);
}

static void
end_pass(CustomScanState *node)
{
	PassState  *state = (PassState *) node;
	ListCell   *lc;

	if (state->buffer != NULL)
		tuplestore_end(state->buffer);
	foreach(lc, state->children)
		ExecEndNode((PlanState *) lfirst(lc));
}

/*
 * A buffer that is still there holds the rows of the children, unless
 * their parameters changed: the scan returns them again, as a Material
 * does, and reads no child again. A squelch may have released the
 * buffer; then the scan rescans its children and fills a new one.
 */
static void
rescan_pass(CustomScanState *node)
{
	PassState  *state = (PassState *) node;
	ListCell   *lc;

	ExecScanReScan(&node->ss);
	if (state->buffer != NULL && node->ss.ps.chgParam == NULL)
	{
		tuplestore_rescan(state->buffer);
		return;
	}
	if (state->buffer != NULL)
	{
		tuplestore_end(state->buffer);
		state->buffer = NULL;
	}
	foreach(lc, state->children)
	{
		PlanState  *child = (PlanState *) lfirst(lc);

		if (node->ss.ps.chgParam != NULL)
			UpdateChangedParamSet(child, node->ss.ps.chgParam);
		if (child->chgParam == NULL)
			ExecReScan(child);
	}
	state->done = 0;
}

static void
shutdown_pass(CustomScanState *node)
{
	((PassState *) node)->shutdowns++;
}

/*
 * The parent reads no more rows: release the buffer, which a rescan fills
 * again, and squelch every child, even one the scan has not started, as a
 * provider must.
 */
static void
squelch_pass(CustomScanState *node)
{
	PassState  *state = (PassState *) node;
	ListCell   *lc;

	state->squelches++;
	if (state->buffer != NULL)
	{
		tuplestore_end(state->buffer);
		state->buffer = NULL;
		state->releases++;
	}
	foreach(lc, state->children)
		ExecSquelchNode((PlanState *) lfirst(lc));
}

/*
 * A scan that asked for memory of its own shows the share it got:
 * EXPLAIN shows the shares of the core's operators only under a resource
 * manager. EXPLAIN ANALYZE shows how many times the scan was squelched
 * and shut down, and a buffered one how many buffers it filled and
 * released, for a scan that runs where EXPLAIN is printed.
 */
static void
explain_pass(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

	if (cscan->flags & CUSTOMPATH_GP_MEMORY_INTENSIVE)
		ExplainPropertyInteger("Memory Share", "kB",
							   PlanStateOperatorMemKB(&node->ss.ps), es);
	if (es->analyze)
	{
		ExplainPropertyInteger("Squelches", NULL,
							   ((PassState *) node)->squelches, es);
		ExplainPropertyInteger("Shutdowns", NULL,
							   ((PassState *) node)->shutdowns, es);
		if (((PassState *) node)->buffered)
		{
			ExplainPropertyInteger("Buffers Filled", NULL,
								   ((PassState *) node)->builds, es);
			ExplainPropertyInteger("Buffers Released", NULL,
								   ((PassState *) node)->releases, es);
		}
	}
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("test_custom_scan.child_rows_only",
							 "The custom scan says it reads rows only from its children.",
							 NULL, &child_rows_only, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.projection",
							 "The custom scan can project.",
							 NULL, &projection, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.memory_intensive",
							 "The custom scan asks for memory of its own.",
							 NULL, &memory_intensive, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.memory_overlaps_children",
							 "The custom scan needs its memory at the same time as the operators under it.",
							 NULL, &memory_overlaps_children, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.last_branch_first",
							 "The custom scan reads the branches of a UNION ALL from the last to the first.",
							 NULL, &last_branch_first, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.strewn",
							 "The custom scan of a table says its rows are strewn over the segments.",
							 NULL, &strewn, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.outer_join_locus",
							 "The custom scan of a table says a NULL key may lie on any segment.",
							 NULL, &outer_join_locus, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.buffer",
							 "The custom scan reads the rows of its children into a buffer first.",
							 NULL, &buffer, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);

	plain_exec_methods = pass_exec_methods;
	plain_exec_methods.SquelchCustomScan = NULL;
	RegisterCustomScanMethods(&pass_scan_methods);

	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = wrap_rel;
}
