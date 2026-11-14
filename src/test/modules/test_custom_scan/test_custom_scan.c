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
 * A table aliased "cscan" is wrapped too, as a provider's scan of the
 * table: the scan reads the table (scanrelid), and keeps its one child,
 * the table's own path, in lefttree rather than custom_plans, as some
 * providers do. A scan of a table that returns no columns is left alone.
 * With test_custom_scan.strewn, the scan of a table says its rows are
 * strewn over the segments, as if the provider read a copy of the table
 * laid out otherwise; it still reads the table.
 *
 * DML and row-locking queries are left alone: EvalPlanQual rechecks their
 * rows through the scan, which this provider does not implement.
 *
 * The scan keeps a value node in custom_private, as providers commonly
 * do. Three settings choose its flags: test_custom_scan.last_child_first
 * (CUSTOMPATH_GP_LAST_CHILD_FIRST), test_custom_scan.projection
 * (CUSTOMPATH_SUPPORT_PROJECTION) and test_custom_scan.memory_intensive
 * (CUSTOMPATH_GP_MEMORY_INTENSIVE). With test_custom_scan.hide_first_child,
 * the first branch of a UNION ALL is left out of the children, as if the
 * provider read it itself; the scan then returns the other branches only,
 * so a test only plans such a query.
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
#include "utils/guc.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* The alias of the subqueries the provider wraps. */
#define WRAPPED_ALIAS "cscan"

#define PASS_NAME "TestPassThrough"

static bool last_child_first = false;
static bool projection = false;
static bool memory_intensive = false;
static bool hide_first_child = false;
static bool strewn = false;

typedef struct PassState
{
	CustomScanState css;
	List	   *children;		/* custom_ps, or the child in lefttree */
	int			done;			/* the children read to their end */
	int			shutdowns;		/* calls of the shutdown callback */
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
};

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
		  (rte->rtekind == RTE_RELATION && !rte->inh &&
		   rte->relkind == RELKIND_RELATION)) ||
		strcmp(rte->eref->aliasname, WRAPPED_ALIAS) != 0)
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

	/* The branches of a UNION ALL become the children. */
	if (IsA(best, AppendPath))
		children = list_copy(((AppendPath *) best)->subpaths);
	else
		children = list_make1(best);
	if (hide_first_child && list_length(children) > 1)
		children = list_delete_first(children);
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
	cpath->path.motionHazard = best->motionHazard;
	cpath->path.rescannable = best->rescannable;
	cpath->path.sameslice_relids = best->sameslice_relids;
	cpath->flags = 0;
	if (last_child_first)
		cpath->flags |= CUSTOMPATH_GP_LAST_CHILD_FIRST;
	if (projection)
		cpath->flags |= CUSTOMPATH_SUPPORT_PROJECTION;
	if (memory_intensive)
		cpath->flags |= CUSTOMPATH_GP_MEMORY_INTENSIVE;
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

	cscan->scan.plan.targetlist = tlist;

	/*
	 * The children check the subquery's restrictions themselves. The scan
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
	/* The number of children in custom_plans, a value node. */
	cscan->custom_private = list_make1(makeInteger(list_length(custom_plans)));
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

	state->css.methods = &pass_exec_methods;
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
}

/* The next row of the children, or an empty slot after the last. */
static TupleTableSlot *
next_row(ScanState *ss)
{
	PassState  *state = (PassState *) ss;
	CustomScan *cscan = (CustomScan *) ss->ps.plan;
	int			nchildren = list_length(state->children);

	while (state->done < nchildren)
	{
		int			child = state->done;
		TupleTableSlot *slot;

		if (cscan->flags & CUSTOMPATH_GP_LAST_CHILD_FIRST)
			child = nchildren - 1 - state->done;
		slot = ExecProcNode((PlanState *) list_nth(state->children, child));
		if (!TupIsNull(slot))
			return ExecCopySlot(ss->ss_ScanTupleSlot, slot);
		state->done++;
	}
	return ExecClearTuple(ss->ss_ScanTupleSlot);
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
	ListCell   *lc;

	foreach(lc, ((PassState *) node)->children)
		ExecEndNode((PlanState *) lfirst(lc));
}

static void
rescan_pass(CustomScanState *node)
{
	PassState  *state = (PassState *) node;
	ListCell   *lc;

	foreach(lc, state->children)
	{
		PlanState  *child = (PlanState *) lfirst(lc);

		if (node->ss.ps.chgParam != NULL)
			UpdateChangedParamSet(child, node->ss.ps.chgParam);
		if (child->chgParam == NULL)
			ExecReScan(child);
	}
	state->done = 0;
	ExecScanReScan(&node->ss);
}

static void
shutdown_pass(CustomScanState *node)
{
	((PassState *) node)->shutdowns++;
}

/*
 * A scan that asked for memory of its own shows the share it got:
 * EXPLAIN shows the shares of the core's operators only under a resource
 * manager. EXPLAIN ANALYZE shows how many times the scan was shut down,
 * for a scan that runs where EXPLAIN is printed.
 */
static void
explain_pass(CustomScanState *node, List *ancestors, ExplainState *es)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

	if (cscan->flags & CUSTOMPATH_GP_MEMORY_INTENSIVE)
		ExplainPropertyInteger("Memory Share", "kB",
							   PlanStateOperatorMemKB(&node->ss.ps), es);
	if (es->analyze)
		ExplainPropertyInteger("Shutdowns", NULL,
							   ((PassState *) node)->shutdowns, es);
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("test_custom_scan.last_child_first",
							 "The custom scan runs its children from the last to the first.",
							 NULL, &last_child_first, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.projection",
							 "The custom scan can project.",
							 NULL, &projection, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.memory_intensive",
							 "The custom scan asks for memory of its own.",
							 NULL, &memory_intensive, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.hide_first_child",
							 "The first branch of a UNION ALL is not a child of the custom scan.",
							 NULL, &hide_first_child, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("test_custom_scan.strewn",
							 "The custom scan of a table says its rows are strewn over the segments.",
							 NULL, &strewn, false, PGC_USERSET, 0,
							 NULL, NULL, NULL);

	RegisterCustomScanMethods(&pass_scan_methods);

	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = wrap_rel;
}
