/*-------------------------------------------------------------------------
 *
 * planshare.c
 *	  Plan shared plan
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/planshare.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/subselect.h"
#include "optimizer/planshare.h"

static ShareInputScan *
make_shareinputscan(PlannerInfo *root, Plan *inputplan)
{
	ShareInputScan *sisc;
	Path		sipath;

	sisc = makeNode(ShareInputScan);

	/*
	 * A ShareInputScan is a non-projecting pass-through of its input plan, so
	 * its targetlist must describe the input's output tuples.  Normally we can
	 * just copy inputplan->targetlist, but a ModifyTable keeps the output of a
	 * writable/DML CTE (its RETURNING list) in returningLists and leaves
	 * plan.targetlist NIL -- the executor derives a ModifyTable's result
	 * descriptor from RETURNING in ExecInitModifyTable().  Copying that empty
	 * targetlist would make the ShareInputScan advertise zero output columns
	 * (natts == 0), so a consumer of the CTE that references its columns reads
	 * past the end of the tuple descriptor ("attribute number N exceeds number
	 * of columns 0", or a bogus attribute under JIT tuple deforming).  Build a
	 * pass-through targetlist matching the RETURNING output instead.  setrefs.c
	 * (set_dummy_tlist_references) later rewrites the exprs into positional
	 * OUTER_VAR references, so only the column count and types matter here.
	 */
	if (inputplan->targetlist == NIL &&
		IsA(inputplan, ModifyTable) &&
		((ModifyTable *) inputplan)->returningLists != NIL)
	{
		List	   *returningList = (List *)
			linitial(((ModifyTable *) inputplan)->returningLists);
		List	   *newtlist = NIL;
		ListCell   *lc;

		foreach(lc, returningList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Var		   *var = makeVarFromTargetEntry(OUTER_VAR, tle);

			newtlist = lappend(newtlist,
							   makeTargetEntry((Expr *) var,
											   tle->resno,
											   tle->resname,
											   tle->resjunk));
		}
		sisc->scan.plan.targetlist = newtlist;
	}
	else
		sisc->scan.plan.targetlist = copyObject(inputplan->targetlist);
	sisc->scan.plan.lefttree = inputplan;
	sisc->scan.plan.flow = copyObject(inputplan->flow);

	sisc->cross_slice = false;
	sisc->producer_slice_id = -1;
	sisc->this_slice_id = -1;
	sisc->nconsumers = 0;
	sisc->discard_output = false;

	sisc->scan.plan.qual = NIL;
	sisc->scan.plan.righttree = NULL;

	cost_shareinputscan(&sipath, root, inputplan->total_cost, inputplan->plan_rows, inputplan->plan_width);

	sisc->scan.plan.startup_cost = sipath.startup_cost;
	sisc->scan.plan.total_cost = sipath.total_cost; 
	sisc->scan.plan.plan_rows = inputplan->plan_rows;
	sisc->scan.plan.plan_width = inputplan->plan_width;

	return sisc;
}

/*
 * Prepare a subplan for sharing. After this, you can call
 * share_prepared_plan() as many times as you want to share this plan.
 *
 * This doesn't do much at the moment. One little optimization is that
 * if the subplan is a ShareInputScan, we make the new ShareInputScans
 * be siblings of that, rather than creating a ShareInputScan on
 * top of a ShareInputScan.
 */
Plan *
prepare_plan_for_sharing(PlannerInfo *root, Plan *common)
{
	Plan *shared = common;

	if (IsA(common, ShareInputScan))
	{
		shared = common->lefttree;
	}

	return shared;
}

/*
 * Create a ShareInputScan to scan the given subplan. The subplan
 * must've been prepared for sharing by calling
 * prepare_plan_for_sharing().
 */
Plan *
share_prepared_plan(PlannerInfo *root, Plan *common)
{
	return (Plan *) make_shareinputscan(root, common);
}
