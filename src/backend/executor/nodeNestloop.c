/*-------------------------------------------------------------------------
 *
 * nodeNestloop.c
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

extern bool Test_print_prefetch_joinqual;

static void splitJoinQualExpr(NestLoopState *nlstate);
static void extractFuncExprArgs(FuncExprState *fstate, List **lclauses, List **rclauses);

/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecNestLoop_guts(NestLoopState *node)
{
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	List	   *joinqual;
	List	   *otherqual;
	ExprContext *econtext;
	ListCell   *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a join tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * MPP-4165: My fix for MPP-3300 was correct in that we avoided
	 * the *deadlock* but had very unexpected (and painful)
	 * performance characteristics: we basically de-pipeline and
	 * de-parallelize execution of any query which has motion below
	 * us.
	 *
	 * So now prefetch_inner is set (see createplan.c) if we have *any* motion
	 * below us. If we don't have any motion, it doesn't matter.
	 *
	 * See motion_sanity_walker() for details on how a deadlock may occur.
	 */
	if (node->prefetch_inner)
	{
		/*
		 * Prefetch inner is Greenplum specific behavior.
		 * However, inner plan may depend on outer plan as
		 * outerParams. If so, we have to fake those params
		 * to avoid null pointer reference issue. And because
		 * of the nestParams, those inner results prefetched
		 * will be discarded (following code will rescan inner,
		 * even if inner's top is material node because of chgParam
		 * it will be re-executed too) that it is safe to fake
		 * nestParams here. The target is to materialize motion scan.
		 */
		if (nl->nestParams)
		{
			EState	   *estate = node->js.ps.state;

			econtext->ecxt_outertuple = ExecInitNullTupleSlot(estate,
															  ExecGetResultType(outerPlan));
			fake_outer_params(&(node->js));
		}

		innerTupleSlot = ExecProcNode(innerPlan);
		node->reset_inner = true;
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			/*
			 * Finished one complete scan of the inner side. Mark it here
			 * so that we don't keep checking for inner nulls at subsequent
			 * iterations.
			 */
			node->nl_innerSideScanned = true;
		}

		if ((node->js.jointype == JOIN_LASJ_NOTIN) &&
				(!node->nl_innerSideScanned) &&
				(node->nl_InnerJoinKeys && isJoinExprNull(node->nl_InnerJoinKeys, econtext)))
		{
			/*
			 * If LASJ_NOTIN and a null was found on the inner side, all tuples
			 * We'll read no more from either inner or outer subtree. To keep our
			 * in outer sider will be treated as "not in" tuples in inner side.
			 */
			ENL1_printf("Found NULL tuple on the inner side, clean out");
			return NULL;
		}

		ExecReScan(innerPlan);
		ResetExprContext(econtext);

		node->prefetch_inner = false;
		node->reset_inner = false;
	}

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{
			ENL1_printf("getting new outer tuple");
			outerTupleSlot = ExecProcNode(outerPlan);

			/*
			 * if there are no more outer tuples, then the join is complete..
			 */
			if (TupIsNull(outerTupleSlot))
			{
				ENL1_printf("no outer tuple, ending join");
				return NULL;
			}

			ENL1_printf("saving new outer tuple information");
			econtext->ecxt_outertuple = outerTupleSlot;
			node->nl_NeedNewOuter = false;
			node->nl_MatchedOuter = false;

			/*
			 * fetch the values of any outer Vars that must be passed to the
			 * inner scan, and store them in the appropriate PARAM_EXEC slots.
			 */
			foreach(lc, nl->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
				int			paramno = nlp->paramno;
				ParamExecData *prm;

				prm = &(econtext->ecxt_param_exec_vals[paramno]);
				/* Param value should be an OUTER_VAR var */
				Assert(IsA(nlp->paramval, Var));
				Assert(nlp->paramval->varno == OUTER_VAR);
				Assert(nlp->paramval->varattno > 0);
				prm->value = slot_getattr(outerTupleSlot,
										  nlp->paramval->varattno,
										  &(prm->isnull));
				/* Flag parameter value as changed */
				innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
													 paramno);
			}

			/*
			 * now rescan the inner plan
			 */
			ENL1_printf("rescanning inner plan");
			if (node->require_inner_reset || node->reset_inner)
			{
				ExecReScan(innerPlan);
				node->reset_inner = false;
			}
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);

		node->reset_inner = true;
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			ENL1_printf("no inner tuple, need new outer tuple");

			node->nl_NeedNewOuter = true;
			/*
			 * Finished one complete scan of the inner side. Mark it here
			 * so that we don't keep checking for inner nulls at subsequent
			 * iterations.
			 */
			node->nl_innerSideScanned = true;

			if (!node->nl_MatchedOuter &&
				(node->js.jointype == JOIN_LEFT ||
				 node->js.jointype == JOIN_ANTI ||
				 node->js.jointype == JOIN_LASJ_NOTIN))
			{
				/*
				 * We are doing an outer join and there were no join matches
				 * for this outer tuple.  Generate a fake join tuple with
				 * nulls for the inner tuple, and return it if it passes the
				 * non-join quals.
				 */
				econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

				ENL1_printf("testing qualification for outer-join tuple");

				if (otherqual == NIL || ExecQual(otherqual, econtext, false))
				{
					/*
					 * qualification was satisfied so we project and return
					 * the slot containing the result tuple using
					 * ExecProject().
					 */
					ENL1_printf("qualification succeeded, projecting tuple");

					return ExecProject(node->js.ps.ps_ProjInfo, NULL);
				}
				else
					InstrCountFiltered2(node, 1);
			}

			/*
			 * Otherwise just return to top of loop for a new outer tuple.
			 */
			continue;
		}


		if ((node->js.jointype == JOIN_LASJ_NOTIN) &&
				(!node->nl_innerSideScanned) &&
				(node->nl_InnerJoinKeys && isJoinExprNull(node->nl_InnerJoinKeys, econtext)))
		{
			/*
			 * If LASJ_NOTIN and a null was found on the inner side, all tuples
			 * We'll read no more from either inner or outer subtree. To keep our
			 * in outer sider will be treated as "not in" tuples in inner side.
			 */
			ENL1_printf("Found NULL tuple on the inner side, clean out");
			return NULL;
		}

		/*
		 * at this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext, node->nl_qualResultForNull))
		{
			node->nl_MatchedOuter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_LASJ_NOTIN || node->js.jointype == JOIN_ANTI)
			{
				node->nl_NeedNewOuter = true;
				continue;		/* return to top of loop */
			}

			/*
			 * In a semijoin, we'll consider returning the first match, but
			 * after that we're done with this outer tuple.
			 */
			if (node->js.jointype == JOIN_SEMI)
				node->nl_NeedNewOuter = true;

			if (otherqual == NIL || ExecQual(otherqual, econtext, false))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				TupleTableSlot *result;

				ENL1_printf("qualification succeeded, projecting tuple");

				result = ExecProject(node->js.ps.ps_ProjInfo, NULL);

				return result;
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

TupleTableSlot *
ExecNestLoop(NestLoopState *node)
{
	TupleTableSlot *result;

	result = ExecNestLoop_guts(node);

	if (TupIsNull(result))
	{
		/*
		 * CDB: We'll read no more from inner subtree. To keep our
		 * sibling QEs from being starved, tell source QEs not to
		 * clog up the pipeline with our never-to-be-consumed
		 * data.
		 */
		ExecSquelchNode((PlanState *) node);
	}

	return result;
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
	NestLoopState *nlstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			   "initializing node");

	/*
	 * create state structure
	 */
	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan *) node;
	nlstate->js.ps.state = estate;

	nlstate->shared_outer = node->shared_outer;

	nlstate->prefetch_inner = node->join.prefetch_inner;

	/*
	 * Greeplum specific:
	 * prefetch joinqual and prefetch qual are old methods
	 * to get rid of motion deadlock. Motion nodes in joinqual
	 * or planqual are in SubPlan expressions. Thus the motion
	 * can also appear in any TargetList which means old ways
	 * do not consider all cases and motion dealocks are not
	 * only limited JOIN. For 6X Stable version, we have to make
	 * sure for ABI compatible thus we have to keep these fields
	 * introduced by previous fix, just set them to false. This
	 * logic also exists in ExecInitHashJoin & ExecInitMergeJoin.
	 */
	nlstate->prefetch_joinqual = false;
	nlstate->prefetch_qual = false;

	/*CDB-OLAP*/
	nlstate->reset_inner = false;
	nlstate->require_inner_reset = !node->singleton_outer;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlstate->js.ps);

	/*
	 * initialize child expressions
	 */
	nlstate->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) nlstate);
	nlstate->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) nlstate);

	/*
	 * initialize child nodes
	 *
	 * If we have no parameters to pass into the inner rel from the outer,
	 * tell the inner child that cheap rescans would be good.  If we do have
	 * such parameters, then there is no point in REWIND support at all in the
	 * inner child, because it will always be rescanned with fresh parameter
	 * values.
	 */

	/*
	 * XXX ftian: Because share input need to make the whole thing into a tree,
	 * we can put the underlying share only under one shareinputscan.  During execution,
	 * we need the shareinput node that has underlying subtree be inited/executed first.
	 * This means, 
	 * 	1. Init and first ExecProcNode call must be in the same order
	 *	2. Init order above is the same as the tree walking order in cdbmutate.c
	 * For nest loop join, it is more strange than others.  Depends on prefetch_inner,
	 * the execution order may change.  Handle this correctly here.
	 * 
	 * Until we find a better way to handle the dependency of ShareInputScan on 
	 * execution order, this is pretty much what we have to deal with.
	 */
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	if (nlstate->prefetch_inner)
	{
		innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);
		if (!node->shared_outer)
			outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	}
	else
	{
		if (!node->shared_outer)
			outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
		innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);
	}

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &nlstate->js.ps);

	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
		case JOIN_LASJ_NOTIN:
			nlstate->nl_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(nlstate)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&nlstate->js.ps);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

    if (node->join.jointype == JOIN_LASJ_NOTIN)
    {
    	splitJoinQualExpr(nlstate);
    	/*
    	 * For LASJ_NOTIN, when we evaluate the join condition, we want to
    	 * return true when one of the conditions is NULL, so we exclude
    	 * that tuple from the output.
    	 */
		nlstate->nl_qualResultForNull = true;
    }
    else
    {
        nlstate->nl_qualResultForNull = false;
    }

	NL1_printf("ExecInitNestLoop: %s\n",
			   "node initialized");

	return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndNestLoop(NestLoopState *node)
{
	NL1_printf("ExecEndNestLoop: %s\n",
			   "ending node processing");

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * close down subplans
	 */
	if (!node->shared_outer)
		ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n",
			   "node processing ended");

	EndPlanStateGpmonPkt(&node->js.ps);
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoop(NestLoopState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	Insist(outerPlan);

	/*
	 * If outerPlan->chgParam is not null then plan will be automatically
	 * re-scanned by first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	/*
	 * innerPlan is re-scanned for each new outer tuple and MUST NOT be
	 * re-scanned from here or you'll get troubles from inner index scans when
	 * outer Vars are used as run-time keys...
	 */

	node->nl_NeedNewOuter = true;
	node->nl_MatchedOuter = false;
	node->nl_innerSideScanned = false;
}

/* ----------------------------------------------------------------
 * splitJoinQualExpr
 *
 * Deconstruct the join clauses into outer and inner argument values, so
 * that we can evaluate those subexpressions separately. Note: for constant
 * expression we don't need to split (MPP-21294). However, if constant expressions
 * have peer splittable expressions we *do* split those.
 *
 * This is used for NOTIN joins, as we need to look for NULLs on both
 * inner and outer side.
 * ----------------------------------------------------------------
 */
static void
splitJoinQualExpr(NestLoopState *nlstate)
{
	List *lclauses = NIL;
	List *rclauses = NIL;
	ListCell *lc = NULL;

	foreach(lc, nlstate->js.joinqual)
	{
		GenericExprState *exprstate = (GenericExprState *) lfirst(lc);
		switch (exprstate->xprstate.type)
		{
		case T_FuncExprState:
			extractFuncExprArgs((FuncExprState *) exprstate, &lclauses, &rclauses);
			break;
		case T_BoolExprState:
		{
			BoolExprState *bstate = (BoolExprState *) exprstate;
			ListCell *argslc = NULL;
			foreach(argslc,bstate->args)
			{
				FuncExprState *fstate = (FuncExprState *) lfirst(argslc);
				Expr          *expr   = ((ExprState *) fstate)->expr;

				/*
				 * Greenplum will pull up not-in sublink to a specific join LASJ,
				 * this kind of join's joinqual might contain a NULL const here,
				 * for such case we do not need to split it. A case that can
				 * reach here is:
				 *
				 *   create table t1(a int not null, b int not null);
				 *   create table t2(a int not null, b int not null);
				 *   explain  select 1 from t1 where (NULL, b) not in (select a, b from t2);
				 *
				 * The above SQL in Greenplum will be turned in a join whose qual contains
				 * a bool expr (NULL = t2.a) and (t1.b = t2.b), this piece of expr will be
				 * evaluated to (t1.b = t2.b) and NULL by the following code path:
				 *   subquery_planner
				 *     -> preprocess_qual_conditions(root, (Node *) parse->jointree)
				 *     -> preprocess_expression
				 *     -> eval_const_expressions
				 *     -> eval_const_expressions_mutator
				 *
				 * That is why here we have to consider const case, and only null const
				 * (other const cases, true or false will be simplified during the above
				 * code path).
				 *
				 * We do nothing here for NULL const.
				 *
				 * See Issue: https://github.com/greenplum-db/gpdb/issues/13212 for details.
				 */
				if (IsA(expr, Const) && ((Const *) expr)->constisnull)
					continue;
				else if (!IsA(fstate, FuncExprState))
				{
					elog(ERROR,
						 "Expect FuncExprState or NULL const here, but found tag %d",
						 nodeTag(fstate));
				}

				extractFuncExprArgs(fstate, &lclauses, &rclauses);
			}
			break;
		}
		case T_ExprState:
			/* For constant and distinct expression we don't need to split */
			if ((exprstate->xprstate.expr->type == T_Const) ||
				(exprstate->xprstate.expr->type == T_DistinctExpr))
			{
				/*
				 * Distinct and constant expressions do not need to be
				 * splitted into left and right as they don't need to be
				 * considered for NULL value special cases
				 */
				continue;
			}

			elog(ERROR, "unexpected expression type in NestLoopJoin qual");

			break; /* Unreachable */
		default:
			elog(ERROR, "unexpected expression type in NestLoopJoin qual");
		}
	}
	Assert(NIL == nlstate->nl_InnerJoinKeys && NIL == nlstate->nl_OuterJoinKeys);
	nlstate->nl_InnerJoinKeys = rclauses;
	nlstate->nl_OuterJoinKeys = lclauses;
}


/* ----------------------------------------------------------------
 * extractFuncExprArgs
 *
 * Extract the arguments of a FuncExpr and append them into two
 * given lists:
 *   - lclauses for the left side of the expression,
 *   - rclauses for the right side
 *
 * This function is only used for LASJ. Once we find a NULL from inner side, we
 * can skip the join and just return an empty set as result. This is only true
 * if the equality operator is strict, that is, if a tuple from inner side is
 * NULL then the equality operator returns NULL.
 *
 * If the number of arguments is not two, we just return leaving lclauses and
 * rclauses remaining NULL. In this case, the LASJ join would be actually
 * performed.
 * ----------------------------------------------------------------
 */
static void
extractFuncExprArgs(FuncExprState *fstate, List **lclauses, List **rclauses)
{
	Node *clause;

	if (list_length(fstate->args) != 2)
		return;

	/* Check for strictness of the equality operator */
	clause = (Node *)fstate->xprstate.expr;
	if ((is_opclause(clause) && op_strict(((OpExpr *) clause)->opno)) ||
			(is_funcclause(clause) && func_strict(((FuncExpr *) clause)->funcid)))
	{
		*lclauses = lappend(*lclauses, linitial(fstate->args));
		*rclauses = lappend(*rclauses, lsecond(fstate->args));
	}
}
