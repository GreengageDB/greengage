/*-------------------------------------------------------------------------
 *
 * subselect.c
 *	  Planning routines for subselects.
 *
 * This module deals with SubLinks and CTEs, but not subquery RTEs (i.e.,
 * not sub-SELECT-in-FROM cases).
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/plan/subselect.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/gp_distribution_policy.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/paramassign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/subselect.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "rewrite/rewriteManip.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "cdb/cdbllize.h"
#include "cdb/cdbmutate.h"
#include "cdb/cdbpathtoplan.h"
#include "cdb/cdbsubselect.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbpath.h"

/* source-code-compatibility hacks for pull_varnos() API change */
#define pull_varnos(a,b) pull_varnos_new(a,b)

typedef struct convert_testexpr_context
{
	PlannerInfo *root;
	List	   *subst_nodes;	/* Nodes to substitute for Params */
} convert_testexpr_context;

typedef struct process_sublinks_context
{
	PlannerInfo *root;
	bool		isTopQual;
} process_sublinks_context;

typedef struct finalize_primnode_context
{
	PlannerInfo *root;
	Bitmapset  *paramids;		/* Non-local PARAM_EXEC paramids found */
} finalize_primnode_context;

typedef struct inline_cte_walker_context
{
	const char *ctename;		/* name and relative level of target CTE */
	int			levelsup;
	Query	   *ctequery;		/* query to substitute */
} inline_cte_walker_context;


static Node *build_subplan(PlannerInfo *root, Plan *plan, PlannerInfo *subroot,
						   List *plan_params,
						   SubLinkType subLinkType, int subLinkId,
						   Node *testexpr, List *testexpr_paramids,
						   bool unknownEqFalse);
static List *generate_subquery_params(PlannerInfo *root, List *tlist,
									  List **paramIds);
static Node *convert_testexpr_mutator(Node *node,
									  convert_testexpr_context *context);
static bool subplan_is_hashable(PlannerInfo *root, Plan *plan);
static bool subpath_is_hashable(PlannerInfo *root, Path *path);
static bool test_opexpr_is_hashable(OpExpr *testexpr, List *param_ids);
static bool hash_ok_operator(OpExpr *expr);
#if 0
/*
 * The following several functions are used by SS_process_ctes.
 * But SS_process_ctes is commentted of because gpdb does not
 * use it.
 */
static bool contain_dml(Node *node);
static bool contain_dml_walker(Node *node, void *context);
static bool contain_outer_selfref(Node *node);
static bool contain_outer_selfref_walker(Node *node, Index *depth);
static void inline_cte(PlannerInfo *root, CommonTableExpr *cte);
static bool inline_cte_walker(Node *node, inline_cte_walker_context *context);
#endif
static bool simplify_EXISTS_query(PlannerInfo *root, Query *query);
static Query *convert_EXISTS_to_ANY(PlannerInfo *root, Query *subselect,
									Node **testexpr, List **paramIds);
static Node *replace_correlation_vars_mutator(Node *node, PlannerInfo *root);
static Node *process_sublinks_mutator(Node *node,
									  process_sublinks_context *context);
static Bitmapset *finalize_plan(PlannerInfo *root,
								Plan *plan,
								int gather_param,
								Bitmapset *valid_params,
								Bitmapset *scan_params);
static bool finalize_primnode(Node *node, finalize_primnode_context *context);
static bool finalize_agg_primnode(Node *node, finalize_primnode_context *context);

extern	double global_work_mem(PlannerInfo *root);
static bool contain_outer_selfref_walker(Node *node, Index *depth);

static bool splan_is_initplan(List *plan_params, SubLinkType subLinkType);

/*
 * Get the datatype/typmod/collation of the first column of the plan's output.
 *
 * This information is stored for ARRAY_SUBLINK execution and for
 * exprType()/exprTypmod()/exprCollation(), which have no way to get at the
 * plan associated with a SubPlan node.  We really only need the info for
 * EXPR_SUBLINK and ARRAY_SUBLINK subplans, but for consistency we save it
 * always.
 */
static void
get_first_col_type(Plan *plan, Oid *coltype, int32 *coltypmod,
				   Oid *colcollation)
{
	/* In cases such as EXISTS, tlist might be empty; arbitrarily use VOID */
	if (plan->targetlist)
	{
		TargetEntry *tent = linitial_node(TargetEntry, plan->targetlist);

		if (!tent->resjunk)
		{
			*coltype = exprType((Node *) tent->expr);
			*coltypmod = exprTypmod((Node *) tent->expr);
			*colcollation = exprCollation((Node *) tent->expr);
			return;
		}
	}
	*coltype = VOIDOID;
	*coltypmod = -1;
	*colcollation = InvalidOid;
}

/**
 * Returns true if query refers to a distributed table.
 */
bool QueryHasDistributedRelation(Query *q, bool recursive)
{
	ListCell   *rt = NULL;

	foreach(rt, q->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

		if (rte->rtekind == RTE_SUBQUERY
				&& recursive
				&& QueryHasDistributedRelation(rte->subquery, true))
			return true;

		if (rte->relid != InvalidOid
				&& rte->rtekind == RTE_RELATION)
		{
			GpPolicy *policy = GpPolicyFetch(rte->relid);
			if (GpPolicyIsPartitioned(policy))
			{
				pfree(policy);
				return true;
			}
			pfree(policy);
		}
	}
	return false;
}

typedef struct CorrelatedVarWalkerContext
{
	int maxLevelsUp;
} CorrelatedVarWalkerContext;

/**
 *  Walker finds the deepest correlation nesting i.e. maximum levelsup among all
 *  vars in subquery.
 */
static bool
CorrelatedVarWalker(Node *node, CorrelatedVarWalkerContext *ctx)
{
	Assert(ctx);

	if (node == NULL)
	{
		return false;
	}
	else if (IsA(node, Var))
	{
		Var * v = (Var *) node;
		if (v->varlevelsup > ctx->maxLevelsUp)
		{
			ctx->maxLevelsUp = v->varlevelsup;
		}
		return false;
	}
	else if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, CorrelatedVarWalker, ctx, 0 /* flags */);
	}

	return expression_tree_walker(node, CorrelatedVarWalker, ctx);
}

/**
 * Returns true if subquery is correlated
 */
bool
IsSubqueryCorrelated(Query *sq)
{
	Assert(sq);
	CorrelatedVarWalkerContext ctx;
	ctx.maxLevelsUp = 0;
	CorrelatedVarWalker((Node *) sq, &ctx);
	return (ctx.maxLevelsUp > 0);
}

/*
 * Check multi-level correlated subquery in Postgres legacy planner
 *
 * We could support one-level correlated subquery by adding
 * broadcast + result(param filter). For multi-level scenario
 * we should prevent planner from adding another motion above
 * result node which is from one-level correlated subquery.
 *
 * In this function, firstly we find the top root which refer
 * to Param, then check table distribution below current root
 * Not support if any distributed table exist.
 */
void
check_multi_subquery_correlated(PlannerInfo *root, Var *var)
{
	int levelsup;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;
	if (var->varlevelsup <= 1)
		return;

	for (levelsup = var->varlevelsup; levelsup > 0; levelsup--)
	{
		PlannerInfo *parent_root = root->parent_root;

		if (parent_root == NULL)
			elog(ERROR, "not found parent root when checking skip-level correlations");

		/*
		 * Only check sublink not include subquery
		 */
		if(parent_root->parse->hasSubLinks &&
			QueryHasDistributedRelation(root->parse, parent_root->is_correlated_subplan))
		{
			ereport(ERROR,
					errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 		errmsg("correlated subquery with skip-level correlations is not supported"));
		}

		root = root->parent_root;
	}

	return;
}

/*
 * Convert a SubLink (as created by the parser) into a SubPlan.
 *
 * We are given the SubLink's contained query, type, ID, and testexpr.  We are
 * also told if this expression appears at top level of a WHERE/HAVING qual.
 *
 * Note: we assume that the testexpr has been AND/OR flattened (actually,
 * it's been through eval_const_expressions), but not converted to
 * implicit-AND form; and any SubLinks in it should already have been
 * converted to SubPlans.  The subquery is as yet untouched, however.
 *
 * The result is whatever we need to substitute in place of the SubLink node
 * in the executable expression.  If we're going to do the subplan as a
 * regular subplan, this will be the constructed SubPlan node.  If we're going
 * to do the subplan as an InitPlan, the SubPlan node instead goes into
 * root->init_plans, and what we return here is an expression tree
 * representing the InitPlan's result: usually just a Param node representing
 * a single scalar result, but possibly a row comparison tree containing
 * multiple Param nodes, or for a MULTIEXPR subquery a simple NULL constant
 * (since the real output Params are elsewhere in the tree, and the MULTIEXPR
 * subquery itself is in a resjunk tlist entry whose value is uninteresting).
 */
static Node *
make_subplan(PlannerInfo *root, Query *orig_subquery,
			 SubLinkType subLinkType, int subLinkId,
			 Node *testexpr, bool isTopQual)
{
	Query	   *subquery;
	bool		simple_exists = false;
	double		tuple_fraction = 1.0;
	PlannerInfo *subroot;
	RelOptInfo *final_rel;
	Path	   *best_path;
	Plan	   *plan;
	List	   *plan_params;
	Node	   *result;

	/*
	 * Copy the source Query node.  This is a quick and dirty kluge to resolve
	 * the fact that the parser can generate trees with multiple links to the
	 * same sub-Query node, but the planner wants to scribble on the Query.
	 * Try to clean this up when we do querytree redesign...
	 */
	subquery = copyObject(orig_subquery);

	/*
	 * If it's an EXISTS subplan, we might be able to simplify it.
	 */
	if (subLinkType == EXISTS_SUBLINK)
		simple_exists = simplify_EXISTS_query(root, subquery);

	/*
	 * For an EXISTS subplan, tell lower-level planner to expect that only the
	 * first tuple will be retrieved.  For ALL and ANY subplans, we will be
	 * able to stop evaluating if the test condition fails or matches, so very
	 * often not all the tuples will be retrieved; for lack of a better idea,
	 * specify 50% retrieval.  For EXPR, MULTIEXPR, and ROWCOMPARE subplans,
	 * use default behavior (we're only expecting one row out, anyway).
	 *
	 * NOTE: if you change these numbers, also change cost_subplan() in
	 * path/costsize.c.
	 *
	 * XXX If an ANY subplan is uncorrelated, build_subplan may decide to hash
	 * its output.  In that case it would've been better to specify full
	 * retrieval.  At present, however, we can only check hashability after
	 * we've made the subplan :-(.  (Determining whether it'll fit in work_mem
	 * is the really hard part.)  Therefore, we don't want to be too
	 * optimistic about the percentage of tuples retrieved, for fear of
	 * selecting a plan that's bad for the materialization case.
	 */
	if (subLinkType == EXISTS_SUBLINK)
		tuple_fraction = 1.0;	/* just like a LIMIT 1 */
	else if (subLinkType == ALL_SUBLINK ||
			 subLinkType == ANY_SUBLINK)
		tuple_fraction = 0.5;	/* 50% */
	else
		tuple_fraction = 0.0;	/* default behavior */

	/* plan_params should not be in use in current query level */
	Assert(root->plan_params == NIL);

	PlannerConfig *config = CopyPlannerConfig(root->config);

	if (Gp_role == GP_ROLE_DISPATCH)
		config->is_under_subplan = true;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/*
		 * Disable CTE sharing in initplan.
		 *
		 * Such subLinkType below could become initplan,
		 * so we shouldn't apply cte sharing scan inside them
		 * and then back to normal scan.
		 */
		config->gp_cte_sharing = config->gp_cte_sharing ?
				!(subLinkType == ROWCOMPARE_SUBLINK ||
				 subLinkType == ARRAY_SUBLINK ||
				 subLinkType == EXPR_SUBLINK ||
				 subLinkType == MULTIEXPR_SUBLINK ||
				 subLinkType == EXISTS_SUBLINK) : config->gp_cte_sharing;
	}
	/*
	 * Strictly speaking, the order of rows in a subquery doesn't matter.
	 * Consider e.g. "WHERE IN (SELECT ...)". But in case of
	 * "ARRAY(SELECT foo ORDER BY bar)", we'd like to honor the ORDER BY,
	 * and construct the array in that order.
	 */
	if (subLinkType == ARRAY_SUBLINK)
		config->honor_order_by = true;
	else
		config->honor_order_by = false;

	/*
	 * Greenplum specific behavior:
	 * config->may_rescan is used to guide if
	 * we should add materialize path over motion
	 * in the left tree of a join.
	 */
	config->may_rescan = true;

	/* Generate Paths for the subquery */
	subroot = subquery_planner(root->glob, subquery,
							   root,
							   false,
							   tuple_fraction,
							   config);

	/* Isolate the params needed by this specific subplan */
	plan_params = root->plan_params;
	root->plan_params = NIL;

	/*
	 * Select best Path and turn it into a Plan.  At least for now, there
	 * seems no reason to postpone doing that.
	 */
	final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
	best_path = get_cheapest_fractional_path(final_rel, tuple_fraction);

	/*
	 * Greenplum specific behavior:
	 * Here we only need to handle general locus path,
	 * segmentgeneral is correct because of later processing.
	 * If we find that it is a general locus path that
	 * contains volatile target list or havingQual, we should
	 * turn it into singleQE.
	 */
	if (CdbPathLocus_IsGeneral(best_path->locus) &&
		(contain_volatile_functions((Node *) subroot->parse->havingQual) ||
		 contain_volatile_functions((Node *) best_path->pathtarget->exprs)))
		CdbPathLocus_MakeSingleQE(&(best_path->locus), getgpsegmentCount());

	best_path = cdbllize_adjust_init_plan_path(root, best_path);

	subroot->curSlice = palloc0(sizeof(PlanSlice));
	subroot->curSlice->gangType = GANGTYPE_UNALLOCATED;

	if (splan_is_initplan(plan_params, subLinkType))
		unset_allow_append_initplan_for_function_scan();

	plan = create_plan(subroot, best_path, subroot->curSlice);
	/* Decorate the top node of the plan with a Flow node. */
	plan->flow = cdbpathtoplan_create_flow(subroot, best_path->locus);

	set_allow_append_initplan_for_function_scan();
	Assert(get_allow_append_initplan_for_function_scan() == true);

	/* And convert to SubPlan or InitPlan format. */
	result = build_subplan(root, plan, subroot, plan_params,
						   subLinkType, subLinkId,
						   testexpr, NIL, isTopQual);

	/*
	 * If it's a correlated EXISTS with an unimportant targetlist, we might be
	 * able to transform it to the equivalent of an IN and then implement it
	 * by hashing.  We don't have enough information yet to tell which way is
	 * likely to be better (it depends on the expected number of executions of
	 * the EXISTS qual, and we are much too early in planning the outer query
	 * to be able to guess that).  So we generate both plans, if possible, and
	 * leave it to setrefs.c to decide which to use.
	 */
	if (simple_exists && IsA(result, SubPlan))
	{
		Node	   *newtestexpr;
		List	   *paramIds;

		/* Make a second copy of the original subquery */
		subquery = copyObject(orig_subquery);
		/* and re-simplify */
		simple_exists = simplify_EXISTS_query(root, subquery);
		Assert(simple_exists);
		/* See if it can be converted to an ANY query */
		subquery = convert_EXISTS_to_ANY(root, subquery,
										 &newtestexpr, &paramIds);
		if (subquery)
		{
			/* Generate Paths for the ANY subquery; we'll need all rows */
			subroot = subquery_planner(root->glob, subquery,
									   root,
									   false, 0.0, config);

			/* Isolate the params needed by this specific subplan */
			plan_params = root->plan_params;
			root->plan_params = NIL;

			/* Select best Path */
			final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
			best_path = final_rel->cheapest_total_path;

			/* Now we can check if it'll fit in work_mem */
			if (subpath_is_hashable(root, best_path))
			{
				SubPlan    *hashplan;
				AlternativeSubPlan *asplan;

				subroot->curSlice = palloc0(sizeof(PlanSlice));
				subroot->curSlice->gangType = GANGTYPE_UNALLOCATED;

				/* OK, finish planning the ANY subquery */
				plan = create_plan(subroot, best_path, subroot->curSlice);
				/* Decorate the top node of the plan with a Flow node. */
				plan->flow = cdbpathtoplan_create_flow(subroot, best_path->locus);

				/* ... and convert to SubPlan format */
				hashplan = castNode(SubPlan,
									build_subplan(root, plan, subroot,
												  plan_params,
												  ANY_SUBLINK, 0,
												  newtestexpr,
												  paramIds,
												  true));
				/* Check we got what we expected */
				Assert(hashplan->parParam == NIL);
				Assert(hashplan->useHashTable);

				/* Leave it to setrefs.c to decide which plan to use */
				asplan = makeNode(AlternativeSubPlan);
				asplan->subplans = list_make2(result, hashplan);
				result = (Node *) asplan;
				root->hasAlternativeSubPlans = true;
			}
		}
	}

	return result;
}

/*
 * Build a SubPlan node given the raw inputs --- subroutine for make_subplan
 *
 * Returns either the SubPlan, or a replacement expression if we decide to
 * make it an InitPlan, as explained in the comments for make_subplan.
 */
static Node *
build_subplan(PlannerInfo *root, Plan *plan, PlannerInfo *subroot,
			  List *plan_params,
			  SubLinkType subLinkType, int subLinkId,
			  Node *testexpr, List *testexpr_paramids,
			  bool unknownEqFalse)
{
	Node	   *result;
	SubPlan    *splan;
	ListCell   *lc;
	Bitmapset  *plan_param_set;

	/*
	 * Initialize the SubPlan node.  Note plan_id, plan_name, and cost fields
	 * are set further down.
	 */
	splan = makeNode(SubPlan);
	splan->subLinkType = subLinkType;
	splan->testexpr = NULL;
	splan->paramIds = NIL;
	get_first_col_type(plan, &splan->firstColType, &splan->firstColTypmod,
					   &splan->firstColCollation);
	splan->useHashTable = false;
	splan->unknownEqFalse = unknownEqFalse;
	splan->parallel_safe = plan->parallel_safe;
	splan->is_initplan = false;
	splan->is_multirow = false;
	splan->setParam = NIL;
	splan->parParam = NIL;
	splan->args = NIL;
	splan->extParam = NIL;

	plan_param_set  = NULL;
	/*
	 * Make parParam and args lists of param IDs and expressions that current
	 * query level will pass to this child plan.
	 */
	foreach(lc, plan_params)
	{
		PlannerParamItem *pitem = (PlannerParamItem *) lfirst(lc);
		Node	   *arg = pitem->item;

		/*
		 * The Var, PlaceHolderVar, Aggref or GroupingFunc has already been
		 * adjusted to have the correct varlevelsup, phlevelsup, or
		 * agglevelsup.
		 *
		 * If it's a PlaceHolderVar, Aggref or GroupingFunc, its arguments
		 * might contain SubLinks, which have not yet been processed (see the
		 * comments for SS_replace_correlation_vars).  Do that now.
		 */
		if (IsA(arg, PlaceHolderVar) ||
			IsA(arg, Aggref) ||
			IsA(arg, GroupingFunc))
			arg = SS_process_sublinks(root, arg, false);

		splan->parParam = lappend_int(splan->parParam, pitem->paramId);
		splan->args = lappend(splan->args, arg);
		plan_param_set = bms_add_member(plan_param_set, pitem->paramId);
	}

	/*
	 * Un-correlated or undirect correlated plans of EXISTS, EXPR, ARRAY,
	 * ROWCOMPARE, or MULTIEXPR types can be used as initPlans.  For EXISTS,
	 * EXPR, or ARRAY, we return a Param referring to the result of evaluating
	 * the initPlan.  For ROWCOMPARE, we must modify the testexpr tree to
	 * contain PARAM_EXEC Params instead of the PARAM_SUBLINK Params emitted
	 * by the parser, and then return that tree.  For MULTIEXPR, we return a
	 * null constant: the resjunk targetlist item containing the SubLink does
	 * not need to return anything useful, since the referencing Params are
	 * elsewhere.
	 */
	if (splan->parParam == NIL && subLinkType == EXISTS_SUBLINK)
	{
		Param	   *prm;

		Assert(testexpr == NULL);
		prm = generate_new_exec_param(root, BOOLOID, -1, InvalidOid);
		splan->setParam = list_make1_int(prm->paramid);
		splan->is_initplan = true;
		result = (Node *) prm;
	}
	else if (splan->parParam == NIL && subLinkType == EXPR_SUBLINK)
	{
		TargetEntry *te = linitial(plan->targetlist);
		Param	   *prm;

		Assert(!te->resjunk);
		Assert(testexpr == NULL);
		prm = generate_new_exec_param(root,
									  exprType((Node *) te->expr),
									  exprTypmod((Node *) te->expr),
									  exprCollation((Node *) te->expr));
		splan->setParam = list_make1_int(prm->paramid);
		splan->is_initplan = true;
		result = (Node *) prm;
	}
	else if (splan->parParam == NIL && subLinkType == ARRAY_SUBLINK)
	{
		TargetEntry *te = linitial(plan->targetlist);
		Oid			arraytype;
		Param	   *prm;

		Assert(!te->resjunk);
		Assert(testexpr == NULL);
		arraytype = get_promoted_array_type(exprType((Node *) te->expr));
		if (!OidIsValid(arraytype))
			elog(ERROR, "could not find array type for datatype %s",
				 format_type_be(exprType((Node *) te->expr)));
		prm = generate_new_exec_param(root,
									  arraytype,
									  exprTypmod((Node *) te->expr),
									  exprCollation((Node *) te->expr));
		splan->setParam = list_make1_int(prm->paramid);
		splan->is_initplan = true;
		result = (Node *) prm;
	}
	else if (splan->parParam == NIL && subLinkType == ROWCOMPARE_SUBLINK)
	{
		/* Adjust the Params */
		List	   *params;

		Assert(testexpr != NULL);
		params = generate_subquery_params(root,
										  plan->targetlist,
										  &splan->paramIds);
		result = convert_testexpr(root,
								  testexpr,
								  params);
		splan->setParam = list_copy(splan->paramIds);
		splan->is_initplan = true;

		/*
		 * The executable expression is returned to become part of the outer
		 * plan's expression tree; it is not kept in the initplan node.
		 */
	}
	else if (subLinkType == MULTIEXPR_SUBLINK)
	{
		/*
		 * Whether it's an initplan or not, it needs to set a PARAM_EXEC Param
		 * for each output column.
		 */
		List	   *params;

		Assert(testexpr == NULL);
		params = generate_subquery_params(root,
										  plan->targetlist,
										  &splan->setParam);

		/*
		 * Save the list of replacement Params in the n'th cell of
		 * root->multiexpr_params; setrefs.c will use it to replace
		 * PARAM_MULTIEXPR Params.
		 */
		while (list_length(root->multiexpr_params) < subLinkId)
			root->multiexpr_params = lappend(root->multiexpr_params, NIL);
		lc = list_nth_cell(root->multiexpr_params, subLinkId - 1);
		Assert(lfirst(lc) == NIL);
		lfirst(lc) = params;

		/* It can be an initplan if there are no parParams. */
		if (splan->parParam == NIL)
		{
			splan->is_initplan = true;
			result = (Node *) makeNullConst(RECORDOID, -1, InvalidOid);
		}
		else
		{
			splan->is_initplan = false;
			result = (Node *) splan;
		}
	}
	else
	{
		/*
		 * Adjust the Params in the testexpr, unless caller already took care
		 * of it (as indicated by passing a list of Param IDs).
		 */
		if (testexpr && testexpr_paramids == NIL)
		{
			List	   *params;

			params = generate_subquery_params(root,
											  plan->targetlist,
											  &splan->paramIds);
			splan->testexpr = convert_testexpr(root,
											   testexpr,
											   params);
		}
		else
		{
			splan->testexpr = testexpr;
			splan->paramIds = testexpr_paramids;
		}

		splan->is_multirow = true; /* CDB: take note. */

		/*
		 * We can't convert subplans of ALL_SUBLINK or ANY_SUBLINK types to
		 * initPlans, even when they are uncorrelated or undirect correlated,
		 * because we need to scan the output of the subplan for each outer
		 * tuple.  But if it's a not-direct-correlated IN (= ANY) test, we
		 * might be able to use a hashtable to avoid comparing all the tuples.
		 *
		 * TODO siva - I believe we should've pulled these up to be NL joins.
		 * We may want to assert that this is never exercised.
		 */
		if (subLinkType == ANY_SUBLINK &&
			splan->parParam == NIL &&
			subplan_is_hashable(root, plan) &&
			testexpr_is_hashable(splan->testexpr, splan->paramIds))
			splan->useHashTable = true;

		/*
		 * Otherwise, we have the option to tack a Material node onto the top
		 * of the subplan, to reduce the cost of reading it repeatedly.  This
		 * is pointless for a direct-correlated subplan, since we'd have to
		 * recompute its results each time anyway.  For uncorrelated/undirect
		 * correlated subplans, we add Material unless the subplan's top plan
		 * node would materialize its output anyway.  Also, if enable_material
		 * is false, then the user does not want us to materialize anything
		 * unnecessarily, so we don't.
		 */
		else if (splan->parParam == NIL && enable_material &&
				 !ExecMaterializesOutput(nodeTag(plan)))
			plan = materialize_finished_plan(root, plan);

		result = (Node *) splan;
	}

	AssertEquivalent(splan->is_initplan, !splan->is_multirow && splan->parParam == NIL);

	/*
	 * Add the subplan and its PlannerInfo to the global lists.
	 */
	root->glob->subplans = lappend(root->glob->subplans, plan);
	root->glob->subroots = lappend(root->glob->subroots, subroot);
	splan->plan_id = list_length(root->glob->subplans);

	if (splan->is_initplan)
		root->init_plans = lappend(root->init_plans, splan);

	/*
	 * A parameterless subplan (not initplan) should be prepared to handle
	 * REWIND efficiently.  If it has direct parameters then there's no point
	 * since it'll be reset on each scan anyway; and if it's an initplan then
	 * there's no point since it won't get re-run without parameter changes
	 * anyway.  The input of a hashed subplan doesn't need REWIND either.
	 */
	if (splan->parParam == NIL && !splan->is_initplan && !splan->useHashTable)
		root->glob->rewindPlanIDs = bms_add_member(root->glob->rewindPlanIDs,
												   splan->plan_id);

	/* Label the subplan for EXPLAIN purposes */
	splan->plan_name = palloc(32 + 12 * list_length(splan->setParam));
	sprintf(splan->plan_name, "%s %d",
			splan->is_initplan ? "InitPlan" : "SubPlan",
			splan->plan_id);
	if (splan->setParam)
	{
		char	   *ptr = splan->plan_name + strlen(splan->plan_name);

		ptr += sprintf(ptr, " (returns ");
		foreach(lc, splan->setParam)
		{
			ptr += sprintf(ptr, "$%d%s",
						   lfirst_int(lc),
						   lnext(lc) ? "," : ")");
		}
	}

	/* Lastly, fill in the cost estimates for use later */
	cost_subplan(root, splan, plan);

	return result;
}

/*
 * generate_subquery_params: build a list of Params representing the output
 * columns of a sublink's sub-select, given the sub-select's targetlist.
 *
 * We also return an integer list of the paramids of the Params.
 */
static List *
generate_subquery_params(PlannerInfo *root, List *tlist, List **paramIds)
{
	List	   *result;
	List	   *ids;
	ListCell   *lc;

	result = ids = NIL;
	foreach(lc, tlist)
	{
		TargetEntry *tent = (TargetEntry *) lfirst(lc);
		Param	   *param;

		if (tent->resjunk)
			continue;

		param = generate_new_exec_param(root,
										exprType((Node *) tent->expr),
										exprTypmod((Node *) tent->expr),
										exprCollation((Node *) tent->expr));
		result = lappend(result, param);
		ids = lappend_int(ids, param->paramid);
	}

	*paramIds = ids;
	return result;
}

/*
 * generate_subquery_vars: build a list of Vars representing the output
 * columns of a sublink's sub-select, given the sub-select's targetlist.
 * The Vars have the specified varno (RTE index).
 */
List *
generate_subquery_vars(PlannerInfo *root, List *tlist, Index varno)
{
	List	   *result;
	ListCell   *lc;

	result = NIL;
	foreach(lc, tlist)
	{
		TargetEntry *tent = (TargetEntry *) lfirst(lc);
		Var		   *var;

		if (tent->resjunk)
			continue;

		var = makeVarFromTargetEntry(varno, tent);
		result = lappend(result, var);
	}

	return result;
}

/*
 * convert_testexpr: convert the testexpr given by the parser into
 * actually executable form.  This entails replacing PARAM_SUBLINK Params
 * with Params or Vars representing the results of the sub-select.  The
 * nodes to be substituted are passed in as the List result from
 * generate_subquery_params or generate_subquery_vars.
 */
Node *
convert_testexpr(PlannerInfo *root,
				 Node *testexpr,
				 List *subst_nodes)
{
	convert_testexpr_context context;

	context.root = root;
	context.subst_nodes = subst_nodes;
	return convert_testexpr_mutator(testexpr, &context);
}

static Node *
convert_testexpr_mutator(Node *node,
						 convert_testexpr_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_SUBLINK)
		{
			if (param->paramid <= 0 ||
				param->paramid > list_length(context->subst_nodes))
				elog(ERROR, "unexpected PARAM_SUBLINK ID: %d", param->paramid);

			/*
			 * We copy the list item to avoid having doubly-linked
			 * substructure in the modified parse tree.  This is probably
			 * unnecessary when it's a Param, but be safe.
			 */
			return (Node *) copyObject(list_nth(context->subst_nodes,
												param->paramid - 1));
		}
	}
	if (IsA(node, SubLink))
	{
		/*
		 * If we come across a nested SubLink, it is neither necessary nor
		 * correct to recurse into it: any PARAM_SUBLINKs we might find inside
		 * belong to the inner SubLink not the outer. So just return it as-is.
		 *
		 * This reasoning depends on the assumption that nothing will pull
		 * subexpressions into or out of the testexpr field of a SubLink, at
		 * least not without replacing PARAM_SUBLINKs first.  If we did want
		 * to do that we'd need to rethink the parser-output representation
		 * altogether, since currently PARAM_SUBLINKs are only unique per
		 * SubLink not globally across the query.  The whole point of
		 * replacing them with Vars or PARAM_EXEC nodes is to make them
		 * globally unique before they escape from the SubLink's testexpr.
		 *
		 * Note: this can't happen when called during SS_process_sublinks,
		 * because that recursively processes inner SubLinks first.  It can
		 * happen when called from convert_ANY_sublink_to_join, though.
		 */
		return node;
	}
	return expression_tree_mutator(node,
								   convert_testexpr_mutator,
								   (void *) context);
}

/*
 * subplan_is_hashable: can we implement an ANY subplan by hashing?
 *
 * This is not responsible for checking whether the combining testexpr
 * is suitable for hashing.  We only look at the subquery itself.
 */
static bool
subplan_is_hashable(PlannerInfo *root, Plan *plan)
{
	double		subquery_size;

	/*
	 * The estimated size of the subquery result must fit in work_mem. (Note:
	 * we use heap tuple overhead here even though the tuples will actually be
	 * stored as MinimalTuples; this provides some fudge factor for hashtable
	 * overhead.)
	 */
	subquery_size = plan->plan_rows *
		(MAXALIGN(plan->plan_width) + MAXALIGN(SizeofHeapTupleHeader));
	if (subquery_size > global_work_mem(root))
		return false;

	return true;
}

/*
 * subpath_is_hashable: can we implement an ANY subplan by hashing?
 *
 * Identical to subplan_is_hashable, but work from a Path for the subplan.
 */
static bool
subpath_is_hashable(PlannerInfo *root, Path *path)
{
	double		subquery_size;

	/*
	 * The estimated size of the subquery result must fit in work_mem. (Note:
	 * we use heap tuple overhead here even though the tuples will actually be
	 * stored as MinimalTuples; this provides some fudge factor for hashtable
	 * overhead.)
	 */
	subquery_size = path->rows *
		(MAXALIGN(path->pathtarget->width) + MAXALIGN(SizeofHeapTupleHeader));
	if (subquery_size > global_work_mem(root))
		return false;

	return true;
}

/*
 * testexpr_is_hashable: is an ANY SubLink's test expression hashable?
 *
 * To identify LHS vs RHS of the hash expression, we must be given the
 * list of output Param IDs of the SubLink's subquery.
 */
bool
testexpr_is_hashable(Node *testexpr, List *param_ids)
{
	/*
	 * The testexpr must be a single OpExpr, or an AND-clause containing only
	 * OpExprs, each of which satisfy test_opexpr_is_hashable().
	 */
	if (testexpr && IsA(testexpr, OpExpr))
	{
		if (test_opexpr_is_hashable((OpExpr *) testexpr, param_ids))
			return true;
	}
	else if (is_andclause(testexpr))
	{
		ListCell   *l;

		foreach(l, ((BoolExpr *) testexpr)->args)
		{
			Node	   *andarg = (Node *) lfirst(l);

			if (!IsA(andarg, OpExpr))
				return false;
			if (!test_opexpr_is_hashable((OpExpr *) andarg, param_ids))
				return false;
		}
		return true;
	}

	return false;
}

static bool
test_opexpr_is_hashable(OpExpr *testexpr, List *param_ids)
{
	/*
	 * The combining operator must be hashable and strict.  The need for
	 * hashability is obvious, since we want to use hashing.  Without
	 * strictness, behavior in the presence of nulls is too unpredictable.  We
	 * actually must assume even more than plain strictness: it can't yield
	 * NULL for non-null inputs, either (see nodeSubplan.c).  However, hash
	 * indexes and hash joins assume that too.
	 */
	if (!hash_ok_operator(testexpr))
		return false;

	/*
	 * The left and right inputs must belong to the outer and inner queries
	 * respectively; hence Params that will be supplied by the subquery must
	 * not appear in the LHS, and Vars of the outer query must not appear in
	 * the RHS.  (Ordinarily, this must be true because of the way that the
	 * parser builds an ANY SubLink's testexpr ... but inlining of functions
	 * could have changed the expression's structure, so we have to check.
	 * Such cases do not occur often enough to be worth trying to optimize, so
	 * we don't worry about trying to commute the clause or anything like
	 * that; we just need to be sure not to build an invalid plan.)
	 */
	if (list_length(testexpr->args) != 2)
		return false;
	if (contain_exec_param((Node *) linitial(testexpr->args), param_ids))
		return false;
	if (contain_var_clause((Node *) lsecond(testexpr->args)))
		return false;
	return true;
}

/*
 * Check expression is hashable + strict
 *
 * We could use op_hashjoinable() and op_strict(), but do it like this to
 * avoid a redundant cache lookup.
 */
static bool
hash_ok_operator(OpExpr *expr)
{
	Oid			opid = expr->opno;

	/* quick out if not a binary operator */
	if (list_length(expr->args) != 2)
		return false;
	if (opid == ARRAY_EQ_OP)
	{
		/* array_eq is strict, but must check input type to ensure hashable */
		/* XXX record_eq will need same treatment when it becomes hashable */
		Node	   *leftarg = linitial(expr->args);

		return op_hashjoinable(opid, exprType(leftarg));
	}
	else
	{
		/* else must look up the operator properties */
		HeapTuple	tup;
		Form_pg_operator optup;

		tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for operator %u", opid);
		optup = (Form_pg_operator) GETSTRUCT(tup);
		if (!optup->oprcanhash || !func_strict(optup->oprcode))
		{
			ReleaseSysCache(tup);
			return false;
		}
		ReleaseSysCache(tup);
		return true;
	}
}


#if 0
/*
 * GPDB doesn't use initplan + CteScan, so running SS_process_ctes will only
 * generate unused initplans. Keep commented out to avoid merge conflicts with
 * upstream.
 */

/*
 * SS_process_ctes: process a query's WITH list
 *
 * Consider each CTE in the WITH list and either ignore it (if it's an
 * unreferenced SELECT), "inline" it to create a regular sub-SELECT-in-FROM,
 * or convert it to an initplan.
 *
 * A side effect is to fill in root->cte_plan_ids with a list that
 * parallels root->parse->cteList and provides the subplan ID for
 * each CTE's initplan, or a dummy ID (-1) if we didn't make an initplan.
 */
void
SS_process_ctes(PlannerInfo *root)
{
	ListCell   *lc;

	Assert(root->cte_plan_ids == NIL);

	foreach(lc, root->parse->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
		CmdType		cmdType = ((Query *) cte->ctequery)->commandType;
		Query	   *subquery;
		PlannerInfo *subroot;
		RelOptInfo *final_rel;
		Path	   *best_path;
		Plan	   *plan;
		SubPlan    *splan;
		int			paramid;

		/*
		 * Ignore SELECT CTEs that are not actually referenced anywhere.
		 */
		if (cte->cterefcount == 0 && cmdType == CMD_SELECT)
		{
			/* Make a dummy entry in cte_plan_ids */
			root->cte_plan_ids = lappend_int(root->cte_plan_ids, -1);
			continue;
		}

		/*
		 * Consider inlining the CTE (creating RTE_SUBQUERY RTE(s)) instead of
		 * implementing it as a separately-planned CTE.
		 *
		 * We cannot inline if any of these conditions hold:
		 *
		 * 1. The user said not to (the CTEMaterializeAlways option).
		 *
		 * 2. The CTE is recursive.
		 *
		 * 3. The CTE has side-effects; this includes either not being a plain
		 * SELECT, or containing volatile functions.  Inlining might change
		 * the side-effects, which would be bad.
		 *
		 * 4. The CTE is multiply-referenced and contains a self-reference to
		 * a recursive CTE outside itself.  Inlining would result in multiple
		 * recursive self-references, which we don't support.
		 *
		 * Otherwise, we have an option whether to inline or not.  That should
		 * always be a win if there's just a single reference, but if the CTE
		 * is multiply-referenced then it's unclear: inlining adds duplicate
		 * computations, but the ability to absorb restrictions from the outer
		 * query level could outweigh that.  We do not have nearly enough
		 * information at this point to tell whether that's true, so we let
		 * the user express a preference.  Our default behavior is to inline
		 * only singly-referenced CTEs, but a CTE marked CTEMaterializeNever
		 * will be inlined even if multiply referenced.
		 *
		 * Note: we check for volatile functions last, because that's more
		 * expensive than the other tests needed.
		 */
		if ((cte->ctematerialized == CTEMaterializeNever ||
			 (cte->ctematerialized == CTEMaterializeDefault &&
			  cte->cterefcount == 1)) &&
			!cte->cterecursive &&
			cmdType == CMD_SELECT &&
			!contain_dml(cte->ctequery) &&
			(cte->cterefcount <= 1 ||
			 !contain_outer_selfref(cte->ctequery)) &&
			!contain_volatile_functions(cte->ctequery))
		{
			inline_cte(root, cte);
			/* Make a dummy entry in cte_plan_ids */
			root->cte_plan_ids = lappend_int(root->cte_plan_ids, -1);
			continue;
		}

		/*
		 * Copy the source Query node.  Probably not necessary, but let's keep
		 * this similar to make_subplan.
		 */
		subquery = (Query *) copyObject(cte->ctequery);

		/* plan_params should not be in use in current query level */
		Assert(root->plan_params == NIL);

		/*
		 * Generate Paths for the CTE query.  Always plan for full retrieval
		 * --- we don't have enough info to predict otherwise.
		 */
		subroot = subquery_planner(root->glob, subquery,
								   root,
								   cte->cterecursive, 0.0,
								   root->config);

		/*
		 * Since the current query level doesn't yet contain any RTEs, it
		 * should not be possible for the CTE to have requested parameters of
		 * this level.
		 */
		if (root->plan_params)
			elog(ERROR, "unexpected outer reference in CTE query");

		/*
		 * Select best Path and turn it into a Plan.  At least for now, there
		 * seems no reason to postpone doing that.
		 */
		final_rel = fetch_upper_rel(subroot, UPPERREL_FINAL, NULL);
		best_path = final_rel->cheapest_total_path;

		plan = create_plan(subroot, best_path);

		/*
		 * Make a SubPlan node for it.  This is just enough unlike
		 * build_subplan that we can't share code.
		 *
		 * Note plan_id, plan_name, and cost fields are set further down.
		 */
		splan = makeNode(SubPlan);
		splan->subLinkType = CTE_SUBLINK;
		splan->testexpr = NULL;
		splan->paramIds = NIL;
		get_first_col_type(plan, &splan->firstColType, &splan->firstColTypmod,
						   &splan->firstColCollation);
		splan->useHashTable = false;
		splan->unknownEqFalse = false;

		/*
		 * CTE scans are not considered for parallelism (cf
		 * set_rel_consider_parallel), and even if they were, initPlans aren't
		 * parallel-safe.
		 */
		splan->parallel_safe = false;
		splan->setParam = NIL;
		splan->parParam = NIL;
		splan->args = NIL;

		/*
		 * The node can't have any inputs (since it's an initplan), so the
		 * parParam and args lists remain empty.  (It could contain references
		 * to earlier CTEs' output param IDs, but CTE outputs are not
		 * propagated via the args list.)
		 */

		/*
		 * Assign a param ID to represent the CTE's output.  No ordinary
		 * "evaluation" of this param slot ever happens, but we use the param
		 * ID for setParam/chgParam signaling just as if the CTE plan were
		 * returning a simple scalar output.  (Also, the executor abuses the
		 * ParamExecData slot for this param ID for communication among
		 * multiple CteScan nodes that might be scanning this CTE.)
		 */
		paramid = assign_special_exec_param(root);
		splan->setParam = list_make1_int(paramid);

		/*
		 * Add the subplan and its PlannerInfo to the global lists.
		 */
		root->glob->subplans = lappend(root->glob->subplans, plan);
		root->glob->subroots = lappend(root->glob->subroots, subroot);
		splan->plan_id = list_length(root->glob->subplans);

		root->init_plans = lappend(root->init_plans, splan);

		root->cte_plan_ids = lappend_int(root->cte_plan_ids, splan->plan_id);

		/* Label the subplan for EXPLAIN purposes */
		splan->plan_name = psprintf("CTE %s", cte->ctename);

		/* Lastly, fill in the cost estimates for use later */
		cost_subplan(root, splan, plan);
	}
}

/*
 * contain_dml: is any subquery not a plain SELECT?
 *
 * We reject SELECT FOR UPDATE/SHARE as well as INSERT etc.
 */
static bool
contain_dml(Node *node)
{
	return contain_dml_walker(node, NULL);
}

static bool
contain_dml_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		if (query->commandType != CMD_SELECT ||
			query->rowMarks != NIL)
			return true;

		return query_tree_walker(query, contain_dml_walker, context, 0);
	}
	return expression_tree_walker(node, contain_dml_walker, context);
}
#endif
/*
 * contain_outer_selfref: is there an external recursive self-reference?
 */
bool
contain_outer_selfref(Node *node)
{
	Index		depth = 0;

	/*
	 * We should be starting with a Query, so that depth will be 1 while
	 * examining its immediate contents.
	 */
	Assert(IsA(node, Query));

	return contain_outer_selfref_walker(node, &depth);
}

static bool
contain_outer_selfref_walker(Node *node, Index *depth)
{
	if (node == NULL)
		return false;
	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		/*
		 * Check for a self-reference to a CTE that's above the Query that our
		 * search started at.
		 */
		if (rte->rtekind == RTE_CTE &&
			rte->self_reference &&
			rte->ctelevelsup >= *depth)
			return true;
		return false;			/* allow range_table_walker to continue */
	}
	if (IsA(node, Query))
	{
		/* Recurse into subquery, tracking nesting depth properly */
		Query	   *query = (Query *) node;
		bool		result;

		(*depth)++;

		result = query_tree_walker(query, contain_outer_selfref_walker,
								   (void *) depth, QTW_EXAMINE_RTES_BEFORE);

		(*depth)--;

		return result;
	}
	return expression_tree_walker(node, contain_outer_selfref_walker,
								  (void *) depth);
}
#if 0
/*
 * inline_cte: convert RTE_CTE references to given CTE into RTE_SUBQUERYs
 */
static void
inline_cte(PlannerInfo *root, CommonTableExpr *cte)
{
	struct inline_cte_walker_context context;

	context.ctename = cte->ctename;
	/* Start at levelsup = -1 because we'll immediately increment it */
	context.levelsup = -1;
	context.ctequery = castNode(Query, cte->ctequery);

	(void) inline_cte_walker((Node *) root->parse, &context);
}

static bool
inline_cte_walker(Node *node, inline_cte_walker_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		context->levelsup++;

		/*
		 * Visit the query's RTE nodes after their contents; otherwise
		 * query_tree_walker would descend into the newly inlined CTE query,
		 * which we don't want.
		 */
		(void) query_tree_walker(query, inline_cte_walker, context,
								 QTW_EXAMINE_RTES_AFTER);

		context->levelsup--;

		return false;
	}
	else if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) node;

		if (rte->rtekind == RTE_CTE &&
			strcmp(rte->ctename, context->ctename) == 0 &&
			rte->ctelevelsup == context->levelsup)
		{
			/*
			 * Found a reference to replace.  Generate a copy of the CTE query
			 * with appropriate level adjustment for outer references (e.g.,
			 * to other CTEs).
			 */
			Query	   *newquery = copyObject(context->ctequery);

			if (context->levelsup > 0)
				IncrementVarSublevelsUp((Node *) newquery, context->levelsup, 1);

			/*
			 * Convert the RTE_CTE RTE into a RTE_SUBQUERY.
			 *
			 * Historically, a FOR UPDATE clause has been treated as extending
			 * into views and subqueries, but not into CTEs.  We preserve this
			 * distinction by not trying to push rowmarks into the new
			 * subquery.
			 */
			rte->rtekind = RTE_SUBQUERY;
			rte->subquery = newquery;
			rte->security_barrier = false;

			/* Zero out CTE-specific fields */
			rte->ctename = NULL;
			rte->ctelevelsup = 0;
			rte->self_reference = false;
			rte->coltypes = NIL;
			rte->coltypmods = NIL;
			rte->colcollations = NIL;
		}

		return false;
	}

	return expression_tree_walker(node, inline_cte_walker, context);
}
#endif

/*
 * convert_ANY_sublink_to_join: try to convert an ANY SubLink to a join
 *
 * The caller has found an ANY SubLink at the top level of one of the query's
 * qual clauses, but has not checked the properties of the SubLink further.
 * Decide whether it is appropriate to process this SubLink in join style.
 * If so, form a JoinExpr and return it.  Return NULL if the SubLink cannot
 * be converted to a join.
 *
 * The only non-obvious input parameter is available_rels: this is the set
 * of query rels that can safely be referenced in the sublink expression.
 * (We must restrict this to avoid changing the semantics when a sublink
 * is present in an outer join's ON qual.)  The conversion must fail if
 * the converted qual would reference any but these parent-query relids.
 *
 * On success, the returned JoinExpr has larg = NULL and rarg = the jointree
 * item representing the pulled-up subquery.  The caller must set larg to
 * represent the relation(s) on the lefthand side of the new join, and insert
 * the JoinExpr into the upper query's jointree at an appropriate place
 * (typically, where the lefthand relation(s) had been).  Note that the
 * passed-in SubLink must also be removed from its original position in the
 * query quals, since the quals of the returned JoinExpr replace it.
 * (Notionally, we replace the SubLink with a constant TRUE, then elide the
 * redundant constant from the qual.)
 *
 * On success, the caller is also responsible for recursively applying
 * pull_up_sublinks processing to the rarg and quals of the returned JoinExpr.
 * (On failure, there is no need to do anything, since pull_up_sublinks will
 * be applied when we recursively plan the sub-select.)
 *
 * Side effects of a successful conversion include adding the SubLink's
 * subselect to the query's rangetable, so that it can be referenced in
 * the JoinExpr's rarg.
 */
JoinExpr *
convert_ANY_sublink_to_join(PlannerInfo *root, SubLink *sublink,
							Relids available_rels)
{
	JoinExpr   *result;
	Query	   *parse = root->parse;
	Query	   *subselect = (Query *) sublink->subselect;
	Relids		upper_varnos;
	int			rtindex;
	RangeTblEntry *rte;
	RangeTblRef *rtr;
	List	   *subquery_vars;
	Node	   *quals;
	bool		correlated;
	ParseState *pstate;

	Assert(sublink->subLinkType == ANY_SUBLINK);
	Assert(IsA(subselect, Query));

	/* Delete ORDER BY and DISTINCT.
	 *
	 * There is no need to do the group-by or order-by inside the
	 * subquery, if we have decided to pull up the sublink. For the
	 * group-by case, after the sublink pull-up, there will be a semi-join
	 * plan node generated in top level, which will weed out duplicate
	 * tuples naturally. For the order-by case, after the sublink pull-up,
	 * the subquery will become a jointree, inside which the tuples' order
	 * doesn't matter. In a summary, it's safe to elimate the group-by or
	 * order-by causes here.
	*/
	cdbsubselect_drop_orderby(subselect);
	cdbsubselect_drop_distinct(subselect);

	/*
	 * If uncorrelated, and no Var nodes on lhs, the subquery will be executed
	 * only once.  It should become an InitPlan, but make_subplan() doesn't
	 * handle that case, so just flatten it for now.
	 * CDB TODO: Let it become an InitPlan, so its QEs can be recycled.
	 */
	correlated = contain_vars_of_level_or_above(sublink->subselect, 1);

	if (correlated)
	{
		/*
		 * Under certain conditions, we cannot pull up the subquery as a join.
		 */
		if (!is_simple_subquery(root, subselect, NULL, NULL))
			return NULL;

		/*
		 * Do not pull subqueries with correlation in a func expr in the from
		 * clause of the subselect
		 */
		if (has_correlation_in_funcexpr_rte(subselect->rtable))
			return NULL;

		if (contain_subplans(subselect->jointree->quals))
			return NULL;
	}

	/*
	 * The test expression must contain some Vars of the parent query,
	 * else it's not gonna be a join.  (Note that it won't have Vars
	 * referring to the subquery, rather Params.)
	 */
	upper_varnos = pull_varnos(root, sublink->testexpr);
	if (bms_is_empty(upper_varnos))
		return NULL;

	/*
	 * However, it can't refer to anything outside available_rels.
	 */
	if (!bms_is_subset(upper_varnos, available_rels))
		return NULL;

	/*
	 * The combining operators and left-hand expressions mustn't be volatile.
	 */
	if (contain_volatile_functions(sublink->testexpr))
		return NULL;

	/* Create a dummy ParseState for addRangeTableEntryForSubquery */
	pstate = make_parsestate(NULL);

	/*
	 * Okay, pull up the sub-select into upper range table.
	 *
	 * We rely here on the assumption that the outer query has no references
	 * to the inner (necessarily true, other than the Vars that we build
	 * below). Therefore this is a lot easier than what pull_up_subqueries has
	 * to go through.
	 *
	 * If the subquery is correlated, i.e. it refers to any Vars of the
	 * parent query, mark it as lateral.
	 */
	rte = addRangeTableEntryForSubquery(pstate,
										subselect,
										makeAlias("ANY_subquery", NIL),
										correlated,	/* lateral */
										false);
	parse->rtable = lappend(parse->rtable, rte);
	rtindex = list_length(parse->rtable);

	/*
	 * Form a RangeTblRef for the pulled-up sub-select.
	 */
	rtr = makeNode(RangeTblRef);
	rtr->rtindex = rtindex;

	/*
	 * Build a list of Vars representing the subselect outputs.
	 */
	subquery_vars = generate_subquery_vars(root,
										   subselect->targetList,
										   rtindex);

	/*
	 * Build the new join's qual expression, replacing Params with these Vars.
	 */
	quals = convert_testexpr(root, sublink->testexpr, subquery_vars);

	result = makeNode(JoinExpr);
	result->jointype = JOIN_SEMI;
	result->isNatural = false;
	result->larg = NULL;		/* caller must fill this in */
	result->rarg = (Node *) rtr;
	result->usingClause = NIL;
	result->quals = quals;
	result->alias = NULL;
	result->rtindex = 0;

	return result;
}

/*
 * convert_EXISTS_sublink_to_join: try to convert an EXISTS SubLink to a join
 *
 * The API of this function is identical to convert_ANY_sublink_to_join's,
 * except that we also support the case where the caller has found NOT EXISTS,
 * so we need an additional input parameter "under_not".
 */
JoinExpr *
convert_EXISTS_sublink_to_join(PlannerInfo *root, SubLink *sublink,
							   bool under_not, Relids available_rels)
{
	JoinExpr   *result;
	Query	   *parse = root->parse;
	Query	   *subselect = (Query *) sublink->subselect;
	Node	   *whereClause;
	int			rtoffset;
	int			varno;
	Relids		clause_varnos;
	Relids		upper_varnos;

	Assert(sublink->subLinkType == EXISTS_SUBLINK);

	/*
	 * Can't flatten if it contains WITH.  (We could arrange to pull up the
	 * WITH into the parent query's cteList, but that risks changing the
	 * semantics, since a WITH ought to be executed once per associated query
	 * call.)  Note that convert_ANY_sublink_to_join doesn't have to reject
	 * this case, since it just produces a subquery RTE that doesn't have to
	 * get flattened into the parent query.
	 */
	if (subselect->cteList)
		return NULL;

	/*
	 * Copy the subquery so we can modify it safely (see comments in
	 * make_subplan).
	 */
	subselect = copyObject(subselect);

	/*
	 * See if the subquery can be simplified based on the knowledge that it's
	 * being used in EXISTS().  If we aren't able to get rid of its
	 * targetlist, we have to fail, because the pullup operation leaves us
	 * with noplace to evaluate the targetlist.
	 */
	if (!simplify_EXISTS_query(root, subselect))
		return NULL;

	/*
	 * Separate out the WHERE clause.  (We could theoretically also remove
	 * top-level plain JOIN/ON clauses, but it's probably not worth the
	 * trouble.)
	 */
	whereClause = subselect->jointree->quals;
	subselect->jointree->quals = NULL;

	/*
	 * The rest of the sub-select must not refer to any Vars of the parent
	 * query.  (Vars of higher levels should be okay, though.)
	 */
	if (contain_vars_of_level((Node *) subselect, 1))
		return NULL;

	/*
	 * On the other hand, the WHERE clause must contain some Vars of the
	 * parent query, else it's not gonna be a join.
	 */
	if (!contain_vars_of_level(whereClause, 1))
		return NULL;

	/*
	 * We don't risk optimizing if the WHERE clause is volatile, either.
	 */
	if (contain_volatile_functions(whereClause))
		return NULL;

	/*
	 * The subquery must have a nonempty jointree, but we can make it so.
	 */
	replace_empty_jointree(subselect);

	/*
	 * Prepare to pull up the sub-select into top range table.
	 *
	 * We rely here on the assumption that the outer query has no references
	 * to the inner (necessarily true). Therefore this is a lot easier than
	 * what pull_up_subqueries has to go through.
	 *
	 * In fact, it's even easier than what convert_ANY_sublink_to_join has to
	 * do.  The machinations of simplify_EXISTS_query ensured that there is
	 * nothing interesting in the subquery except an rtable and jointree, and
	 * even the jointree FromExpr no longer has quals.  So we can just append
	 * the rtable to our own and use the FromExpr in our jointree. But first,
	 * adjust all level-zero varnos in the subquery to account for the rtable
	 * merger.
	 */
	rtoffset = list_length(parse->rtable);
	OffsetVarNodes((Node *) subselect, rtoffset, 0);
	OffsetVarNodes(whereClause, rtoffset, 0);

	/*
	 * Upper-level vars in subquery will now be one level closer to their
	 * parent than before; in particular, anything that had been level 1
	 * becomes level zero.
	 */
	IncrementVarSublevelsUp((Node *) subselect, -1, 1);
	IncrementVarSublevelsUp(whereClause, -1, 1);

	/*
	 * Now that the WHERE clause is adjusted to match the parent query
	 * environment, we can easily identify all the level-zero rels it uses.
	 * The ones <= rtoffset belong to the upper query; the ones > rtoffset do
	 * not.
	 */
	clause_varnos = pull_varnos(root, whereClause);
	upper_varnos = NULL;
	while ((varno = bms_first_member(clause_varnos)) >= 0)
	{
		if (varno <= rtoffset)
			upper_varnos = bms_add_member(upper_varnos, varno);
	}
	bms_free(clause_varnos);
	Assert(!bms_is_empty(upper_varnos));

	/*
	 * Now that we've got the set of upper-level varnos, we can make the last
	 * check: only available_rels can be referenced.
	 */
	if (!bms_is_subset(upper_varnos, available_rels))
		return NULL;

	/* Now we can attach the modified subquery rtable to the parent */
	parse->rtable = list_concat(parse->rtable, subselect->rtable);

	/*
	 * And finally, build the JoinExpr node.
	 */
	result = makeNode(JoinExpr);
	result->jointype = under_not ? JOIN_ANTI : JOIN_SEMI;
	result->isNatural = false;
	result->larg = NULL;		/* caller must fill this in */
	/* flatten out the FromExpr node if it's useless */
	if (list_length(subselect->jointree->fromlist) == 1)
		result->rarg = (Node *) linitial(subselect->jointree->fromlist);
	else
		result->rarg = (Node *) subselect->jointree;
	result->usingClause = NIL;
	result->quals = whereClause;
	result->alias = NULL;
	result->rtindex = 0;		/* we don't need an RTE for it */

	return result;
}

/*
 * simplify_EXISTS_query: remove any useless stuff in an EXISTS's subquery
 *
 * The only thing that matters about an EXISTS query is whether it returns
 * zero or more than zero rows.  Therefore, we can remove certain SQL features
 * that won't affect that.  The only part that is really likely to matter in
 * typical usage is simplifying the targetlist: it's a common habit to write
 * "SELECT * FROM" even though there is no need to evaluate any columns.
 *
 * Note: by suppressing the targetlist we could cause an observable behavioral
 * change, namely that any errors that might occur in evaluating the tlist
 * won't occur, nor will other side-effects of volatile functions.  This seems
 * unlikely to bother anyone in practice.
 *
 * Returns true if was able to discard the targetlist, else false.
 */
static bool
simplify_EXISTS_query(PlannerInfo *root, Query *query)
{
	/*
	 * PostgreSQL:
	 *
	 * We don't try to simplify at all if the query uses set operations,
	 * aggregates, grouping sets, SRFs, modifying CTEs, HAVING, OFFSET, or FOR
	 * UPDATE/SHARE; none of these seem likely in normal usage and their
	 * possible effects are complex.  (Note: we could ignore an "OFFSET 0"
	 * clause, but that traditionally is used as an optimization fence, so we
	 * don't.)
	 *
	 * In GPDB, we try a bit harder: Try to demote HAVING to WHERE, in case
	 * there are no aggregates or volatile functions. If that fails, only
	 * then give up. Also, just discard any window functions; they
	 * shouldn't affect the number of rows returned.
	 */
	if (query->commandType != CMD_SELECT ||
		query->setOperations ||
#if 0
		query->hasAggs ||
#endif
		query->groupingSets ||
#if 0
		query->hasWindowFuncs ||
		query->havingQual ||
#endif
		query->hasTargetSRFs ||
		query->hasModifyingCTE ||
		query->limitOffset ||
		query->rowMarks)
		return false;

	/*
	 * If the whereClause contains some Vars of the parent query or the rest of
	 * the sub-select refers to any Vars of the parent, this EXISTS sublink is
	 * a correlated sublink.
	 */
	bool is_correlated = contain_vars_of_level(query->jointree->quals, 1) ||
						 contain_vars_of_level((Node *) query, 1);

	/*
	 * LIMIT with a constant positive (or NULL) value doesn't affect the
	 * semantics of EXISTS, so let's ignore such clauses.  This is worth doing
	 * because people accustomed to certain other DBMSes may be in the habit
	 * of writing EXISTS(SELECT ... LIMIT 1) as an optimization.  If there's a
	 * LIMIT with anything else as argument, though, we can't simplify.
	 */
	if (query->limitCount)
	{
		/*
		 * The LIMIT clause has not yet been through eval_const_expressions,
		 * so we have to apply that here.  It might seem like this is a waste
		 * of cycles, since the only case plausibly worth worrying about is
		 * "LIMIT 1" ... but what we'll actually see is "LIMIT int8(1::int4)",
		 * so we have to fold constants or we're not going to recognize it.
		 */
		Node	   *node = eval_const_expressions(root, query->limitCount);
		Const	   *limit;

		/* Might as well update the query if we simplified the clause. */
		query->limitCount = node;

		if (!IsA(node, Const))
			return false;

		limit = (Const *) node;
		Assert(limit->consttype == INT8OID);
		if (!limit->constisnull && DatumGetInt64(limit->constvalue) <= 0)
			return false;

		/* Whether or not the targetlist is safe, we can drop the LIMIT. */
		query->limitCount = NULL;
	}

	if (query->havingQual)
	{
		/*
		 * If HAVING has no aggregates and volatile functions, demote
		 * it to WHERE.
		 * Note: In addition to these rules, subquery_planner() also
		 * checks if HAVING has subplans, which is not relevant here as
		 * there are not going to be any subplans at this stage.
		 */
		if (!contain_aggs_of_level(query->havingQual, 0) &&
		    !contain_volatile_functions(query->havingQual))
		{
			query->jointree->quals = make_and_qual(query->jointree->quals,
												   query->havingQual);
			query->havingQual = NULL;
			query->hasAggs = false;
		}
		else
			return false;
	}

	/*
	 * Otherwise, we can throw away the targetlist, as well as any GROUP,
	 * WINDOW, DISTINCT, and ORDER BY clauses; none of those clauses will
	 * change a nonzero-rows result to zero rows or vice versa.  (Furthermore,
	 * since our parsetree representation of these clauses depends on the
	 * targetlist, we'd better throw them away if we drop the targetlist.)
	 *
	 * We only throw targetlist in correlated sublinks. For uncorrelated
	 * sublinks, we'll do nothing to it's targetlist, since it will be
	 * optimized to a InitPlan Node, which need targetlist.
	 */
	if (is_correlated)
		query->targetList = NIL;

	/*
	 * Delete GROUP BY if no aggregates.
	 *
	 * Note: It's important that we don't clear hasAggs, even though we
	 * removed any possible aggregates from the targetList! If you have a
	 * subquery like "SELECT SUM(foo) ...", we don't need to compute the sum,
	 * but we must still aggregate all the rows, and return a single row,
	 * regardless of how many input rows there are. (In particular, even
	 * if there are no input rows).
	 */
	if (!query->hasAggs)
		query->groupClause = NIL;

	/*
	 * Those clauses could be throwed in correlated and uncorrelated sublinks,
	 * it will not change the correctness of the results, except windowClause.
	 *
	 * Because Greenplum will try to simplify the EXISTS sublink that has Window
	 * Function Node, if we just drop windowClause but not drop WindowFunc node
	 * for a window agg, it'll cause inconsistent and error will happend.
	 */
	if (is_correlated)
		query->windowClause = NIL;
	query->distinctClause = NIL;
	query->sortClause = NIL;
	query->hasDistinctOn = false;

	return true;
}

/*
 * remove_useless_EXISTS_sublink
 * 		Check if the EXISTS sublink doesn't actually need to be executed at all,
 * 		and return TRUE/FALSE directly for it in that case. Otherwise return
 * 		NULL.
 */
Node *
remove_useless_EXISTS_sublink(PlannerInfo *root, Query *subselect, bool under_not)
{
    /*
     * Can't flatten if it contains WITH.  (We could arrange to pull up the
     * WITH into the parent query's cteList, but that risks changing the
     * semantics, since a WITH ought to be executed once per associated query
     * call.)  Note that convert_ANY_sublink_to_join doesn't have to reject
     * this case, since it just produces a subquery RTE that doesn't have to
     * get flattened into the parent query.
     */
    if (subselect->cteList)
        return NULL;

    /*
     * Copy the subquery so we can modify it safely (see comments in
     * make_subplan).
     */
    subselect = copyObject(subselect);

	/*
	 * 'LIMIT n' makes EXISTS false when n <= 0, and doesn't affect the
	 * outcome when n > 0.
	 */
	if (subselect->limitCount)
	{
		Node	*node = eval_const_expressions(root, subselect->limitCount);
		Const	*limit;

		subselect->limitCount = node;

		if (!IsA(node, Const))
			return NULL;

		limit = (Const *) node;
		Assert(limit->consttype == INT8OID);
		if (!limit->constisnull && DatumGetInt64(limit->constvalue) <= 0)
			return makeBoolConst(under_not, false);

		subselect->limitCount = NULL;
	}

	/*
	 * If subquery has aggregates without GROUP BY or HAVING, its result is
	 * exactly one row (assuming no errors), unless that row is discarded by
	 * LIMIT/OFFSET.
	 */
	if (subselect->hasAggs &&
		subselect->groupClause == NIL &&
		subselect->havingQual == NULL)
	{
		/*
		 * 'OFFSET m' falsifies EXISTS for m >= 1, and doesn't affect the
		 * outcome for m < 1, given that the subquery yields at most one row.
		 */
		if (subselect->limitOffset)
		{
			Node	*node = eval_const_expressions(root, subselect->limitOffset);
			Const	*limit;

			subselect->limitOffset = node;

			if (!IsA(node, Const))
				return NULL;

			limit = (Const *) node;
			Assert(limit->consttype == INT8OID);
			if (!limit->constisnull && DatumGetInt64(limit->constvalue) > 0)
				return makeBoolConst(under_not, false);
		}

		return makeBoolConst(!under_not, false);
	}

	return NULL;
}

/*
 * convert_EXISTS_to_ANY: try to convert EXISTS to a hashable ANY sublink
 *
 * The subselect is expected to be a fresh copy that we can munge up,
 * and to have been successfully passed through simplify_EXISTS_query.
 *
 * On success, the modified subselect is returned, and we store a suitable
 * upper-level test expression at *testexpr, plus a list of the subselect's
 * output Params at *paramIds.  (The test expression is already Param-ified
 * and hence need not go through convert_testexpr, which is why we have to
 * deal with the Param IDs specially.)
 *
 * On failure, returns NULL.
 */
static Query *
convert_EXISTS_to_ANY(PlannerInfo *root, Query *subselect,
					  Node **testexpr, List **paramIds)
{
	Node	   *whereClause;
	List	   *leftargs,
			   *rightargs,
			   *opids,
			   *opcollations,
			   *newWhere,
			   *tlist,
			   *testlist,
			   *paramids;
	ListCell   *lc,
			   *rc,
			   *oc,
			   *cc;
	AttrNumber	resno;

	/*
	 * Query must not require a targetlist, since we have to insert a new one.
	 * Caller should have dealt with the case already.
	 */
	Assert(subselect->targetList == NIL);

	/*
	 * Separate out the WHERE clause.  (We could theoretically also remove
	 * top-level plain JOIN/ON clauses, but it's probably not worth the
	 * trouble.)
	 */
	whereClause = subselect->jointree->quals;
	subselect->jointree->quals = NULL;

	/*
	 * The rest of the sub-select must not refer to any Vars of the parent
	 * query.  (Vars of higher levels should be okay, though.)
	 *
	 * Note: we need not check for Aggs separately because we know the
	 * sub-select is as yet unoptimized; any uplevel Agg must therefore
	 * contain an uplevel Var reference.  This is not the case below ...
	 */
	if (contain_vars_of_level((Node *) subselect, 1))
		return NULL;

	/*
	 * We don't risk optimizing if the WHERE clause is volatile, either.
	 */
	if (contain_volatile_functions(whereClause))
		return NULL;

	/*
	 * Clean up the WHERE clause by doing const-simplification etc on it.
	 * Aside from simplifying the processing we're about to do, this is
	 * important for being able to pull chunks of the WHERE clause up into the
	 * parent query.  Since we are invoked partway through the parent's
	 * preprocess_expression() work, earlier steps of preprocess_expression()
	 * wouldn't get applied to the pulled-up stuff unless we do them here. For
	 * the parts of the WHERE clause that get put back into the child query,
	 * this work is partially duplicative, but it shouldn't hurt.
	 *
	 * Note: we do not run flatten_join_alias_vars.  This is OK because any
	 * parent aliases were flattened already, and we're not going to pull any
	 * child Vars (of any description) into the parent.
	 *
	 * Note: passing the parent's root to eval_const_expressions is
	 * technically wrong, but we can get away with it since only the
	 * boundParams (if any) are used, and those would be the same in a
	 * subroot.
	 */
	whereClause = eval_const_expressions(root, whereClause);
	whereClause = (Node *) canonicalize_qual((Expr *) whereClause, false);
	whereClause = (Node *) make_ands_implicit((Expr *) whereClause);

	/*
	 * We now have a flattened implicit-AND list of clauses, which we try to
	 * break apart into "outervar = innervar" hash clauses. Anything that
	 * can't be broken apart just goes back into the newWhere list.  Note that
	 * we aren't trying hard yet to ensure that we have only outer or only
	 * inner on each side; we'll check that if we get to the end.
	 */
	leftargs = rightargs = opids = opcollations = newWhere = NIL;
	foreach(lc, (List *) whereClause)
	{
		OpExpr	   *expr = (OpExpr *) lfirst(lc);

		if (IsA(expr, OpExpr) &&
			hash_ok_operator(expr))
		{
			Node	   *leftarg = (Node *) linitial(expr->args);
			Node	   *rightarg = (Node *) lsecond(expr->args);

			if (contain_vars_of_level(leftarg, 1))
			{
				leftargs = lappend(leftargs, leftarg);
				rightargs = lappend(rightargs, rightarg);
				opids = lappend_oid(opids, expr->opno);
				opcollations = lappend_oid(opcollations, expr->inputcollid);
				continue;
			}
			if (contain_vars_of_level(rightarg, 1))
			{
				/*
				 * We must commute the clause to put the outer var on the
				 * left, because the hashing code in nodeSubplan.c expects
				 * that.  This probably shouldn't ever fail, since hashable
				 * operators ought to have commutators, but be paranoid.
				 */
				expr->opno = get_commutator(expr->opno);
				if (OidIsValid(expr->opno) && hash_ok_operator(expr))
				{
					leftargs = lappend(leftargs, rightarg);
					rightargs = lappend(rightargs, leftarg);
					opids = lappend_oid(opids, expr->opno);
					opcollations = lappend_oid(opcollations, expr->inputcollid);
					continue;
				}
				/* If no commutator, no chance to optimize the WHERE clause */
				return NULL;
			}
		}
		/* Couldn't handle it as a hash clause */
		newWhere = lappend(newWhere, expr);
	}

	/*
	 * If we didn't find anything we could convert, fail.
	 */
	if (leftargs == NIL)
		return NULL;

	/*
	 * There mustn't be any parent Vars or Aggs in the stuff that we intend to
	 * put back into the child query.  Note: you might think we don't need to
	 * check for Aggs separately, because an uplevel Agg must contain an
	 * uplevel Var in its argument.  But it is possible that the uplevel Var
	 * got optimized away by eval_const_expressions.  Consider
	 *
	 * SUM(CASE WHEN false THEN uplevelvar ELSE 0 END)
	 */
	if (contain_vars_of_level((Node *) newWhere, 1) ||
		contain_vars_of_level((Node *) rightargs, 1))
		return NULL;
	if (root->parse->hasAggs &&
		(contain_aggs_of_level((Node *) newWhere, 1) ||
		 contain_aggs_of_level((Node *) rightargs, 1)))
		return NULL;

	/*
	 * And there can't be any child Vars in the stuff we intend to pull up.
	 * (Note: we'd need to check for child Aggs too, except we know the child
	 * has no aggs at all because of simplify_EXISTS_query's check. The same
	 * goes for window functions.)
	 */
	if (contain_vars_of_level((Node *) leftargs, 0))
		return NULL;

	/*
	 * Also reject sublinks in the stuff we intend to pull up.  (It might be
	 * possible to support this, but doesn't seem worth the complication.)
	 */
	if (contain_subplans((Node *) leftargs))
		return NULL;

	/*
	 * Okay, adjust the sublevelsup in the stuff we're pulling up.
	 */
	IncrementVarSublevelsUp((Node *) leftargs, -1, 1);

	/*
	 * Put back any child-level-only WHERE clauses.
	 */
	if (newWhere)
		subselect->jointree->quals = (Node *) make_ands_explicit(newWhere);

	/*
	 * Build a new targetlist for the child that emits the expressions we
	 * need.  Concurrently, build a testexpr for the parent using Params to
	 * reference the child outputs.  (Since we generate Params directly here,
	 * there will be no need to convert the testexpr in build_subplan.)
	 */
	tlist = testlist = paramids = NIL;
	resno = 1;
	forfour(lc, leftargs, rc, rightargs, oc, opids, cc, opcollations)
	{
		Node	   *leftarg = (Node *) lfirst(lc);
		Node	   *rightarg = (Node *) lfirst(rc);
		Oid			opid = lfirst_oid(oc);
		Oid			opcollation = lfirst_oid(cc);
		Param	   *param;

		param = generate_new_exec_param(root,
										exprType(rightarg),
										exprTypmod(rightarg),
										exprCollation(rightarg));
		tlist = lappend(tlist,
						makeTargetEntry((Expr *) rightarg,
										resno++,
										NULL,
										false));
		testlist = lappend(testlist,
						   make_opclause(opid, BOOLOID, false,
										 (Expr *) leftarg, (Expr *) param,
										 InvalidOid, opcollation));
		paramids = lappend_int(paramids, param->paramid);
	}

	/* Put everything where it should go, and we're done */
	subselect->targetList = tlist;
	*testexpr = (Node *) make_ands_explicit(testlist);
	*paramIds = paramids;

	return subselect;
}


/*
 * Replace correlation vars (uplevel vars) with Params.
 *
 * Uplevel PlaceHolderVars and aggregates are replaced, too.
 *
 * Note: it is critical that this runs immediately after SS_process_sublinks.
 * Since we do not recurse into the arguments of uplevel PHVs and aggregates,
 * they will get copied to the appropriate subplan args list in the parent
 * query with uplevel vars not replaced by Params, but only adjusted in level
 * (see replace_outer_placeholdervar and replace_outer_agg).  That's exactly
 * what we want for the vars of the parent level --- but if a PHV's or
 * aggregate's argument contains any further-up variables, they have to be
 * replaced with Params in their turn. That will happen when the parent level
 * runs SS_replace_correlation_vars.  Therefore it must do so after expanding
 * its sublinks to subplans.  And we don't want any steps in between, else
 * those steps would never get applied to the argument expressions, either in
 * the parent or the child level.
 *
 * Another fairly tricky thing going on here is the handling of SubLinks in
 * the arguments of uplevel PHVs/aggregates.  Those are not touched inside the
 * intermediate query level, either.  Instead, SS_process_sublinks recurses on
 * them after copying the PHV or Aggref expression into the parent plan level
 * (this is actually taken care of in build_subplan).
 */
Node *
SS_replace_correlation_vars(PlannerInfo *root, Node *expr)
{
	/* No setup needed for tree walk, so away we go */
	return replace_correlation_vars_mutator(expr, root);
}

static Node *
replace_correlation_vars_mutator(Node *node, PlannerInfo *root)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup > 0)
			return (Node *) replace_outer_var(root, (Var *) node);
	}
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup > 0)
			return (Node *) replace_outer_placeholdervar(root,
														 (PlaceHolderVar *) node);
	}
	if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup > 0)
			return (Node *) replace_outer_agg(root, (Aggref *) node);
	}
	if (IsA(node, GroupingFunc))
	{
		if (((GroupingFunc *) node)->agglevelsup > 0)
			return (Node *) replace_outer_grouping(root, (GroupingFunc *) node);
	}
	if (IsA(node, GroupId))
	{
		if (((GroupId *) node)->agglevelsup > 0)
			return (Node *) replace_outer_group_id(root, (GroupId *) node);
	}
	return expression_tree_mutator(node,
								   replace_correlation_vars_mutator,
								   (void *) root);
}

/*
 * Expand SubLinks to SubPlans in the given expression.
 *
 * The isQual argument tells whether or not this expression is a WHERE/HAVING
 * qualifier expression.  If it is, any sublinks appearing at top level need
 * not distinguish FALSE from UNKNOWN return values.
 */
Node *
SS_process_sublinks(PlannerInfo *root, Node *expr, bool isQual)
{
	process_sublinks_context context;

	context.root = root;
	context.isTopQual = isQual;
	return process_sublinks_mutator(expr, &context);
}

static Node *
process_sublinks_mutator(Node *node, process_sublinks_context *context)
{
	process_sublinks_context locContext;

	locContext.root = context->root;

	if (node == NULL)
		return NULL;
	if (IsA(node, SubLink))
	{
		SubLink    *sublink = (SubLink *) node;
		Node	   *testexpr;

		/*
		 * First, recursively process the lefthand-side expressions, if any.
		 * They're not top-level anymore.
		 */
		locContext.isTopQual = false;
		testexpr = process_sublinks_mutator(sublink->testexpr, &locContext);

		/*
		 * Now build the SubPlan node and make the expr to return.
		 */
		return make_subplan(context->root,
							(Query *) sublink->subselect,
							sublink->subLinkType,
							sublink->subLinkId,
							testexpr,
							context->isTopQual);
	}

	/*
	 * Don't recurse into the arguments of an outer PHV, Aggref or
	 * GroupingFunc here.  Any SubLinks in the arguments have to be dealt with
	 * at the outer query level; they'll be handled when build_subplan
	 * collects the PHV, Aggref or GroupingFunc into the arguments to be
	 * passed down to the current subplan.
	 */
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup > 0)
			return node;
	}
	else if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup > 0)
			return node;
	}
	else if (IsA(node, GroupingFunc))
	{
		if (((GroupingFunc *) node)->agglevelsup > 0)
			return node;
	}

	/*
	 * We should never see a SubPlan expression in the input (since this is
	 * the very routine that creates 'em to begin with).  We shouldn't find
	 * ourselves invoked directly on a Query, either.
	 */
	Assert(!IsA(node, SubPlan));
	Assert(!IsA(node, AlternativeSubPlan));
	Assert(!IsA(node, Query));

	/*
	 * Because make_subplan() could return an AND or OR clause, we have to
	 * take steps to preserve AND/OR flatness of a qual.  We assume the input
	 * has been AND/OR flattened and so we need no recursion here.
	 *
	 * (Due to the coding here, we will not get called on the List subnodes of
	 * an AND; and the input is *not* yet in implicit-AND format.  So no check
	 * is needed for a bare List.)
	 *
	 * Anywhere within the top-level AND/OR clause structure, we can tell
	 * make_subplan() that NULL and FALSE are interchangeable.  So isTopQual
	 * propagates down in both cases.  (Note that this is unlike the meaning
	 * of "top level qual" used in most other places in Postgres.)
	 */
	if (is_andclause(node))
	{
		List	   *newargs = NIL;
		ListCell   *l;

		/* Still at qual top-level */
		locContext.isTopQual = context->isTopQual;

		foreach(l, ((BoolExpr *) node)->args)
		{
			Node	   *newarg;

			newarg = process_sublinks_mutator(lfirst(l), &locContext);
			if (is_andclause(newarg))
				newargs = list_concat(newargs, ((BoolExpr *) newarg)->args);
			else
				newargs = lappend(newargs, newarg);
		}
		return (Node *) make_andclause(newargs);
	}

	if (is_orclause(node))
	{
		List	   *newargs = NIL;
		ListCell   *l;

		/* Still at qual top-level */
		locContext.isTopQual = context->isTopQual;

		foreach(l, ((BoolExpr *) node)->args)
		{
			Node	   *newarg;

			newarg = process_sublinks_mutator(lfirst(l), &locContext);
			if (is_orclause(newarg))
				newargs = list_concat(newargs, ((BoolExpr *) newarg)->args);
			else
				newargs = lappend(newargs, newarg);
		}
		return (Node *) make_orclause(newargs);
	}

	/*
	 * If we recurse down through anything other than an AND or OR node, we
	 * are definitely not at top qual level anymore.
	 */
	locContext.isTopQual = false;

	return expression_tree_mutator(node,
								   process_sublinks_mutator,
								   (void *) &locContext);
}

/*
 * SS_identify_outer_params - identify the Params available from outer levels
 *
 * This must be run after SS_replace_correlation_vars and SS_process_sublinks
 * processing is complete in a given query level as well as all of its
 * descendant levels (which means it's most practical to do it at the end of
 * processing the query level).  We compute the set of paramIds that outer
 * levels will make available to this level+descendants, and record it in
 * root->outer_params for use while computing extParam/allParam sets in final
 * plan cleanup.  (We can't just compute it then, because the upper levels'
 * plan_params lists are transient and will be gone by then.)
 *
 * Input:
 * 	root - PlannerInfo structure that is necessary for walking the tree
 * Output:
 * 	plan->extParam and plan->allParam - attach params to top of the plan
 */
void
SS_identify_outer_params(PlannerInfo *root)
{
	Bitmapset  *outer_params;
	PlannerInfo *proot;
	ListCell   *l;

	/*
	 * If no parameters have been assigned anywhere in the tree, we certainly
	 * don't need to do anything here.
	 */
	if (root->glob->paramExecTypes == NIL)
		return;

	/*
	 * Scan all query levels above this one to see which parameters are due to
	 * be available from them, either because lower query levels have
	 * requested them (via plan_params) or because they will be available from
	 * initPlans of those levels.
	 */
	outer_params = NULL;
	for (proot = root->parent_root; proot != NULL; proot = proot->parent_root)
	{
		/* Include ordinary Var/PHV/Aggref/GroupingFunc params */
		foreach(l, proot->plan_params)
		{
			PlannerParamItem *pitem = (PlannerParamItem *) lfirst(l);

			outer_params = bms_add_member(outer_params, pitem->paramId);
		}
		/* Include any outputs of outer-level initPlans */
		foreach(l, proot->init_plans)
		{
			SubPlan    *initsubplan = (SubPlan *) lfirst(l);
			ListCell   *l2;

			foreach(l2, initsubplan->setParam)
			{
				outer_params = bms_add_member(outer_params, lfirst_int(l2));
			}
		}
		/* Include worktable ID, if a recursive query is being planned */
		if (proot->wt_param_id >= 0)
			outer_params = bms_add_member(outer_params, proot->wt_param_id);
	}
	root->outer_params = outer_params;
}

/*
 * SS_charge_for_initplans - account for initplans in Path costs & parallelism
 *
 * If any initPlans have been created in the current query level, they will
 * get attached to the Plan tree created from whichever Path we select from
 * the given rel.  Increment all that rel's Paths' costs to account for them,
 * and make sure the paths get marked as parallel-unsafe, since we can't
 * currently transmit initPlans to parallel workers.
 *
 * This is separate from SS_attach_initplans because we might conditionally
 * create more initPlans during create_plan(), depending on which Path we
 * select.  However, Paths that would generate such initPlans are expected
 * to have included their cost already.
 */
void
SS_charge_for_initplans(PlannerInfo *root, RelOptInfo *final_rel)
{
	Cost		initplan_cost;
	ListCell   *lc;

	/* Nothing to do if no initPlans */
	if (root->init_plans == NIL)
		return;

	/*
	 * Compute the cost increment just once, since it will be the same for all
	 * Paths.  We assume each initPlan gets run once during top plan startup.
	 * This is a conservative overestimate, since in fact an initPlan might be
	 * executed later than plan startup, or even not at all.
	 */
	initplan_cost = 0;
	foreach(lc, root->init_plans)
	{
		SubPlan    *initsubplan = (SubPlan *) lfirst(lc);

		initplan_cost += initsubplan->startup_cost + initsubplan->per_call_cost;
	}

	/*
	 * Now adjust the costs and parallel_safe flags.
	 */
	foreach(lc, final_rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);

		path->startup_cost += initplan_cost;
		path->total_cost += initplan_cost;
		path->parallel_safe = false;
	}

	/*
	 * Forget about any partial paths and clear consider_parallel, too;
	 * they're not usable if we attached an initPlan.
	 */
	final_rel->partial_pathlist = NIL;
	final_rel->consider_parallel = false;

	/* We needn't do set_cheapest() here, caller will do it */
}

/*
 * SS_attach_initplans - attach initplans to topmost plan node
 *
 * Attach any initplans created in the current query level to the specified
 * plan node, which should normally be the topmost node for the query level.
 * (In principle the initPlans could go in any node at or above where they're
 * referenced; but there seems no reason to put them any lower than the
 * topmost node, so we don't bother to track exactly where they came from.)
 * We do not touch the plan node's cost; the initplans should have been
 * accounted for in path costing.
 */
void
SS_attach_initplans(PlannerInfo *root, Plan *plan)
{
	/*
	 * GPDB: make a copy of the list, because it gets free'd from the plan
	 * later, in the remove_unused_initplans() step.
	 */
	plan->initPlan = list_copy(root->init_plans);
}

/*
 * SS_finalize_plan - do final parameter processing for a completed Plan.
 *
 * This recursively computes the extParam and allParam sets for every Plan
 * node in the given plan tree.  (Oh, and RangeTblFunction.funcparams too.)
 *
 * We assume that SS_finalize_plan has already been run on any initplans or
 * subplans the plan tree could reference.
 */
void
SS_finalize_plan(PlannerInfo *root, Plan *plan)
{
	/* No setup needed, just recurse through plan tree. */
	(void) finalize_plan(root, plan, -1, root->outer_params, NULL);
}

/*
 * Recursive processing of all nodes in the plan tree
 *
 * gather_param is the rescan_param of an ancestral Gather/GatherMerge,
 * or -1 if there is none.
 *
 * valid_params is the set of param IDs supplied by outer plan levels
 * that are valid to reference in this plan node or its children.
 *
 * scan_params is a set of param IDs to force scan plan nodes to reference.
 * This is for EvalPlanQual support, and is always NULL at the top of the
 * recursion.
 *
 * The return value is the computed allParam set for the given Plan node.
 * This is just an internal notational convenience: we can add a child
 * plan's allParams to the set of param IDs of interest to this level
 * in the same statement that recurses to that child.
 *
 * Do not scribble on caller's values of valid_params or scan_params!
 *
 * Note: although we attempt to deal with initPlans anywhere in the tree, the
 * logic is not really right.  The problem is that a plan node might return an
 * output Param of its initPlan as a targetlist item, in which case it's valid
 * for the parent plan level to reference that same Param; the parent's usage
 * will be converted into a Var referencing the child plan node by setrefs.c.
 * But this function would see the parent's reference as out of scope and
 * complain about it.  For now, this does not matter because the planner only
 * attaches initPlans to the topmost plan node in a query level, so the case
 * doesn't arise.  If we ever merge this processing into setrefs.c, maybe it
 * can be handled more cleanly.
 */
static Bitmapset *
finalize_plan(PlannerInfo *root, Plan *plan,
			  int gather_param,
			  Bitmapset *valid_params,
			  Bitmapset *scan_params)
{
	finalize_primnode_context context;
	int			locally_added_param;
	Bitmapset  *nestloop_params;
	Bitmapset  *initExtParam;
	Bitmapset  *initSetParam;
	Bitmapset  *child_params;
	ListCell   *l;

	if (plan == NULL)
		return NULL;

	context.root = root;
	context.paramids = NULL;	/* initialize set to empty */
	locally_added_param = -1;	/* there isn't one */
	nestloop_params = NULL;		/* there aren't any */

	/*
	 * Examine any initPlans to determine the set of external params they
	 * reference and the set of output params they supply.  (We assume
	 * SS_finalize_plan was run on them already.)
	 */
	initExtParam = initSetParam = NULL;

	/*
	 * In gpdb, we traverse init_plans in PlannerInfo to fetch initSetParam fetch.
	 * In upstream should be `foreach(l, plan->initPlan)`
	 *
	 * The different is introduced since sometimes we create a Materized node upon
	 * subplan, so initPlan info is hidden under Materized Node lefttree.
	 */
	foreach(l, root->init_plans)
	{
		SubPlan    *initsubplan = (SubPlan *) lfirst(l);
		Plan	   *initplan = planner_subplan_get_plan(root, initsubplan);
		ListCell   *l2;

		initExtParam = bms_add_members(initExtParam, initplan->extParam);
		foreach(l2, initsubplan->setParam)
		{
			initSetParam = bms_add_member(initSetParam, lfirst_int(l2));
		}

		/*
		 * For gpdb, we need extParam to evaluate if we can process initplan
		 * in ExecutorStart.
		 */
		if (initplan->extParam)
		{
			int paramid;
			ListCell *lc;
			Bitmapset *upperset = NULL;
			Bitmapset *parentset = NULL;
			Bitmapset *extset = initplan->extParam;

			foreach(lc, initsubplan->parParam)
			{
				int tmpid = lfirst_int(lc);
				parentset = bms_add_member(parentset, tmpid);
			}

			upperset = bms_difference(extset, parentset);
			while ((paramid = bms_first_member(upperset)) >= 0)
				initsubplan->extParam = lappend_int(initsubplan->extParam, paramid);
		}
	}

	/* Any setParams are validly referenceable in this node and children */
	if (initSetParam)
		valid_params = bms_union(valid_params, initSetParam);

	/*
	 * When we call finalize_primnode, context.paramids sets are automatically
	 * merged together.  But when recursing to self, we have to do it the hard
	 * way.  We want the paramids set to include params in subplans as well as
	 * at this level.
	 */

	/* Find params in targetlist and qual */
	finalize_primnode((Node *) plan->targetlist, &context);
	finalize_primnode((Node *) plan->qual, &context);

	/*
	 * If it's a parallel-aware scan node, mark it as dependent on the parent
	 * Gather/GatherMerge's rescan Param.
	 */
	if (plan->parallel_aware)
	{
		if (gather_param < 0)
			elog(ERROR, "parallel-aware plan node is not below a Gather");
		context.paramids =
			bms_add_member(context.paramids, gather_param);
	}

	/* Check additional node-type-specific fields */
	switch (nodeTag(plan))
	{
		case T_Result:
			finalize_primnode(((Result *) plan)->resconstantqual,
							  &context);
			break;

		case T_SeqScan:
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_SampleScan:
			finalize_primnode((Node *) ((SampleScan *) plan)->tablesample,
							  &context);
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_IndexScan:
			finalize_primnode((Node *) ((IndexScan *) plan)->indexqual,
							  &context);
			finalize_primnode((Node *) ((IndexScan *) plan)->indexorderby,
							  &context);

			/*
			 * we need not look at indexqualorig, since it will have the same
			 * param references as indexqual.  Likewise, we can ignore
			 * indexorderbyorig.
			 */
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_IndexOnlyScan:
			finalize_primnode((Node *) ((IndexOnlyScan *) plan)->indexqual,
							  &context);
			finalize_primnode((Node *) ((IndexOnlyScan *) plan)->recheckqual,
							  &context);
			finalize_primnode((Node *) ((IndexOnlyScan *) plan)->indexorderby,
							  &context);

			/*
			 * we need not look at indextlist, since it cannot contain Params.
			 */
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_BitmapIndexScan:
			finalize_primnode((Node *) ((BitmapIndexScan *) plan)->indexqual,
							  &context);

			/*
			 * we need not look at indexqualorig, since it will have the same
			 * param references as indexqual.
			 */
			break;

		case T_BitmapHeapScan:
			finalize_primnode((Node *) ((BitmapHeapScan *) plan)->bitmapqualorig,
							  &context);
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_TidScan:
			finalize_primnode((Node *) ((TidScan *) plan)->tidquals,
							  &context);
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_SubqueryScan:
			{
				SubqueryScan *sscan = (SubqueryScan *) plan;
				RelOptInfo *rel;
				Bitmapset  *subquery_params;

				/* We must run finalize_plan on the subquery */
				rel = find_base_rel(root, sscan->scan.scanrelid);
				subquery_params = rel->subroot->outer_params;
				if (gather_param >= 0)
					subquery_params = bms_add_member(bms_copy(subquery_params),
													 gather_param);
				finalize_plan(rel->subroot, sscan->subplan, gather_param,
							  subquery_params, NULL);

				/* Now we can add its extParams to the parent's params */
				context.paramids = bms_add_members(context.paramids,
												   sscan->subplan->extParam);
				/* We need scan_params too, though */
				context.paramids = bms_add_members(context.paramids,
												   scan_params);
			}
			break;

		case T_TableFunctionScan:
			{
				RangeTblEntry *rte;
				RangeTblFunction *rtfunc;

				rte = rt_fetch(((TableFunctionScan *) plan)->scan.scanrelid,
							   root->parse->rtable);
				Assert(rte->rtekind == RTE_TABLEFUNCTION);
				Assert(list_length(rte->functions) == 1);
				rtfunc = (RangeTblFunction *) linitial(rte->functions);
				finalize_primnode(rtfunc->funcexpr, &context);

				/*
				 * GPDB_94_MERGE_FIXME: should we do something about params in
				 * the function expressions, like for FunctionScan nodes below?
				 */
			}
			/* TableFunctionScan's lefttree is like SubqueryScan's subplan. */
			context.paramids = bms_add_members(context.paramids,
								 plan->lefttree->extParam);
			break;

		case T_FunctionScan:
			{
				FunctionScan *fscan = (FunctionScan *) plan;
				ListCell   *lc;

				/*
				 * Call finalize_primnode independently on each function
				 * expression, so that we can record which params are
				 * referenced in each, in order to decide which need
				 * re-evaluating during rescan.
				 */
				foreach(lc, fscan->functions)
				{
					RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);
					finalize_primnode_context funccontext;

					funccontext = context;
					funccontext.paramids = NULL;

					finalize_primnode(rtfunc->funcexpr, &funccontext);

					/* remember results for execution */
					rtfunc->funcparams = funccontext.paramids;

					/* add the function's params to the overall set */
					context.paramids = bms_add_members(context.paramids,
													   funccontext.paramids);
				}

				context.paramids = bms_add_members(context.paramids,
												   scan_params);
			}
			break;

		case T_TableFuncScan:
			finalize_primnode((Node *) ((TableFuncScan *) plan)->tablefunc,
							  &context);
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_ValuesScan:
			finalize_primnode((Node *) ((ValuesScan *) plan)->values_lists,
							  &context);
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_CteScan:
			{
				/*
				 * You might think we should add the node's cteParam to
				 * paramids, but we shouldn't because that param is just a
				 * linkage mechanism for multiple CteScan nodes for the same
				 * CTE; it is never used for changed-param signaling.  What we
				 * have to do instead is to find the referenced CTE plan and
				 * incorporate its external paramids, so that the correct
				 * things will happen if the CTE references outer-level
				 * variables.  See test cases for bug #4902.  (We assume
				 * SS_finalize_plan was run on the CTE plan already.)
				 */
				int			plan_id = ((CteScan *) plan)->ctePlanId;
				Plan	   *cteplan;

				/* so, do this ... */
				if (plan_id < 1 || plan_id > list_length(root->glob->subplans))
					elog(ERROR, "could not find plan for CteScan referencing plan ID %d",
						 plan_id);
				cteplan = (Plan *) list_nth(root->glob->subplans, plan_id - 1);
				context.paramids =
					bms_add_members(context.paramids, cteplan->extParam);

#ifdef NOT_USED
				/* ... but not this */
				context.paramids =
					bms_add_member(context.paramids,
								   ((CteScan *) plan)->cteParam);
#endif

				context.paramids = bms_add_members(context.paramids,
												   scan_params);
			}
			break;

		case T_WorkTableScan:
			context.paramids =
				bms_add_member(context.paramids,
							   ((WorkTableScan *) plan)->wtParam);
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_NamedTuplestoreScan:
			context.paramids = bms_add_members(context.paramids, scan_params);
			break;

		case T_ForeignScan:
			{
				ForeignScan *fscan = (ForeignScan *) plan;

				finalize_primnode((Node *) fscan->fdw_exprs,
								  &context);
				finalize_primnode((Node *) fscan->fdw_recheck_quals,
								  &context);

				/* We assume fdw_scan_tlist cannot contain Params */
				context.paramids = bms_add_members(context.paramids,
												   scan_params);
			}
			break;

		case T_CustomScan:
			{
				CustomScan *cscan = (CustomScan *) plan;
				ListCell   *lc;

				finalize_primnode((Node *) cscan->custom_exprs,
								  &context);
				/* We assume custom_scan_tlist cannot contain Params */
				context.paramids =
					bms_add_members(context.paramids, scan_params);

				/* child nodes if any */
				foreach(lc, cscan->custom_plans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(lc),
													  gather_param,
													  valid_params,
													  scan_params));
				}
			}
			break;

		case T_ModifyTable:
			{
				ModifyTable *mtplan = (ModifyTable *) plan;
				ListCell   *l;

				/* Force descendant scan nodes to reference epqParam */
				locally_added_param = mtplan->epqParam;
				valid_params = bms_add_member(bms_copy(valid_params),
											  locally_added_param);
				scan_params = bms_add_member(bms_copy(scan_params),
											 locally_added_param);
				finalize_primnode((Node *) mtplan->returningLists,
								  &context);
				finalize_primnode((Node *) mtplan->onConflictSet,
								  &context);
				finalize_primnode((Node *) mtplan->onConflictWhere,
								  &context);
				/* exclRelTlist contains only Vars, doesn't need examination */
				foreach(l, mtplan->plans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  gather_param,
													  valid_params,
													  scan_params));
				}
			}
			break;

		case T_Append:
			{
				ListCell   *l;

				foreach(l, ((Append *) plan)->appendplans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  gather_param,
													  valid_params,
													  scan_params));
				}
			}
			break;

		case T_MergeAppend:
			{
				ListCell   *l;

				foreach(l, ((MergeAppend *) plan)->mergeplans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  gather_param,
													  valid_params,
													  scan_params));
				}
			}
			break;

		case T_BitmapAnd:
			{
				ListCell   *l;

				foreach(l, ((BitmapAnd *) plan)->bitmapplans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  gather_param,
													  valid_params,
													  scan_params));
				}
			}
			break;

		case T_BitmapOr:
			{
				ListCell   *l;

				foreach(l, ((BitmapOr *) plan)->bitmapplans)
				{
					context.paramids =
						bms_add_members(context.paramids,
										finalize_plan(root,
													  (Plan *) lfirst(l),
													  gather_param,
													  valid_params,
													  scan_params));
				}
			}
			break;

		case T_NestLoop:
			{
				ListCell   *l;

				finalize_primnode((Node *) ((Join *) plan)->joinqual,
								  &context);
				/* collect set of params that will be passed to right child */
				foreach(l, ((NestLoop *) plan)->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam *) lfirst(l);

					nestloop_params = bms_add_member(nestloop_params,
													 nlp->paramno);
				}
			}
			break;

		case T_MergeJoin:
			finalize_primnode((Node *) ((Join *) plan)->joinqual,
							  &context);
			finalize_primnode((Node *) ((MergeJoin *) plan)->mergeclauses,
							  &context);
			break;

		case T_HashJoin:
			finalize_primnode((Node *) ((Join *) plan)->joinqual,
							  &context);
			finalize_primnode((Node *) ((HashJoin *) plan)->hashclauses,
							  &context);
			finalize_primnode((Node *) ((HashJoin *) plan)->hashqualclauses,
							  &context);
			break;

		case T_Motion:

			finalize_primnode((Node *) ((Motion *) plan)->hashExprs,
							  &context);
			break;

		case T_Limit:
			finalize_primnode(((Limit *) plan)->limitOffset,
							  &context);
			finalize_primnode(((Limit *) plan)->limitCount,
							  &context);
			break;

		case T_PartitionSelector:
			/* the paramid in PartitionSelector struct is a special executor param
			 * which is used to do partition pruning in an Append node on the other
			 * side of the join. It can also contain normal executor params in
			 * part_prune_info field.
			 * But all of the params above are only used to compute which partitions
			 * on other side of a join can contain rows that match the join quals.
			 * The tuple from the child plan will pass to the outerplan node directly
			 * after the computation. So the params above won't affect the output of
			 * this plan node.
			 * The params in part_prune_info field still can affect the result of the
			 * outer join, but the params in part_prune_info are also in join qual or
			 * join filter of outer join node, so that these params will be added to
			 * outer join plan's extParam and allParam whatever.
			 * And PartitionSelector node don't support rescan for now, as the above,
			 * don't add the paramids here won't affect the execute result.
			 */

			break;
			
		case T_RecursiveUnion:
			/* child nodes are allowed to reference wtParam */
			locally_added_param = ((RecursiveUnion *) plan)->wtParam;
			valid_params = bms_add_member(bms_copy(valid_params),
										  locally_added_param);
			/* wtParam does *not* get added to scan_params */
			break;

		case T_LockRows:
			/* Force descendant scan nodes to reference epqParam */
			locally_added_param = ((LockRows *) plan)->epqParam;
			valid_params = bms_add_member(bms_copy(valid_params),
										  locally_added_param);
			scan_params = bms_add_member(bms_copy(scan_params),
										 locally_added_param);
			break;

		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;

				/*
				 * AGG_HASHED plans need to know which Params are referenced
				 * in aggregate calls.  Do a separate scan to identify them.
				 */
				if (agg->aggstrategy == AGG_HASHED)
				{
					finalize_primnode_context aggcontext;

					aggcontext.root = root;
					aggcontext.paramids = NULL;
					finalize_agg_primnode((Node *) agg->plan.targetlist,
										  &aggcontext);
					finalize_agg_primnode((Node *) agg->plan.qual,
										  &aggcontext);
					agg->aggParams = aggcontext.paramids;
				}
			}
			break;

		case T_WindowAgg:
			finalize_primnode(((WindowAgg *) plan)->startOffset,
							  &context);
			finalize_primnode(((WindowAgg *) plan)->endOffset,
							  &context);
			break;

		case T_Gather:
			/* child nodes are allowed to reference rescan_param, if any */
			locally_added_param = ((Gather *) plan)->rescan_param;
			if (locally_added_param >= 0)
			{
				valid_params = bms_add_member(bms_copy(valid_params),
											  locally_added_param);

				/*
				 * We currently don't support nested Gathers.  The issue so
				 * far as this function is concerned would be how to identify
				 * which child nodes depend on which Gather.
				 */
				Assert(gather_param < 0);
				/* Pass down rescan_param to child parallel-aware nodes */
				gather_param = locally_added_param;
			}
			/* rescan_param does *not* get added to scan_params */
			break;

		case T_GatherMerge:
			/* child nodes are allowed to reference rescan_param, if any */
			locally_added_param = ((GatherMerge *) plan)->rescan_param;
			if (locally_added_param >= 0)
			{
				valid_params = bms_add_member(bms_copy(valid_params),
											  locally_added_param);

				/*
				 * We currently don't support nested Gathers.  The issue so
				 * far as this function is concerned would be how to identify
				 * which child nodes depend on which Gather.
				 */
				Assert(gather_param < 0);
				/* Pass down rescan_param to child parallel-aware nodes */
				gather_param = locally_added_param;
			}
			/* rescan_param does *not* get added to scan_params */
			break;

		case T_ProjectSet:
		case T_Hash:
		case T_Material:
		case T_Sort:
		case T_ShareInputScan:
		case T_Unique:
		case T_SetOp:
		case T_SplitUpdate:
		case T_TupleSplit:
			/* no node-type-specific fields need fixing */
			break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(plan));
	}

	/* Process left and right child plans, if any */
	/*
	 * In a TableFunctionScan, the 'lefttree' is more like a SubQueryScan's
	 * subplan, and contains a plan that's already been finalized by the
	 * inner invocation of subquery_planner(). So skip that.
	 */
	if (!IsA(plan, TableFunctionScan))
	{
		child_params = finalize_plan(root,
									 plan->lefttree,
									 gather_param,
									 valid_params,
									 scan_params);
		context.paramids = bms_add_members(context.paramids, child_params);
	}

	if (nestloop_params)
	{
		/* right child can reference nestloop_params as well as valid_params */
		child_params = finalize_plan(root,
									 plan->righttree,
									 gather_param,
									 bms_union(nestloop_params, valid_params),
									 scan_params);
		/*
		 * Currently GPDB doesn't fully support lateral, following sql will
		 * pass params by a motion. Then cause panic in QE.
		 * So add a walker to check whether motion in righttree of nestloop
		 * will pass params, if true throw an error to avoid panic in QE.
		 * explain SELECT * FROM
		 * (VALUES (0.0),(10.4),(100.7)) v(nrows),
		 * LATERAL (SELECT count(*) FROM test_tablesample
		 *       TABLESAMPLE system_rows (nrows)) ss;
		 */
		if (IsA(plan, NestLoop) && !bms_is_empty(nestloop_params))
			checkMotionWithParam((Node*) plan->righttree, nestloop_params, root);

		/* ... and they don't count as parameters used at my level */
		child_params = bms_difference(child_params, nestloop_params);
		bms_free(nestloop_params);
	}
	else
	{
		/* easy case */
		child_params = finalize_plan(root,
									 plan->righttree,
									 gather_param,
									 valid_params,
									 scan_params);
	}
	context.paramids = bms_add_members(context.paramids, child_params);

	/*
	 * Any locally generated parameter doesn't count towards its generating
	 * plan node's external dependencies.  (Note: if we changed valid_params
	 * and/or scan_params, we leak those bitmapsets; not worth the notational
	 * trouble to clean them up.)
	 */
	if (locally_added_param >= 0)
	{
		context.paramids = bms_del_member(context.paramids,
										  locally_added_param);
	}

	/* Now we have all the paramids referenced in this node and children */

	if (!bms_is_subset(context.paramids, valid_params))
		elog(ERROR, "plan should not reference subplan's variable");

	/*
	 * The plan node's allParam and extParam fields should include all its
	 * referenced paramids, plus contributions from any child initPlans.
	 * However, any setParams of the initPlans should not be present in the
	 * parent node's extParams, only in its allParams.  (It's possible that
	 * some initPlans have extParams that are setParams of other initPlans.)
	 */

	/* allParam must include initplans' extParams and setParams */
	plan->allParam = bms_union(context.paramids, initExtParam);
	plan->allParam = bms_add_members(plan->allParam, initSetParam);
	/* extParam must include any initplan extParams */
	plan->extParam = bms_union(context.paramids, initExtParam);
	/* but not any initplan setParams */
	plan->extParam = bms_del_members(plan->extParam, initSetParam);

	/*
	 * Currently GPDB doesn't fully support shareinputscan referencing outer
	 * rels.
	 */
	if (IsA(plan, ShareInputScan) && !bms_is_empty(plan->extParam))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("shareinputscan with outer refs is not supported by GPDB")));

	/*
	 * For speed at execution time, make sure extParam/allParam are actually
	 * NULL if they are empty sets.
	 */
	if (bms_is_empty(plan->extParam))
		plan->extParam = NULL;
	if (bms_is_empty(plan->allParam))
		plan->allParam = NULL;

	return plan->allParam;
}

/*
 * finalize_primnode: add IDs of all PARAM_EXEC params appearing in the given
 * expression tree to the result set.
 */
static bool
finalize_primnode(Node *node, finalize_primnode_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		if (((Param *) node)->paramkind == PARAM_EXEC)
		{
			int			paramid = ((Param *) node)->paramid;

			context->paramids = bms_add_member(context->paramids, paramid);
		}
		return false;			/* no more to do here */
	}
	if (IsA(node, SubPlan))
	{
		SubPlan    *subplan = (SubPlan *) node;
		Plan	   *plan = planner_subplan_get_plan(context->root, subplan);
		ListCell   *lc;
		Bitmapset  *subparamids;

		/* Recurse into the testexpr, but not into the Plan */
		finalize_primnode(subplan->testexpr, context);

		/*
		 * Remove any param IDs of output parameters of the subplan that were
		 * referenced in the testexpr.  These are not interesting for
		 * parameter change signaling since we always re-evaluate the subplan.
		 * Note that this wouldn't work too well if there might be uses of the
		 * same param IDs elsewhere in the plan, but that can't happen because
		 * generate_new_exec_param never tries to merge params.
		 */
		foreach(lc, subplan->paramIds)
		{
			context->paramids = bms_del_member(context->paramids,
											   lfirst_int(lc));
		}

		/* Also examine args list */
		finalize_primnode((Node *) subplan->args, context);

		/*
		 * Add params needed by the subplan to paramids, but excluding those
		 * we will pass down to it.  (We assume SS_finalize_plan was run on
		 * the subplan already.)
		 */
		subparamids = bms_copy(plan->extParam);
		foreach(lc, subplan->parParam)
		{
			subparamids = bms_del_member(subparamids, lfirst_int(lc));
		}
		context->paramids = bms_join(context->paramids, subparamids);

		return false;			/* no more to do here */
	}
	return expression_tree_walker(node, finalize_primnode,
								  (void *) context);
}

/*
 * finalize_agg_primnode: find all Aggref nodes in the given expression tree,
 * and add IDs of all PARAM_EXEC params appearing within their aggregated
 * arguments to the result set.
 */
static bool
finalize_agg_primnode(Node *node, finalize_primnode_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		Aggref	   *agg = (Aggref *) node;

		/* we should not consider the direct arguments, if any */
		finalize_primnode((Node *) agg->args, context);
		finalize_primnode((Node *) agg->aggfilter, context);
		return false;			/* there can't be any Aggrefs below here */
	}
	return expression_tree_walker(node, finalize_agg_primnode,
								  (void *) context);
}

/*
 * SS_make_initplan_output_param - make a Param for an initPlan's output
 *
 * The plan is expected to return a scalar value of the given type/collation.
 *
 * Note that in some cases the initplan may not ever appear in the finished
 * plan tree.  If that happens, we'll have wasted a PARAM_EXEC slot, which
 * is no big deal.
 */
Param *
SS_make_initplan_output_param(PlannerInfo *root,
							  Oid resulttype, int32 resulttypmod,
							  Oid resultcollation)
{
	return generate_new_exec_param(root, resulttype,
								   resulttypmod, resultcollation);
}

/*
 * SS_make_initplan_from_plan - given a plan tree, make it an InitPlan
 *
 * We build an EXPR_SUBLINK SubPlan node and put it into the initplan
 * list for the outer query level.  A Param that represents the initplan's
 * output has already been assigned using SS_make_initplan_output_param.
 *
 * We treat root->init_plans like the old PlannerInitPlan global here.
 */
void
SS_make_initplan_from_plan(PlannerInfo *root,
						   PlannerInfo *subroot, Plan *plan,
						   PlanSlice *subslice,
						   Param *prm, bool is_initplan_func_sublink)
{
	SubPlan    *node;

	/*
	 * Add the subplan and its PlannerInfo to the global lists.
	 */
	root->glob->subplans = lappend(root->glob->subplans, plan);
	root->glob->subroots = lappend(root->glob->subroots, subroot);

	/*
	 * Create a SubPlan node and add it to the outer list of InitPlans. Note
	 * it has to appear after any other InitPlans it might depend on (see
	 * comments in ExecReScan).
	 */
	node = makeNode(SubPlan);
	if (is_initplan_func_sublink)
		node->subLinkType = INITPLAN_FUNC_SUBLINK;
	else
		node->subLinkType = EXPR_SUBLINK;
	node->plan_id = list_length(root->glob->subplans);
	node->plan_name = psprintf("InitPlan %d (returns $%d)",
							   node->plan_id, prm->paramid);
	get_first_col_type(plan, &node->firstColType, &node->firstColTypmod,
					   &node->firstColCollation);
	node->is_initplan = true;
	node->setParam = list_make1_int(prm->paramid);

	root->init_plans = lappend(root->init_plans, node);

	/*
	 * The node can't have any inputs (since it's an initplan), so the
	 * parParam and args lists remain empty.
	 */

	/* NB PostgreSQL calculates subplan cost here, but GPDB does it elsewhere. */
}


bool
splan_is_initplan(List *plan_params, SubLinkType subLinkType)
{
	/*
	 * un-correlated or undirect correlated plans of EXISTS, EXPR, ARRAY,
	 * ROWCOMPARE, or MULTIEXPR types can be used as initPlans.
	 */
	if (plan_params == NIL && (
			subLinkType == EXISTS_SUBLINK ||
			subLinkType == EXPR_SUBLINK ||
			subLinkType == ARRAY_SUBLINK ||
			subLinkType == ROWCOMPARE_SUBLINK ||
			subLinkType == MULTIEXPR_SUBLINK
	))
		return true;
	return false;
}
