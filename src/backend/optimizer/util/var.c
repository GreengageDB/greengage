/*-------------------------------------------------------------------------
 *
 * var.c
 *	  Var node manipulation routines
 *
 * Note: for most purposes, PlaceHolderVar is considered a Var too,
 * even if its contained expression is variable-free.  Also, CurrentOfExpr
 * is treated as a Var for purposes of determining whether an expression
 * contains variables.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/var.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup.h"
#include "access/sysattr.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/prep.h"
#include "optimizer/var.h"
#include "optimizer/walkers.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"


typedef struct
{
	Relids		varnos;
	int			sublevels_up;
} pull_varnos_context;

typedef struct
{
	Bitmapset  *varattnos;
	Index		varno;
} pull_varattnos_context;

typedef struct
{
	List	   *vars;
	int			sublevels_up;
} pull_vars_context;

typedef struct
{
	int			var_location;
	int			sublevels_up;
} locate_var_of_level_context;

typedef struct
{
	List	   *varlist;
	PVCAggregateBehavior aggbehavior;
	PVCPlaceHolderBehavior phbehavior;
} pull_var_clause_context;

typedef struct
{
	PlannerInfo *root;
	int			sublevels_up;
	bool		possible_sublink;		    /* could aliases include a SubLink? */
	bool		inserted_sublink;	     	/* have we inserted a SubLink? */
	Node      **root_parse_rtable_arrray;   /* array form of root->parse->rtable */
} flatten_join_alias_vars_context;

static bool pull_varnos_walker(Node *node,
				   pull_varnos_context *context);
static bool pull_varattnos_walker(Node *node, pull_varattnos_context *context);
static bool pull_vars_walker(Node *node, pull_vars_context *context);
static bool contain_var_clause_walker(Node *node, void *context);
static bool contain_vars_of_level_walker(Node *node, int *sublevels_up);
static bool locate_var_of_level_walker(Node *node,
						   locate_var_of_level_context *context);
static bool pull_var_clause_walker(Node *node,
					   pull_var_clause_context *context);
static Node *flatten_join_alias_vars_mutator(Node *node,
								flatten_join_alias_vars_context *context);
static Relids alias_relid_set(PlannerInfo *root, Relids relids);
static Node **rtable_to_array(List *rtable);

/*
 * cdb_walk_vars
 *	  Invoke callback function on each Var and/or Aggref node in an expression.
 *    If a callback returns true, no further nodes are visited, and true is
 *    returned.  Otherwise after visiting all nodes, false is returned.
 *
 * Will recurse into sublinks.	Also, may be invoked directly on a Query.
 */
typedef struct Cdb_walk_vars_context
{
    Cdb_walk_vars_callback_Var      	callback_var;
    Cdb_walk_vars_callback_Aggref   	callback_aggref;
    Cdb_walk_vars_callback_CurrentOf    callback_currentof;
    Cdb_walk_vars_callback_placeholdervar callback_placeholdervar;
    void                           	   *context;
    int                             	sublevelsup;
} Cdb_walk_vars_context;

static bool
cdb_walk_vars_walker(Node *node, void *wvwcontext)
{
    Cdb_walk_vars_context  *ctx = (Cdb_walk_vars_context *)wvwcontext;

	if (node == NULL)
		return false;

    if (IsA(node, Var) &&
        ctx->callback_var != NULL)
		return ctx->callback_var((Var *)node, ctx->context, ctx->sublevelsup);

    if (IsA(node, Aggref) &&
        ctx->callback_aggref != NULL)
        return ctx->callback_aggref((Aggref *)node, ctx->context, ctx->sublevelsup);

    if (IsA(node, CurrentOfExpr) &&
        ctx->callback_currentof != NULL)
        return ctx->callback_currentof((CurrentOfExpr *)node, ctx->context, ctx->sublevelsup);
	
    if (IsA(node, PlaceHolderVar) &&
        ctx->callback_placeholdervar != NULL)
        return ctx->callback_placeholdervar((PlaceHolderVar *)node, ctx->context, ctx->sublevelsup);
	
    if (IsA(node, Query))
	{
		bool    b;

		/* Recurse into subselects */
		ctx->sublevelsup++;
		b = query_tree_walker((Query *)node, cdb_walk_vars_walker, ctx, 0);
		ctx->sublevelsup--;
		return b;
	}
	return expression_tree_walker(node, cdb_walk_vars_walker, ctx);
}                               /* cdb_walk_vars_walker */

bool
cdb_walk_vars(Node                         *node,
              Cdb_walk_vars_callback_Var    callback_var,
              Cdb_walk_vars_callback_Aggref callback_aggref,
              Cdb_walk_vars_callback_CurrentOf callback_currentof,
              Cdb_walk_vars_callback_placeholdervar callback_placeholdervar,
              void                         *context,
              int                           levelsup)
{
	Cdb_walk_vars_context   ctx;

    ctx.callback_var = callback_var;
    ctx.callback_aggref = callback_aggref;
    ctx.callback_currentof = callback_currentof;
    ctx.callback_placeholdervar = callback_placeholdervar;
    ctx.context = context;
    ctx.sublevelsup = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment levelsdown.
	 */
	return query_or_expression_tree_walker(node, cdb_walk_vars_walker, &ctx, 0);
}                               /* cdb_walk_vars */

/*
 * pull_varnos
 *		Create a set of all the distinct varnos present in a parsetree.
 *		Only varnos that reference level-zero rtable entries are considered.
 *
 * NOTE: this is used on not-yet-planned expressions.  It may therefore find
 * bare SubLinks, and if so it needs to recurse into them to look for uplevel
 * references to the desired rtable level!	But when we find a completed
 * SubPlan, we only need to look at the parameters passed to the subplan.
 */

Relids
pull_varnos(Node *node)
{
	return pull_varnos_of_level(node, 0);
}

/*
 * pull_varnos_of_level
 *		Create a set of all the distinct varnos present in a parsetree.
 *		Only Vars of the specified level are considered.
 */
Relids
pull_varnos_of_level(Node *node, int levelsup)
{
	pull_varnos_context context;

	context.varnos = NULL;
	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	query_or_expression_tree_walker(node,
									pull_varnos_walker,
									(void *) &context,
									0);

	return context.varnos;
}

/*
 * CDB
 */
Relids
pull_upper_varnos(Node *node)
{
	return pull_varnos_of_level(node, 1);
}


static bool
pull_varnos_walker(Node *node, pull_varnos_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == context->sublevels_up)
			context->varnos = bms_add_member(context->varnos, var->varno);
		return false;
	}
	if (IsA(node, CurrentOfExpr))
	{
		CurrentOfExpr *cexpr = (CurrentOfExpr *) node;

		if (context->sublevels_up == 0)
			context->varnos = bms_add_member(context->varnos, cexpr->cvarno);
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		/*
		 * A PlaceHolderVar acts as a variable of its syntactic scope, or
		 * lower than that if it references only a subset of the rels in its
		 * syntactic scope.  It might also contain lateral references, but we
		 * should ignore such references when computing the set of varnos in
		 * an expression tree.  Also, if the PHV contains no variables within
		 * its syntactic scope, it will be forced to be evaluated exactly at
		 * the syntactic scope, so take that as the relid set.
		 */
		PlaceHolderVar *phv = (PlaceHolderVar *) node;
		pull_varnos_context subcontext;

		subcontext.varnos = NULL;
		subcontext.sublevels_up = context->sublevels_up;
		(void) pull_varnos_walker((Node *) phv->phexpr, &subcontext);
		if (phv->phlevelsup == context->sublevels_up)
		{
			subcontext.varnos = bms_int_members(subcontext.varnos,
												phv->phrels);
			if (bms_is_empty(subcontext.varnos))
				context->varnos = bms_add_members(context->varnos,
												  phv->phrels);
		}
		context->varnos = bms_join(context->varnos, subcontext.varnos);
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		bool		result;

		context->sublevels_up++;
		result = query_tree_walker((Query *) node, pull_varnos_walker,
								   (void *) context, 0);
		context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node, pull_varnos_walker,
								  (void *) context);
}


/*
 * pull_varattnos
 *		Find all the distinct attribute numbers present in an expression tree,
 *		and add them to the initial contents of *varattnos.
 *		Only Vars of the given varno and rtable level zero are considered.
 *
 * Attribute numbers are offset by FirstLowInvalidHeapAttributeNumber so that
 * we can include system attributes (e.g., OID) in the bitmap representation.
 *
 * Currently, this does not support unplanned subqueries; that is not needed
 * for current uses.  It will handle already-planned SubPlan nodes, though,
 * looking into only the "testexpr" and the "args" list.  (The subplan cannot
 * contain any other references to Vars of the current level.)
 */
void
pull_varattnos(Node *node, Index varno, Bitmapset **varattnos)
{
	pull_varattnos_context context;

	context.varattnos = *varattnos;
	context.varno = varno;

	(void) pull_varattnos_walker(node, &context);

	*varattnos = context.varattnos;
}

static bool
pull_varattnos_walker(Node *node, pull_varattnos_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varno == context->varno && var->varlevelsup == 0)
			context->varattnos =
				bms_add_member(context->varattnos,
						 var->varattno - FirstLowInvalidHeapAttributeNumber);
		return false;
	}

	/* Should not find an unplanned subquery */
	Assert(!IsA(node, Query));

	return expression_tree_walker(node, pull_varattnos_walker,
								  (void *) context);
}

static bool
contain_ctid_var_reference_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		Index		scanrelid = *((Index *) context);

		if (var->varno == scanrelid &&
			var->varattno == SelfItemPointerAttributeNumber &&
			(int)var->varlevelsup == 0)
		{
			return true;		/* abort the tree traversal and return true */
		}
	}
	return expression_tree_walker(node, contain_ctid_var_reference_walker, context);
}

bool
contain_ctid_var_reference(Scan *scan)
{
	Index	   scanrelid = scan->scanrelid;

	/* Check if targetlist contains a var node referencing the ctid column */
	if (expression_tree_walker((Node *) scan->plan.targetlist,
							   contain_ctid_var_reference_walker,
							   &scanrelid))
		return true;

	/* Check if qual contains a var node referencing the ctid column */
	if (expression_tree_walker((Node *) scan->plan.qual,
							   contain_ctid_var_reference_walker,
							   &scanrelid))
		return true;

	return false;
}

/*
 * pull_vars_of_level
 *		Create a list of all Vars (and PlaceHolderVars) referencing the
 *		specified query level in the given parsetree.
 *
 * Caution: the Vars are not copied, only linked into the list.
 */
List *
pull_vars_of_level(Node *node, int levelsup)
{
	pull_vars_context context;

	context.vars = NIL;
	context.sublevels_up = levelsup;

	/*
	 * Must be prepared to start with a Query or a bare expression tree; if
	 * it's a Query, we don't want to increment sublevels_up.
	 */
	query_or_expression_tree_walker(node,
									pull_vars_walker,
									(void *) &context,
									0);

	return context.vars;
}

static bool
pull_vars_walker(Node *node, pull_vars_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == context->sublevels_up)
			context->vars = lappend(context->vars, var);
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) node;

		if (phv->phlevelsup == context->sublevels_up)
			context->vars = lappend(context->vars, phv);
		/* we don't want to look into the contained expression */
		return false;
	}
	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		bool		result;

		context->sublevels_up++;
		result = query_tree_walker((Query *) node, pull_vars_walker,
								   (void *) context, 0);
		context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node, pull_vars_walker,
								  (void *) context);
}


/*
 * contain_var_clause
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  (of the current query level).
 *
 *	  Returns true if any varnode found.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
bool
contain_var_clause(Node *node)
{
	return contain_var_clause_walker(node, NULL);
}

static bool
contain_var_clause_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup == 0)
			return true;		/* abort the tree traversal and return true */
		return false;
	}
	if (IsA(node, CurrentOfExpr))
		return true;
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup == 0)
			return true;		/* abort the tree traversal and return true */
		/* else fall through to check the contained expr */
	}
	return expression_tree_walker(node, contain_var_clause_walker, context);
}


/*
 * contain_vars_of_level
 *	  Recursively scan a clause to discover whether it contains any Var nodes
 *	  of the specified query level.
 *
 *	  Returns true if any such Var found.
 *
 * Will recurse into sublinks.  Also, may be invoked directly on a Query.
 */
bool
contain_vars_of_level(Node *node, int levelsup)
{
	int			sublevels_up = levelsup;

	return query_or_expression_tree_walker(node,
										   contain_vars_of_level_walker,
										   (void *) &sublevels_up,
										   0);
}

static bool
contain_vars_of_level_walker(Node *node, int *sublevels_up)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup == *sublevels_up)
			return true;		/* abort tree traversal and return true */
		return false;
	}
	if (IsA(node, CurrentOfExpr))
	{
		if (*sublevels_up == 0)
			return true;
		return false;
	}
	if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup == *sublevels_up)
			return true;		/* abort the tree traversal and return true */
		/* else fall through to check the contained expr */
	}
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		bool		result;

		(*sublevels_up)++;
		result = query_tree_walker((Query *) node,
								   contain_vars_of_level_walker,
								   (void *) sublevels_up,
								   0);
		(*sublevels_up)--;
		return result;
	}
	return expression_tree_walker(node,
								  contain_vars_of_level_walker,
								  (void *) sublevels_up);
}


/*
 * locate_var_of_level
 *	  Find the parse location of any Var of the specified query level.
 *
 * Returns -1 if no such Var is in the querytree, or if they all have
 * unknown parse location.  (The former case is probably caller error,
 * but we don't bother to distinguish it from the latter case.)
 *
 * Will recurse into sublinks.  Also, may be invoked directly on a Query.
 *
 * Note: it might seem appropriate to merge this functionality into
 * contain_vars_of_level, but that would complicate that function's API.
 * Currently, the only uses of this function are for error reporting,
 * and so shaving cycles probably isn't very important.
 */
int
locate_var_of_level(Node *node, int levelsup)
{
	locate_var_of_level_context context;

	context.var_location = -1;	/* in case we find nothing */
	context.sublevels_up = levelsup;

	(void) query_or_expression_tree_walker(node,
										   locate_var_of_level_walker,
										   (void *) &context,
										   0);

	return context.var_location;
}

static bool
locate_var_of_level_walker(Node *node,
						   locate_var_of_level_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == context->sublevels_up &&
			var->location >= 0)
		{
			context->var_location = var->location;
			return true;		/* abort tree traversal and return true */
		}
		return false;
	}
	if (IsA(node, CurrentOfExpr))
	{
		/* since CurrentOfExpr doesn't carry location, nothing we can do */
		return false;
	}
	/* No extra code needed for PlaceHolderVar; just look in contained expr */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		bool		result;

		context->sublevels_up++;
		result = query_tree_walker((Query *) node,
								   locate_var_of_level_walker,
								   (void *) context,
								   0);
		context->sublevels_up--;
		return result;
	}
	return expression_tree_walker(node,
								  locate_var_of_level_walker,
								  (void *) context);
}

/*
 * contain_vars_of_level_or_above
 *	  Recursively scan a clause to discover whether it contains any Var or
 *    Aggref nodes of the specified query level or above.  For example,
 *    pass 1 to detect all nonlocal Vars.
 *
 *	  Returns true if any such Var found.
 *
 * Will recurse into sublinks.	Also, may be invoked directly on a Query.
 */
static bool
contain_vars_of_level_or_above_cbVar(Var *var, void *unused, int sublevelsup)
{
	if ((int)var->varlevelsup >= sublevelsup)
		return true;		    /* abort tree traversal and return true */
    return false;
}

static bool
contain_vars_of_level_or_above_cbAggref(Aggref *aggref, void *unused, int sublevelsup)
{
	if ((int)aggref->agglevelsup >= sublevelsup)
        return true;

    /* visit aggregate's args */
	return cdb_walk_vars((Node *)aggref->args,
                         contain_vars_of_level_or_above_cbVar,
                         contain_vars_of_level_or_above_cbAggref,
						 NULL,
                         NULL,
                         NULL, // GPDB_84_MERGE_FIXME: Can arguments of Aggref contain PlaceHolderVars ?
                         sublevelsup);
}

bool
contain_vars_of_level_or_above_cbPlaceHolderVar(PlaceHolderVar *placeholdervar, void *unused, int sublevelsup)
{
	if(placeholdervar->phlevelsup >= sublevelsup)
		return true;

	/* visit placeholder's contained expression */
	return cdb_walk_vars((Node*)placeholdervar->phexpr,
						 contain_vars_of_level_or_above_cbVar,
						 contain_vars_of_level_or_above_cbAggref,
						 NULL,
						 contain_vars_of_level_or_above_cbPlaceHolderVar,
						 NULL,
						 sublevelsup);
}

bool
contain_vars_of_level_or_above(Node *node, int levelsup)
{
	return cdb_walk_vars(node,
                         contain_vars_of_level_or_above_cbVar,
                         contain_vars_of_level_or_above_cbAggref,
						 NULL,
                         contain_vars_of_level_or_above_cbPlaceHolderVar,
                         NULL,
                         levelsup);
}

/*
 * pull_var_clause
 *	  Recursively pulls all Var nodes from an expression clause.
 *
 *	  Aggrefs are handled according to 'aggbehavior':
 *		PVC_REJECT_AGGREGATES		throw error if Aggref found
 *		PVC_INCLUDE_AGGREGATES		include Aggrefs in output list
 *		PVC_RECURSE_AGGREGATES		recurse into Aggref arguments
 *	  Vars within an Aggref's expression are included only in the last case.
 *
 *	  PlaceHolderVars are handled according to 'phbehavior':
 *		PVC_REJECT_PLACEHOLDERS		throw error if PlaceHolderVar found
 *		PVC_INCLUDE_PLACEHOLDERS	include PlaceHolderVars in output list
 *		PVC_RECURSE_PLACEHOLDERS	recurse into PlaceHolderVar arguments
 *	  Vars within a PHV's expression are included only in the last case.
 *
 *	  CurrentOfExpr nodes are ignored in all cases.
 *
 *	  Upper-level vars (with varlevelsup > 0) should not be seen here,
 *	  likewise for upper-level Aggrefs and PlaceHolderVars.
 *
 *	  Returns list of nodes found.  Note the nodes themselves are not
 *	  copied, only referenced.
 *
 * Does not examine subqueries, therefore must only be used after reduction
 * of sublinks to subplans!
 */
List *
pull_var_clause(Node *node, PVCAggregateBehavior aggbehavior,
				PVCPlaceHolderBehavior phbehavior)
{
	pull_var_clause_context context;

	context.varlist = NIL;
	context.aggbehavior = aggbehavior;
	context.phbehavior = phbehavior;

	pull_var_clause_walker(node, &context);
	return context.varlist;
}

static bool
pull_var_clause_walker(Node *node, pull_var_clause_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup != 0)
			elog(ERROR, "Upper-level Var found where not expected");
		context->varlist = lappend(context->varlist, node);
		return false;
	}
	else if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup != 0)
			elog(ERROR, "Upper-level Aggref found where not expected");
		switch (context->aggbehavior)
		{
			case PVC_REJECT_AGGREGATES:
				elog(ERROR, "Aggref found where not expected");
				break;
			case PVC_INCLUDE_AGGREGATES:
				context->varlist = lappend(context->varlist, node);
				/* we do NOT descend into the contained expression */
				return false;
			case PVC_RECURSE_AGGREGATES:
				/* ignore the aggregate, look at its argument instead */
				break;
		}
	}
	else if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup != 0)
			elog(ERROR, "Upper-level PlaceHolderVar found where not expected");
		switch (context->phbehavior)
		{
			case PVC_REJECT_PLACEHOLDERS:
				elog(ERROR, "PlaceHolderVar found where not expected");
				break;
			case PVC_INCLUDE_PLACEHOLDERS:
				context->varlist = lappend(context->varlist, node);
				/* we do NOT descend into the contained expression */
				return false;
			case PVC_RECURSE_PLACEHOLDERS:
				/* ignore the placeholder, look at its argument instead */
				break;
		}
	}
	return expression_tree_walker(node, pull_var_clause_walker,
								  (void *) context);
}


/*
 * flatten_join_alias_vars
 *	  Replace Vars that reference JOIN outputs with references to the original
 *	  relation variables instead.  This allows quals involving such vars to be
 *	  pushed down.  Whole-row Vars that reference JOIN relations are expanded
 *	  into RowExpr constructs that name the individual output Vars.  This
 *	  is necessary since we will not scan the JOIN as a base relation, which
 *	  is the only way that the executor can directly handle whole-row Vars.
 *
 * This also adjusts relid sets found in some expression node types to
 * substitute the contained base rels for any join relid.
 *
 * If a JOIN contains sub-selects that have been flattened, its join alias
 * entries might now be arbitrary expressions, not just Vars.  This affects
 * this function in one important way: we might find ourselves inserting
 * SubLink expressions into subqueries, and we must make sure that their
 * Query.hasSubLinks fields get set to TRUE if so.  If there are any
 * SubLinks in the join alias lists, the outer Query should already have
 * hasSubLinks = TRUE, so this is only relevant to un-flattened subqueries.
 *
 * NOTE: this is used on not-yet-planned expressions.  We do not expect it
 * to be applied directly to the whole Query, so if we see a Query to start
 * with, we do want to increment sublevels_up (this occurs for LATERAL
 * subqueries).
 */
Node *
flatten_join_alias_vars(PlannerInfo *root, Node *node)
{
	flatten_join_alias_vars_context context;

	context.root = root;
	context.sublevels_up = 0;
	/* flag whether join aliases could possibly contain SubLinks */
	context.possible_sublink = root->parse->hasSubLinks;
	/* if hasSubLinks is already true, no need to work hard */
	context.inserted_sublink = root->parse->hasSubLinks;

	/*
	 * The following funcation call flatten_join_alias_vars_mutator()
	 * will walk the expr and it will frequently access root
	 * parse tree's rtable using list_nth. When the rtable is huge,
	 * performance is poor. Here we cache the rtable list into array
	 * to achieve random access to speed up a lot when rtable is huge.
	 * See Github issue https://github.com/GreengageDB/greengage/issues/11379
	 * for details.
	 */
	context.root_parse_rtable_arrray = rtable_to_array(root->parse->rtable);

	return flatten_join_alias_vars_mutator(node, &context);
}

static Node *
flatten_join_alias_vars_mutator(Node *node,
								flatten_join_alias_vars_context *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		RangeTblEntry *rte;
		Node	   *newvar;

		/* No change unless Var belongs to a JOIN of the target level */
		if (var->varlevelsup != context->sublevels_up)
			return node;		/* no need to copy, really */
		rte = (RangeTblEntry *) (context->root_parse_rtable_arrray[var->varno-1]);
		if (rte->rtekind != RTE_JOIN)
			return node;
		if (var->varattno == InvalidAttrNumber)
		{
			/* Must expand whole-row reference */
			RowExpr    *rowexpr;
			List	   *fields = NIL;
			List	   *colnames = NIL;
			AttrNumber	attnum;
			ListCell   *lv;
			ListCell   *ln;

			attnum = 0;
			Assert(list_length(rte->joinaliasvars) == list_length(rte->eref->colnames));
			forboth(lv, rte->joinaliasvars, ln, rte->eref->colnames)
			{
				newvar = (Node *) lfirst(lv);
				attnum++;
				/* Ignore dropped columns */
				if (newvar == NULL)
					continue;
				newvar = copyObject(newvar);

				/*
				 * If we are expanding an alias carried down from an upper
				 * query, must adjust its varlevelsup fields.
				 */
				if (context->sublevels_up != 0)
					IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);
				/* Preserve original Var's location, if possible */
				if (IsA(newvar, Var))
					((Var *) newvar)->location = var->location;
				/* Recurse in case join input is itself a join */
				/* (also takes care of setting inserted_sublink if needed) */
				newvar = flatten_join_alias_vars_mutator(newvar, context);
				fields = lappend(fields, newvar);
				/* We need the names of non-dropped columns, too */
				colnames = lappend(colnames, copyObject((Node *) lfirst(ln)));
			}
			rowexpr = makeNode(RowExpr);
			rowexpr->args = fields;
			rowexpr->row_typeid = var->vartype;
			rowexpr->row_format = COERCE_IMPLICIT_CAST;
			rowexpr->colnames = colnames;
			rowexpr->location = var->location;

			return (Node *) rowexpr;
		}

		/* Expand join alias reference */
		Assert(var->varattno > 0);
		newvar = (Node *) list_nth(rte->joinaliasvars, var->varattno - 1);
		Assert(newvar != NULL);
		newvar = copyObject(newvar);

		/*
		 * If we are expanding an alias carried down from an upper query, must
		 * adjust its varlevelsup fields.
		 */
		if (context->sublevels_up != 0)
			IncrementVarSublevelsUp(newvar, context->sublevels_up, 0);

		/* Preserve original Var's location, if possible */
		if (IsA(newvar, Var))
			((Var *) newvar)->location = var->location;

		/* Recurse in case join input is itself a join */
		newvar = flatten_join_alias_vars_mutator(newvar, context);

		/* Detect if we are adding a sublink to query */
		if (context->possible_sublink && !context->inserted_sublink)
			context->inserted_sublink = checkExprHasSubLink(newvar);

		return newvar;
	}
	if (IsA(node, PlaceHolderVar))
	{
		/* Copy the PlaceHolderVar node with correct mutation of subnodes */
		PlaceHolderVar *phv;

		phv = (PlaceHolderVar *) expression_tree_mutator(node,
											 flatten_join_alias_vars_mutator,
														 (void *) context);
		/* now fix PlaceHolderVar's relid sets */
		if (phv->phlevelsup == context->sublevels_up)
		{
			phv->phrels = alias_relid_set(context->root,
										  phv->phrels);
		}
		return (Node *) phv;
	}

	if (IsA(node, Query))
	{
		/* Recurse into RTE subquery or not-yet-planned sublink subquery */
		Query	   *newnode;
		bool		save_inserted_sublink;

		context->sublevels_up++;
		save_inserted_sublink = context->inserted_sublink;
		context->inserted_sublink = ((Query *) node)->hasSubLinks;
		newnode = query_tree_mutator((Query *) node,
									 flatten_join_alias_vars_mutator,
									 (void *) context,
									 QTW_IGNORE_JOINALIASES);
		newnode->hasSubLinks |= context->inserted_sublink;
		context->inserted_sublink = save_inserted_sublink;
		context->sublevels_up--;
		return (Node *) newnode;
	}
	/* Already-planned tree not supported */
	Assert(!IsA(node, SubPlan));
	/* Shouldn't need to handle these planner auxiliary nodes here */
	Assert(!IsA(node, SpecialJoinInfo));
	Assert(!IsA(node, LateralJoinInfo));
	Assert(!IsA(node, PlaceHolderInfo));
	Assert(!IsA(node, MinMaxAggInfo));

	return expression_tree_mutator(node, flatten_join_alias_vars_mutator,
								   (void *) context);
}

/*
 * alias_relid_set: in a set of RT indexes, replace joins by their
 * underlying base relids
 */
static Relids
alias_relid_set(PlannerInfo *root, Relids relids)
{
	Relids		result = NULL;
	Relids		tmprelids;
	int			rtindex;

	tmprelids = bms_copy(relids);
	while ((rtindex = bms_first_member(tmprelids)) >= 0)
	{
		RangeTblEntry *rte = rt_fetch(rtindex, root->parse->rtable);

		if (rte->rtekind == RTE_JOIN)
			result = bms_join(result, get_relids_for_join(root, rtindex));
		else
			result = bms_add_member(result, rtindex);
	}
	bms_free(tmprelids);
	return result;
}

/*
 * Turn parse tree's rtable to array.
 * This can speed up a lot when it uses list_nth
 * very often and the rtable is huge.
 */
static Node **
rtable_to_array(List *rtable)
{
	ListCell   *lc;
	int         i;
	Node      **arr = (Node **)palloc(sizeof(Node*) * list_length(rtable));

	foreach_with_count(lc, rtable, i)
	{
		arr[i] = (Node *) lfirst(lc);
	}

	return arr;
}
