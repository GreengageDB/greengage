/*-------------------------------------------------------------------------
 *
 * pathnode.c
 *	  Routines to manipulate pathlists and create path nodes
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/pathnode.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"
#include "foreign/fdwapi.h"
#include "nodes/extensible.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "optimizer/tlist.h"

#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "cdb/cdbhash.h"        /* cdb_default_distribution_opfamily_for_type() */
#include "cdb/cdbmutate.h"
#include "cdb/cdbpath.h"        /* cdb_create_motion_path() etc */
#include "cdb/cdbpathlocus.h"
#include "cdb/cdbutil.h"		/* getgpsegmentCount() */
#include "cdb/cdbvars.h"
#include "executor/nodeHash.h"
#include "utils/guc.h"

typedef enum
{
	COSTS_EQUAL,				/* path costs are fuzzily equal */
	COSTS_BETTER1,				/* first path is cheaper than second */
	COSTS_BETTER2,				/* second path is cheaper than first */
	COSTS_DIFFERENT				/* neither path dominates the other on cost */
} PathCostComparison;

/*
 * STD_FUZZ_FACTOR is the normal fuzz factor for compare_path_costs_fuzzily.
 * XXX is it worth making this user-controllable?  It provides a tradeoff
 * between planner runtime and the accuracy of path cost comparisons.
 */
#define STD_FUZZ_FACTOR 1.01

static List *translate_sub_tlist(List *tlist, int relid);
static int	append_total_cost_compare(const void *a, const void *b);
static int	append_startup_cost_compare(const void *a, const void *b);
static List *reparameterize_pathlist_by_child(PlannerInfo *root,
											  List *pathlist,
											  RelOptInfo *child_rel);

static bool set_append_path_locus(PlannerInfo *root, Path *pathnode, RelOptInfo *rel,
					  List *pathkeys);
static CdbPathLocus
adjust_modifytable_subpaths(PlannerInfo *root, CmdType operation,
							List *resultRelations, List *subpaths,
							List *is_split_updates);

/*****************************************************************************
 *		MISC. PATH UTILITIES
 *****************************************************************************/

/*
 * compare_path_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for the specified criterion.
 */
int
compare_path_costs(Path *path1, Path *path2, CostSelector criterion)
{
	if (criterion == STARTUP_COST)
	{
		if (path1->startup_cost < path2->startup_cost)
			return -1;
		if (path1->startup_cost > path2->startup_cost)
			return +1;

		/*
		 * If paths have the same startup cost (not at all unlikely), order
		 * them by total cost.
		 */
		if (path1->total_cost < path2->total_cost)
			return -1;
		if (path1->total_cost > path2->total_cost)
			return +1;
	}
	else
	{
		if (path1->total_cost < path2->total_cost)
			return -1;
		if (path1->total_cost > path2->total_cost)
			return +1;

		/*
		 * If paths have the same total cost, order them by startup cost.
		 */
		if (path1->startup_cost < path2->startup_cost)
			return -1;
		if (path1->startup_cost > path2->startup_cost)
			return +1;
	}
	return 0;
}

/*
 * compare_path_fractional_costs
 *	  Return -1, 0, or +1 according as path1 is cheaper, the same cost,
 *	  or more expensive than path2 for fetching the specified fraction
 *	  of the total tuples.
 *
 * If fraction is <= 0 or > 1, we interpret it as 1, ie, we select the
 * path with the cheaper total_cost.
 */
int
compare_fractional_path_costs(Path *path1, Path *path2,
							  double fraction)
{
	Cost		cost1,
				cost2;

	if (fraction <= 0.0 || fraction >= 1.0)
		return compare_path_costs(path1, path2, TOTAL_COST);
	cost1 = path1->startup_cost +
		fraction * (path1->total_cost - path1->startup_cost);
	cost2 = path2->startup_cost +
		fraction * (path2->total_cost - path2->startup_cost);
	if (cost1 < cost2)
		return -1;
	if (cost1 > cost2)
		return +1;
	return 0;
}

/*
 * compare_path_costs_fuzzily
 *	  Compare the costs of two paths to see if either can be said to
 *	  dominate the other.
 *
 * We use fuzzy comparisons so that add_path() can avoid keeping both of
 * a pair of paths that really have insignificantly different cost.
 *
 * The fuzz_factor argument must be 1.0 plus delta, where delta is the
 * fraction of the smaller cost that is considered to be a significant
 * difference.  For example, fuzz_factor = 1.01 makes the fuzziness limit
 * be 1% of the smaller cost.
 *
 * The two paths are said to have "equal" costs if both startup and total
 * costs are fuzzily the same.  Path1 is said to be better than path2 if
 * it has fuzzily better startup cost and fuzzily no worse total cost,
 * or if it has fuzzily better total cost and fuzzily no worse startup cost.
 * Path2 is better than path1 if the reverse holds.  Finally, if one path
 * is fuzzily better than the other on startup cost and fuzzily worse on
 * total cost, we just say that their costs are "different", since neither
 * dominates the other across the whole performance spectrum.
 *
 * This function also enforces a policy rule that paths for which the relevant
 * one of parent->consider_startup and parent->consider_param_startup is false
 * cannot survive comparisons solely on the grounds of good startup cost, so
 * we never return COSTS_DIFFERENT when that is true for the total-cost loser.
 * (But if total costs are fuzzily equal, we compare startup costs anyway,
 * in hopes of eliminating one path or the other.)
 */
static PathCostComparison
compare_path_costs_fuzzily(Path *path1, Path *path2, double fuzz_factor)
{
#define CONSIDER_PATH_STARTUP_COST(p)  \
	((p)->param_info == NULL ? (p)->parent->consider_startup : (p)->parent->consider_param_startup)

	/*
	 * Check total cost first since it's more likely to be different; many
	 * paths have zero startup cost.
	 */
	if (path1->total_cost > path2->total_cost * fuzz_factor)
	{
		/* path1 fuzzily worse on total cost */
		if (CONSIDER_PATH_STARTUP_COST(path1) &&
			path2->startup_cost > path1->startup_cost * fuzz_factor)
		{
			/* ... but path2 fuzzily worse on startup, so DIFFERENT */
			return COSTS_DIFFERENT;
		}
		/* else path2 dominates */
		return COSTS_BETTER2;
	}
	if (path2->total_cost > path1->total_cost * fuzz_factor)
	{
		/* path2 fuzzily worse on total cost */
		if (CONSIDER_PATH_STARTUP_COST(path2) &&
			path1->startup_cost > path2->startup_cost * fuzz_factor)
		{
			/* ... but path1 fuzzily worse on startup, so DIFFERENT */
			return COSTS_DIFFERENT;
		}
		/* else path1 dominates */
		return COSTS_BETTER1;
	}
	/* fuzzily the same on total cost ... */
	if (path1->startup_cost > path2->startup_cost * fuzz_factor)
	{
		/* ... but path1 fuzzily worse on startup, so path2 wins */
		return COSTS_BETTER2;
	}
	if (path2->startup_cost > path1->startup_cost * fuzz_factor)
	{
		/* ... but path2 fuzzily worse on startup, so path1 wins */
		return COSTS_BETTER1;
	}
	/* fuzzily the same on both costs */
	return COSTS_EQUAL;

#undef CONSIDER_PATH_STARTUP_COST
}

/*
 * set_cheapest
 *	  Find the minimum-cost paths from among a relation's paths,
 *	  and save them in the rel's cheapest-path fields.
 *
 * cheapest_total_path is normally the cheapest-total-cost unparameterized
 * path; but if there are no unparameterized paths, we assign it to be the
 * best (cheapest least-parameterized) parameterized path.  However, only
 * unparameterized paths are considered candidates for cheapest_startup_path,
 * so that will be NULL if there are no unparameterized paths.
 *
 * The cheapest_parameterized_paths list collects all parameterized paths
 * that have survived the add_path() tournament for this relation.  (Since
 * add_path ignores pathkeys for a parameterized path, these will be paths
 * that have best cost or best row count for their parameterization.  We
 * may also have both a parallel-safe and a non-parallel-safe path in some
 * cases for the same parameterization in some cases, but this should be
 * relatively rare since, most typically, all paths for the same relation
 * will be parallel-safe or none of them will.)
 *
 * cheapest_parameterized_paths always includes the cheapest-total
 * unparameterized path, too, if there is one; the users of that list find
 * it more convenient if that's included.
 *
 * This is normally called only after we've finished constructing the path
 * list for the rel node.
 */
void
set_cheapest(RelOptInfo *parent_rel)
{
	Path	   *cheapest_startup_path;
	Path	   *cheapest_total_path;
	Path	   *best_param_path;
	List	   *parameterized_paths;
	ListCell   *p;

	Assert(IsA(parent_rel, RelOptInfo));

	if (parent_rel->pathlist == NIL)
		elog(ERROR, "could not devise a query plan for the given query");

	cheapest_startup_path = cheapest_total_path = best_param_path = NULL;
	parameterized_paths = NIL;

	foreach(p, parent_rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(p);
		int			cmp;

		if (path->param_info)
		{
			/* Parameterized path, so add it to parameterized_paths */
			parameterized_paths = lappend(parameterized_paths, path);

			/*
			 * If we have an unparameterized cheapest-total, we no longer care
			 * about finding the best parameterized path, so move on.
			 */
			if (cheapest_total_path)
				continue;

			/*
			 * Otherwise, track the best parameterized path, which is the one
			 * with least total cost among those of the minimum
			 * parameterization.
			 */
			if (best_param_path == NULL)
				best_param_path = path;
			else
			{
				switch (bms_subset_compare(PATH_REQ_OUTER(path),
										   PATH_REQ_OUTER(best_param_path)))
				{
					case BMS_EQUAL:
						/* keep the cheaper one */
						if (compare_path_costs(path, best_param_path,
											   TOTAL_COST) < 0)
							best_param_path = path;
						break;
					case BMS_SUBSET1:
						/* new path is less-parameterized */
						best_param_path = path;
						break;
					case BMS_SUBSET2:
						/* old path is less-parameterized, keep it */
						break;
					case BMS_DIFFERENT:

						/*
						 * This means that neither path has the least possible
						 * parameterization for the rel.  We'll sit on the old
						 * path until something better comes along.
						 */
						break;
				}
			}
		}
		else
		{
			/* Unparameterized path, so consider it for cheapest slots */
			if (cheapest_total_path == NULL)
			{
				cheapest_startup_path = cheapest_total_path = path;
				continue;
			}

			/*
			 * If we find two paths of identical costs, try to keep the
			 * better-sorted one.  The paths might have unrelated sort
			 * orderings, in which case we can only guess which might be
			 * better to keep, but if one is superior then we definitely
			 * should keep that one.
			 */
			cmp = compare_path_costs(cheapest_startup_path, path, STARTUP_COST);
			if (cmp > 0 ||
				(cmp == 0 &&
				 compare_pathkeys(cheapest_startup_path->pathkeys,
								  path->pathkeys) == PATHKEYS_BETTER2))
				cheapest_startup_path = path;

			cmp = compare_path_costs(cheapest_total_path, path, TOTAL_COST);
			if (cmp > 0 ||
				(cmp == 0 &&
				 compare_pathkeys(cheapest_total_path->pathkeys,
								  path->pathkeys) == PATHKEYS_BETTER2))
				cheapest_total_path = path;
		}
	}

	/* Add cheapest unparameterized path, if any, to parameterized_paths */
	if (cheapest_total_path)
		parameterized_paths = lcons(cheapest_total_path, parameterized_paths);

	/*
	 * If there is no unparameterized path, use the best parameterized path as
	 * cheapest_total_path (but not as cheapest_startup_path).
	 */
	if (cheapest_total_path == NULL)
		cheapest_total_path = best_param_path;
	Assert(cheapest_total_path != NULL);

	parent_rel->cheapest_startup_path = cheapest_startup_path;
	parent_rel->cheapest_total_path = cheapest_total_path;
	parent_rel->cheapest_unique_path = NULL;	/* computed only if needed */
	parent_rel->cheapest_parameterized_paths = parameterized_paths;
}

/*
 * add_path
 *	  Consider a potential implementation path for the specified parent rel,
 *	  and add it to the rel's pathlist if it is worthy of consideration.
 *	  A path is worthy if it has a better sort order (better pathkeys) or
 *	  cheaper cost (on either dimension), or generates fewer rows, than any
 *	  existing path that has the same or superset parameterization rels.
 *	  We also consider parallel-safe paths more worthy than others.
 *
 *	  We also remove from the rel's pathlist any old paths that are dominated
 *	  by new_path --- that is, new_path is cheaper, at least as well ordered,
 *	  generates no more rows, requires no outer rels not required by the old
 *	  path, and is no less parallel-safe.
 *
 *	  In most cases, a path with a superset parameterization will generate
 *	  fewer rows (since it has more join clauses to apply), so that those two
 *	  figures of merit move in opposite directions; this means that a path of
 *	  one parameterization can seldom dominate a path of another.  But such
 *	  cases do arise, so we make the full set of checks anyway.
 *
 *	  There are two policy decisions embedded in this function, along with
 *	  its sibling add_path_precheck.  First, we treat all parameterized paths
 *	  as having NIL pathkeys, so that they cannot win comparisons on the
 *	  basis of sort order.  This is to reduce the number of parameterized
 *	  paths that are kept; see discussion in src/backend/optimizer/README.
 *
 *	  Second, we only consider cheap startup cost to be interesting if
 *	  parent_rel->consider_startup is true for an unparameterized path, or
 *	  parent_rel->consider_param_startup is true for a parameterized one.
 *	  Again, this allows discarding useless paths sooner.
 *
 *	  The pathlist is kept sorted by total_cost, with cheaper paths
 *	  at the front.  Within this routine, that's simply a speed hack:
 *	  doing it that way makes it more likely that we will reject an inferior
 *	  path after a few comparisons, rather than many comparisons.
 *	  However, add_path_precheck relies on this ordering to exit early
 *	  when possible.
 *
 *	  NOTE: discarded Path objects are immediately pfree'd to reduce planner
 *	  memory consumption.  We dare not try to free the substructure of a Path,
 *	  since much of it may be shared with other Paths or the query tree itself;
 *	  but just recycling discarded Path nodes is a very useful savings in
 *	  a large join tree.  We can recycle the List nodes of pathlist, too.
 *
 *	  NB: The Path that is passed to add_path() must be considered invalid
 *	  upon return, and not touched again by the caller, because we free it
 *	  if we already know of a better path.  Likewise, a Path that is passed
 *	  to add_path() must not be shared as a subpath of any other Path of the
 *	  same join level.
 *
 *	  As noted in optimizer/README, deleting a previously-accepted Path is
 *	  safe because we know that Paths of this rel cannot yet be referenced
 *	  from any other rel, such as a higher-level join.  However, in some cases
 *	  it is possible that a Path is referenced by another Path for its own
 *	  rel; we must not delete such a Path, even if it is dominated by the new
 *	  Path.  Currently this occurs only for IndexPath objects, which may be
 *	  referenced as children of BitmapHeapPaths as well as being paths in
 *	  their own right.  Hence, we don't pfree IndexPaths when rejecting them.
 *
 * 'parent_rel' is the relation entry to which the path corresponds.
 * 'new_path' is a potential path for parent_rel.
 *
 * Returns nothing, but modifies parent_rel->pathlist.
 */
void
add_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;	/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	List	   *new_path_pathkeys;
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;

	/*
	 * This is a convenient place to check for query cancel --- no part of the
	 * planner goes very long without calling add_path().
	 */
	CHECK_FOR_INTERRUPTS();

	if (!new_path)
		return;

	/*
	 * GPDB: Check that the correct locus has been determined for the Path.
	 * This can easily be missing from upstream code that construct Paths
	 * that haven't been modified in GPDB to set the locus correctly.
	 */
	if (!CdbLocusType_IsValid(new_path->locus.locustype))
		elog(ERROR, "path of type %u is missing distribution locus", new_path->pathtype);
	Assert(cdbpathlocus_is_valid(new_path->locus));

	/* Pretend parameterized paths have no pathkeys, per comment above */
	new_path_pathkeys = new_path->param_info ? NIL : new_path->pathkeys;

	/*
	 * Loop to check proposed new path against old paths.  Note it is possible
	 * for more than one old path to be tossed out because new_path dominates
	 * it.
	 *
	 * We can't use foreach here because the loop body may delete the current
	 * list cell.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->pathlist); p1 != NULL; p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		PathCostComparison costcmp;
		PathKeysComparison keyscmp;
		BMS_Comparison outercmp;

		p1_next = lnext(p1);

		/*
		 * Do a fuzzy cost comparison with standard fuzziness limit.
		 */
		costcmp = compare_path_costs_fuzzily(new_path, old_path,
											 STD_FUZZ_FACTOR);

		/*
		 * If the two paths compare differently for startup and total cost,
		 * then we want to keep both, and we can skip comparing pathkeys and
		 * required_outer rels.  If they compare the same, proceed with the
		 * other comparisons.  Row count is checked last.  (We make the tests
		 * in this order because the cost comparison is most likely to turn
		 * out "different", and the pathkeys comparison next most likely.  As
		 * explained above, row count very seldom makes a difference, so even
		 * though it's cheap to compare there's not much point in checking it
		 * earlier.)
		 */
		if (costcmp != COSTS_DIFFERENT)
		{
			/* Similarly check to see if either dominates on pathkeys */
			List	   *old_path_pathkeys;

			old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
			keyscmp = compare_pathkeys(new_path_pathkeys,
									   old_path_pathkeys);

			/*
			 * GPDB: If the new path has different locus than the other path,
			 * keep it, like we keep paths with different pathkeys. We can
			 * avoid the (Gather) Motion at the top of the plan, if we choose
			 * a plan that produces the result at the right locus to begin
			 * with. In particular, if it's a two-stage aggregate plan, it might
			 * be cheaper to perform the Finalize Aggregate stage in the QD than
			 * redistribute it to all segments, if that avoids a Gather Motion
			 * at the top.
			 *
			 * Only do this for the "upper rels". The join planning code hasn't
			 * been updated to consider plans with multiple loci. Keeping extra
			 * paths might be a win, but it might also lead to erratic behavior.
			 * For example, a Hash Join only considers the cheapest input paths,
			 * but a Merge Join would consider all paths with sorted input. A
			 * path with a suitable locus migh therefore win with a Merge Join
			 * but not even be considered a Hash Join, even though the Hash Join
			 * path would be cheaper.
			 *
			 * Parts of the upper planner functions could have similar issues,
			 * but it seems more limited in scope.
			 */
			if (keyscmp != PATHKEYS_DIFFERENT &&
				parent_rel->reloptkind == RELOPT_UPPER_REL &&
				!cdbpathlocus_equal(new_path->locus, old_path->locus))
			{
				keyscmp = PATHKEYS_DIFFERENT;
			}

			if (keyscmp != PATHKEYS_DIFFERENT)
			{
				switch (costcmp)
				{
					case COSTS_EQUAL:
						outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
													  PATH_REQ_OUTER(old_path));
						if (keyscmp == PATHKEYS_BETTER1)
						{
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET1) &&
								new_path->rows <= old_path->rows &&
								new_path->parallel_safe >= old_path->parallel_safe)
								remove_old = true;	/* new dominates old */
						}
						else if (keyscmp == PATHKEYS_BETTER2)
						{
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET2) &&
								new_path->rows >= old_path->rows &&
								new_path->parallel_safe <= old_path->parallel_safe)
								accept_new = false; /* old dominates new */
						}
						else	/* keyscmp == PATHKEYS_EQUAL */
						{
							if (outercmp == BMS_EQUAL)
							{
								/*
								 * Same pathkeys and outer rels, and fuzzily
								 * the same cost, so keep just one; to decide
								 * which, first check parallel-safety, then
								 * rows, then do a fuzzy cost comparison with
								 * very small fuzz limit.  (We used to do an
								 * exact cost comparison, but that results in
								 * annoying platform-specific plan variations
								 * due to roundoff in the cost estimates.)	If
								 * things are still tied, arbitrarily keep
								 * only the old path.  Notice that we will
								 * keep only the old path even if the
								 * less-fuzzy comparison decides the startup
								 * and total costs compare differently.
								 */
								if (new_path->parallel_safe >
									old_path->parallel_safe)
									remove_old = true;	/* new dominates old */
								else if (new_path->parallel_safe <
										 old_path->parallel_safe)
									accept_new = false; /* old dominates new */
								else if (new_path->rows < old_path->rows)
									remove_old = true;	/* new dominates old */
								else if (new_path->rows > old_path->rows)
									accept_new = false; /* old dominates new */
								else if (compare_path_costs_fuzzily(new_path,
																	old_path,
																	1.0000000001) == COSTS_BETTER1)
									remove_old = true;	/* new dominates old */
								else
									accept_new = false; /* old equals or
														 * dominates new */
							}
							else if (outercmp == BMS_SUBSET1 &&
									 new_path->rows <= old_path->rows &&
									 new_path->parallel_safe >= old_path->parallel_safe)
								remove_old = true;	/* new dominates old */
							else if (outercmp == BMS_SUBSET2 &&
									 new_path->rows >= old_path->rows &&
									 new_path->parallel_safe <= old_path->parallel_safe)
								accept_new = false; /* old dominates new */
							/* else different parameterizations, keep both */
						}
						break;
					case COSTS_BETTER1:
						if (keyscmp != PATHKEYS_BETTER2)
						{
							outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
														  PATH_REQ_OUTER(old_path));
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET1) &&
								new_path->rows <= old_path->rows &&
								new_path->parallel_safe >= old_path->parallel_safe)
								remove_old = true;	/* new dominates old */
						}
						break;
					case COSTS_BETTER2:
						if (keyscmp != PATHKEYS_BETTER1)
						{
							outercmp = bms_subset_compare(PATH_REQ_OUTER(new_path),
														  PATH_REQ_OUTER(old_path));
							if ((outercmp == BMS_EQUAL ||
								 outercmp == BMS_SUBSET2) &&
								new_path->rows >= old_path->rows &&
								new_path->parallel_safe <= old_path->parallel_safe)
								accept_new = false; /* old dominates new */
						}
						break;
					case COSTS_DIFFERENT:

						/*
						 * can't get here, but keep this case to keep compiler
						 * quiet
						 */
						break;
				}
			}
		}

		/*
		 * Remove current element from pathlist if dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->pathlist = list_delete_cell(parent_rel->pathlist,
													p1, p1_prev);

			/*
			 * Delete the data pointed-to by the deleted cell, if possible
			 */
			if (!IsA(old_path, IndexPath))
				pfree(old_path);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (new_path->total_cost >= old_path->total_cost)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the pathlist; we will not add new_path, and we assume
		 * new_path cannot dominate any other elements of the pathlist.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place in pathlist */
		if (insert_after)
			lappend_cell(parent_rel->pathlist, insert_after, new_path);
		else
			parent_rel->pathlist = lcons(new_path, parent_rel->pathlist);
	}
	else
	{
		/* Reject and recycle the new path */
		if (!IsA(new_path, IndexPath))
			pfree(new_path);
	}
}                               /* add_path */

/*
 * add_path_precheck
 *	  Check whether a proposed new path could possibly get accepted.
 *	  We assume we know the path's pathkeys and parameterization accurately,
 *	  and have lower bounds for its costs.
 *
 * Note that we do not know the path's rowcount, since getting an estimate for
 * that is too expensive to do before prechecking.  We assume here that paths
 * of a superset parameterization will generate fewer rows; if that holds,
 * then paths with different parameterizations cannot dominate each other
 * and so we can simply ignore existing paths of another parameterization.
 * (In the infrequent cases where that rule of thumb fails, add_path will
 * get rid of the inferior path.)
 *
 * At the time this is called, we haven't actually built a Path structure,
 * so the required information has to be passed piecemeal.
 */
bool
add_path_precheck(RelOptInfo *parent_rel,
				  Cost startup_cost, Cost total_cost,
				  List *pathkeys, Relids required_outer)
{
	List	   *new_path_pathkeys;
	bool		consider_startup;
	ListCell   *p1;

	/* Pretend parameterized paths have no pathkeys, per add_path policy */
	new_path_pathkeys = required_outer ? NIL : pathkeys;

	/* Decide whether new path's startup cost is interesting */
	consider_startup = required_outer ? parent_rel->consider_param_startup : parent_rel->consider_startup;

	foreach(p1, parent_rel->pathlist)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		PathKeysComparison keyscmp;

		/*
		 * We are looking for an old_path with the same parameterization (and
		 * by assumption the same rowcount) that dominates the new path on
		 * pathkeys as well as both cost metrics.  If we find one, we can
		 * reject the new path.
		 *
		 * Cost comparisons here should match compare_path_costs_fuzzily.
		 */
		if (total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
		{
			/* new path can win on startup cost only if consider_startup */
			if (startup_cost > old_path->startup_cost * STD_FUZZ_FACTOR ||
				!consider_startup)
			{
				/* new path loses on cost, so check pathkeys... */
				List	   *old_path_pathkeys;

				old_path_pathkeys = old_path->param_info ? NIL : old_path->pathkeys;
				keyscmp = compare_pathkeys(new_path_pathkeys,
										   old_path_pathkeys);
				if (keyscmp == PATHKEYS_EQUAL ||
					keyscmp == PATHKEYS_BETTER2)
				{
					/* new path does not win on pathkeys... */
					if (bms_equal(required_outer, PATH_REQ_OUTER(old_path)))
					{
						/* Found an old path that dominates the new one */
						return false;
					}
				}
			}
		}
		else
		{
			/*
			 * Since the pathlist is sorted by total_cost, we can stop looking
			 * once we reach a path with a total_cost larger than the new
			 * path's.
			 */
			break;
		}
	}

	return true;
}

/*
 * add_partial_path
 *	  Like add_path, our goal here is to consider whether a path is worthy
 *	  of being kept around, but the considerations here are a bit different.
 *	  A partial path is one which can be executed in any number of workers in
 *	  parallel such that each worker will generate a subset of the path's
 *	  overall result.
 *
 *	  As in add_path, the partial_pathlist is kept sorted with the cheapest
 *	  total path in front.  This is depended on by multiple places, which
 *	  just take the front entry as the cheapest path without searching.
 *
 *	  We don't generate parameterized partial paths for several reasons.  Most
 *	  importantly, they're not safe to execute, because there's nothing to
 *	  make sure that a parallel scan within the parameterized portion of the
 *	  plan is running with the same value in every worker at the same time.
 *	  Fortunately, it seems unlikely to be worthwhile anyway, because having
 *	  each worker scan the entire outer relation and a subset of the inner
 *	  relation will generally be a terrible plan.  The inner (parameterized)
 *	  side of the plan will be small anyway.  There could be rare cases where
 *	  this wins big - e.g. if join order constraints put a 1-row relation on
 *	  the outer side of the topmost join with a parameterized plan on the inner
 *	  side - but we'll have to be content not to handle such cases until
 *	  somebody builds an executor infrastructure that can cope with them.
 *
 *	  Because we don't consider parameterized paths here, we also don't
 *	  need to consider the row counts as a measure of quality: every path will
 *	  produce the same number of rows.  Neither do we need to consider startup
 *	  costs: parallelism is only used for plans that will be run to completion.
 *	  Therefore, this routine is much simpler than add_path: it needs to
 *	  consider only pathkeys and total cost.
 *
 *	  As with add_path, we pfree paths that are found to be dominated by
 *	  another partial path; this requires that there be no other references to
 *	  such paths yet.  Hence, GatherPaths must not be created for a rel until
 *	  we're done creating all partial paths for it.  Unlike add_path, we don't
 *	  take an exception for IndexPaths as partial index paths won't be
 *	  referenced by partial BitmapHeapPaths.
 */
void
add_partial_path(RelOptInfo *parent_rel, Path *new_path)
{
	bool		accept_new = true;	/* unless we find a superior old path */
	ListCell   *insert_after = NULL;	/* where to insert new item */
	ListCell   *p1;
	ListCell   *p1_prev;
	ListCell   *p1_next;

	/* Check for query cancel. */
	CHECK_FOR_INTERRUPTS();

	/* Path to be added must be parallel safe. */
	Assert(new_path->parallel_safe);

	/* Relation should be OK for parallelism, too. */
	Assert(parent_rel->consider_parallel);

	/*
	 * As in add_path, throw out any paths which are dominated by the new
	 * path, but throw out the new path if some existing path dominates it.
	 */
	p1_prev = NULL;
	for (p1 = list_head(parent_rel->partial_pathlist); p1 != NULL;
		 p1 = p1_next)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		bool		remove_old = false; /* unless new proves superior */
		PathKeysComparison keyscmp;

		p1_next = lnext(p1);

		/* Compare pathkeys. */
		keyscmp = compare_pathkeys(new_path->pathkeys, old_path->pathkeys);

		/* Unless pathkeys are incompable, keep just one of the two paths. */
		if (keyscmp != PATHKEYS_DIFFERENT)
		{
			if (new_path->total_cost > old_path->total_cost * STD_FUZZ_FACTOR)
			{
				/* New path costs more; keep it only if pathkeys are better. */
				if (keyscmp != PATHKEYS_BETTER1)
					accept_new = false;
			}
			else if (old_path->total_cost > new_path->total_cost
					 * STD_FUZZ_FACTOR)
			{
				/* Old path costs more; keep it only if pathkeys are better. */
				if (keyscmp != PATHKEYS_BETTER2)
					remove_old = true;
			}
			else if (keyscmp == PATHKEYS_BETTER1)
			{
				/* Costs are about the same, new path has better pathkeys. */
				remove_old = true;
			}
			else if (keyscmp == PATHKEYS_BETTER2)
			{
				/* Costs are about the same, old path has better pathkeys. */
				accept_new = false;
			}
			else if (old_path->total_cost > new_path->total_cost * 1.0000000001)
			{
				/* Pathkeys are the same, and the old path costs more. */
				remove_old = true;
			}
			else
			{
				/*
				 * Pathkeys are the same, and new path isn't materially
				 * cheaper.
				 */
				accept_new = false;
			}
		}

		/*
		 * Remove current element from partial_pathlist if dominated by new.
		 */
		if (remove_old)
		{
			parent_rel->partial_pathlist =
				list_delete_cell(parent_rel->partial_pathlist, p1, p1_prev);
			pfree(old_path);
			/* p1_prev does not advance */
		}
		else
		{
			/* new belongs after this old path if it has cost >= old's */
			if (new_path->total_cost >= old_path->total_cost)
				insert_after = p1;
			/* p1_prev advances */
			p1_prev = p1;
		}

		/*
		 * If we found an old path that dominates new_path, we can quit
		 * scanning the partial_pathlist; we will not add new_path, and we
		 * assume new_path cannot dominate any later path.
		 */
		if (!accept_new)
			break;
	}

	if (accept_new)
	{
		/* Accept the new path: insert it at proper place */
		if (insert_after)
			lappend_cell(parent_rel->partial_pathlist, insert_after, new_path);
		else
			parent_rel->partial_pathlist =
				lcons(new_path, parent_rel->partial_pathlist);
	}
	else
	{
		/* Reject and recycle the new path */
		pfree(new_path);
	}
}

/*
 * add_partial_path_precheck
 *	  Check whether a proposed new partial path could possibly get accepted.
 *
 * Unlike add_path_precheck, we can ignore startup cost and parameterization,
 * since they don't matter for partial paths (see add_partial_path).  But
 * we do want to make sure we don't add a partial path if there's already
 * a complete path that dominates it, since in that case the proposed path
 * is surely a loser.
 */
bool
add_partial_path_precheck(RelOptInfo *parent_rel, Cost total_cost,
						  List *pathkeys)
{
	ListCell   *p1;

	/*
	 * Our goal here is twofold.  First, we want to find out whether this path
	 * is clearly inferior to some existing partial path.  If so, we want to
	 * reject it immediately.  Second, we want to find out whether this path
	 * is clearly superior to some existing partial path -- at least, modulo
	 * final cost computations.  If so, we definitely want to consider it.
	 *
	 * Unlike add_path(), we always compare pathkeys here.  This is because we
	 * expect partial_pathlist to be very short, and getting a definitive
	 * answer at this stage avoids the need to call add_path_precheck.
	 */
	foreach(p1, parent_rel->partial_pathlist)
	{
		Path	   *old_path = (Path *) lfirst(p1);
		PathKeysComparison keyscmp;

		keyscmp = compare_pathkeys(pathkeys, old_path->pathkeys);
		if (keyscmp != PATHKEYS_DIFFERENT)
		{
			if (total_cost > old_path->total_cost * STD_FUZZ_FACTOR &&
				keyscmp != PATHKEYS_BETTER1)
				return false;
			if (old_path->total_cost > total_cost * STD_FUZZ_FACTOR &&
				keyscmp != PATHKEYS_BETTER2)
				return true;
		}
	}

	/*
	 * This path is neither clearly inferior to an existing partial path nor
	 * clearly good enough that it might replace one.  Compare it to
	 * non-parallel plans.  If it loses even before accounting for the cost of
	 * the Gather node, we should definitely reject it.
	 *
	 * Note that we pass the total_cost to add_path_precheck twice.  This is
	 * because it's never advantageous to consider the startup cost of a
	 * partial path; the resulting plans, if run in parallel, will be run to
	 * completion.
	 */
	if (!add_path_precheck(parent_rel, total_cost, total_cost, pathkeys,
						   NULL))
		return false;

	return true;
}


/*****************************************************************************
 *		PATH NODE CREATION ROUTINES
 *****************************************************************************/

/*
 * create_seqscan_path
 *	  Creates a path corresponding to a sequential scan, returning the
 *	  pathnode.
 */
Path *
create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
					Relids required_outer, int parallel_workers)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_SeqScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = parallel_workers > 0 ? true : false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = parallel_workers;
	pathnode->pathkeys = NIL;	/* seqscan has unordered result */

	pathnode->locus = cdbpathlocus_from_baserel(root, rel);
	pathnode->motionHazard = false;
	pathnode->rescannable = true;
	pathnode->sameslice_relids = rel->relids;

	cost_seqscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_samplescan_path
 *	  Creates a path node for a sampled table scan.
 */
Path *
create_samplescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_SampleScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* samplescan has unordered result */

	pathnode->locus = cdbpathlocus_from_baserel(root, rel);
	pathnode->motionHazard = false;
	pathnode->rescannable = true;
	pathnode->sameslice_relids = rel->relids;

	cost_samplescan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_index_path
 *	  Creates a path node for an index scan.
 *
 * 'index' is a usable index.
 * 'indexclauses' is a list of IndexClause nodes representing clauses
 *			to be enforced as qual conditions in the scan.
 * 'indexorderbys' is a list of bare expressions (no RestrictInfos)
 *			to be used as index ordering operators in the scan.
 * 'indexorderbycols' is an integer list of index column numbers (zero based)
 *			the ordering operators can be used with.
 * 'pathkeys' describes the ordering of the path.
 * 'indexscandir' is ForwardScanDirection or BackwardScanDirection
 *			for an ordered index, or NoMovementScanDirection for
 *			an unordered index.
 * 'indexonly' is true if an index-only scan is wanted.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 * 'partial_path' is true if constructing a parallel index scan path.
 *
 * Returns the new path node.
 */
IndexPath *
create_index_path(PlannerInfo *root,
				  IndexOptInfo *index,
				  List *indexclauses,
				  List *indexorderbys,
				  List *indexorderbycols,
				  List *pathkeys,
				  ScanDirection indexscandir,
				  bool indexonly,
				  Relids required_outer,
				  double loop_count,
				  bool partial_path)
{
	IndexPath  *pathnode = makeNode(IndexPath);
	RelOptInfo *rel = index->rel;

	pathnode->path.pathtype = indexonly ? T_IndexOnlyScan : T_IndexScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = pathkeys;

	pathnode->indexinfo = index;
	pathnode->indexclauses = indexclauses;
	pathnode->indexorderbys = indexorderbys;
	pathnode->indexorderbycols = indexorderbycols;
	pathnode->indexscandir = indexscandir;

	/* Distribution is same as the base table. */
	pathnode->path.locus = cdbpathlocus_from_baserel(root, rel);
	pathnode->path.motionHazard = false;
	pathnode->path.rescannable = true;
	pathnode->path.sameslice_relids = rel->relids;

	cost_index(pathnode, root, loop_count, partial_path);

	return pathnode;
}

/*
 * create_bitmap_heap_path
 *	  Creates a path node for a bitmap scan.
 *
 * 'bitmapqual' is a tree of IndexPath, BitmapAndPath, and BitmapOrPath nodes.
 * 'required_outer' is the set of outer relids for a parameterized path.
 * 'loop_count' is the number of repetitions of the indexscan to factor into
 *		estimates of caching behavior.
 *
 * loop_count should match the value used when creating the component
 * IndexPaths.
 */
BitmapHeapPath *
create_bitmap_heap_path(PlannerInfo *root,
						RelOptInfo *rel,
						Path *bitmapqual,
						Relids required_outer,
						double loop_count,
						int parallel_degree)
{
	BitmapHeapPath *pathnode = makeNode(BitmapHeapPath);

	pathnode->path.pathtype = T_BitmapHeapScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = parallel_degree > 0 ? true : false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = parallel_degree;
	pathnode->path.pathkeys = NIL;	/* always unordered */

	/* Distribution is same as the base table. */
	pathnode->path.locus = cdbpathlocus_from_baserel(root, rel);
	pathnode->path.motionHazard = false;
	pathnode->path.rescannable = true;
	pathnode->path.sameslice_relids = rel->relids;

	pathnode->bitmapqual = bitmapqual;

	cost_bitmap_heap_scan(&pathnode->path, root, rel,
						  pathnode->path.param_info,
						  bitmapqual, loop_count);

	return pathnode;
}

/*
 * create_bitmap_and_path
 *	  Creates a path node representing a BitmapAnd.
 */
BitmapAndPath *
create_bitmap_and_path(PlannerInfo *root,
					   RelOptInfo *rel,
					   List *bitmapquals)
{
	BitmapAndPath *pathnode = makeNode(BitmapAndPath);
	Relids		required_outer = NULL;
	ListCell   *lc;

	pathnode->path.pathtype = T_BitmapAnd;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;

	/*
	 * Identify the required outer rels as the union of what the child paths
	 * depend on.  (Alternatively, we could insist that the caller pass this
	 * in, but it's more convenient and reliable to compute it here.)
	 */
	foreach(lc, bitmapquals)
	{
		Path	   *bitmapqual = (Path *) lfirst(lc);

		required_outer = bms_add_members(required_outer,
										 PATH_REQ_OUTER(bitmapqual));
	}
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);

	/*
	 * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
	 * parallel-safe if and only if rel->consider_parallel is set.  So, we can
	 * set the flag for this path based only on the relation-level flag,
	 * without actually iterating over the list of children.
	 */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;

	pathnode->path.pathkeys = NIL;	/* always unordered */

	pathnode->bitmapquals = bitmapquals;

	/* this sets bitmapselectivity as well as the regular cost fields: */
	cost_bitmap_and_node(pathnode, root);

	return pathnode;
}

/*
 * create_bitmap_or_path
 *	  Creates a path node representing a BitmapOr.
 */
BitmapOrPath *
create_bitmap_or_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  List *bitmapquals)
{
	BitmapOrPath *pathnode = makeNode(BitmapOrPath);
	Relids		required_outer = NULL;
	ListCell   *lc;

	pathnode->path.pathtype = T_BitmapOr;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;

	/*
	 * Identify the required outer rels as the union of what the child paths
	 * depend on.  (Alternatively, we could insist that the caller pass this
	 * in, but it's more convenient and reliable to compute it here.)
	 */
	foreach(lc, bitmapquals)
	{
		Path	   *bitmapqual = (Path *) lfirst(lc);

		required_outer = bms_add_members(required_outer,
										 PATH_REQ_OUTER(bitmapqual));
	}
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);

	/*
	 * Currently, a BitmapHeapPath, BitmapAndPath, or BitmapOrPath will be
	 * parallel-safe if and only if rel->consider_parallel is set.  So, we can
	 * set the flag for this path based only on the relation-level flag,
	 * without actually iterating over the list of children.
	 */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;

	pathnode->path.pathkeys = NIL;	/* always unordered */

	pathnode->bitmapquals = bitmapquals;

	/* this sets bitmapselectivity as well as the regular cost fields: */
	cost_bitmap_or_node(pathnode, root);

	return pathnode;
}

/*
 * create_tidscan_path
 *	  Creates a path corresponding to a scan by TID, returning the pathnode.
 */
TidPath *
create_tidscan_path(PlannerInfo *root, RelOptInfo *rel, List *tidquals,
					Relids required_outer)
{

	if (!REL_SUPPORTS_TID_SCAN(rel))
		return NULL;

	TidPath    *pathnode = makeNode(TidPath);

	pathnode->path.pathtype = T_TidScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;	/* always unordered */

	pathnode->tidquals = tidquals;

	/* Distribution is same as the base table. */
	pathnode->path.locus = cdbpathlocus_from_baserel(root, rel);
	pathnode->path.motionHazard = false;
	pathnode->path.rescannable = true;
	pathnode->path.sameslice_relids = rel->relids;

	cost_tidscan(&pathnode->path, root, rel, tidquals,
				 pathnode->path.param_info);

	return pathnode;
}

/*
 * create_append_path
 *	  Creates a path corresponding to an Append plan, returning the
 *	  pathnode.
 *
 * Note that we must handle subpaths = NIL, representing a dummy access path.
 * Also, there are callers that pass root = NULL.
 */
AppendPath *
create_append_path(PlannerInfo *root,
				   RelOptInfo *rel,
				   List *subpaths, List *partial_subpaths,
				   List *pathkeys, Relids required_outer,
				   int parallel_workers, bool parallel_aware,
				   List *partitioned_rels, double rows)
{
	AppendPath *pathnode = makeNode(AppendPath);
	ListCell   *l;

	Assert(!parallel_aware || parallel_workers > 0);

	pathnode->path.pathtype = T_Append;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;

	/*
	 * When generating an Append path for a partitioned table, there may be
	 * parameters that are useful so we can eliminate certain partitions
	 * during execution.  Here we'll go all the way and fully populate the
	 * parameter info data as we do for normal base relations.  However, we
	 * need only bother doing this for RELOPT_BASEREL rels, as
	 * RELOPT_OTHER_MEMBER_REL's Append paths are merged into the base rel's
	 * Append subpaths.  It would do no harm to do this, we just avoid it to
	 * save wasting effort.
	 */
	if (partitioned_rels != NIL && root && rel->reloptkind == RELOPT_BASEREL)
		pathnode->path.param_info = get_baserel_parampathinfo(root,
															  rel,
															  required_outer);
	else
		pathnode->path.param_info = get_appendrel_parampathinfo(rel,
																required_outer);

	pathnode->path.parallel_aware = parallel_aware;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->partitioned_rels = list_copy(partitioned_rels);

	pathnode->path.motionHazard = false;
	pathnode->path.rescannable = true;

	/*
	 * For parallel append, non-partial paths are sorted by descending total
	 * costs. That way, the total time to finish all non-partial paths is
	 * minimized.  Also, the partial paths are sorted by descending startup
	 * costs.  There may be some paths that require to do startup work by a
	 * single worker.  In such case, it's better for workers to choose the
	 * expensive ones first, whereas the leader should choose the cheapest
	 * startup plan.
	 */
	if (pathnode->path.parallel_aware)
	{
		/*
		 * We mustn't fiddle with the order of subpaths when the Append has
		 * pathkeys.  The order they're listed in is critical to keeping the
		 * pathkeys valid.
		 */
		Assert(pathkeys == NIL);

		subpaths = list_qsort(subpaths, append_total_cost_compare);
		partial_subpaths = list_qsort(partial_subpaths,
									  append_startup_cost_compare);
	}
	pathnode->first_partial_path = list_length(subpaths);
	pathnode->subpaths = list_concat(subpaths, partial_subpaths);

	/*
	 * Apply query-wide LIMIT if known and path is for sole base relation.
	 * (Handling this at this low level is a bit klugy.)
	 */
	if (root != NULL && bms_equal(rel->relids, root->all_baserels))
		pathnode->limit_tuples = root->limit_tuples;
	else
		pathnode->limit_tuples = -1.0;

	if (!set_append_path_locus(root, (Path *) pathnode, rel, NIL))
		return NULL;

	foreach(l, pathnode->subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);

		pathnode->path.parallel_safe = pathnode->path.parallel_safe &&
			subpath->parallel_safe;

		/* All child paths must have same parameterization */
		Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
	}

	Assert(!parallel_aware || pathnode->path.parallel_safe);

	/*
	 * If there's exactly one child path, the Append is a no-op and will be
	 * discarded later (in setrefs.c); therefore, we can inherit the child's
	 * size and cost, as well as its pathkeys if any (overriding whatever the
	 * caller might've said).  Otherwise, we must do the normal costsize
	 * calculation.
	 */
	if (list_length(pathnode->subpaths) == 1)
	{
		Path	   *child = (Path *) linitial(pathnode->subpaths);

		pathnode->path.rows = child->rows;
		pathnode->path.startup_cost = child->startup_cost;
		pathnode->path.total_cost = child->total_cost;
		pathnode->path.pathkeys = child->pathkeys;
	}
	else
		cost_append(pathnode);

	/* If the caller provided a row estimate, override the computed value. */
	if (rows >= 0)
		pathnode->path.rows = rows;

	return pathnode;
}

/*
 * append_total_cost_compare
 *	  qsort comparator for sorting append child paths by total_cost descending
 *
 * For equal total costs, we fall back to comparing startup costs; if those
 * are equal too, break ties using bms_compare on the paths' relids.
 * (This is to avoid getting unpredictable results from qsort.)
 */
static int
append_total_cost_compare(const void *a, const void *b)
{
	Path	   *path1 = (Path *) lfirst(*(ListCell **) a);
	Path	   *path2 = (Path *) lfirst(*(ListCell **) b);
	int			cmp;

	cmp = compare_path_costs(path1, path2, TOTAL_COST);
	if (cmp != 0)
		return -cmp;
	return bms_compare(path1->parent->relids, path2->parent->relids);
}

/*
 * append_startup_cost_compare
 *	  qsort comparator for sorting append child paths by startup_cost descending
 *
 * For equal startup costs, we fall back to comparing total costs; if those
 * are equal too, break ties using bms_compare on the paths' relids.
 * (This is to avoid getting unpredictable results from qsort.)
 */
static int
append_startup_cost_compare(const void *a, const void *b)
{
	Path	   *path1 = (Path *) lfirst(*(ListCell **) a);
	Path	   *path2 = (Path *) lfirst(*(ListCell **) b);
	int			cmp;

	cmp = compare_path_costs(path1, path2, STARTUP_COST);
	if (cmp != 0)
		return -cmp;
	return bms_compare(path1->parent->relids, path2->parent->relids);
}

/*
 * create_merge_append_path
 *	  Creates a path corresponding to a MergeAppend plan, returning the
 *	  pathnode.
 */
MergeAppendPath *
create_merge_append_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 List *subpaths,
						 List *pathkeys,
						 Relids required_outer,
						 List *partitioned_rels)
{
	MergeAppendPath *pathnode = makeNode(MergeAppendPath);
	Cost		input_startup_cost;
	Cost		input_total_cost;
	ListCell   *l;

	pathnode->path.pathtype = T_MergeAppend;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_appendrel_parampathinfo(rel,
															required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = pathkeys;
	pathnode->partitioned_rels = list_copy(partitioned_rels);
	pathnode->subpaths = subpaths;

	/*
	 * Apply query-wide LIMIT if known and path is for sole base relation.
	 * (Handling this at this low level is a bit klugy.)
	 */
	if (bms_equal(rel->relids, root->all_baserels))
		pathnode->limit_tuples = root->limit_tuples;
	else
		pathnode->limit_tuples = -1.0;

	/*
	 * Add Motions to the child nodes as needed, and determine the locus
	 * of the MergeAppend itself.
	 */
	if (!set_append_path_locus(root, (Path *) pathnode, rel, pathkeys))
		return NULL;

	/*
	 * Add up the sizes and costs of the input paths.
	 */
	pathnode->path.rows = 0;
	input_startup_cost = 0;
	input_total_cost = 0;
	foreach(l, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);

		pathnode->path.rows += subpath->rows;
		pathnode->path.parallel_safe = pathnode->path.parallel_safe &&
			subpath->parallel_safe;

		if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
		{
			/* Subpath is adequately ordered, we won't need to sort it */
			input_startup_cost += subpath->startup_cost;
			input_total_cost += subpath->total_cost;
		}
		else
		{
			/* We'll need to insert a Sort node, so include cost for that */
			Path		sort_path;	/* dummy for result of cost_sort */

			cost_sort(&sort_path,
					  root,
					  pathkeys,
					  subpath->total_cost,
					  /* GPDB: pass subpath->rows because it's been adjusted for # of segments */
					  subpath->rows,
					  subpath->pathtarget->width,
					  0.0,
					  work_mem,
					  pathnode->limit_tuples);
			input_startup_cost += sort_path.startup_cost;
			input_total_cost += sort_path.total_cost;
		}

		/* All child paths must have same parameterization */
		Assert(bms_equal(PATH_REQ_OUTER(subpath), required_outer));
	}

	/*
	 * Now we can compute total costs of the MergeAppend.  If there's exactly
	 * one child path, the MergeAppend is a no-op and will be discarded later
	 * (in setrefs.c); otherwise we do the normal cost calculation.
	 */
	if (list_length(subpaths) == 1)
	{
		pathnode->path.startup_cost = input_startup_cost;
		pathnode->path.total_cost = input_total_cost;
	}
	else
		cost_merge_append(&pathnode->path, root,
						  pathkeys, list_length(subpaths),
						  input_startup_cost, input_total_cost,
						  pathnode->path.rows);

	return pathnode;
}

/*
 * Set the locus of an Append or MergeAppend path.
 *
 * This modifies the 'subpaths', costs fields, and locus of 'pathnode'.
 * It will return false if fails to create motion for parameterized path.
 */
static bool
set_append_path_locus(PlannerInfo *root, Path *pathnode, RelOptInfo *rel,
					  List *pathkeys)
{
	ListCell   *l;
	CdbLocusType targetlocustype;
	CdbPathLocus targetlocus;
	int			numsegments;
	List	   *subpaths;
	List	  **subpaths_out;
	List	   *new_subpaths;

	/*
	 * Init max_numsegments to slient compiler.
	 * This variable is only used when result
	 * locus is partitioned.
	 */
	int 	    max_numsegments = -1;

	if (IsA(pathnode, AppendPath))
		subpaths_out = &((AppendPath *) pathnode)->subpaths;
	else if (IsA(pathnode, MergeAppendPath))
		subpaths_out = &((MergeAppendPath *) pathnode)->subpaths;
	else
		elog(ERROR, "unexpected append path type: %d", nodeTag(pathnode));
	subpaths = *subpaths_out;
	*subpaths_out = NIL;

	/* If no subpath, any worker can execute this Append.  Result has 0 rows. */
	if (!subpaths)
	{
		CdbPathLocus_MakeGeneral(&pathnode->locus);
		return true;
	}

	/*
	 * Do a first pass over the children to determine what locus the result
	 * should have, based on the loci of the children.
	 *
	 * We only determine the target locus type here, the number of segments is
	 * figured out later. We treat also all partitioned types the same for now,
	 * using Strewn to represent them all, and figure out later if we can mark
	 * it hashed, or if have to leave it strewn.
	 *
	 * We will record the max number of segments of each subpath here for later
	 * use.
	 */
	static const struct
	{
		CdbLocusType a;
		CdbLocusType b;
		CdbLocusType result;
	} append_locus_compatibility_table[] =
	{
		/*
		 * If any of the children have 'outerquery' locus, bring all the subpaths
		 * to the outer query's locus.
		 */
		{ CdbLocusType_OuterQuery, CdbLocusType_OuterQuery,     CdbLocusType_OuterQuery },
		{ CdbLocusType_OuterQuery, CdbLocusType_Entry,          CdbLocusType_OuterQuery },
		{ CdbLocusType_OuterQuery, CdbLocusType_SingleQE,       CdbLocusType_OuterQuery },
		{ CdbLocusType_OuterQuery, CdbLocusType_Strewn,         CdbLocusType_OuterQuery },
		{ CdbLocusType_OuterQuery, CdbLocusType_Replicated,     CdbLocusType_OuterQuery },
		{ CdbLocusType_OuterQuery, CdbLocusType_SegmentGeneral, CdbLocusType_OuterQuery },
		{ CdbLocusType_OuterQuery, CdbLocusType_General,        CdbLocusType_OuterQuery },
		
		/*
		 * Similarly, if any of the children have 'entry' locus, bring all the subpaths
		 * to the entry db.
		 */
		{ CdbLocusType_Entry, CdbLocusType_Entry,          CdbLocusType_Entry },
		{ CdbLocusType_Entry, CdbLocusType_SingleQE,       CdbLocusType_Entry },
		{ CdbLocusType_Entry, CdbLocusType_Strewn,         CdbLocusType_Entry },
		{ CdbLocusType_Entry, CdbLocusType_Replicated,     CdbLocusType_Entry },
		{ CdbLocusType_Entry, CdbLocusType_SegmentGeneral, CdbLocusType_Entry },
		{ CdbLocusType_Entry, CdbLocusType_General,        CdbLocusType_Entry },

		/* similarly, if there are single QE children, bring everything to a single QE */
		{ CdbLocusType_SingleQE, CdbLocusType_SingleQE,       CdbLocusType_SingleQE },
		{ CdbLocusType_SingleQE, CdbLocusType_Strewn,         CdbLocusType_SingleQE },
		{ CdbLocusType_SingleQE, CdbLocusType_Replicated,     CdbLocusType_SingleQE },
		{ CdbLocusType_SingleQE, CdbLocusType_SegmentGeneral, CdbLocusType_SingleQE },
		{ CdbLocusType_SingleQE, CdbLocusType_General,        CdbLocusType_SingleQE },

		/*
		 * If everything is partitioned, then the result can be partitioned, too.
		 * But if it's a mix of partitioned and replicated, then we have to bring
		 * everything to a single QE. Otherwise, the replicated children
		 * will contribute rows on every QE.
		 * If it's a mix of partitioned and general, we still consider the
		 * result as partitioned. But the general part will be restricted to
		 * only produce rows on a single QE.
		 */
		{ CdbLocusType_Strewn, CdbLocusType_Strewn,         CdbLocusType_Strewn },
		{ CdbLocusType_Strewn, CdbLocusType_Replicated,     CdbLocusType_SingleQE },
		{ CdbLocusType_Strewn, CdbLocusType_SegmentGeneral, CdbLocusType_Strewn },
		{ CdbLocusType_Strewn, CdbLocusType_General,        CdbLocusType_Strewn },

		{ CdbLocusType_Replicated, CdbLocusType_Replicated, CdbLocusType_Replicated },
		{ CdbLocusType_Replicated, CdbLocusType_SegmentGeneral, CdbLocusType_Replicated },
		{ CdbLocusType_Replicated, CdbLocusType_General,        CdbLocusType_Replicated },

		{ CdbLocusType_SegmentGeneral, CdbLocusType_SegmentGeneral, CdbLocusType_SegmentGeneral },
		{ CdbLocusType_SegmentGeneral, CdbLocusType_General,        CdbLocusType_SegmentGeneral },

		{ CdbLocusType_General, CdbLocusType_General,        CdbLocusType_General },
	};
	targetlocustype = CdbLocusType_General;
	foreach(l, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);
		CdbLocusType subtype;
		int			i;

		if (CdbPathLocus_IsPartitioned(subpath->locus))
			subtype = CdbLocusType_Strewn;
		else
			subtype = subpath->locus.locustype;

		if (l == list_head(subpaths))
		{
			targetlocustype = subtype;
			max_numsegments = CdbPathLocus_NumSegments(subpath->locus);
			continue;
		}

		max_numsegments = Max(max_numsegments,
							  CdbPathLocus_NumSegments(subpath->locus));

		for (i = 0; i < lengthof(append_locus_compatibility_table); i++)
		{
			if ((append_locus_compatibility_table[i].a == targetlocustype &&
				 append_locus_compatibility_table[i].b == subtype) ||
				(append_locus_compatibility_table[i].a == subtype &&
				 append_locus_compatibility_table[i].b == targetlocustype))
			{
				targetlocustype = append_locus_compatibility_table[i].result;
				break;
			}
		}
		if (i == lengthof(append_locus_compatibility_table))
			elog(ERROR, "could not determine target locus for Append");
	}

	/*
	 * Now compute the 'numsegments', and the hash keys if it's a partitioned
	 * type.
	 */
	if (targetlocustype == CdbLocusType_Entry)
	{
		/* nothing more to do */
		CdbPathLocus_MakeEntry(&targetlocus);
	}
	else if (targetlocustype == CdbLocusType_OuterQuery)
	{
		/* nothing more to do */
		CdbPathLocus_MakeOuterQuery(&targetlocus);
	}
	else if (targetlocustype == CdbLocusType_General)
	{
		/* nothing more to do */
		CdbPathLocus_MakeGeneral(&targetlocus);
	}
	else if (targetlocustype == CdbLocusType_SingleQE ||
			 targetlocustype == CdbLocusType_Replicated ||
			 targetlocustype == CdbLocusType_SegmentGeneral)
	{
		/* By default put Append node on all the segments */
		numsegments = getgpsegmentCount();
		foreach(l, subpaths)
		{
			Path	   *subpath = (Path *) lfirst(l);

			/*
			 * Align numsegments to be the common segments among the children.
			 * Partitioned children will need to be motioned, so ignore them.
			 */
			if (CdbPathLocus_IsSingleQE(subpath->locus) ||
				CdbPathLocus_IsSegmentGeneral(subpath->locus) ||
				CdbPathLocus_IsReplicated(subpath->locus))
			{
				numsegments = Min(numsegments,
								  CdbPathLocus_NumSegments(subpath->locus));
			}
		}
		CdbPathLocus_MakeSimple(&targetlocus, targetlocustype, numsegments);
	}
	else if (targetlocustype == CdbLocusType_Strewn)
	{
		bool		isfirst = true;

		/* By default put Append node on all the segments */
		numsegments = getgpsegmentCount();
		CdbPathLocus_MakeNull(&targetlocus);
		foreach(l, subpaths)
		{
			Path	   *subpath = (Path *) lfirst(l);
			CdbPathLocus projectedlocus;

			if (CdbPathLocus_IsGeneral(subpath->locus) ||
				CdbPathLocus_IsSegmentGeneral(subpath->locus))
			{
				/* Afterwards, General/SegmentGeneral will be projected as Strewn */
				CdbPathLocus_MakeStrewn(&projectedlocus, numsegments);
			}
			else
			{
				Assert(CdbPathLocus_IsPartitioned(subpath->locus));
				projectedlocus = subpath->locus;

				/* Transform subpath locus into the appendrel's space for comparison. */
				if (subpath->parent->reloptkind == RELOPT_OTHER_MEMBER_REL &&
					subpath->parent != rel &&
					(CdbPathLocus_IsHashed(subpath->locus) || CdbPathLocus_IsHashedOJ(subpath->locus)))
				{
					CdbPathLocus l;

					l = cdbpathlocus_pull_above_projection(root,
						                                   subpath->locus,
						                                   subpath->parent->relids,
						                                   subpath->parent->reltarget->exprs,
						                                   rel->reltarget->exprs,
						                                   rel->relid);
					if (CdbPathLocus_IsHashed(l) || CdbPathLocus_IsHashedOJ(l))
						projectedlocus = l;
				}
			}

			/*
			 * CDB: If all the scans are distributed alike, set
			 * the result locus to match.  Otherwise, if all are partitioned,
			 * set it to strewn.  A mixture of partitioned and non-partitioned
			 * scans should not occur after above correction;
			 *
			 * CDB TODO: When the scans are not all partitioned alike, and the
			 * result is joined with another rel, consider pushing the join
			 * below the Append so that child tables that are properly
			 * distributed can be joined in place.
			 */
			if (isfirst)
			{
				targetlocus = projectedlocus;
				isfirst = false;
			}
			else if (cdbpathlocus_equal(targetlocus, projectedlocus))
			{
				/* compatible */
			}
			else
			{
				/*
				 * subpaths have different distributed policy, mark it as random
				 * distributed and set the numsegments to the maximum of all
				 * subpaths to not missing any tuples.
				 *
				 * max_numsegments is computed in the first deduction loop,
				 * even here we use projectdlocus, the numsegments never change.
				 */
				CdbPathLocus_MakeStrewn(&targetlocus, max_numsegments);
				break;
			}
		}
	}
	else
		elog(ERROR, "unexpected Append target locus type");

	/* Ok, we now know the target locus. Add Motions/Projections to any subpaths that need it */
	new_subpaths = NIL;
	foreach(l, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(l);

		if (CdbPathLocus_IsPartitioned(targetlocus))
		{
			if (CdbPathLocus_IsGeneral(subpath->locus) ||
				CdbPathLocus_IsSegmentGeneral(subpath->locus))
			{
				/*
				 * If a General/SegmentGeneral is mixed with other Strewn's,
				 * add a projection path with cdb_restrict_clauses, so that only
				 * a single QE will actually produce rows.
				 */
				RestrictInfo *restrict_info;

				if (CdbPathLocus_IsGeneral(subpath->locus))
					numsegments = targetlocus.numsegments;
				else
					numsegments = subpath->locus.numsegments;

				restrict_info = make_restrictinfo((Expr *) makeSegmentFilterExpr(
													  gp_session_id % numsegments),
												  true,		/* is_pushed_down */
												  false,	/* outerjoin_delayed */
												  true,		/* pseudoconstant */
												  0,		/* security_level */
												  NULL,		/* required_relids */
												  NULL,		/* outer_relids */
												  NULL);	/* nullable_relids */

				subpath = (Path *) create_projection_path_with_quals(
					root,
					subpath->parent,
					subpath,
					subpath->pathtarget,
					list_make1(restrict_info),
					false);

				/*
				 * We use the skill of Result plannode with one time filter
				 * gp_execution_segment() = <segid> here, so we should update
				 * direct dispatch info when creating plan.
				 */
				((ProjectionPath *) subpath)->direct_dispath_contentIds = list_make1_int(gp_session_id % numsegments);

				CdbPathLocus_MakeStrewn(&(subpath->locus),
				                        numsegments);
			}

			/* we already determined that all the loci are compatible */
			Assert(CdbPathLocus_IsPartitioned(subpath->locus));
		}
		else
		{
			if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
				subpath = cdbpath_create_motion_path(root, subpath, pathkeys, false, targetlocus);
			else
				subpath = cdbpath_create_motion_path(root, subpath, NIL, false, targetlocus);

			if (subpath == NULL)
				return false;
		}

		pathnode->sameslice_relids = bms_union(pathnode->sameslice_relids, subpath->sameslice_relids);

		if (subpath->motionHazard)
			pathnode->motionHazard = true;

		if (!subpath->rescannable)
			pathnode->rescannable = false;

		new_subpaths = lappend(new_subpaths, subpath);
	}
	pathnode->locus = targetlocus;

	*subpaths_out = new_subpaths;

	return true;
}

/*
 * create_group_result_path
 *	  Creates a path representing a Result-and-nothing-else plan.
 *
 * This is only used for degenerate grouping cases, in which we know we
 * need to produce one result row, possibly filtered by a HAVING qual.
 */
GroupResultPath *
create_group_result_path(PlannerInfo *root, RelOptInfo *rel,
						 PathTarget *target, List *havingqual)
{
	GroupResultPath *pathnode = makeNode(GroupResultPath);

	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	pathnode->path.param_info = NULL;	/* there are no other rels... */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;
	pathnode->quals = havingqual;

	/*
	 * We can't quite use cost_resultscan() because the quals we want to
	 * account for are not baserestrict quals of the rel.  Might as well just
	 * hack it here.
	 */
	pathnode->path.rows = 1;
	pathnode->path.startup_cost = target->cost.startup;
	pathnode->path.total_cost = target->cost.startup +
		cpu_tuple_cost + target->cost.per_tuple;

	/*
	 * Add cost of qual, if any --- but we ignore its selectivity, since our
	 * rowcount estimate should be 1 no matter what the qual is.
	 */
	if (havingqual)
	{
		QualCost	qual_cost;

		cost_qual_eval(&qual_cost, havingqual, root);
		/* havingqual is evaluated once at startup */
		pathnode->path.startup_cost += qual_cost.startup + qual_cost.per_tuple;
		pathnode->path.total_cost += qual_cost.startup + qual_cost.per_tuple;
	}

	/* Result can be on any segments */
	CdbPathLocus_MakeGeneral(&pathnode->path.locus);
	pathnode->path.motionHazard = false;
	pathnode->path.rescannable = true;

	return pathnode;
}

/*
 * create_material_path
 *	  Creates a path corresponding to a Material plan, returning the
 *	  pathnode.
 */
MaterialPath *
create_material_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath)
{
	MaterialPath *pathnode = makeNode(MaterialPath);

	Assert(subpath->parent == rel);

	pathnode->path.pathtype = T_Material;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->path.locus = subpath->locus;
	pathnode->path.motionHazard = subpath->motionHazard;
	pathnode->cdb_strict = false;
	pathnode->path.rescannable = true; /* Independent of sub-path */
	pathnode->path.sameslice_relids = subpath->sameslice_relids;

	pathnode->subpath = subpath;

	cost_material(&pathnode->path,
				  root,
				  subpath->startup_cost,
				  subpath->total_cost,
				  subpath->rows,
				  subpath->pathtarget->width);

	return pathnode;
}

/*
 * create_unique_path
 *	  Creates a path representing elimination of distinct rows from the
 *	  input data.  Distinct-ness is defined according to the needs of the
 *	  semijoin represented by sjinfo.  If it is not possible to identify
 *	  how to make the data unique, NULL is returned.
 *
 * If used at all, this is likely to be called repeatedly on the same rel;
 * and the input subpath should always be the same (the cheapest_total path
 * for the rel).  So we cache the result.
 */
UniquePath *
create_unique_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				   SpecialJoinInfo *sjinfo)
{
	UniquePath *pathnode;
	Path		sort_path;		/* dummy for result of cost_sort */
	Path		agg_path;		/* dummy for result of cost_agg */
	MemoryContext oldcontext;
	int			numCols;
	CdbPathLocus locus;
	bool		add_motion = false;
	double		numsegments;

	/* Caller made a mistake if subpath isn't cheapest_total ... */
	Assert(subpath == rel->cheapest_total_path);
	Assert(subpath->parent == rel);
	/* ... or if SpecialJoinInfo is the wrong one */
	Assert(sjinfo->jointype == JOIN_SEMI);
	Assert(bms_equal(rel->relids, sjinfo->syn_righthand));

	/* If result already cached, return it */
	if (rel->cheapest_unique_path)
		return (UniquePath *) rel->cheapest_unique_path;

	/* If it's not possible to unique-ify, return NULL */
	if (!(sjinfo->semi_can_btree || sjinfo->semi_can_hash))
		return NULL;

	/*
	 * When called during GEQO join planning, we are in a short-lived memory
	 * context.  We must make sure that the path and any subsidiary data
	 * structures created for a baserel survive the GEQO cycle, else the
	 * baserel is trashed for future GEQO cycles.  On the other hand, when we
	 * are creating those for a joinrel during GEQO, we don't want them to
	 * clutter the main planning context.  Upshot is that the best solution is
	 * to explicitly allocate memory in the same context the given RelOptInfo
	 * is in.
	 */
	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));

	/* Repartition first if duplicates might be on different QEs. */
	if (!CdbPathLocus_IsBottleneck(subpath->locus) &&
		!cdbpathlocus_is_hashed_on_exprs(subpath->locus, sjinfo->semi_rhs_exprs, false))
	{
		int			numsegments = CdbPathLocus_NumSegments(subpath->locus);

		List	   *opfamilies = NIL;
		List	   *sortrefs = NIL;
		ListCell   *lc;

		foreach(lc, sjinfo->semi_rhs_exprs)
		{
			Node	   *expr = lfirst(lc);
			Oid			opfamily;

			opfamily = cdb_default_distribution_opfamily_for_type(exprType(expr));
			opfamilies = lappend_oid(opfamilies, opfamily);
			sortrefs = lappend_int(sortrefs, 0);
		}

		locus = cdbpathlocus_from_exprs(root,
										subpath->parent,
										sjinfo->semi_rhs_exprs, opfamilies, sortrefs, numsegments);
        subpath = cdbpath_create_motion_path(root, subpath, NIL, false, locus);
		/*
		 * We probably add agg/sort node above the added motion node, but it is
		 * possible to add an agg/sort node below this motion node also,
		 * which might be optimal in some cases?
		 */
		add_motion = true;
		if (subpath == NULL)
			elog(ERROR, "could not create motion path");
	}
	else
		locus = subpath->locus;

	if (CdbPathLocus_IsPartitioned(locus))
		numsegments = CdbPathLocus_NumSegments(locus);
	else
		numsegments = 1;

	/*
	 * If we get here, we can unique-ify using at least one of sorting and
	 * hashing.  Start building the result Path object.
	 */
	pathnode = makeNode(UniquePath);

	pathnode->path.pathtype = T_Unique;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.locus = locus;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;

	/*
	 * Assume the output is unsorted, since we don't necessarily have pathkeys
	 * to represent it.  (This might get overridden below.)
	 */
	pathnode->path.pathkeys = NIL;

	pathnode->subpath = subpath;
	pathnode->in_operators = sjinfo->semi_operators;
	pathnode->uniq_exprs = sjinfo->semi_rhs_exprs;

	/*
	 * If the input is a relation and it has a unique index that proves the
	 * semi_rhs_exprs are unique, then we don't need to do anything.  Note
	 * that relation_has_unique_index_for automatically considers restriction
	 * clauses for the rel, as well.
	 */
	if (rel->rtekind == RTE_RELATION && sjinfo->semi_can_btree &&
		relation_has_unique_index_for(root, rel, NIL,
									  sjinfo->semi_rhs_exprs,
									  sjinfo->semi_operators))
	{
		/*
		 * For UNIQUE_PATH_NOOP, it is possible that subpath could be a
		 * motion node. It is not allowed to add a motion node above a
		 * motion node so we simply disallow this unique path although
		 * in theory we could improve this.
		 */
		if (add_motion)
			return NULL;
		pathnode->umethod = UNIQUE_PATH_NOOP;
		pathnode->path.rows = clamp_row_est(rel->rows / numsegments);
		pathnode->path.startup_cost = subpath->startup_cost;
		pathnode->path.total_cost = subpath->total_cost;
		pathnode->path.pathkeys = subpath->pathkeys;

		rel->cheapest_unique_path = (Path *) pathnode;

		MemoryContextSwitchTo(oldcontext);

		return pathnode;
	}

	/*
	 * If the input is a subquery whose output must be unique already, then we
	 * don't need to do anything.  The test for uniqueness has to consider
	 * exactly which columns we are extracting; for example "SELECT DISTINCT
	 * x,y" doesn't guarantee that x alone is distinct. So we cannot check for
	 * this optimization unless semi_rhs_exprs consists only of simple Vars
	 * referencing subquery outputs.  (Possibly we could do something with
	 * expressions in the subquery outputs, too, but for now keep it simple.)
	 */
	if (rel->rtekind == RTE_SUBQUERY)
	{
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

		if (query_supports_distinctness(rte->subquery))
		{
			List	   *sub_tlist_colnos;

			sub_tlist_colnos = translate_sub_tlist(sjinfo->semi_rhs_exprs,
												   rel->relid);

			if (sub_tlist_colnos &&
				query_is_distinct_for(rte->subquery,
									  sub_tlist_colnos,
									  sjinfo->semi_operators))
			{
				/* Subpath node could be a motion. See previous comment for details. */
				if (add_motion)
					return NULL;
				pathnode->umethod = UNIQUE_PATH_NOOP;
				pathnode->path.rows = clamp_row_est(rel->rows / numsegments);
				pathnode->path.startup_cost = subpath->startup_cost;
				pathnode->path.total_cost = subpath->total_cost;
				pathnode->path.pathkeys = subpath->pathkeys;

				rel->cheapest_unique_path = (Path *) pathnode;

				MemoryContextSwitchTo(oldcontext);

				return pathnode;
			}
		}
	}

	/* Estimate number of output rows */
	pathnode->path.rows = estimate_num_groups(root,
											  sjinfo->semi_rhs_exprs,
											  rel->rows,
											  NULL);
	numCols = list_length(sjinfo->semi_rhs_exprs);

	if (sjinfo->semi_can_btree)
	{
		/*
		 * Estimate cost for sort+unique implementation
		 */
		cost_sort(&sort_path, root, NIL,
				  subpath->total_cost,
				  rel->rows / numsegments,
				  subpath->pathtarget->width,
				  0.0,
				  work_mem,
				  -1.0);

		/*
		 * Charge one cpu_operator_cost per comparison per input tuple. We
		 * assume all columns get compared at most of the tuples. (XXX
		 * probably this is an overestimate.)  This should agree with
		 * create_upper_unique_path.
		 */
		sort_path.total_cost += cpu_operator_cost * (rel->rows / numsegments) * numCols;
	}

	if (sjinfo->semi_can_hash)
	{
		/*
		 * Estimate the overhead per hashtable entry at 64 bytes (same as in
		 * planner.c).
		 */
		int			hashentrysize = subpath->pathtarget->width + 64;

		if (hashentrysize * pathnode->path.rows > work_mem * 1024L)
		{
			/*
			 * We should not try to hash.  Hack the SpecialJoinInfo to
			 * remember this, in case we come through here again.
			 */
			sjinfo->semi_can_hash = false;
		}
		else
			cost_agg(&agg_path, root,
					 AGG_HASHED, NULL,
					 numCols, pathnode->path.rows / planner_segment_count(NULL),
					 NIL,
					 subpath->startup_cost,
					 subpath->total_cost,
					 (rel->rows / numsegments),
					 subpath->pathtarget->width);
	}

	if (sjinfo->semi_can_btree && sjinfo->semi_can_hash)
	{
		if (agg_path.total_cost < sort_path.total_cost)
			pathnode->umethod = UNIQUE_PATH_HASH;
		else
			pathnode->umethod = UNIQUE_PATH_SORT;
	}
	else if (sjinfo->semi_can_btree)
		pathnode->umethod = UNIQUE_PATH_SORT;
	else if (sjinfo->semi_can_hash)
		pathnode->umethod = UNIQUE_PATH_HASH;
	else
	{
		/* we can get here only if we abandoned hashing above */
		MemoryContextSwitchTo(oldcontext);
		return NULL;
	}

	if (pathnode->umethod == UNIQUE_PATH_HASH)
	{
		pathnode->path.startup_cost = agg_path.startup_cost;
		pathnode->path.total_cost = agg_path.total_cost;
	}
	else
	{
		pathnode->path.startup_cost = sort_path.startup_cost;
		pathnode->path.total_cost = sort_path.total_cost;
	}

	rel->cheapest_unique_path = (Path *) pathnode;

	MemoryContextSwitchTo(oldcontext);

	/* see MPP-1140 */
	if (pathnode->umethod == UNIQUE_PATH_HASH)
	{
		/* hybrid hash agg is not rescannable, and may present a motion hazard */
		pathnode->path.motionHazard = subpath->motionHazard;
		pathnode->path.rescannable = false;
	}
	else
	{
		/* sort or plain implies materialization and breaks deadlock cycle.
		 *  (NB: Must not reset motionHazard when sort is eliminated due to
		 *  existing ordering; but Unique sort is never optimized away at present.)
		 */
		pathnode->path.motionHazard = subpath->motionHazard;

		/* Same reasoning applies to rescanablilty.  If no actual sort is placed
		 * in the plan, then rescannable is set correctly to the subpath value.
		 * If sort intervenes, it should be set to true.  We depend
		 * on the above claim that sort will always intervene.
		 */
		pathnode->path.rescannable = true;
	}

	return pathnode;
}

/*
 * create_unique_rowid_path (GPDB)
 *
 * Create a UniquePath to deduplicate based on a RowIdExp column. This is
 * used as part of implementing semi-joins (such as "x IN (SELECT ...)").
 *
 * In PostgreSQL, semi-joins are implemented with JOIN_SEMI join types, or
 * by first eliminating duplicates from the inner side, and then performing
 * normal inner join (that's JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER). GPDB
 * has a third way to implement them: Perform an inner join first, and then
 * eliminate duplicates from the result. The JOIN_DEDUP_SEMI and
 * JOIN_DEDUP_SEMI_REVERSE join types indicate such plans.
 *
 * The JOIN_DEDUP_SEMI plan will look something like this:
 *
 * postgres=# explain select * from s where exists (select 1 from r where s.a = r.b);
 *                                                   QUERY PLAN                                                   
 * ---------------------------------------------------------------------------------------------------------------
 *  Gather Motion 3:1  (slice1; segments: 3)  (cost=153.50..155.83 rows=100 width=8)
 *    ->  HashAggregate  (cost=153.50..153.83 rows=34 width=8)
 *          Group Key: (RowIdExpr)
 *          ->  Redistribute Motion 3:3  (slice2; segments: 3)  (cost=11.75..153.00 rows=34 width=8)
 *                Hash Key: (RowIdExpr)
 *                ->  Hash Join  (cost=11.75..151.00 rows=34 width=8)
 *                      Hash Cond: (r.b = s.a)
 *                      ->  Seq Scan on r  (cost=0.00..112.00 rows=3334 width=4)
 *                      ->  Hash  (cost=8.00..8.00 rows=100 width=8)
 *                            ->  Broadcast Motion 3:3  (slice3; segments: 3)  (cost=0.00..8.00 rows=100 width=8)
 *                                  ->  Seq Scan on s  (cost=0.00..4.00 rows=34 width=8)
 *  Optimizer: Postgres query optimizer
 * (12 rows)
 *
 * In PostgreSQL, this is never better than doing a JOIN_SEMI directly.
 * But it can be a win in GPDB, if the distribution of the outer and inner
 * relations don't match, and the outer relation is much larger than the
 * inner relation. In the above example, a normal semi-join would have to
 * have 's' on the outer side, and 'r' on the inner side. A hash semi-join
 * can't be performed the other way 'round, because the duplicate
 * elimination in a semi-join is done when building the hash table.
 * Furthermore, you can't have a Broadcast motion on the outer side of
 * a semi-join, because that could also generate duplicates. That leaves
 * the planner no choice, but to redistribute the larger 'r' relation,
 * in a JOIN_SEMI plan.
 *
 * So in GPDB, we try to implement semi-joins as a inner joins, followed
 * by an explicit UniquePath to eliminate the duplicates. That allows the
 * above plan, where the smaller 's' relation is Broadcast to all the
 * segments, and the duplicates that can arise from doing that are eliminated
 * above the join. You get one more Motion than with a JOIN_SEMI plan, but
 * each Motion has to move much fewer rows.
 *
 * The role of this function is to insert the UniquePath to represent
 * the deduplication above the join. Returns a UniquePath node representing
 * a "DISTINCT ON (RowIdExpr)" operator, where (r1,...,rn) represents a unique
 * identifier for each row of the cross product of the tables specified by
 * the 'distinct_relids' parameter.
 *
 * NB: The returned node shares the given 'distinct_relids' bitmapset object;
 * so the caller must not free or modify it during the node's lifetime.
 *
 * If a row's duplicates might occur in more than one partition, a Motion
 * operator will be needed to bring them together.  Since this path might
 * not be chosen, we won't take the time to create a CdbMotionPath node here.
 * Just estimate what the cost would be, and assign a dummy locus; leave
 * the real work for create_plan().
 */
UniquePath *
create_unique_rowid_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path        *subpath,
						 Relids       required_outer,
						 int          rowidexpr_id)
{
	UniquePath *pathnode;
	CdbPathLocus locus;
	Path		sort_path;		/* dummy for result of cost_sort */
	Path		agg_path;		/* dummy for result of cost_agg */
	int			numCols;
	bool		all_btree;
	bool		all_hash;
	double		numsegments;

	Assert(rowidexpr_id > 0);

	/*
	 * For easier merging (albeit it's going to manual), keep this function
	 * similar to create_unique_path(). In this function, we deduplicate based
	 * on RowIdExpr that we generate on the fly. Sorting and hashing are both
	 * possible, but we keep these as variables to resemble
	 * create_unique_path().
	 */
	all_btree = true;
	all_hash = enable_hashagg;	/* don't consider hash if not enabled */

	RowIdExpr *rowidexpr = makeNode(RowIdExpr);
	rowidexpr->rowidexpr_id = rowidexpr_id;

	subpath->pathtarget = copy_pathtarget(subpath->pathtarget);
	add_column_to_pathtarget(subpath->pathtarget, (Expr *) rowidexpr, 0);

	/* Repartition first if duplicates might be on different QEs. */
	if (!CdbPathLocus_IsBottleneck(subpath->locus))
	{
		int			numsegments = CdbPathLocus_NumSegments(subpath->locus);

		locus = cdbpathlocus_from_exprs(root,
										subpath->parent,
										list_make1(rowidexpr),
										list_make1_oid(cdb_default_distribution_opfamily_for_type(INT8OID)),
										list_make1_int(0),
										numsegments);
		subpath = cdbpath_create_motion_path(root, subpath, NIL, false, locus);
		if (!subpath)
			return NULL;

		/*
		 * The motion path has been created correctly, but there's a little
		 * problem with the locus. The locus has RowIdExpr as the distribution
		 * key, but because there are no Vars in it, the EC machinery will
		 * consider it a pseudo-constant. We don't want that, as it would
		 * mean that all rows were considered to live on the same segment,
		 * which is not how this works. Therefore set the locus of the Unique
		 * path to Strewn, which doesn't have that problem. No node above the
		 * Unique will care about the row id expresssion, so it's OK to forget
		 * that the rows are currently hashed by the row id.
		 */
		CdbPathLocus_MakeStrewn(&locus, numsegments);
	}
	else
	{
		/* XXX If the join result is on a single node, a DEDUP plan probably doesn't
		 * make sense.
		 */
		locus = subpath->locus;
	}

	if (CdbPathLocus_IsPartitioned(locus))
		numsegments = CdbPathLocus_NumSegments(locus);
	else
		numsegments = 1;

	/*
	 * Start building the result Path object.
	 */
	pathnode = makeNode(UniquePath);

	pathnode->path.pathtype = T_Unique;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.locus = locus;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;

	/*
	 * Treat the output as always unsorted, since we don't necessarily have
	 * pathkeys to represent it.
	 */
	pathnode->path.pathkeys = NIL;

	pathnode->subpath = subpath;
	pathnode->in_operators = list_make1_oid(Int8EqualOperator);
	pathnode->uniq_exprs = list_make1(rowidexpr);

	/*
	 * This just removes duplicates generated by broadcasting rows earlier.
	 */
	pathnode->path.rows = clamp_row_est(rel->rows / numsegments);
	numCols = 1;		/* the RowIdExpr */

	if (all_btree)
	{
		/*
		 * Estimate cost for sort+unique implementation
		 */
		cost_sort(&sort_path, root, NIL,
				  subpath->total_cost,
				  (rel->rows / numsegments),
				  rel->reltarget->width,
				  0, work_mem,
				  -1.0);

		/*
		 * Charge one cpu_operator_cost per comparison per input tuple. We
		 * assume all columns get compared at most of the tuples. (XXX
		 * probably this is an overestimate.)  This should agree with
		 * make_unique.
		 */
		sort_path.total_cost += cpu_operator_cost * (rel->rows / numsegments) * numCols;
	}

	if (all_hash)
	{
		/*
		 * Estimate the overhead per hashtable entry at 64 bytes (same as in
		 * planner.c).
		 */
		int			hashentrysize = subpath->pathtarget->width + 64;

		if (hashentrysize * pathnode->path.rows > work_mem * 1024L)
			all_hash = false;	/* don't try to hash */
		else
			cost_agg(&agg_path, root,
					 AGG_HASHED, 0,
					 numCols, ((Path *)pathnode)->rows,
					 NIL, /* no quals */
					 subpath->startup_cost,
					 subpath->total_cost,
					 (rel->rows / numsegments),
					 false /* streaming */
				);
	}

	if (all_btree && all_hash)
	{
		if (agg_path.total_cost < sort_path.total_cost)
			pathnode->umethod = UNIQUE_PATH_HASH;
		else
			pathnode->umethod = UNIQUE_PATH_SORT;
	}
	else if (all_btree)
		pathnode->umethod = UNIQUE_PATH_SORT;
	else if (all_hash)
		pathnode->umethod = UNIQUE_PATH_HASH;
	else
	{
		Assert(false);
	}

	if (pathnode->umethod == UNIQUE_PATH_HASH)
	{
		pathnode->path.startup_cost = agg_path.startup_cost;
		pathnode->path.total_cost = agg_path.total_cost;
	}
	else
	{
		pathnode->path.startup_cost = sort_path.startup_cost;
		pathnode->path.total_cost = sort_path.total_cost;
	}

	/* see MPP-1140 */
	if (pathnode->umethod == UNIQUE_PATH_HASH)
	{
		/* hybrid hash agg is not rescannable, and may present a motion hazard */
		pathnode->path.motionHazard = subpath->motionHazard;
		pathnode->path.rescannable = false;
	}
	else
	{
		/* sort or plain implies materialization and breaks deadlock cycle.
		 *  (NB: Must not reset motionHazard when sort is eliminated due to
		 *  existing ordering; but Unique sort is never optimized away at present.)
		 */
		pathnode->path.motionHazard = subpath->motionHazard;

		/* Same reasoning applies to rescanablilty.  If no actual sort is placed
		 * in the plan, then rescannable is set correctly to the subpath value.
		 * If sort intervenes, it should be set to true.  We depend
		 * on the above claim that sort will always intervene.
		 */
		pathnode->path.rescannable = true;
	}

	return pathnode;
}                               /* create_unique_rowid_path */

/*
 * create_gather_merge_path
 *
 *	  Creates a path corresponding to a gather merge scan, returning
 *	  the pathnode.
 */
GatherMergePath *
create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
						 PathTarget *target, List *pathkeys,
						 Relids required_outer, double *rows)
{
	GatherMergePath *pathnode = makeNode(GatherMergePath);
	Cost		input_startup_cost = 0;
	Cost		input_total_cost = 0;

	Assert(subpath->parallel_safe);
	Assert(pathkeys);

	pathnode->path.pathtype = T_GatherMerge;
	pathnode->path.parent = rel;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;

	pathnode->subpath = subpath;
	pathnode->num_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->path.pathtarget = target ? target : rel->reltarget;
	pathnode->path.rows += subpath->rows;

	if (pathkeys_contained_in(pathkeys, subpath->pathkeys))
	{
		/* Subpath is adequately ordered, we won't need to sort it */
		input_startup_cost += subpath->startup_cost;
		input_total_cost += subpath->total_cost;
	}
	else
	{
		/* We'll need to insert a Sort node, so include cost for that */
		Path		sort_path;	/* dummy for result of cost_sort */

		cost_sort(&sort_path,
				  root,
				  pathkeys,
				  subpath->total_cost,
				  subpath->rows,
				  subpath->pathtarget->width,
				  0.0,
				  work_mem,
				  -1);
		input_startup_cost += sort_path.startup_cost;
		input_total_cost += sort_path.total_cost;
	}

	cost_gather_merge(pathnode, root, rel, pathnode->path.param_info,
					  input_startup_cost, input_total_cost, rows);

	return pathnode;
}

/*
 * translate_sub_tlist - get subquery column numbers represented by tlist
 *
 * The given targetlist usually contains only Vars referencing the given relid.
 * Extract their varattnos (ie, the column numbers of the subquery) and return
 * as an integer List.
 *
 * If any of the tlist items is not a simple Var, we cannot determine whether
 * the subquery's uniqueness condition (if any) matches ours, so punt and
 * return NIL.
 */
static List *
translate_sub_tlist(List *tlist, int relid)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, tlist)
	{
		Var		   *var = (Var *) lfirst(l);

		if (!var || !IsA(var, Var) ||
			var->varno != relid)
			return NIL;			/* punt */

		result = lappend_int(result, var->varattno);
	}
	return result;
}

/*
 * create_gather_path
 *	  Creates a path corresponding to a gather scan, returning the
 *	  pathnode.
 *
 * 'rows' may optionally be set to override row estimates from other sources.
 */
GatherPath *
create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
				   PathTarget *target, Relids required_outer, double *rows)
{
	GatherPath *pathnode = makeNode(GatherPath);

	Assert(subpath->parallel_safe);

	pathnode->path.pathtype = T_Gather;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;	/* Gather has unordered result */

	pathnode->subpath = subpath;
	pathnode->num_workers = subpath->parallel_workers;
	pathnode->single_copy = false;

	if (pathnode->num_workers == 0)
	{
		pathnode->path.pathkeys = subpath->pathkeys;
		pathnode->num_workers = 1;
		pathnode->single_copy = true;
	}

	cost_gather(pathnode, root, rel, pathnode->path.param_info, rows);

	/* GPDB_96_MERGE_FIXME: how do data distribution locus and parallelism work together? */
	pathnode->path.locus = subpath->locus;

	return pathnode;
}

/*
 * create_subqueryscan_path
 *	  Creates a path corresponding to a scan of a subquery,
 *	  returning the pathnode.
 */
SubqueryScanPath *
create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
						 List *pathkeys, CdbPathLocus locus, Relids required_outer)
{
	SubqueryScanPath *pathnode = makeNode(SubqueryScanPath);

	pathnode->path.pathtype = T_SubqueryScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->subpath = subpath;

	pathnode->path.locus = locus;
	pathnode->path.motionHazard = subpath->motionHazard;
	pathnode->path.rescannable = false;
	pathnode->path.sameslice_relids = NULL;

	pathnode->required_outer = bms_copy(required_outer);
	cost_subqueryscan(pathnode, root, rel, pathnode->path.param_info);

	return pathnode;
}

/*
 * create_functionscan_path
 *	  Creates a path corresponding to a sequential scan of a function,
 *	  returning the pathnode.
 */
Path *
create_functionscan_path(PlannerInfo *root, RelOptInfo *rel,
						 RangeTblEntry *rte,
						 List *pathkeys, Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);
	ListCell   *lc;

	pathnode->pathtype = T_FunctionScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = pathkeys;

	/*
	 * Decide where to execute the FunctionScan.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char		exec_location = PROEXECLOCATION_ANY;
		bool		contain_mutables = false;
		bool		contain_outer_params = false;

		/*
		 * If the function desires to run on segments, mark randomly-distributed.
		 * If expression contains mutable functions, evaluate it on entry db.
		 * Otherwise let it be evaluated in the same slice as its parent operator.
		 */
		Assert(rte->rtekind == RTE_FUNCTION);

		foreach (lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			if (rinfo->contain_outer_query_references)
			{
				contain_outer_params = true;
				break;
			}
		}

		foreach (lc, rte->functions)
		{
			RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

			if (rtfunc->funcexpr && IsA(rtfunc->funcexpr, FuncExpr))
			{
				FuncExpr   *funcexpr = (FuncExpr *) rtfunc->funcexpr;
				char		this_exec_location;

				this_exec_location = func_exec_location(funcexpr->funcid);

				switch (this_exec_location)
				{
					case PROEXECLOCATION_ANY:
						/*
						 * This can be executed anywhere. Remember if it was
						 * mutable (or contained any mutable arguments), that
						 * will affect the decision after this loop on where
						 * to actually execute it.
						 */
						if (!contain_mutables)
							contain_mutables = contain_mutable_functions((Node *) funcexpr);
						break;
					case PROEXECLOCATION_COORDINATOR:
						/*
						 * This function forces the execution to coordinator.
						 */
						if (exec_location == PROEXECLOCATION_ALL_SEGMENTS)
						{
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 (errmsg("cannot mix EXECUTE ON COORDINATOR and ALL SEGMENTS functions in same function scan"))));
						}
						exec_location = PROEXECLOCATION_COORDINATOR;
						break;
					case PROEXECLOCATION_INITPLAN:
						/*
						 * This function forces the execution to coordinator.
						 */
						if (exec_location == PROEXECLOCATION_ALL_SEGMENTS)
						{
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 (errmsg("cannot mix EXECUTE ON INITPLAN and ALL SEGMENTS functions in same function scan"))));
						}
						exec_location = PROEXECLOCATION_INITPLAN;
						break;
					case PROEXECLOCATION_ALL_SEGMENTS:
						/*
						 * This function forces the execution to segments.
						 */
						if (exec_location == PROEXECLOCATION_COORDINATOR)
						{
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 (errmsg("cannot mix EXECUTE ON COORDINATOR and ALL SEGMENTS functions in same function scan"))));
						}
						exec_location = PROEXECLOCATION_ALL_SEGMENTS;
						break;
					default:
						elog(ERROR, "unrecognized proexeclocation '%c'", exec_location);
				}
			}
			else
			{
				/*
				 * The expression might've been simplified into a Const. Which can
				 * be executed anywhere.
				 */
			}

			if (!contain_outer_params &&
				contains_outer_params(rtfunc->funcexpr, root))
				contain_outer_params = true;
		}

		switch (exec_location)
		{
			case PROEXECLOCATION_ANY:
				/*
				 * If all the functions are ON ANY, we presumably could execute
				 * the function scan anywhere. However, historically, before the
				 * EXECUTE ON syntax was introduced, we always executed
				 * non-IMMUTABLE functions on the coordinator. Keep that behavior
				 * for backwards compatibility.
				 */
				if (contain_outer_params)
					CdbPathLocus_MakeOuterQuery(&pathnode->locus);
				else if (contain_mutables)
					CdbPathLocus_MakeEntry(&pathnode->locus);
				else
					CdbPathLocus_MakeGeneral(&pathnode->locus);
				break;
			case PROEXECLOCATION_COORDINATOR:
				if (contain_outer_params)
					elog(ERROR, "cannot execute EXECUTE ON COORDINATOR function in a subquery with arguments from outer query");
				CdbPathLocus_MakeEntry(&pathnode->locus);
				break;
			case PROEXECLOCATION_INITPLAN:
				if (contain_outer_params)
					elog(ERROR, "cannot execute EXECUTE ON INITPLAN function in a subquery with arguments from outer query");
				CdbPathLocus_MakeEntry(&pathnode->locus);
				break;
			case PROEXECLOCATION_ALL_SEGMENTS:
				if (contain_outer_params)
					elog(ERROR, "cannot execute EXECUTE ON ALL SEGMENTS function in a subquery with arguments from outer query");
				CdbPathLocus_MakeStrewn(&pathnode->locus,
										getgpsegmentCount());
				break;
			default:
				elog(ERROR, "unrecognized proexeclocation '%c'", exec_location);
		}
	}
	else
		CdbPathLocus_MakeEntry(&pathnode->locus);

	pathnode->motionHazard = false;

	/*
	 * FunctionScan is always rescannable. It uses a tuplestore to
	 * materialize the results all by itself.
	 */
	pathnode->rescannable = true;

	pathnode->sameslice_relids = NULL;

	cost_functionscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_tablefunction_path
 *	  Creates a path corresponding to a sequential scan of a table function,
 *	  returning the pathnode.
 *
 * NB: This is a GPDB specific thing, to support this syntax:
 *
 *   SELECT * FROM multiset_5( TABLE( SELECT * from example) ) order by a, b;
 *
 * Despite the similar name, this is completely different from the upstream
 * create_tablefuncscan_path() function below! The other function deals with
 * XMLTABLE and similar functions.
 */
TableFunctionScanPath *
create_tablefunction_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
						  List *pathkeys, Relids required_outer)
{
	TableFunctionScanPath *pathnode = makeNode(TableFunctionScanPath);

	/* Setup the basics of the TableFunction path */
	pathnode->path.pathtype = T_TableFunctionScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys	   = NIL;		/* no way to specify output ordering */
	pathnode->subpath = subpath;

	pathnode->path.motionHazard = true;      /* better safe than sorry */
	pathnode->path.rescannable  = false;     /* better safe than sorry */

	/*
	 * Inherit the locus of the input subquery's path.  This is necessary to handle the
	 * case of a General locus, e.g. if all the data has been concentrated to a
	 * single segment then the output will all be on that segment, otherwise the
	 * output must be declared as randomly distributed because we do not know
	 * what relationship, if any, there is between the input data and the output
	 * data.
	 */
	pathnode->path.locus = subpath->locus;

	/* Mark the output as random if the input is partitioned */
	if (CdbPathLocus_IsPartitioned(pathnode->path.locus))
		CdbPathLocus_MakeStrewn(&pathnode->path.locus,
								CdbPathLocus_NumSegments(pathnode->path.locus));
	pathnode->path.sameslice_relids = NULL;

	cost_tablefunction(pathnode, root, rel, pathnode->path.param_info);

	return pathnode;
}

/*
 * create_tablefuncscan_path
 *	  Creates a path corresponding to a sequential scan of a table function,
 *	  returning the pathnode.
 */
Path *
create_tablefuncscan_path(PlannerInfo *root, RelOptInfo *rel,
						  Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_TableFuncScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */
	CdbPathLocus_MakeGeneral(&pathnode->locus);

	cost_tablefuncscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_valuesscan_path
 *	  Creates a path corresponding to a scan of a VALUES list,
 *	  returning the pathnode.
 */
Path *
create_valuesscan_path(PlannerInfo *root, RelOptInfo *rel,
					   RangeTblEntry *rte,
					   Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_ValuesScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	/*
	 * CDB: If VALUES list contains mutable functions, evaluate it on entry db.
	 * Otherwise let it be evaluated in the same slice as its parent operator.
	 */
	Assert(rte->rtekind == RTE_VALUES);
	if (contain_mutable_functions((Node *)rte->values_lists))
		CdbPathLocus_MakeEntry(&pathnode->locus);
	else
	{
		/*
		 * ValuesScan can be on any segment.
		 */
		CdbPathLocus_MakeGeneral(&pathnode->locus);
	}

	pathnode->motionHazard = false;
	pathnode->rescannable = true;
	pathnode->sameslice_relids = NULL;

	cost_valuesscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_ctescan_path
 *	  Creates a path corresponding to a scan of a non-self-reference CTE,
 *	  returning the pathnode.
 */
Path *
create_ctescan_path(PlannerInfo *root, RelOptInfo *rel,
					Path *subpath, CdbPathLocus locus,
					List *pathkeys,
					Relids required_outer)
{
	CtePath	   *ctepath = makeNode(CtePath);
	Path	   *pathnode = &ctepath->path;

	pathnode->pathtype = T_CteScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = pathkeys;
	pathnode->locus = locus;

	pathnode->sameslice_relids = NULL;

	/*
	 * GPDB: we do have the subpath, at least if it's not a shared cte.
	 */
	if (subpath)
	{
		/* copy the cost estimates from the subpath */
		double		numsegments;

		if (CdbPathLocus_IsPartitioned(locus))
			numsegments = CdbPathLocus_NumSegments(locus);
		else
			numsegments = 1;

		pathnode->rows = clamp_row_est(rel->rows / numsegments);
		pathnode->startup_cost = subpath->startup_cost;
		pathnode->total_cost = subpath->total_cost;

		pathnode->motionHazard = subpath->motionHazard;
		pathnode->rescannable = subpath->rescannable;

		ctepath->subpath = subpath;
	}
	else
	{
		/*
	 	 * We can't extract these two values from the subplan, so we simple set
	 	 * them to their worst case here.
		 */
		pathnode->motionHazard = true;
		pathnode->rescannable = false;
		/* Shared scan. We'll use the cost estimates from the CTE rel. */
		cost_ctescan(pathnode, root, rel, pathnode->param_info);
	}

	return pathnode;
}

/*
 * create_namedtuplestorescan_path
 *	  Creates a path corresponding to a scan of a named tuplestore, returning
 *	  the pathnode.
 */
Path *
create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
								Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_NamedTuplestoreScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	cost_namedtuplestorescan(pathnode, root, rel, pathnode->param_info);

	/*
	 * When this is used in triggers that run on QEs, the locus is ignored
	 * and the scan is executed locally on the QE anyway. On QD, it's not
	 * clear if named tuplestores are populated correctly in triggers, but if
	 * it does work t all, Entry seems most appropriate.
	 */
	CdbPathLocus_MakeEntry(&pathnode->locus);

	return pathnode;
}

/*
 * create_resultscan_path
 *	  Creates a path corresponding to a scan of an RTE_RESULT relation,
 *	  returning the pathnode.
 */
Path *
create_resultscan_path(PlannerInfo *root, RelOptInfo *rel,
					   Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);

	pathnode->pathtype = T_Result;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	{
		char		exec_location;
		exec_location = check_execute_on_functions((Node *) rel->reltarget->exprs);

		/*
		 * A function with EXECUTE ON { COORDINATOR | ALL SEGMENTS } attribute
		 * must be a set-returning function, a subquery has set-returning 
		 * functions in tlist can't be pulled up as RTE_RESULT relation.
		 */
		Assert(exec_location == PROEXECLOCATION_ANY);
		CdbPathLocus_MakeGeneral(&pathnode->locus);
	}

	cost_resultscan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

/*
 * create_worktablescan_path
 *	  Creates a path corresponding to a scan of a self-reference CTE,
 *	  returning the pathnode.
 */
Path *
create_worktablescan_path(PlannerInfo *root, RelOptInfo *rel,
						  CdbPathLocus ctelocus,
						  Relids required_outer)
{
	Path	   *pathnode = makeNode(Path);
	CdbPathLocus result;

	/*
	 * Between Recursive union plannode node and WorkTableScan plannode
	 * there must be no Motion nodes because the execution of WorkTableScan
	 * depends on the Recursive union's data structure.
	 *
	 * To avoid Motion nodes, we set the locus of the WorkTableScan to Strewn
	 * for certain cases. For example, if the locus of the non-recursive path of
	 * the CTE is Hashed, we need to set the locus of the WorkTableScan to Strewn
	 * instead of Hashed. Otherwise, if the WorkTableScan is part of a JOIN, we
	 * could end up redistribute the other side (always inner side for now) of
	 * the JOIN with incorrect hash keys.
	 */
	if (ctelocus.locustype == CdbLocusType_Entry)
		CdbPathLocus_MakeEntry(&result);
	else if (ctelocus.locustype == CdbLocusType_SingleQE)
		CdbPathLocus_MakeSingleQE(&result, ctelocus.numsegments);
	else if (ctelocus.locustype == CdbLocusType_OuterQuery)
		CdbPathLocus_MakeOuterQuery(&result);
	else if (ctelocus.locustype == CdbLocusType_SegmentGeneral
				|| ctelocus.locustype == CdbLocusType_General)
	{
		/* See comments in set_worktable_pathlist */
		elog(ERROR,
			 "worktable scan path can never have "
			 "segmentgeneral or general locus.");
	}
	else
		CdbPathLocus_MakeStrewn(&result, ctelocus.numsegments);

	pathnode->pathtype = T_WorkTableScan;
	pathnode->parent = rel;
	pathnode->pathtarget = rel->reltarget;
	pathnode->param_info = get_baserel_parampathinfo(root, rel,
													 required_outer);
	pathnode->parallel_aware = false;
	pathnode->parallel_safe = rel->consider_parallel;
	pathnode->parallel_workers = 0;
	pathnode->pathkeys = NIL;	/* result is always unordered */

	pathnode->locus = result;
	pathnode->motionHazard = false;
	pathnode->rescannable = true;
	pathnode->sameslice_relids = rel->relids;

	/* Cost is the same as for a regular CTE scan */
	cost_ctescan(pathnode, root, rel, pathnode->param_info);

	return pathnode;
}

bool
path_contains_inner_index(Path *path)
{

	if (IsA(path, IndexPath))
		return true;
	else if (IsA(path, BitmapHeapPath))
		return true;
	else if (IsA(path, AppendPath))
	{
		/* MPP-2377: Append paths may conceal inner-index scans, if
		 * any of the subpaths are indexpaths or bitmapheap-paths we
		 * have to do more checking */
		ListCell   *l;

		/* scan the subpaths of the Append */
		foreach(l, ((AppendPath *)path)->subpaths)
		{
			Path	   *subpath = (Path *)lfirst(l);

			if (path_contains_inner_index(subpath))
				return true;
		}
	}

	return false;
}

/*
 * create_foreignscan_path
 *	  Creates a path corresponding to a scan of a foreign base table,
 *	  returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignPaths function of a foreign data wrapper.
 * We make the FDW supply all fields of the path, since we do not have any way
 * to calculate them in core.  However, there is a usually-sane default for
 * the pathtarget (rel->reltarget), so we let a NULL for "target" select that.
 */
ForeignPath *
create_foreignscan_path(PlannerInfo *root, RelOptInfo *rel,
						PathTarget *target,
						double rows, Cost startup_cost, Cost total_cost,
						List *pathkeys,
						Relids required_outer,
						Path *fdw_outerpath,
						List *fdw_private)
{
	ForeignPath *pathnode = makeNode(ForeignPath);

	/* Historically some FDWs were confused about when to use this */
	Assert(IS_SIMPLE_REL(rel));

	pathnode->path.pathtype = T_ForeignScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target ? target : rel->reltarget;
	pathnode->path.param_info = get_baserel_parampathinfo(root, rel,
														  required_outer);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.rows = rows;
	pathnode->path.startup_cost = startup_cost;
	pathnode->path.total_cost = total_cost;
	pathnode->path.pathkeys = pathkeys;
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		switch (rel->exec_location)
		{
			case FTEXECLOCATION_ANY:
				CdbPathLocus_MakeGeneral(&(pathnode->path.locus));
				break;
			case FTEXECLOCATION_ALL_SEGMENTS:
				pathnode->path.locus = cdbpathlocus_from_baserel(root, rel);
				break;
			case FTEXECLOCATION_COORDINATOR:
				CdbPathLocus_MakeEntry(&(pathnode->path.locus));
				break;
			default:
				elog(ERROR, "unrecognized exec_location '%c'", rel->exec_location);
		}
	}
	else
	{
		/* make entry locus for utility role */
		CdbPathLocus_MakeEntry(&(pathnode->path.locus));
	}
	pathnode->fdw_outerpath = fdw_outerpath;
	pathnode->fdw_private = fdw_private;

	return pathnode;
}

/*
 * create_foreign_join_path
 *	  Creates a path corresponding to a scan of a foreign join,
 *	  returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected
 * to be called by the GetForeignJoinPaths function of a foreign data wrapper.
 * We make the FDW supply all fields of the path, since we do not have any way
 * to calculate them in core.  However, there is a usually-sane default for
 * the pathtarget (rel->reltarget), so we let a NULL for "target" select that.
 */
ForeignPath *
create_foreign_join_path(PlannerInfo *root, RelOptInfo *rel,
						 PathTarget *target,
						 double rows, Cost startup_cost, Cost total_cost,
						 List *pathkeys,
						 Relids required_outer,
						 Path *fdw_outerpath,
						 List *fdw_private)
{
	ForeignPath *pathnode = makeNode(ForeignPath);

	/*
	 * We should use get_joinrel_parampathinfo to handle parameterized paths,
	 * but the API of this function doesn't support it, and existing
	 * extensions aren't yet trying to build such paths anyway.  For the
	 * moment just throw an error if someone tries it; eventually we should
	 * revisit this.
	 */
	if (!bms_is_empty(required_outer) || !bms_is_empty(rel->lateral_relids))
		elog(ERROR, "parameterized foreign joins are not supported yet");

	pathnode->path.pathtype = T_ForeignScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target ? target : rel->reltarget;
	pathnode->path.param_info = NULL;	/* XXX see above */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.rows = rows;
	pathnode->path.startup_cost = startup_cost;
	pathnode->path.total_cost = total_cost;
	pathnode->path.pathkeys = pathkeys;
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		ForeignServer *server = NULL;

		switch (rel->exec_location)
		{
			case FTEXECLOCATION_ANY:
				CdbPathLocus_MakeGeneral(&(pathnode->path.locus));
				break;
			case FTEXECLOCATION_ALL_SEGMENTS:
				server = GetForeignServer(rel->serverid);
				if (server)
					CdbPathLocus_MakeStrewn(&(pathnode->path.locus), server->num_segments);
				else
					CdbPathLocus_MakeStrewn(&(pathnode->path.locus), getgpsegmentCount());
				break;
			case FTEXECLOCATION_COORDINATOR:
				CdbPathLocus_MakeEntry(&(pathnode->path.locus));
				break;
			default:
				elog(ERROR, "unrecognized exec_location '%c'", rel->exec_location);
		}
	}
	else
	{
		/* make entry locus for utility role */
		CdbPathLocus_MakeEntry(&(pathnode->path.locus));
	}
	pathnode->fdw_outerpath = fdw_outerpath;
	pathnode->fdw_private = fdw_private;

	return pathnode;
}

/*
 * create_foreign_upper_path
 *	  Creates a path corresponding to an upper relation that's computed
 *	  directly by an FDW, returning the pathnode.
 *
 * This function is never called from core Postgres; rather, it's expected to
 * be called by the GetForeignUpperPaths function of a foreign data wrapper.
 * We make the FDW supply all fields of the path, since we do not have any way
 * to calculate them in core.  However, there is a usually-sane default for
 * the pathtarget (rel->reltarget), so we let a NULL for "target" select that.
 */
ForeignPath *
create_foreign_upper_path(PlannerInfo *root, RelOptInfo *rel,
						  PathTarget *target,
						  double rows, Cost startup_cost, Cost total_cost,
						  List *pathkeys,
						  Path *fdw_outerpath,
						  List *fdw_private)
{
	ForeignPath *pathnode = makeNode(ForeignPath);
	/*
	 * Upper relations should never have any lateral references, since joining
	 * is complete.
	 */
	Assert(bms_is_empty(rel->lateral_relids));

	pathnode->path.pathtype = T_ForeignScan;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target ? target : rel->reltarget;
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel;
	pathnode->path.parallel_workers = 0;
	pathnode->path.rows = rows;
	pathnode->path.startup_cost = startup_cost;
	pathnode->path.total_cost = total_cost;
	pathnode->path.pathkeys = pathkeys;
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		ForeignServer *server = NULL;

		switch (rel->exec_location)
		{
			case FTEXECLOCATION_ANY:
				CdbPathLocus_MakeGeneral(&(pathnode->path.locus));
				break;
			case FTEXECLOCATION_ALL_SEGMENTS:
				server = GetForeignServer(rel->serverid);
				if (server)
					CdbPathLocus_MakeStrewn(&(pathnode->path.locus), server->num_segments);
				else
					CdbPathLocus_MakeStrewn(&(pathnode->path.locus), getgpsegmentCount());
				break;
			case FTEXECLOCATION_COORDINATOR:
				CdbPathLocus_MakeEntry(&(pathnode->path.locus));
				break;
			default:
				elog(ERROR, "unrecognized exec_location '%c'", rel->exec_location);
		}
	}
	else
	{
		/* make entry locus for utility role */
		CdbPathLocus_MakeEntry(&(pathnode->path.locus));
	}
	pathnode->fdw_outerpath = fdw_outerpath;
	pathnode->fdw_private = fdw_private;

	return pathnode;
}

/*
 * calc_nestloop_required_outer
 *	  Compute the required_outer set for a nestloop join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_nestloop_required_outer(Relids outerrelids,
							 Relids outer_paramrels,
							 Relids innerrelids,
							 Relids inner_paramrels)
{
	Relids		required_outer;

	/* inner_path can require rels from outer path, but not vice versa */
	Assert(!bms_overlap(outer_paramrels, innerrelids));
	/* easy case if inner path is not parameterized */
	if (!inner_paramrels)
		return bms_copy(outer_paramrels);
	/* else, form the union ... */
	required_outer = bms_union(outer_paramrels, inner_paramrels);
	/* ... and remove any mention of now-satisfied outer rels */
	required_outer = bms_del_members(required_outer,
									 outerrelids);
	/* maintain invariant that required_outer is exactly NULL if empty */
	if (bms_is_empty(required_outer))
	{
		bms_free(required_outer);
		required_outer = NULL;
	}
	return required_outer;
}

/*
 * calc_non_nestloop_required_outer
 *	  Compute the required_outer set for a merge or hash join path
 *
 * Note: result must not share storage with either input
 */
Relids
calc_non_nestloop_required_outer(Path *outer_path, Path *inner_path)
{
	Relids		outer_paramrels = PATH_REQ_OUTER(outer_path);
	Relids		inner_paramrels = PATH_REQ_OUTER(inner_path);
	Relids		required_outer;

	/* neither path can require rels from the other */
	Assert(!bms_overlap(outer_paramrels, inner_path->parent->relids));
	Assert(!bms_overlap(inner_paramrels, outer_path->parent->relids));
	/* form the union ... */
	required_outer = bms_union(outer_paramrels, inner_paramrels);
	/* we do not need an explicit test for empty; bms_union gets it right */
	return required_outer;
}

/*
 * create_nestloop_path
 *	  Creates a pathnode corresponding to a nestloop join between two
 *	  relations.
 *
 * 'joinrel' is the join relation.
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_nestloop
 * 'extra' contains various information about the join
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 *
 * Returns the resulting path node.
 */
Path *
create_nestloop_path(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 JoinType jointype,
					 JoinType orig_jointype,		/* CDB */
					 JoinCostWorkspace *workspace,
					 JoinPathExtraData *extra,
					 Path *outer_path,
					 Path *inner_path,
					 List *restrict_clauses,
					 List *redistribution_clauses,	/* CDB */
					 List *pathkeys,
					 Relids required_outer)
{
	NestPath   *pathnode;
	CdbPathLocus join_locus;
	Relids		outer_req_outer = PATH_REQ_OUTER(outer_path);
	bool		outer_must_be_local = !bms_is_empty(outer_req_outer);
	Relids		inner_req_outer = PATH_REQ_OUTER(inner_path);
	bool		inner_must_be_local = !bms_is_empty(inner_req_outer);
	int			rowidexpr_id;

	/* Add motion nodes above subpaths and decide where to join. */
	join_locus = cdbpath_motion_for_join(root,
										 orig_jointype,
										 &outer_path,       /* INOUT */
										 &inner_path,       /* INOUT */
										 &rowidexpr_id,		/* OUT */
										 redistribution_clauses,
										 restrict_clauses,
										 pathkeys,
										 NIL,
										 outer_must_be_local,
										 inner_must_be_local);
	if (CdbPathLocus_IsNull(join_locus))
		return NULL;

	/* Outer might not be ordered anymore after motion. */
	if (!outer_path->pathkeys)
		pathkeys = NIL;

	/*
	 * If this join path is parameterized by a parameter above this path, then
	 * this path needs to be rescannable. A NestLoop is rescannable, when both
	 * outer and inner paths rescannable, so make them both rescannable.
	 */
	if (!outer_path->rescannable && !bms_is_empty(required_outer))
	{
		MaterialPath *matouter = create_material_path(root, outer_path->parent, outer_path);

		matouter->cdb_shield_child_from_rescans = true;

		outer_path = (Path *) matouter;
	}

	/*
	 * If outer has at most one row, NJ will make at most one pass over inner.
	 * Else materialize inner rel after motion so NJ can loop over results.
	 */
	if (!inner_path->rescannable && !bms_is_empty(required_outer))
	{
		/*
		 * NLs potentially rescan the inner; if our inner path
		 * isn't rescannable we have to add a materialize node
		 */
		MaterialPath *matinner = create_material_path(root, inner_path->parent, inner_path);

		matinner->cdb_shield_child_from_rescans = true;

		/*
		 * If we have motion on the outer, to avoid a deadlock; we
		 * need to set cdb_strict. In order for materialize to
		 * fully fetch the underlying (required to avoid our
		 * deadlock hazard) we must set cdb_strict!
		 */
		if (inner_path->motionHazard && outer_path->motionHazard)
		{
			matinner->cdb_strict = true;
			matinner->path.motionHazard = false;
		}

		inner_path = (Path *) matinner;
	}

	/*
	 * If the inner path is parameterized by the outer, we must drop any
	 * restrict_clauses that are due to be moved into the inner path.  We have
	 * to do this now, rather than postpone the work till createplan time,
	 * because the restrict_clauses list can affect the size and cost
	 * estimates for this path.
	 */
	if (bms_overlap(inner_req_outer, outer_path->parent->relids))
	{
		Relids		inner_and_outer = bms_union(inner_path->parent->relids,
												inner_req_outer);
		List	   *jclauses = NIL;
		ListCell   *lc;

		foreach(lc, restrict_clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			if (!join_clause_is_movable_into(rinfo,
											 inner_path->parent->relids,
											 inner_and_outer))
				jclauses = lappend(jclauses, rinfo);
		}
		restrict_clauses = jclauses;
	}


	pathnode = makeNode(NestPath);
	pathnode->path.pathtype = T_NestLoop;
	pathnode->path.parent = joinrel;
	pathnode->path.pathtarget = joinrel->reltarget;
	pathnode->path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  extra->sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = joinrel->consider_parallel &&
		outer_path->parallel_safe && inner_path->parallel_safe;
	/* This is a foolish way to estimate parallel_workers, but for now... */
	pathnode->path.parallel_workers = outer_path->parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->jointype = jointype;
	pathnode->inner_unique = extra->inner_unique;
	pathnode->outerjoinpath = outer_path;
	pathnode->innerjoinpath = inner_path;
	pathnode->joinrestrictinfo = restrict_clauses;

	pathnode->path.locus = join_locus;
	pathnode->path.motionHazard = outer_path->motionHazard || inner_path->motionHazard;

	/* we're only as rescannable as our child plans */
	pathnode->path.rescannable = outer_path->rescannable && inner_path->rescannable;

	pathnode->path.sameslice_relids = bms_union(inner_path->sameslice_relids, outer_path->sameslice_relids);

	/*
	 * inner_path & outer_path are possibly modified above. Let's recalculate
	 * the initial cost.
	 */
	initial_cost_nestloop(root, workspace, jointype,
						  outer_path, inner_path,
						  extra);

	final_cost_nestloop(root, pathnode, workspace, extra);

	if (orig_jointype == JOIN_DEDUP_SEMI ||
		orig_jointype == JOIN_DEDUP_SEMI_REVERSE)
	{
		return (Path *) create_unique_rowid_path(root,
												 joinrel,
												 (Path *) pathnode,
												 pathnode->innerjoinpath->parent->relids,
												 rowidexpr_id);
	}

	/*
	 * Greengage specific behavior:
	 * If we find the join locus is general or segmentgeneral,
	 * we should check the joinqual, if it contains volatile functions
	 * we have to turn the join path to singleQE.
	 *
	 * NB: we do not add this logic in the above create_unique_rowid_path
	 * code block, the reason is:
	 *   create_unique_rowid_path is a technique to implement semi join
	 *   using normal join, it can only happens for sublink query:
	 *   1. if the sublink query contains volatile target list or havingQual
	 *      it cannot be pulled up in pull_up_subquery, so it will be a
	 *      subselect and be handled in the function set_subquery_pathlist
	 *   2. if the sublink query contains volatile functions in joinqual
	 *      or where clause, it will be handled in set_rel_pathlist and
	 *      here.
	 */
	return turn_volatile_seggen_to_singleqe(root,
											(Path *) pathnode,
											(Node *) (pathnode->joinrestrictinfo));
}

/*
 * create_mergejoin_path
 *	  Creates a pathnode corresponding to a mergejoin join between
 *	  two relations
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_mergejoin
 * 'extra' contains various information about the join
 * 'outer_path' is the outer path
 * 'inner_path' is the inner path
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'pathkeys' are the path keys of the new join path
 * 'required_outer' is the set of required outer rels
 * 'mergeclauses' are the RestrictInfo nodes to use as merge clauses
 *		(this should be a subset of the restrict_clauses list)
 * 'allmergeclauses' are the RestrictInfo nodes that are of the form
 *      required of merge clauses (equijoin between outer and inner rel).
 *      Consists of the ones to be used for merging ('mergeclauses') plus
 *      any others in 'restrict_clauses' that are to be applied after the
 *      merge.  We use them for motion planning.  (CDB)

 * 'outersortkeys' are the sort varkeys for the outer relation
 *      or NIL to use existing ordering
 * 'innersortkeys' are the sort varkeys for the inner relation
 *      or NIL to use existing ordering
 */
Path *
create_mergejoin_path(PlannerInfo *root,
					  RelOptInfo *joinrel,
					  JoinType jointype,
					  JoinType orig_jointype,		/* CDB */
					  JoinCostWorkspace *workspace,
					  JoinPathExtraData *extra,
					  Path *outer_path,
					  Path *inner_path,
					  List *restrict_clauses,
					  List *pathkeys,
					  Relids required_outer,
					  List *mergeclauses,
					  List *redistribution_clauses,	/* CDB */
					  List *outersortkeys,
					  List *innersortkeys)
{
	MergePath  *pathnode = makeNode(MergePath);
	CdbPathLocus join_locus;
	List	   *outermotionkeys;
	List	   *innermotionkeys;
	bool		preserve_outer_ordering;
	bool		preserve_inner_ordering;
	int			rowidexpr_id;

	/*
	 * GPDB_92_MERGE_FIXME: Should we keep the pathkeys_contained_in calls?
	 */
	/*
	 * Do subpaths have useful ordering?
	 */
	if (outersortkeys == NIL)           /* must preserve existing ordering */
		outermotionkeys = outer_path->pathkeys;
	else if (pathkeys_contained_in(outersortkeys, outer_path->pathkeys))
		outermotionkeys = outersortkeys;/* lucky coincidence, already ordered */
	else                                /* existing order useless; must sort */
		outermotionkeys = NIL;

	if (innersortkeys == NIL)
		innermotionkeys = inner_path->pathkeys;
	else if (pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
		innermotionkeys = innersortkeys;
	else
		innermotionkeys = NIL;

	/*
	 * Add motion nodes above subpaths and decide where to join.
	 *
	 * If we're explicitly sorting one or both sides of the join, don't choose
	 * a Motion that would break that ordering again. But as a special case,
	 * if there are no merge clauses, then there is no join order that would need
	 * preserving. That case can occur with a query like "a FULL JOIN b ON true"
	 */
	if (mergeclauses)
	{
		preserve_outer_ordering = (outersortkeys == NIL);
		preserve_inner_ordering = (innersortkeys == NIL);
	}
	else
		preserve_outer_ordering = preserve_inner_ordering = false;

	preserve_outer_ordering = preserve_outer_ordering || !bms_is_empty(PATH_REQ_OUTER(outer_path));
	preserve_inner_ordering = preserve_inner_ordering || !bms_is_empty(PATH_REQ_OUTER(inner_path));

	join_locus = cdbpath_motion_for_join(root,
										 orig_jointype,
										 &outer_path,       /* INOUT */
										 &inner_path,       /* INOUT */
										 &rowidexpr_id,
										 redistribution_clauses,
										 restrict_clauses,
										 outermotionkeys,
										 innermotionkeys,
										 preserve_outer_ordering,
										 preserve_inner_ordering);
	if (CdbPathLocus_IsNull(join_locus))
		return NULL;

	/*
	 * Sort is not needed if subpath is already well enough ordered and a
	 * disordering motion node (with pathkeys == NIL) hasn't been added.
	 */
	if (outermotionkeys &&
		outer_path->pathkeys)
		outersortkeys = NIL;
	if (innermotionkeys &&
		inner_path->pathkeys)
		innersortkeys = NIL;

	pathnode->jpath.path.pathtype = T_MergeJoin;
	pathnode->jpath.path.parent = joinrel;
	pathnode->jpath.path.pathtarget = joinrel->reltarget;
	pathnode->jpath.path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  extra->sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->jpath.path.parallel_aware = false;
	pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
		outer_path->parallel_safe && inner_path->parallel_safe;
	/* This is a foolish way to estimate parallel_workers, but for now... */
	pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;
	pathnode->jpath.path.pathkeys = pathkeys;

	pathnode->jpath.path.locus = join_locus;

	pathnode->jpath.path.motionHazard = outer_path->motionHazard || inner_path->motionHazard;
	pathnode->jpath.path.rescannable = outer_path->rescannable && inner_path->rescannable;
	pathnode->jpath.path.sameslice_relids = bms_union(inner_path->sameslice_relids, outer_path->sameslice_relids);

	pathnode->jpath.jointype = jointype;
	pathnode->jpath.inner_unique = extra->inner_unique;
	pathnode->jpath.outerjoinpath = outer_path;
	pathnode->jpath.innerjoinpath = inner_path;
	pathnode->jpath.joinrestrictinfo = restrict_clauses;
	pathnode->path_mergeclauses = mergeclauses;
	pathnode->outersortkeys = outersortkeys;
	pathnode->innersortkeys = innersortkeys;
	/* pathnode->skip_mark_restore will be set by final_cost_mergejoin */
	/* pathnode->materialize_inner will be set by final_cost_mergejoin */

	/*
	 * inner_path & outer_path are possibly modified above. Let's recalculate
	 * the initial cost.
	 */
	initial_cost_mergejoin(root, workspace, jointype, mergeclauses,
						   outer_path, inner_path,
						   outersortkeys, innersortkeys,
						   extra);

	final_cost_mergejoin(root, pathnode, workspace, extra);

	if (orig_jointype == JOIN_DEDUP_SEMI ||
		orig_jointype == JOIN_DEDUP_SEMI_REVERSE)
	{
		return (Path *) create_unique_rowid_path(root,
												 joinrel,
												 (Path *) pathnode,
												 pathnode->jpath.innerjoinpath->parent->relids,
												 rowidexpr_id);
	}

	/*
	 * See the comments at the end of create_nestloop_path.
	 */
	return turn_volatile_seggen_to_singleqe(root,
											(Path *) pathnode,
											(Node *) (pathnode->jpath.joinrestrictinfo));
}

/*
 * create_hashjoin_path
 *	  Creates a pathnode corresponding to a hash join between two relations.
 *
 * 'joinrel' is the join relation
 * 'jointype' is the type of join required
 * 'workspace' is the result from initial_cost_hashjoin
 * 'extra' contains various information about the join
 * 'outer_path' is the cheapest outer path
 * 'inner_path' is the cheapest inner path
 * 'parallel_hash' to select Parallel Hash of inner path (shared hash table)
 * 'restrict_clauses' are the RestrictInfo nodes to apply at the join
 * 'required_outer' is the set of required outer rels
 * 'hashclauses' are the RestrictInfo nodes to use as hash clauses
 *		(this should be a subset of the restrict_clauses list)
 */
Path *
create_hashjoin_path(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 JoinType jointype,
					 JoinType orig_jointype,		/* CDB */
					 JoinCostWorkspace *workspace,
					 JoinPathExtraData *extra,
					 Path *outer_path,
					 Path *inner_path,
					 bool parallel_hash,
					 List *restrict_clauses,
					 Relids required_outer,
					 List *redistribution_clauses,	/* CDB */
					 List *hashclauses)
{
	HashPath   *pathnode;
	CdbPathLocus join_locus;
	bool		outer_must_be_local = !bms_is_empty(PATH_REQ_OUTER(outer_path));
	bool		inner_must_be_local = !bms_is_empty(PATH_REQ_OUTER(inner_path));
	int			rowidexpr_id;

	/* Add motion nodes above subpaths and decide where to join. */
	join_locus = cdbpath_motion_for_join(root,
										 orig_jointype,
										 &outer_path,       /* INOUT */
										 &inner_path,       /* INOUT */
										 &rowidexpr_id,
										 redistribution_clauses,
										 restrict_clauses,
										 NIL,   /* don't care about ordering */
										 NIL,
										 outer_must_be_local,
										 inner_must_be_local);
	if (CdbPathLocus_IsNull(join_locus))
		return NULL;

	/*
	 * CDB: If gp_enable_hashjoin_size_heuristic is set, disallow inner
	 * joins where the inner rel is the larger of the two inputs.
	 *
	 * Note cdbpath_motion_for_join() has to precede this so we can get
	 * the right row count, in case Broadcast Motion is inserted above an
	 * input path.
	 */
	if (jointype == JOIN_INNER && gp_enable_hashjoin_size_heuristic)
	{
		double		outersize;
		double		innersize;

		outersize = ExecHashRowSize(outer_path->parent->reltarget->width) *
			outer_path->rows;
		innersize = ExecHashRowSize(inner_path->parent->reltarget->width) *
			inner_path->rows;

		if (innersize > outersize)
			return NULL;
	}

	pathnode = makeNode(HashPath);

	pathnode->jpath.path.pathtype = T_HashJoin;
	pathnode->jpath.path.parent = joinrel;
	pathnode->jpath.path.pathtarget = joinrel->reltarget;
	pathnode->jpath.path.param_info =
		get_joinrel_parampathinfo(root,
								  joinrel,
								  outer_path,
								  inner_path,
								  extra->sjinfo,
								  required_outer,
								  &restrict_clauses);
	pathnode->jpath.path.parallel_aware =
		joinrel->consider_parallel && parallel_hash;
	pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
		outer_path->parallel_safe && inner_path->parallel_safe;
	/* This is a foolish way to estimate parallel_workers, but for now... */
	pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;

	/*
	 * A hashjoin never has pathkeys, since its output ordering is
	 * unpredictable due to possible batching.  XXX If the inner relation is
	 * small enough, we could instruct the executor that it must not batch,
	 * and then we could assume that the output inherits the outer relation's
	 * ordering, which might save a sort step.  However there is considerable
	 * downside if our estimate of the inner relation size is badly off. For
	 * the moment we don't risk it.  (Note also that if we wanted to take this
	 * seriously, joinpath.c would have to consider many more paths for the
	 * outer rel than it does now.)
	 */
	pathnode->jpath.path.pathkeys = NIL;
	pathnode->jpath.path.locus = join_locus;

	pathnode->jpath.jointype = jointype;
	pathnode->jpath.inner_unique = extra->inner_unique;
	pathnode->jpath.outerjoinpath = outer_path;
	pathnode->jpath.innerjoinpath = inner_path;
	pathnode->jpath.joinrestrictinfo = restrict_clauses;
	pathnode->path_hashclauses = hashclauses;
	/* final_cost_hashjoin will fill in pathnode->num_batches */

	/*
	 * If hash table overflows to disk, and an ancestor node requests rescan
	 * (e.g. because the HJ is in the inner subtree of a NJ), then the HJ has
	 * to be redone, including rescanning the inner rel in order to rebuild
	 * the hash table.
	 */
	pathnode->jpath.path.rescannable = outer_path->rescannable && inner_path->rescannable;

	/* see the comment above; we may have a motion hazard on our inner ?! */
	if (pathnode->jpath.path.rescannable)
		pathnode->jpath.path.motionHazard = outer_path->motionHazard;
	else
		pathnode->jpath.path.motionHazard = outer_path->motionHazard || inner_path->motionHazard;
	pathnode->jpath.path.sameslice_relids = bms_union(inner_path->sameslice_relids, outer_path->sameslice_relids);

	/*
	 * inner_path & outer_path are possibly modified above. Let's recalculate
	 * the initial cost.
	 */
	initial_cost_hashjoin(root, workspace, jointype, hashclauses,
						  outer_path, inner_path,
						  extra, parallel_hash);

	final_cost_hashjoin(root, pathnode, workspace, extra);

	if (orig_jointype == JOIN_DEDUP_SEMI ||
		orig_jointype == JOIN_DEDUP_SEMI_REVERSE)
	{
		return (Path *) create_unique_rowid_path(root,
												 joinrel,
												 (Path *) pathnode,
												 pathnode->jpath.innerjoinpath->parent->relids,
												 rowidexpr_id);
	}

	/*
	 * See the comments at the end of create_nestloop_path.
	 */
	return turn_volatile_seggen_to_singleqe(root,
											(Path *) pathnode,
											(Node *) (pathnode->jpath.joinrestrictinfo));
}

/*
 * create_projection_path
 *	  Creates a pathnode that represents performing a projection.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
ProjectionPath *
create_projection_path(PlannerInfo *root,
					   RelOptInfo *rel,
					   Path *subpath,
					   PathTarget *target)
{
	return create_projection_path_with_quals(root, rel,
											 subpath, target,
											 NIL, false);
}

ProjectionPath *
create_projection_path_with_quals(PlannerInfo *root,
								  RelOptInfo *rel,
								  Path *subpath,
								  PathTarget *target,
								  List *restrict_clauses,
								  bool need_param)
{
	ProjectionPath *pathnode = makeNode(ProjectionPath);
	PathTarget *oldtarget;

	/*
	 * We mustn't put a ProjectionPath directly above another; it's useless
	 * and will confuse create_projection_plan.  Rather than making sure all
	 * callers handle that, let's implement it here, by stripping off any
	 * ProjectionPath in what we're given.  Given this rule, there won't be
	 * more than one.
	 */
	if (IsA(subpath, ProjectionPath))
	{
		ProjectionPath *subpp = (ProjectionPath *) subpath;

		Assert(subpp->path.parent == rel);
		subpath = subpp->subpath;
		if (subpp->cdb_restrict_clauses != NIL)
			restrict_clauses = list_concat_unique(restrict_clauses, subpp->cdb_restrict_clauses);
		Assert(!IsA(subpath, ProjectionPath));
	}

	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	pathnode->path.param_info = need_param ? subpath->param_info : NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe &&
		is_parallel_safe(root, (Node *) target->exprs);
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Projection does not change the sort order */
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->path.locus = subpath->locus;
	pathnode->path.sameslice_relids = subpath->sameslice_relids;

	pathnode->subpath = subpath;

	/*
	 * We might not need a separate Result node.  If the input plan node type
	 * can project, we can just tell it to project something else.  Or, if it
	 * can't project but the desired target has the same expression list as
	 * what the input will produce anyway, we can still give it the desired
	 * tlist (possibly changing its ressortgroupref labels, but nothing else).
	 * Note: in the latter case, create_projection_plan has to recheck our
	 * conclusion; see comments therein.
	 *
	 * GPDB: The 'restrict_clauses' is a GPDB addition. If the subpath supports
	 * Filters, we could push them down too. But currently this is only used on
	 * top of Material paths, which don't support it, so it doesn't matter.
	 */
	oldtarget = subpath->pathtarget;
	if (!restrict_clauses &&
		(is_projection_capable_path(subpath) ||
		 equal(oldtarget->exprs, target->exprs)))
	{
		/* No separate Result node needed */
		pathnode->dummypp = true;

		/*
		 * Set cost of plan as subpath's cost, adjusted for tlist replacement.
		 */
		pathnode->path.rows = subpath->rows;
		pathnode->path.startup_cost = subpath->startup_cost +
			(target->cost.startup - oldtarget->cost.startup);
		pathnode->path.total_cost = subpath->total_cost +
			(target->cost.startup - oldtarget->cost.startup) +
			(target->cost.per_tuple - oldtarget->cost.per_tuple) * subpath->rows;
	}
	else
	{
		/* We really do need the Result node */
		pathnode->dummypp = false;

		/*
		 * The Result node's cost is cpu_tuple_cost per row, plus the cost of
		 * evaluating the tlist.  There is no qual to worry about.
		 */
		pathnode->path.rows = subpath->rows;
		pathnode->path.startup_cost = subpath->startup_cost +
			target->cost.startup;
		pathnode->path.total_cost = subpath->total_cost +
			target->cost.startup +
			(cpu_tuple_cost + target->cost.per_tuple) * subpath->rows;

		pathnode->cdb_restrict_clauses = restrict_clauses;
	}

	return pathnode;
}

/*
 * apply_projection_to_path
 *	  Add a projection step, or just apply the target directly to given path.
 *
 * This has the same net effect as create_projection_path(), except that if
 * a separate Result plan node isn't needed, we just replace the given path's
 * pathtarget with the desired one.  This must be used only when the caller
 * knows that the given path isn't referenced elsewhere and so can be modified
 * in-place.
 *
 * If the input path is a GatherPath or GatherMergePath, we try to push the
 * new target down to its input as well; this is a yet more invasive
 * modification of the input path, which create_projection_path() can't do.
 *
 * Note that we mustn't change the source path's parent link; so when it is
 * add_path'd to "rel" things will be a bit inconsistent.  So far that has
 * not caused any trouble.
 *
 * 'rel' is the parent relation associated with the result
 * 'path' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
Path *
apply_projection_to_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path *path,
						 PathTarget *target)
{
	QualCost	oldcost;

	/*
	 * If given path can't project, we might need a Result node, so make a
	 * separate ProjectionPath.
	 */
	if (!is_projection_capable_path(path))
		return (Path *) create_projection_path(root, rel, path, target);

	/*
	 * We can just jam the desired tlist into the existing path, being sure to
	 * update its cost estimates appropriately.
	 */
	oldcost = path->pathtarget->cost;
	path->pathtarget = target;

	path->startup_cost += target->cost.startup - oldcost.startup;
	path->total_cost += target->cost.startup - oldcost.startup +
		(target->cost.per_tuple - oldcost.per_tuple) * path->rows;

	/*
	 * If the path happens to be a Gather or GatherMerge path, we'd like to
	 * arrange for the subpath to return the required target list so that
	 * workers can help project.  But if there is something that is not
	 * parallel-safe in the target expressions, then we can't.
	 */
	if ((IsA(path, GatherPath) ||IsA(path, GatherMergePath)) &&
		is_parallel_safe(root, (Node *) target->exprs))
	{
		/*
		 * We always use create_projection_path here, even if the subpath is
		 * projection-capable, so as to avoid modifying the subpath in place.
		 * It seems unlikely at present that there could be any other
		 * references to the subpath, but better safe than sorry.
		 *
		 * Note that we don't change the parallel path's cost estimates; it
		 * might be appropriate to do so, to reflect the fact that the bulk of
		 * the target evaluation will happen in workers.
		 */
		if (IsA(path, GatherPath))
		{
			GatherPath *gpath = (GatherPath *) path;

			gpath->subpath = (Path *)
				create_projection_path(root,
									   gpath->subpath->parent,
									   gpath->subpath,
									   target);
		}
		else
		{
			GatherMergePath *gmpath = (GatherMergePath *) path;

			gmpath->subpath = (Path *)
				create_projection_path(root,
									   gmpath->subpath->parent,
									   gmpath->subpath,
									   target);
		}
	}
	else if (path->parallel_safe &&
			 !is_parallel_safe(root, (Node *) target->exprs))
	{
		/*
		 * We're inserting a parallel-restricted target list into a path
		 * currently marked parallel-safe, so we have to mark it as no longer
		 * safe.
		 */
		path->parallel_safe = false;
	}

	return path;
}

/*
 * create_set_projection_path
 *	  Creates a pathnode that represents performing a projection that
 *	  includes set-returning functions.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 */
ProjectSetPath *
create_set_projection_path(PlannerInfo *root,
						   RelOptInfo *rel,
						   Path *subpath,
						   PathTarget *target)
{
	ProjectSetPath *pathnode = makeNode(ProjectSetPath);
	double		tlist_rows;
	ListCell   *lc;

	pathnode->path.pathtype = T_ProjectSet;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe &&
		is_parallel_safe(root, (Node *) target->exprs);
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Projection does not change the sort order XXX? */
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->path.locus = subpath->locus;

	pathnode->subpath = subpath;

	/*
	 * Estimate number of rows produced by SRFs for each row of input; if
	 * there's more than one in this node, use the maximum.
	 */
	tlist_rows = 1;
	foreach(lc, target->exprs)
	{
		Node	   *node = (Node *) lfirst(lc);
		double		itemrows;

		itemrows = expression_returns_set_rows(root, node);
		if (tlist_rows < itemrows)
			tlist_rows = itemrows;
	}

	/*
	 * In addition to the cost of evaluating the tlist, charge cpu_tuple_cost
	 * per input row, and half of cpu_tuple_cost for each added output row.
	 * This is slightly bizarre maybe, but it's what 9.6 did; we may revisit
	 * this estimate later.
	 */
	pathnode->path.rows = subpath->rows * tlist_rows;
	pathnode->path.startup_cost = subpath->startup_cost +
		target->cost.startup;
	pathnode->path.total_cost = subpath->total_cost +
		target->cost.startup +
		(cpu_tuple_cost + target->cost.per_tuple) * subpath->rows +
		(pathnode->path.rows - subpath->rows) * cpu_tuple_cost / 2;

	return pathnode;
}

/*
 * create_sort_path
 *	  Creates a pathnode that represents performing an explicit sort.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'pathkeys' represents the desired sort order
 * 'limit_tuples' is the estimated bound on the number of output tuples,
 *		or -1 if no LIMIT or couldn't estimate
 */
SortPath *
create_sort_path(PlannerInfo *root,
				 RelOptInfo *rel,
				 Path *subpath,
				 List *pathkeys,
				 double limit_tuples)
{
	SortPath   *pathnode = makeNode(SortPath);

	Assert(pathkeys != NIL);

	pathnode->path.pathtype = T_Sort;
	pathnode->path.parent = rel;
	/* Sort doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = pathkeys;
	pathnode->path.locus = subpath->locus;

	pathnode->subpath = subpath;

	cost_sort(&pathnode->path, root, pathkeys,
			  subpath->total_cost,
			  subpath->rows,
			  subpath->pathtarget->width,
			  0.0,				/* XXX comparison_cost shouldn't be 0? */
			  work_mem, limit_tuples);

	return pathnode;
}

#ifdef NOT_USED /* Group nodes are not used in GPDB */
/*
 * create_group_path
 *	  Creates a pathnode that represents performing grouping of presorted input
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'numGroups' is the estimated number of groups
 */
GroupPath *
create_group_path(PlannerInfo *root,
				  RelOptInfo *rel,
				  Path *subpath,
				  List *groupClause,
				  List *qual,
				  double numGroups)
{
	GroupPath  *pathnode = makeNode(GroupPath);
	PathTarget *target = rel->reltarget;

	pathnode->path.pathtype = T_Group;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Group doesn't change sort ordering */
	pathnode->path.pathkeys = subpath->pathkeys;

	pathnode->subpath = subpath;

	pathnode->groupClause = groupClause;
	pathnode->qual = qual;

	cost_group(&pathnode->path, root,
			   list_length(groupClause),
			   numGroups,
			   qual,
			   subpath->startup_cost, subpath->total_cost,
			   subpath->rows);

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	return pathnode;
}
#endif

/*
 * create_upper_unique_path
 *	  Creates a pathnode that represents performing an explicit Unique step
 *	  on presorted input.
 *
 * This produces a Unique plan node, but the use-case is so different from
 * create_unique_path that it doesn't seem worth trying to merge the two.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'numCols' is the number of grouping columns
 * 'numGroups' is the estimated number of groups
 *
 * The input path must be sorted on the grouping columns, plus possibly
 * additional columns; so the first numCols pathkeys are the grouping columns
 */
UpperUniquePath *
create_upper_unique_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path *subpath,
						 int numCols,
						 double numGroups)
{
	UpperUniquePath *pathnode = makeNode(UpperUniquePath);

	pathnode->path.pathtype = T_Unique;
	pathnode->path.parent = rel;
	/* Unique doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* Unique doesn't change the input ordering */
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->path.locus = subpath->locus;

	pathnode->subpath = subpath;
	pathnode->numkeys = numCols;

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.  (XXX probably this is
	 * an overestimate.)
	 */
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost +
		cpu_operator_cost * subpath->rows * numCols;
	pathnode->path.rows = numGroups;

	return pathnode;
}

/*
 * create_agg_path
 *	  Creates a pathnode that represents performing aggregation/grouping
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'aggstrategy' is the Agg node's basic implementation strategy
 * 'aggsplit' is the Agg node's aggregate-splitting mode
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'qual' is the HAVING quals if any
 * 'aggcosts' contains cost info about the aggregate functions to be computed
 * 'numGroups' is the estimated number of groups (1 if not grouping)
 */
AggPath *
create_agg_path(PlannerInfo *root,
				RelOptInfo *rel,
				Path *subpath,
				PathTarget *target,
				AggStrategy aggstrategy,
				AggSplit aggsplit,
				bool streaming,
				List *groupClause,
				List *qual,
				const AggClauseCosts *aggcosts,
				double numGroups)
{
	AggPath    *pathnode = makeNode(AggPath);

	pathnode->path.pathtype = T_Agg;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	if (aggstrategy == AGG_SORTED)
		pathnode->path.pathkeys = subpath->pathkeys;	/* preserves order */
	else
		pathnode->path.pathkeys = NIL;	/* output is unordered */
	pathnode->subpath = subpath;
	pathnode->streaming = streaming;

	pathnode->aggstrategy = aggstrategy;
	pathnode->aggsplit = aggsplit;
	pathnode->numGroups = numGroups;
	pathnode->groupClause = groupClause;
	pathnode->qual = qual;

	cost_agg(&pathnode->path, root,
			 aggstrategy, aggcosts,
			 list_length(groupClause), numGroups,
			 qual,
			 subpath->startup_cost, subpath->total_cost,
			 subpath->rows, subpath->pathtarget->width);

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	pathnode->path.locus = subpath->locus;

	return pathnode;
}

/*
 * create_tup_split_path
 *	  Creates a pathnode that represents performing TupleSplit
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'groupClause' is a list of SortGroupClause's representing the grouping
 * 'numGroups' is the estimated number of groups (1 if not grouping)
 * 'bitmapset' is the bitmap of DQA expr Index in PathTarget
 * 'numDisDQAs' is the number of bitmapset size
 */
TupleSplitPath *
create_tup_split_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  Path *subpath,
					  PathTarget *target,
					  List *groupClause,
					  List *dqa_expr_lst)
{
	TupleSplitPath *pathnode = makeNode(TupleSplitPath);

	pathnode->path.pathtype = T_TupleSplit;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;

	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.pathkeys = NIL;

	pathnode->subpath = subpath;
	pathnode->groupClause = groupClause;

	pathnode->dqa_expr_lst = dqa_expr_lst;

	cost_tup_split(&pathnode->path, root, list_length(dqa_expr_lst),
				   subpath->startup_cost, subpath->total_cost,
				   subpath->rows);

	CdbPathLocus_MakeStrewn(&pathnode->path.locus,
							subpath->locus.numsegments);

	return pathnode;
}

/*
 * create_groupingsets_path
 *	  Creates a pathnode that represents performing GROUPING SETS aggregation
 *
 * GroupingSetsPath represents sorted grouping with one or more grouping sets.
 * The input path's result must be sorted to match the last entry in
 * rollup_groupclauses.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'having_qual' is the HAVING quals if any
 * 'rollups' is a list of RollupData nodes
 * 'agg_costs' contains cost info about the aggregate functions to be computed
 */
GroupingSetsPath *
create_groupingsets_path(PlannerInfo *root,
						 RelOptInfo *rel,
						 Path *subpath,
						 AggSplit aggsplit,
						 List *having_qual,
						 AggStrategy aggstrategy,
						 List *rollups,
						 const AggClauseCosts *agg_costs)
{
	GroupingSetsPath *pathnode = makeNode(GroupingSetsPath);
	PathTarget *target = rel->reltarget;
	ListCell   *lc;
	bool		is_first = true;
	bool		is_first_sort = true;

	/* The topmost generated Plan node will be an Agg */
	pathnode->path.pathtype = T_Agg;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	pathnode->path.param_info = subpath->param_info;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->subpath = subpath;

	/*
	 * Simplify callers by downgrading AGG_SORTED to AGG_PLAIN, and AGG_MIXED
	 * to AGG_HASHED, here if possible.
	 */
	if (aggstrategy == AGG_SORTED &&
		list_length(rollups) == 1 &&
		((RollupData *) linitial(rollups))->groupClause == NIL)
		aggstrategy = AGG_PLAIN;

	if (aggstrategy == AGG_MIXED &&
		list_length(rollups) == 1)
		aggstrategy = AGG_HASHED;

	/*
	 * Output will be in sorted order by group_pathkeys if, and only if, there
	 * is a single rollup operation on a non-empty list of grouping
	 * expressions.
	 */
	if (aggstrategy == AGG_SORTED && list_length(rollups) == 1)
		pathnode->path.pathkeys = root->group_pathkeys;
	else
		pathnode->path.pathkeys = NIL;

	pathnode->aggsplit = aggsplit;
	pathnode->aggstrategy = aggstrategy;
	pathnode->rollups = rollups;
	pathnode->qual = having_qual;

	Assert(rollups != NIL);
	Assert(aggstrategy != AGG_PLAIN || list_length(rollups) == 1);
	Assert(aggstrategy != AGG_MIXED || list_length(rollups) > 1);

	foreach(lc, rollups)
	{
		RollupData *rollup = lfirst(lc);
		List	   *gsets = rollup->gsets;
		int			numGroupCols = list_length(linitial(gsets));

		/*
		 * In AGG_SORTED or AGG_PLAIN mode, the first rollup takes the
		 * (already-sorted) input, and following ones do their own sort.
		 *
		 * In AGG_HASHED mode, there is one rollup for each grouping set.
		 *
		 * In AGG_MIXED mode, the first rollups are hashed, the first
		 * non-hashed one takes the (already-sorted) input, and following ones
		 * do their own sort.
		 */
		if (is_first)
		{
			cost_agg(&pathnode->path, root,
					 aggstrategy,
					 agg_costs,
					 numGroupCols,
					 estimate_num_groups_on_segment(rollup->numGroups,
													subpath->rows,
													subpath->locus),
					 having_qual,
					 subpath->startup_cost,
					 subpath->total_cost,
					 subpath->rows,
					 subpath->pathtarget->width);
			is_first = false;
			if (!rollup->is_hashed)
				is_first_sort = false;
		}
		else
		{
			Path		sort_path;	/* dummy for result of cost_sort */
			Path		agg_path;	/* dummy for result of cost_agg */

			if (rollup->is_hashed || is_first_sort)
			{
				/*
				 * Account for cost of aggregation, but don't charge input
				 * cost again
				 */
				cost_agg(&agg_path, root,
						 rollup->is_hashed ? AGG_HASHED : AGG_SORTED,
						 agg_costs,
						 numGroupCols,
						 estimate_num_groups_on_segment(rollup->numGroups,
														subpath->rows,
														subpath->locus),
						 having_qual,
						 0.0, 0.0,
						 subpath->rows,
						 subpath->pathtarget->width);
				if (!rollup->is_hashed)
					is_first_sort = false;
			}
			else
			{
				/* Account for cost of sort, but don't charge input cost again */
				cost_sort(&sort_path, root, NIL,
						  0.0,
						  subpath->rows,
						  subpath->pathtarget->width,
						  0.0,
						  work_mem,
						  -1.0);

				/* Account for cost of aggregation */

				cost_agg(&agg_path, root,
						 AGG_SORTED,
						 agg_costs,
						 numGroupCols,
						 estimate_num_groups_on_segment(rollup->numGroups,
														subpath->rows,
														subpath->locus),
						 having_qual,
						 sort_path.startup_cost,
						 sort_path.total_cost,
						 sort_path.rows,
						 subpath->pathtarget->width);
			}

			pathnode->path.total_cost += agg_path.total_cost;
			pathnode->path.rows += agg_path.rows;
		}
	}

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	/*
	 * If this is a one-stage aggregate, the caller should already have
	 * ensured that the data is distributed so that a one-stage aggregate
	 * works, and the distribution is preserved. But if this is the first
	 * stage of a multi-stage aggregate, if any distribution key columns
	 * are part of rollups, they will be set to NULLs for the rolled up
	 * rows. That breaks the distribution.
	 */
	if (CdbPathLocus_IsPartitioned(subpath->locus))
		CdbPathLocus_MakeStrewn(&pathnode->path.locus,
								CdbPathLocus_NumSegments(subpath->locus));
	else
		pathnode->path.locus = subpath->locus;

	return pathnode;
}

/*
 * create_minmaxagg_path
 *	  Creates a pathnode that represents computation of MIN/MAX aggregates
 *
 * 'rel' is the parent relation associated with the result
 * 'target' is the PathTarget to be computed
 * 'mmaggregates' is a list of MinMaxAggInfo structs
 * 'quals' is the HAVING quals if any
 */
MinMaxAggPath *
create_minmaxagg_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  PathTarget *target,
					  List *mmaggregates,
					  List *quals)
{
	MinMaxAggPath *pathnode = makeNode(MinMaxAggPath);
	Cost		initplan_cost;
	ListCell   *lc;
	CdbLocusType locustype = CdbLocusType_Null;
	int			numsegments = -1;

	/* The topmost generated Plan node will be a Result */
	pathnode->path.pathtype = T_Result;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	/* A MinMaxAggPath implies use of subplans, so cannot be parallel-safe */
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	/* Result is one unordered row */
	pathnode->path.rows = 1;
	pathnode->path.pathkeys = NIL;

	pathnode->mmaggregates = mmaggregates;
	pathnode->quals = quals;

	/* Calculate cost of all the initplans ... */
	initplan_cost = 0;
	foreach(lc, mmaggregates)
	{
		MinMaxAggInfo *mminfo = (MinMaxAggInfo *) lfirst(lc);

		initplan_cost += mminfo->pathcost;

		/*
		 * All the subpaths should have SingleQE locus, if the underlying
		 * table is partitioned, build_minmax_path() ensures that. But
		 * double-check here.
		 */
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			if (locustype == CdbLocusType_Null)
			{
				locustype = mminfo->path->locus.locustype;
				numsegments = mminfo->path->locus.numsegments;
			}
			else if (CdbPathLocus_IsPartitioned(mminfo->path->locus))
			{
				elog(ERROR, "minmax path has unexpected path locus of type %d",
					 mminfo->path->locus.locustype);
			}
			else if (locustype != mminfo->path->locus.locustype)
			{
				elog(ERROR, "minmax paths have different loci");
			}
		}
	}

	if (mmaggregates == NIL)
	{
		locustype = CdbLocusType_General;
		/* numsegments is useless for general locus, so should be -1 */
		numsegments = -1;
	}

	/* we checked that all the child paths have compatible loci */
	if (Gp_role == GP_ROLE_DISPATCH)
		CdbPathLocus_MakeSimple(&pathnode->path.locus, locustype, numsegments);
	else
		CdbPathLocus_MakeEntry(&pathnode->path.locus);

	/* add tlist eval cost for each output row, plus cpu_tuple_cost */
	pathnode->path.startup_cost = initplan_cost + target->cost.startup;
	pathnode->path.total_cost = initplan_cost + target->cost.startup +
		target->cost.per_tuple + cpu_tuple_cost;

	/*
	 * Add cost of qual, if any --- but we ignore its selectivity, since our
	 * rowcount estimate should be 1 no matter what the qual is.
	 */
	if (quals)
	{
		QualCost	qual_cost;

		cost_qual_eval(&qual_cost, quals, root);
		pathnode->path.startup_cost += qual_cost.startup;
		pathnode->path.total_cost += qual_cost.startup + qual_cost.per_tuple;
	}

	return pathnode;
}

/*
 * create_windowagg_path
 *	  Creates a pathnode that represents computation of window functions
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'target' is the PathTarget to be computed
 * 'windowFuncs' is a list of WindowFunc structs
 * 'winclause' is a WindowClause that is common to all the WindowFuncs
 *
 * The input must be sorted according to the WindowClause's PARTITION keys
 * plus ORDER BY keys.
 */
WindowAggPath *
create_windowagg_path(PlannerInfo *root,
					  RelOptInfo *rel,
					  Path *subpath,
					  PathTarget *target,
					  List *windowFuncs,
					  WindowClause *winclause)
{
	WindowAggPath *pathnode = makeNode(WindowAggPath);

	pathnode->path.pathtype = T_WindowAgg;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* WindowAgg preserves the input sort order */
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->path.locus = subpath->locus;

	pathnode->subpath = subpath;
	pathnode->winclause = winclause;

	/*
	 * For costing purposes, assume that there are no redundant partitioning
	 * or ordering columns; it's not worth the trouble to deal with that
	 * corner case here.  So we just pass the unmodified list lengths to
	 * cost_windowagg.
	 */
	cost_windowagg(&pathnode->path, root,
				   windowFuncs,
				   list_length(winclause->partitionClause),
				   list_length(winclause->orderClause),
				   subpath->startup_cost,
				   subpath->total_cost,
				   subpath->rows);

	/* add tlist eval cost for each output row */
	pathnode->path.startup_cost += target->cost.startup;
	pathnode->path.total_cost += target->cost.startup +
		target->cost.per_tuple * pathnode->path.rows;

	return pathnode;
}

/*
 * create_setop_path
 *	  Creates a pathnode that represents computation of INTERSECT or EXCEPT
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'cmd' is the specific semantics (INTERSECT or EXCEPT, with/without ALL)
 * 'strategy' is the implementation strategy (sorted or hashed)
 * 'distinctList' is a list of SortGroupClause's representing the grouping
 * 'flagColIdx' is the column number where the flag column will be, if any
 * 'firstFlag' is the flag value for the first input relation when hashing;
 *		or -1 when sorting
 * 'numGroups' is the estimated number of distinct groups
 * 'outputRows' is the estimated number of output rows
 */
SetOpPath *
create_setop_path(PlannerInfo *root,
				  RelOptInfo *rel,
				  Path *subpath,
				  SetOpCmd cmd,
				  SetOpStrategy strategy,
				  List *distinctList,
				  AttrNumber flagColIdx,
				  int firstFlag,
				  double numGroups,
				  double outputRows)
{
	SetOpPath  *pathnode = makeNode(SetOpPath);

	pathnode->path.pathtype = T_SetOp;
	pathnode->path.parent = rel;
	/* SetOp doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	/* SetOp preserves the input sort order if in sort mode */
	pathnode->path.pathkeys =
		(strategy == SETOP_SORTED) ? subpath->pathkeys : NIL;
	pathnode->path.locus = subpath->locus;

	pathnode->subpath = subpath;
	pathnode->cmd = cmd;
	pathnode->strategy = strategy;
	pathnode->distinctList = distinctList;
	pathnode->flagColIdx = flagColIdx;
	pathnode->firstFlag = firstFlag;
	pathnode->numGroups = numGroups;

	/*
	 * Charge one cpu_operator_cost per comparison per input tuple. We assume
	 * all columns get compared at most of the tuples.
	 */
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost +
		cpu_operator_cost * subpath->rows * list_length(distinctList);
	pathnode->path.rows = outputRows;

	return pathnode;
}

/*
 * create_recursiveunion_path
 *	  Creates a pathnode that represents a recursive UNION node
 *
 * 'rel' is the parent relation associated with the result
 * 'leftpath' is the source of data for the non-recursive term
 * 'rightpath' is the source of data for the recursive term
 * 'target' is the PathTarget to be computed
 * 'distinctList' is a list of SortGroupClause's representing the grouping
 * 'wtParam' is the ID of Param representing work table
 * 'numGroups' is the estimated number of groups
 *
 * For recursive UNION ALL, distinctList is empty and numGroups is zero
 */
RecursiveUnionPath *
create_recursiveunion_path(PlannerInfo *root,
						   RelOptInfo *rel,
						   Path *leftpath,
						   Path *rightpath,
						   PathTarget *target,
						   List *distinctList,
						   int wtParam,
						   double numGroups)
{
	RecursiveUnionPath *pathnode = makeNode(RecursiveUnionPath);

	pathnode->path.pathtype = T_RecursiveUnion;
	pathnode->path.parent = rel;
	pathnode->path.pathtarget = target;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		leftpath->parallel_safe && rightpath->parallel_safe;
	/* Foolish, but we'll do it like joins for now: */
	pathnode->path.parallel_workers = leftpath->parallel_workers;
	/* RecursiveUnion result is always unsorted */
	pathnode->path.pathkeys = NIL;

	pathnode->leftpath = leftpath;
	pathnode->rightpath = rightpath;
	pathnode->distinctList = distinctList;
	pathnode->wtParam = wtParam;
	pathnode->numGroups = numGroups;

	cost_recursive_union(&pathnode->path, leftpath, rightpath);

	return pathnode;
}

/*
 * create_lockrows_path
 *	  Creates a pathnode that represents acquiring row locks
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'rowMarks' is a list of PlanRowMark's
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 */
LockRowsPath *
create_lockrows_path(PlannerInfo *root, RelOptInfo *rel,
					 Path *subpath, List *rowMarks, int epqParam)
{
	LockRowsPath *pathnode = makeNode(LockRowsPath);

	pathnode->path.pathtype = T_LockRows;
	pathnode->path.parent = rel;
	/* LockRows doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	pathnode->path.rows = subpath->rows;

	/*
	 * The result cannot be assumed sorted, since locking might cause the sort
	 * key columns to be replaced with new values.
	 */
	pathnode->path.pathkeys = NIL;

	pathnode->path.locus = subpath->locus;

	pathnode->subpath = subpath;
	pathnode->rowMarks = rowMarks;
	pathnode->epqParam = epqParam;

	/*
	 * We should charge something extra for the costs of row locking and
	 * possible refetches, but it's hard to say how much.  For now, use
	 * cpu_tuple_cost per row.
	 */
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost +
		cpu_tuple_cost * subpath->rows;

	return pathnode;
}

/*
 * create_modifytable_path
 *	  Creates a pathnode that represents performing INSERT/UPDATE/DELETE mods
 *
 * 'rel' is the parent relation associated with the result
 * 'operation' is the operation type
 * 'canSetTag' is true if we set the command tag/es_processed
 * 'nominalRelation' is the parent RT index for use of EXPLAIN
 * 'rootRelation' is the partitioned table root RT index, or 0 if none
 * 'partColsUpdated' is true if any partitioning columns are being updated,
 *		either from the target relation or a descendent partitioned table.
 * 'resultRelations' is an integer list of actual RT indexes of target rel(s)
 * 'subpaths' is a list of Path(s) producing source data (one per rel)
 * 'subroots' is a list of PlannerInfo structs (one per rel)
 * 'withCheckOptionLists' is a list of WCO lists (one per rel)
 * 'returningLists' is a list of RETURNING tlists (one per rel)
 * 'rowMarks' is a list of PlanRowMarks (non-locking only)
 * 'onconflict' is the ON CONFLICT clause, or NULL
 * 'epqParam' is the ID of Param for EvalPlanQual re-eval
 */
ModifyTablePath *
create_modifytable_path(PlannerInfo *root, RelOptInfo *rel,
						CmdType operation, bool canSetTag,
						Index nominalRelation, Index rootRelation,
						bool partColsUpdated,
						List *resultRelations, List *subpaths,
						List *subroots,
						List *withCheckOptionLists, List *returningLists,
						List *is_split_updates,
						List *rowMarks, OnConflictExpr *onconflict,
						int epqParam)
{
	ModifyTablePath *pathnode = makeNode(ModifyTablePath);
	double		total_size;
	ListCell   *lc;

	Assert(list_length(resultRelations) == list_length(subpaths));
	Assert(list_length(resultRelations) == list_length(subroots));
	Assert(withCheckOptionLists == NIL ||
		   list_length(resultRelations) == list_length(withCheckOptionLists));
	Assert(returningLists == NIL ||
		   list_length(resultRelations) == list_length(returningLists));
	Assert(list_length(resultRelations) == list_length(is_split_updates));

	pathnode->path.pathtype = T_ModifyTable;
	pathnode->path.parent = rel;
	/* pathtarget is not interesting, just make it minimally valid */
	pathnode->path.pathtarget = rel->reltarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = 0;
	pathnode->path.pathkeys = NIL;

	/*
	 * Put Motions on top of the subpaths as needed, and set the locus of the
	 * ModifyTable path itself.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
		pathnode->path.locus =
			adjust_modifytable_subpaths(root, operation,
										resultRelations, subpaths,
										is_split_updates);
	else
	{
		/* don't allow split updates in utility mode. */
		if (Gp_role == GP_ROLE_UTILITY && operation == CMD_UPDATE &&
			list_member_int(is_split_updates, (int) true))
		{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot update distribution key columns in utility mode")));
		}

		CdbPathLocus_MakeEntry(&pathnode->path.locus);
	}

	/*
	 * Compute cost & rowcount as sum of subpath costs & rowcounts.
	 *
	 * Currently, we don't charge anything extra for the actual table
	 * modification work, nor for the WITH CHECK OPTIONS or RETURNING
	 * expressions if any.  It would only be window dressing, since
	 * ModifyTable is always a top-level node and there is no way for the
	 * costs to change any higher-level planning choices.  But we might want
	 * to make it look better sometime.
	 */
	pathnode->path.startup_cost = 0;
	pathnode->path.total_cost = 0;
	pathnode->path.rows = 0;
	total_size = 0;
	foreach(lc, subpaths)
	{
		Path	   *subpath = (Path *) lfirst(lc);

		if (lc == list_head(subpaths))	/* first node? */
			pathnode->path.startup_cost = subpath->startup_cost;
		pathnode->path.total_cost += subpath->total_cost;
		pathnode->path.rows += subpath->rows;
		total_size += subpath->pathtarget->width * subpath->rows;
	}

	/*
	 * Set width to the average width of the subpath outputs.  XXX this is
	 * totally wrong: we should report zero if no RETURNING, else an average
	 * of the RETURNING tlist widths.  But it's what happened historically,
	 * and improving it is a task for another day.
	 */
	if (pathnode->path.rows > 0)
		total_size /= pathnode->path.rows;
	pathnode->path.pathtarget->width = rint(total_size);

	pathnode->operation = operation;
	pathnode->canSetTag = canSetTag;
	pathnode->nominalRelation = nominalRelation;
	pathnode->rootRelation = rootRelation;
	pathnode->partColsUpdated = partColsUpdated;
	pathnode->resultRelations = resultRelations;
	pathnode->is_split_updates = is_split_updates;
	pathnode->subpaths = subpaths;
	pathnode->subroots = subroots;
	pathnode->withCheckOptionLists = withCheckOptionLists;
	pathnode->returningLists = returningLists;
	pathnode->rowMarks = rowMarks;
	pathnode->onconflict = onconflict;
	pathnode->epqParam = epqParam;

	return pathnode;
}


/*
 * Add Motions to children of a ModifyTable path, so that data
 * is modified on the correct segments.
 *
 * The input to a ModifyTable node must be distributed according to the
 * DISTRIBUTED BY of the target table. Add Motion paths to the child
 * plans for that. Returns a locus to represent the distribution of the
 * ModifyTable node itself.
 */
static CdbPathLocus
adjust_modifytable_subpaths(PlannerInfo *root, CmdType operation,
							List *resultRelations, List *subpaths,
							List *is_split_updates)
{
	/*
	 * The input plans must be distributed correctly.
	 */
	ListCell   *lcr,
			   *lcp,
			   *lci = NULL;
	bool		all_subplans_entry = true,
				all_subplans_replicated = true;
	int			numsegments = -1;

	if (operation == CMD_UPDATE)
		lci = list_head(is_split_updates);

	forboth(lcr, resultRelations, lcp, subpaths)
	{
		int			rti = lfirst_int(lcr);
		Path	   *subpath = (Path *) lfirst(lcp);
		RangeTblEntry *rte = rt_fetch(rti, root->parse->rtable);
		GpPolicy   *targetPolicy;
		GpPolicyType targetPolicyType;

		Assert(rte->rtekind == RTE_RELATION);

		targetPolicy = GpPolicyFetch(rte->relid);
		targetPolicyType = targetPolicy->ptype;

		numsegments = Max(targetPolicy->numsegments, numsegments);

		if (targetPolicyType == POLICYTYPE_PARTITIONED)
		{
			all_subplans_entry = false;
			all_subplans_replicated = false;
		}
		else if (targetPolicyType == POLICYTYPE_ENTRY)
		{
			/* Coordinator-only table */
			all_subplans_replicated = false;
		}
		else if (targetPolicyType == POLICYTYPE_REPLICATED)
		{
			all_subplans_entry = false;
		}
		else
			elog(ERROR, "unrecognized policy type %u", targetPolicyType);

		if (operation == CMD_INSERT)
		{
			subpath = create_motion_path_for_insert(root, targetPolicy, subpath);
		}
		else if (operation == CMD_DELETE)
		{
			subpath = create_motion_path_for_upddel(root, rti, targetPolicy, subpath);
		}
		else if (operation == CMD_UPDATE)
		{
			bool		is_split_update;

			is_split_update = (bool) lfirst_int(lci);

			if (is_split_update)
				subpath = create_split_update_path(root, rti, targetPolicy, subpath);
			else
				subpath = create_motion_path_for_upddel(root, rti, targetPolicy, subpath);

			lci = lnext(lci);
		}
		lfirst(lcp) = subpath;
	}

	/*
	 * Set the distribution of the ModifyTable node itself. If there is only
	 * one subplan, or all the subplans have a compatible distribution, then
	 * we could mark the ModifyTable with the same distribution key. Otherwise,
	 * mark the ModifyTable with Strewn.
	 * We may set a proper locus of Hashed for the ModifyTable to eliminate
	 * extra redistribution, but after discussion we decided it's not worthy
	 * to do because the scenario is not common and the benefit is limited.
	 */
	if (all_subplans_entry)
	{
		CdbPathLocus resultLocus;

		CdbPathLocus_MakeEntry(&resultLocus);
		return resultLocus;
	}
	else if (all_subplans_replicated)
	{
		CdbPathLocus resultLocus;

		Assert(numsegments >= 0);

		CdbPathLocus_MakeReplicated(&resultLocus, numsegments);
		return resultLocus;
	}
	else
	{
		CdbPathLocus resultLocus;

		Assert(numsegments >= 0);

		CdbPathLocus_MakeStrewn(&resultLocus, numsegments);

		return resultLocus;
	}
}

/*
 * create_limit_path
 *	  Creates a pathnode that represents performing LIMIT/OFFSET
 *
 * In addition to providing the actual OFFSET and LIMIT expressions,
 * the caller must provide estimates of their values for costing purposes.
 * The estimates are as computed by preprocess_limit(), ie, 0 represents
 * the clause not being present, and -1 means it's present but we could
 * not estimate its value.
 *
 * 'rel' is the parent relation associated with the result
 * 'subpath' is the path representing the source of data
 * 'limitOffset' is the actual OFFSET expression, or NULL
 * 'limitCount' is the actual LIMIT expression, or NULL
 * 'offset_est' is the estimated value of the OFFSET expression
 * 'count_est' is the estimated value of the LIMIT expression
 *
 * Greengage specific change: the return type is changed to Path
 * because at the end of function, we need to check if it is
 * segment general locus and may create other kind of path.
 */
Path *
create_limit_path(PlannerInfo *root, RelOptInfo *rel,
				  Path *subpath,
				  Node *limitOffset, Node *limitCount,
				  int64 offset_est, int64 count_est)
{
	LimitPath  *pathnode = makeNode(LimitPath);

	pathnode->path.pathtype = T_Limit;
	pathnode->path.parent = rel;
	/* Limit doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	/* For now, assume we are above any joins, so no parameterization */
	pathnode->path.param_info = NULL;
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = rel->consider_parallel &&
		subpath->parallel_safe;
	pathnode->path.parallel_workers = subpath->parallel_workers;
	pathnode->path.rows = subpath->rows;
	pathnode->path.startup_cost = subpath->startup_cost;
	pathnode->path.total_cost = subpath->total_cost;
	pathnode->path.pathkeys = subpath->pathkeys;
	pathnode->path.locus = subpath->locus;
	pathnode->subpath = subpath;
	pathnode->limitOffset = limitOffset;
	pathnode->limitCount = limitCount;

	/*
	 * Adjust the output rows count and costs according to the offset/limit.
	 */
	adjust_limit_rows_costs(&pathnode->path.rows,
							&pathnode->path.startup_cost,
							&pathnode->path.total_cost,
							offset_est, count_est);

	/*
	 * Greengage specific behavior:
	 * If the limit path's locus is general or segmentgeneral
	 * we have to make it singleQE.
	 */
	if (contain_volatile_functions(pathnode->limitOffset) || contain_volatile_functions(pathnode->limitCount))
		return turn_volatile_seggen_to_singleqe(root, (Path *) pathnode, NULL);
	else
		return (Path *)pathnode;
}

/*
 * adjust_limit_rows_costs
 *	  Adjust the size and cost estimates for a LimitPath node according to the
 *	  offset/limit.
 *
 * This is only a cosmetic issue if we are at top level, but if we are
 * building a subquery then it's important to report correct info to the outer
 * planner.
 *
 * When the offset or count couldn't be estimated, use 10% of the estimated
 * number of rows emitted from the subpath.
 *
 * XXX we don't bother to add eval costs of the offset/limit expressions
 * themselves to the path costs.  In theory we should, but in most cases those
 * expressions are trivial and it's just not worth the trouble.
 */
void
adjust_limit_rows_costs(double *rows,	/* in/out parameter */
						Cost *startup_cost, /* in/out parameter */
						Cost *total_cost,	/* in/out parameter */
						int64 offset_est,
						int64 count_est)
{
	double		input_rows = *rows;
	Cost		input_startup_cost = *startup_cost;
	Cost		input_total_cost = *total_cost;

	if (offset_est != 0)
	{
		double		offset_rows;

		if (offset_est > 0)
			offset_rows = (double) offset_est;
		else
			offset_rows = clamp_row_est(input_rows * 0.10);
		if (offset_rows > *rows)
			offset_rows = *rows;
		if (input_rows > 0)
			*startup_cost +=
				(input_total_cost - input_startup_cost)
				* offset_rows / input_rows;
		*rows -= offset_rows;
		if (*rows < 1)
			*rows = 1;
	}

	if (count_est != 0)
	{
		double		count_rows;

		if (count_est > 0)
			count_rows = (double) count_est;
		else
			count_rows = clamp_row_est(input_rows * 0.10);
		if (count_rows > *rows)
			count_rows = *rows;
		if (input_rows > 0)
			*total_cost = *startup_cost +
				(input_total_cost - input_startup_cost)
				* count_rows / input_rows;
		*rows = count_rows;
		if (*rows < 1)
			*rows = 1;
	}
}


/*
 * reparameterize_path
 *		Attempt to modify a Path to have greater parameterization
 *
 * We use this to attempt to bring all child paths of an appendrel to the
 * same parameterization level, ensuring that they all enforce the same set
 * of join quals (and thus that that parameterization can be attributed to
 * an append path built from such paths).  Currently, only a few path types
 * are supported here, though more could be added at need.  We return NULL
 * if we can't reparameterize the given path.
 *
 * Note: we intentionally do not pass created paths to add_path(); it would
 * possibly try to delete them on the grounds of being cost-inferior to the
 * paths they were made from, and we don't want that.  Paths made here are
 * not necessarily of general-purpose usefulness, but they can be useful
 * as members of an append path.
 */
Path *
reparameterize_path(PlannerInfo *root, Path *path,
					Relids required_outer,
					double loop_count)
{
	RelOptInfo *rel = path->parent;

	/* Can only increase, not decrease, path's parameterization */
	if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
		return NULL;
	switch (path->pathtype)
	{
		case T_SeqScan:
			return create_seqscan_path(root, rel, required_outer, 0);
		case T_SampleScan:
			return (Path *) create_samplescan_path(root, rel, required_outer);
		case T_IndexScan:
		case T_IndexOnlyScan:
			{
				IndexPath  *ipath = (IndexPath *) path;
				IndexPath  *newpath = makeNode(IndexPath);

				/*
				 * We can't use create_index_path directly, and would not want
				 * to because it would re-compute the indexqual conditions
				 * which is wasted effort.  Instead we hack things a bit:
				 * flat-copy the path node, revise its param_info, and redo
				 * the cost estimate.
				 */
				memcpy(newpath, ipath, sizeof(IndexPath));
				newpath->path.param_info =
					get_baserel_parampathinfo(root, rel, required_outer);
				cost_index(newpath, root, loop_count, false);
				return (Path *) newpath;
			}
		case T_BitmapHeapScan:
			{
				BitmapHeapPath *bpath = (BitmapHeapPath *) path;

				return (Path *) create_bitmap_heap_path(root,
														rel,
														bpath->bitmapqual,
														required_outer,
														loop_count, 0);
			}
		case T_SubqueryScan:
			{
				SubqueryScanPath *spath = (SubqueryScanPath *) path;

				return (Path *) create_subqueryscan_path(root,
														 rel,
														 spath->subpath,
														 spath->path.pathkeys,
														 spath->path.locus,
														 required_outer);
			}
		case T_Result:
			/* Supported only for RTE_RESULT scan paths */
			if (IsA(path, Path))
				return create_resultscan_path(root, rel, required_outer);
			break;
		case T_Append:
			{
				AppendPath *apath = (AppendPath *) path;
				List	   *childpaths = NIL;
				List	   *partialpaths = NIL;
				int			i;
				ListCell   *lc;

				/* Reparameterize the children */
				i = 0;
				foreach(lc, apath->subpaths)
				{
					Path	   *spath = (Path *) lfirst(lc);

					spath = reparameterize_path(root, spath,
												required_outer,
												loop_count);
					if (spath == NULL)
						return NULL;
					/* We have to re-split the regular and partial paths */
					if (i < apath->first_partial_path)
						childpaths = lappend(childpaths, spath);
					else
						partialpaths = lappend(partialpaths, spath);
					i++;
				}
				return (Path *)
					create_append_path(root, rel, childpaths, partialpaths,
									   apath->path.pathkeys, required_outer,
									   apath->path.parallel_workers,
									   apath->path.parallel_aware,
									   apath->partitioned_rels,
									   -1);
			}
		default:
			break;
	}
	return NULL;
}

/*
 * reparameterize_path_by_child
 * 		Given a path parameterized by the parent of the given child relation,
 * 		translate the path to be parameterized by the given child relation.
 *
 * The function creates a new path of the same type as the given path, but
 * parameterized by the given child relation.  Most fields from the original
 * path can simply be flat-copied, but any expressions must be adjusted to
 * refer to the correct varnos, and any paths must be recursively
 * reparameterized.  Other fields that refer to specific relids also need
 * adjustment.
 *
 * The cost, number of rows, width and parallel path properties depend upon
 * path->parent, which does not change during the translation. Hence those
 * members are copied as they are.
 *
 * If the given path can not be reparameterized, the function returns NULL.
 */
Path *
reparameterize_path_by_child(PlannerInfo *root, Path *path,
							 RelOptInfo *child_rel)
{

#define FLAT_COPY_PATH(newnode, node, nodetype)  \
	( (newnode) = makeNode(nodetype), \
	  memcpy((newnode), (node), sizeof(nodetype)) )

#define ADJUST_CHILD_ATTRS(node) \
	((node) = \
	 (List *) adjust_appendrel_attrs_multilevel(root, (Node *) (node), \
												child_rel->relids, \
												child_rel->top_parent_relids))

#define REPARAMETERIZE_CHILD_PATH(path) \
do { \
	(path) = reparameterize_path_by_child(root, (path), child_rel); \
	if ((path) == NULL) \
		return NULL; \
} while(0)

#define REPARAMETERIZE_CHILD_PATH_LIST(pathlist) \
do { \
	if ((pathlist) != NIL) \
	{ \
		(pathlist) = reparameterize_pathlist_by_child(root, (pathlist), \
													  child_rel); \
		if ((pathlist) == NIL) \
			return NULL; \
	} \
} while(0)

	Path	   *new_path;
	ParamPathInfo *new_ppi;
	ParamPathInfo *old_ppi;
	Relids		required_outer;

	/*
	 * If the path is not parameterized by parent of the given relation, it
	 * doesn't need reparameterization.
	 */
	if (!path->param_info ||
		!bms_overlap(PATH_REQ_OUTER(path), child_rel->top_parent_relids))
		return path;

	/*
	 * If possible, reparameterize the given path, making a copy.
	 *
	 * This function is currently only applied to the inner side of a nestloop
	 * join that is being partitioned by the partitionwise-join code.  Hence,
	 * we need only support path types that plausibly arise in that context.
	 * (In particular, supporting sorted path types would be a waste of code
	 * and cycles: even if we translated them here, they'd just lose in
	 * subsequent cost comparisons.)  If we do see an unsupported path type,
	 * that just means we won't be able to generate a partitionwise-join plan
	 * using that path type.
	 */
	switch (nodeTag(path))
	{
		case T_Path:
			FLAT_COPY_PATH(new_path, path, Path);
			break;

		case T_IndexPath:
			{
				IndexPath  *ipath;

				FLAT_COPY_PATH(ipath, path, IndexPath);
				ADJUST_CHILD_ATTRS(ipath->indexclauses);
				new_path = (Path *) ipath;
			}
			break;

		case T_BitmapHeapPath:
			{
				BitmapHeapPath *bhpath;

				FLAT_COPY_PATH(bhpath, path, BitmapHeapPath);
				REPARAMETERIZE_CHILD_PATH(bhpath->bitmapqual);
				new_path = (Path *) bhpath;
			}
			break;

		case T_BitmapAndPath:
			{
				BitmapAndPath *bapath;

				FLAT_COPY_PATH(bapath, path, BitmapAndPath);
				REPARAMETERIZE_CHILD_PATH_LIST(bapath->bitmapquals);
				new_path = (Path *) bapath;
			}
			break;

		case T_BitmapOrPath:
			{
				BitmapOrPath *bopath;

				FLAT_COPY_PATH(bopath, path, BitmapOrPath);
				REPARAMETERIZE_CHILD_PATH_LIST(bopath->bitmapquals);
				new_path = (Path *) bopath;
			}
			break;

		case T_ForeignPath:
			{
				ForeignPath *fpath;
				ReparameterizeForeignPathByChild_function rfpc_func;

				FLAT_COPY_PATH(fpath, path, ForeignPath);
				if (fpath->fdw_outerpath)
					REPARAMETERIZE_CHILD_PATH(fpath->fdw_outerpath);

				/* Hand over to FDW if needed. */
				rfpc_func =
					path->parent->fdwroutine->ReparameterizeForeignPathByChild;
				if (rfpc_func)
					fpath->fdw_private = rfpc_func(root, fpath->fdw_private,
												   child_rel);
				new_path = (Path *) fpath;
			}
			break;

		case T_CustomPath:
			{
				CustomPath *cpath;

				FLAT_COPY_PATH(cpath, path, CustomPath);
				REPARAMETERIZE_CHILD_PATH_LIST(cpath->custom_paths);
				if (cpath->methods &&
					cpath->methods->ReparameterizeCustomPathByChild)
					cpath->custom_private =
						cpath->methods->ReparameterizeCustomPathByChild(root,
																		cpath->custom_private,
																		child_rel);
				new_path = (Path *) cpath;
			}
			break;

		case T_NestPath:
			{
				JoinPath   *jpath;

				FLAT_COPY_PATH(jpath, path, NestPath);

				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				new_path = (Path *) jpath;
			}
			break;

		case T_MergePath:
			{
				JoinPath   *jpath;
				MergePath  *mpath;

				FLAT_COPY_PATH(mpath, path, MergePath);

				jpath = (JoinPath *) mpath;
				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				ADJUST_CHILD_ATTRS(mpath->path_mergeclauses);
				new_path = (Path *) mpath;
			}
			break;

		case T_HashPath:
			{
				JoinPath   *jpath;
				HashPath   *hpath;

				FLAT_COPY_PATH(hpath, path, HashPath);

				jpath = (JoinPath *) hpath;
				REPARAMETERIZE_CHILD_PATH(jpath->outerjoinpath);
				REPARAMETERIZE_CHILD_PATH(jpath->innerjoinpath);
				ADJUST_CHILD_ATTRS(jpath->joinrestrictinfo);
				ADJUST_CHILD_ATTRS(hpath->path_hashclauses);
				new_path = (Path *) hpath;
			}
			break;

		case T_AppendPath:
			{
				AppendPath *apath;

				FLAT_COPY_PATH(apath, path, AppendPath);
				REPARAMETERIZE_CHILD_PATH_LIST(apath->subpaths);
				new_path = (Path *) apath;
			}
			break;

		case T_GatherPath:
			{
				GatherPath *gpath;

				FLAT_COPY_PATH(gpath, path, GatherPath);
				REPARAMETERIZE_CHILD_PATH(gpath->subpath);
				new_path = (Path *) gpath;
			}
			break;

		default:

			/* We don't know how to reparameterize this path. */
			return NULL;
	}

	/*
	 * Adjust the parameterization information, which refers to the topmost
	 * parent. The topmost parent can be multiple levels away from the given
	 * child, hence use multi-level expression adjustment routines.
	 */
	old_ppi = new_path->param_info;
	required_outer =
		adjust_child_relids_multilevel(root, old_ppi->ppi_req_outer,
									   child_rel->relids,
									   child_rel->top_parent_relids);

	/* If we already have a PPI for this parameterization, just return it */
	new_ppi = find_param_path_info(new_path->parent, required_outer);

	/*
	 * If not, build a new one and link it to the list of PPIs. For the same
	 * reason as explained in mark_dummy_rel(), allocate new PPI in the same
	 * context the given RelOptInfo is in.
	 */
	if (new_ppi == NULL)
	{
		MemoryContext oldcontext;
		RelOptInfo *rel = path->parent;

		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));

		new_ppi = makeNode(ParamPathInfo);
		new_ppi->ppi_req_outer = bms_copy(required_outer);
		new_ppi->ppi_rows = old_ppi->ppi_rows;
		new_ppi->ppi_clauses = old_ppi->ppi_clauses;
		ADJUST_CHILD_ATTRS(new_ppi->ppi_clauses);
		rel->ppilist = lappend(rel->ppilist, new_ppi);

		MemoryContextSwitchTo(oldcontext);
	}
	bms_free(required_outer);

	new_path->param_info = new_ppi;

	/*
	 * Adjust the path target if the parent of the outer relation is
	 * referenced in the targetlist. This can happen when only the parent of
	 * outer relation is laterally referenced in this relation.
	 */
	if (bms_overlap(path->parent->lateral_relids,
					child_rel->top_parent_relids))
	{
		new_path->pathtarget = copy_pathtarget(new_path->pathtarget);
		ADJUST_CHILD_ATTRS(new_path->pathtarget->exprs);
	}

	return new_path;
}

/*
 * reparameterize_pathlist_by_child
 * 		Helper function to reparameterize a list of paths by given child rel.
 */
static List *
reparameterize_pathlist_by_child(PlannerInfo *root,
								 List *pathlist,
								 RelOptInfo *child_rel)
{
	ListCell   *lc;
	List	   *result = NIL;

	foreach(lc, pathlist)
	{
		Path	   *path = reparameterize_path_by_child(root, lfirst(lc),
														child_rel);

		if (path == NULL)
		{
			list_free(result);
			return NIL;
		}

		result = lappend(result, path);
	}

	return result;
}
