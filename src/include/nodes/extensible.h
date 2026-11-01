/*-------------------------------------------------------------------------
 *
 * extensible.h
 *	  Definitions for extensible nodes and custom scans
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/extensible.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXTENSIBLE_H
#define EXTENSIBLE_H

#include "access/parallel.h"
#include "commands/explain.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"

/* maximum length of an extensible node identifier */
#define EXTNODENAME_MAX_LEN					64

/*
 * An extensible node is a new type of node defined by an extension.  The
 * type is always T_ExtensibleNode, while the extnodename identifies the
 * specific type of node.  extnodename can be looked up to find the
 * ExtensibleNodeMethods for this node type.
 */
typedef struct ExtensibleNode
{
	NodeTag		type;
	const char *extnodename;	/* identifier of ExtensibleNodeMethods */
} ExtensibleNode;

/*
 * node_size is the size of an extensible node of this type in bytes.
 *
 * nodeCopy is a function which performs a deep copy from oldnode to newnode.
 * It does not need to copy type or extnodename, which are copied by the
 * core system.
 *
 * nodeEqual is a function which performs a deep equality comparison between
 * a and b and returns true or false accordingly.  It does not need to compare
 * type or extnodename, which are compared by the core system.
 *
 * nodeOut is a serialization function for the node type.  It should use the
 * output conventions typical for outfuncs.c.  It does not need to output
 * type or extnodename; the core system handles those.
 *
 * nodeRead is a deserialization function for the node type.  It does not need
 * to read type or extnodename; the core system handles those.  It should fetch
 * the next token using pg_strtok() from the current input stream, and then
 * reconstruct the private fields according to the manner in readfuncs.c.
 *
 * All callbacks are mandatory.
 */
typedef struct ExtensibleNodeMethods
{
	const char *extnodename;
	Size		node_size;
	void		(*nodeCopy) (struct ExtensibleNode *newnode,
							 const struct ExtensibleNode *oldnode);
	bool		(*nodeEqual) (const struct ExtensibleNode *a,
							  const struct ExtensibleNode *b);
	void		(*nodeOut) (struct StringInfoData *str,
							const struct ExtensibleNode *node);
	void		(*nodeRead) (struct ExtensibleNode *node);
} ExtensibleNodeMethods;

extern void RegisterExtensibleNodeMethods(const ExtensibleNodeMethods *method);
extern const ExtensibleNodeMethods *GetExtensibleNodeMethods(const char *name,
															 bool missing_ok);

/*
 * Flags for custom paths, indicating what capabilities the resulting scan
 * will have.
 */
#define CUSTOMPATH_SUPPORT_BACKWARD_SCAN	0x0001
#define CUSTOMPATH_SUPPORT_MARK_RESTORE		0x0002

/*
 * Greengage: the flag comes from PostgreSQL 15. Before it, the planner took
 * every custom scan to compute its output expressions; now a Result above a
 * scan without the flag computes them. The planner reads the flag on the
 * path and on the plan, so PlanCustomPath copies the path's flags to
 * CustomScan.flags, as it does the flags below.
 */
#define CUSTOMPATH_SUPPORT_PROJECTION		0x0004

/*
 * Greengage: the custom scan reads rows only from its children, the plans
 * the core makes from custom_paths, as an Agg or a Hash Join does: no
 * relation itself and no plan it adds in PlanCustomPath. One without
 * children returns no rows of a relation, as a Values Scan. Targeted
 * dispatch then sends its slice where the scans under it go. The flag
 * counts only for a scan whose scanrelid is 0: a custom scan of a table
 * follows the table's restrictions, as a Seq Scan does, where the path's
 * locus is the table's and not Strewn. Any other custom scan turns
 * targeted dispatch off, as a foreign scan does: the core cannot tell
 * where the provider reads its rows.
 */
#define CUSTOMPATH_GP_CHILD_ROWS_ONLY		0x00010000

/*
 * Greengage: the custom scan needs memory of its own, as a Hash or a Sort
 * does. The resource manager gives such a scan a share of the query's
 * memory (memquota.c), which it reads with PlanStateOperatorMemKB; a scan
 * without the flag gets the small share of an operator that needs none.
 *
 * Eager free gives the operators above a blocking one, such as a Sort, the
 * memory of those below it, which are done once it has read its input: a
 * scan should release its memory when it has returned its last row, as a
 * Sort does, and in SquelchCustomScan, when its parent stops reading it, as
 * a LIMIT does. Eager free does not count the memory a scan keeps after a
 * squelch, as a Sort that may be rescanned keeps its own: once a blocking
 * operator above the LIMIT has read its input, the operators above it get
 * the memory of the scan too, so such a plan may use more than the query's
 * memory.
 */
#define CUSTOMPATH_GP_MEMORY_INTENSIVE		0x00020000

/*
 * Greengage: the custom scan needs its memory at the same time as the
 * operators under it, not after them: it takes the memory before one of
 * its children has returned all its rows. So does a scan that builds a
 * table from one child and keeps it while it reads the next, as a hash
 * join keeps the table of its inner side while it reads its outer side,
 * and a scan that takes its memory when it starts. The flag holds the bit
 * of CUSTOMPATH_GP_MEMORY_INTENSIVE, so a provider needs to set only one
 * of the two, and a test of CUSTOMPATH_GP_MEMORY_INTENSIVE finds a scan
 * with either. Test this flag as
 * (flags & CUSTOMPATH_GP_MEMORY_OVERLAPS_CHILDREN) ==
 * CUSTOMPATH_GP_MEMORY_OVERLAPS_CHILDREN.
 *
 * Eager free counts a scan with CUSTOMPATH_GP_MEMORY_INTENSIVE alone as it
 * counts a WindowAgg: the scan takes memory only once rows reach it, when
 * the blocking operators under it, such as a Sort, have read their input
 * and the operators under those have released their memory. So the scan
 * and those operators get the same memory, which they use in turn. A scan
 * with this flag gets a share of its own beside the operators under it
 * instead, as a Hash does beside the outer side of its join, and they
 * split the memory.
 */
#define CUSTOMPATH_GP_MEMORY_OVERLAPS_CHILDREN \
	(CUSTOMPATH_GP_MEMORY_INTENSIVE | 0x00040000)

/*
 * Custom path methods.  Mostly, we just need to know how to convert a
 * CustomPath to a plan.
 *
 * Greengage: a custom scan's children are visited as any plan's are: the
 * plan walkers and mutators, the slice table, squelch and the walks of
 * plan states go into custom_plans and custom_ps. The walkers do not look
 * into custom_private, as in PostgreSQL: keep in custom_exprs what the
 * core must see.
 *
 * As for any path, the provider sets the fields of its CustomPath that
 * Greengage adds: locus, where the rows lie, which for a scan over
 * children is usually that of the child it returns the rows of;
 * motionHazard and rescannable, from its children; sameslice_relids. A
 * scan of the parent of an inheritance tree or of a partitioned table
 * returns the rows of its members too, so its locus is that of the rel's
 * Append path, not one from cdbpathlocus_from_baserel, which knows the
 * parent table only. The plan reaches the segments, so the library that
 * registers the scan's methods is loaded there too.
 *
 * List custom_plans in the order the scan runs them: a hash join lists
 * its inner side first, as it builds its table before it reads the outer
 * side. The walk that picks the producer of a CTE shared by several scans
 * visits lefttree, righttree and then custom_plans in list order, and the
 * first scan of the CTE it meets produces it. A producer shared with
 * another slice writes out the whole CTE at its first row, and the other
 * slice waits for it. So a child must reach its scans of a shared CTE
 * before the scan waits on a later child, as when the scan reads each
 * child to its end before it starts the next; otherwise each slice may
 * wait for the other. Motions under two children deadlock the same way
 * when the scan reads a row from one and then waits on the other, as a
 * join without prefetch_inner would (see motion_sanity_walker), and the
 * core does not check how a custom scan reads its children. EXPLAIN shows
 * the children in the order of custom_ps, which the provider may build
 * otherwise.
 */
typedef struct CustomPathMethods
{
	const char *CustomName;

	/* Convert Path to a Plan */
	struct Plan *(*PlanCustomPath) (PlannerInfo *root,
									RelOptInfo *rel,
									struct CustomPath *best_path,
									List *tlist,
									List *clauses,
									List *custom_plans);
	struct List *(*ReparameterizeCustomPathByChild) (PlannerInfo *root,
													 List *custom_private,
													 RelOptInfo *child_rel);
}			CustomPathMethods;

/*
 * Custom scan.  Here again, there's not much to do: we need to be able to
 * generate a ScanState corresponding to the scan.
 */
typedef struct CustomScanMethods
{
	const char *CustomName;

	/* Create execution state (CustomScanState) from a CustomScan plan node */
	Node	   *(*CreateCustomScanState) (CustomScan *cscan);
} CustomScanMethods;

/*
 * Execution-time methods for a CustomScanState.  This is more complex than
 * what we need for a custom path or scan.
 *
 * Greengage: a squelch tells the scan that its parent reads no more rows
 * from it until a rescan, as a Limit does once it has its rows. It calls
 * SquelchCustomScan, which may release the scan's memory and must squelch
 * each child the scan will not read again, in outerPlanState,
 * innerPlanState or custom_ps, with ExecSquelchNode, even a child the scan
 * has not started: otherwise a Motion under the child may keep senders
 * on other segments waiting, and the query may hang (see ExecSquelchNode).
 * A scan keeps a child unsquelched only to read on from it after a rescan
 * without rescanning it, as a Material keeps a Motion it buffers. Without
 * the callback, the core squelches all the children.
 *
 * The callback runs on the coordinator and on the segments, at most once
 * until the next rescan, and may come before the scan's first row, after
 * its last, or after ShutdownCustomScan. A squelch does not call
 * ShutdownCustomScan, since a rescan may follow it: a Nested Loop
 * squelches itself each time it runs out of rows, and a SubPlan rescans it
 * for its next row. So the scan rebuilds on rescan what it released in
 * the callback, or keeps it, as a Sort keeps its rows under
 * EXEC_FLAG_REWIND, EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK. A plan need not
 * end with a squelch, so EndCustomScan still releases what is left.
 * ExecReScan clears the squelch of the node it rescans only, so
 * ReScanCustomScan rescans each child the scan reads again; a scan that
 * stops reading a child early squelches it, as a Limit does.
 *
 * ShutdownCustomScan runs where PostgreSQL runs it, after each run of the
 * executor, and the scan may be asked for more rows after it, as by the
 * next FETCH from a cursor that cannot scroll. The coordinator initializes
 * the whole plan, the slices the segments run too, so it calls
 * BeginCustomScan, ShutdownCustomScan and EndCustomScan of a scan that only
 * the segments run: a scan takes its memory and other resources when it
 * first runs, not in BeginCustomScan.
 */
typedef struct CustomExecMethods
{
	const char *CustomName;

	/* Required executor methods */
	void		(*BeginCustomScan) (CustomScanState *node,
									EState *estate,
									int eflags);
	TupleTableSlot *(*ExecCustomScan) (CustomScanState *node);
	void		(*EndCustomScan) (CustomScanState *node);
	void		(*ReScanCustomScan) (CustomScanState *node);

	/* Optional methods: needed if mark/restore is supported */
	void		(*MarkPosCustomScan) (CustomScanState *node);
	void		(*RestrPosCustomScan) (CustomScanState *node);

	/* Optional methods: needed if parallel execution is supported */
	Size		(*EstimateDSMCustomScan) (CustomScanState *node,
										  ParallelContext *pcxt);
	void		(*InitializeDSMCustomScan) (CustomScanState *node,
											ParallelContext *pcxt,
											void *coordinate);
	void		(*ReInitializeDSMCustomScan) (CustomScanState *node,
											  ParallelContext *pcxt,
											  void *coordinate);
	void		(*InitializeWorkerCustomScan) (CustomScanState *node,
											   shm_toc *toc,
											   void *coordinate);
	void		(*ShutdownCustomScan) (CustomScanState *node);

	/* Optional: print additional information in EXPLAIN */
	void		(*ExplainCustomScan) (CustomScanState *node,
									  List *ancestors,
									  ExplainState *es);

	/* Greengage, optional: the parent reads no more rows (see above) */
	void		(*SquelchCustomScan) (CustomScanState *node);
} CustomExecMethods;

extern void RegisterCustomScanMethods(const CustomScanMethods *methods);
extern const CustomScanMethods *GetCustomScanMethods(const char *CustomName,
													 bool missing_ok);

#endif							/* EXTENSIBLE_H */
