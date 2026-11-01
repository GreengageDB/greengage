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
#define CUSTOMPATH_SUPPORT_PROJECTION		0x0004

/*
 * Greengage: the scan executes its custom_plans from the last to the
 * first, as a hash join builds its inner side before it reads the outer
 * one. Without it, they execute in list order. Walks that must follow
 * the order of execution, such as the one that assigns the producers of
 * shared scans, visit the children so.
 */
#define CUSTOMPATH_GP_LAST_CHILD_FIRST		0x00010000

/*
 * Greengage: a custom scan's children are visited as any plan's are: the
 * plan walkers and mutators, the slice table, squelch and the walks of
 * plan states go into custom_plans and custom_ps. Targeted dispatch follows
 * the restrictions of the table a custom scan reads (scanrelid), where the
 * path's locus is the table's and not Strewn, or else its children, if
 * they reach every relation of the scan's rel (custom_relids); otherwise
 * the provider reads a relation itself, and the scan turns targeted
 * dispatch off, as a foreign scan does. An extension whose custom scans
 * have children of their own needs these.
 *
 * The walkers do not look into custom_private, as in PostgreSQL: keep in
 * custom_exprs what the core must see. Squelch reaches the children but
 * does not call ShutdownCustomScan, since a rescan may follow a squelch:
 * the callback runs where PostgreSQL runs it, after each run of the
 * executor.
 */
#define CUSTOMSCAN_GP_CHILDREN 1

/*
 * Greengage: the custom scan needs memory of its own, as a Hash or a Sort
 * does. The resource manager gives such a scan a share of the query's
 * memory (memquota.c), which it reads with PlanStateOperatorMemKB; a scan
 * without the flag gets the small share of an operator that needs none.
 * The flag passes from the path to the plan as the provider sets it.
 *
 * Eager free gives the operators above a blocking one, such as a Sort, the
 * memory of those below it, which are done once it has read its input: a
 * scan should release its memory when it has read its children to the
 * end. A squelch, as from a LIMIT, does not tell the scan to release it,
 * and the scan keeps it until the plan ends, as a Sort that may be
 * rescanned keeps its own.
 */
#define CUSTOMPATH_GP_MEMORY_INTENSIVE		0x00020000

/*
 * Custom path methods.  Mostly, we just need to know how to convert a
 * CustomPath to a plan.
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
} CustomExecMethods;

extern void RegisterCustomScanMethods(const CustomScanMethods *methods);
extern const CustomScanMethods *GetCustomScanMethods(const char *CustomName,
													 bool missing_ok);

#endif							/* EXTENSIBLE_H */
