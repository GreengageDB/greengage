/*-------------------------------------------------------------------------
 *
 * tempcat.c
 *	  In-memory catalog for temporary table metadata.
 *
 * Temporary table metadata (pg_class rows, pg_attribute rows, index entries,
 * etc.) is stored entirely in backend-private memory instead of on-disk
 * pg_catalog tables.  Virtual tuples are mixed with regular on-disk tuples
 * during system catalog scans so the rest of the system sees a unified view.
 *
 * Because temporary tables are visible only within a single session, there
 * is no need for shared memory, locks, or MVCC bookkeeping (xmin/xmax).
 * Transaction support uses a simple snapshot stack: each transaction or
 * savepoint pushes a copy of the current state, and commit/abort pops or
 * discards it.
 *
 * This solves the pg_catalog bloating problem caused by applications that
 * create and drop many temporary tables.  Without this, every temp table
 * leaves dead tuples in shared catalog tables, triggering expensive
 * autovacuum runs that affect the entire database.
 *
 * The feature is gated by the GUC gp_enable_temp_memory_catalog.  When
 * enabled, DDL functions wrap temp-table operations in
 * BEGIN_TEMP_TABLE_SCOPE / END_TEMP_TABLE_SCOPE (see tempcat.h), and the
 * catalog DML layer (CatalogTupleInsert, etc.) redirects writes to this
 * module.  System catalog scans (systable_beginscan, etc.) merge virtual
 * tuples from tempcat alongside regular on-disk tuples.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/tempcat.c
 *
 *-------------------------------------------------------------------------
 */
 
#include "postgres.h"

#include "pgstat.h"
#include "miscadmin.h"
#include "access/tempcat.h"
#include "access/relscan.h"
#include "access/valid.h"
#include "access/memtup.h"
#include "access/sysattr.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_statistic.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "lib/stringinfo.h"

/*****************************************************************************
		  TYPEDEFS, MACRO DECLARATIONS AND CONST STATIC VARIABLES
 *****************************************************************************/

/*
 * Maximum number of scan key columns supported by tempcat scans.
 * The widest system catalog indexes have 4 columns (e.g. pg_amop_fam_strat,
 * pg_seclabel_object, pg_depend_depender).  If a future catalog index exceeds
 * this, tempcat_beginscan has an assert.
 */
#define TempcatIndexMaxAttributes 4

/* List of in-memory tuples. */
typedef struct
{
	dlist_node	node;
	HeapTuple	tup;
}	DListHeapTuple;

/* List of virtual tuples of single relation */
typedef struct
{
	dlist_node	node;
	Oid			relid;
	int			tuples_num;		/* number of virtual tuples */
	dlist_head	tuples;			/* list of virtual tuples */
}	TempcatSnapshotRelationData;

struct TempCatScanData
{
	TempcatSnapshotRelationData *rel;
	TupleDesc	tupdesc;
	ScanKey key;
	int nkeys;
	AttrNumber	attrNumbers[TempcatIndexMaxAttributes];
	FmgrInfo    attrOpFuncs[TempcatIndexMaxAttributes];	/* operator functions from sk_func (return bool) */
	FmgrInfo    attrCmpFuncs[TempcatIndexMaxAttributes];	/* btree cmp functions from type cache (three-way) */
	Oid         attrCollations[TempcatIndexMaxAttributes];
	bool scan_finish_returned;
	bool inmem_tuplist_init_done;
	dlist_head inmem_tuplist;		/* list of virtual tuples */
};

/*
 * Snapshot represents state of virtual heap for current transaction or
 * savepoint.
 */
struct TempcatSnapshotData
{
	/* Previous snapshot to rollback to. */
	struct TempcatSnapshotData *prev;
	/* Optional name of a savepoint. Can be NULL. */
	char	   *name;
	/* State of relations that can contain virtual tuples */
	dlist_head relationData; /* TempcatSnapshotRelationData::node */
}	TempcatSnapshotData;

typedef struct TempcatSnapshotData *TempcatSnapshot;

/* Determine whether given snapshot is a root snapshot. */
#define TempcatSnapshotIsRoot(sn) ( !PointerIsValid((sn)->prev) )

/* Determine whether given snapshot is anonymous. */
#define TempcatSnapshotIsAnonymous(sn) ( !PointerIsValid((sn)->name) )

/* Determine whether there is a transaction in progress. */
#define TempcatTransactionInProgress() \
	( PointerIsValid(TempcatSnapshotGetCurrent()->prev))

/*****************************************************************************
							 GLOBAL VARIABLES
 *****************************************************************************/

/* Memory context used to store virtual catalog */
static MemoryContext LocalMemoryContextPrivate = NULL;

/* Whether in-memory catalog feature is enabled */
bool enable_temp_memory_catalog = false;
/* Whether current DDL operation targets a temp table (see tempcat.h) */
bool temp_table_scope = false;
/* Counters used to generate unique virtual ItemPointers */
static uint32 CurrentTempcatBlockId = 0;
static uint16 CurrentTempcatOffset = 1; /* NB: 0 is considered invalid */

/* Current snapshot */
static TempcatSnapshot CurrentTempcatSnapshotPrivate = NULL;

/* Dirty flag: set when tempcat content changes, cleared after serialization */
static bool TempcatDirtyFlag = false;

/*****************************************************************************
							UTILITY PROCEDURES
 *****************************************************************************/

/*
 * Get memory context for storing virtual catalog. Create one if necessary.
 */
static MemoryContext
GetLocalMemoryContext(void)
{
	if (!PointerIsValid(LocalMemoryContextPrivate))
	{
		LocalMemoryContextPrivate = AllocSetContextCreate(
														  NULL,
											"Virtual catalog memory context",
													ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);
	}

	return LocalMemoryContextPrivate;
}

/*
 * Generate unique virtual ItemPointer
 */
static ItemPointerData
GenTempcatItemPointerData(void)
{
	ItemPointerData res;

	BlockIdSet(&(res.ip_blkid), CurrentTempcatBlockId);
	res.ip_posid = CurrentTempcatOffset | TEMPCAT_ITEM_POINTER_BIT;

	CurrentTempcatOffset++;

	if (CurrentTempcatOffset > MaxHeapTuplesPerPage)
	{
		CurrentTempcatOffset = 1;
		CurrentTempcatBlockId++;

#ifdef TEMPCAT_DEBUG
		elog(NOTICE, "TEMPCAT: GenTempcatItemPointerData, CurrentTempcatOffset > MaxHeapTuplesPerPage (%d), new values - CurrentTempcatOffset = %d, CurrentTempcatBlockId = %d",
		  MaxHeapTuplesPerPage, CurrentTempcatOffset, CurrentTempcatBlockId);
#endif
	}

	return res;
}

/*
 * Free single DListHeapTuple
 */
static void
DListHeapTupleFree(DListHeapTuple *tup)
{
	heap_freetuple(tup->tup);
	pfree(tup);
}

/*
 * Free list of DListHeapTuple's
 */
static void
TempcatDListFree(dlist_head *head)
{
	while (!dlist_is_empty(head))
	{
		DListHeapTuple *dlist_tup = (DListHeapTuple *) dlist_pop_head_node(head);

		DListHeapTupleFree(dlist_tup);
	}
}

/*
 * Create a new empty snapshot.
 */
static TempcatSnapshot
TempcatSnapshotCreateEmpty(void)
{
	TempcatSnapshot result;
	MemoryContext oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());

	result = palloc0(sizeof(TempcatSnapshotData));
	MemoryContextSwitchTo(oldctx);
	return result;
}

/*
 * Create a snapshot copy.
 */
static TempcatSnapshot
TempcatSnapshotCopy(TempcatSnapshot src, const char *dst_name)
{
	dlist_iter	iter;
	MemoryContext oldctx;
	TempcatSnapshot dst = TempcatSnapshotCreateEmpty();

	oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());
	dst->name = dst_name ? pstrdup(dst_name) : NULL;

	dlist_foreach(iter, &src->relationData)
	{
		TempcatSnapshotRelationData *src_rel = dlist_container(TempcatSnapshotRelationData, node, iter.cur);
		TempcatSnapshotRelationData *dst_rel = palloc(sizeof(TempcatSnapshotRelationData));
		dlist_iter tup_iter;

		dst_rel->relid = src_rel->relid;
		dst_rel->tuples_num = src_rel->tuples_num;
		dlist_init(&dst_rel->tuples);

		dlist_foreach(tup_iter, &src_rel->tuples)
		{
			DListHeapTuple *src_dlist_tup = (DListHeapTuple *) tup_iter.cur;
			DListHeapTuple *dst_dlist_tup = palloc(sizeof(DListHeapTuple));

			dst_dlist_tup->tup = heap_copytuple(src_dlist_tup->tup);
			dlist_push_tail(&dst_rel->tuples, &dst_dlist_tup->node);
		}

		dlist_push_tail(&dst->relationData, &dst_rel->node);
	}

	MemoryContextSwitchTo(oldctx);
	return dst;
}

/*
 * Free snapshot.
 */
static void
TempcatSnapshotFree(TempcatSnapshot tempcat_snapshot)
{
	dlist_iter iter;

	dlist_foreach(iter, &tempcat_snapshot->relationData)
	{
		TempcatSnapshotRelationData *rel_entry = dlist_container(TempcatSnapshotRelationData, node, iter.cur);
		TempcatDListFree(&rel_entry->tuples);
	}

	if (PointerIsValid(tempcat_snapshot->name))
		pfree(tempcat_snapshot->name);

	pfree(tempcat_snapshot);
}

/*
 * Get current snapshot. Create one if necessary.
 */
static TempcatSnapshot
TempcatSnapshotGetCurrent(void)
{
	if (!PointerIsValid(CurrentTempcatSnapshotPrivate))
		CurrentTempcatSnapshotPrivate = TempcatSnapshotCreateEmpty();

	return CurrentTempcatSnapshotPrivate;
}

/*
 * Places a snapshot on top of snapshots stack. Placed snapshot becomes
 * current.
 */
static inline void
TempcatSnapshotPushBack(TempcatSnapshot tempcat_snapshot)
{
	tempcat_snapshot->prev = TempcatSnapshotGetCurrent();
	CurrentTempcatSnapshotPrivate = tempcat_snapshot;
}

/*
 * Removes snapshot from top of snapshots stack.
 *
 * Returns valid TempcatSnapshot or NULL if only root snapshot left.
 */
static TempcatSnapshot
TempcatSnapshotPopBack(void)
{
	TempcatSnapshot curr = TempcatSnapshotGetCurrent();

	if (TempcatSnapshotIsRoot(curr))
		return NULL;

	CurrentTempcatSnapshotPrivate = curr->prev;
	curr->prev = NULL;
	return curr;
}

/*
 * Creates a copy of current snapshot with given name (can be NULL) and places
 * it on top of snapshots stack. This copy becomes current snapshot.
 */
static void
TempcatSnapshotCreate(const char *name)
{
	TempcatSnapshot src = TempcatSnapshotGetCurrent();
	TempcatSnapshot dst = TempcatSnapshotCopy(src, name);

	TempcatSnapshotPushBack(dst);
}

/*
 * Makes given snapshot a root one.
 */
static void
TempcatSnapshotPushFront(TempcatSnapshot tempcat_snapshot)
{
	TempcatSnapshot temp = TempcatSnapshotGetCurrent();

	while (!TempcatSnapshotIsRoot(temp))
		temp = temp->prev;

	temp->prev = tempcat_snapshot;
	tempcat_snapshot->prev = NULL;
}

/*****************************************************************************
							 MAIN PROCEDURES
 *****************************************************************************/

/*
 * Make preparations related to virtual catalog on transaction begin.
 *
 * NB: There could be already a transaction in progress.
 */
void
tempcat_begin_transaction(void)
{
#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_begin_transaction, transaction is already in progress: %u",
		 TempcatTransactionInProgress());
#endif

	if (TempcatTransactionInProgress())
		return;

	/* begin transaction */
	TempcatSnapshotCreate(NULL);
	Assert(TempcatTransactionInProgress());
	Assert(TempcatSnapshotIsAnonymous(TempcatSnapshotGetCurrent()));
}

/*
 * Perform actions related to virtual catalog on transaction commit.
 *
 * NB: There could be actually no transaction in progress.
 */
void
tempcat_end_transaction(void)
{
#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_end_transaction result (1 - commit, 0 - rollback)"
		 ", transaction is in progress: %u", TempcatTransactionInProgress());
#endif

	if (!TempcatTransactionInProgress())
		return;

	Assert(TempcatSnapshotIsAnonymous(TempcatSnapshotGetCurrent()));

	/* Commit transaction. 1) Save top snapshot to the bottom of the stack. */
	TempcatSnapshotPushFront(TempcatSnapshotPopBack());
	/* 2) get rid of all snapshots except the root one */
	tempcat_abort_transaction();
}

/*
 * Perform actions related to virtual catalog on transaction abort.
 *
 * NB: There could be in fact no transaction running.
 */
void
tempcat_abort_transaction(void)
{
	TempcatSnapshot tempcat_snapshot;

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_abort_transaction, transaction is in progress: %u (it's OK if this procedure is called from tempcat_end_transaction - see the code)",
		 TempcatTransactionInProgress());
#endif

	for (;;)
	{
		tempcat_snapshot = TempcatSnapshotPopBack();
		if (!tempcat_snapshot)	/* root snapshot reached */
			break;

		TempcatSnapshotFree(tempcat_snapshot);
	}

	Assert(!TempcatTransactionInProgress());
	TempcatDirtyFlag = true;
}

/*
 * Perform actions related to virtual catalog on savepoint creation.
 */
void
tempcat_define_savepoint(const char *name)
{
	Assert(TempcatTransactionInProgress());
	Assert(TempcatSnapshotIsAnonymous(TempcatSnapshotGetCurrent()));

	/*
	 * Value of `name` argument can be NULL in 'rollback to savepoint' case.
	 * This case is already handled by tempcat_rollback_to_savepoint.
	 */
	if (!PointerIsValid(name))
		return;

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_define_safepoint, name = '%s'", name);
#endif

	TempcatSnapshotCreate(name);	/* savepoint to rollback to */
	TempcatSnapshotCreate(NULL);	/* current snapshot to store changes */

	Assert(TempcatTransactionInProgress());
}

/*
 * Perform actions related to virtual catalog on `rollback to savepoint`.
 *
 * NB: There is no need to re-check case of savepoint name (upper / lower) or
 * that savepoint exists.
 */
void
tempcat_rollback_to_savepoint(const char *name)
{
	Assert(PointerIsValid(name));
	Assert(TempcatTransactionInProgress());
	Assert(TempcatSnapshotIsAnonymous(TempcatSnapshotGetCurrent()));

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_rollback_to_savepoint, name = '%s'", name);
#endif

	/*
	 * Pop snapshots from the stack and free them until a snapshot with given
	 * name will be reached.
	 */
	for (;;)
	{
		TempcatSnapshot tempcat_snapshot = TempcatSnapshotGetCurrent();

		Assert(!TempcatSnapshotIsRoot(tempcat_snapshot));

		if ((!TempcatSnapshotIsAnonymous(tempcat_snapshot)) &&
			(strcmp(tempcat_snapshot->name, name) == 0))
			break;

		TempcatSnapshotFree(TempcatSnapshotPopBack());
	}

	/* Create a new current snapshot to store changes. */
	TempcatSnapshotCreate(NULL);
	TempcatDirtyFlag = true;
}

static TempcatSnapshotRelationData* find_relation_entry(TempcatSnapshot snapshot, Relation rel) {
	dlist_iter iter;
	dlist_foreach(iter, &snapshot->relationData)
	{
		TempcatSnapshotRelationData *entry = dlist_container(TempcatSnapshotRelationData, node, iter.cur);
		if (entry->relid == rel->rd_rel->oid)
			return entry;
	}
	return NULL;
}

static TempcatSnapshotRelationData* find_or_create_relation_entry(TempcatSnapshot snapshot, Relation rel) {
	TempcatSnapshotRelationData *entry = find_relation_entry(snapshot, rel);
	if (entry == NULL)
	{
		entry = palloc(sizeof(TempcatSnapshotRelationData));
		entry->relid = rel->rd_rel->oid;
		dlist_init(&entry->tuples);
		entry->tuples_num = 0;
		dlist_push_tail(&snapshot->relationData, &entry->node);
	}
	return entry;
}

bool
tempcat_is_fetched(TempCatScanData* scan)
{
	return scan && scan->inmem_tuplist_init_done && !scan->scan_finish_returned;
}

/*
 * Insert a tuple. Basically heap_insert implementation for virtual tuples.
 * Returns true if tuple was inserted, false otherwise.
 */
void
tempcat_insert(Relation relation, HeapTuple htup)
{
	TempcatSnapshot tempcat_snapshot;
	MemoryContext oldctx;
	DListHeapTuple *dlist_tup;
	TempcatSnapshotRelationData *relation_entry;

	tempcat_snapshot = TempcatSnapshotGetCurrent();

	oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());
	htup->t_self = GenTempcatItemPointerData();
	dlist_tup = palloc(sizeof(DListHeapTuple));
	dlist_tup->tup = heap_copytuple(htup);

	relation_entry = find_or_create_relation_entry(tempcat_snapshot, relation);
	MemoryContextSwitchTo(oldctx);

	dlist_push_tail(&relation_entry->tuples,
					&dlist_tup->node);
	relation_entry->tuples_num++;

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_insert, dlist_tup->tup->t_self = %08X/%04X, oid = %d, inmemory tuples num = %d, heaptup oid = %d, relation relid = %d",
		 BlockIdGetBlockNumber(&dlist_tup->tup->t_self.ip_blkid),
		 dlist_tup->tup->t_self.ip_posid, HeapTupleGetOid(dlist_tup->tup),
		 relation_entry->tuples_num,
		 HeapTupleGetOid(htup), RelationGetRelid(relation)
		);
#endif

	CacheInvalidateHeapTuple(relation, dlist_tup->tup, NULL);
	pgstat_count_heap_insert(relation, 1);
	TempcatDirtyFlag = true;
}

/*
 * Delete tuple. Basically heap_delete implementation for virtual tuples.
 */
void
tempcat_delete(Relation relation, ItemPointer tid)
{
	TempcatSnapshot tempcat_snapshot;
	dlist_iter	iter;
	TempcatSnapshotRelationData *relation_entry;

	if (!IsTempcatItemPointer(tid))
		return;

	tempcat_snapshot = TempcatSnapshotGetCurrent();
	relation_entry = find_relation_entry(tempcat_snapshot, relation);
	if (relation_entry == NULL)
		return;
	
	dlist_foreach(iter, &relation_entry->tuples)
	{
		DListHeapTuple *dlist_tup = (DListHeapTuple *) iter.cur;

		if (ItemPointerEquals(&dlist_tup->tup->t_self, tid))
		{
			pgstat_count_heap_delete(relation);
			CacheInvalidateHeapTuple(relation, dlist_tup->tup, NULL);

			dlist_delete(&dlist_tup->node);
			DListHeapTupleFree(dlist_tup);
			relation_entry->tuples_num--;

#ifdef TEMPCAT_DEBUG
			elog(NOTICE, "TEMPCAT: tempcat_delete, tid = %08X/%04X - entry found and deleted, tuples_num = %d, rd_id = %d",
				 BlockIdGetBlockNumber(&tid->ip_blkid), tid->ip_posid,
				 relation_entry->tuples_num, relation->rd_id
				);
#endif

			TempcatDirtyFlag = true;
			return;
		}
	}

	elog(ERROR, "TEMPCAT: in-memory tuple not found during delete");
}

void
tempcat_update_inplace(Relation relation, HeapTuple htup)
{
	tempcat_update(relation, &htup->t_self, htup);
}

/*
 * Update tuple. Basically heap_update implementation for virtual tuples.
 * Returns true if tuple was updated, false otherwise.
 */
void
tempcat_update(Relation relation, ItemPointer otid, HeapTuple newtup)
{
	TempcatSnapshot tempcat_snapshot;
	dlist_iter	iter;
	TempcatSnapshotRelationData *relation_entry;

	if (!IsTempcatItemPointer(otid))
		return;

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_update, looking for otid = %08X/%04X",
		 BlockIdGetBlockNumber(&otid->ip_blkid), otid->ip_posid);
#endif

	tempcat_snapshot = TempcatSnapshotGetCurrent();
	
	relation_entry = find_relation_entry(tempcat_snapshot, relation);
	if (relation_entry == NULL)
		elog(ERROR, "TEMPCAT: relation entry not found during update");

	dlist_foreach(iter, &relation_entry->tuples)
	{
		DListHeapTuple *dlist_tup = (DListHeapTuple *) iter.cur;

		if (ItemPointerEquals(&dlist_tup->tup->t_self, otid))
		{
			MemoryContext oldctx = MemoryContextSwitchTo(GetLocalMemoryContext());

			CacheInvalidateHeapTuple(relation, dlist_tup->tup, newtup);
			heap_freetuple(dlist_tup->tup);
			newtup->t_self = GenTempcatItemPointerData();
			dlist_tup->tup = heap_copytuple(newtup);
			MemoryContextSwitchTo(oldctx);

			pgstat_count_heap_update(relation, false);

#ifdef TEMPCAT_DEBUG
			elog(NOTICE, "TEMPCAT: tempcat_update - entry found and updated, newtup->t_self = %08X/%04X, oid = %d, tuples_num = %d",
				 BlockIdGetBlockNumber(&newtup->t_self.ip_blkid), newtup->t_self.ip_posid,
				 HeapTupleGetOid(dlist_tup->tup),
				 relation_entry->tuples_num);
#endif
			TempcatDirtyFlag = true;
			return;
		}
	}

	elog(ERROR, "TEMPCAT: in-memory tuple not found during update");
}

/*
 * Determine whether a virtual heap tuple matches the WHERE condition
 * represented by the scan keys.
 *
 * Compares each scan key argument against the corresponding tuple attribute
 * using DirectFunctionCall2Coll and evaluates the B-tree strategy.
 *
 * Returns true if the tuple satisfies all scan key conditions.
 */
static bool
tempcat_index_tuple_matches_where_condition(TempCatScanData *scan, HeapTuple tup)
{
	for (int keyIndex = 0; keyIndex < scan->nkeys; keyIndex++)
	{
		bool	isnull;
		Datum	val;

		val = heap_getattr(tup, scan->attrNumbers[keyIndex], scan->tupdesc, &isnull);
		if (isnull)
			return false;

		/*
		 * Use the operator function directly (e.g. oideq, int4lt).  The
		 * ScanKey's sk_func already encodes the correct semantics for the
		 * strategy (=, <, <=, >, >=) and returns a boolean.
		 */
		if (!DatumGetBool(FunctionCall2Coll(
				&scan->attrOpFuncs[keyIndex],
				scan->attrCollations[keyIndex],
				val,
				scan->key[keyIndex].sk_argument)))
			return false;
	}

	return true;
}

/*
 * Compare two virtual heap tuples by index key order.
 *
 * Returns a value < 0 if first < second, 0 if equal, > 0 if first > second,
 * evaluated across all index key attributes.
 */
static int
tempcat_index_compare_tuples(TempCatScanData *scan, HeapTuple first, HeapTuple second)
{
	for (int keyIndex = 0; keyIndex < scan->nkeys; keyIndex++)
	{
		bool	isnull_first, isnull_second;
		Datum	val_first, val_second;
		int		cmp;

		val_first = heap_getattr(first, scan->attrNumbers[keyIndex], scan->tupdesc, &isnull_first);
		val_second = heap_getattr(second, scan->attrNumbers[keyIndex], scan->tupdesc, &isnull_second);

		/* NULLs sort to end */
		if (isnull_first && isnull_second)
			continue;
		if (isnull_first)
			return 1;
		if (isnull_second)
			return -1;

		cmp = DatumGetInt32(FunctionCall2Coll(
			&scan->attrCmpFuncs[keyIndex],
			scan->attrCollations[keyIndex],
			val_first,
			val_second));

		if (cmp != 0)
			return cmp;
	}

	return 0;
}

/*
 * Filter and insert a tuple into scan->inmem_tuplist in sorted order.
 *
 * Returns:
 * true - tuple added (matched WHERE condition)
 * false - tuple not added (filtered out)
 */
static bool
tempcat_index_insert_tuple_in_sorted_list(TempCatScanData *scan, HeapTuple tup)
{
	DListHeapTuple *dlist_tup;
	dlist_node *insert_after = &scan->inmem_tuplist.head;
	dlist_iter	iter;

	if (!tempcat_index_tuple_matches_where_condition(scan, tup))
		return false;

	/* Using regular transaction memory context here. */
	dlist_tup = palloc(sizeof(DListHeapTuple));
	dlist_tup->tup = heap_copytuple(tup);

	dlist_foreach(iter, &scan->inmem_tuplist)
	{
		DListHeapTuple *dlist_curr = (DListHeapTuple *) iter.cur;

		if (tempcat_index_compare_tuples(scan, dlist_curr->tup, tup) >= 0)
			break;

		insert_after = iter.cur;
	}

	dlist_insert_after(insert_after, &dlist_tup->node);

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_index_insert_tuple_in_sorted_list scan = %p, tup oid = %d, tuple added to list",
		 scan, HeapTupleGetOid(tup));
#endif

	return true;
}

/*
 * Initialize scan for virtual catalog. Basically index_beginscan
 * implementation for virtual tuples.
 */
TempCatScanData *
tempcat_beginscan(Relation relation, int nkeys, ScanKey key)
{
	MemoryContext	oldCtx;
	TempCatScanData *scan;
	TempcatSnapshot	tempcat_snapshot;
	TempcatSnapshotRelationData *relation_entry;

	/*
	 * Guard against recursion.  lookup_type_cache() below performs catalog
	 * lookups that may re-enter systable_beginscan → tempcat_beginscan.
	 * When that happens, return NULL so the recursive scan falls through
	 * to the normal on-disk path.  This follows the 1C patch approach.
	 */
	static bool nested = false;

	if (nested)
		return NULL;

	Assert(nkeys <= TempcatIndexMaxAttributes);

	tempcat_snapshot = TempcatSnapshotGetCurrent();
	relation_entry = find_relation_entry(tempcat_snapshot, relation);
	if (!relation_entry)
		return NULL;

	nested = true;

	oldCtx = MemoryContextSwitchTo(GetLocalMemoryContext());
	scan = palloc_object(TempCatScanData);
	MemoryContextSwitchTo(oldCtx);

	scan->rel = relation_entry;
	scan->tupdesc = RelationGetDescr(relation);
	scan->key = key;
	scan->nkeys = nkeys;
	for (int i = 0; i < nkeys; i++) {
		TypeCacheEntry *typeEntry;

		scan->attrNumbers[i] = key[i].sk_attno;
		scan->attrCollations[i] = key[i].sk_collation;

		/* Operator function from ScanKey (e.g. oideq, int4lt): returns bool.
		 * Used by tempcat_index_tuple_matches_where_condition. */
		scan->attrOpFuncs[i] = key[i].sk_func;

		/* Btree comparison function from type cache (e.g. btoidcmp):
		 * returns int32 three-way (<0, 0, >0).
		 * Used by tempcat_index_compare_tuples for sort ordering. */
		typeEntry = lookup_type_cache(
			scan->tupdesc->attrs[key[i].sk_attno - 1].atttypid,
			TYPECACHE_CMP_PROC_FINFO);
		Assert(OidIsValid(typeEntry->cmp_proc_finfo.fn_oid));
		scan->attrCmpFuncs[i] = typeEntry->cmp_proc_finfo;
	}

	nested = false;

	/*
	 * inmem_tuplist is initialized when tempcat_getnext is called first
	 * time. We are not doing it here because:
	 *
	 * 1) It's more efficient this way, since sometimes beginscan/rescan are
	 * called without any actual scanning
	 *
	 * 2) Sometimes scan keys are not fully initialized at beginscan time
	 *
	 * 3) We would like to filter tuples by WHERE condition ASAP, otherwise
	 * memory will be wasted on tuples that will be filtered anyway
	 */
	scan->scan_finish_returned = false;
	scan->inmem_tuplist_init_done = false;
	dlist_init(&scan->inmem_tuplist);

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_beginscan, scan = %p, nkeys = %d",
		 scan, nkeys);
#endif

	return scan;
}

/*
 * Free scan for virtual catalog. Basically index_endscan
 * implementation for virtual tuples.
 */
void
tempcat_endscan(TempCatScanData *scan)
{
	/* Free in-memory tuples left. */
	TempcatDListFree(&scan->inmem_tuplist);

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_endscan, scan = %p, relation relid = %u",
		 scan, scan->rel->relid);
#endif

	pfree(scan);
}

/*
 * Reinitialize scan for virtual catalog. Basically index_rescan
 * implementation for virtual tuples.
 */
void
tempcat_rescan(TempCatScanData *scan, ScanKey keys, int nkeys)
{
	TempcatDListFree(&scan->inmem_tuplist);
	if (keys)
		scan->key = keys;
	if (nkeys > 0)
		scan->nkeys = nkeys;
	scan->scan_finish_returned = false;
	scan->inmem_tuplist_init_done = false;
	dlist_init(&scan->inmem_tuplist);
}

/*
 * Make sure scan->inmem_tuplist is initialized.
 */
static void
tempcat_index_make_sure_inmem_tuplist_init_done(TempCatScanData *scan)
{
	dlist_iter	iter;

	/* initialize scan->inmem_tuplist during first call */
	if (scan->inmem_tuplist_init_done)
		return;

	dlist_foreach(iter, &scan->rel->tuples)
	{
		DListHeapTuple *dlist_curr = (DListHeapTuple *) iter.cur;

		(void) tempcat_index_insert_tuple_in_sorted_list(scan, dlist_curr->tup);
	}

	scan->inmem_tuplist_init_done = true;
}

/*
 * Get next virtual tuple from a scan.
 */
HeapTuple
tempcat_getnext(TempCatScanData *scan, BufferHeapTupleTableSlot *slot)
{
	DListHeapTuple *node;
	HeapTuple	tup;

	/* Initialize scan->inmem_tuplist during first call. */
	tempcat_index_make_sure_inmem_tuplist_init_done(scan);

	if (dlist_is_empty(&scan->inmem_tuplist))
	{
		scan->scan_finish_returned = true;
		return NULL;
	}

	node = (DListHeapTuple *) dlist_pop_head_node(&scan->inmem_tuplist);
	Assert(PointerIsValid(node));

	tup = node->tup;
	pfree(node);	/* free only the list node wrapper, not the tuple */

#ifdef TEMPCAT_DEBUG
	elog(NOTICE, "TEMPCAT: tempcat_getnext, scan = %p, return tuple tid = %08X/%04X",
		 scan,
		 BlockIdGetBlockNumber(&tup->t_self.ip_blkid),
		 tup->t_self.ip_posid);
#endif

	return tup;
}

bool
tempcat_is_dirty(void)
{
	return TempcatDirtyFlag;
}

void
tempcat_clear_dirty(void)
{
	TempcatDirtyFlag = false;
}

/*
 * Serialization format (pointer-free — t_data is serialized separately
 * from the HeapTupleData header since t_data is a pointer):
 *   int32   ntuples        -- number of virtual tuples that follow
 *   For each tuple:
 *     Oid     reloid       -- relation OID the tuple belongs to
 *     uint32  t_len        -- length of the tuple data (HeapTupleHeader)
 *     ItemPointerData t_self -- self pointer
 *     Oid     t_tableOid   -- table OID
 *     char    tupdata[t_len] -- raw HeapTupleHeader bytes
 */
void
tempcat_serialize(int *len, char **data)
{
	TempcatSnapshot snapshot;
	dlist_iter	rel_iter;
	StringInfoData buf;
	int32	ntuples = 0;

	snapshot = TempcatSnapshotGetCurrent();

	initStringInfo(&buf);

	appendBinaryStringInfo(&buf, (const char *) &ntuples, sizeof(int32));

	dlist_foreach(rel_iter, &snapshot->relationData)
	{
		TempcatSnapshotRelationData *rel_entry =
			dlist_container(TempcatSnapshotRelationData, node, rel_iter.cur);
		dlist_iter tup_iter;

		dlist_foreach(tup_iter, &rel_entry->tuples)
		{
			DListHeapTuple *dh = (DListHeapTuple *) tup_iter.cur;
			uint32	t_len = dh->tup->t_len;

			appendBinaryStringInfo(&buf, (const char *) &rel_entry->relid, sizeof(Oid));
			appendBinaryStringInfo(&buf, (const char *) &t_len, sizeof(uint32));
			appendBinaryStringInfo(&buf, (const char *) &dh->tup->t_self, sizeof(ItemPointerData));
			appendBinaryStringInfo(&buf, (const char *) &dh->tup->t_tableOid, sizeof(Oid));
			appendBinaryStringInfo(&buf, (const char *) dh->tup->t_data, t_len);
			ntuples++;
		}
	}

	/* Fill in ntuples at the beginning */
	memcpy(buf.data, &ntuples, sizeof(int32));

	*len = buf.len;
	*data = buf.data;
}

/*
 * Deserialize tempcat state from a buffer produced by tempcat_serialize.
 * The existing state is first cleared, then rebuilt by calling
 * tempcat_insert for each tuple.
 */
void
tempcat_deserialize(int len, const char *data)
{
	const char *end = data + len;
	const char *ptr = data;
	int32	ntuples;
	int32	i;

	/* Clear any existing state */
	while (TempcatSnapshotPopBack() != NULL)
		/* pop all non-root snapshots */;

	if (len < (int) sizeof(int32))
		return;

	memcpy(&ntuples, ptr, sizeof(int32));
	ptr += sizeof(int32);

	for (i = 0; i < ntuples; i++)
	{
		HeapTupleData htup;
		uint32	t_len;
		Oid		reloid;
		Relation	rel;

		if (ptr + sizeof(Oid) + sizeof(uint32) + sizeof(ItemPointerData) + sizeof(Oid) > end)
			break;

		memcpy(&reloid, ptr, sizeof(Oid));
		ptr += sizeof(Oid);
		memcpy(&t_len, ptr, sizeof(uint32));
		ptr += sizeof(uint32);

		/* Validate t_len before allocating */
		if (t_len > (uint32)(end - ptr))
			break;

		/* Reconstruct the HeapTupleData with a proper t_data pointer */
		htup.t_len = t_len;
		memcpy(&htup.t_self, ptr, sizeof(ItemPointerData));
		ptr += sizeof(ItemPointerData);
		memcpy(&htup.t_tableOid, ptr, sizeof(Oid));
		ptr += sizeof(Oid);
		htup.t_data = (HeapTupleHeader) palloc(t_len);
		memcpy((char *) htup.t_data, ptr, t_len);
		ptr += t_len;

		rel = relation_open(reloid, AccessShareLock);
		tempcat_insert(rel, &htup);
		relation_close(rel, AccessShareLock);

		pfree((void *) htup.t_data);
	}
}
