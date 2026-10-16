/*-------------------------------------------------------------------------
 *
 * indexam.c
 *	  general index access method routines
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/index/indexam.c
 *
 * INTERFACE ROUTINES
 *		index_open		- open an index relation by relation OID
 *		index_close		- close an index relation
 *		index_beginscan - start a scan of an index with amgettuple
 *		index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *		index_rescan	- restart a scan of an index
 *		index_endscan	- end a scan
 *		index_insert	- insert an index tuple into a relation
 *		index_markpos	- mark a scan position
 *		index_restrpos	- restore a scan position
 *		index_parallelscan_estimate - estimate shared memory for parallel scan
 *		index_parallelscan_initialize - initialize parallel scan
 *		index_parallelrescan  - (re)start a parallel scan of an index
 *		index_beginscan_parallel - join parallel index scan
 *		index_getnext_tid	- get the next TID from a scan
 *		index_fetch_heap		- get the scan's next heap tuple
 *		index_getnext_slot	- get the next tuple from a scan
 *		index_getbitmap - get all tuples from a scan
 *      index_initbitmap - get an empty bitmap
 *		index_bulk_delete	- bulk deletion of index tuples
 *		index_vacuum_cleanup	- post-deletion cleanup of an index
 *		index_can_return	- does index support index-only scans?
 *		index_getprocid - get a support procedure OID
 *		index_getprocinfo - get a support procedure's lookup info
 *
 * NOTES
 *		This file contains the index_ routines which used
 *		to be a scattered collection of stuff in access/genam.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/bitmap_private.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "catalog/tempcat.h"
#include "executor/tuptable.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/fmgroids.h"

/* GPDB_17_MERGE_FIXE: Remove, once we have aminsertcleanup() */
#include "access/brin_internal.h"

/* ----------------------------------------------------------------
 *					macros used in index_ routines
 *
 * Note: the ReindexIsProcessingIndex() check in RELATION_CHECKS is there
 * to check that we don't try to scan or do retail insertions into an index
 * that is currently being rebuilt or pending rebuild.  This helps to catch
 * things that don't work when reindexing system catalogs, as well as prevent
 * user errors like index expressions that access their own tables.  The check
 * doesn't prevent the actual rebuild because we don't use RELATION_CHECKS
 * when calling the index AM's ambuild routine, and there is no reason for
 * ambuild to call its subsidiary routines through this file.
 * ----------------------------------------------------------------
 */
#define RELATION_CHECKS \
do { \
	Assert(RelationIsValid(indexRelation)); \
	Assert(PointerIsValid(indexRelation->rd_indam)); \
	if (unlikely(ReindexIsProcessingIndex(RelationGetRelid(indexRelation)))) \
		ereport(ERROR, \
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
				 errmsg("cannot access index \"%s\" while it is being reindexed", \
						RelationGetRelationName(indexRelation)))); \
} while(0)

#define SCAN_CHECKS \
( \
	AssertMacro(IndexScanIsValid(scan)), \
	AssertMacro(RelationIsValid(scan->indexRelation)), \
	AssertMacro(PointerIsValid(scan->indexRelation->rd_indam)) \
)

#define CHECK_REL_PROCEDURE(pname) \
do { \
	if (indexRelation->rd_indam->pname == NULL) \
		elog(ERROR, "function %s is not defined for index %s", \
			 CppAsString(pname), RelationGetRelationName(indexRelation)); \
} while(0)

#define CHECK_SCAN_PROCEDURE(pname) \
do { \
	if (scan->indexRelation->rd_indam->pname == NULL) \
		elog(ERROR, "function %s is not defined for index %s", \
			 CppAsString(pname), RelationGetRelationName(scan->indexRelation)); \
} while(0)

static IndexScanDesc index_beginscan_internal(Relation indexRelation,
											  int nkeys, int norderbys, Snapshot snapshot,
											  ParallelIndexScanDesc pscan, bool temp_snap);

/*
 * GPDB: the catalog rows of this session's temporary objects kept in memory
 * (see tempcat.c), to merge into a scan of a catalog index in index order.
 * The rows in memory are looked for at the first fetch after a (re)scan.
 * Between fetches, the next on-disk row (of an index scan) or index entry
 * (of an index-only scan), if it comes after the next row in memory, waits:
 * the row in 'stash', the entry in the index AM, which is not asked for the
 * next one meanwhile.
 */
typedef struct IndexTempcatMerge
{
	TempcatScan tempscan;
	TupleTableSlot *stash;
	bool		stashed;		/* an on-disk row or entry waits */
	bool		disk_done;		/* the index has no more entries */
	ItemPointerData stash_tid;
	bool		stash_recheck;
	IndexTuple	stash_itup;
	struct TupleDescData *stash_itupdesc;
	HeapTuple	hitup;			/* index columns of the last row in memory */

	/* the above as of index_markpos(), see index_tempcat_markpos() */
	bool		mark_valid;
	int			mark_next;
	bool		mark_stashed;
	bool		mark_disk_done;
	ItemPointerData mark_tid;
	bool		mark_recheck;
	TupleTableSlot *mark_slot;
	IndexTuple	mark_itup;
	IndexTuple	restored_itup;	/* copy of mark_itup in stash_itup */
} IndexTempcatMerge;

static void index_tempcat_begin(IndexScanDesc scan);
static void index_tempcat_end(IndexScanDesc scan);
static void index_tempcat_markpos(IndexScanDesc scan);
static void index_tempcat_restrpos(IndexScanDesc scan);
static ItemPointer index_getnext_tid_disk(IndexScanDesc scan,
										  ScanDirection direction);
static ItemPointer index_getnext_tid_tempcat(IndexScanDesc scan,
											 ScanDirection direction);
static bool index_getnext_slot_disk(IndexScanDesc scan,
									ScanDirection direction,
									TupleTableSlot *slot);


/* ----------------------------------------------------------------
 *				   index_ interface functions
 * ----------------------------------------------------------------
 */

/* ----------------
 *		index_open - open an index relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the index.  (Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		index already.)
 *
 *		An error is raised if the index does not exist.
 *
 *		This is a convenience routine adapted for indexscan use.
 *		Some callers may prefer to use relation_open directly.
 * ----------------
 */
Relation
index_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_open(relationId, lockmode);

	if (r->rd_rel->relkind != RELKIND_INDEX &&
		r->rd_rel->relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		index_close - close an index relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond index_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void
index_close(Relation relation, LOCKMODE lockmode)
{
	LockRelId	relid = relation->rd_lockInfo.lockRelId;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* The relcache does the real work... */
	RelationClose(relation);

	if (lockmode != NoLock)
		UnlockRelationId(&relid, lockmode);
}

/* ----------------
 *		index_insert - insert an index tuple into a relation
 * ----------------
 */
bool
index_insert(Relation indexRelation,
			 Datum *values,
			 bool *isnull,
			 ItemPointer heap_t_ctid,
			 Relation heapRelation,
			 IndexUniqueCheck checkUnique,
			 IndexInfo *indexInfo)
{
	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(aminsert);

	if (!(indexRelation->rd_indam->ampredlocks))
		CheckForSerializableConflictIn(indexRelation,
									   (HeapTuple) NULL,
									   InvalidBuffer);

	return indexRelation->rd_indam->aminsert(indexRelation, values, isnull,
											 heap_t_ctid, heapRelation,
											 checkUnique, indexInfo);
}

/* -------------------------
 *		index_insert_cleanup - clean up after all index inserts are done
 * -------------------------
 */
void
index_insert_cleanup(Relation indexRelation,
					 IndexInfo *indexInfo)
{
	RELATION_CHECKS;
	Assert(indexInfo);

	/*
	 * GPDB_17_MERGE_FIXME: Call aminsertcleanup callback here, when it becomes
	 * available. For now, assume that BRIN is the only index type for which we
	 * clean up at the end of an insert session.
	 */
	if (indexRelation->rd_rel->relam == BRIN_AM_OID && indexInfo->ii_AmCache)
		brininsertcleanup(indexInfo);
#if 0
	if (indexRelation->rd_indam->aminsertcleanup && indexInfo->ii_AmCache)
		indexRelation->rd_indam->aminsertcleanup(indexInfo);
#endif
}

/*
 * index_beginscan - start a scan of an index with amgettuple
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
IndexScanDesc
index_beginscan(Relation heapRelation,
				Relation indexRelation,
				Snapshot snapshot,
				int nkeys, int norderbys)
{
	IndexScanDesc scan;

	scan = index_beginscan_internal(indexRelation, nkeys, norderbys, snapshot, NULL, false);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->heapRelation = heapRelation;
	scan->xs_snapshot = snapshot;

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heapRelation);

	return scan;
}

/*
 * index_beginscan_bitmap - start a scan of an index with amgetbitmap
 *
 * As above, caller had better be holding some lock on the parent heap
 * relation, even though it's not explicitly mentioned here.
 */
IndexScanDesc
index_beginscan_bitmap(Relation indexRelation,
					   Snapshot snapshot,
					   int nkeys)
{
	IndexScanDesc scan;

	scan = index_beginscan_internal(indexRelation, nkeys, 0, snapshot, NULL, false);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by RelationGetIndexScan.
	 */
	scan->xs_snapshot = snapshot;

	return scan;
}

/*
 * index_beginscan_internal --- common code for index_beginscan variants
 */
static IndexScanDesc
index_beginscan_internal(Relation indexRelation,
						 int nkeys, int norderbys, Snapshot snapshot,
						 ParallelIndexScanDesc pscan, bool temp_snap)
{
	IndexScanDesc scan;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(ambeginscan);

	if (!(indexRelation->rd_indam->ampredlocks))
		PredicateLockRelation(indexRelation, snapshot);

	/*
	 * We hold a reference count to the relcache entry throughout the scan.
	 */
	RelationIncrementReferenceCount(indexRelation);

	/*
	 * Tell the AM to open a scan.
	 */
	scan = indexRelation->rd_indam->ambeginscan(indexRelation, nkeys,
												norderbys);
	/* Initialize information for parallel scan. */
	scan->parallel_scan = pscan;
	scan->xs_temp_snap = temp_snap;

	return scan;
}

/* ----------------
 *		index_rescan  - (re)start a scan of an index
 *
 * During a restart, the caller may specify a new set of scankeys and/or
 * orderbykeys; but the number of keys cannot differ from what index_beginscan
 * was told.  (Later we might relax that to "must not exceed", but currently
 * the index AMs tend to assume that scan->numberOfKeys is what to believe.)
 * To restart the scan without changing keys, pass NULL for the key arrays.
 * (Of course, keys *must* be passed on the first call, unless
 * scan->numberOfKeys is zero.)
 * ----------------
 */
void
index_rescan(IndexScanDesc scan,
			 ScanKey keys, int nkeys,
			 ScanKey orderbys, int norderbys)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amrescan);

	Assert(nkeys == scan->numberOfKeys);
	Assert(norderbys == scan->numberOfOrderBys);

	/* Release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	scan->kill_prior_tuple = false; /* for safety */
	scan->xs_heap_continue = false;

	scan->indexRelation->rd_indam->amrescan(scan, keys, nkeys,
											orderbys, norderbys);

	/* GPDB: look for the catalog rows in memory anew, see above */
	index_tempcat_end(scan);
	scan->xs_tempcat_pending = !scan->xs_tempcat_off &&
		scan->heapRelation != NULL &&
		scan->numberOfOrderBys == 0 &&
		scan->parallel_scan == NULL &&
		IsCatalogRelation(scan->heapRelation);
}

/* ----------------
 *		index_endscan - end a scan
 * ----------------
 */
void
index_endscan(IndexScanDesc scan)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amendscan);

	/* Release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
	{
		table_index_fetch_end(scan->xs_heapfetch);
		scan->xs_heapfetch = NULL;
	}

	/* GPDB: and the scan of catalog rows in memory */
	index_tempcat_end(scan);

	/* End the AM's scan */
	scan->indexRelation->rd_indam->amendscan(scan);

	/* Release index refcount acquired by index_beginscan */
	RelationDecrementReferenceCount(scan->indexRelation);

	if (scan->xs_temp_snap)
		UnregisterSnapshot(scan->xs_snapshot);

	/* Release the scan data structure itself */
	IndexScanEnd(scan);
}

/* ----------------
 *		index_markpos  - mark a scan position
 * ----------------
 */
void
index_markpos(IndexScanDesc scan)
{
	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(ammarkpos);

	scan->indexRelation->rd_indam->ammarkpos(scan);

	/* GPDB: and the position in the catalog rows in memory */
	if (unlikely(scan->xs_tempcat_pending))
		index_tempcat_begin(scan);
	if (unlikely(scan->xs_tempcat != NULL))
		index_tempcat_markpos(scan);
}

/* ----------------
 *		index_restrpos	- restore a scan position
 *
 * NOTE: this only restores the internal scan state of the index AM.  See
 * comments for ExecRestrPos().
 *
 * NOTE: For heap, in the presence of HOT chains, mark/restore only works
 * correctly if the scan's snapshot is MVCC-safe; that ensures that there's at
 * most one returnable tuple in each HOT chain, and so restoring the prior
 * state at the granularity of the index AM is sufficient.  Since the only
 * current user of mark/restore functionality is nodeMergejoin.c, this
 * effectively means that merge-join plans only work for MVCC snapshots.  This
 * could be fixed if necessary, but for now it seems unimportant.
 * ----------------
 */
void
index_restrpos(IndexScanDesc scan)
{
	Assert(IsMVCCSnapshot(scan->xs_snapshot));

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amrestrpos);

	/* release resources (like buffer pins) from table accesses */
	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	scan->kill_prior_tuple = false; /* for safety */
	scan->xs_heap_continue = false;

	scan->indexRelation->rd_indam->amrestrpos(scan);

	/* GPDB: and the position in the catalog rows in memory */
	if (unlikely(scan->xs_tempcat != NULL))
		index_tempcat_restrpos(scan);
}

/*
 * index_parallelscan_estimate - estimate shared memory for parallel scan
 *
 * Currently, we don't pass any information to the AM-specific estimator,
 * so it can probably only return a constant.  In the future, we might need
 * to pass more information.
 */
Size
index_parallelscan_estimate(Relation indexRelation, Snapshot snapshot)
{
	Size		nbytes;

	RELATION_CHECKS;

	nbytes = offsetof(ParallelIndexScanDescData, ps_snapshot_data);
	nbytes = add_size(nbytes, EstimateSnapshotSpace(snapshot));
	nbytes = MAXALIGN(nbytes);

	/*
	 * If amestimateparallelscan is not provided, assume there is no
	 * AM-specific data needed.  (It's hard to believe that could work, but
	 * it's easy enough to cater to it here.)
	 */
	if (indexRelation->rd_indam->amestimateparallelscan != NULL)
		nbytes = add_size(nbytes,
						  indexRelation->rd_indam->amestimateparallelscan());

	return nbytes;
}

/*
 * index_parallelscan_initialize - initialize parallel scan
 *
 * We initialize both the ParallelIndexScanDesc proper and the AM-specific
 * information which follows it.
 *
 * This function calls access method specific initialization routine to
 * initialize am specific information.  Call this just once in the leader
 * process; then, individual workers attach via index_beginscan_parallel.
 */
void
index_parallelscan_initialize(Relation heapRelation, Relation indexRelation,
							  Snapshot snapshot, ParallelIndexScanDesc target)
{
	Size		offset;

	RELATION_CHECKS;

	offset = add_size(offsetof(ParallelIndexScanDescData, ps_snapshot_data),
					  EstimateSnapshotSpace(snapshot));
	offset = MAXALIGN(offset);

	target->ps_relid = RelationGetRelid(heapRelation);
	target->ps_indexid = RelationGetRelid(indexRelation);
	target->ps_offset = offset;
	SerializeSnapshot(snapshot, target->ps_snapshot_data);

	/* aminitparallelscan is optional; assume no-op if not provided by AM */
	if (indexRelation->rd_indam->aminitparallelscan != NULL)
	{
		void	   *amtarget;

		amtarget = OffsetToPointer(target, offset);
		indexRelation->rd_indam->aminitparallelscan(amtarget);
	}
}

/* ----------------
 *		index_parallelrescan  - (re)start a parallel scan of an index
 * ----------------
 */
void
index_parallelrescan(IndexScanDesc scan)
{
	SCAN_CHECKS;

	if (scan->xs_heapfetch)
		table_index_fetch_reset(scan->xs_heapfetch);

	/* amparallelrescan is optional; assume no-op if not provided by AM */
	if (scan->indexRelation->rd_indam->amparallelrescan != NULL)
		scan->indexRelation->rd_indam->amparallelrescan(scan);
}

/*
 * index_beginscan_parallel - join parallel index scan
 *
 * Caller must be holding suitable locks on the heap and the index.
 */
IndexScanDesc
index_beginscan_parallel(Relation heaprel, Relation indexrel, int nkeys,
						 int norderbys, ParallelIndexScanDesc pscan)
{
	Snapshot	snapshot;
	IndexScanDesc scan;

	Assert(RelationGetRelid(heaprel) == pscan->ps_relid);
	snapshot = RestoreSnapshot(pscan->ps_snapshot_data);
	RegisterSnapshot(snapshot);
	scan = index_beginscan_internal(indexrel, nkeys, norderbys, snapshot,
									pscan, true);

	/*
	 * Save additional parameters into the scandesc.  Everything else was set
	 * up by index_beginscan_internal.
	 */
	scan->heapRelation = heaprel;
	scan->xs_snapshot = snapshot;

	/* prepare to fetch index matches from table */
	scan->xs_heapfetch = table_index_fetch_begin(heaprel);

	return scan;
}

/* ----------------
 * index_getnext_tid - get the next TID from a scan
 *
 * The result is the next TID satisfying the scan keys,
 * or NULL if no more matching tuples exist.
 * ----------------
 */
ItemPointer
index_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	/* GPDB: index-only scans merge in the catalog rows in memory, see above */
	if (unlikely(scan->xs_tempcat_pending) && scan->xs_want_itup)
		index_tempcat_begin(scan);
	if (unlikely(scan->xs_tempcat != NULL) && scan->xs_want_itup)
		return index_getnext_tid_tempcat(scan, direction);

	return index_getnext_tid_disk(scan, direction);
}

/* The upstream index_getnext_tid(): the index entries only */
static ItemPointer
index_getnext_tid_disk(IndexScanDesc scan, ScanDirection direction)
{
	bool		found;

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgettuple);

	Assert(TransactionIdIsValid(RecentGlobalXmin));

	/*
	 * The AM's amgettuple proc finds the next index entry matching the scan
	 * keys, and puts the TID into scan->xs_heaptid.  It should also set
	 * scan->xs_recheck and possibly scan->xs_itup/scan->xs_hitup, though we
	 * pay no attention to those fields here.
	 */
	found = scan->indexRelation->rd_indam->amgettuple(scan, direction);

	/* Reset kill flag immediately for safety */
	scan->kill_prior_tuple = false;
	scan->xs_heap_continue = false;

	/* If we're out of index entries, we're done */
	if (!found)
	{
		/* release resources (like buffer pins) from table accesses */
		if (scan->xs_heapfetch)
			table_index_fetch_reset(scan->xs_heapfetch);

		return NULL;
	}
	Assert(ItemPointerIsValid(&scan->xs_heaptid));

	pgstat_count_index_tuples(scan->indexRelation, 1);

	/* Return the TID of the tuple we found. */
	return &scan->xs_heaptid;
}

/* ----------------
 *		index_fetch_heap - get the scan's next heap tuple
 *
 * The result is a visible heap tuple associated with the index TID most
 * recently fetched by index_getnext_tid, or NULL if no more matching tuples
 * exist.  (There can be more than one matching tuple because of HOT chains,
 * although when using an MVCC snapshot it should be impossible for more than
 * one such tuple to exist.)
 *
 * On success, the buffer containing the heap tup is pinned (the pin will be
 * dropped in a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
bool
index_fetch_heap(IndexScanDesc scan, TupleTableSlot *slot)
{
	bool		all_dead = false;
	bool		found;

	found = table_index_fetch_tuple(scan->xs_heapfetch, &scan->xs_heaptid,
									scan->xs_snapshot, slot,
									&scan->xs_heap_continue, &all_dead);

	if (found)
		pgstat_count_heap_fetch(scan->indexRelation);

	/*
	 * If we scanned a whole HOT chain and found only dead tuples, tell index
	 * AM to kill its entry for that TID (this will take effect in the next
	 * amgettuple call, in index_getnext_tid).  We do not do this when in
	 * recovery because it may violate MVCC to do so.  See comments in
	 * RelationGetIndexScan().
	 */
	if (!scan->xactStartedInRecovery)
		scan->kill_prior_tuple = all_dead;

	return found;
}

/* ----------------
 *		index_getnext_slot - get the next tuple from a scan
 *
 * The result is true if a tuple satisfying the scan keys and the snapshot was
 * found, false otherwise.  The tuple is stored in the specified slot.
 *
 * On success, resources (like buffer pins) are likely to be held, and will be
 * dropped by a future index_getnext_tid, index_fetch_heap or index_endscan
 * call).
 *
 * Note: caller must check scan->xs_recheck, and perform rechecking of the
 * scan keys if required.  We do not do that here because we don't have
 * enough information to do it efficiently in the general case.
 * ----------------
 */
bool
index_getnext_slot(IndexScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	IndexTempcatMerge *m;
	TupleTableSlot *disk = NULL;
	bool		from_stash = false;
	HeapTuple	vtup;

	/* GPDB: merge in the catalog rows in memory, see above */
	if (unlikely(scan->xs_tempcat_pending))
		index_tempcat_begin(scan);
	if (likely(scan->xs_tempcat == NULL))
		return index_getnext_slot_disk(scan, direction, slot);

	m = scan->xs_tempcat;
	/* (none if the merge began for an index-only scan) */
	if (m->stash == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(m));

		m->stash = table_slot_create(scan->heapRelation, NULL);
		MemoryContextSwitchTo(oldcxt);
	}
	if (m->stashed)
	{
		disk = m->stash;
		from_stash = true;
	}
	else if (!m->disk_done)
	{
		if (index_getnext_slot_disk(scan, direction, slot))
			disk = slot;
		else
			m->disk_done = true;
	}

	vtup = tempcat_merge_next(m->tempscan, !ScanDirectionIsBackward(direction),
							  disk ? ExecFetchSlotHeapTuple(disk, false, NULL) : NULL);
	if (vtup != NULL)
	{
		if (disk == slot)
		{
			ExecCopySlot(m->stash, slot);
			m->stashed = true;
			m->stash_tid = scan->xs_heaptid;
			m->stash_recheck = scan->xs_recheck;
		}
		ExecForceStoreHeapTuple(vtup, slot, false);
		slot->tts_tid = vtup->t_self;
		slot->tts_tableOid = RelationGetRelid(scan->heapRelation);
		scan->xs_heaptid = vtup->t_self;
		/* the keys of the index scan matched loosely, see tempcat.c */
		scan->xs_recheck = true;
		return true;
	}

	if (from_stash)
	{
		ExecCopySlot(slot, m->stash);
		ExecClearTuple(m->stash);
		m->stashed = false;
		slot->tts_tid = m->stash_tid;
		slot->tts_tableOid = RelationGetRelid(scan->heapRelation);
		scan->xs_heaptid = m->stash_tid;
		scan->xs_recheck = m->stash_recheck;
	}
	return disk != NULL;
}

/* The upstream index_getnext_slot(): the on-disk rows only */
static bool
index_getnext_slot_disk(IndexScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	for (;;)
	{
		if (!scan->xs_heap_continue)
		{
			ItemPointer tid;

			/* Time to fetch the next TID from the index */
			tid = index_getnext_tid_disk(scan, direction);

			/* If we're out of index entries, we're done */
			if (tid == NULL)
				break;

			Assert(ItemPointerEquals(tid, &scan->xs_heaptid));
		}

		/*
		 * Fetch the next (or only) visible heap tuple for this index entry.
		 * If we don't find anything, loop around and grab the next TID from
		 * the index.
		 */
		Assert(ItemPointerIsValid(&scan->xs_heaptid));
		if (index_fetch_heap(scan, slot))
			return true;
	}

	return false;
}

/* GPDB: look for the catalog rows in memory to merge in, see above */
static void
index_tempcat_begin(IndexScanDesc scan)
{
	TempcatScan tempscan;
	MemoryContext oldcxt;
	IndexTempcatMerge *m;

	scan->xs_tempcat_pending = false;

	/* (in the scan's context, which the fetches need not run in) */
	oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(scan));
	tempscan = tempcat_begin_index_scan(scan->heapRelation,
										scan->indexRelation,
										scan->xs_snapshot,
										scan->numberOfKeys,
										scan->keyData);
	if (tempscan == NULL)
	{
		MemoryContextSwitchTo(oldcxt);
		return;
	}

	m = palloc0(sizeof(IndexTempcatMerge));
	m->tempscan = tempscan;
	if (!scan->xs_want_itup)
		m->stash = table_slot_create(scan->heapRelation, NULL);
	scan->xs_tempcat = m;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * GPDB: index_getnext_tid() of an index-only scan merging in the catalog
 * rows in memory.  These come with their index columns in xs_hitup, and
 * with xs_recheck, as the keys matched them loosely; their TIDs are
 * IsTempcatTid(), with nothing to fetch from the heap.
 */
static ItemPointer
index_getnext_tid_tempcat(IndexScanDesc scan, ScanDirection direction)
{
	IndexTempcatMerge *m = scan->xs_tempcat;
	bool		have_disk = m->stashed;
	HeapTuple	vtup;

	if (m->hitup != NULL)
	{
		heap_freetuple(m->hitup);
		m->hitup = NULL;
		scan->xs_hitup = NULL;
	}

	if (!have_disk && !m->disk_done)
	{
		if (index_getnext_tid_disk(scan, direction) != NULL)
		{
			have_disk = true;
			m->stash_tid = scan->xs_heaptid;
			m->stash_recheck = scan->xs_recheck;
			m->stash_itup = scan->xs_itup;
			m->stash_itupdesc = scan->xs_itupdesc;
			if (m->stash_itup == NULL)
				elog(ERROR, "index-only scan of a catalog returned no index tuple");
		}
		else
			m->disk_done = true;
	}

	vtup = tempcat_merge_next_itup(m->tempscan,
								   !ScanDirectionIsBackward(direction),
								   have_disk ? m->stash_itup : NULL,
								   m->stash_itupdesc);
	if (vtup != NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(m));

		m->stashed = have_disk;
		m->hitup = tempcat_index_columns(m->tempscan, vtup,
										 &scan->xs_hitupdesc);
		MemoryContextSwitchTo(oldcxt);
		scan->xs_hitup = m->hitup;
		scan->xs_heaptid = vtup->t_self;
		scan->xs_recheck = true;
		return &scan->xs_heaptid;
	}

	if (!have_disk)
		return NULL;
	m->stashed = false;
	scan->xs_heaptid = m->stash_tid;
	scan->xs_recheck = m->stash_recheck;
	scan->xs_itup = m->stash_itup;
	scan->xs_itupdesc = m->stash_itupdesc;
	return &scan->xs_heaptid;
}

/*
 * GPDB: the index AM marks the position of the last entry it returned,
 * which may be the on-disk row or entry still waiting to be returned after
 * rows in memory: mark that too, and how far the rows in memory got.
 */
static void
index_tempcat_markpos(IndexScanDesc scan)
{
	IndexTempcatMerge *m = scan->xs_tempcat;
	MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(m));

	m->mark_valid = true;
	m->mark_next = tempcat_scan_getpos(m->tempscan);
	m->mark_stashed = m->stashed;
	m->mark_disk_done = m->disk_done;
	if (m->stashed)
	{
		m->mark_tid = m->stash_tid;
		m->mark_recheck = m->stash_recheck;
		if (m->stash != NULL)
		{
			if (m->mark_slot == NULL)
				m->mark_slot = table_slot_create(scan->heapRelation, NULL);
			ExecCopySlot(m->mark_slot, m->stash);
		}
		else
		{
			if (m->mark_itup != NULL)
				pfree(m->mark_itup);
			m->mark_itup = CopyIndexTuple(m->stash_itup);
		}
	}
	MemoryContextSwitchTo(oldcxt);
}

static void
index_tempcat_restrpos(IndexScanDesc scan)
{
	IndexTempcatMerge *m = scan->xs_tempcat;

	if (!m->mark_valid)
		elog(ERROR, "no marked position in the catalog rows in memory");

	tempcat_scan_setpos(m->tempscan, m->mark_next);
	m->stashed = m->mark_stashed;
	m->disk_done = m->mark_disk_done;
	if (m->stash != NULL)
		ExecClearTuple(m->stash);
	if (m->stashed)
	{
		m->stash_tid = m->mark_tid;
		m->stash_recheck = m->mark_recheck;
		if (m->stash != NULL)
			ExecCopySlot(m->stash, m->mark_slot);
		else
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(GetMemoryChunkContext(m));

			if (m->restored_itup != NULL)
				pfree(m->restored_itup);
			m->restored_itup = CopyIndexTuple(m->mark_itup);
			m->stash_itup = m->restored_itup;
			m->stash_itupdesc = RelationGetDescr(scan->indexRelation);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

/* GPDB: end the scan of catalog rows in memory, if any */
static void
index_tempcat_end(IndexScanDesc scan)
{
	IndexTempcatMerge *m = scan->xs_tempcat;

	scan->xs_tempcat_pending = false;
	if (m == NULL)
		return;
	scan->xs_tempcat = NULL;
	tempcat_endscan(m->tempscan);
	if (m->stash != NULL)
		ExecDropSingleTupleTableSlot(m->stash);
	if (m->mark_slot != NULL)
		ExecDropSingleTupleTableSlot(m->mark_slot);
	if (m->mark_itup != NULL)
		pfree(m->mark_itup);
	if (m->restored_itup != NULL)
		pfree(m->restored_itup);
	if (m->hitup != NULL)
	{
		heap_freetuple(m->hitup);
		scan->xs_hitup = NULL;
	}
	pfree(m);
}

/*
 * index_initbitmap -- get an empty bitmap
 * */
void
index_initbitmap(IndexScanDesc scan, Node **bitmapP)
{
    Relation relation = scan->indexRelation;

    if (relation->rd_amhandler == F_BMHANDLER)
    {
        bminitbitmap(bitmapP);
    }
    else if (relation->rd_amhandler == F_BTHANDLER)
    {
        *bitmapP = (Node *)tbm_create(work_mem * 1024L, NULL);
    }
    else
    {
        elog(ERROR, "Not support rd_amhandler %u to initbitmap under bitmapscan",
            relation->rd_amhandler);
    }

    return;
}

/* ----------------
 *		index_getbitmap - get all tuples at once from an index scan
 *
 *		it invokes am's getmulti function to get a bitmap. If am is an on-disk
 *		bitmap index access method (see bitmap.h), then a StreamBitmap is
 *		returned; a TIDBitmap otherwise. Note that an index am's getmulti
 *		function can assume that the bitmap that it's given as argument is of
 *		the same type as what the function constructs itself.
 *
 *  	GPDB: Since GPDB also support StreamBitmap node in bitmap index.
 *  	So normally we need to create specific bitmap node in the amgetbitmap AM.
 *  	This makes the function different from upstream.
 * ----------------
 */
int64
index_getbitmap(IndexScanDesc scan, Node **bitmapP)
{
	int64		ntids;

	SCAN_CHECKS;
	CHECK_SCAN_PROCEDURE(amgetbitmap);

	/* just make sure this is false... */
	scan->kill_prior_tuple = false;

	/*
	 * have the am's getbitmap proc do all the work.
	 */
	ntids = scan->indexRelation->rd_indam->amgetbitmap(scan, bitmapP);

	pgstat_count_index_tuples(scan->indexRelation, ntids);

	return ntids;
}

/* ----------------
 *		index_bulk_delete - do mass deletion of index entries
 *
 *		callback routine tells whether a given main-heap tuple is
 *		to be deleted
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_bulk_delete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback,
				  void *callback_state)
{
	Relation	indexRelation = info->index;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(ambulkdelete);

	return indexRelation->rd_indam->ambulkdelete(info, stats,
												 callback, callback_state);
}

/* ----------------
 *		index_vacuum_cleanup - do post-deletion cleanup of an index
 *
 *		return value is an optional palloc'd struct of statistics
 * ----------------
 */
IndexBulkDeleteResult *
index_vacuum_cleanup(IndexVacuumInfo *info,
					 IndexBulkDeleteResult *stats)
{
	Relation	indexRelation = info->index;

	RELATION_CHECKS;
	CHECK_REL_PROCEDURE(amvacuumcleanup);

	return indexRelation->rd_indam->amvacuumcleanup(info, stats);
}

/* ----------------
 *		index_can_return
 *
 *		Does the index access method support index-only scans for the given
 *		column?
 * ----------------
 */
bool
index_can_return(Relation indexRelation, int attno)
{
	RELATION_CHECKS;

	/* amcanreturn is optional; assume false if not provided by AM */
	if (indexRelation->rd_indam->amcanreturn == NULL)
		return false;

	return indexRelation->rd_indam->amcanreturn(indexRelation, attno);
}

/* ----------------
 *		index_getprocid
 *
 *		Index access methods typically require support routines that are
 *		not directly the implementation of any WHERE-clause query operator
 *		and so cannot be kept in pg_amop.  Instead, such routines are kept
 *		in pg_amproc.  These registered procedure OIDs are assigned numbers
 *		according to a convention established by the access method.
 *		The general index code doesn't know anything about the routines
 *		involved; it just builds an ordered list of them for
 *		each attribute on which an index is defined.
 *
 *		As of Postgres 8.3, support routines within an operator family
 *		are further subdivided by the "left type" and "right type" of the
 *		query operator(s) that they support.  The "default" functions for a
 *		particular indexed attribute are those with both types equal to
 *		the index opclass' opcintype (note that this is subtly different
 *		from the indexed attribute's own type: it may be a binary-compatible
 *		type instead).  Only the default functions are stored in relcache
 *		entries --- access methods can use the syscache to look up non-default
 *		functions.
 *
 *		This routine returns the requested default procedure OID for a
 *		particular indexed attribute.
 * ----------------
 */
RegProcedure
index_getprocid(Relation irel,
				AttrNumber attnum,
				uint16 procnum)
{
	RegProcedure *loc;
	int			nproc;
	int			procindex;

	nproc = irel->rd_indam->amsupport;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	loc = irel->rd_support;

	Assert(loc != NULL);

	return loc[procindex];
}

/* ----------------
 *		index_getprocinfo
 *
 *		This routine allows index AMs to keep fmgr lookup info for
 *		support procs in the relcache.  As above, only the "default"
 *		functions for any particular indexed attribute are cached.
 *
 * Note: the return value points into cached data that will be lost during
 * any relcache rebuild!  Therefore, either use the callinfo right away,
 * or save it only after having acquired some type of lock on the index rel.
 * ----------------
 */
FmgrInfo *
index_getprocinfo(Relation irel,
				  AttrNumber attnum,
				  uint16 procnum)
{
	FmgrInfo   *locinfo;
	int			nproc;
	int			procindex;

	nproc = irel->rd_indam->amsupport;

	Assert(procnum > 0 && procnum <= (uint16) nproc);

	procindex = (nproc * (attnum - 1)) + (procnum - 1);

	locinfo = irel->rd_supportinfo;

	Assert(locinfo != NULL);

	locinfo += procindex;

	/* Initialize the lookup info if first time through */
	if (locinfo->fn_oid == InvalidOid)
	{
		RegProcedure *loc = irel->rd_support;
		RegProcedure procId;

		Assert(loc != NULL);

		procId = loc[procindex];

		/*
		 * Complain if function was not found during IndexSupportInitialize.
		 * This should not happen unless the system tables contain bogus
		 * entries for the index opclass.  (If an AM wants to allow a support
		 * function to be optional, it can use index_getprocid.)
		 */
		if (!RegProcedureIsValid(procId))
			elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
				 procnum, attnum, RelationGetRelationName(irel));

		fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt);
	}

	return locinfo;
}

/* ----------------
 *		index_store_float8_orderby_distances
 *
 *		Convert AM distance function's results (that can be inexact)
 *		to ORDER BY types and save them into xs_orderbyvals/xs_orderbynulls
 *		for a possible recheck.
 * ----------------
 */
void
index_store_float8_orderby_distances(IndexScanDesc scan, Oid *orderByTypes,
									 IndexOrderByDistance *distances,
									 bool recheckOrderBy)
{
	int			i;

	Assert(distances || !recheckOrderBy);

	scan->xs_recheckorderby = recheckOrderBy;

	for (i = 0; i < scan->numberOfOrderBys; i++)
	{
		if (orderByTypes[i] == FLOAT8OID)
		{
#ifndef USE_FLOAT8_BYVAL
			/* must free any old value to avoid memory leakage */
			if (!scan->xs_orderbynulls[i])
				pfree(DatumGetPointer(scan->xs_orderbyvals[i]));
#endif
			if (distances && !distances[i].isnull)
			{
				scan->xs_orderbyvals[i] = Float8GetDatum(distances[i].value);
				scan->xs_orderbynulls[i] = false;
			}
			else
			{
				scan->xs_orderbyvals[i] = (Datum) 0;
				scan->xs_orderbynulls[i] = true;
			}
		}
		else if (orderByTypes[i] == FLOAT4OID)
		{
			/* convert distance function's result to ORDER BY type */
#ifndef USE_FLOAT4_BYVAL
			/* must free any old value to avoid memory leakage */
			if (!scan->xs_orderbynulls[i])
				pfree(DatumGetPointer(scan->xs_orderbyvals[i]));
#endif
			if (distances && !distances[i].isnull)
			{
				scan->xs_orderbyvals[i] = Float4GetDatum((float4) distances[i].value);
				scan->xs_orderbynulls[i] = false;
			}
			else
			{
				scan->xs_orderbyvals[i] = (Datum) 0;
				scan->xs_orderbynulls[i] = true;
			}
		}
		else
		{
			/*
			 * If the ordering operator's return value is anything else, we
			 * don't know how to convert the float8 bound calculated by the
			 * distance function to that.  The executor won't actually need
			 * the order by values we return here, if there are no lossy
			 * results, so only insist on converting if the *recheck flag is
			 * set.
			 */
			if (scan->xs_recheckorderby)
				elog(ERROR, "ORDER BY operator must return float8 or float4 if the distance function is lossy");
			scan->xs_orderbynulls[i] = true;
		}
	}
}
