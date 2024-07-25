/*-------------------------------------------------------------------------
 *
 * gp_fastsequence.c
 *    routines to maintain a light-weight sequence table.
 *
 * Portions Copyright (c) 2009, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/catalog/gp_fastsequence.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/appendonlywriter.h"
#include "access/htup_details.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/indexing.h"
#include "utils/relcache.h"
#include "utils/fmgroids.h"
#include "access/genam.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "utils/faultinjector.h"
#include "utils/syscache.h"

static void insert_or_update_fastsequence(
	Relation gp_fastsequence_rel,
	HeapTuple oldTuple,
	TupleDesc tupleDesc,
	Oid objid,
	int64 objmod,
	int64 newLastSequence);

/*
 * gp_fastsequence is used to generate and keep track of row numbers for AO
 * and CO tables. Row numbers for AO/CO tables act as a component to form TID,
 * stored in index tuples and used during index scans to lookup intended
 * tuple. Hence this number must be monotonically incrementing value. Also
 * should not rollback irrespective of insert/update transaction aborting for
 * AO/CO table, as reusing row numbers even across aborted transactions would
 * yield wrong results for index scans. Also, entries in gp_fastsequence must
 * only exist for lifespan of the corresponding table.
 *
 * Given those special needs, this function inserts one initial row to
 * fastsequence for segfile 0 (used for special cases like CTAS, ALTER, TRUNCATE,
 * and same transaction create and insert).  Only segfile 0 can be used to insert
 * tuples within same transaction creating the table hence initial entry is
 * only created for these. Entries for rest of segfiles will get created with
 * frozenXids during inserts. These entries are inserted while creating the
 * AO/CO table to leverage MVCC to clear out gp_fastsequence entries incase of
 * aborts/failures. All future calls to insert_or_update_fastsequence() for
 * segfile 0 will perform inplace update.
 */
void
InsertInitialFastSequenceEntries(Oid objid)
{
	Relation gp_fastsequence_rel;
	TupleDesc tupleDesc;
	Datum *values;
	bool *nulls;
	HeapTuple tuple = NULL;

	/*
	 * Open and lock the gp_fastsequence catalog table.
	 */
	gp_fastsequence_rel = table_open(FastSequenceRelationId, RowExclusiveLock);
	tupleDesc = RelationGetDescr(gp_fastsequence_rel);

	values = palloc0(sizeof(Datum) * tupleDesc->natts);
	nulls = palloc0(sizeof(bool) * tupleDesc->natts);

	values[Anum_gp_fastsequence_objid - 1] = ObjectIdGetDatum(objid);
	values[Anum_gp_fastsequence_last_sequence - 1] = Int64GetDatum(0);

	/* Insert enrty for segfile 0 */
	values[Anum_gp_fastsequence_objmod - 1] = Int64GetDatum(RESERVED_SEGNO);
	tuple = heaptuple_form_to(tupleDesc, values, nulls, NULL, NULL);
	CatalogTupleInsert(gp_fastsequence_rel, tuple);
	heap_freetuple(tuple);

	table_close(gp_fastsequence_rel, RowExclusiveLock);
}

/*
 * insert or update the existing fast sequence number for (objid, objmod).
 *
 * If such an entry exists in the table, it is provided in oldTuple. This tuple
 * is updated with the new value. Otherwise, a new tuple is inserted into the
 * table.
 */
static void
insert_or_update_fastsequence(Relation gp_fastsequence_rel,
					HeapTuple oldTuple,
					TupleDesc tupleDesc,
					Oid objid,
					int64 objmod,
					int64 newLastSequence)
{
	Datum *values;
	bool *nulls;
	HeapTuple newTuple;

	values = palloc0(sizeof(Datum) * tupleDesc->natts);
	nulls = palloc0(sizeof(bool) * tupleDesc->natts);

	/*
	 * If such a tuple does not exist, insert a new one.
	 */
	if (!HeapTupleIsValid(oldTuple))
	{
		values[Anum_gp_fastsequence_objid - 1] = ObjectIdGetDatum(objid);
		values[Anum_gp_fastsequence_objmod - 1] = Int64GetDatum(objmod);
		values[Anum_gp_fastsequence_last_sequence - 1] = Int64GetDatum(newLastSequence);

		newTuple = heaptuple_form_to(tupleDesc, values, nulls, NULL, NULL);

		/* insert the tuple */
		CatalogTupleInsert(gp_fastsequence_rel, newTuple);

#ifdef FAULT_INJECTOR
		FaultInjector_InjectFaultIfSet(
								"insert_fastsequence_before_freeze",
								DDLNotSpecified,
								"", //databaseName
								RelationGetRelationName(gp_fastsequence_rel));
#endif

		/* freeze the tuple */
		heap_freeze_tuple_wal_logged(gp_fastsequence_rel, newTuple);

#ifdef FAULT_INJECTOR
		FaultInjector_InjectFaultIfSet(
								"insert_fastsequence_after_freeze",
								DDLNotSpecified,
								"", //databaseName
								RelationGetRelationName(gp_fastsequence_rel));
#endif

		elogif(Debug_appendonly_print_insert_tuple, LOG,
			   "Frozen insert to gp_fastsequence (rel, segno, last_sequence): (%u, %ld, %ld)",
			   objid,
			   objmod,
			   newLastSequence);

		heap_freetuple(newTuple);
	}
	else
	{
		bool isNull;
		int64 currentLastSequence;
#ifdef USE_ASSERT_CHECKING
		Oid oldObjid;
		int64 oldObjmod;
		
		oldObjid = heap_getattr(oldTuple, Anum_gp_fastsequence_objid, tupleDesc, &isNull);
		Assert(!isNull);
		oldObjmod = heap_getattr(oldTuple, Anum_gp_fastsequence_objmod, tupleDesc, &isNull);
		Assert(!isNull);
		Assert(oldObjid == objid && oldObjmod == objmod);
#endif

		currentLastSequence = heap_getattr(oldTuple, Anum_gp_fastsequence_last_sequence, tupleDesc, &isNull);
		if (newLastSequence < currentLastSequence)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("gp_fastsequence value shouldn't go backwards for AO table"),
					 errdetail("current value:" INT64_FORMAT " new value:" INT64_FORMAT,
							   currentLastSequence, newLastSequence)));

		values[Anum_gp_fastsequence_objid - 1] = ObjectIdGetDatum(objid);
		values[Anum_gp_fastsequence_objmod - 1] = Int64GetDatum(objmod);
		values[Anum_gp_fastsequence_last_sequence - 1] = Int64GetDatum(newLastSequence);

		newTuple = heap_form_tuple(tupleDesc, values, nulls);
		newTuple->t_data->t_ctid = oldTuple->t_data->t_ctid;
		newTuple->t_self = oldTuple->t_self;
		heap_inplace_update(gp_fastsequence_rel, newTuple);

		elogif(Debug_appendonly_print_insert_tuple, LOG,
			   "In-place update to gp_fastsequence (ctid, rel, segno, last_sequence): ((%u, %u), %u, %ld, %ld)",
			   ItemPointerGetBlockNumberNoCheck(&newTuple->t_data->t_ctid),
			   ItemPointerGetOffsetNumberNoCheck(&newTuple->t_data->t_ctid),
			   objid,
			   objmod,
			   newLastSequence);

		heap_freetuple(newTuple);
	}
	
	pfree(values);
	pfree(nulls);
}

/*
 * GetFastSequences
 *
 * Get a list of consecutive sequence numbers. The starting sequence
 * number is the current stored value in the table plus 1.
 *
 * If there is not such an entry for objid in the table, create
 * one here and starting value as 1 is returned.
 *
 * The existing entry for objid in the table is updated with a new
 * lastsequence value.
 */
int64 GetFastSequences(Oid objid, int64 objmod, int64 numSequences)
{
	Relation gp_fastsequence_rel;
	ScanKeyData scankey[2];
	SysScanDesc scan;
	TupleDesc tupleDesc;
	HeapTuple tuple;
	int64 firstSequence;
	Datum lastSequenceDatum;
	int64 newLastSequence;

	gp_fastsequence_rel = table_open(FastSequenceRelationId, RowExclusiveLock);
	tupleDesc = RelationGetDescr(gp_fastsequence_rel);

	/*
	 * SELECT * FROM gp_fastsequence
	 * WHERE objid = :1 AND objmod = :2
	 * FOR UPDATE
	 */
	ScanKeyInit(&scankey[0],
				Anum_gp_fastsequence_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objid));
	ScanKeyInit(&scankey[1],
				Anum_gp_fastsequence_objmod,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(objmod));
	scan = systable_beginscan(gp_fastsequence_rel, FastSequenceObjidObjmodIndexId, true,
							  NULL, 2, scankey);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
	{
		firstSequence = 1;
		newLastSequence = numSequences;
	}
	else
	{
		bool isNull;

		lastSequenceDatum = heap_getattr(tuple, Anum_gp_fastsequence_last_sequence,
										tupleDesc, &isNull);
		
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got an invalid lastsequence number: NULL")));
		
		firstSequence = DatumGetInt64(lastSequenceDatum) + 1;
		newLastSequence = firstSequence + numSequences - 1;
	}

	insert_or_update_fastsequence(gp_fastsequence_rel, tuple, tupleDesc,
						objid, objmod, newLastSequence);

	systable_endscan(scan);
		
	/*
	 * gp_fastsequence table locking for AO inserts uses bottom up approach
	 * meaning the locks are first acquired on the segments and later on the
	 * coordinator.
	 * Hence, it is essential that we release the lock here to avoid
	 * any form of coordinator-segment resource deadlock. E.g. A transaction
	 * trying to reindex gp_fastsequence has acquired a lock on it on the
	 * coordinator but is blocked on the segment as another transaction which
	 * is an insert operation has acquired a lock first on segment and is
	 * trying to acquire a lock on the Coordinator. Deadlock!
	 */
	table_close(gp_fastsequence_rel, RowExclusiveLock);

	return firstSequence;
}


/*
 * ReadLastSequence
 *
 * Read the last_sequence attribute from gp_fastsequence by obiid and objmod.
 * If there is not such an entry for objid in the table, return 0.
 */
int64 ReadLastSequence(Oid objid, int64 objmod)
{
	Relation gp_fastsequence_rel;
	ScanKeyData scankey[2];
	SysScanDesc scan;
	TupleDesc tupleDesc;
	HeapTuple tuple;
	int64 lastSequence;

	gp_fastsequence_rel = heap_open(FastSequenceRelationId, AccessShareLock);
	tupleDesc = RelationGetDescr(gp_fastsequence_rel);

	/*
	 * SELECT * FROM gp_fastsequence
	 * WHERE objid = :1 AND objmod = :2
	 */
	ScanKeyInit(&scankey[0],
				Anum_gp_fastsequence_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objid));
	ScanKeyInit(&scankey[1],
				Anum_gp_fastsequence_objmod,
				BTEqualStrategyNumber, F_INT8EQ,
				Int64GetDatum(objmod));
	scan = systable_beginscan(gp_fastsequence_rel, FastSequenceObjidObjmodIndexId, true,
							  NULL, 2, scankey);

	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		lastSequence = 0;
	}
	else
	{
		bool isNull;

		lastSequence = heap_getattr(tuple, Anum_gp_fastsequence_last_sequence,
									tupleDesc, &isNull);

		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("got an invalid lastsequence number: NULL")));
	}

	systable_endscan(scan);

	/*
	 * gp_fastsequence table locking for AO inserts uses bottom up approach
	 * meaning the locks are first acquired on the segments and later on the
	 * coordinator.
	 * Hence, it is essential that we release the lock here to avoid
	 * any form of coordinator-segment resource deadlock. E.g. A transaction
	 * trying to reindex gp_fastsequence has acquired a lock on it on the
	 * coordinator but is blocked on the segment as another transaction which
	 * is an insert operation has acquired a lock first on segment and is
	 * trying to acquire a lock on the Coordinator. Deadlock!
	 */
	heap_close(gp_fastsequence_rel, AccessShareLock);

	return lastSequence;
}

/*
 * ReadAllLastSequences
 *
 * Convenient function to read lastsequence of every objmod.
 * Record the sequence numbers in the passed-in array.
 * All the returned numbers should be non-negative.
 */
void ReadAllLastSequences(Oid objid, int64 *seqs)
{
	Assert(seqs);

	for (int objmod = 0; objmod < MAX_AOREL_CONCURRENCY; objmod++)
	{
		seqs[objmod] = ReadLastSequence(objid, objmod);
 		/* 
		 * ReadLastSequence() is expected to return 0 if the seg doesn't 
		 * exist. Otherwise, it should return a positive number.
		 */
		Assert(seqs[objmod] >= 0);
	}
}

/*
 * RemoveFastSequenceEntry
 *
 * Remove all entries associated with the given object id.
 * And, since gp_fastsequence is cleared, the existing 
 * pg_attribute_encoding.lastrownum does not make sense anymore.
 * Clear them too based on the AO relation OID.
 *
 * If the given objid is an invalid OID, this function simply
 * returns.
 *
 * It is okay for the given valid objid to have no entries in
 * gp_fastsequence.
 */
void
RemoveFastSequenceEntry(Oid relid, Oid objid)
{
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc sscan;
	HeapTuple	tuple;

	if (!OidIsValid(objid))
		return;

	rel = table_open(FastSequenceRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_gp_fastsequence_objid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objid));

	sscan = systable_beginscan(rel, FastSequenceObjidObjmodIndexId, true,
							   NULL, 1, &scankey);

	while ((tuple = systable_getnext(sscan)) != NULL)
	{
		CatalogTupleDelete(rel, &tuple->t_self);
	}

	systable_endscan(sscan);
	table_close(rel, RowExclusiveLock);

	ClearAttributeEncodingLastrownums(relid);
}
