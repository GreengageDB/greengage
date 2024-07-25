/*------------------------------------------------------------------------------
 *
 * appendonly_visimap
 *   maintain a visibility bitmap for append-only tuples.
 *
 * This file provides the user facade for the visibility map handling
 * for append-only tables.
 *
 * The visibiliy map entry is responsible for handling the operations
 * on an individual row in the visimap auxilary relation.
 * The visibility map store is responsible for storing and finding
 * visibility map entries.
 *
 * Copyright (c) 2013-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/appendonly_visimap.h
 *
 *------------------------------------------------------------------------------
*/
#ifndef APPENDONLY_VISIMAP_H
#define APPENDONLY_VISIMAP_H

#include "access/appendonlytid.h"
#include "access/appendonly_visimap_entry.h"
#include "access/appendonly_visimap_store.h"
#include "access/tableam.h"
#include "storage/buffile.h"
#include "utils/snapshot.h"

/*
 * The uncompressed visibility entry bitmap should not be larger than 4 KB.
 * Therefore it can cover at most 32768 entries.
 */
#define APPENDONLY_VISIMAP_MAX_RANGE 32768
#define APPENDONLY_VISIMAP_MAX_BITMAP_SIZE 4096

/*
 * The max value of visiMapEntry->bitmap->nwords
 */
#define APPENDONLY_VISIMAP_MAX_BITMAP_WORD_COUNT \
	(APPENDONLY_VISIMAP_MAX_BITMAP_SIZE / sizeof(bitmapword))

/*
 * Data structure for the ao visibility map processing.
 *
 */
typedef struct AppendOnlyVisimap
{
	/*
	 * Memory context to use for all visibility map related allocations.
	 */
	MemoryContext memoryContext;

	/*
	 * Information about the current visibility map entry. Each visibility map
	 * entry corresponds to a tuple in the visibility map table.
	 */
	AppendOnlyVisimapEntry visimapEntry;

	/*
	 * Support operations to search, load, and store visibility map entries.
	 */
	AppendOnlyVisimapStore visimapStore;

} AppendOnlyVisimap;

/*
 * Data structure to scan an ao visibility map.
 */
typedef struct AppendOnlyVisimapScan
{
	AppendOnlyVisimap visimap;

	SysScanDesc indexScan;

	bool		isFinished;
} AppendOnlyVisimapScan;

/*
 * Data structure to support deletion using the visibility map.
 */
typedef struct AppendOnlyVisimapDelete
{
	/*
	 * The visimap we delete a possible large number of tuples from
	 */
	AppendOnlyVisimap *visiMap;

	/*
	 * A hash table that stores meta information all dirty visimap entries
	 * currently stored in the spill file. This means that we store in-memory
	 * around 20 byte per visimap entry. The resulting overhead is in the area
	 * of 1MB per 1 billion rows.
	 */
	HTAB	   *dirtyEntryCache;

	/*
	 * A workfile storing the updated visimap entries. It is a consequtive
	 * list of dirty (compressed) visimap bitmaps that needs to be updated in
	 * the visimap later.
	 */
	BufFile *workfile;
} AppendOnlyVisimapDelete;


void AppendOnlyVisimap_Init(
					   AppendOnlyVisimap *visiMap,
					   Oid visimapRelid,
					   LOCKMODE lockmode,
					   Snapshot appendonlyMetaDataSnapshot);

bool AppendOnlyVisimap_IsVisible(
							AppendOnlyVisimap *visiMap,
							AOTupleId *tupleId);

void AppendOnlyVisimap_Finish(
						 AppendOnlyVisimap *visiMap,
						 LOCKMODE lockmode);

void AppendOnlyVisimap_DeleteSegmentFile(
									AppendOnlyVisimap *visiMap,
									int segno);

int64 AppendOnlyVisimap_GetSegmentFileHiddenTupleCount(
												 AppendOnlyVisimap *visiMap,
												 int segno);

int64 AppendOnlyVisimap_GetRelationHiddenTupleCount(
											  AppendOnlyVisimap *visiMap);

void AppendOnlyVisimapScan_Init(
						   AppendOnlyVisimapScan *visiMapScan,
						   Oid visimapRelid,
						   LOCKMODE lockmode,
						   Snapshot appendonlyMetadataSnapshot);

extern void AppendOnlyVisimap_Init_forUniqueCheck(
									   AppendOnlyVisimap *visiMap,
									   Relation aoRel,
									   Snapshot snapshot);

extern void AppendOnlyVisimap_Finish_forUniquenessChecks(
												   AppendOnlyVisimap *visiMap);

extern void AppendOnlyVisimap_Init_forIndexOnlyScan(
									   AppendOnlyVisimap *visiMap,
									   Relation aoRel,
									   Snapshot snapshot);

extern void AppendOnlyVisimap_Finish_forIndexOnlyScan(
												   AppendOnlyVisimap *visiMap);

bool AppendOnlyVisimapScan_GetNextInvisible(
									   AppendOnlyVisimapScan *visiMapScan,
									   AOTupleId *tupleId);

void AppendOnlyVisimapScan_Finish(
							 AppendOnlyVisimapScan *visiMapScan,
							 LOCKMODE lockmode);

void AppendOnlyVisimapDelete_Init(
							 AppendOnlyVisimapDelete *visiMapDelete,
							 AppendOnlyVisimap *visiMap);

TM_Result AppendOnlyVisimapDelete_Hide(
							 AppendOnlyVisimapDelete *visiMapDelete, AOTupleId *aoTupleId);

void AppendOnlyVisimapDelete_Finish(
							   AppendOnlyVisimapDelete *visiMapDelete);

void
AppendOnlyVisimapDelete_LoadTuple(AppendOnlyVisimapDelete *visiMapDelete,
								   AOTupleId *aoTupleId);

bool
AppendOnlyVisimapDelete_IsVisible(AppendOnlyVisimapDelete *visiMapDelete,
								  AOTupleId *aoTupleId);

/*
 * AppendOnlyVisimap_UniqueCheck
 *
 * During a uniqueness check, look up the visimap to see if a tuple was deleted
 * by a *committed* transaction.
 *
 * If this uniqueness check is part of an UPDATE, we consult the visiMapDelete
 * structure. Otherwise, we consult the visiMap structure. Only one of these
 * arguments should be supplied.
 */
static inline bool AppendOnlyVisimap_UniqueCheck(AppendOnlyVisimapDelete *visiMapDelete,
												 AppendOnlyVisimap *visiMap,
												 AOTupleId *aoTupleId,
												 Snapshot appendOnlyMetaDataSnapshot)
{
	Snapshot          save_snapshot;
	bool              visible;

	Assert(appendOnlyMetaDataSnapshot->snapshot_type == SNAPSHOT_DIRTY ||
			   appendOnlyMetaDataSnapshot->snapshot_type == SNAPSHOT_SELF);

	if (visiMapDelete)
	{
		/* part of an UPDATE */
		Assert(!visiMap);
		Assert(visiMapDelete->visiMap);

		/* Save the snapshot used for the delete half of the UPDATE */
		save_snapshot = visiMapDelete->visiMap->visimapStore.snapshot;

		/*
		 * Replace with per-tuple snapshot meant for uniqueness checks. See
		 * AppendOnlyVisimap_Init_forUniqueCheck() for details on why we can't
		 * set up the metadata snapshot at init time.
		 */
		visiMapDelete->visiMap->visimapStore.snapshot = appendOnlyMetaDataSnapshot;

		visible = AppendOnlyVisimapDelete_IsVisible(visiMapDelete, aoTupleId);

		/* Restore the snapshot used for the delete half of the UPDATE */
		visiMapDelete->visiMap->visimapStore.snapshot = save_snapshot;
	}
	else
	{
		/* part of a COPY/INSERT */
		Assert(visiMap);

		/*
		 * Set up per-tuple snapshot meant for uniqueness checks. See
		 * AppendOnlyVisimap_Init_forUniqueCheck() for details on why we can't
		 * set up the metadata snapshot at init time.
		 */
		visiMap->visimapStore.snapshot = appendOnlyMetaDataSnapshot;

		visible = AppendOnlyVisimap_IsVisible(visiMap, aoTupleId);

		visiMap->visimapStore.snapshot = NULL; /* be a good citizen */
	}

	return visible;
}

#endif
