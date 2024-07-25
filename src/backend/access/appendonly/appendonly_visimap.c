/*------------------------------------------------------------------------------
 *
 * AppendOnlyVisimap
 *   maintain a visibility bitmap.
 *
 * Copyright (c) 2013-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/appendonly/appendonly_visimap.c
 *
 *------------------------------------------------------------------------------
*/
#include "postgres.h"

#include "access/appendonly_visimap.h"
#include "access/appendonly_visimap_entry.h"
#include "access/appendonly_visimap_store.h"
#include "access/appendonlytid.h"
#include "access/hash.h"
#include "cdb/cdbappendonlyblockdirectory.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/*
 * Key structure for the visimap deletion hash table.
 */
typedef struct AppendOnlyVisiMapDeleteKey
{
	/*
	 * Segno of the dirty visimap entry.
	 *
	 * MPP-23546: Changed the type of segno from int to uint64.  With uint
	 * (4-bytes), additional 4-bytes were being used for padding. The padding
	 * bits may differ for two keys causing two otherwise equal objects to be
	 * treated as unequal by hash functions. Keeping type to uint64 does not
	 * change the value of sizeof(AppendOnlyVisiMapDeleteKey) but eliminates
	 * padding.
	 */
	uint64		segno;

	/*
	 * First row num of the dirty visimap entry.
	 */
	uint64		firstRowNum;
} AppendOnlyVisiMapDeleteKey;

/*
 * Key/Value structure for the visimap deletion hash table.
 */
typedef struct AppendOnlyVisiMapDeleteData
{
	/*
	 * Key of the visimap entry
	 */
	AppendOnlyVisiMapDeleteKey key;

	/*
	 * Location of the latest dirty version of the visimap bitmap in the
	 * BufFile.
	 */
	int 		workFileno;
	off_t		workFileOffset;

	/*
	 * Tuple id of the visimap entry if the visimap entry existed before.
	 */
	ItemPointerData tupleTid;
} AppendOnlyVisiMapDeleteData;



static void AppendOnlyVisimap_Store(
						AppendOnlyVisimap *visiMap);

static void AppendOnlyVisimap_Find(
					   AppendOnlyVisimap *visiMap,
					   AOTupleId *tupleId);

static void AppendOnlyVisimapDelete_Stash(
						AppendOnlyVisimapDelete *visiMapDelete);

static void AppendOnlyVisimapDelete_Find(
						AppendOnlyVisimapDelete *visiMapDelete,
						AOTupleId *aoTupleId);

/*
 * Finishes the visimap operations.
 * No other function should be called with the given
 * visibility map after this function has been called.
 */
void
AppendOnlyVisimap_Finish(
						 AppendOnlyVisimap *visiMap,
						 LOCKMODE lockmode)
{
	if (AppendOnlyVisimapEntry_HasChanged(&visiMap->visimapEntry))
	{
		AppendOnlyVisimap_Store(visiMap);
	}

	AppendOnlyVisimapStore_Finish(&visiMap->visimapStore, lockmode);
	AppendOnlyVisimapEntry_Finish(&visiMap->visimapEntry);

	MemoryContextDelete(visiMap->memoryContext);
	visiMap->memoryContext = NULL;
}

/*
 * Initializes the visimap data structure.
 *
 * It assumes a zero-allocated visibility map.
 * Should not be called twice.
 */
void
AppendOnlyVisimap_Init(
					   AppendOnlyVisimap *visiMap,
					   Oid visimapRelid,
					   LOCKMODE lockmode,
					   Snapshot appendOnlyMetaDataSnapshot)
{
	MemoryContext oldContext;

	Assert(visiMap);
	Assert(OidIsValid(visimapRelid));

	visiMap->memoryContext = AllocSetContextCreate(
												   CurrentMemoryContext,
												   "VisiMapContext",
												   ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(
									   visiMap->memoryContext);

	AppendOnlyVisimapEntry_Init(&visiMap->visimapEntry,
								visiMap->memoryContext);

	AppendOnlyVisimapStore_Init(&visiMap->visimapStore,
								visimapRelid,
								lockmode,
								appendOnlyMetaDataSnapshot,
								visiMap->memoryContext);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Moves the visibility map entry so that the given
 * AO tuple id is covered by it.
 * If necessary a new map entry is initialized.
 *
 * Assumes that all previous changed information have been
 * stored.
 * Should not be called when the append-only table has no relation
 * Assumes that the visibility has been initialized and not finished.
 */
static void
AppendOnlyVisimap_Find(
					   AppendOnlyVisimap *visiMap,
					   AOTupleId *aoTupleId)
{
	Assert(visiMap);
	Assert(aoTupleId);

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Append-only visi map: Find entry for "
		   "(tupleId) = %s",
		   AOTupleIdToString(aoTupleId));

	if (!AppendOnlyVisimapStore_Find(&visiMap->visimapStore,
									 AOTupleIdGet_segmentFileNum(aoTupleId),
									 AppendOnlyVisimapEntry_GetFirstRowNum(
																		   &visiMap->visimapEntry, aoTupleId),
									 &visiMap->visimapEntry))
	{
		/*
		 * There is no entry that covers the given tuple id.
		 */
		AppendOnlyVisimapEntry_New(&visiMap->visimapEntry, aoTupleId);
	}
}

/*
 * Checks if a tuple is visible according to the visibility map.
 * A positive result is a necessary but not sufficient condition for
 * a tuple to be visible to the user.
 *
 * Assumes that the visibility has been initialized and not finished.
 */
bool
AppendOnlyVisimap_IsVisible(
							AppendOnlyVisimap *visiMap,
							AOTupleId *aoTupleId)
{
	Assert(visiMap);

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Append-only visi map: Visibility check: "
		   "(tupleId) = %s",
		   AOTupleIdToString(aoTupleId));

	if (!AppendOnlyVisimapEntry_CoversTuple(&visiMap->visimapEntry,
											aoTupleId))
	{
		/* if necessary persist the current entry before moving. */
		if (AppendOnlyVisimapEntry_HasChanged(&visiMap->visimapEntry))
		{
			AppendOnlyVisimap_Store(visiMap);
		}

		AppendOnlyVisimap_Find(visiMap, aoTupleId);
	}

	/* visimap entry is now positioned to cover the aoTupleId */
	return AppendOnlyVisimapEntry_IsVisible(&visiMap->visimapEntry,
											aoTupleId);
}

/*
 * Stores the current visibility map entry information
 * in the relation either as update or delete.
 *
 * Should not be called if AppendOnlyVisimap_Find has not been
 * called earlier.
 * It may be called when the visibility map entry has not changed. However
 * that is usually wasteful.
 *
 * Assumes that the visibility has been initialized and not finished.
 */
void
AppendOnlyVisimap_Store(
						AppendOnlyVisimap *visiMap)
{
	Assert(visiMap);
	Assert(AppendOnlyVisimapEntry_IsValid(&visiMap->visimapEntry));

	AppendOnlyVisimapStore_Store(&visiMap->visimapStore, &visiMap->visimapEntry);

}

/*
 * If the tuple is not in the current visimap range, the current visimap entry
 * is stashed away and the correct one is loaded or read from the spill file.
 */
void
AppendOnlyVisimapDelete_LoadTuple(AppendOnlyVisimapDelete *visiMapDelete,
								  AOTupleId *aoTupleId)
{
	Assert(visiMapDelete);
	Assert(visiMapDelete->visiMap);
	Assert(aoTupleId);

	/* if the tuple is already covered, we are done */
	if (AppendOnlyVisimapEntry_CoversTuple(&visiMapDelete->visiMap->visimapEntry,
											aoTupleId))
		return;

	/* if necessary persist the current entry before moving. */
	if (AppendOnlyVisimapEntry_HasChanged(&visiMapDelete->visiMap->visimapEntry))
		AppendOnlyVisimapDelete_Stash(visiMapDelete);

	AppendOnlyVisimapDelete_Find(visiMapDelete, aoTupleId);
}

/*
 * Deletes all visibility information for the given segment file.
 */
void
AppendOnlyVisimap_DeleteSegmentFile(
									AppendOnlyVisimap *visiMap,
									int segno)
{
	Assert(visiMap);

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Delete visimap for segment file %d", segno);

	AppendOnlyVisimapStore_DeleteSegmentFile(&visiMap->visimapStore,
											 segno);
}

/*
 * Returns the number of hidden tuples in the relation.
 */
int64
AppendOnlyVisimap_GetRelationHiddenTupleCount(
											  AppendOnlyVisimap *visiMap)
{
	Assert(visiMap);
	return AppendOnlyVisimapStore_GetRelationHiddenTupleCount(
															  &visiMap->visimapStore, &visiMap->visimapEntry);
}

/*
 * Returns the number of hidden tuples in a given segment file.
 */
int64
AppendOnlyVisimap_GetSegmentFileHiddenTupleCount(
												 AppendOnlyVisimap *visiMap,
												 int segno)
{
	Assert(visiMap);
	return AppendOnlyVisimapStore_GetSegmentFileHiddenTupleCount(
																 &visiMap->visimapStore, &visiMap->visimapEntry, segno);
}

/*
 * Starts a new scan for invisible tuple ids.
 */
void
AppendOnlyVisimapScan_Init(
						   AppendOnlyVisimapScan *visiMapScan,
						   Oid visimapRelid,
						   LOCKMODE lockmode,
						   Snapshot appendonlyMetadataSnapshot)
{
	Assert(visiMapScan);
	Assert(OidIsValid(visimapRelid));

	AppendOnlyVisimap_Init(&visiMapScan->visimap, visimapRelid,
						   lockmode,
						   appendonlyMetadataSnapshot);
	visiMapScan->indexScan = AppendOnlyVisimapStore_BeginScan(
															  &visiMapScan->visimap.visimapStore,
															  0,
															  NULL);
	visiMapScan->isFinished = false;
}

/*
 * Returns the next tuple id in the visimap scan that is invisible.
 *
 * If there was a previous successful call to this function during this can,
 * the tupleId parameter should contain the value of the last call.
 * The contents of tupleId is undefined if false is returned.
 */
bool
AppendOnlyVisimapScan_GetNextInvisible(
									   AppendOnlyVisimapScan *visiMapScan,
									   AOTupleId *tupleId)
{
	bool		found;

	Assert(visiMapScan);
	Assert(tupleId);
	Assert(!visiMapScan->isFinished);

	found = false;
	while (!found && !visiMapScan->isFinished)
	{
		if (!AppendOnlyVisimapEntry_IsValid(
											&visiMapScan->visimap.visimapEntry))
		{
			if (!AppendOnlyVisimapStore_GetNext(
												&visiMapScan->visimap.visimapStore,
												visiMapScan->indexScan,
												ForwardScanDirection,
												&visiMapScan->visimap.visimapEntry,
												NULL))
			{
				visiMapScan->isFinished = true;
				return false;
			}
			AOTupleIdSetInvalid(tupleId);
		}

		if (!AppendOnlyVisimapEntry_GetNextInvisible(
													 &visiMapScan->visimap.visimapEntry,
													 tupleId))
		{
			/*
			 * no more invisible tuples in this visimap entry. Try next one
			 */
			AppendOnlyVisimapEntry_Reset(&visiMapScan->visimap.visimapEntry);
		}
		else
		{
			/* Found a tuple. The tuple is is already in the out parameter. */
			found = true;
		}

	}

	return true;
}

/*
 * Finishes a visimap scan.
 */
void
AppendOnlyVisimapScan_Finish(AppendOnlyVisimapScan *visiMapScan,
							 LOCKMODE lockmode)
{
	AppendOnlyVisimapStore_EndScan(&visiMapScan->visimap.visimapStore,
								   visiMapScan->indexScan);
	AppendOnlyVisimap_Finish(&visiMapScan->visimap, lockmode);
}


/*
 * Hash function for the visimap deletion hash table.
 */
static uint32
hash_delete_key(const void *key, Size keysize)
{
	Assert(keysize == sizeof(AppendOnlyVisiMapDeleteKey));
	return DatumGetUInt32(hash_any((const unsigned char *) key,
								   keysize));
}

/*
 * Hash function for comparing two keys of type
 * AppendOnlyVisiMapDeleteKey.  Equality of keys is of interest and
 * not ordering between them (greater/less).
 */
static int
hash_compare_keys(const void *key1, const void *key2, Size keysize)
{
	Assert(keysize == sizeof(AppendOnlyVisiMapDeleteKey));
	AppendOnlyVisiMapDeleteKey *k1 = (AppendOnlyVisiMapDeleteKey *) key1;
	AppendOnlyVisiMapDeleteKey *k2 = (AppendOnlyVisiMapDeleteKey *) key2;

	if ((k1->segno == k2->segno) && (k1->firstRowNum == k2->firstRowNum))
	{
		return 0;
	}
	return 1;
}

/*
 * Inits the visimap delete helper structure.
 *
 * This prepares the hash table and opens the temporary file.
 */
void
AppendOnlyVisimapDelete_Init(
							 AppendOnlyVisimapDelete *visiMapDelete,
							 AppendOnlyVisimap *visiMap)
{
	HASHCTL		hash_ctl;

	Assert(visiMapDelete);
	Assert(visiMap);

	visiMapDelete->visiMap = visiMap;

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(AppendOnlyVisiMapDeleteKey);
	hash_ctl.entrysize = sizeof(AppendOnlyVisiMapDeleteData);
	hash_ctl.hash = hash_delete_key;
	hash_ctl.match = hash_compare_keys;
	hash_ctl.hcxt = visiMap->memoryContext;
	visiMapDelete->dirtyEntryCache = hash_create("VisimapEntryCache",
												 4, /* start small and extend */
												 &hash_ctl,
												 HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	visiMapDelete->workfile = BufFileCreateTemp("visimap_delete", false /* interXact */);
}

/*
 * Rebuild the visimap entry based on the data contents and
 * a few other pieces of information.
 */
static void
AppendOnlyVisimapDelete_RebuildEntry(AppendOnlyVisimapEntry *visimapEntry, int segno, int64 firstRowNum, ItemPointer tid)
{
	MemoryContext oldContext;
	size_t		dataSize;

	visimapEntry->segmentFileNum = segno;
	visimapEntry->firstRowNum = firstRowNum;

	dataSize = VARSIZE(visimapEntry->data) -
		offsetof(AppendOnlyVisimapData, data);

	oldContext = MemoryContextSwitchTo(visimapEntry->memoryContext);
	AppendOnlyVisiMapEnty_ReadData(visimapEntry, dataSize);
	MemoryContextSwitchTo(oldContext);

	/*
	 * We only stash away a visimap entry when it is dirty. Thus, we mark the
	 * visimap entry again as dirty during unstash
	 */
	visimapEntry->dirty = true;
	memcpy(&visimapEntry->tupleTid, tid, sizeof(ItemPointerData));
}

/*
 * Unstashes a dirty visimap from the spill file.
 */
static void
AppendOnlyVisimapDelete_Unstash(
								AppendOnlyVisimapDelete *visiMapDelete, int segno, int64 firstRowNum, AppendOnlyVisiMapDeleteData *deleteData)
{
	AppendOnlyVisimap *visiMap;
	uint64		len,
				dataLen;
	AppendOnlyVisiMapDeleteKey key;

	Assert(visiMapDelete);

	visiMap = visiMapDelete->visiMap;

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Append-only visi map delete: Unstash dirty visimap entry %d/" INT64_FORMAT
		   ", (fileno %d, offset " INT64_FORMAT ")",
		   segno, firstRowNum, deleteData->workFileno, (int64)deleteData->workFileOffset);

	if (BufFileSeek(visiMapDelete->workfile, deleteData->workFileno,
					deleteData->workFileOffset, SEEK_SET) != 0)
	{
		ereport(ERROR,
				(errmsg("failed to seek visimap delete buf file"),
				 errdetail("location (fileno %d, offset " INT64_FORMAT ") visimap entry: %d/" INT64_FORMAT,
						   deleteData->workFileno, (int64)deleteData->workFileOffset,
						   segno, firstRowNum)));
	}

	len = BufFileRead(visiMapDelete->workfile, &key, sizeof(key));
	if (len != sizeof(key))
	{
		ereport(ERROR,
				(errmsg("failed to read visimap delete buf file"),
				 errdetail("location (fileno %d, offset " INT64_FORMAT ") visimap entry: %d/" INT64_FORMAT,
						   deleteData->workFileno, (int64)deleteData->workFileOffset,
						   segno, firstRowNum)));
	}

	Assert(key.segno == segno);
	Assert(key.firstRowNum == firstRowNum);

	len = BufFileRead(visiMapDelete->workfile, visiMap->visimapEntry.data, 4);
	if (len != 4)
	{
		elog(ERROR, "failed to read visimap delete buf file");
	}
	dataLen = VARSIZE(visiMap->visimapEntry.data);

	/* Now read the remaining part of the entry */
	len = BufFileRead(visiMapDelete->workfile,
					  ((char *) visiMap->visimapEntry.data) + 4, dataLen - 4);
	if (len != dataLen - 4)
	{
		ereport(ERROR,
				(errmsg("failed to read visimap delete buf file"),
				 errdetail("location (fileno %d, offset " INT64_FORMAT ") visimap entry: %d/"
							INT64_FORMAT ", len " INT64_FORMAT,
							deleteData->workFileno, (int64)deleteData->workFileOffset,
							segno, firstRowNum, dataLen)));
	}

	AppendOnlyVisimapDelete_RebuildEntry(&visiMap->visimapEntry,
										 segno,
										 firstRowNum,
										 &deleteData->tupleTid);
}

/*
 * Moves the visibility map entry so that the given
 * AO tuple id is covered by it.
 * If necessary a new map entry is initialized.
 * Uses the visimap dirty cache.
 *
 * Assumes that all previous changed information have been
 * stored.
 * Should not be called when the append-only table has no relation
 * Assumes that the visibility has been initialized and not finished.
 */
static void
AppendOnlyVisimapDelete_Find(
							 AppendOnlyVisimapDelete *visiMapDelete,
							 AOTupleId *aoTupleId)
{
	AppendOnlyVisimap *visiMap;
	uint64		firstRowNum;
	AppendOnlyVisiMapDeleteData *r;
	AppendOnlyVisiMapDeleteKey key;

	Assert(visiMapDelete);
	Assert(aoTupleId);

	visiMap = visiMapDelete->visiMap;

	firstRowNum = AppendOnlyVisimapEntry_GetFirstRowNum(
														&visiMap->visimapEntry, aoTupleId);

	key.segno = AOTupleIdGet_segmentFileNum(aoTupleId);
	key.firstRowNum = firstRowNum;

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Append-only visi map delete: Search dirty visimap entry "
		   INT64_FORMAT "/" INT64_FORMAT, key.segno, key.firstRowNum);

	bool		found = false;

	r = hash_search(visiMapDelete->dirtyEntryCache, &key,
					HASH_FIND, &found);

	if (found)
	{
		Assert(r);

		elogif(Debug_appendonly_print_visimap, LOG,
			   "Append-only visi map delete: Found dirty visimap entry "
			   INT64_FORMAT "/" INT64_FORMAT,
			   r->key.segno, r->key.firstRowNum);
		Assert(r->key.firstRowNum == key.firstRowNum);
		Assert(r->key.segno == key.segno);
		AppendOnlyVisimapDelete_Unstash(visiMapDelete, key.segno, key.firstRowNum, r);
	}
	else
	{
		if (!AppendOnlyVisimapStore_Find(&visiMap->visimapStore,
										 AOTupleIdGet_segmentFileNum(aoTupleId),
										 AppendOnlyVisimapEntry_GetFirstRowNum(
																			   &visiMap->visimapEntry, aoTupleId),
										 &visiMap->visimapEntry))
		{
			/*
			 * There is no entry that covers the given tuple id.
			 */
			AppendOnlyVisimapEntry_New(&visiMap->visimapEntry, aoTupleId);
		}
	}
}

/*
 * This function stashes away a dirty visimap entry.
 * It stores the compression bitmap in the spill file and
 * sets the meta information in the hash table.
 */
static void
AppendOnlyVisimapDelete_Stash(
							  AppendOnlyVisimapDelete *visiMapDelete)
{
	AppendOnlyVisimap *visiMap;
	AppendOnlyVisiMapDeleteData *r;
	AppendOnlyVisiMapDeleteKey key;
	MemoryContext oldContext;
	bool		found;
	off_t		offset;
	int 		fileno;

	Assert(visiMapDelete);
	visiMap = visiMapDelete->visiMap;
	Assert(visiMap);

	key.segno = visiMap->visimapEntry.segmentFileNum;
	key.firstRowNum = visiMap->visimapEntry.firstRowNum;
	found = false;
	r = hash_search(visiMapDelete->dirtyEntryCache, &key,
					HASH_ENTER, &found);

	if (!found)
	{
		r->workFileOffset = 0;
		r->workFileno = -1;
		memset(&r->tupleTid, 0, sizeof(ItemPointerData));
	}
	Assert(r->key.firstRowNum == key.firstRowNum);
	Assert(r->key.segno == key.segno);

	oldContext = MemoryContextSwitchTo(visiMap->memoryContext);
	AppendOnlyVisimapEntry_WriteData(&visiMap->visimapEntry);

	/*
	 * If the BufFile was seeked to an internal position for reading a
	 * previously stashed visimap entry before we were called, we must seek
	 * till the end of it before writing new visimap entries.
	 */
	if (BufFileSeek(visiMapDelete->workfile, 0, 0, SEEK_END) != 0)
		elog(ERROR, "failed to seek to end of visimap buf file");
	BufFileTell(visiMapDelete->workfile, &fileno, &offset);

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Append-only visi map delete: Stash dirty visimap entry %d/" INT64_FORMAT,
		   visiMap->visimapEntry.segmentFileNum, visiMap->visimapEntry.firstRowNum);

	if (BufFileWrite(visiMapDelete->workfile, &key, sizeof(key)) != sizeof(key))
	{
		elog(ERROR, "Failed to write visimap delete spill key information: "
			 "segno " INT64_FORMAT ", first row " INT64_FORMAT ", offset "
			 INT64_FORMAT ", length %lu",
			 key.segno, key.firstRowNum, (int64)offset, sizeof(key));
	}
	int size = VARSIZE(visiMap->visimapEntry.data);
	if (BufFileWrite(visiMapDelete->workfile, visiMap->visimapEntry.data, size) != size)
	{
		elog(ERROR, "Failed to write visimap delete spill key information: "
			 "segno " INT64_FORMAT ", first row " INT64_FORMAT ", offset "
			 INT64_FORMAT ", length %d", key.segno, key.firstRowNum,
			 (int64)(offset + sizeof(key)), VARSIZE(visiMap->visimapEntry.data));
	}
	memcpy(&r->tupleTid, &visiMap->visimapEntry.tupleTid, sizeof(ItemPointerData));
	r->workFileOffset = offset;
	r->workFileno = fileno;

	MemoryContextSwitchTo(oldContext);

	visiMap->visimapEntry.dirty = false;
}

/*
 * Hides a given tuple id.
 *
 * Loads the entry for aoTupleId and sets the visibility bit for it.
 *
 * Should only be called when in-order delete of tuples can
 * be guranteed. This means that the tuples are deleted in increasing order.
 * A special case there this function can be used is when only
 * a single tuple is deleted.
 * In all other cases, AppendOnlyVisimapDelete_Hide needs to be used.
 */
TM_Result
AppendOnlyVisimapDelete_Hide(AppendOnlyVisimapDelete *visiMapDelete, AOTupleId *aoTupleId)
{
	Assert(visiMapDelete);
	Assert(visiMapDelete->visiMap);
	Assert(aoTupleId);

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Append-only visi map delete: Hide tuple "
		   "(tupleId) = %s",
		   AOTupleIdToString(aoTupleId));

	AppendOnlyVisimapDelete_LoadTuple(visiMapDelete, aoTupleId);

	return AppendOnlyVisimapEntry_HideTuple(&visiMapDelete->visiMap->visimapEntry, aoTupleId);
}

static void
AppendOnlyVisimapDelete_WriteBackStashedEntries(AppendOnlyVisimapDelete *visiMapDelete)
{
	AppendOnlyVisiMapDeleteData *deleteData;
	int64		len,
				dataLen;
	off_t		currentOffset;
	int			currentFileno;
	
	AppendOnlyVisimap *visiMap;
	bool		found;
	AppendOnlyVisiMapDeleteKey key;

	visiMap = visiMapDelete->visiMap;
	Assert(visiMap);

	if (hash_get_num_entries(visiMapDelete->dirtyEntryCache) == 0)
	{
		return;
	}

	if (BufFileSeek(visiMapDelete->workfile, 0, 0, SEEK_SET) != 0)
	{
		elog(ERROR, "Failed to seek to visimap delete spill beginning");
	}

	/* Get next entry */
	BufFileTell(visiMapDelete->workfile, &currentFileno, &currentOffset);
	len = BufFileRead(visiMapDelete->workfile, &key, sizeof(key));
	while (len == sizeof(key))
	{
		elogif(Debug_appendonly_print_visimap, LOG,
			   "Append-only visi map delete: Got next dirty visimap: "
			   INT64_FORMAT "/" INT64_FORMAT ", offset " INT64_FORMAT,
			   key.segno, key.firstRowNum, (int64)currentOffset);

		/* VARSIZE is only using the first four byte */
		len = BufFileRead(visiMapDelete->workfile, visiMap->visimapEntry.data, 4);
		if (len != 4)
		{
			elog(ERROR, "Failed to read visimap delete spill data");
		}
		dataLen = VARSIZE(visiMap->visimapEntry.data);
		Assert(dataLen <= APPENDONLY_VISIMAP_DATA_BUFFER_SIZE);

		/* Now read the remaining part of the entry */
		len = BufFileRead(visiMapDelete->workfile,
						  ((char *) visiMap->visimapEntry.data) + 4, dataLen - 4);
		if (len != (dataLen - 4))
		{
			elog(ERROR, "Failed to read visimap delete spill data");
		}

		/*
		 * Now we search the hash entry and check if we here have the most
		 * recent version of the visimap entry
		 */
		found = false;
		deleteData = hash_search(visiMapDelete->dirtyEntryCache, &key,
								 HASH_FIND, &found);

		if (!found)
		{
			elog(ERROR, "Found a stashed visimap entry without corresponding meta data: "
				 "offset " INT64_FORMAT, (int64)currentOffset);
		}
		Assert(deleteData);
		Assert(deleteData->key.firstRowNum == key.firstRowNum);
		Assert(deleteData->key.segno == key.segno);
		if (currentFileno != deleteData->workFileno ||
			currentOffset != deleteData->workFileOffset)
		{
			elogif(Debug_appendonly_print_visimap, LOG,
				   "Append-only visi map delete: Found out-dated stashed dirty visimap: "
				   "current (fileno %d, offset " INT64_FORMAT ") expected (fileno %d, offset " INT64_FORMAT ")",
				   currentFileno, (int64)currentOffset,
				   deleteData->workFileno, (int64)deleteData->workFileOffset);
		}
		else
		{
			/*
			 * Until this point on the data field of the visimap entry has
			 * valid information. After this the visimap entry is fully
			 * rebuild.
			 */
			AppendOnlyVisimapDelete_RebuildEntry(&visiMap->visimapEntry,
												 deleteData->key.segno,
												 deleteData->key.firstRowNum,
												 &deleteData->tupleTid);
			AppendOnlyVisimap_Store(visiMapDelete->visiMap);
		}

		BufFileTell(visiMapDelete->workfile, &currentFileno, &currentOffset);
		len = BufFileRead(visiMapDelete->workfile, &key, sizeof(key));
	}
	if (len != 0)
	{
		elog(ERROR, "Failed to read visimap delete spill data");
	}
}

/*
 * Checks if the given tuple id is visible according to the visimapDelete
 * support structure.
 * A positive result is a necessary but not sufficient condition for
 * a tuple to be visible to the user.
 *
 * Loads the entry for the tuple id before checking the bit.
 */
bool
AppendOnlyVisimapDelete_IsVisible(AppendOnlyVisimapDelete *visiMapDelete,
								  AOTupleId *aoTupleId)
{
	AppendOnlyVisimap *visiMap;

	Assert(visiMapDelete);
	Assert(aoTupleId);

	elogif(Debug_appendonly_print_visimap, LOG,
		   "Append-only visi map delete: IsVisible check "
		   "(tupleId) = %s",
		   AOTupleIdToString(aoTupleId));

	visiMap = visiMapDelete->visiMap;
	Assert(visiMap);

	AppendOnlyVisimapDelete_LoadTuple(visiMapDelete, aoTupleId);

	return AppendOnlyVisimapEntry_IsVisible(&visiMap->visimapEntry, aoTupleId);
}


/*
 * Finishes the delete operation.
 * All the dirty visimap entries are read from the spill file and
 * stored in the visimap heap table.
 */
void
AppendOnlyVisimapDelete_Finish(
							   AppendOnlyVisimapDelete *visiMapDelete)
{
	AppendOnlyVisiMapDeleteData *deleteData;
	AppendOnlyVisimap *visiMap;
	bool		found;
	AppendOnlyVisiMapDeleteKey key;

	visiMap = visiMapDelete->visiMap;
	Assert(visiMap);

	elogif(Debug_appendonly_print_visimap, LOG, "Write-back all dirty visimap entries");

	/*
	 * Write back the current change because it is be definition the newest.
	 */
	if (AppendOnlyVisimapEntry_HasChanged(&visiMap->visimapEntry))
	{
		AppendOnlyVisimap_Store(visiMapDelete->visiMap);

		/*
		 * Make the hash map entry invalid so that we do not overwrite the
		 * entry later
		 */
		key.segno = visiMap->visimapEntry.segmentFileNum;
		key.firstRowNum = visiMap->visimapEntry.firstRowNum;
		found = false;
		deleteData = hash_search(visiMapDelete->dirtyEntryCache, &key,
								 HASH_FIND, &found);

		if (found)
		{
			deleteData->workFileOffset = INT64_MAX;
			deleteData->workFileno = -1;
			memset(&deleteData->tupleTid, 0, sizeof(ItemPointerData));
		}
	}

	AppendOnlyVisimapDelete_WriteBackStashedEntries(visiMapDelete);

	hash_destroy(visiMapDelete->dirtyEntryCache);
	BufFileClose(visiMapDelete->workfile);
}

/*
 * AppendOnlyVisimap_Init_forUniqueCheck
 *
 * Initializes the visimap to determine if tuples were deleted as a part of
 * uniqueness checks.
 *
 * Note: we defer setting up the appendOnlyMetaDataSnapshot for the visibility
 * map to the index_unique_check() table AM call. This is because
 * snapshots used for unique index lookups are special and don't follow the
 * usual allocation or registration mechanism. They may be stack-allocated and a
 * new snapshot object may be passed to every unique index check (this happens
 * when SNAPSHOT_DIRTY is passed). While technically, we could set up the
 * metadata snapshot in advance for SNAPSHOT_SELF, the alternative is fine.
 */
void AppendOnlyVisimap_Init_forUniqueCheck(
	AppendOnlyVisimap *visiMap,
	Relation aoRel,
	Snapshot snapshot)
{
	Oid visimaprelid;

	Assert(snapshot->snapshot_type == SNAPSHOT_DIRTY ||
			   snapshot->snapshot_type == SNAPSHOT_SELF);

	GetAppendOnlyEntryAuxOids(aoRel,
							  NULL, NULL, &visimaprelid);
	if (!OidIsValid(visimaprelid))
		elog(ERROR, "Could not find visimap for relation: %u", aoRel->rd_id);

	ereportif(Debug_appendonly_print_visimap, LOG,
			  (errmsg("Append-only visimap init for unique checks"),
				  errdetail("(aoRel = %u, visimaprel = %u)",
							aoRel->rd_id, visimaprelid)));

	AppendOnlyVisimap_Init(visiMap,
						   visimaprelid,
						   AccessShareLock,
						   InvalidSnapshot /* appendOnlyMetaDataSnapshot */);
}

void
AppendOnlyVisimap_Finish_forUniquenessChecks(
	AppendOnlyVisimap *visiMap)
{
	AppendOnlyVisimapStore *visimapStore = &visiMap->visimapStore;
	/*
	 * The snapshot was never set or reset to NULL in between calls to
	 * AppendOnlyVisimap_UniqueCheck().
	 */
	Assert(visimapStore->snapshot == InvalidSnapshot);

	ereportif(Debug_appendonly_print_visimap, LOG,
			  (errmsg("Append-only visimap finish for unique checks"),
				  errdetail("(visimaprel = %u, visimapidxrel = %u)",
							visimapStore->visimapRelation->rd_id,
							visimapStore->visimapRelation->rd_id)));

	AppendOnlyVisimapStore_Finish(&visiMap->visimapStore, AccessShareLock);
	AppendOnlyVisimapEntry_Finish(&visiMap->visimapEntry);

	MemoryContextDelete(visiMap->memoryContext);
	visiMap->memoryContext = NULL;
}

/*
 * AppendOnlyVisimap_Init_forIndexOnlyScan
 *
 * Initializes the visimap to determine if tuples were deleted as a part of
 * index-only scan.
 * 
 * Note: the input snapshot should be an MVCC snapshot.
 */
void AppendOnlyVisimap_Init_forIndexOnlyScan(
	AppendOnlyVisimap *visiMap,
	Relation aoRel,
	Snapshot snapshot)
{
	Oid visimaprelid;

	GetAppendOnlyEntryAuxOids(aoRel,
							  NULL, NULL, &visimaprelid);
	if (!OidIsValid(visimaprelid))
		elog(ERROR, "Could not find visimap for relation: %u", aoRel->rd_id);

	ereportif(Debug_appendonly_print_visimap, LOG,
			  (errmsg("Append-only visimap init for index-only scan"),
				  errdetail("(aoRel = %u, visimaprel = %u)",
							aoRel->rd_id, visimaprelid)));

	Assert(IsMVCCSnapshot(snapshot));

	AppendOnlyVisimap_Init(visiMap,
						   visimaprelid,
						   AccessShareLock,
						   snapshot /* appendOnlyMetaDataSnapshot */);
}

void
AppendOnlyVisimap_Finish_forIndexOnlyScan(
	AppendOnlyVisimap *visiMap)
{
	AppendOnlyVisimapStore *visimapStore = &visiMap->visimapStore;

	ereportif(Debug_appendonly_print_visimap, LOG,
			  (errmsg("Append-only visimap finish for index-only scan"),
				  errdetail("(visimaprel = %u, visimapidxrel = %u)",
							visimapStore->visimapRelation->rd_id,
							visimapStore->visimapRelation->rd_id)));

	AppendOnlyVisimapStore_Finish(&visiMap->visimapStore, AccessShareLock);
	AppendOnlyVisimapEntry_Finish(&visiMap->visimapEntry);

	MemoryContextDelete(visiMap->memoryContext);
	visiMap->memoryContext = NULL;
}
