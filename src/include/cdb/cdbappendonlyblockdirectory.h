/*------------------------------------------------------------------------------
 *
 * cdbappendonlyblockdirectory.h
 *
 * Portions Copyright (c) 2009, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbappendonlyblockdirectory.h
 *
 *------------------------------------------------------------------------------
 */
#ifndef CDBAPPENDONLYBLOCKDIRECTORY_H
#define CDBAPPENDONLYBLOCKDIRECTORY_H

#include "access/aosegfiles.h"
#include "access/aocssegfiles.h"
#include "access/appendonlytid.h"
#include "access/skey.h"
#include "catalog/indexing.h"

extern int gp_blockdirectory_entry_min_range;
extern int gp_blockdirectory_minipage_size;

/*
 * In-memory equivalent of on-disk data structure MinipageEntry, used to
 * represent a block directory entry.
 */
typedef struct AppendOnlyBlockDirectoryEntry
{
	/*
	 * The range of blocks covered by the Block Directory entry, which is the
	 * continuous range [firstRowNum, lastRowNum]. There are no gaps (or holes)
	 * within this range. However, there may be gaps between successive block
	 * directory entries. For e.g. entry0 could have range [1,50] and entry1
	 * could have: [100,150]. The reason gaps arise between successive entries
	 * is that we allocate row numbers using the gp_fastsequence mechanism,
	 * which allocates blocks of row numbers of a pre-determined size (that may
	 * be larger than the number of blocks being inserted)
	 */
	struct range
	{
		int64		fileOffset;
		int64		firstRowNum;

		int64		afterFileOffset;
		int64		lastRowNum;
	} range;

} AppendOnlyBlockDirectoryEntry;

/*
 * The entry in the minipage.
 */
typedef struct MinipageEntry
{
	int64 firstRowNum;
	int64 fileOffset;
	int64 rowCount;
} MinipageEntry;

/*
 * Define a varlena type for a minipage.
 */
typedef struct Minipage
{
	/* Total length. Must be the first. */
	int32 _len;
	int32 version;
	uint32 nEntry;
	
	/* Varlena array */
	MinipageEntry entry[1];
} Minipage;

/*
 * Define the relevant info for a minipage for each
 * column group.
 */
typedef struct MinipagePerColumnGroup
{
	Minipage *minipage;
	uint32 numMinipageEntries;
	ItemPointerData tupleTid;
	/* cached entry number from last call to find_minipage_entry() */
	int cached_entry_no;
} MinipagePerColumnGroup;

/*
 * I don't know the ideal value here. But let us put approximate
 * 8 minipages per heap page.
 */
#define NUM_MINIPAGE_ENTRIES (((MaxHeapTupleSize)/8 - sizeof(HeapTupleHeaderData) - 64 * 3)\
							  / sizeof(MinipageEntry))

#define IsMinipageFull(minipagePerColumnGroup) \
	((minipagePerColumnGroup)->numMinipageEntries == (uint32) gp_blockdirectory_minipage_size)

#define InvalidEntryNum (-1)

/*
 * Define a structure for the append-only relation block directory.
 */
typedef struct AppendOnlyBlockDirectory
{
	Relation aoRel;
	Snapshot appendOnlyMetaDataSnapshot;
	Relation blkdirRel;
	Relation blkdirIdx;
	CatalogIndexState indinfo;
	int numColumnGroups;
	bool isAOCol;

	MemoryContext memoryContext;

	int				totalSegfiles;
	FileSegInfo 	**segmentFileInfo;

	/*
	 * Current segment file number.
	 */
	int currentSegmentFileNum;
	FileSegInfo *currentSegmentFileInfo;

	/*
	 * Last minipage that contains an array of MinipageEntries.
	 */
	MinipagePerColumnGroup *minipages;

	/*
	 * Some temporary space to help form tuples to be inserted into
	 * the block directory, and to help the index scan.
	 */
	Datum *values;
	bool *nulls;
	int numScanKeys;
	ScanKey scanKeys;
	StrategyNumber *strategyNumbers;

	/* Column numbers (zero based) of columns we need to fetch */
	AttrNumber		   *proj_atts;
	AttrNumber			num_proj_atts;

}	AppendOnlyBlockDirectory;


typedef struct AOFetchBlockMetadata
{
	/*
	 * Current cached block directory entry.
	 * FIXME: At times, we rely upon the values in this struct to be valid even
	 * when AOFetchBlockMetadata->valid = false. This indicates that this should
	 * live elsewhere.
	 */
	AppendOnlyBlockDirectoryEntry blockDirectoryEntry;

	/*
	 * Since we have opted to embed this struct inside AppendOnlyFetchDescData
	 * (as opposed to allocating/deallocating it separately), keep a valid flag
	 * to indicate whether the metadata stored here is junk or not.
	 */
	bool valid;

	int64 fileOffset;
	
	int32 overallBlockLen;
	
	int64 firstRowNum;
	int64 lastRowNum;
	
	bool		gotContents;
} AOFetchBlockMetadata;

typedef struct AOFetchSegmentFile
{
	bool isOpen;
	
	int num;
	
	int64 logicalEof;
} AOFetchSegmentFile;

/*
 * Tracks block directory scan state for block-directory based ANALYZE.
 */
typedef struct AOBlkDirScanData
{
	AppendOnlyBlockDirectory	*blkdir;
	SysScanDesc					sysscan;
	int							segno;
	int							colgroupno;
	int							mpentryno;
} AOBlkDirScanData, *AOBlkDirScan;

extern void AppendOnlyBlockDirectoryEntry_GetBeginRange(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							*fileOffset,
	int64							*firstRowNum);
extern void AppendOnlyBlockDirectoryEntry_GetEndRange(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							*afterFileOffset,
	int64							*lastRowNum);
extern bool AppendOnlyBlockDirectoryEntry_RangeHasRow(
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64							checkRowNum);
extern bool AppendOnlyBlockDirectory_GetEntry(
	AppendOnlyBlockDirectory		*blockDirectory,
	AOTupleId 						*aoTupleId,
	int                             columnGroupNo,
	AppendOnlyBlockDirectoryEntry	*directoryEntry,
	int64 				*attnum_to_rownum);
extern bool AppendOnlyBlockDirectory_GetEntryForPartialScan(
	AppendOnlyBlockDirectory		*blockDirectory,
	BlockNumber 					blkno,
	int                             columnGroupNo,
	AppendOnlyBlockDirectoryEntry	*dirEntry,
	int 							*fsInfoIdx);
extern int64 AOBlkDirScan_GetRowNum(
	AOBlkDirScan					blkdirscan,
	int								targsegno,
	int								colgroupno,
	int64							targrow,
	int64							*startrow);
extern bool AppendOnlyBlockDirectory_CoversTuple(
	AppendOnlyBlockDirectory		*blockDirectory,
	AOTupleId 						*aoTupleId);
extern bool blkdir_entry_exists(AppendOnlyBlockDirectory *blockDirectory,
	AOTupleId 				*aoTupleId,
	int 					columnGroupNo);
extern void AppendOnlyBlockDirectory_Init_forInsert(
	AppendOnlyBlockDirectory *blockDirectory,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo *segmentFileInfo,
	int64 lastSequence,
	Relation aoRel,
	int segno,
	int numColumnGroups,
	bool isAOCol);
extern void AppendOnlyBlockDirectory_Init_forSearch(
	AppendOnlyBlockDirectory *blockDirectory,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo **segmentFileInfo,
	int totalSegfiles,
	Relation aoRel,
	int numColumnGroups,
	bool isAOCol,
	bool *proj);
extern void AppendOnlyBlockDirectory_Init_forUniqueChecks(AppendOnlyBlockDirectory *blockDirectory,
														  Relation aoRel,
														  int numColumnGroups,
														  Snapshot snapshot);
extern void AppendOnlyBlockDirectory_Init_forIndexOnlyScan(AppendOnlyBlockDirectory *blockDirectory,
														   Relation aoRel,
														   int numColumnGroups,
														   Snapshot snapshot);
extern void AppendOnlyBlockDirectory_Init_writeCols(
	AppendOnlyBlockDirectory *blockDirectory,
	Snapshot appendOnlyMetaDataSnapshot,
	FileSegInfo *segmentFileInfo,
	Relation aoRel,
	int segno,
	int numColumnGroups,
	bool isAOCol);
extern bool
AppendOnlyBlockDirectory_InsertEntry(AppendOnlyBlockDirectory *blockDirectory,
									 int columnGroupNo,
									 int64 firstRowNum,
									 int64 fileOffset,
									 int64 rowCount);
extern void
AppendOnlyBlockDirectory_DeleteSegmentFile(AppendOnlyBlockDirectory *blockDirectory,
										   int columnGroupNo,
										   int segno,
										   Snapshot snapshot);
extern void AppendOnlyBlockDirectory_End_forInsert(
	AppendOnlyBlockDirectory *blockDirectory);
extern void AppendOnlyBlockDirectory_End_forSearch(
	AppendOnlyBlockDirectory *blockDirectory);
extern void AppendOnlyBlockDirectory_End_writeCols(
	AppendOnlyBlockDirectory *blockDirectory, List *newvals);
extern void
AppendOnlyBlockDirectory_DeleteSegmentFiles(Oid blkdirrelid,
											Snapshot snapshot,
											int segno);
extern void AppendOnlyBlockDirectory_End_forSearch_InSequence(
	AOBlkDirScan seqscan);
extern void AppendOnlyBlockDirectory_End_forUniqueChecks(
	AppendOnlyBlockDirectory *blockDirectory);
extern void AppendOnlyBlockDirectory_End_forIndexOnlyScan(
	AppendOnlyBlockDirectory *blockDirectory);

extern void AppendOnlyBlockDirectory_InsertPlaceholder(AppendOnlyBlockDirectory *blockDirectory,
												  int64 firstRowNum,
												  int64 fileOffset,
												  int columnGroupNo);
/*
 * AppendOnlyBlockDirectory_UniqueCheck
 *
 * Check to see if there is a block directory entry for the tuple. If no such
 * entry exists, the tuple doesn't exist physically in the segfile.
 *
 * Note: We need to use the passed in per-tuple snapshot to perform the block
 * directory lookup. See AppendOnlyBlockDirectory_Init_forUniqueCheck() for
 * details on why we can't set up the metadata snapshot at init time.
 */
static inline bool AppendOnlyBlockDirectory_UniqueCheck(
	AppendOnlyBlockDirectory		*blockDirectory,
	AOTupleId 						*aoTupleId,
	Snapshot						appendOnlyMetaDataSnapshot
)
{
	bool covers;

	Assert(appendOnlyMetaDataSnapshot->snapshot_type == SNAPSHOT_DIRTY ||
			   appendOnlyMetaDataSnapshot->snapshot_type == SNAPSHOT_SELF);

	Assert(blockDirectory->appendOnlyMetaDataSnapshot == InvalidSnapshot);

	/* Set up the snapshot to use for the block directory scan */
	blockDirectory->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;

	covers = AppendOnlyBlockDirectory_CoversTuple(blockDirectory,
												  aoTupleId);

	/*
	 * Reset the metadata snapshot to avoid leaking a stack reference. We have
	 * to do this since SNAPSHOT_DIRTY is stack-allocated.
	 */
	blockDirectory->appendOnlyMetaDataSnapshot = InvalidSnapshot;

	return covers;
}

static inline uint32
minipage_size(uint32 nEntry)
{
	return offsetof(Minipage, entry) + sizeof(MinipageEntry) * nEntry;
}

/*
 * copy_out_minipage
 *
 * Copy out the minipage content from a deformed tuple.
 */
static inline void
copy_out_minipage(MinipagePerColumnGroup *minipageInfo,
				  Datum minipage_value,
				  bool minipage_isnull)
{
	struct varlena *value;
	struct varlena *detoast_value;

	Assert(!minipage_isnull);

	value = (struct varlena *)
		DatumGetPointer(minipage_value);
	detoast_value = pg_detoast_datum(value);
	Assert(VARSIZE(detoast_value) <= minipage_size(NUM_MINIPAGE_ENTRIES));

	memcpy(minipageInfo->minipage, detoast_value, VARSIZE(detoast_value));
	if (detoast_value != value)
		pfree(detoast_value);

	Assert(minipageInfo->minipage->nEntry <= NUM_MINIPAGE_ENTRIES);

	minipageInfo->numMinipageEntries = minipageInfo->minipage->nEntry;
	minipageInfo->cached_entry_no = InvalidEntryNum;
}

static inline void
AOBlkDirScan_Init(AOBlkDirScan blkdirscan,
				  AppendOnlyBlockDirectory *blkdir)
{
	blkdirscan->blkdir = blkdir;
	blkdirscan->sysscan = NULL;
	blkdirscan->segno = -1;
	blkdirscan->colgroupno = 0;
	blkdirscan->mpentryno = InvalidEntryNum;
}

/* should be called before fetch_finish() */
static inline void
AOBlkDirScan_Finish(AOBlkDirScan blkdirscan)
{
	/*
	 * Make sure blkdir hasn't been destroyed by fetch_finish(),
	 * or systable_endscan_ordered() will be crashed for sysscan
	 * is holding blkdir relation which is freed.
	 */
	Assert(blkdirscan->blkdir != NULL);

	if (blkdirscan->sysscan != NULL)
	{
		systable_endscan_ordered(blkdirscan->sysscan);
		blkdirscan->sysscan = NULL;
	}
	blkdirscan->segno = -1;
	blkdirscan->colgroupno = 0;
	blkdirscan->blkdir = NULL;
}

#endif
