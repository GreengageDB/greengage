/*-------------------------------------------------------------------------
 *
 * cdbappendonlyam.h
 *	  append-only relation access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2007, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbappendonlyam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBAPPENDONLYAM_H
#define CDBAPPENDONLYAM_H

#include "access/htup.h"
#include "access/memtup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "access/appendonly_visimap.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

#include "access/appendonlytid.h"
#include "access/appendonlywriter.h"

#include "cdb/cdbbufferedappend.h"
#include "cdb/cdbbufferedread.h"
#include "cdb/cdbvarblock.h"

#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"
#include "cdb/cdbappendonlyblockdirectory.h"

#define DEFAULT_COMPRESS_LEVEL				 (0)
#define MIN_APPENDONLY_BLOCK_SIZE			 (8 * 1024)
#define DEFAULT_APPENDONLY_BLOCK_SIZE		(32 * 1024)
#define MAX_APPENDONLY_BLOCK_SIZE			 (2 * 1024 * 1024)
#define DEFAULT_VARBLOCK_TEMPSPACE_LEN   	 (4 * 1024)
#define DEFAULT_FS_SAFE_WRITE_SIZE			 (0)

/*
 * Check if an attribute value is missing in an AO/CO row according to the row number
 * and the mapping from attnum to "lastrownum" for the corresponding table/segment.
 *
 * See comment for AppendOnlyExecutorReadBlock_BindingInit() for an explanation 
 * on AO tables, which applies to CO tables as well.
 */
#define AO_ATTR_VAL_IS_MISSING(rowNum, colno, segmentFileNum, attnum_to_rownum) \
		((rowNum) <= (attnum_to_rownum)[(colno) * MAX_AOREL_CONCURRENCY + (segmentFileNum)])

extern AppendOnlyBlockDirectory *GetAOBlockDirectory(Relation relation);

/*
 * AppendOnlyInsertDescData is used for inserting data into append-only
 * relations. It serves an equivalent purpose as AppendOnlyScanDescData
 * (relscan.h) only that the later is used for scanning append-only 
 * relations. 
 */
typedef struct AppendOnlyInsertDescData
{
	Relation		aoi_rel;
	Snapshot		appendOnlyMetaDataSnapshot;
	MemTupleBinding *mt_bind;
	File			appendFile;
	int				appendFilePathNameMaxLen;
	char			*appendFilePathName;
	int64			insertCount;
	int64			varblockCount;
	int64           rowCount; /* total row count before insert */
	int64           numSequences; /* total number of available sequences */
	int64           lastSequence; /* last used sequence */
	BlockNumber		cur_segno;
	FileSegInfo     *fsInfo;
	VarBlockMaker	varBlockMaker;
	int64			bufferCount;
	int64			blockFirstRowNum;
	bool			usingChecksum;
	bool			useNoToast;
	bool			skipModCountIncrement;
	int32			completeHeaderLen;
	uint8			*tempSpace;

	int32			usableBlockSize;
	int32			maxDataLen;
	int32			tempSpaceLen;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */

	/*
	 * These serve the equivalent purpose of the uppercase constants of the same
	 * name in tuptoaster.h but here we make these values dynamic.
	 */	
	int32			toast_tuple_threshold;
	int32			toast_tuple_target;
	AppendOnlyStorageAttributes storageAttributes;
	AppendOnlyStorageWrite		storageWrite;

	uint8			*nonCompressedData;

	/* The block directory for the appendonly relation. */
	AppendOnlyBlockDirectory blockDirectory;
	Oid segrelid;
} AppendOnlyInsertDescData;

typedef AppendOnlyInsertDescData *AppendOnlyInsertDesc;

typedef struct AppendOnlyExecutorReadBlock
{
	MemoryContext	memoryContext;

	AppendOnlyStorageRead	*storageRead;

	AttrNumber 		curLargestAttnum; /* the largest attnum stored in memtuple currently being read */
	int64 			*attnum_to_rownum; /*attnum to rownum mapping, used in building memtuple binding */
	MemTupleBinding *mt_bind;
	/*
	 * When reading a segfile that's using version < AOSegfileFormatVersion_GP5,
	 * that is, was created before GPDB 5.0 and upgraded with pg_upgrade, we need
	 * to convert numeric attributes on the fly to new format. numericAtts
	 * is an array of attribute numbers (0-based), of all numeric columns (including
	 * domains over numerics). This array is created lazily when first needed.
	 */
	int			   *numericAtts;
	int				numNumericAtts;

	int				segmentFileNum;

	int64			totalRowsScanned;
	int64			blockRowsProcessed;

	int64			blockFirstRowNum;
	int64			headerOffsetInFile;
	uint8			*dataBuffer;
	int32			dataLen;
	int 			executorBlockKind;
	int 			rowCount;
	bool			isLarge;
	bool			isCompressed;

	uint8			*uncompressedBuffer; /* for decompression */

	uint8			*largeContentBuffer;
	int32			largeContentBufferLen;

	VarBlockReader  varBlockReader;
	int				readerItemCount;
	int				currentItemCount;
	
	uint8			*singleRow;
	int32			singleRowLen;
} AppendOnlyExecutorReadBlock;

/*
 * Descriptor for append-only table scans.
 *
 * Used for scan of append only relations using BufferedRead and VarBlocks
 */
typedef struct AppendOnlyScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* scan parameters */
	Relation	aos_rd;				/* target relation descriptor */
	Snapshot	appendOnlyMetaDataSnapshot;

	/*
	 * Snapshot to use for non-metadata operations.
	 * Usually snapshot = appendOnlyMetaDataSnapshot, but they
	 * differ e.g. if gp_select_invisible is set.
	 */ 
	Snapshot    snapshot;

	Index       aos_scanrelid;
	int			aos_nkeys;			/* number of scan keys */
	ScanKey		aos_key;			/* array of scan key descriptors */
	
	/* file segment scan state */
	int			aos_filenamepath_maxlen;
	char		*aos_filenamepath;
									/* the current segment file pathname. */
	int			aos_total_segfiles;	/* the relation file segment number */
	int			aos_segfiles_processed; /* num of segfiles already processed */
	FileSegInfo **aos_segfile_arr;	/* array of all segfiles information */
	bool		aos_need_new_segfile;
	bool		aos_done_all_segfiles;
	
	MemoryContext	aoScanInitContext; /* mem context at init time */

	int32			usableBlockSize;
	int32			maxDataLen;

	AppendOnlyExecutorReadBlock	executorReadBlock;

	/* current scan state */
	bool		needNextBuffer;

	bool	initedStorageRoutines;

	AppendOnlyStorageAttributes	storageAttributes;
	AppendOnlyStorageRead		storageRead;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */
	
	/*
	 * The block directory info.
	 *
	 * For AO tables, the block directory is built during the first index
	 * creation. If set indicates whether to build block directory while
	 * scanning.
	 */
	AppendOnlyBlockDirectory *blockDirectory;

	/**
	 * The visibility map is used during scans
	 * to check tuple visibility using visi map.
	 */ 
	AppendOnlyVisimap visibilityMap;

	/*
	 * used by `analyze`
	 */

	/*
	 * targrow: the output of the Row-based sampler (Alogrithm S), denotes a
	 * rownumber in the flattened row number space that is the target of a sample,
	 * which starts from 0.
	 * In other words, if we have seg0 rownums: [1, 100], seg1 rownums: [1, 200]
	 * If targrow = 150, then we are referring to seg1's rownum=51.
	 *
	 * In the context of TABLESAMPLE, this is the next row to be sampled.
	 */
	int64				targrow;

	/*
	 * segfirstrow: pointing to the next starting row which is used to check
	 * the distance to `targrow`
	 */
	int64				segfirstrow;

	/*
	 * segrowsprocessed: track the rows processed under the current segfile.
	 * Don't miss updating it accordingly when "segfirstrow" is updated.
	 */
	int64				segrowsprocessed;

	AOBlkDirScan		blkdirscan;

	/* For Bitmap scan */
	int			rs_cindex;		/* current tuple's index in tbmres->offsets */
	struct AppendOnlyFetchDescData *aofetch;

	/*
	 * The total number of bytes read, compressed, across all segment files, so
	 * far. This is used for scan progress reporting.
	 */
	int64		totalBytesRead;

	/*
	 * The next block of AO_MAX_TUPLES_PER_HEAP_BLOCK tuples to be considered
	 * for TABLESAMPLE. This only corresponds to tuples that are physically
	 * present in segfiles (excludes aborted tuples). This "block" is purely a
	 * logical grouping of tuples (in the flat row number space spanning segs).
	 * It does NOT correspond to the concept of a "logical heap block" (block
	 * number in a ctid).
	 *
	 * The choice of AO_MAX_TUPLES_PER_HEAP_BLOCK is somewhat arbitrary. It
	 * could have been anything (that can be represented with an OffsetNumber,
	 * to comply with the TSM API).
	 */
	int64 		sampleTargetBlk;
}	AppendOnlyScanDescData;

typedef AppendOnlyScanDescData *AppendOnlyScanDesc;

/*
 * Statistics on the latest fetch.
 */
typedef struct AppendOnlyFetchDetail
{
	int64		rangeFileOffset;
	int64		rangeFirstRowNum;
	int64		rangeAfterFileOffset;
	int64		rangeLastRowNum;
					/*
					 * The range covered by the Block Directory.
					 */
	
	int64		skipBlockCount;
					/*
					 * Number of blocks skipped since the previous block processed in
					 * the range.
					 */
	
	int64		blockFileOffset;
	int32		blockOverallLen;
	int64		blockFirstRowNum;
	int64		blockLastRowNum;
	bool		isCompressed;
	bool		isLargeContent;
					/*
					 * The last block processed.
					 */

} AppendOnlyFetchDetail;


/*
 * Used for fetch individual tuples from specified by TID of append only relations 
 * using the AO Block Directory, BufferedRead and VarBlocks
 */
typedef struct AppendOnlyFetchDescData
{
	Relation		relation;
	Snapshot		appendOnlyMetaDataSnapshot;

	/*
	 * Snapshot to use for non-metadata operations.
	 * Usually snapshot = appendOnlyMetaDataSnapshot, but they
	 * differ e.g. if gp_select_invisible is set.
	 */ 
	Snapshot    snapshot;

	MemoryContext	initContext;

	AppendOnlyStorageAttributes	storageAttributes;
	AppendOnlyStorageRead		storageRead;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */


	int				totalSegfiles;
	FileSegInfo 	**segmentFileInfo;

	char			*segmentFileName;
	int				segmentFileNameMaxLen;

	/*
	 * Array containing the maximum row number in each aoseg (to be consulted
	 * during fetch). This is a sparse array as not all segments are involved
	 * in a scan. Sparse entries are marked with InvalidAORowNum.
	 *
	 * Note:
	 * If we have no updates and deletes, the total_tupcount is equal to the
	 * maximum row number. But after some updates and deletes, the maximum row
	 * number is always much bigger than total_tupcount, so this carries the
	 * last sequence from gp_fastsequence.
	 */
	int64			lastSequence[AOTupleId_MultiplierSegmentFileNum];

	int32			usableBlockSize;

	AppendOnlyBlockDirectory	blockDirectory;

	AppendOnlyExecutorReadBlock executorReadBlock;

	AOFetchSegmentFile currentSegmentFile;
	
	int64		scanNextFileOffset;
	int64		scanNextRowNum;

	int64		scanAfterFileOffset;
	int64		scanLastRowNum;

	AOFetchBlockMetadata currentBlock;

	int64	skipBlockCount;

	AppendOnlyVisimap visibilityMap;

}	AppendOnlyFetchDescData;

typedef AppendOnlyFetchDescData *AppendOnlyFetchDesc;

/*
 * AppendOnlyDeleteDescData is used for delete data from append-only
 * relations. It serves an equivalent purpose as AppendOnlyScanDescData
 * (relscan.h) only that the later is used for scanning append-only
 * relations.
 */
typedef struct AppendOnlyDeleteDescData
{
	/*
	 * Relation to delete from
	 */
	Relation	aod_rel;

	/*
	 * Snapshot to use for meta data operations
	 */
	Snapshot	appendOnlyMetaDataSnapshot;

	/*
	 * visibility map
	 */
	AppendOnlyVisimap visibilityMap;

	/*
	 * Visimap delete support structure. Used to handle out-of-order deletes
	 */
	AppendOnlyVisimapDelete visiMapDelete;

}			AppendOnlyDeleteDescData;

typedef struct AppendOnlyDeleteDescData *AppendOnlyDeleteDesc;

typedef struct AppendOnlyUniqueCheckDescData
{
	AppendOnlyBlockDirectory *blockDirectory;
	/* visimap to check for deleted tuples as part of INSERT/COPY */
	AppendOnlyVisimap 		 *visimap;
	/* visimap support structure to check for deleted tuples as part of UPDATE */
	AppendOnlyVisimapDelete  *visiMapDelete;
} AppendOnlyUniqueCheckDescData;

typedef struct AppendOnlyUniqueCheckDescData *AppendOnlyUniqueCheckDesc;

typedef struct AppendOnlyIndexOnlyDescData
{
	AppendOnlyBlockDirectory *blockDirectory;
	AppendOnlyVisimap 		 *visimap;
} AppendOnlyIndexOnlyDescData, *AppendOnlyIndexOnlyDesc;

/*
 * Descriptor for fetches from table via an index.
 */
typedef struct IndexFetchAppendOnlyData
{
	IndexFetchTableData xs_base;			/* AM independent part of the descriptor */

	AppendOnlyFetchDesc aofetch;			/* used only for index scans */

	AppendOnlyIndexOnlyDesc indexonlydesc;	/* used only for index only scans */
} IndexFetchAppendOnlyData;

/* ----------------
 *		function prototypes for appendonly access method
 * ----------------
 */

extern AppendOnlyScanDesc appendonly_beginrangescan(Relation relation, 
		Snapshot snapshot,
		Snapshot appendOnlyMetaDataSnapshot, 
		int *segfile_no_arr, int segfile_count,
		int nkeys, ScanKey keys);

extern TableScanDesc appendonly_beginscan(Relation relation,
										  Snapshot snapshot,
										  int nkeys, struct ScanKeyData *key,
										  ParallelTableScanDesc pscan,
										  uint32 flags);
extern void appendonly_rescan(TableScanDesc scan, ScanKey key,
								bool set_params, bool allow_strat,
								bool allow_sync, bool allow_pagemode);
extern void appendonly_endscan(TableScanDesc scan);
extern bool appendonly_getnextslot(TableScanDesc scan,
								   ScanDirection direction,
								   TupleTableSlot *slot);
extern bool appendonly_get_target_tuple(AppendOnlyScanDesc aoscan,
										int64 targrow,
										TupleTableSlot *slot);
extern AppendOnlyFetchDesc appendonly_fetch_init(
	Relation 	relation,
	Snapshot    snapshot,
	Snapshot 	appendOnlyMetaDataSnapshot);
extern bool appendonly_fetch(
	AppendOnlyFetchDesc aoFetchDesc,
	AOTupleId *aoTid,
	TupleTableSlot *slot);
extern void appendonly_fetch_finish(AppendOnlyFetchDesc aoFetchDesc);
extern AppendOnlyIndexOnlyDesc appendonly_index_only_init(Relation relation,
														  Snapshot snapshot);
extern bool appendonly_index_only_check(AppendOnlyIndexOnlyDesc indexonlydesc,
										AOTupleId *aotid,
										Snapshot snapshot);
extern void appendonly_index_only_finish(AppendOnlyIndexOnlyDesc indexonlydesc);
extern void appendonly_dml_init(Relation relation);
extern AppendOnlyInsertDesc appendonly_insert_init(Relation rel,
												   int segno,
												   int64 num_rows);
extern void appendonly_insert(
		AppendOnlyInsertDesc aoInsertDesc, 
		MemTuple instup, 
		AOTupleId *aoTupleId);
extern void appendonly_insert_finish(AppendOnlyInsertDesc aoInsertDesc);
extern void appendonly_dml_finish(Relation relation);

extern AppendOnlyDeleteDesc appendonly_delete_init(Relation rel);
extern TM_Result appendonly_delete(
		AppendOnlyDeleteDesc aoDeleteDesc,
		AOTupleId* aoTupleId);
extern void appendonly_delete_finish(AppendOnlyDeleteDesc aoDeleteDesc);

extern bool appendonly_positionscan(AppendOnlyScanDesc aoscan,
									AppendOnlyBlockDirectoryEntry *dirEntry,
									int fsInfoIdx);
/*
 * Update total bytes read for the entire scan. If the block was compressed,
 * update it with the compressed length. If the block was not compressed, update
 * it with the uncompressed length.
 */
static inline void
AppendOnlyScanDesc_UpdateTotalBytesRead(AppendOnlyScanDesc scan)
{
	Assert(scan->storageRead.isActive);

	if (scan->storageRead.current.isCompressed)
		scan->totalBytesRead += scan->storageRead.current.compressedLen;
	else
		scan->totalBytesRead += scan->storageRead.current.uncompressedLen;
}

static inline int64
AppendOnlyScanDesc_TotalTupCount(AppendOnlyScanDesc scan)
{
	Assert(scan != NULL);

	int64 totalrows = 0;
	FileSegInfo **seginfo = scan->aos_segfile_arr;

    for (int i = 0; i < scan->aos_total_segfiles; i++)
    {
	    if (seginfo[i]->state != AOSEG_STATE_AWAITING_DROP)
		    totalrows += seginfo[i]->total_tupcount;
    }

    return totalrows;
}

#endif   /* CDBAPPENDONLYAM_H */
