/*-------------------------------------------------------------------------
 *
 * appendonlyam.c
 *	  append-only relation access method code
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2008-2009, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/appendonly/appendonlyam.c
 *
 *
 * INTERFACE ROUTINES
 *		appendonly_beginscan		- begin relation scan
 *		appendonly_rescan			- restart a relation scan
 *		appendonly_endscan			- end relation scan
 *		appendonly_getnextslot		- retrieve next tuple in scan
 *		appendonly_insert_init		- initialize an insert operation
 *		appendonly_insert			- insert tuple into a relation
 *		appendonly_insert_finish	- finish an insert operation
 *
 * NOTES
 *	  This file contains the appendonly_ routines which implement
 *	  the access methods used for all append-only relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include "catalog/storage.h"
#include "access/multixact.h"
#include "catalog/storage_xlog.h"

#include "access/aosegfiles.h"
#include "access/appendonlytid.h"
#include "access/appendonlywriter.h"
#include "access/aomd.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/namespace.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attribute_encoding.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbappendonlystorageformat.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/datum.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


typedef enum AoExecutorBlockKind
{
	AoExecutorBlockKind_None = 0,
	AoExecutorBlockKind_VarBlock,
	AoExecutorBlockKind_SingleRow,
	MaxAoExecutorBlockKind		/* must always be last */
}			AoExecutorBlockKind;

static void AppendOnlyExecutionReadBlock_SetSegmentFileNum(
											   AppendOnlyExecutorReadBlock *executorReadBlock,
											   int segmentFileNum);

static void AppendOnlyExecutionReadBlock_SetPositionInfo(
											 AppendOnlyExecutorReadBlock *executorReadBlock,
											 int64 blockFirstRowNum);

static void AppendOnlyExecutorReadBlock_Init(
								 AppendOnlyExecutorReadBlock *executorReadBlock,
								 Relation relation,
								 MemoryContext memoryContext,
								 AppendOnlyStorageRead *storageRead,
								 int32 usableBlockSize);

static void AppendOnlyExecutorReadBlock_Finish(
								   AppendOnlyExecutorReadBlock *executorReadBlock);

static void AppendOnlyExecutorReadBlock_ResetCounts(
										AppendOnlyExecutorReadBlock *executorReadBlock);

static void AppendOnlyScanDesc_UpdateTotalBytesRead(
										AppendOnlyScanDesc scan);

/* ----------------
 *		initscan - scan code common to appendonly_beginscan and appendonly_rescan
 * ----------------
 */
static void
initscan(AppendOnlyScanDesc scan, ScanKey key)
{
	/*
	 * copy the scan key, if appropriate
	 */
	if (key != NULL)
		memcpy(scan->aos_key, key, scan->aos_nkeys * sizeof(ScanKeyData));

	scan->aos_filenamepath[0] = '\0';
	scan->aos_segfiles_processed = 0;
	scan->aos_need_new_segfile = true;	/* need to assign a file to be scanned */
	scan->aos_done_all_segfiles = false;
	scan->needNextBuffer = true;

	if (scan->initedStorageRoutines)
		AppendOnlyExecutorReadBlock_ResetCounts(
												&scan->executorReadBlock);

	scan->executorReadBlock.mt_bind = NULL;

	pgstat_count_heap_scan(scan->aos_rd);
}

/*
 * Open the next file segment to scan and allocate all resources needed for it.
 */
static bool
SetNextFileSegForRead(AppendOnlyScanDesc scan)
{
	Relation	reln = scan->aos_rd;
	int			segno = -1;
	int64		eof = 0;
	int			formatversion = -2; /* some invalid value */
	bool		finished_all_files = true;	/* assume */
	int32		fileSegNo;

	Assert(scan->aos_need_new_segfile); /* only call me when last segfile
										 * completed */
	Assert(!scan->aos_done_all_segfiles);	/* don't call me if I told you to
											 * stop */


	if (!scan->initedStorageRoutines)
	{
		PGFunction *fns = NULL;

		AppendOnlyStorageRead_Init(
								   &scan->storageRead,
								   scan->aoScanInitContext,
								   scan->usableBlockSize,
								   NameStr(scan->aos_rd->rd_rel->relname),
								   scan->title,
								   &scan->storageAttributes);

		/*
		 * There is no guarantee that the current memory context will be
		 * preserved between calls, so switch to a safe memory context for
		 * retrieving compression information.
		 */
		MemoryContext oldMemoryContext = MemoryContextSwitchTo(scan->aoScanInitContext);

		/* Get the relation specific compression functions */

		fns = get_funcs_for_compression(scan->storageAttributes.compressType);
		scan->storageRead.compression_functions = fns;

		if (scan->storageRead.compression_functions != NULL)
		{
			PGFunction	cons = fns[COMPRESSION_CONSTRUCTOR];
			CompressionState *cs;
			StorageAttributes sa;

			sa.comptype = scan->storageAttributes.compressType;
			sa.complevel = scan->storageAttributes.compressLevel;
			sa.blocksize = scan->usableBlockSize;

			/*
			 * The relation's tuple descriptor allows the compression
			 * constructor to make decisions about how to compress or
			 * decompress the relation given it's structure.
			 */
			cs = callCompressionConstructor(cons,
											RelationGetDescr(reln),
											&sa,
											false /* decompress */ );
			scan->storageRead.compressionState = cs;
		}

		/* Switch back to caller's memory context. */
		MemoryContextSwitchTo(oldMemoryContext);

		AppendOnlyExecutorReadBlock_Init(
										 &scan->executorReadBlock,
										 scan->aos_rd,
										 scan->aoScanInitContext,
										 &scan->storageRead,
										 scan->usableBlockSize);

		scan->needNextBuffer = true;	/* so we read a new buffer right away */

		scan->initedStorageRoutines = true;
	}

	/*
	 * Do we have more segment files to read or are we done?
	 */
	while (scan->aos_segfiles_processed < scan->aos_total_segfiles)
	{
		/* still have more segment files to read. get info of the next one */
		FileSegInfo *fsinfo = scan->aos_segfile_arr[scan->aos_segfiles_processed];

		segno = fsinfo->segno;
		formatversion = fsinfo->formatversion;
		eof = (int64) fsinfo->eof;

		scan->aos_segfiles_processed++;

		/*
		 * If the 'eof' is zero or it's just a lingering dropped segment
		 * (which we see as dead, too), skip it.
		 */
		if (eof > 0 && fsinfo->state != AOSEG_STATE_AWAITING_DROP)
		{
			/* Initialize the block directory for inserts if needed. */
			if (scan->blockDirectory)
			{
				AppendOnlyBlockDirectory_Init_forInsert(scan->blockDirectory,
														scan->appendOnlyMetaDataSnapshot,
														fsinfo,
														0,	/* lastSequence */
														scan->aos_rd,
														segno,	/* segno */
														1,	/* columnGroupNo */
														false);
			}

			finished_all_files = false;
			break;
		}
	}

	if (finished_all_files)
	{
		/* finished reading all segment files */
		scan->aos_need_new_segfile = false;
		scan->aos_done_all_segfiles = true;

		return false;
	}

	MakeAOSegmentFileName(reln, segno, InvalidFileNumber, &fileSegNo, scan->aos_filenamepath);
	Assert(strlen(scan->aos_filenamepath) + 1 <= scan->aos_filenamepath_maxlen);

	Assert(scan->initedStorageRoutines);

	AppendOnlyStorageRead_OpenFile(
								   &scan->storageRead,
								   scan->aos_filenamepath,
								   formatversion,
								   eof);

	AppendOnlyExecutionReadBlock_SetSegmentFileNum(
												   &scan->executorReadBlock,
												   segno);

	AppendOnlyExecutionReadBlock_SetPositionInfo(
												 &scan->executorReadBlock,
												  /* blockFirstRowNum */ 1);

	/* ready to go! */
	scan->aos_need_new_segfile = false;


	elogif(Debug_appendonly_print_scan, LOG,
		   "Append-only scan initialize for table '%s', %u/%u/%u, segment file %u, EOF " INT64_FORMAT ", "
		   "(compression = %s, usable blocksize %d)",
		   NameStr(scan->aos_rd->rd_rel->relname),
		   scan->aos_rd->rd_node.spcNode,
		   scan->aos_rd->rd_node.dbNode,
		   scan->aos_rd->rd_node.relNode,
		   segno,
		   eof,
		   (scan->storageAttributes.compress ? "true" : "false"),
		   scan->usableBlockSize);


	return true;
}

/*
 * Similar to SetNextFileSegForRead(), except that we explicitly specify the
 * seg to be read (via 'fsInfoIdx', an index into the scan's segfile array).
 *
 * We return true if we are successfully able to open the target segment.
 *
 * Since SetNextFileSegForRead() opens the next segment starting from
 * aoscan->aos_segfiles_processed, skipping empty/awaiting-drop segs, we also
 * check if the seg opened isn't the one we targeted. If it isn't, then the
 * target seg was empty/awaiting-drop, and we return false.
 */
static bool
SetSegFileForRead(AppendOnlyScanDesc aoscan, int fsInfoIdx)
{
	Assert(fsInfoIdx >= 0 && fsInfoIdx < aoscan->aos_total_segfiles);

	/*
	 * Advance aos_segfiles_processed pointer to target segment, so that it
	 * is considered as the "next" segment.
	 */
	aoscan->aos_segfiles_processed = fsInfoIdx;

	return SetNextFileSegForRead(aoscan) &&
		(aoscan->aos_segfiles_processed - fsInfoIdx == 1);
}

/*
 * errcontext_appendonly_insert_block_user_limit
 *
 * Add an errcontext() line showing the table name but little else because this is a user
 * caused error.
 */
static int
errcontext_appendonly_insert_block_user_limit(AppendOnlyInsertDesc aoInsertDesc)
{
	char	   *relationName = NameStr(aoInsertDesc->aoi_rel->rd_rel->relname);

	errcontext("Append-Only table '%s'", relationName);

	return 0;
}

/*
 * Open the next file segment for write.
 */
static void
SetCurrentFileSegForWrite(AppendOnlyInsertDesc aoInsertDesc)
{
	RelFileNodeBackend rnode;

	FileSegInfo *fsinfo;
	int64		eof;
	int64		eof_uncompressed;
	int64		varblockcount;
	int32		fileSegNo;

	rnode.node = aoInsertDesc->aoi_rel->rd_node;
	rnode.backend = aoInsertDesc->aoi_rel->rd_backend;

	/* Make the 'segment' file name */
	MakeAOSegmentFileName(aoInsertDesc->aoi_rel,
						  aoInsertDesc->cur_segno, InvalidFileNumber,
						  &fileSegNo,
						  aoInsertDesc->appendFilePathName);
	Assert(strlen(aoInsertDesc->appendFilePathName) + 1 <= aoInsertDesc->appendFilePathNameMaxLen);

	/*
	 * Now, get the information for the file segment we are going to append
	 * to.
	 */
	aoInsertDesc->fsInfo = GetFileSegInfo(aoInsertDesc->aoi_rel,
										  aoInsertDesc->appendOnlyMetaDataSnapshot,
										  aoInsertDesc->cur_segno,
										  true);

	/* Never insert into a segment that is awaiting a drop */
	if (aoInsertDesc->fsInfo->state == AOSEG_STATE_AWAITING_DROP)
		elog(ERROR, "cannot insert into segno (%d) from AO relid %u that is in state AOSEG_STATE_AWAITING_DROP",
			 aoInsertDesc->cur_segno, RelationGetRelid(aoInsertDesc->aoi_rel));

	fsinfo = aoInsertDesc->fsInfo;
	Assert(fsinfo);
	eof = (int64) fsinfo->eof;
	eof_uncompressed = (int64) fsinfo->eof_uncompressed;
	varblockcount = (int64) fsinfo->varblockcount;
	aoInsertDesc->rowCount = fsinfo->total_tupcount;

	/*
	 * Segment file #0 is created when the Append-Only table is created.
	 *
	 * Other segment files are created on-demand under transaction.
	 */
	if (aoInsertDesc->cur_segno > 0 && eof == 0)
	{
		AppendOnlyStorageWrite_TransactionCreateFile(&aoInsertDesc->storageWrite,
													 &rnode,
													 aoInsertDesc->cur_segno);
	}

	/*
	 * Open the existing file for write.
	 */
	AppendOnlyStorageWrite_OpenFile(&aoInsertDesc->storageWrite,
									aoInsertDesc->appendFilePathName,
									aoInsertDesc->fsInfo->formatversion,
									eof,
									eof_uncompressed,
									&rnode,
									aoInsertDesc->cur_segno);

	/* reset counts */
	aoInsertDesc->insertCount = 0;
	aoInsertDesc->varblockCount = 0;

	/*
	 * Use the current block count from the segfile info so our system log
	 * error messages are accurate.
	 */
	aoInsertDesc->bufferCount = varblockcount;
}

/*
 * Finished scanning this file segment. Close it.
 */
static void
CloseScannedFileSeg(AppendOnlyScanDesc scan)
{
	AppendOnlyStorageRead_CloseFile(&scan->storageRead);

	scan->aos_need_new_segfile = true;
}

/*
 * Finished writing to this file segment. Update catalog and close file.
 */
static void
CloseWritableFileSeg(AppendOnlyInsertDesc aoInsertDesc)
{
	int64		fileLen;
	int64		fileLen_uncompressed;

	AppendOnlyStorageWrite_TransactionFlushAndCloseFile(&aoInsertDesc->storageWrite,
														&fileLen,
														&fileLen_uncompressed);

	/*
	 * Update the AO segment info table with our new eof
	 */
	UpdateFileSegInfo(aoInsertDesc->aoi_rel,
					  aoInsertDesc->cur_segno,
					  fileLen,
					  fileLen_uncompressed,
					  aoInsertDesc->insertCount,
					  aoInsertDesc->varblockCount,
					  (aoInsertDesc->skipModCountIncrement ? 0 : 1),
					  AOSEG_STATE_USECURRENT);

	pfree(aoInsertDesc->fsInfo);
	aoInsertDesc->fsInfo = NULL;

	elogif(Debug_appendonly_print_insert, LOG,
		   "Append-only scan closed write file segment #%d for table %s "
		   "(file length " INT64_FORMAT ", insert count " INT64_FORMAT ", VarBlock count " INT64_FORMAT,
		   aoInsertDesc->cur_segno,
		   NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
		   fileLen,
		   aoInsertDesc->insertCount,
		   aoInsertDesc->varblockCount);

}

/* ------------------------------------------------------------------------------ */

static void
AppendOnlyExecutorReadBlock_GetContents(AppendOnlyExecutorReadBlock *executorReadBlock)
{
	VarBlockCheckError varBlockCheckError;

	if (!executorReadBlock->isCompressed)
	{
		if (!executorReadBlock->isLarge)
		{
			/*
			 * Small content.
			 */
			executorReadBlock->dataBuffer =
				AppendOnlyStorageRead_GetBuffer(executorReadBlock->storageRead);

			elogif(Debug_appendonly_print_scan, LOG,
				   "Append-only scan read small non-compressed block for table '%s' "
				   "(length = %d, segment file '%s', block offset in file = " INT64_FORMAT ")",
				   AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
				   executorReadBlock->dataLen,
				   AppendOnlyStorageRead_SegmentFileName(executorReadBlock->storageRead),
				   executorReadBlock->headerOffsetInFile);
		}
		else
		{
			/*
			 * Large row.
			 */

			/* UNDONE: Error out if NOTOAST isn't ON. */

			/* UNDONE: Error out if it is not a single row */
			Assert(executorReadBlock->executorBlockKind == AoExecutorBlockKind_SingleRow);

			/*
			 * Enough room in our private buffer? UNDONE: Is there a way to
			 * avoid the 2nd copy later doProcessTuple?
			 */
			if (executorReadBlock->largeContentBufferLen < executorReadBlock->dataLen)
			{
				MemoryContext oldMemoryContext;

				/*
				 * Buffer too small.
				 */
				oldMemoryContext =
					MemoryContextSwitchTo(executorReadBlock->memoryContext);

				if (executorReadBlock->largeContentBuffer != NULL)
				{
					/*
					 * Make sure we set the our pointer to NULL here in case
					 * the subsequent allocation fails.  Otherwise cleanup
					 * will get confused.
					 */
					pfree(executorReadBlock->largeContentBuffer);
					executorReadBlock->largeContentBuffer = NULL;
				}

				executorReadBlock->largeContentBuffer = (uint8 *) palloc(executorReadBlock->dataLen);
				executorReadBlock->largeContentBufferLen = executorReadBlock->dataLen;

				/*
				 * Deallocation and allocation done.  Go back to caller
				 * memory-context.
				 */
				MemoryContextSwitchTo(oldMemoryContext);
			}

			executorReadBlock->dataBuffer = executorReadBlock->largeContentBuffer;

			AppendOnlyStorageRead_Content(executorReadBlock->storageRead,
										  executorReadBlock->dataBuffer,
										  executorReadBlock->dataLen);

			elogif(Debug_appendonly_print_scan, LOG,
				   "Append-only scan read large row for table '%s' "
				   "(length = %d, segment file '%s', "
				   "block offset in file = " INT64_FORMAT ")",
				   AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
				   executorReadBlock->dataLen,
				   AppendOnlyStorageRead_SegmentFileName(executorReadBlock->storageRead),
				   executorReadBlock->headerOffsetInFile);
		}
	}
	else
	{
		int32		compressedLen =
		AppendOnlyStorageRead_CurrentCompressedLen(executorReadBlock->storageRead);

		/*
		 * AppendOnlyStorageWrite does not report compressed for large content
		 * metadata.
		 */
		Assert(!executorReadBlock->isLarge);

		/*
		 * Decompress into our temporary buffer.
		 */
		executorReadBlock->dataBuffer = executorReadBlock->uncompressedBuffer;

		AppendOnlyStorageRead_Content(executorReadBlock->storageRead,
									  executorReadBlock->dataBuffer,
									  executorReadBlock->dataLen);

		elogif(Debug_appendonly_print_scan, LOG,
			   "Append-only scan read decompressed block for table '%s' "
			   "(compressed length %d, length = %d, segment file '%s', "
			   "block offset in file = " INT64_FORMAT ")",
			   AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
			   compressedLen,
			   executorReadBlock->dataLen,
			   AppendOnlyStorageRead_SegmentFileName(executorReadBlock->storageRead),
			   executorReadBlock->headerOffsetInFile);
	}

	/*
	 * The executorBlockKind value is what the executor -- i.e. the upper part
	 * of this appendonlyam module! -- has stored in the Append-Only Storage
	 * header.  We interpret it here.
	 */

	switch (executorReadBlock->executorBlockKind)
	{
		case AoExecutorBlockKind_VarBlock:
			varBlockCheckError = VarBlockIsValid(executorReadBlock->dataBuffer, executorReadBlock->dataLen);
			if (varBlockCheckError != VarBlockCheckOk)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("VarBlock is not valid, valid block check error %d, detail '%s'",
								varBlockCheckError,
								VarBlockGetCheckErrorStr()),
						 errdetail_appendonly_read_storage_content_header(executorReadBlock->storageRead),
						 errcontext_appendonly_read_storage_block(executorReadBlock->storageRead)));

			/*
			 * Now use the VarBlock module to extract the items out.
			 */
			VarBlockReaderInit(&executorReadBlock->varBlockReader,
							   executorReadBlock->dataBuffer,
							   executorReadBlock->dataLen);

			executorReadBlock->readerItemCount = VarBlockReaderItemCount(&executorReadBlock->varBlockReader);

			executorReadBlock->currentItemCount = 0;

			if (executorReadBlock->rowCount != executorReadBlock->readerItemCount)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("row count %d in append-only storage header does not match VarBlock item count %d",
								executorReadBlock->rowCount,
								executorReadBlock->readerItemCount),
						 errdetail_appendonly_read_storage_content_header(executorReadBlock->storageRead),
						 errcontext_appendonly_read_storage_block(executorReadBlock->storageRead)));
			}

			elogif(Debug_appendonly_print_scan, LOG,
				   "append-only scan read VarBlock for table '%s' with %d items (block offset in file = " INT64_FORMAT ")",
				   AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
				   executorReadBlock->readerItemCount,
				   executorReadBlock->headerOffsetInFile);
			break;

		case AoExecutorBlockKind_SingleRow:
			if (executorReadBlock->rowCount != 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("row count %d in append-only storage header is not 1 for single row",
								executorReadBlock->rowCount),
						 errdetail_appendonly_read_storage_content_header(executorReadBlock->storageRead),
						 errcontext_appendonly_read_storage_block(executorReadBlock->storageRead)));
			}
			executorReadBlock->singleRow = executorReadBlock->dataBuffer;
			executorReadBlock->singleRowLen = executorReadBlock->dataLen;

			elogif(Debug_appendonly_print_scan, LOG, "Append-only scan read single row for table '%s' with length %d (block offset in file = " INT64_FORMAT ")",
				   AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
				   executorReadBlock->singleRowLen,
				   executorReadBlock->headerOffsetInFile);

			break;

		default:
			elog(ERROR, "Unrecognized append-only executor block kind: %d",
				 executorReadBlock->executorBlockKind);
			break;
	}
}

static bool
AppendOnlyExecutorReadBlock_GetBlockInfo(AppendOnlyStorageRead *storageRead,
										 AppendOnlyExecutorReadBlock *executorReadBlock)
{
	int64		blockFirstRowNum = executorReadBlock->blockFirstRowNum;

	if (!AppendOnlyStorageRead_GetBlockInfo(storageRead,
											&executorReadBlock->dataLen,
											&executorReadBlock->executorBlockKind,
											&executorReadBlock->blockFirstRowNum,
											&executorReadBlock->rowCount,
											&executorReadBlock->isLarge,
											&executorReadBlock->isCompressed))
	{
		return false;
	}

	/*
	 * If the firstRowNum is not stored in the AOBlock,
	 * executorReadBlock->blockFirstRowNum is set to -1. Since this is
	 * properly updated by calling functions
	 * AppendOnlyExecutionReadBlock_SetPositionInfo and
	 * AppendOnlyExecutionReadBlock_FinishedScanBlock, we restore the last
	 * value when the block does not contain firstRowNum.
	 */
	if (executorReadBlock->blockFirstRowNum < 0)
	{
		executorReadBlock->blockFirstRowNum = blockFirstRowNum;
	}

	executorReadBlock->headerOffsetInFile =
		AppendOnlyStorageRead_CurrentHeaderOffsetInFile(storageRead);

	/* Start curLargestAttnum from 1, this will be updated in AppendOnlyExecutorReadBlock_BindingInit() */
	executorReadBlock->curLargestAttnum = 1;

	/* mt_bind should be recreated for the new block */
	if (executorReadBlock->mt_bind)
	{
		destroy_memtuple_binding(executorReadBlock->mt_bind);
		executorReadBlock->mt_bind = NULL;
	}

	/* UNDONE: Check blockFirstRowNum */

	return true;
}

static void
AppendOnlyExecutionReadBlock_SetSegmentFileNum(AppendOnlyExecutorReadBlock *executorReadBlock,
											   int segmentFileNum)
{
	executorReadBlock->segmentFileNum = segmentFileNum;
}

static void
AppendOnlyExecutionReadBlock_SetPositionInfo(AppendOnlyExecutorReadBlock *executorReadBlock,
											 int64 blockFirstRowNum)
{
	executorReadBlock->blockFirstRowNum = blockFirstRowNum;
}

static void
AppendOnlyExecutionReadBlock_FinishedScanBlock(AppendOnlyExecutorReadBlock *executorReadBlock)
{
	executorReadBlock->blockFirstRowNum += executorReadBlock->rowCount;
}

/*
 * Initialize the ExecutorReadBlock once.  Assumed to be zeroed out before the call.
 */
static void
AppendOnlyExecutorReadBlock_Init(AppendOnlyExecutorReadBlock *executorReadBlock,
								 Relation relation,
								 MemoryContext memoryContext,
								 AppendOnlyStorageRead *storageRead,
								 int32 usableBlockSize)
{
	MemoryContext oldcontext;

	AssertArg(MemoryContextIsValid(memoryContext));

	oldcontext = MemoryContextSwitchTo(memoryContext);
	executorReadBlock->uncompressedBuffer = (uint8 *) palloc0(usableBlockSize * sizeof(uint8));

	executorReadBlock->storageRead = storageRead;
	executorReadBlock->memoryContext = memoryContext;

	Assert(relation); /* should have a valid relation */
	executorReadBlock->attnum_to_rownum = GetAttnumToLastrownumMapping(RelationGetRelid(relation),
												RelationGetNumberOfAttributes(relation));

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Free the space allocated inside ExexcutorReadBlock.
 */
static void
AppendOnlyExecutorReadBlock_Finish(AppendOnlyExecutorReadBlock *executorReadBlock)
{
	if (executorReadBlock->uncompressedBuffer)
	{
		pfree(executorReadBlock->uncompressedBuffer);
		executorReadBlock->uncompressedBuffer = NULL;
	}

	if (executorReadBlock->numericAtts)
	{
		pfree(executorReadBlock->numericAtts);
		executorReadBlock->numericAtts = NULL;
	}

	if (executorReadBlock->mt_bind)
	{
		pfree(executorReadBlock->mt_bind);
		executorReadBlock->mt_bind = NULL;
	}

	if (executorReadBlock->attnum_to_rownum)
	{
		pfree(executorReadBlock->attnum_to_rownum);
		executorReadBlock->attnum_to_rownum = NULL;
	}
}

static void
AppendOnlyExecutorReadBlock_ResetCounts(AppendOnlyExecutorReadBlock *executorReadBlock)
{
	executorReadBlock->totalRowsScanned = 0;
}

/*
 * Initialize the memtuple attribute bindings.
 *
 * Here, we figure out how many attributes are physically stored in the
 * memtuple based on the row number. Any row with a row number larger than
 * the pg_attribute_encoding.lastrownums number associated with the attribute
 * and current segno should have the attribute physically stored in memtuple.
 * For example, imagine we have this attnum-to-rownum mapping for segno=1:
 * 	(attnum=1, lastrownums=100)
 * 	(attnum=2, lastrownums=200)
 * 	(attnum=3, lastrownums=1000)
 * 	(attnum=4, lastrownums=2000)
 * And assume we are reading a memtuple with row number = 1500, we will know that
 * the first three attribute should be physically stored in the memtuple, but the
 * fourth attribute and onwards are not.
 *
 * So if lastrownum=0 for an attribute and segment pair, it effectively indicates
 * that all rows in the segment carry that attribute in the on-disk memtuple.
 *
 * Note that, the attnum_to_row array is first divided based on attribute numbers,
 * so the above mapping will be represented in attnum_to_row as (assume there's no
 * other segno being used):
 *     [
 *       0, 100, 0, ...(125 zeroes)...,       <-- for attnum=1
 *       0, 200, 0, ...(125 zeroes)...,       <-- for attnum=2
 *       0, 1000, 0, ...(125 zeroes)...,      <-- for attnum=3
 *       0, 2000, 0, ...(125 zeroes)...,      <-- for attnum=4
 *       0, ...(all zeroes)...
 *     ]
 */
static void
AppendOnlyExecutorReadBlock_BindingInit(AppendOnlyExecutorReadBlock *executorReadBlock,
									   TupleTableSlot *slot,
									   int64 rowNum)
{
	int segno = executorReadBlock->segmentFileNum;
	int largestAttnum = executorReadBlock->curLargestAttnum;
	MemoryContext oldContext;

	/* for any row to be read, there's at least one column data in the row */
	Assert(largestAttnum > 0);
	Assert(executorReadBlock->attnum_to_rownum != NULL);

	/* Find the number of attributes that are not missing in the row. */
	while (largestAttnum < slot->tts_tupleDescriptor->natts && 
			!AO_ATTR_VAL_IS_MISSING(rowNum, largestAttnum, segno, executorReadBlock->attnum_to_rownum))
		largestAttnum++;

	/*
	 * If we already created the bindings and also the largest attnum have not changed, 
	 * we do not need to recreate the bindings again.
	 */
	if (executorReadBlock->mt_bind && largestAttnum == executorReadBlock->curLargestAttnum)
		return;

	/* Otherwise, we have to create/recreate bindings */
	oldContext = MemoryContextSwitchTo(executorReadBlock->memoryContext);

	/* destroy the previous bindings */
	if (executorReadBlock->mt_bind)
		destroy_memtuple_binding(executorReadBlock->mt_bind);

	/*
	 * MemTupleBinding should be created from the slot's tuple descriptor
	 * (plus the expected largest attnum that we calculated above). We should
	 * not using the tuple descriptor in the relation which could be different
	 * in case like alter table rewrite.
	 */
	executorReadBlock->mt_bind = create_memtuple_binding(slot->tts_tupleDescriptor, largestAttnum);
	MemoryContextSwitchTo(oldContext);

	executorReadBlock->curLargestAttnum = largestAttnum;
}


static bool
AppendOnlyExecutorReadBlock_ProcessTuple(AppendOnlyExecutorReadBlock *executorReadBlock,
										 int64 rowNum,
										 MemTuple tuple,
										 int32 tupleLen,
										 int nkeys,
										 ScanKey key,
										 TupleTableSlot *slot)
{
	bool		valid = true;

	/* Assume for HeapKeyTestUsingSlot define. */
	ItemPointerData fake_ctid;
	AOTupleId  *aoTupleId = (AOTupleId *) &fake_ctid;
	int			formatVersion = executorReadBlock->storageRead->formatVersion;

	AOSegfileFormatVersion_CheckValid(formatVersion);

	AOTupleIdInit(aoTupleId, executorReadBlock->segmentFileNum, rowNum);

	/*
	 * Is it legal to call this function with NULL slot?  The
	 * HeapKeyTestUsingSlot call below assumes that the slot is not NULL.
	 */
	Assert (slot);

	AppendOnlyExecutorReadBlock_BindingInit(executorReadBlock, slot, rowNum);

	{
		Assert(executorReadBlock->mt_bind);

		ExecClearTuple(slot);
		memtuple_deform(tuple, executorReadBlock->mt_bind, slot->tts_values, slot->tts_isnull);
		slot->tts_tid = fake_ctid;
		ExecStoreVirtualTuple(slot);
	}

	/* skip visibility test, all tuples are visible */

	if (key != NULL)
		HeapKeyTestUsingSlot(slot, nkeys, key, valid);

	elogif(Debug_appendonly_print_scan_tuple && valid, LOG,
		   "Append-only scan tuple for table '%s' "
		   "(AOTupleId %s, tuple length %d, memtuple length %d, block offset in file " INT64_FORMAT ")",
		   AppendOnlyStorageRead_RelationName(executorReadBlock->storageRead),
		   AOTupleIdToString(aoTupleId),
		   tupleLen,
		   memtuple_get_size(tuple),
		   executorReadBlock->headerOffsetInFile);

	return valid;
}

static bool
AppendOnlyExecutorReadBlock_ScanNextTuple(AppendOnlyExecutorReadBlock *executorReadBlock,
										  int nkeys,
										  ScanKey key,
										  TupleTableSlot *slot)
{
	MemTuple	tuple;

	Assert(slot);

	switch (executorReadBlock->executorBlockKind)
	{
		case AoExecutorBlockKind_VarBlock:

			/*
			 * get the next item (tuple) from the varblock
			 */
			while (true)
			{
				int			itemLen = 0;
				uint8	   *itemPtr;
				int64		rowNum;

				itemPtr = VarBlockReaderGetNextItemPtr(
													   &executorReadBlock->varBlockReader,
													   &itemLen);

				if (itemPtr == NULL)
				{
					/* no more items in the varblock, get new buffer */
					AppendOnlyExecutionReadBlock_FinishedScanBlock(
																   executorReadBlock);
					return false;
				}

				executorReadBlock->currentItemCount++;

				executorReadBlock->totalRowsScanned++;

				if (itemLen > 0)
				{
					tuple = (MemTuple) itemPtr;

					rowNum = executorReadBlock->blockFirstRowNum +
						executorReadBlock->currentItemCount - INT64CONST(1);

					if (AppendOnlyExecutorReadBlock_ProcessTuple(
																 executorReadBlock,
																 rowNum,
																 tuple,
																 itemLen,
																 nkeys,
																 key,
																 slot))
						return true;
				}

			}

			/* varblock sanity check */
			if (executorReadBlock->readerItemCount !=
				executorReadBlock->currentItemCount)
				elog(NOTICE, "Varblock mismatch: Reader count %d, found %d items\n",
					 executorReadBlock->readerItemCount,
					 executorReadBlock->currentItemCount);
			break;

		case AoExecutorBlockKind_SingleRow:
			{
				int32		singleRowLen;

				if (executorReadBlock->singleRow == NULL)
				{
					AppendOnlyExecutionReadBlock_FinishedScanBlock(
																   executorReadBlock);
					return false;
					/* Force fetching new block. */
				}

				Assert(executorReadBlock->singleRowLen != 0);

				tuple = (MemTuple) executorReadBlock->singleRow;
				singleRowLen = executorReadBlock->singleRowLen;

				/*
				 * Indicated used up for scan.
				 */
				executorReadBlock->singleRow = NULL;
				executorReadBlock->singleRowLen = 0;

				executorReadBlock->totalRowsScanned++;

				if (AppendOnlyExecutorReadBlock_ProcessTuple(
															 executorReadBlock,
															 executorReadBlock->blockFirstRowNum,
															 tuple,
															 singleRowLen,
															 nkeys,
															 key,
															 slot))
					return true;
			}
			break;

		default:
			elog(ERROR, "Unrecognized append-only executor block kind: %d",
				 executorReadBlock->executorBlockKind);
			break;
	}

	AppendOnlyExecutionReadBlock_FinishedScanBlock(
												   executorReadBlock);
	return false;
	/* No match. */
}

static bool
AppendOnlyExecutorReadBlock_FetchTuple(AppendOnlyExecutorReadBlock *executorReadBlock,
									   int64 rowNum,
									   int nkeys,
									   ScanKey key,
									   TupleTableSlot *slot)
{
	MemTuple	tuple;
	int			itemNum;

	Assert(rowNum >= executorReadBlock->blockFirstRowNum);
	Assert(rowNum <=
		   executorReadBlock->blockFirstRowNum +
		   executorReadBlock->rowCount - 1);

	/*
	 * Get 0-based index to tuple.
	 */
	itemNum =
		(int) (rowNum - executorReadBlock->blockFirstRowNum);

	switch (executorReadBlock->executorBlockKind)
	{
		case AoExecutorBlockKind_VarBlock:
			{
				uint8	   *itemPtr;
				int			itemLen;

				itemPtr = VarBlockReaderGetItemPtr(
												   &executorReadBlock->varBlockReader,
												   itemNum,
												   &itemLen);
				Assert(itemPtr != NULL);

				tuple = (MemTuple) itemPtr;

				if (AppendOnlyExecutorReadBlock_ProcessTuple(
															 executorReadBlock,
															 rowNum,
															 tuple,
															 itemLen,
															 nkeys,
															 key,
															 slot))
					return true;
			}
			break;

		case AoExecutorBlockKind_SingleRow:
			{
				Assert(itemNum == 0);
				Assert(executorReadBlock->singleRow != NULL);
				Assert(executorReadBlock->singleRowLen != 0);

				tuple = (MemTuple) executorReadBlock->singleRow;

				if (AppendOnlyExecutorReadBlock_ProcessTuple(
															 executorReadBlock,
															 rowNum,
															 tuple,
															 executorReadBlock->singleRowLen,
															 nkeys,
															 key,
															 slot))
					return true;
			}
			break;

		default:
			elog(ERROR, "Unrecognized append-only executor block kind: %d",
				 executorReadBlock->executorBlockKind);
			break;
	}

	return false;
	/* No match. */
}

/* ------------------------------------------------------------------------------ */

/*
 * You can think of this scan routine as get next "executor" AO block.
 */
static bool
getNextBlock(AppendOnlyScanDesc scan)
{
	if (scan->aos_need_new_segfile)
	{
		/*
		 * Need to open a new segment file.
		 */
		if (!SetNextFileSegForRead(scan))
			return false;
	}

	if (!AppendOnlyExecutorReadBlock_GetBlockInfo(
												  &scan->storageRead,
												  &scan->executorReadBlock))
	{
		if (scan->blockDirectory)
		{
			AppendOnlyBlockDirectory_End_forInsert(scan->blockDirectory);
		}

		/* done reading the file */
		CloseScannedFileSeg(scan);

		return false;
	}

	if (scan->blockDirectory)
	{
		AppendOnlyBlockDirectory_InsertEntry(
			scan->blockDirectory, 0,
			scan->executorReadBlock.blockFirstRowNum,
			scan->executorReadBlock.headerOffsetInFile,
			scan->executorReadBlock.rowCount);
	}

	AppendOnlyExecutorReadBlock_GetContents(
											&scan->executorReadBlock);

	AppendOnlyScanDesc_UpdateTotalBytesRead(scan);
	pgstat_count_buffer_read_ao(scan->aos_rd,
								RelationGuessNumberOfBlocksFromSize(scan->totalBytesRead));

	return true;
}

static int
appendonly_locate_target_segment(AppendOnlyScanDesc scan, int64 targrow)
{
	int64 rowcount;

	for (int i = scan->aos_segfiles_processed - 1; i < scan->aos_total_segfiles; i++)
	{
		if (i < 0)
			continue;

		rowcount = scan->aos_segfile_arr[i]->total_tupcount;
		if (rowcount <= 0)
			continue;

		if (scan->segfirstrow + rowcount - 1 >= targrow)
		{
			/* found the target segment */
			return i;
		}

		/* continue next segment */
		scan->segfirstrow += rowcount;
		scan->segrowsprocessed = 0;
	}

	/* row is beyond the total number of rows in the relation */
	return -1;
}

/*
 * returns the segfile number in which `targrow` locates  
 */
static int
appendonly_getsegment(AppendOnlyScanDesc scan, int64 targrow)
{
	int segidx, segno;

	/* locate the target segment */
	segidx = appendonly_locate_target_segment(scan, targrow);
	if (segidx < 0)
	{
		CloseScannedFileSeg(scan);

		/* done reading all segfiles */
		Assert(scan->aos_done_all_segfiles);

		return -1;
	}

	if (segidx + 1 > scan->aos_segfiles_processed)
	{
		/* done current segfile */
		CloseScannedFileSeg(scan);
		/*
		 * Adjust aos_segfiles_processed to guide
		 * SetNextFileSegForRead() opening next
		 * right segfile.
		 */
		scan->aos_segfiles_processed = segidx;
	}

	segno = scan->aos_segfile_arr[segidx]->segno;
	Assert(segno > InvalidFileSegNumber && segno <= AOTupleId_MaxSegmentFileNum);

	if (scan->aos_need_new_segfile)
	{
		if (SetNextFileSegForRead(scan))
		{
			Assert(scan->segrowsprocessed == 0);
			scan->needNextBuffer = true;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unexpected behavior, failed to open segno %d during scanning AO table %s",
							segno, RelationGetRelationName(scan->aos_rd))));
		}
	}

	return segno;
}

static inline int64
appendonly_block_remaining_rows(AppendOnlyScanDesc scan)
{
	return (scan->executorReadBlock.rowCount - scan->executorReadBlock.blockRowsProcessed);
}

/*
 * locates the block in which `targrow` exists
 */
static void
appendonly_getblock(AppendOnlyScanDesc scan, int64 targrow, int64 *startrow)
{
	AppendOnlyExecutorReadBlock *varblock = &scan->executorReadBlock;
	int64 rowcount = InvalidAORowNum;

	if (!scan->needNextBuffer)
	{
		/* we have a current block */
		rowcount = appendonly_block_remaining_rows(scan);
		Assert(rowcount >= 0);

		if (*startrow + rowcount - 1 >= targrow)
		{
			/* row lies in current block, nothing to do */
			return;
		}
		else
		{
			/* skip scanning remaining rows */
			*startrow += rowcount;
			scan->needNextBuffer = true;
		}
	}

	/*
	 * Keep reading block headers until we find the block containing
	 * the target row.
	 */
	while (true)
	{
		elogif(Debug_appendonly_print_scan, LOG,
			   "appendonly_getblock(): [targrow: %ld, currow: %ld, diff: %ld, "
			   "startrow: %ld, rowcount: %ld, segfirstrow: %ld, segrowsprocessed: %ld, "
			   "blockRowsProcessed: %ld, blockRowCount: %d]", targrow, *startrow + rowcount - 1,
			   *startrow + rowcount - 1 - targrow, *startrow, rowcount, scan->segfirstrow,
			   scan->segrowsprocessed, varblock->blockRowsProcessed,
			   varblock->rowCount);

		if (AppendOnlyExecutorReadBlock_GetBlockInfo(&scan->storageRead, varblock))
		{
			/* new block, reset blockRowsProcessed */
			varblock->blockRowsProcessed = 0;
			rowcount = appendonly_block_remaining_rows(scan);
			Assert(rowcount > 0);
			if (*startrow + rowcount - 1 >= targrow)
			{
				int64 blocksRead;

				AppendOnlyExecutorReadBlock_GetContents(varblock);

				AppendOnlyScanDesc_UpdateTotalBytesRead(scan);
				blocksRead = RelationGuessNumberOfBlocksFromSize(scan->totalBytesRead);
				pgstat_count_buffer_read_ao(scan->aos_rd,
											blocksRead);

				/* got a new buffer to consume */
				scan->needNextBuffer = false;
				return;
			}

			*startrow += rowcount;
			AppendOnlyExecutionReadBlock_FinishedScanBlock(varblock);
			AppendOnlyStorageRead_SkipCurrentBlock(&scan->storageRead);
			/* continue next block */
		}
		else
			pg_unreachable(); /* unreachable code */
	}
}

/*
 * block directory based get_target_tuple()
 */
static bool
appendonly_blkdirscan_get_target_tuple(AppendOnlyScanDesc scan, int64 targrow, TupleTableSlot *slot)
{
	int segno, segidx;
	int64 rownum, rowsprocessed;
	AOTupleId aotid;
	AppendOnlyBlockDirectory *blkdir = &scan->aofetch->blockDirectory;

	Assert(scan->blkdirscan != NULL);

	/* locate the target segment */
	segidx = appendonly_locate_target_segment(scan, targrow);
	if (segidx < 0)
		return false;

	scan->aos_segfiles_processed = segidx + 1;

	segno = scan->aos_segfile_arr[segidx]->segno;
	Assert(segno > InvalidFileSegNumber && segno <= AOTupleId_MaxSegmentFileNum);

	/*
	 * Note: It is safe to assume that the scan's segfile array and the
	 * blockdir's segfile array are identical. Otherwise, we should stop
	 * processing and throw an exception to make the error visible.
	 */
	if (blkdir->segmentFileInfo[segidx]->segno != segno)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("segfile array contents in both scan descriptor "
				 		"and block directory are not identical on "
						"append-optimized relation '%s'",
						RelationGetRelationName(blkdir->aoRel))));
	}

	/* Set the current segfile info to the target one. */
	blkdir->currentSegmentFileNum = blkdir->segmentFileInfo[segidx]->segno;
	blkdir->currentSegmentFileInfo = blkdir->segmentFileInfo[segidx];

	/*
	 * "segfirstrow" should be always pointing to the first row of
	 * a new segfile in blkdir based ANALYZE, only locate_target_segment
	 * could update its value.
	 * 
	 * "segrowsprocessed" is used for tracking the position of
	 * processed rows in the current segfile.
	 */
	rowsprocessed = scan->segfirstrow + scan->segrowsprocessed;
	/* locate the target row by seqscan block directory */
	rownum = AOBlkDirScan_GetRowNum(scan->blkdirscan,
									segno,
									0,
									targrow,
									&rowsprocessed);

	elogif(Debug_appendonly_print_scan, LOG,
		   "AOBlkDirScan_GetRowNum(segno: %d, col: %d, targrow: %ld): "
		   "[segfirstrow: %ld, segrowsprocessed: %ld, rownum: %ld, cached_entry_no: %d]",
		   segno, 0, targrow, scan->segfirstrow, scan->segrowsprocessed, rownum,
		   blkdir->minipages[0].cached_entry_no);
	
	if (rownum < 0)
		return false;

	scan->segrowsprocessed = rowsprocessed - scan->segfirstrow;

	/* form the target tuple TID */
	AOTupleIdInit(&aotid, segno, rownum);

	/* ensure the target minipage entry was stored in fetch descriptor */
	Assert(scan->blkdirscan->mpentryno != InvalidEntryNum);
	Assert(blkdir->minipages == &blkdir->minipages[0]);

	/*
	 * Update cached_entry_no to the entry obtained from
	 * AOBlkDirScan_GetRowNum(), then we can reuse it directly
	 * during fetch below.
	 * See cached_entry_no in find_minipage_entry().
	 */
	blkdir->minipages[0].cached_entry_no = scan->blkdirscan->mpentryno;

	/* fetch the target tuple */
	if(!appendonly_fetch(scan->aofetch, &aotid, slot))
		return false;

	/* OK to return this tuple */
	pgstat_count_heap_fetch(scan->aos_rd);

	return true;
}

/*
 * Given a specific target row number 'targrow' (in the space of all row numbers
 * physically present in the table, i.e. across all segfiles), scan and return
 * the corresponding tuple in 'slot'.
 *
 * If the tuple is visible, return true. Otherwise, return false.
 *
 * Note: for the duration of the scan, we expect targrow to be monotonically
 * increasing in between successive calls.
 */
bool
appendonly_get_target_tuple(AppendOnlyScanDesc aoscan, int64 targrow, TupleTableSlot *slot)
{
	AppendOnlyExecutorReadBlock *varblock = &aoscan->executorReadBlock;
	bool visible;
	int64 rowsprocessed, rownum;
	int segno;
	AOTupleId aotid;

	if (aoscan->blkdirscan != NULL)
		return appendonly_blkdirscan_get_target_tuple(aoscan, targrow, slot);

	segno = appendonly_getsegment(aoscan, targrow);
	if (segno < 0)
		return false;

	rowsprocessed = aoscan->segfirstrow + aoscan->segrowsprocessed;

	appendonly_getblock(aoscan, targrow, &rowsprocessed);

	aoscan->segrowsprocessed = rowsprocessed - aoscan->segfirstrow;

	Assert(rowsprocessed + varblock->rowCount - 1 >= targrow);
	rownum = varblock->blockFirstRowNum + (targrow - rowsprocessed);

	elogif(Debug_appendonly_print_scan, LOG,
		   "appendonly_getblock() returns: [segno: %d, rownum: %ld]", segno, rownum);

	/* form the target tuple TID */
	AOTupleIdInit(&aotid, segno, rownum);

	visible = (aoscan->snapshot == SnapshotAny ||
			   AppendOnlyVisimap_IsVisible(&aoscan->visibilityMap, &aotid));

	if (visible && AppendOnlyExecutorReadBlock_FetchTuple(varblock, rownum, 0, NULL, slot))
	{
		/* OK to return this tuple */
		pgstat_count_heap_fetch(aoscan->aos_rd);
	}
	else
	{
		if (slot != NULL)
			ExecClearTuple(slot);
		
		return false;
	}

	return true;
}

/* ----------------
 *		appendonlygettup - fetch next appendonly tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple in forward direction; return the next tuple in scan->aos_ctup,
 *		or set scan->aos_ctup.t_data = NULL if no more tuples.
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 * ----------------
 */
static bool
appendonlygettup(AppendOnlyScanDesc scan,
				 ScanDirection dir pg_attribute_unused(),
				 int nkeys,
				 ScanKey key,
				 TupleTableSlot *slot)
{
	Assert(ScanDirectionIsForward(dir));
	/* should not be in ANALYZE/SampleScan - we use a different API */
	Assert((scan->rs_base.rs_flags & SO_TYPE_ANALYZE) == 0);
	Assert((scan->rs_base.rs_flags & SO_TYPE_SAMPLESCAN) == 0);
	Assert(scan->usableBlockSize > 0);

	bool		isSnapshotAny = (scan->snapshot == SnapshotAny);

	for (;;)
	{
		bool		found;

		if (scan->needNextBuffer)
		{
			/*
			 * Get the next block. We call this function until we successfully
			 * get a block to process, or finished reading all the data (all
			 * 'segment' files) for this relation.
			 */
			while (!getNextBlock(scan))
			{
				/* have we read all this relation's data. done! */
				if (scan->aos_done_all_segfiles)
					return false;
			}

			scan->needNextBuffer = false;
		}

		found = AppendOnlyExecutorReadBlock_ScanNextTuple(&scan->executorReadBlock,
														  nkeys,
														  key,
														  slot);
		if (found)
		{

			/*
			 * Need to get the Block Directory entry that covers the TID.
			 */
			AOTupleId  *aoTupleId = (AOTupleId *) &slot->tts_tid;

			if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&scan->visibilityMap, aoTupleId))
			{
				/* The tuple is invisible */
			}
			else
			{
				/* The tuple is visible */
				return true;
			}
		}
		else
		{
			/* no more items in the varblock, get new buffer */
			scan->needNextBuffer = true;
		}
	}
}

static void
cancelLastBuffer(AppendOnlyInsertDesc aoInsertDesc)
{
	if (aoInsertDesc->nonCompressedData != NULL)
	{
		Assert(AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));
		AppendOnlyStorageWrite_CancelLastBuffer(&aoInsertDesc->storageWrite);
		aoInsertDesc->nonCompressedData = NULL;
	}
	else
		Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));
}

static void
setupNextWriteBlock(AppendOnlyInsertDesc aoInsertDesc)
{
	Assert(aoInsertDesc->nonCompressedData == NULL);
	Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

	/* Set the firstRowNum for the block */
	aoInsertDesc->blockFirstRowNum = aoInsertDesc->lastSequence + 1;
	AppendOnlyStorageWrite_SetFirstRowNum(&aoInsertDesc->storageWrite,
										  aoInsertDesc->blockFirstRowNum);

	aoInsertDesc->nonCompressedData =
		AppendOnlyStorageWrite_GetBuffer(
											&aoInsertDesc->storageWrite,
											AoHeaderKind_SmallContent);

	/*
		* Prepare our VarBlock for items.  Leave room for the Append-Only
		* Storage header.
		*/
	VarBlockMakerInit(&aoInsertDesc->varBlockMaker,
						aoInsertDesc->nonCompressedData,
						aoInsertDesc->maxDataLen,
						aoInsertDesc->tempSpace,
						aoInsertDesc->tempSpaceLen);

	aoInsertDesc->bufferCount++;
}

static void
finishWriteBlock(AppendOnlyInsertDesc aoInsertDesc)
{
	int			executorBlockKind;
	int			itemCount;
	int32		dataLen;

	executorBlockKind = AoExecutorBlockKind_VarBlock;
	/* Assume. */

	itemCount = VarBlockMakerItemCount(&aoInsertDesc->varBlockMaker);
	if (itemCount == 0)
	{
		/*
		 * "Cancel" the last block allocation, if one.
		 */
		cancelLastBuffer(aoInsertDesc);
		return;
	}

	dataLen = VarBlockMakerFinish(&aoInsertDesc->varBlockMaker);

	aoInsertDesc->varblockCount++;

	if (itemCount == 1)
	{
		dataLen = VarBlockCollapseToSingleItem(
												/* target */ aoInsertDesc->nonCompressedData,
												/* source */ aoInsertDesc->nonCompressedData,
												/* sourceLen */ dataLen);
		executorBlockKind = AoExecutorBlockKind_SingleRow;
	}

	aoInsertDesc->storageWrite.logicalBlockStartOffset =
		BufferedAppendNextBufferPosition(&(aoInsertDesc->storageWrite.bufferedAppend));

	AppendOnlyStorageWrite_FinishBuffer(
										&aoInsertDesc->storageWrite,
										dataLen,
										executorBlockKind,
										itemCount);
	aoInsertDesc->nonCompressedData = NULL;
	Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

	elogif(Debug_appendonly_print_insert, LOG,
			"Append-only insert finished uncompressed block for table '%s' "
			"(length = %d, application specific %d, item count %d, block count " INT64_FORMAT ")",
			NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
			dataLen,
			executorBlockKind,
			itemCount,
			aoInsertDesc->bufferCount);

	/* Insert an entry to the block directory */
	AppendOnlyBlockDirectory_InsertEntry(
		&aoInsertDesc->blockDirectory,
		0,
		aoInsertDesc->blockFirstRowNum,
		AppendOnlyStorageWrite_LogicalBlockStartOffset(&aoInsertDesc->storageWrite),
		itemCount);

	Assert(aoInsertDesc->nonCompressedData == NULL);
	Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));
}

static void
appendonly_blkdirscan_init(AppendOnlyScanDesc scan)
{
	if (scan->aofetch == NULL)
		scan->aofetch = appendonly_fetch_init(scan->aos_rd,
											  scan->snapshot,
											  scan->appendOnlyMetaDataSnapshot);

	scan->blkdirscan = palloc0(sizeof(AOBlkDirScanData));
	AOBlkDirScan_Init(scan->blkdirscan, &scan->aofetch->blockDirectory);
}

static void
appendonly_blkdirscan_finish(AppendOnlyScanDesc scan)
{
	AOBlkDirScan_Finish(scan->blkdirscan);
	pfree(scan->blkdirscan);
	scan->blkdirscan = NULL;

	if (scan->aofetch != NULL)
	{
		appendonly_fetch_finish(scan->aofetch);
		pfree(scan->aofetch);
		scan->aofetch = NULL;
	}
}


/* ----------------------------------------------------------------
 *					 append-only access method interface
 * ----------------------------------------------------------------
 */

/*
 * appendonly_beginrangescan_internal()
 *
 * Begins a scan over a subset of segment info files.
 *
 * Should only be called with valid seginfos for the given relation.
 * Should only be called with an aoentry based on the same snapshot.
 *
 * The ownership of the seginfos and aoentry are transferred to the scan descriptor.
 */
static AppendOnlyScanDesc
appendonly_beginrangescan_internal(Relation relation,
								   Snapshot snapshot,
								   Snapshot appendOnlyMetaDataSnapshot,
								   FileSegInfo **seginfo,
								   int segfile_count,
								   int nkeys,
								   ScanKey key,
								   ParallelTableScanDesc parallel_scan,
								   uint32 flags)
{
	AppendOnlyScanDesc scan;
	AppendOnlyStorageAttributes *attr;
	StringInfoData titleBuf;
	bool		checksum = true;
	int32		blocksize = -1;
	int16		compresslevel = 0;
	NameData	compresstype;

	GetAppendOnlyEntryAttributes(relation->rd_id, &blocksize, &compresslevel, &checksum, &compresstype);

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate scan descriptor
	 */
	scan = (AppendOnlyScanDesc) palloc0(sizeof(AppendOnlyScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	scan->aos_filenamepath_maxlen = AOSegmentFilePathNameLen(relation) + 1;
	scan->aos_filenamepath = (char *) palloc(scan->aos_filenamepath_maxlen);
	scan->aos_filenamepath[0] = '\0';
	scan->usableBlockSize = blocksize;
	scan->aos_rd = relation;
	scan->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	scan->snapshot = snapshot;
	scan->aos_nkeys = nkeys;
	scan->aoScanInitContext = CurrentMemoryContext;

	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Scan of Append-Only Row-Oriented relation '%s'",
					 RelationGetRelationName(relation));
	scan->title = titleBuf.data;


	/*
	 * Fill in Append-Only Storage layer attributes.
	 */
	attr = &scan->storageAttributes;

	/*
	 * These attributes describe the AppendOnly format to be scanned.
	 */
	if (strcmp(NameStr(compresstype), "") == 0 ||
		pg_strcasecmp(NameStr(compresstype), "none") == 0)
	{
		attr->compress = false;
		attr->compressType = "none";
	}
	else
	{
		attr->compress = true;
		attr->compressType = pstrdup(NameStr(compresstype));
	}
	attr->compressLevel = compresslevel;
	attr->checksum = checksum;

	/* UNDONE: We are calling the static header length routine here. */
	scan->maxDataLen =
		scan->usableBlockSize -
		AppendOnlyStorageFormat_RegularHeaderLenNeeded(scan->storageAttributes.checksum);


	/*
	 * Get information about all the file segments we need to scan
	 */
	scan->aos_segfile_arr = seginfo;

	scan->aos_total_segfiles = segfile_count;

	/*
	 * we do this here instead of in initscan() because appendonly_rescan also
	 * calls initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->aos_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->aos_key = NULL;

	/* pgstat_initstats(relation); */
	initscan(scan, key);

	scan->blockDirectory = NULL;

	if ((flags & SO_TYPE_ANALYZE) != 0 || (flags & SO_TYPE_SAMPLESCAN) != 0)
	{
		scan->segrowsprocessed = 0;
		scan->segfirstrow = 0;
		scan->targrow = 0;
	}

	scan->blkdirscan = NULL;

	if (segfile_count > 0)
	{
		Oid			visimaprelid;
		Oid			blkdirrelid;

		GetAppendOnlyEntryAuxOids(relation,
								  NULL,
								  &blkdirrelid,
								  &visimaprelid);

		AppendOnlyVisimap_Init(&scan->visibilityMap,
							   visimaprelid,
							   AccessShareLock,
							   appendOnlyMetaDataSnapshot);

		/*
		 * Initialize a AOBlkdirScan only if we are doing sampling and if we
		 * have a blkdir relation.
		 */
		if ((flags & SO_TYPE_ANALYZE) != 0 || (flags & SO_TYPE_SAMPLESCAN) != 0)
		{
			if (OidIsValid(blkdirrelid) && gp_enable_blkdir_sampling)
				appendonly_blkdirscan_init(scan);
		}
	}

	scan->totalBytesRead = 0;

	scan->sampleTargetBlk = -1;

	return scan;
}

/*
 * appendonly_beginrangescan
 *
 * begins range-limited relation scan
 */
AppendOnlyScanDesc
appendonly_beginrangescan(Relation relation,
						  Snapshot snapshot,
						  Snapshot appendOnlyMetaDataSnapshot,
						  int *segfile_no_arr, int segfile_count,
						  int nkeys, ScanKey keys)
{
	/*
	 * Get the pg_appendonly information for this table
	 */

	FileSegInfo **seginfo = palloc0(sizeof(FileSegInfo *) * segfile_count);
	int			i;

	for (i = 0; i < segfile_count; i++)
	{
		seginfo[i] = GetFileSegInfo(relation, appendOnlyMetaDataSnapshot,
									segfile_no_arr[i], false);
	}
	return appendonly_beginrangescan_internal(relation,
											  snapshot,
											  appendOnlyMetaDataSnapshot,
											  seginfo,
											  segfile_count,
											  nkeys,
											  keys,
											  NULL,
											  0);
}

/* ----------------
 *		appendonly_beginscan	- begin relation scan
 * ----------------
 */
TableScanDesc
appendonly_beginscan(Relation relation,
					 Snapshot snapshot,
					 int nkeys, struct ScanKeyData *key,
					 ParallelTableScanDesc pscan,
					 uint32 flags)
{
	Snapshot	appendOnlyMetaDataSnapshot;
	int			segfile_count;
	FileSegInfo **seginfo;
	AppendOnlyScanDesc aoscan;

	appendOnlyMetaDataSnapshot = snapshot;
	if (appendOnlyMetaDataSnapshot == SnapshotAny)
	{
		/*
		 * The append-only meta data should never be fetched with
		 * SnapshotAny as bogus results are returned.
		 * We use SnapshotSelf for metadata, as regular MVCC snapshot can hide
		 * newly globally inserted tuples from global index build process.
		 */
		appendOnlyMetaDataSnapshot = SnapshotSelf;
	}

	/*
	 * Get the pg_appendonly information for this table
	 */
	seginfo = GetAllFileSegInfo(relation,
								appendOnlyMetaDataSnapshot, &segfile_count, NULL);
	
	aoscan = appendonly_beginrangescan_internal(relation,
												snapshot,
												appendOnlyMetaDataSnapshot,
												seginfo,
												segfile_count,
												nkeys,
												key,
												pscan,
												flags);

	return (TableScanDesc) aoscan;
}

/* ----------------
 *		appendonly_rescan		- restart a relation scan
 *
 *
 * TODO: instead of freeing resources here and reallocating them in initscan
 * over and over see which of them can be refactored into appendonly_beginscan
 * and persist there until endscan is finally reached. For now this will do.
 *
 * GPDB_12_MERGE_FEATURE_NOT_SUPPORTED: When doing an initial rescan with `table_rescan`,
 * the values for the new flags (introduced by Table AM API) are
 * set to false. This means that whichever ScanOptions flags that were initially set will be
 * used for the rescan. However with TABLESAMPLE, the new flags may be modified.
 * Additionally, allow_sync, allow_strat, and allow_pagemode may
 * need to be implemented for AO/CO in order to properly use them.
 * You may view `syncscan.c` as an example to see how heap added scan
 * synchronization support.
 * ----------------
 */
void
appendonly_rescan(TableScanDesc scan, ScanKey key,
				  bool set_params, bool allow_strat,
				  bool allow_sync, bool allow_pagemode)
{
	AppendOnlyScanDesc aoscan = (AppendOnlyScanDesc) scan;

	CloseScannedFileSeg(aoscan);

	AppendOnlyStorageRead_FinishSession(&aoscan->storageRead);

	aoscan->initedStorageRoutines = false;

	AppendOnlyExecutorReadBlock_Finish(&aoscan->executorReadBlock);

	aoscan->aos_need_new_segfile = true;

	/*
	 * reinitialize scan descriptor
	 */
	initscan(aoscan, key);

	/* TABLESAMPLE related state */
	aoscan->segrowsprocessed = 0;
	aoscan->segfirstrow = 0;
	aoscan->targrow = 0;
	aoscan->sampleTargetBlk = -1;
	if (aoscan->blkdirscan)
	{
		appendonly_blkdirscan_finish(aoscan);
		appendonly_blkdirscan_init(aoscan);
	}
}

/*
 * Position an AO scan to start from a segno specified by the 'fsInfoIdx' in
 * the scan's segfile array, and offset specified by blkdir entry 'dirEntry'.
 *
 * If we are unable to position the scan, we return false.
 */
bool
appendonly_positionscan(AppendOnlyScanDesc aoscan,
						AppendOnlyBlockDirectoryEntry *dirEntry,
						int fsInfoIdx)
{
	int64 	beginFileOffset = dirEntry->range.fileOffset;
	int64 	afterFileOffset = dirEntry->range.afterFileOffset;

	Assert(dirEntry);

	if (!SetSegFileForRead(aoscan, fsInfoIdx))
	{
		/* target segment is empty/awaiting-drop */
		return false;
	}

	if (beginFileOffset > aoscan->storageRead.logicalEof)
	{
		/* position maps to a hole at the end of the segfile */
		return false;
	}

	AppendOnlyStorageRead_SetTemporaryStart(&aoscan->storageRead,
											beginFileOffset,
											afterFileOffset);

	return true;
}

/* ----------------
 *		appendonly_endscan	- end relation scan
 * ----------------
 */
void
appendonly_endscan(TableScanDesc scan)
{
	AppendOnlyScanDesc aoscan = (AppendOnlyScanDesc) scan;

	RelationDecrementReferenceCount(aoscan->aos_rd);

	if (aoscan->aos_key)
		pfree(aoscan->aos_key);

	if (aoscan->aos_segfile_arr)
	{
		for (int seginfo_no = 0; seginfo_no < aoscan->aos_total_segfiles; seginfo_no++)
		{
			pfree(aoscan->aos_segfile_arr[seginfo_no]);
		}

		pfree(aoscan->aos_segfile_arr);
	}

	CloseScannedFileSeg(aoscan);

	AppendOnlyStorageRead_FinishSession(&aoscan->storageRead);

	aoscan->initedStorageRoutines = false;

	AppendOnlyExecutorReadBlock_Finish(&aoscan->executorReadBlock);

	if (aoscan->aos_total_segfiles > 0)
		AppendOnlyVisimap_Finish(&aoscan->visibilityMap, AccessShareLock);

	if (aoscan->blkdirscan != NULL)
		appendonly_blkdirscan_finish(aoscan);

	if (aoscan->aofetch)
	{
		appendonly_fetch_finish(aoscan->aofetch);
		pfree(aoscan->aofetch);
		aoscan->aofetch = NULL;
	}

	pfree(aoscan->aos_filenamepath);

	pfree(aoscan->title);

	pfree(aoscan);
}

/* ----------------
 *		appendonly_getnextslot - retrieve next tuple in scan
 * ----------------
 */
bool
appendonly_getnextslot(TableScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	AppendOnlyScanDesc aoscan = (AppendOnlyScanDesc) scan;

	if (appendonlygettup(aoscan, direction, aoscan->rs_base.rs_nkeys, aoscan->aos_key, slot))
	{
		pgstat_count_heap_getnext(aoscan->aos_rd);

		return true;
	}
	else
	{
		if (slot)
			ExecClearTuple(slot);

		return false;
	}
}

static void
closeFetchSegmentFile(AppendOnlyFetchDesc aoFetchDesc)
{
	Assert(aoFetchDesc->currentSegmentFile.isOpen);

	AppendOnlyStorageRead_CloseFile(&aoFetchDesc->storageRead);

	aoFetchDesc->currentSegmentFile.isOpen = false;
}

static bool
openFetchSegmentFile(AppendOnlyFetchDesc aoFetchDesc,
					 int openSegmentFileNum)
{
	int			i;

	FileSegInfo *fsInfo;
	int			segmentFileNum;
	int64		logicalEof;
	int32		fileSegNo;

	Assert(!aoFetchDesc->currentSegmentFile.isOpen);

	i = 0;
	while (true)
	{
		if (i >= aoFetchDesc->totalSegfiles)
			return false;
		/* Segment file not visible in catalog information. */

		fsInfo = aoFetchDesc->segmentFileInfo[i];
		segmentFileNum = fsInfo->segno;
		if (openSegmentFileNum == segmentFileNum)
		{
			if (fsInfo->state == AOSEG_STATE_AWAITING_DROP)
			{
				/*
				 * File compacted, but not dropped. All information are
				 * declared invisible
				 */
				return false;
			}
			logicalEof = (int64) fsInfo->eof;
			break;
		}
		i++;
	}

	/*
	 * Don't try to open a segment file when its EOF is 0, since the file may
	 * not exist. See MPP-8280.
	 */
	if (logicalEof == 0)
		return false;

	MakeAOSegmentFileName(
						  aoFetchDesc->relation,
						  openSegmentFileNum, InvalidFileNumber,
						  &fileSegNo,
						  aoFetchDesc->segmentFileName);
	Assert(strlen(aoFetchDesc->segmentFileName) + 1 <=
		   aoFetchDesc->segmentFileNameMaxLen);

	/* UNDONE: Appropriate to use Try here? */
	if (!AppendOnlyStorageRead_TryOpenFile(
										   &aoFetchDesc->storageRead,
										   aoFetchDesc->segmentFileName,
										   fsInfo->formatversion,
										   logicalEof))
		return false;

	aoFetchDesc->currentSegmentFile.num = openSegmentFileNum;
	aoFetchDesc->currentSegmentFile.logicalEof = logicalEof;

	aoFetchDesc->currentSegmentFile.isOpen = true;

	return true;
}

static bool
fetchNextBlock(AppendOnlyFetchDesc aoFetchDesc)
{
	AppendOnlyExecutorReadBlock *executorReadBlock =
	&aoFetchDesc->executorReadBlock;

	/*
	 * Try to read next block.
	 */
	if (!AppendOnlyExecutorReadBlock_GetBlockInfo(
												  &aoFetchDesc->storageRead,
												  &aoFetchDesc->executorReadBlock))
		return false;
	/* Hit end of range. */

	/*
	 * Unpack information into member variables.
	 */
	aoFetchDesc->currentBlock.valid = true;
	aoFetchDesc->currentBlock.fileOffset =
		executorReadBlock->headerOffsetInFile;
	aoFetchDesc->currentBlock.overallBlockLen =
		AppendOnlyStorageRead_OverallBlockLen(
											  &aoFetchDesc->storageRead);
	aoFetchDesc->currentBlock.firstRowNum =
		executorReadBlock->blockFirstRowNum;
	aoFetchDesc->currentBlock.lastRowNum =
		executorReadBlock->blockFirstRowNum +
		executorReadBlock->rowCount - 1;

	aoFetchDesc->currentBlock.gotContents = false;

	return true;
}

/*
 * Fetch the tuple from the block indicated by the block directory entry that
 * covers the tuple.
 */
static void
fetchFromCurrentBlock(AppendOnlyFetchDesc aoFetchDesc,
					  int64 rowNum,
					  TupleTableSlot *slot)
{
	bool							fetched;
	AOFetchBlockMetadata 			*currentBlock = &aoFetchDesc->currentBlock;
	AppendOnlyExecutorReadBlock 	*executorReadBlock = &aoFetchDesc->executorReadBlock;
	AppendOnlyBlockDirectoryEntry 	*entry = &currentBlock->blockDirectoryEntry;

	if (!currentBlock->gotContents)
	{
		/*
		 * Do decompression if necessary and get contents.
		 */
		AppendOnlyExecutorReadBlock_GetContents(executorReadBlock);

		currentBlock->gotContents = true;
	}

	fetched = AppendOnlyExecutorReadBlock_FetchTuple(executorReadBlock,
													 rowNum,
													 /* nkeys */ 0,
													 /* key */ NULL,
													 slot);
	if (!fetched)
	{
		if (AppendOnlyBlockDirectoryEntry_RangeHasRow(entry, rowNum))
		{
			/*
			 * We fell into a hole inside the resolved block directory entry
			 * we obtained from AppendOnlyBlockDirectory_GetEntry().
			 * This should not be happening for versions >= GP7. Scream
			 * appropriately. See AppendOnlyBlockDirectoryEntry for details.
			 */
			ereportif(aoFetchDesc->relation->rd_appendonly->version >= AORelationVersion_GP7,
					  ERROR,
					  (errcode(ERRCODE_INTERNAL_ERROR),
						  errmsg("tuple with row number %ld not found in block directory entry range", rowNum),
						  errdetail("block directory entry: (fileOffset = %ld, firstRowNum = %ld, "
									"afterFileOffset = %ld, lastRowNum = %ld)",
									entry->range.fileOffset,
									entry->range.firstRowNum,
									entry->range.afterFileOffset,
									entry->range.lastRowNum)));
		}
		else
		{
			/*
			 * The resolved block directory entry we obtained from
			 * AppendOnlyBlockDirectory_GetEntry() has range s.t.
			 * firstRowNum < lastRowNum < rowNum
			 * This can happen when rowNum maps to an aborted transaction, and
			 * we find an earlier committed block directory row due to the
			 * <= scan condition in AppendOnlyBlockDirectory_GetEntry().
			 */
		}
	}
}

static void
positionFirstBlockOfRange(AppendOnlyFetchDesc aoFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetBeginRange(&aoFetchDesc->currentBlock.blockDirectoryEntry,
												&aoFetchDesc->scanNextFileOffset,
												&aoFetchDesc->scanNextRowNum);
}

static void
positionLimitToEndOfRange(AppendOnlyFetchDesc aoFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetEndRange(&aoFetchDesc->currentBlock.blockDirectoryEntry,
											  &aoFetchDesc->scanAfterFileOffset,
											  &aoFetchDesc->scanLastRowNum);
}


static void
positionSkipCurrentBlock(AppendOnlyFetchDesc aoFetchDesc)
{
	aoFetchDesc->scanNextFileOffset =
		aoFetchDesc->currentBlock.fileOffset +
		aoFetchDesc->currentBlock.overallBlockLen;

	aoFetchDesc->scanNextRowNum = aoFetchDesc->currentBlock.lastRowNum + 1;
}

/*
 * Scan through blocks to find row.
 *
 * If row is not represented in any of the blocks covered by the Block Directory, then the row
 * falls into a row gap.  The row must have been aborted or deleted and reclaimed.
 */
static bool
scanToFetchTuple(AppendOnlyFetchDesc aoFetchDesc,
				 int64 rowNum,
				 TupleTableSlot *slot)
{
	if (aoFetchDesc->scanNextFileOffset >=
		aoFetchDesc->scanAfterFileOffset)
		return false;
	/* No more blocks requested for range. */

	if (aoFetchDesc->currentSegmentFile.logicalEof ==
		aoFetchDesc->scanNextFileOffset)
		return false;
	/* No more blocks in this file. */

	if (aoFetchDesc->currentSegmentFile.logicalEof <
		aoFetchDesc->scanNextFileOffset)
		return false;
/* UNDONE:Why does our next scan position go beyond logical EOF ? */

	/*
	 * Temporarily restrict our reading to just the range.
	 */
	AppendOnlyStorageRead_SetTemporaryRange(
											&aoFetchDesc->storageRead,
											aoFetchDesc->scanNextFileOffset,
											aoFetchDesc->scanAfterFileOffset);
	AppendOnlyExecutionReadBlock_SetSegmentFileNum(
												   &aoFetchDesc->executorReadBlock,
												   aoFetchDesc->currentSegmentFile.num);
	AppendOnlyExecutionReadBlock_SetPositionInfo(
												 &aoFetchDesc->executorReadBlock,
												 aoFetchDesc->scanNextRowNum);

	aoFetchDesc->skipBlockCount = 0;
	while (true)
	{
		/*
		 * Fetch block starting at scanNextFileOffset.
		 */
		if (!fetchNextBlock(aoFetchDesc))
			return false;
		/* No more blocks. */

		/*
		 * Examine new current block header information.
		 */
		if (rowNum < aoFetchDesc->currentBlock.firstRowNum)
		{
			/*
			 * Since we have read a new block, the temporary range for the
			 * read needs to be adjusted accordingly. Otherwise, the
			 * underlying bufferedRead may stop reading more data because of
			 * the previously-set smaller temporary range.
			 */
			int64		beginFileOffset = aoFetchDesc->currentBlock.fileOffset;
			int64		afterFileOffset = aoFetchDesc->currentBlock.fileOffset +
			aoFetchDesc->currentBlock.overallBlockLen;

			AppendOnlyStorageRead_SetTemporaryRange(
													&aoFetchDesc->storageRead,
													beginFileOffset,
													afterFileOffset);

			return false;
			/* Row fell in gap between blocks. */
		}

		if (rowNum <= aoFetchDesc->currentBlock.lastRowNum)
		{
			fetchFromCurrentBlock(aoFetchDesc, rowNum, slot);
			return true;
		}

		/*
		 * Update information to get next block.
		 */
		Assert(!aoFetchDesc->currentBlock.gotContents);

		/* MPP-17061: reach the end of range covered by block directory entry */
		if ((aoFetchDesc->currentBlock.fileOffset +
			 aoFetchDesc->currentBlock.overallBlockLen) >=
			aoFetchDesc->scanAfterFileOffset)
		{
			return false;
		}

		AppendOnlyExecutionReadBlock_FinishedScanBlock(
													   &aoFetchDesc->executorReadBlock);

		AppendOnlyStorageRead_SkipCurrentBlock(
											   &aoFetchDesc->storageRead);
		aoFetchDesc->skipBlockCount++;
	}
}

static void
resetCurrentBlockInfo(AOFetchBlockMetadata * currentBlock)
{
	currentBlock->valid = false;
	currentBlock->firstRowNum = 0;
	currentBlock->lastRowNum = 0;
}

AppendOnlyFetchDesc
appendonly_fetch_init(Relation relation,
					  Snapshot snapshot,
					  Snapshot appendOnlyMetaDataSnapshot)
{
	AppendOnlyFetchDesc				aoFetchDesc;
	AppendOnlyStorageAttributes	   *attr;
	PGFunction					   *fns;
	StringInfoData					titleBuf;
	Oid 					segrelid;
	Oid 					visimaprelid;
	int								segno;

	GetAppendOnlyEntryAuxOids(relation, &segrelid, NULL, &visimaprelid);

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate scan descriptor
	 */
	aoFetchDesc = (AppendOnlyFetchDesc) palloc0(sizeof(AppendOnlyFetchDescData));

	aoFetchDesc->relation = relation;
	aoFetchDesc->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	aoFetchDesc->snapshot = snapshot;

	aoFetchDesc->initContext = CurrentMemoryContext;

	aoFetchDesc->segmentFileNameMaxLen = AOSegmentFilePathNameLen(relation) + 1;
	aoFetchDesc->segmentFileName =
		(char *) palloc(aoFetchDesc->segmentFileNameMaxLen);
	aoFetchDesc->segmentFileName[0] = '\0';

	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Fetch of Append-Only Row-Oriented relation '%s'",
					 RelationGetRelationName(relation));
	aoFetchDesc->title = titleBuf.data;

	bool		checksum = true;
	int32		blocksize = -1;
	int16		compresslevel = 0;
	NameData	compresstype;

	GetAppendOnlyEntryAttributes(relation->rd_id, &blocksize, &compresslevel, &checksum, &compresstype);
	/*
	 * Fill in Append-Only Storage layer attributes.
	 */
	attr = &aoFetchDesc->storageAttributes;

	/*
	 * These attributes describe the AppendOnly format to be scanned.
	 */
	if (strcmp(NameStr(compresstype), "") == 0 ||
		pg_strcasecmp(NameStr(compresstype), "none") == 0)
	{
		attr->compress = false;
		attr->compressType = "none";
	}
	else
	{
		attr->compress = true;
		attr->compressType = pstrdup(NameStr(compresstype));
	}

	attr->compressLevel = compresslevel;
	attr->checksum = checksum;
	aoFetchDesc->usableBlockSize = blocksize;

	/*
	 * Get information about all the file segments we need to scan
	 */
	aoFetchDesc->segmentFileInfo =
		GetAllFileSegInfo(
						  relation,
						  appendOnlyMetaDataSnapshot,
						  &aoFetchDesc->totalSegfiles,
						  NULL);

	/* 
	 * Initialize lastSequence only for segments which we got above is sufficient,
	 * rather than all AOTupleId_MultiplierSegmentFileNum ones that introducing
	 * too many unnecessary calls in most cases.
	 */
	memset(aoFetchDesc->lastSequence, InvalidAORowNum, sizeof(aoFetchDesc->lastSequence));
	for (int i = -1; i < aoFetchDesc->totalSegfiles; i++)
	{
		/* always initailize segment 0 */
		segno = (i < 0 ? 0 : aoFetchDesc->segmentFileInfo[i]->segno);
		/* set corresponding bit for target segment */
		aoFetchDesc->lastSequence[segno] = ReadLastSequence(segrelid, segno);
	}

	AppendOnlyStorageRead_Init(
							   &aoFetchDesc->storageRead,
							   aoFetchDesc->initContext,
							   aoFetchDesc->usableBlockSize,
							   NameStr(aoFetchDesc->relation->rd_rel->relname),
							   aoFetchDesc->title,
							   &aoFetchDesc->storageAttributes);


	fns = get_funcs_for_compression(attr->compressType);
	aoFetchDesc->storageRead.compression_functions = fns;

	if (fns)
	{
		PGFunction	cons = fns[COMPRESSION_CONSTRUCTOR];
		CompressionState *cs;
		StorageAttributes sa;

		sa.comptype = attr->compressType;
		sa.complevel = attr->compressLevel;
		sa.blocksize = aoFetchDesc->usableBlockSize;


		cs = callCompressionConstructor(cons, RelationGetDescr(relation),
										&sa,
										false /* decompress */ );
		aoFetchDesc->storageRead.compressionState = cs;
	}

	AppendOnlyExecutorReadBlock_Init(
									 &aoFetchDesc->executorReadBlock,
									 aoFetchDesc->relation,
									 aoFetchDesc->initContext,
									 &aoFetchDesc->storageRead,
									 aoFetchDesc->usableBlockSize);

	AppendOnlyBlockDirectory_Init_forSearch(
											&aoFetchDesc->blockDirectory,
											appendOnlyMetaDataSnapshot,
											aoFetchDesc->segmentFileInfo,
											aoFetchDesc->totalSegfiles,
											aoFetchDesc->relation,
											1,
											false,
											NULL);

	AppendOnlyVisimap_Init(&aoFetchDesc->visibilityMap,
						   visimaprelid,
						   AccessShareLock,
						   appendOnlyMetaDataSnapshot);

	return aoFetchDesc;

}

/*
 * appendonly_fetch -- fetch the tuple for a given tid.
 *
 * If the 'slot' is not NULL, the fetched tuple will be assigned to the slot.
 *
 * Return true if such a tuple is found. Otherwise, return false.
 */
bool
appendonly_fetch(AppendOnlyFetchDesc aoFetchDesc,
				 AOTupleId *aoTupleId,
				 TupleTableSlot *slot)
{
	int			segmentFileNum = AOTupleIdGet_segmentFileNum(aoTupleId);
	int64		rowNum = AOTupleIdGet_rowNum(aoTupleId);
	bool		isSnapshotAny = (aoFetchDesc->snapshot == SnapshotAny);

	Assert(segmentFileNum >= 0);

	if (aoFetchDesc->lastSequence[segmentFileNum] == InvalidAORowNum)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Row No. %ld in segment file No. %d is out of scanning scope for target relfilenode %u.",
				 		rowNum, segmentFileNum, aoFetchDesc->relation->rd_node.relNode)));

	/*
	 * This is an improvement for brin. BRIN index stores ranges of TIDs in
	 * terms of block numbers and not specific TIDs, so it's possible that the
	 * fetch function is called with a non-existent TID. The function
	 * appendonly_fetch will access the block directory table first and cache
	 * some MinipageEntrys. If we try to access the non-existent tid, a cache
	 * miss will occur. And we need to search the btree on block directory
	 * table. This is a vary slow operation. So a fast return path was added
	 * here. If the rowNum is bigger than lastsequence, skip it.
	 */
	if (rowNum > aoFetchDesc->lastSequence[segmentFileNum])
	{
		if (slot != NULL)
		{
			ExecClearTuple(slot);
		}
		return false;	/* row has been deleted or updated. */
	}

	/*
	 * Do we have a current block?  If it has the requested tuple, that would
	 * be a great performance optimization.
	 */
	if (aoFetchDesc->currentBlock.valid)
	{
		if (aoFetchDesc->currentSegmentFile.isOpen &&
			segmentFileNum == aoFetchDesc->currentSegmentFile.num &&
			segmentFileNum == aoFetchDesc->blockDirectory.currentSegmentFileNum &&
			segmentFileNum == aoFetchDesc->executorReadBlock.segmentFileNum)
		{
			if (rowNum >= aoFetchDesc->currentBlock.firstRowNum &&
				rowNum <= aoFetchDesc->currentBlock.lastRowNum &&
				AppendOnlyBlockDirectoryEntry_RangeHasRow(
														  &aoFetchDesc->currentBlock.blockDirectoryEntry,
														  rowNum))
			{
				if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aoFetchDesc->visibilityMap, aoTupleId))
				{
					if (slot != NULL)
					{
						ExecClearTuple(slot);
					}
					return false;	/* row has been deleted or updated. */
				}
				fetchFromCurrentBlock(aoFetchDesc, rowNum, slot);
				return true;
			}

			/*
			 * Otherwise, if the current Block Directory entry covers the
			 * request tuples, lets use its information as another performance
			 * optimization.
			 */
			if (AppendOnlyBlockDirectoryEntry_RangeHasRow(
														  &aoFetchDesc->currentBlock.blockDirectoryEntry,
														  rowNum))
			{
				/*
				 * The tuple is covered by the current Block Directory entry,
				 * but is it before or after our current block?
				 */
				if (rowNum < aoFetchDesc->currentBlock.firstRowNum)
				{
					/*
					 * XXX This could happen when an insert is cancelled. In
					 * that case, we fetched the next block that has a higher
					 * firstRowNum when we try to find the first cancelled
					 * row. So for the second or any cancelled row, we enter
					 * here, and re-read the previous block. This seems
					 * inefficient.
					 *
					 * We may be able to fix this by adding an entry to the
					 * block directory for those cancelled inserts.
					 */

					/*
					 * Set scan range to prior blocks.
					 */
					positionFirstBlockOfRange(aoFetchDesc);

					/* Set limit to before current block. */
					aoFetchDesc->scanAfterFileOffset =
						aoFetchDesc->currentBlock.fileOffset;

					aoFetchDesc->scanLastRowNum =
						aoFetchDesc->currentBlock.firstRowNum - 1;
				}
				else
				{
					/*
					 * Set scan range to following blocks.
					 */
					positionSkipCurrentBlock(aoFetchDesc);

					positionLimitToEndOfRange(aoFetchDesc);
				}

				if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aoFetchDesc->visibilityMap, aoTupleId))
				{
					if (slot != NULL)
					{
						ExecClearTuple(slot);
					}
					return false;	/* row has been deleted or updated. */
				}

				if (scanToFetchTuple(aoFetchDesc, rowNum, slot))
					return true;

				if (slot != NULL)
					ExecClearTuple(slot);
				return false;
				/* Segment file not in aoseg table.. */
			}
		}
	}

	/*
	 * Open or switch open, if necessary.
	 */
	if (aoFetchDesc->currentSegmentFile.isOpen &&
		segmentFileNum != aoFetchDesc->currentSegmentFile.num)
	{
#ifdef USE_ASSERT_CHECKING
		/*
		 * Currently, we only support Index Scan on bitmap index and Bitmap Index Scan
		 * on AO tables, so normally the below warning should not happen.
		 * See get_index_paths in indxpath.c.
		 */
		if (segmentFileNum < aoFetchDesc->currentSegmentFile.num)
			ereport(WARNING,
					(errmsg("append-only fetch requires scan prior segment file: segmentFileNum %d, rowNum " INT64_FORMAT ", currentSegmentFileNum %d",
							segmentFileNum, rowNum,
							aoFetchDesc->currentSegmentFile.num)));
#endif
		closeFetchSegmentFile(aoFetchDesc);

		Assert(!aoFetchDesc->currentSegmentFile.isOpen);
	}

	if (!aoFetchDesc->currentSegmentFile.isOpen)
	{
		if (!openFetchSegmentFile(
								  aoFetchDesc,
								  segmentFileNum))
		{
			if (slot != NULL)
				ExecClearTuple(slot);
			return false;
			/* Segment file not in aoseg table.. */
			/* Must be aborted or deleted and reclaimed. */
		}

		/* Reset currentBlock info */
		resetCurrentBlockInfo(&(aoFetchDesc->currentBlock));
	}

	/*
	 * Need to get the Block Directory entry that covers the TID.
	 */
	if (!AppendOnlyBlockDirectory_GetEntry(
										   &aoFetchDesc->blockDirectory,
										   aoTupleId,
										   0,
										   &aoFetchDesc->currentBlock.blockDirectoryEntry,
										   NULL))
	{
		if (slot != NULL)
		{
			ExecClearTuple(slot);
		}
		return false;			/* Row not represented in Block Directory. */
		/* Must be aborted or deleted and reclaimed. */
	}

	if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aoFetchDesc->visibilityMap, aoTupleId))
	{
		if (slot != NULL)
		{
			ExecClearTuple(slot);
		}
		return false;			/* row has been deleted or updated. */
	}


	/*
	 * Set scan range covered by new Block Directory entry.
	 */
	positionFirstBlockOfRange(aoFetchDesc);

	positionLimitToEndOfRange(aoFetchDesc);

	if (scanToFetchTuple(aoFetchDesc, rowNum, slot))
		return true;

	if (slot != NULL)
		ExecClearTuple(slot);
	return false;
	/* Segment file not in aoseg table.. */
}

void
appendonly_fetch_finish(AppendOnlyFetchDesc aoFetchDesc)
{
	RelationDecrementReferenceCount(aoFetchDesc->relation);

	AppendOnlyStorageRead_CloseFile(&aoFetchDesc->storageRead);

	AppendOnlyStorageRead_FinishSession(&aoFetchDesc->storageRead);

	AppendOnlyExecutorReadBlock_Finish(&aoFetchDesc->executorReadBlock);

	AppendOnlyBlockDirectory_End_forSearch(&aoFetchDesc->blockDirectory);

	if (aoFetchDesc->segmentFileInfo)
	{
		FreeAllSegFileInfo(aoFetchDesc->segmentFileInfo, aoFetchDesc->totalSegfiles);
		pfree(aoFetchDesc->segmentFileInfo);
		aoFetchDesc->segmentFileInfo = NULL;
	}

	AppendOnlyVisimap_Finish(&aoFetchDesc->visibilityMap, AccessShareLock);

	pfree(aoFetchDesc->segmentFileName);
	aoFetchDesc->segmentFileName = NULL;

	pfree(aoFetchDesc->title);
}

AppendOnlyIndexOnlyDesc
appendonly_index_only_init(Relation relation, Snapshot snapshot)
{
	AppendOnlyIndexOnlyDesc indexonlydesc = (AppendOnlyIndexOnlyDesc) palloc0(sizeof(AppendOnlyIndexOnlyDescData));

	/* initialize the block directory */
	indexonlydesc->blockDirectory = palloc0(sizeof(AppendOnlyBlockDirectory));
	AppendOnlyBlockDirectory_Init_forIndexOnlyScan(indexonlydesc->blockDirectory,
												   relation,
												   1,
												   snapshot);

	/* initialize the visimap */
	indexonlydesc->visimap = palloc0(sizeof(AppendOnlyVisimap));
	AppendOnlyVisimap_Init_forIndexOnlyScan(indexonlydesc->visimap,
											relation,
											snapshot);
	return indexonlydesc;
}

bool
appendonly_index_only_check(AppendOnlyIndexOnlyDesc indexonlydesc, AOTupleId *aotid, Snapshot snapshot)
{
	if (!AppendOnlyBlockDirectory_CoversTuple(indexonlydesc->blockDirectory, aotid))
		return false;

	/* check SnapshotAny for the case when gp_select_invisible is on */
	if (snapshot != SnapshotAny && !AppendOnlyVisimap_IsVisible(indexonlydesc->visimap, aotid))
		return false;
	
	return true;
}

void
appendonly_index_only_finish(AppendOnlyIndexOnlyDesc indexonlydesc)
{
	/* clean up the block directory */
	AppendOnlyBlockDirectory_End_forIndexOnlyScan(indexonlydesc->blockDirectory);
	pfree(indexonlydesc->blockDirectory);
	indexonlydesc->blockDirectory = NULL;

	/* clean up the visimap */
	AppendOnlyVisimap_Finish_forIndexOnlyScan(indexonlydesc->visimap);
	pfree(indexonlydesc->visimap);
	indexonlydesc->visimap = NULL;
}

/*
 * appendonly_delete_init
 *
 * before using appendonly_delete() to delete tuples from append-only segment
 * files, we need to call this function to initialize the delete desc
 * data structured.
 */
AppendOnlyDeleteDesc
appendonly_delete_init(Relation rel)
{
	Assert(!IsolationUsesXactSnapshot());

	Oid visimaprelid;

	GetAppendOnlyEntryAuxOids(rel, NULL, NULL, &visimaprelid);

	AppendOnlyDeleteDesc aoDeleteDesc = palloc0(sizeof(AppendOnlyDeleteDescData));

	aoDeleteDesc->aod_rel = rel;
	aoDeleteDesc->appendOnlyMetaDataSnapshot = GetActiveSnapshot();

	AppendOnlyVisimap_Init(&aoDeleteDesc->visibilityMap,
						   visimaprelid,
						   RowExclusiveLock,
						   aoDeleteDesc->appendOnlyMetaDataSnapshot);

	AppendOnlyVisimapDelete_Init(&aoDeleteDesc->visiMapDelete,
								 &aoDeleteDesc->visibilityMap);

	return aoDeleteDesc;
}

void
appendonly_delete_finish(AppendOnlyDeleteDesc aoDeleteDesc)
{
	Assert(aoDeleteDesc);

	AppendOnlyVisimapDelete_Finish(&aoDeleteDesc->visiMapDelete);

	AppendOnlyVisimap_Finish(&aoDeleteDesc->visibilityMap, NoLock);

	pfree(aoDeleteDesc);
}

TM_Result
appendonly_delete(AppendOnlyDeleteDesc aoDeleteDesc,
				  AOTupleId *aoTupleId)
{
	Assert(aoDeleteDesc);
	Assert(aoTupleId);

	elogif(Debug_appendonly_print_delete, LOG,
		   "Append-only delete tuple from table '%s' (AOTupleId %s)",
		   NameStr(aoDeleteDesc->aod_rel->rd_rel->relname),
		   AOTupleIdToString(aoTupleId));

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
								   "appendonly_delete",
								   DDLNotSpecified,
								   "", //databaseName
								   RelationGetRelationName(aoDeleteDesc->aod_rel));
	/* tableName */
#endif

	return AppendOnlyVisimapDelete_Hide(&aoDeleteDesc->visiMapDelete, aoTupleId);
}

/*
 * appendonly_insert_init
 *
 * 'segno' must be a segment that has been previously locked for this
 * transaction, by calling LockSegnoForWrite() or ChooseSegnoForWrite().
 *
 * before using appendonly_insert() to insert tuples we need to call
 * this function to initialize our varblock and bufferedAppend structures
 * and memory for appending data into the relation file.
 *
 * 'num_rows': Size of gp_fast_sequence allocation for this insert iteration.
 * If a valid number of rows value is provided, in cases where we have a sense
 * of how many rows we will be inserting (such as multi-insert), we use that to
 * perform the allocation. Otherwise, if 0 is supplied, the default
 * NUM_FAST_SEQUENCES is used. Using a larger range for gp_fast_sequence helps
 * reduce trips to gp_fast_sequence, enhancing performance, especially for
 * concurrent loads.
 *
 * see appendonly_insert() for more specifics about inserting tuples into
 * append only tables.
 */
AppendOnlyInsertDesc
appendonly_insert_init(Relation rel, int segno, int64 num_rows)
{
	AppendOnlyInsertDesc aoInsertDesc;
	int			maxtupsize;
	int64		firstSequence = 0;
	PGFunction *fns;
	int			desiredOverflowBytes = 0;
	size_t		(*desiredCompressionSize) (size_t input);

	AppendOnlyStorageAttributes *attr;

	StringInfoData titleBuf;
	bool		checksum = true;
	int32		blocksize = -1;
	int16		compresslevel = 0;
	NameData	compresstype;

	GetAppendOnlyEntryAttributes(rel->rd_id, &blocksize, &compresslevel, &checksum, &compresstype);

	/*
	 * Get the pg_appendonly information for this table
	 */

	/*
	 * allocate and initialize the insert descriptor
	 */
	aoInsertDesc = (AppendOnlyInsertDesc) palloc0(sizeof(AppendOnlyInsertDescData));

	aoInsertDesc->aoi_rel = rel;

	/*
	 * We want to see an up-to-date view of the metadata. The target segment's
	 * pg_aoseg row is already locked for us.
	 */
	aoInsertDesc->appendOnlyMetaDataSnapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

	aoInsertDesc->mt_bind = create_memtuple_binding(RelationGetDescr(rel), RelationGetNumberOfAttributes(rel));

	aoInsertDesc->appendFile = -1;
	aoInsertDesc->appendFilePathNameMaxLen = AOSegmentFilePathNameLen(rel) + 1;
	aoInsertDesc->appendFilePathName = (char *) palloc(aoInsertDesc->appendFilePathNameMaxLen);
	aoInsertDesc->appendFilePathName[0] = '\0';

	aoInsertDesc->bufferCount = 0;
	aoInsertDesc->blockFirstRowNum = 0;
	aoInsertDesc->insertCount = 0;
	aoInsertDesc->varblockCount = 0;
	aoInsertDesc->rowCount = 0;

	Assert(segno >= 0);
	aoInsertDesc->cur_segno = segno;

	/*
	 * Adding a NOTOAST table attribute in 3.3.3 would require a catalog
	 * change, so in the interim we will test this with a GUC.
	 *
	 * This GUC must have the same value on write and read.
	 */
/* 	aoInsertDesc->useNoToast = aoentry->notoast; */

	/*
	 * Although variable length blocks of AO should be able to accommodate variable length
	 * datums, we still need to keep TOAST for AO_ROW to benefit to performance when query
	 * in-line data.
	 */
	aoInsertDesc->useNoToast = !(rel->rd_tableam->relation_needs_toast_table(rel));

	aoInsertDesc->usableBlockSize = blocksize;

	attr = &aoInsertDesc->storageAttributes;

	/*
	 * These attributes describe the AppendOnly format to be scanned.
	 */
	if (strcmp(NameStr(compresstype), "") == 0 ||
		pg_strcasecmp(NameStr(compresstype), "none") == 0)
	{
		attr->compress = false;
		attr->compressType = "none";
	}
	else
	{
		attr->compress = true;
		attr->compressType = NameStr(compresstype);
	}
	attr->compressLevel = compresslevel;
	attr->checksum = checksum;

	fns = get_funcs_for_compression(NameStr(compresstype));

	CompressionState *cs = NULL;
	CompressionState *verifyCs = NULL;

	if (fns)
	{
		PGFunction	cons = fns[COMPRESSION_CONSTRUCTOR];
		StorageAttributes sa;

		sa.comptype = NameStr(compresstype);
		sa.complevel = compresslevel;
		sa.blocksize = blocksize;

		cs = callCompressionConstructor(cons, RelationGetDescr(rel),
										&sa,
										true /* compress */ );
		if (gp_appendonly_verify_write_block == true)
		{
			verifyCs = callCompressionConstructor(cons, RelationGetDescr(rel),
												  &sa,
												  false /* decompress */ );
		}

		desiredCompressionSize = cs->desired_sz;
		if (desiredCompressionSize != NULL)
		{
			/*
			 * Call the compression's desired size function to find out what
			 * additional space it requires for our block size.
			 */
			desiredOverflowBytes =
				(int) (desiredCompressionSize) (aoInsertDesc->usableBlockSize)
				- aoInsertDesc->usableBlockSize;
			Assert(desiredOverflowBytes >= 0);
		}
	}

	aoInsertDesc->storageAttributes.overflowSize = desiredOverflowBytes;

	initStringInfo(&titleBuf);
	appendStringInfo(&titleBuf, "Write of Append-Only Row-Oriented relation '%s'",
					 RelationGetRelationName(aoInsertDesc->aoi_rel));
	aoInsertDesc->title = titleBuf.data;

	AppendOnlyStorageWrite_Init(
								&aoInsertDesc->storageWrite,
								NULL,
								aoInsertDesc->usableBlockSize,
								RelationGetRelationName(aoInsertDesc->aoi_rel),
								aoInsertDesc->title,
								&aoInsertDesc->storageAttributes,
								XLogIsNeeded() && RelationNeedsWAL(aoInsertDesc->aoi_rel));

	aoInsertDesc->storageWrite.compression_functions = fns;
	aoInsertDesc->storageWrite.compressionState = cs;
	aoInsertDesc->storageWrite.verifyWriteCompressionState = verifyCs;

	elogif(Debug_appendonly_print_insert, LOG,
		   "Append-only insert initialize for table '%s' segment file %u "
		   "(compression = %s, compression type %s, compression level %d)",
		   NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
		   aoInsertDesc->cur_segno,
		   (attr->compress ? "true" : "false"),
		   NameStr(compresstype),
		   attr->compressLevel);

	/*
	 * Temporarily set the firstRowNum for the block so that we can calculate
	 * the correct header length.
	 */
	AppendOnlyStorageWrite_SetFirstRowNum(&aoInsertDesc->storageWrite,
										  1);

	aoInsertDesc->completeHeaderLen =
		AppendOnlyStorageWrite_CompleteHeaderLen(
												 &aoInsertDesc->storageWrite,
												 AoHeaderKind_SmallContent);

	aoInsertDesc->maxDataLen =
		aoInsertDesc->usableBlockSize -
		aoInsertDesc->completeHeaderLen;

	aoInsertDesc->tempSpaceLen = aoInsertDesc->usableBlockSize / 8; /* TODO - come up with a
																	 * more efficient
																	 * calculation */
	aoInsertDesc->tempSpace = (uint8 *) palloc(aoInsertDesc->tempSpaceLen * sizeof(uint8));
	maxtupsize = aoInsertDesc->maxDataLen - VARBLOCK_HEADER_LEN - 4;
	aoInsertDesc->toast_tuple_threshold = maxtupsize / 4;	/* see tuptoaster.h for
															 * more information */
	aoInsertDesc->toast_tuple_target = maxtupsize / 4;

	/* open our current relation file segment for write */
	SetCurrentFileSegForWrite(aoInsertDesc);

	Assert(aoInsertDesc->tempSpaceLen > 0);

	/*
	 * Obtain the next list of fast sequences for this relation.
	 *
	 * Even in the case of no indexes, we need to update the fast sequences,
	 * since the table may contain indexes at some point of time.
	 */
	Assert(aoInsertDesc->fsInfo->segno == segno);

	GetAppendOnlyEntryAuxOids(aoInsertDesc->aoi_rel, &aoInsertDesc->segrelid,
			NULL, NULL);

	firstSequence = GetFastSequences(aoInsertDesc->segrelid, segno, num_rows);
	aoInsertDesc->numSequences = num_rows;

	/* Set last_sequence value */
	Assert(firstSequence > aoInsertDesc->rowCount);
	aoInsertDesc->lastSequence = firstSequence - 1;

	setupNextWriteBlock(aoInsertDesc);

	/* Initialize the block directory. */
	AppendOnlyBlockDirectory_Init_forInsert(
											&(aoInsertDesc->blockDirectory),
aoInsertDesc->appendOnlyMetaDataSnapshot, //CONCERN:Safe to assume all block directory entries for segment are "covered" by same exclusive lock.
											aoInsertDesc->fsInfo, aoInsertDesc->lastSequence,
											rel, segno, 1, false);

	return aoInsertDesc;
}


/*
 *	appendonly_insert		- insert tuple into a varblock
 *
 * Note the following major differences from heap_insert
 *
 * - wal is always bypassed here.
 * - transaction information is of no interest.
 * - tuples inserted into varblocks, not via the postgresql buf/page manager.
 * - no need to pin buffers.
 *
  * The header fields of *tup are updated to match the stored tuple;
  *
  * Unlike heap_insert(), this function doesn't scribble on the input tuple.
  */
void
appendonly_insert(AppendOnlyInsertDesc aoInsertDesc,
				  MemTuple instup,
				  AOTupleId *aoTupleId)
{
	Relation	relation = aoInsertDesc->aoi_rel;
	VarBlockByteLen itemLen;
	uint8	   *itemPtr;
	MemTuple	tup = NULL;
	bool		need_toast;
	bool		isLargeContent;

	Assert(aoInsertDesc->usableBlockSize > 0 && aoInsertDesc->tempSpaceLen > 0);
	Assert(aoInsertDesc->toast_tuple_threshold > 0 && aoInsertDesc->toast_tuple_target > 0);

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
								   "appendonly_insert",
								   DDLNotSpecified,
								   "", //databaseName
								   RelationGetRelationName(aoInsertDesc->aoi_rel));
	/* tableName */
#endif

	if (aoInsertDesc->useNoToast)
		need_toast = false;
	else
		need_toast = (MemTupleHasExternal(instup, aoInsertDesc->mt_bind) ||
					  memtuple_get_size(instup) > aoInsertDesc->toast_tuple_threshold);

	/*
	 * If the new tuple is too big for storage or contains already toasted
	 * out-of-line attributes from some other relation, invoke the toaster.
	 *
	 * Note: below this point, tup is the data we actually intend to store
	 * into the relation; instup is the caller's original untoasted data.
	 */
	if (need_toast)
		tup = toast_insert_or_update_memtup(relation, instup,
											NULL, aoInsertDesc->mt_bind,
											aoInsertDesc->toast_tuple_target,
											0);
	else
		tup = instup;

	/*
	 * get space to insert our next item (tuple)
	 */
	itemLen = memtuple_get_size(tup);
	isLargeContent = false;

	/*
	 * If we are at the limit for append-only storage header's row count,
	 * force this VarBlock to finish.
	 */
	if (VarBlockMakerItemCount(&aoInsertDesc->varBlockMaker) >= AOSmallContentHeader_MaxRowCount)
		itemPtr = NULL;
	else
		itemPtr = VarBlockMakerGetNextItemPtr(&aoInsertDesc->varBlockMaker, itemLen);

	/*
	 * If no more room to place items in the current varblock finish it and
	 * start inserting into the next one.
	 */
	if (itemPtr == NULL)
	{
		if (VarBlockMakerItemCount(&aoInsertDesc->varBlockMaker) == 0)
		{
			/*
			 * Case #1.  The entire tuple cannot fit within a VarBlock.  It is
			 * too large.
			 */
			if (aoInsertDesc->useNoToast)
			{
				/*
				 * Indicate we need to write the large tuple as a large
				 * content multiple-block set.
				 */
				isLargeContent = true;
			}
			else
			{
				/*
				 * Use a different errcontext when user input (tuple contents)
				 * cause the error.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("item too long (check #1): length %d, maxBufferLen %d",
								itemLen, aoInsertDesc->varBlockMaker.maxBufferLen),
						 errcontext_appendonly_insert_block_user_limit(aoInsertDesc)));
			}
		}
		else
		{
			/*
			 * Write out the current VarBlock to make room.
			 */
			finishWriteBlock(aoInsertDesc);
			Assert(aoInsertDesc->nonCompressedData == NULL);
			Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

			/*
			 * Setup a new VarBlock.
			 */
			setupNextWriteBlock(aoInsertDesc);

			itemPtr = VarBlockMakerGetNextItemPtr(&aoInsertDesc->varBlockMaker, itemLen);

			if (itemPtr == NULL)
			{
				/*
				 * Case #2.  The entire tuple cannot fit within a VarBlock. It
				 * is too large.
				 */
				if (aoInsertDesc->useNoToast)
				{
					/*
					 * Indicate we need to write the large tuple as a large
					 * content multiple-block set.
					 */
					isLargeContent = true;
				}
				else
				{
					/*
					 * Use a different errcontext when user input (tuple
					 * contents) cause the error.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("item too long (check #2): length %d, maxBufferLen %d",
									itemLen, aoInsertDesc->varBlockMaker.maxBufferLen),
							 errcontext_appendonly_insert_block_user_limit(aoInsertDesc)));
				}
			}
		}
	}

	if (!isLargeContent)
	{
		/*
		 * We have room in the current VarBlock for the new tuple.
		 */
		Assert(itemPtr != NULL);

		if (itemLen > 0)
			memcpy(itemPtr, tup, itemLen);
	}
	else
	{
		/*
		 * Write the large tuple as a large content multiple-block set.
		 */
		Assert(itemPtr == NULL);
		Assert(!need_toast);
		Assert(instup == tup);

		/*
		 * "Cancel" the last block allocation, if one.
		 */
		cancelLastBuffer(aoInsertDesc);
		Assert(aoInsertDesc->nonCompressedData == NULL);
		Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

		/*
		 * Write large content.
		 */
		AppendOnlyStorageWrite_Content(
									   &aoInsertDesc->storageWrite,
									   (uint8 *) tup,
									   itemLen,
									   AoExecutorBlockKind_SingleRow,
									    /* rowCount */ 1);
		Assert(aoInsertDesc->nonCompressedData == NULL);
		Assert(!AppendOnlyStorageWrite_IsBufferAllocated(&aoInsertDesc->storageWrite));

		setupNextWriteBlock(aoInsertDesc);
	}

	aoInsertDesc->insertCount++;
	aoInsertDesc->lastSequence++;
	if (aoInsertDesc->numSequences > 0)
		(aoInsertDesc->numSequences)--;

	Assert(aoInsertDesc->numSequences >= 0);

	AOTupleIdInit(aoTupleId, aoInsertDesc->cur_segno, aoInsertDesc->lastSequence);

	/*
	 * If the allocated fast sequence numbers are used up, we request for a
	 * next list of fast sequence numbers.
	 */
	if (aoInsertDesc->numSequences == 0)
	{
		int64 firstSequence PG_USED_FOR_ASSERTS_ONLY = GetFastSequences(aoInsertDesc->segrelid,
																		aoInsertDesc->cur_segno,
																		NUM_FAST_SEQUENCES);

		Assert(firstSequence == aoInsertDesc->lastSequence + 1);
		aoInsertDesc->numSequences = NUM_FAST_SEQUENCES;
	}

	elogif(Debug_appendonly_print_insert_tuple, LOG,
		   "Append-only insert tuple for table '%s' "
		   "(AOTupleId %s, memtuple length %d, isLargeRow %s, block count " INT64_FORMAT ")",
		   NameStr(aoInsertDesc->aoi_rel->rd_rel->relname),
		   AOTupleIdToString(aoTupleId),
		   itemLen,
		   (isLargeContent ? "true" : "false"),
		   aoInsertDesc->bufferCount);

	if (tup != instup)
		pfree(tup);
}

/*
 * appendonly_insert_finish
 *
 * when done inserting all the data via appendonly_insert() we need to call
 * this function to flush all remaining data in the buffer into the file.
 */
void
appendonly_insert_finish(AppendOnlyInsertDesc aoInsertDesc)
{
	/*
	 * Finish up that last varblock.
	 */
	finishWriteBlock(aoInsertDesc);

	CloseWritableFileSeg(aoInsertDesc);

	AppendOnlyBlockDirectory_End_forInsert(&(aoInsertDesc->blockDirectory));

	AppendOnlyStorageWrite_FinishSession(&aoInsertDesc->storageWrite);

	UnregisterSnapshot(aoInsertDesc->appendOnlyMetaDataSnapshot);

	destroy_memtuple_binding(aoInsertDesc->mt_bind);

	pfree(aoInsertDesc->title);
	pfree(aoInsertDesc);
}
