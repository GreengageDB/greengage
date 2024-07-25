/*--------------------------------------------------------------------------
 *
 * aocsam.c
 *	  Append only columnar access methods
 *
 * Portions Copyright (c) 2009-2010, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/aocs/aocsam.c
 *
 *--------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/relpath.h"
#include "access/aocssegfiles.h"
#include "access/aomd.h"
#include "access/appendonlytid.h"
#include "access/appendonlywriter.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_appendonly.h"
#include "catalog/pg_attribute_encoding.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbappendonlyblockdirectory.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbappendonlystorageread.h"
#include "cdb/cdbappendonlystoragewrite.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/altertablenodes.h"
#include "pgstat.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datumstream.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

static AOCSScanDesc aocs_beginscan_internal(Relation relation,
						AOCSFileSegInfo **seginfo,
						int total_seg,
						Snapshot snapshot,
						Snapshot appendOnlyMetaDataSnapshot,
						bool *proj,
						AOCSProjectionKind projKind,
						uint32 flags);
/*
 * Open the segment file for a specified column associated with the datum
 * stream.
 */
static void
open_datumstreamread_segfile(
							 char *basepath, Relation rel,
							 AOCSFileSegInfo *segInfo,
							 DatumStreamRead *ds,
							 int colNo)
{
	int			segNo = segInfo->segno;
	char		fn[MAXPGPATH];
	int32		fileSegNo;
	RelFileNode node = rel->rd_node;
	Oid         relid = RelationGetRelid(rel);

	/* Filenum for the column */
	FileNumber	filenum = GetFilenumForAttribute(relid, colNo + 1);

	AOCSVPInfoEntry *e = getAOCSVPEntry(segInfo, colNo);

	FormatAOSegmentFileName(basepath, segNo, filenum, &fileSegNo, fn);
	Assert(strlen(fn) + 1 <= MAXPGPATH);

	Assert(ds);
	datumstreamread_open_file(ds, fn, e->eof, e->eof_uncompressed, node,
							  fileSegNo, segInfo->formatversion);
}

/*
 * Open all segment files associted with the datum stream.
 *
 * Currently, there is one segment file for each column. This function
 * only opens files for those columns which are in the projection.
 *
 * If blockDirectory is not NULL, the first block info is written to
 * the block directory.
 */
static void
open_all_datumstreamread_segfiles(AOCSScanDesc scan, AOCSFileSegInfo *segInfo)
{
	Relation 		rel = scan->rs_base.rs_rd;
	DatumStreamRead **ds = scan->columnScanInfo.ds;
	AttrNumber 		*proj_atts = scan->columnScanInfo.proj_atts;
	AttrNumber 		num_proj_atts = scan->columnScanInfo.num_proj_atts;
	AppendOnlyBlockDirectory *blockDirectory = scan->blockDirectory;
	char *basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);

	Assert(proj_atts);
	for (AttrNumber i = 0; i < num_proj_atts; i++)
	{
		AttrNumber	attno = proj_atts[i];

		open_datumstreamread_segfile(basepath, rel, segInfo, ds[attno], attno);

		/* skip reading block for ANALYZE/SampleScan/partial scan */
		if ((scan->rs_base.rs_flags & SO_TYPE_ANALYZE) != 0 ||
			(scan->rs_base.rs_flags & SO_TYPE_SAMPLESCAN) != 0 ||
			scan->partialScan)
			continue;

		datumstreamread_block(ds[attno], blockDirectory, attno);

		AOCSScanDesc_UpdateTotalBytesRead(scan, attno);
	}

	pfree(basepath);
}

/*
 * Initialise data streams for every column used in this query. For writes, this
 * means all columns.
 */
static void
open_ds_write(Relation rel, DatumStreamWrite **ds, TupleDesc relationTupleDesc, bool checksum)
{
	int			nvp = relationTupleDesc->natts;
	StdRdOptions **opts = RelationGetAttributeOptions(rel);

	/* open datum streams.  It will open segment file underneath */
	for (int i = 0; i < nvp; ++i)
	{
		Form_pg_attribute attr = TupleDescAttr(relationTupleDesc, i);
		char	   *ct;
		int32		clvl;
		int32		blksz;

		StringInfoData titleBuf;

		/* UNDONE: Need to track and dispose of this storage... */
		initStringInfo(&titleBuf);
		appendStringInfo(&titleBuf,
						 "Write of Append-Only Column-Oriented relation '%s', column #%d '%s'",
						 RelationGetRelationName(rel),
						 i + 1,
						 NameStr(attr->attname));

		/*
		 * We always record all the three column specific attributes for each
		 * column of a column oriented table.  Note: checksum is a table level
		 * attribute.
		 */
		if (opts[i] == NULL || opts[i]->blocksize == 0)
			elog(ERROR, "could not find blocksize option for AOCO column in pg_attribute_encoding");
		ct = opts[i]->compresstype;
		clvl = opts[i]->compresslevel;
		blksz = opts[i]->blocksize;

		ds[i] = create_datumstreamwrite(ct,
										clvl,
										checksum,
										blksz,
										attr,
										RelationGetRelationName(rel),
										/* title */ titleBuf.data,
										XLogIsNeeded() && RelationNeedsWAL(rel));

	}

	for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++)
		pfree(opts[i]);
	pfree(opts);
}

/*
 * Initialise data streams for every column used in this query. For writes, this
 * means all columns.
 */
static void
open_ds_read(Relation rel, DatumStreamRead **ds, TupleDesc relationTupleDesc,
			 AttrNumber *proj_atts, AttrNumber num_proj_atts, bool checksum)
{
	/*
	 *  RelationGetAttributeOptions does not always success return opts. e.g.
	 *  `ALTER TABLE ADD COLUMN` with an illegal option.
	 *
	 *  In this situation, the transaction will abort, and the Relation will be
	 *  free. Upstream have sanity check to promise we must have a worked TupleDesc
	 *  attached the Relation during memory recycle. Otherwise, the query will crash.
	 *
	 *  For some reason, we can not put the option validation check into "perp"
	 *  phase for AOCO table ALTER command.
	 *  (commit: e707c19c885fadffe50095cc699e52af1ee64f4b)
	 *
	 *  So, a fake TupleDesc temporary replace into Relation.
	 */

	TupleDesc orig_att = rel->rd_att;
	if (orig_att->tdrefcount == -1)
	{
		rel->rd_att = CreateTemplateTupleDesc(relationTupleDesc->natts);
		rel->rd_att->tdrefcount = 1;
	}

	StdRdOptions **opts = RelationGetAttributeOptions(rel);

	if (orig_att->tdrefcount == -1)
	{
		pfree(rel->rd_att);
		rel->rd_att = orig_att;
	}

	/*
	 * Clear all the entries to NULL first, as the NULL value is used during
	 * closing
	 */
	for (AttrNumber attno = 0; attno < relationTupleDesc->natts; attno++)
		ds[attno] = NULL;

	/* And then initialize the data streams for those columns we need */
	for (AttrNumber i = 0; i < num_proj_atts; i++)
	{
		AttrNumber			attno = proj_atts[i];
		Form_pg_attribute	attr;
		char			   *ct;
		int32				clvl;
		int32				blksz;
		StringInfoData titleBuf;

		Assert(attno <= relationTupleDesc->natts);
		attr = TupleDescAttr(relationTupleDesc, attno);

		/*
		 * We always record all the three column specific attributes for each
		 * column of a column oriented table.  Note: checksum is a table level
		 * attribute.
		 */
		if (opts[attno] == NULL || opts[attno]->blocksize == 0)
			elog(ERROR, "could not find blocksize option for AOCO column in pg_attribute_encoding");
		ct = opts[attno]->compresstype;
		clvl = opts[attno]->compresslevel;
		blksz = opts[attno]->blocksize;

		/* UNDONE: Need to track and dispose of this storage... */
		initStringInfo(&titleBuf);
		appendStringInfo(&titleBuf, "Scan of Append-Only Column-Oriented relation '%s', column #%d '%s'",
						 RelationGetRelationName(rel),
						 attno + 1,
						 NameStr(attr->attname));

		ds[attno] = create_datumstreamread(ct,
										   clvl,
										   checksum,
										   blksz,
										   attr,
										   RelationGetRelationName(rel),
										    /* title */ titleBuf.data);
	}

	for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++)
		pfree(opts[i]);
	pfree(opts);
}

static void
close_ds_read(DatumStreamRead **ds, AttrNumber natts)
{
	for (AttrNumber attno = 0; attno < natts; attno++)
	{
		if (ds[attno])
		{
			destroy_datumstreamread(ds[attno]);
			ds[attno] = NULL;
		}
	}
}

static void
close_ds_write(DatumStreamWrite **ds, int nvp)
{
	int			i;

	for (i = 0; i < nvp; ++i)
	{
		if (ds[i])
		{
			destroy_datumstreamwrite(ds[i]);
			ds[i] = NULL;
		}
	}
}

/*
 * Move the anchor column into the anchor position in the projection array, 
 * which is index 0. If the anchor column is not in the projection, add it.
 * If it's already in the projection, swap it with the current first.
 *
 * Return the new length of the projection array.
 */
int aoco_proj_move_anchor_first(AttrNumber *proj_atts,
								int num_proj_atts,
								int anchor_colno)
{
	int 			anchor_proj_i;

	Assert(proj_atts);

	/* if anchor is already at the first, nothing to do. */
	if (proj_atts[ANCHOR_COL_IN_PROJ] == anchor_colno)
		return num_proj_atts;

	/* find the anchor colno in the projection */
	for (anchor_proj_i = 0; anchor_proj_i < num_proj_atts; anchor_proj_i++)
		if (proj_atts[anchor_proj_i] == anchor_colno)
			break;

	/* move the current first colno elsewhere */
	if (anchor_proj_i == num_proj_atts)
		/* no anchor column in projection, append the current first to the end */
		proj_atts[num_proj_atts++] = proj_atts[ANCHOR_COL_IN_PROJ];
	else
		/* an anchor column is in projection, move the current first there */
		proj_atts[anchor_proj_i] = proj_atts[ANCHOR_COL_IN_PROJ];

	/* finally, place anchor column at first */
	proj_atts[ANCHOR_COL_IN_PROJ] = anchor_colno;

	return num_proj_atts;
}

void
initscan_with_colinfo(AOCSScanDesc scan)
{
	MemoryContext	oldCtx;
	AttrNumber		natts;
	int 			anchor_colno;

	Assert(scan->columnScanInfo.relationTupleDesc);
	natts = scan->columnScanInfo.relationTupleDesc->natts;

	scan->cur_seg = -1;
	scan->segrowsprocessed = 0;

	ItemPointerSet(&scan->cdb_fake_ctid, 0, 0);

	scan->totalBytesRead = 0;

	/* if the table has zero column, the rest of this function is no-op */
	if (natts == 0)
		return;

	oldCtx = MemoryContextSwitchTo(scan->columnScanInfo.scanCtx);

	if (scan->columnScanInfo.ds == NULL)
		scan->columnScanInfo.ds = (DatumStreamRead **)
								  palloc0(natts * sizeof(DatumStreamRead *));

	/* pick the anchor column to scan first */
	anchor_colno = get_anchor_col(scan->seginfo, 
											scan->total_seg,
											natts,
											scan->rs_base.rs_rd,
											scan->columnScanInfo.proj_atts,
											scan->columnScanInfo.num_proj_atts);

	/* the anchor column should be a valid column in the table */
	Assert(anchor_colno >= 0 && anchor_colno < natts);

	/* based on the column projection kind, initialize the projection array */
	switch(scan->columnScanInfo.projKind)
	{
		case AOCS_PROJ_ANY:
			/* we are told to pick any one column, pick the anchor */
			scan->columnScanInfo.num_proj_atts = 1;
			scan->columnScanInfo.proj_atts[0] = anchor_colno;
			break;
		case AOCS_PROJ_SOME:
			/* we are told to pick some columns which has already been initialized */
			break;
		case AOCS_PROJ_ALL:
			/* we are told to project all columns */
			scan->columnScanInfo.num_proj_atts = natts;
			for (AttrNumber attno = 0; attno < natts; attno++)
				scan->columnScanInfo.proj_atts[attno] = attno;
			break;
		default:
			ereport(ERROR, errmsg("Unrecognized AOCS projection kind: %d",
				 				scan->columnScanInfo.projKind));
	}

	scan->columnScanInfo.num_proj_atts = aoco_proj_move_anchor_first(scan->columnScanInfo.proj_atts,
												scan->columnScanInfo.num_proj_atts,
												anchor_colno);

	open_ds_read(scan->rs_base.rs_rd, scan->columnScanInfo.ds,
				 scan->columnScanInfo.relationTupleDesc,
				 scan->columnScanInfo.proj_atts, scan->columnScanInfo.num_proj_atts,
				 scan->checksum);

	MemoryContextSwitchTo(oldCtx);

	pgstat_count_heap_scan(scan->rs_base.rs_rd);
}

static int
open_next_scan_seg(AOCSScanDesc scan)
{
	while (++scan->cur_seg < scan->total_seg)
	{
		AOCSFileSegInfo *curSegInfo = scan->seginfo[scan->cur_seg];

		if (curSegInfo->total_tupcount > 0)
		{
			bool		emptySeg = false;

			/*
			 * If the segment is entirely empty, nothing to do.
			 *
			 * We used to assume the corresponding segments for every column to
			 * be in the same state, and check the state of the first column.
			 * Since the introduction of missing-mode ADD COLUMN, a column could
			 * have missing values in them and we have to find the anchor column
			 * to scan.
			 */

			/*
			 * subtle: we must check for AWAITING_DROP before calling getAOCSVPEntry().
			 * ALTER TABLE ADD COLUMN does not update vpinfos on AWAITING_DROP segments.
			 */
			if (curSegInfo->state == AOSEG_STATE_AWAITING_DROP)
				emptySeg = true;
			else
			{
				AOCSVPInfoEntry *e;

				e = getAOCSVPEntry(curSegInfo, scan->columnScanInfo.proj_atts[ANCHOR_COL_IN_PROJ]);
				if (e->eof == 0)
					elog(ERROR, "inconsistent segment state for relation %s, segment %d, tuple count " INT64_FORMAT,
						 RelationGetRelationName(scan->rs_base.rs_rd),
						 curSegInfo->segno,
						 curSegInfo->total_tupcount);
			}

			if (!emptySeg)
			{

				/*
				 * If the scan also builds the block directory, initialize it
				 * here.
				 */
				if (scan->blockDirectory)
				{
					AppendOnlyBlockDirectory_Init_forInsert(scan->blockDirectory,
															scan->appendOnlyMetaDataSnapshot,
															(FileSegInfo *) curSegInfo,
															0 /* lastSequence */ ,
															scan->rs_base.rs_rd,
															curSegInfo->segno,
															scan->columnScanInfo.relationTupleDesc->natts,
															true);
				}

				open_all_datumstreamread_segfiles(scan, curSegInfo);

				return scan->cur_seg;
			}
		}
	}

	return -1;
}

/*
 * Similar to open_next_scan_seg(), except that we explicitly specify the segno
 * to be opened (via 'fsInfoIdx', an index into the scan's segfile array).
 *
 * We return true if we are successfully able to open the target segment.
 *
 * Since open_next_scan_seg() opens the next segment starting from
 * (scan->cur_seg + 1), skipping empty/awaiting-drop segs, we also check if the
 * seg opened isn't the one we targeted. If it isn't, then the target seg was
 * empty/awaiting-drop, and we return false.
 */
static bool
open_scan_seg(AOCSScanDesc scan, int fsInfoIdx)
{
	Assert(fsInfoIdx >= 0 && fsInfoIdx < scan->total_seg);

	scan->cur_seg = fsInfoIdx - 1;
	return open_next_scan_seg(scan) == fsInfoIdx;
}

static void
close_cur_scan_seg(AOCSScanDesc scan)
{
	if (scan->cur_seg < 0)
		return;

	/*
	 * If rescan is called before we lazily initialized then there is nothing to
	 * do
	 */
	if (scan->columnScanInfo.relationTupleDesc == NULL)
		return;

	for (AttrNumber attno = 0;
		 attno < scan->columnScanInfo.relationTupleDesc->natts;
		 attno++)
	{
		if (scan->columnScanInfo.ds[attno])
			datumstreamread_close_file(scan->columnScanInfo.ds[attno]);
	}

	if (scan->blockDirectory)
		AppendOnlyBlockDirectory_End_forInsert(scan->blockDirectory);
}

static void
aocs_blkdirscan_init(AOCSScanDesc scan)
{
	if (scan->aocsfetch == NULL)
	{
		int natts = RelationGetNumberOfAttributes(scan->rs_base.rs_rd);
		scan->proj = palloc(natts * sizeof(*scan->proj));
		MemSet(scan->proj, true, natts * sizeof(*scan->proj));

		scan->aocsfetch = aocs_fetch_init(scan->rs_base.rs_rd,
										  scan->rs_base.rs_snapshot,
										  scan->appendOnlyMetaDataSnapshot,
										  scan->proj);
	}

	scan->blkdirscan = palloc0(sizeof(AOBlkDirScanData));
	AOBlkDirScan_Init(scan->blkdirscan, &scan->aocsfetch->blockDirectory);
}

static void
aocs_blkdirscan_finish(AOCSScanDesc scan)
{
	AOBlkDirScan_Finish(scan->blkdirscan);
	pfree(scan->blkdirscan);
	scan->blkdirscan = NULL;

	if (scan->aocsfetch != NULL)
	{
		aocs_fetch_finish(scan->aocsfetch);
		pfree(scan->aocsfetch);
		scan->aocsfetch = NULL;
	}

	if (scan->proj != NULL)
	{
		pfree(scan->proj);
		scan->proj = NULL;
	}
}

/*
 * aocs_beginrangescan
 *
 * begins range-limited relation scan
 */
AOCSScanDesc
aocs_beginrangescan(Relation relation,
					Snapshot snapshot,
					Snapshot appendOnlyMetaDataSnapshot,
					int *segfile_no_arr,
					int segfile_count,
					bool *proj)
{
	AOCSFileSegInfo **seginfo;
	int			i;
	AOCSProjectionKind 	projKind = proj ? AOCS_PROJ_SOME : AOCS_PROJ_ALL;

	RelationIncrementReferenceCount(relation);

	seginfo = palloc0(sizeof(AOCSFileSegInfo *) * segfile_count);

	for (i = 0; i < segfile_count; i++)
	{
		seginfo[i] = GetAOCSFileSegInfo(relation, appendOnlyMetaDataSnapshot,
										segfile_no_arr[i], false);
	}
	return aocs_beginscan_internal(relation,
								   seginfo,
								   segfile_count,
								   snapshot,
								   appendOnlyMetaDataSnapshot,
								   proj,
								   projKind,
								   0);
}

AOCSScanDesc
aocs_beginscan(Relation relation,
			   Snapshot snapshot,
			   bool *proj,
			   AOCSProjectionKind projKind,
			   uint32 flags)
{
	AOCSFileSegInfo	  **seginfo;
	Snapshot			aocsMetaDataSnapshot;
	int32				total_seg;

	RelationIncrementReferenceCount(relation);

	/*
	 * The append-only meta data should never be fetched with
	 * SnapshotAny as bogus results are returned.
	 * We use SnapshotSelf for metadata, as regular MVCC snapshot can hide newly
	 * globally inserted tuples from global index build process.
	 */
	if (snapshot != SnapshotAny)
		aocsMetaDataSnapshot = snapshot;
	else
		aocsMetaDataSnapshot = SnapshotSelf;

	seginfo = GetAllAOCSFileSegInfo(relation, aocsMetaDataSnapshot, &total_seg, NULL);
	return aocs_beginscan_internal(relation,
								   seginfo,
								   total_seg,
								   snapshot,
								   aocsMetaDataSnapshot,
								   proj,
								   projKind,
								   flags);
}

/*
 * begin the scan over the given relation.
 */
static AOCSScanDesc
aocs_beginscan_internal(Relation relation,
						AOCSFileSegInfo **seginfo,
						int total_seg,
						Snapshot snapshot,
						Snapshot appendOnlyMetaDataSnapshot,
						bool *proj,
						AOCSProjectionKind projKind,
						uint32 flags)
{
	AOCSScanDesc	scan;
	AttrNumber		natts;
	Oid				visimaprelid;
	Oid				blkdirrelid;

	scan = (AOCSScanDesc) palloc0(sizeof(AOCSScanDescData));
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_flags = flags;
	scan->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	scan->seginfo = seginfo;
	scan->total_seg = total_seg;
	scan->columnScanInfo.scanCtx = CurrentMemoryContext;
	scan->partialScan = false;

	/* relationTupleDesc will be inited by the slot when needed */
	scan->columnScanInfo.relationTupleDesc = NULL;

	/* get the attnums info */
	scan->columnScanInfo.attnum_to_rownum = GetAttnumToLastrownumMapping(RelationGetRelid(relation), RelationGetNumberOfAttributes(relation));

	natts = RelationGetNumberOfAttributes(relation);

	/* always allocate all, but assign one/some/all columns later */
	scan->columnScanInfo.proj_atts = (AttrNumber *)
									 palloc0(natts * sizeof(AttrNumber));

	scan->columnScanInfo.projKind = projKind;

	/*
	 * We get an array of booleans to indicate which columns are needed. But
	 * if you have a very wide table, and you only select a few columns from
	 * it, just scanning the boolean array to figure out which columns are
	 * needed can incur a noticeable overhead in aocs_getnext. So convert it
	 * into an array of the attribute numbers of the required columns.
	 *
	 * In other cases, we either need to scan all columns, or we just
	 * want to project only one of the columns which we will select later. 
	 */
	if (projKind == AOCS_PROJ_SOME)
	{
		Assert(proj); /* caller should indicate which columns to project */
		scan->columnScanInfo.num_proj_atts = 0;

		for (AttrNumber i = 0; i < natts; i++)
		{
			if (proj[i])
				scan->columnScanInfo.proj_atts[scan->columnScanInfo.num_proj_atts++] = i;
		}
	}
#ifdef USE_ASSERT_CHECKING
	else
		Assert(!proj); /* caller should not indicate anything otherwise */
#endif

	scan->columnScanInfo.ds = NULL;

	if ((flags & SO_TYPE_ANALYZE) || (flags & SO_TYPE_SAMPLESCAN) != 0)
	{
		scan->segfirstrow = 0;
		scan->targrow = 0;
	}

	GetAppendOnlyEntryAttributes(RelationGetRelid(relation),
								 NULL,
								 NULL,
								 &scan->checksum,
								 NULL);

	GetAppendOnlyEntryAuxOids(relation,
							  NULL,
							  &blkdirrelid,
							  &visimaprelid);

	scan->blkdirscan = NULL;

	if (scan->total_seg != 0)
	{
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
				aocs_blkdirscan_init(scan);
		}
	}

	scan->sampleTargetBlk = -1;

	return scan;
}

void
aocs_rescan(AOCSScanDesc scan)
{
	close_cur_scan_seg(scan);
	if (scan->columnScanInfo.ds)
		close_ds_read(scan->columnScanInfo.ds, scan->columnScanInfo.relationTupleDesc->natts);
	initscan_with_colinfo(scan);


	/* TABLESAMPLE related state */
	scan->segrowsprocessed = 0;
	scan->segfirstrow = 0;
	scan->targrow = 0;
	scan->sampleTargetBlk = -1;
	if (scan->blkdirscan)
	{
		aocs_blkdirscan_finish(scan);
		aocs_blkdirscan_init(scan);
	}
}

/*
 * Position an AOCS scan to start from a segno specified by the 'fsInfoIdx' in
 * the scan's segfile array, and offset specified by blkdir entry 'dirEntry',
 * for column specified by 'colIdx' in the scan's columnScanInfo.
 *
 * If we are unable to position the scan, we return false.
 */
bool
aocs_positionscan(AOCSScanDesc scan,
				  AppendOnlyBlockDirectoryEntry *dirEntry,
				  int colIdx,
				  int fsInfoIdx)
{
	int64 			beginFileOffset = dirEntry->range.fileOffset;
	int64 			afterFileOffset = dirEntry->range.afterFileOffset;
	DatumStreamRead *ds;
	int 			dsIdx;

	Assert(colIdx >= 0 && colIdx < scan->columnScanInfo.num_proj_atts);
	Assert(dirEntry);

	if (colIdx == 0)
	{
		if (scan->columnScanInfo.relationTupleDesc == NULL)
		{
			scan->columnScanInfo.relationTupleDesc = RelationGetDescr(scan->rs_base.rs_rd);
			/* Pin it! ... and of course release it upon destruction / rescan */
			PinTupleDesc(scan->columnScanInfo.relationTupleDesc);
			initscan_with_colinfo(scan);
		}

		/* Open segfiles for the given segno for each col the first time through. */
		if (!open_scan_seg(scan, fsInfoIdx))
		{
			/* target segment is empty/awaiting-drop */
			return false;
		}
	}

	/* The datum stream array is always of length relnatts */
	dsIdx = scan->columnScanInfo.proj_atts[colIdx];
	Assert(dsIdx >= 0 && dsIdx < RelationGetNumberOfAttributes(scan->rs_base.rs_rd));
	ds = scan->columnScanInfo.ds[dsIdx];

	if (beginFileOffset > ds->ao_read.logicalEof)
	{
		/* position maps to a hole at the end of the segfile */
		return false;
	}

	AppendOnlyStorageRead_SetTemporaryStart(&ds->ao_read,
												beginFileOffset,
												afterFileOffset);

	return true;
}

void
aocs_endscan(AOCSScanDesc scan)
{
	close_cur_scan_seg(scan);

	if (scan->columnScanInfo.ds)
	{
		Assert(scan->columnScanInfo.proj_atts);

		close_ds_read(scan->columnScanInfo.ds, scan->columnScanInfo.relationTupleDesc->natts);
		pfree(scan->columnScanInfo.ds);
		scan->columnScanInfo.ds = NULL;
	}

	if (scan->columnScanInfo.relationTupleDesc)
	{
		Assert(scan->columnScanInfo.proj_atts);

		ReleaseTupleDesc(scan->columnScanInfo.relationTupleDesc);
		scan->columnScanInfo.relationTupleDesc = NULL;
	}

	if (scan->columnScanInfo.proj_atts)
		pfree(scan->columnScanInfo.proj_atts);

	if (scan->columnScanInfo.attnum_to_rownum)
	{
		pfree(scan->columnScanInfo.attnum_to_rownum);
		scan->columnScanInfo.attnum_to_rownum = NULL;
	}

	for (int i = 0; i < scan->total_seg; ++i)
	{
		if (scan->seginfo[i])
		{
			pfree(scan->seginfo[i]);
			scan->seginfo[i] = NULL;
		}
	}

	if (scan->seginfo)
		pfree(scan->seginfo);

	if (scan->total_seg != 0)
		AppendOnlyVisimap_Finish(&scan->visibilityMap, AccessShareLock);

	if (scan->blkdirscan != NULL)
		aocs_blkdirscan_finish(scan);

	RelationDecrementReferenceCount(scan->rs_base.rs_rd);

	pfree(scan);
}

static int
aocs_locate_target_segment(AOCSScanDesc scan, int64 targrow)
{
	int64 rowcount;

	for (int i = scan->cur_seg; i < scan->total_seg; i++)
	{
		if (i < 0)
			continue;

		rowcount = scan->seginfo[i]->total_tupcount;
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
 * block directory based get_target_tuple()
 */
static bool
aocs_blkdirscan_get_target_tuple(AOCSScanDesc scan, int64 targrow, TupleTableSlot *slot)
{
	int segno, segidx;
	int64 rownum = InvalidAORowNum;
	int64 rowsprocessed;
	AOTupleId aotid;
	int ncols = scan->columnScanInfo.relationTupleDesc->natts;
	AppendOnlyBlockDirectory *blkdir = &scan->aocsfetch->blockDirectory;

	Assert(scan->blkdirscan != NULL);

	/* locate the target segment */
	segidx = aocs_locate_target_segment(scan, targrow);
	if (segidx < 0)
		return false;

	/* next starting position in locating segfile */
	scan->cur_seg = segidx;

	segno = scan->seginfo[segidx]->segno;
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

	/*
	 * Unlike ao_row, we set currentSegmentFileNum for ao_column here
	 * just for passing the assertion in extract_minipage() called by
	 * AOBlkDirScan_GetRowNum(). Then it shoule be restored back to
	 * the original value for making AppendOnlyBlockDirectory_GetEntry()
	 * work properly.
	 */
	int currentSegmentFileNum = blkdir->currentSegmentFileNum;
	blkdir->currentSegmentFileNum = blkdir->segmentFileInfo[segidx]->segno;

	/* locate the target row by seqscan block directory */
	for (int col = 0; col < ncols; col++)
	{
		/*
		 * "segfirstrow" should be always pointing to the first row of
		 * a new segfile, only locate_target_segment could update
		 * its value.
		 * 
		 * "segrowsprocessed" is used for tracking the position of
		 * processed rows in the current segfile.
		 */
		rowsprocessed = scan->segfirstrow + scan->segrowsprocessed;

		if ((scan->rs_base.rs_rd)->rd_att->attrs[col].attisdropped)
			continue;

		rownum = AOBlkDirScan_GetRowNum(scan->blkdirscan,
										segno,
										col,
										targrow,
										&rowsprocessed);

		elogif(Debug_appendonly_print_datumstream, LOG,
			   "AOBlkDirScan_GetRowNum(segno: %d, col: %d, targrow: %ld): "
			   "[segfirstrow: %ld, segrowsprocessed: %ld, rownum: %ld, cached_entry_no: %d]",
			   segno, col, targrow, scan->segfirstrow, scan->segrowsprocessed, rownum,
			   blkdir->minipages[col].cached_entry_no);

		if (rownum < 0)
			continue;

		scan->segrowsprocessed = rowsprocessed - scan->segfirstrow;

		/*
		 * Found a column represented in the block directory.
		 * Here we just look for the 1st such column, no need
		 * to read other columns within the same row.
		 */
		break;
	}

	/* restore to the original value as above mentioned */
	blkdir->currentSegmentFileNum = currentSegmentFileNum;

	if (rownum < 0)
		return false;

	/* form the target tuple TID */
	AOTupleIdInit(&aotid, segno, rownum);

	ExecClearTuple(slot);

	/* 
	 * Unlike ao_row, we don't update blkdir->minipages[col].cached_entry_no
	 * before fetching because ao_column requires all other column entries
	 * to form the whole tuple instead of the single one obtained by
	 * AOBlkDirScan_GetRowNum().
	 */

	/* fetch the target tuple */
	if(!aocs_fetch(scan->aocsfetch, &aotid, slot))
		return false;

	/* OK to return this tuple */
	ExecStoreVirtualTuple(slot);
	pgstat_count_heap_fetch(scan->rs_base.rs_rd);

	return true;
}

/*
 * returns the segfile number in which `targrow` locates  
 */
static int
aocs_getsegment(AOCSScanDesc scan, int64 targrow)
{
	int segno, segidx;

	/* locate the target segment */
	segidx = aocs_locate_target_segment(scan, targrow);
	if (segidx < 0)
	{
		/* done reading all segments */
		close_cur_scan_seg(scan);
		scan->cur_seg = -1;
		return -1;
	}

	segno = scan->seginfo[segidx]->segno;
	Assert(segno > InvalidFileSegNumber && segno <= AOTupleId_MaxSegmentFileNum);

	if (segidx > scan->cur_seg)
	{
		close_cur_scan_seg(scan);
		/* adjust cur_seg to fit for open_next_scan_seg() */
		scan->cur_seg = segidx - 1;
		if (open_next_scan_seg(scan) >= 0)
		{
			/* new segment, make sure segrowsprocessed was reset */
			Assert(scan->segrowsprocessed == 0);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unexpected behavior, failed to open segno %d during scanning AOCO table %s",
							segno, RelationGetRelationName(scan->rs_base.rs_rd))));
		}
	}
	
	return segno;
}

static inline int
aocs_block_remaining_rows(DatumStreamRead *ds)
{
	return (ds->blockRowCount - ds->blockRowsProcessed);
}

/*
 * fetches a single column value corresponding to `endrow` (equals to `targrow`)
 * Return the physical row number.
 * Optionally, caller passes a chkvisimap pointer to indicate if they want visimap
 * checking, the result of which will be written to chkvisimap.
 */
static int64
aocs_gettuple_column(AOCSScanDesc scan, AttrNumber attno, int64 startrow, int64 endrow, bool *chkvisimap, TupleTableSlot *slot)
{
	bool isSnapshotAny = (scan->rs_base.rs_snapshot == SnapshotAny);
	DatumStreamRead *ds = scan->columnScanInfo.ds[attno];
	int segno = scan->seginfo[scan->cur_seg]->segno;
	ItemPointerData fake_ctid;
	AOTupleId *aotid = (AOTupleId *) &fake_ctid;
	int64 rowstoprocess, nrows, rownum;
	Datum *values;
	bool *nulls;
	bool visible = true;

	if (ds->blockFirstRowNum <= 0)
		elog(ERROR, "AOCO varblock->blockFirstRowNum should be greater than zero.");

	Assert(segno > InvalidFileSegNumber && segno <= AOTupleId_MaxSegmentFileNum);
	Assert(startrow <= endrow);

	rowstoprocess = endrow - startrow + 1;
	nrows = ds->blockRowsProcessed + rowstoprocess;
	rownum = ds->blockFirstRowNum + nrows - 1;

	/* form the target tuple TID */
	AOTupleIdInit(aotid, segno, rownum);
	slot->tts_tid = fake_ctid;

	if (chkvisimap && !isSnapshotAny && !AppendOnlyVisimap_IsVisible(&scan->visibilityMap, aotid))
	{
		if (slot != NULL)
			ExecClearTuple(slot);

		visible = false;
		/* must update tracking vars before return */
		goto out;
	}

	/* rowNumInBlock = rowNum - blockFirstRowNum */
	datumstreamread_find(ds, rownum - ds->blockFirstRowNum);

	values = slot->tts_values;
	nulls = slot->tts_isnull;

	datumstreamread_get(ds, &(values[attno]), &(nulls[attno]));

out:
	/* update rows processed */
	ds->blockRowsProcessed += rowstoprocess;

	/* provide visimap checking result if caller asked */
	if (chkvisimap)
		*chkvisimap = visible;

	elogif(Debug_appendonly_print_datumstream, LOG,
		   "aocs_gettuple_column(): [attno: %d, targrow: %ld, startrow: %ld, segno: %d, rownum: %ld, "
		   "segfirstrow: %ld, segrowsprocessed: %ld, nth: %d, blockRowCount: %d, blockRowsProcessed: %d, ret: %ld]",
		   attno, endrow, startrow, segno, rownum, scan->segfirstrow, scan->segrowsprocessed, datumstreamread_nth(ds),
		   ds->blockRowCount, ds->blockRowsProcessed, rownum);

	return rownum;
}

/*
 * fetches all columns of the target tuple corresponding to `targrow`
 */
static bool
aocs_gettuple(AOCSScanDesc scan, int64 targrow, TupleTableSlot *slot)
{
	bool ret = true;
	int64 rowcount = InvalidAORowNum;
	int64 rowstoprocess;
	int64 phyrow = 0;

	Assert(scan->cur_seg >= 0);
	Assert(slot != NULL);

	ExecClearTuple(slot);

	rowstoprocess = targrow - scan->segfirstrow + 1;

	/* read from scan->cur_seg */
	for (AttrNumber i = 0; i < scan->columnScanInfo.num_proj_atts; i++)
	{
		AttrNumber attno = scan->columnScanInfo.proj_atts[i];
		DatumStreamRead *ds = scan->columnScanInfo.ds[attno];
		int64 startrow = scan->segfirstrow + scan->segrowsprocessed;
		bool chkvisimap = true;

		/*
		 * After scanning the anchor column, we check if the value is 
		 * missing in this column, if so, we skip the scanning.
		 */
		if (attno != scan->columnScanInfo.proj_atts[ANCHOR_COL_IN_PROJ])
		{
			/* we should've got a valid phyrow when scanning the anchor column */
			Assert(phyrow > 0);
			if (AO_ATTR_VAL_IS_MISSING(phyrow,
														attno,
														scan->seginfo[scan->cur_seg]->segno,
														scan->columnScanInfo.attnum_to_rownum))
			{
				slot->tts_values[attno] = getmissingattr(slot->tts_tupleDescriptor,
															attno + 1,
															&slot->tts_isnull[attno]);
				continue;
			}
		}

		if (ds->blockRowCount <= 0)
			; /* haven't read block */
		else
		{
			/* block was read */
			rowcount = aocs_block_remaining_rows(ds);
			Assert(rowcount >= 0);

			if (startrow + rowcount - 1 >= targrow)
			{
				/* read the value, visimap check only needed for the anchor column */
				phyrow = aocs_gettuple_column(scan,
											attno, startrow, targrow, 
											i == ANCHOR_COL_IN_PROJ ? &chkvisimap : NULL,
											slot);
				if (!chkvisimap)
					ret = false;

				/* haven't finished scanning on current block */
				continue;
			}
			else
				startrow += rowcount; /* skip scanning remaining rows */
		}

		/*
		 * Keep reading block headers until we find the block containing
		 * the target row.
		 */
		while (true)
		{
			if (datumstreamread_block_info(ds))
			{
				rowcount = ds->blockRowCount;
				Assert(rowcount > 0);

				/* new block, reset blockRowsProcessed */
				ds->blockRowsProcessed = 0;

				if (startrow + rowcount - 1 >= targrow)
				{
					int64 blocksRead;

					/* read a new buffer to consume */
					datumstreamread_block_content(ds);

					AOCSScanDesc_UpdateTotalBytesRead(scan, attno);
					blocksRead =
						RelationGuessNumberOfBlocksFromSize(scan->totalBytesRead);
					pgstat_count_buffer_read_ao(scan->rs_base.rs_rd,
												blocksRead);

					/* read the value, visimap check only needed for the anchor column */
					phyrow = aocs_gettuple_column(scan, 
												attno, startrow, targrow, 
												i == ANCHOR_COL_IN_PROJ ? &chkvisimap : NULL,
												slot);
					if (!chkvisimap)
						ret = false;

					/* done this column */
					break;
				}

				startrow += rowcount;
				AppendOnlyStorageRead_SkipCurrentBlock(&ds->ao_read);
				/* continue next block */
			}
			else
				pg_unreachable(); /* unreachable code */
		}
	}

	/* update rows processed */
	scan->segrowsprocessed = rowstoprocess;

	if (ret)
	{
		ExecStoreVirtualTuple(slot);

		pgstat_count_heap_getnext(scan->rs_base.rs_rd);
	}

	return ret;
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
aocs_get_target_tuple(AOCSScanDesc aoscan, int64 targrow, TupleTableSlot *slot)
{
	if (aoscan->columnScanInfo.relationTupleDesc == NULL)
	{
		aoscan->columnScanInfo.relationTupleDesc = slot->tts_tupleDescriptor;
		/* Pin it! ... and of course release it upon destruction / rescan */
		PinTupleDesc(aoscan->columnScanInfo.relationTupleDesc);
		initscan_with_colinfo(aoscan);
	}

	if (aoscan->blkdirscan != NULL)
		return aocs_blkdirscan_get_target_tuple(aoscan, targrow, slot);

	if (aocs_getsegment(aoscan, targrow) < 0)
	{
		/* all done */
		ExecClearTuple(slot);
		return false;
	}

	/*
	 * Unlike AO_ROW, AO_COLUMN may have different varblocks
	 * for different columns, so we get per-column tuple directly
	 * on the way of walking per-column varblock.
	 */
	return aocs_gettuple(aoscan, targrow, slot);
}

bool
aocs_getnext(AOCSScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	Datum	   *d = slot->tts_values;
	bool	   *null = slot->tts_isnull;

	AOTupleId	aoTupleId;
	int64		rowNum = InvalidAORowNum;
	int64		nthInBlock;
	int			err = 0;
	bool		isSnapshotAny = (scan->rs_base.rs_snapshot == SnapshotAny);
	AttrNumber	natts;

	Assert(ScanDirectionIsForward(direction));

	/* should not be in ANALYZE/SampleScan - we use a different API */
	Assert((scan->rs_base.rs_flags & SO_TYPE_ANALYZE) == 0);
	Assert((scan->rs_base.rs_flags & SO_TYPE_SAMPLESCAN) == 0);

	if (scan->columnScanInfo.relationTupleDesc == NULL)
	{
		scan->columnScanInfo.relationTupleDesc = slot->tts_tupleDescriptor;
		/* Pin it! ... and of course release it upon destruction / rescan */
		PinTupleDesc(scan->columnScanInfo.relationTupleDesc);
		initscan_with_colinfo(scan);
	}

	natts = slot->tts_tupleDescriptor->natts;
	Assert(natts <= scan->columnScanInfo.relationTupleDesc->natts);

	while (1)
	{
		AOCSFileSegInfo *curseginfo;

ReadNext:
		/* If necessary, open next seg */
		if (scan->cur_seg < 0 || err < 0)
		{
			/*
			 * Bail out early if we do not have any column in the projection.
			 * Placing here in order to have less impact on the hot path. 
			 */
			if (scan->columnScanInfo.num_proj_atts == 0)
				return false;

			err = open_next_scan_seg(scan);
			if (err < 0)
			{
				/* No more seg, we are at the end */
				ExecClearTuple(slot);
				scan->cur_seg = -1;
				return false;
			}
			scan->segrowsprocessed = 0;
		}

		/* We shouldn't have a 0-column projection as we should've bailed out above */
		Assert(scan->columnScanInfo.num_proj_atts > 0);

		Assert(scan->cur_seg >= 0);
		curseginfo = scan->seginfo[scan->cur_seg];

		/* Read from cur_seg */
		for (AttrNumber i = 0; i < scan->columnScanInfo.num_proj_atts; i++)
		{
			AttrNumber	attno = scan->columnScanInfo.proj_atts[i];

			/*
			 * Check missing value before reading from data files.
			 * 
			 * We don't need to check the missing value for the anchor column.
			 * In fact, we cannot do that either because we don't have the
			 * row number until we've scanned the anchor column.
			 */
			if (attno != scan->columnScanInfo.proj_atts[ANCHOR_COL_IN_PROJ])
			{
				Assert(rowNum > 0);
				if (AO_ATTR_VAL_IS_MISSING(rowNum,
															attno,
															curseginfo->segno,
															scan->columnScanInfo.attnum_to_rownum))
				{
					/*
					 * XXX: should we temporarily store the missing value to avoid repeatedly calling
					 * getmissingattr? The performance gain seems not much though. 
					 */
					d[attno] = getmissingattr(slot->tts_tupleDescriptor, attno + 1, &null[attno]);
					continue;
				}
			}

			/* otherwise, read from data file */
			err = datumstreamread_advance(scan->columnScanInfo.ds[attno]);
			Assert(err >= 0);
			if (err == 0)
			{
				err = datumstreamread_block(scan->columnScanInfo.ds[attno], scan->blockDirectory, attno);
				if (err < 0)
				{
					/*
					 * Ha, cannot read next block, we need to go to next seg
					 */
					close_cur_scan_seg(scan);
					goto ReadNext;
				}

				AOCSScanDesc_UpdateTotalBytesRead(scan, attno);
				pgstat_count_buffer_read_ao(scan->rs_base.rs_rd,
											RelationGuessNumberOfBlocksFromSize(scan->totalBytesRead));

				err = datumstreamread_advance(scan->columnScanInfo.ds[attno]);
				Assert(err > 0);
			}

			/*
			 * Get the column's datum right here since the data structures
			 * should still be hot in CPU data cache memory.
			 */
			datumstreamread_get(scan->columnScanInfo.ds[attno], &d[attno], &null[attno]);

			nthInBlock = datumstreamread_nth(scan->columnScanInfo.ds[attno]);
			if (rowNum == InvalidAORowNum &&
				scan->columnScanInfo.ds[attno]->blockFirstRowNum != InvalidAORowNum)
			{
				Assert(scan->columnScanInfo.ds[attno]->blockFirstRowNum > 0 && nthInBlock >= 0);
				rowNum = scan->columnScanInfo.ds[attno]->blockFirstRowNum + nthInBlock;
			}
#ifdef USE_ASSERT_CHECKING
			/*
			 * the row number from every column should match
			 * XXX: the first assert is repeated code, we should move it outside of
			 * the if/else block if we can be sure blockFirstRowNum cannot be -1 here.
			 */
			else if (scan->columnScanInfo.ds[attno]->blockFirstRowNum != InvalidAORowNum)
			{
				Assert(scan->columnScanInfo.ds[attno]->blockFirstRowNum > 0 && nthInBlock >= 0);
				Assert(rowNum == scan->columnScanInfo.ds[attno]->blockFirstRowNum + nthInBlock);
			}
#endif
		}

		scan->segrowsprocessed++;
		if (rowNum == InvalidAORowNum)
		{
			AOTupleIdInit(&aoTupleId, curseginfo->segno, scan->segrowsprocessed);
		}
		else
		{
			AOTupleIdInit(&aoTupleId, curseginfo->segno, rowNum);
		}

		if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&scan->visibilityMap, &aoTupleId))
		{
			/* The tuple is invisible */
			rowNum = InvalidAORowNum;
			goto ReadNext;
		}
		scan->cdb_fake_ctid = *((ItemPointer) &aoTupleId);

		slot->tts_nvalid = natts;
		slot->tts_tid = scan->cdb_fake_ctid;
		return true;
	}

	Assert(!"Never here");
	return false;
}


/* Open next file segment for write.  See SetCurrentFileSegForWrite */
/* XXX Right now, we put each column to different files */
static void
OpenAOCSDatumStreams(AOCSInsertDesc desc)
{
	RelFileNodeBackend rnode;
	char	   *basepath;
	char		fn[MAXPGPATH];
	int32		fileSegNo;

	AOCSFileSegInfo *seginfo;

	TupleDesc	tupdesc = RelationGetDescr(desc->aoi_rel);
	int			nvp = tupdesc->natts;
	int			i;

	desc->ds = (DatumStreamWrite **) palloc0(sizeof(DatumStreamWrite *) * nvp);

	open_ds_write(desc->aoi_rel, desc->ds, tupdesc,
				  desc->checksum);

	/* Now open seg info file and get eof mark. */
	seginfo = GetAOCSFileSegInfo(desc->aoi_rel,
								 desc->appendOnlyMetaDataSnapshot,
								 desc->cur_segno,
								 true);

	desc->fsInfo = seginfo;

	/* Never insert into a segment that is awaiting a drop */
	if (desc->fsInfo->state == AOSEG_STATE_AWAITING_DROP)
		elog(ERROR,
			 "cannot insert into segno (%d) for AO relid %d that is in state AOSEG_STATE_AWAITING_DROP",
			 desc->cur_segno, RelationGetRelid(desc->aoi_rel));

	desc->rowCount = seginfo->total_tupcount;

	rnode.node = desc->aoi_rel->rd_node;
	rnode.backend = desc->aoi_rel->rd_backend;
	basepath = relpath(rnode, MAIN_FORKNUM);

	for (i = 0; i < nvp; ++i)
	{
		AOCSVPInfoEntry *e = getAOCSVPEntry(seginfo, i);

		/* Filenum for the column */
		FileNumber filenum = GetFilenumForAttribute(RelationGetRelid(desc->aoi_rel), i + 1);

		FormatAOSegmentFileName(basepath, seginfo->segno, filenum, &fileSegNo, fn);
		Assert(strlen(fn) + 1 <= MAXPGPATH);

		datumstreamwrite_open_file(desc->ds[i], fn, e->eof, e->eof_uncompressed,
								   &rnode,
								   fileSegNo, seginfo->formatversion);
	}

	pfree(basepath);
}

static inline void
SetBlockFirstRowNums(DatumStreamWrite **datumStreams,
					 int numDatumStreams,
					 int64 blockFirstRowNum)
{
	int			i;

	Assert(datumStreams != NULL);

	for (i = 0; i < numDatumStreams; i++)
	{
		Assert(datumStreams[i] != NULL);

		datumStreams[i]->blockFirstRowNum = blockFirstRowNum;
	}
}


AOCSInsertDesc
aocs_insert_init(Relation rel, int segno, int64 num_rows)
{
    NameData    nd;
	AOCSInsertDesc desc;
	TupleDesc	tupleDesc;
	int64		firstSequence = 0;

	desc = (AOCSInsertDesc) palloc0(sizeof(AOCSInsertDescData));
	desc->aoi_rel = rel;
	desc->appendOnlyMetaDataSnapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

	/*
	 * Writers uses this since they have exclusive access to the lock acquired
	 * with LockRelationAppendOnlySegmentFile for the segment-file.
	 */

	tupleDesc = RelationGetDescr(desc->aoi_rel);

	Assert(segno >= 0);
	desc->cur_segno = segno;

    GetAppendOnlyEntryAttributes(rel->rd_id,
                                 &desc->blocksz,
                                 (int16 *)&desc->compLevel,
                                 &desc->checksum,
                                 &nd);
    desc->compType = NameStr(nd);

    GetAppendOnlyEntryAuxOids(rel,
                              &desc->segrelid, &desc->blkdirrelid,
                              &desc->visimaprelid);

	OpenAOCSDatumStreams(desc);

	/*
	 * Obtain the next list of fast sequences for this relation.
	 *
	 * Even in the case of no indexes, we need to update the fast sequences,
	 * since the table may contain indexes at some point of time.
	 */
	firstSequence = GetFastSequences(desc->segrelid, segno, num_rows);
	desc->numSequences = num_rows;
	Assert(firstSequence > desc->rowCount);
	desc->lastSequence = firstSequence - 1;

	SetBlockFirstRowNums(desc->ds, tupleDesc->natts, desc->lastSequence + 1);

	/* Initialize the block directory. */
	tupleDesc = RelationGetDescr(rel);
	AppendOnlyBlockDirectory_Init_forInsert(&(desc->blockDirectory),
											desc->appendOnlyMetaDataSnapshot,	/* CONCERN: Safe to
																				 * assume all block
																				 * directory entries for
																				 * segment are "covered"
																				 * by same exclusive
																				 * lock. */
											(FileSegInfo *) desc->fsInfo, desc->lastSequence,
											rel, segno, tupleDesc->natts, true);

	return desc;
}


void
aocs_insert_values(AOCSInsertDesc idesc, Datum *d, bool *null, AOTupleId *aoTupleId)
{
	Relation	rel = idesc->aoi_rel;
	int			i;

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
								   "appendonly_insert",
								   DDLNotSpecified,
								   "",	/* databaseName */
								   RelationGetRelationName(idesc->aoi_rel));	/* tableName */
#endif

	/* As usual, at this moment, we assume one col per vp */
	for (i = 0; i < RelationGetNumberOfAttributes(rel); ++i)
	{
		void	   *toFree1;
		Datum		datum = d[i];
		int			err = datumstreamwrite_put(idesc->ds[i], datum, null[i], &toFree1);

		if (toFree1 != NULL)
		{
			/*
			 * Use the de-toasted and/or de-compressed as datum instead.
			 */
			datum = PointerGetDatum(toFree1);
		}
		if (err < 0)
		{
			int			itemCount = datumstreamwrite_nth(idesc->ds[i]);
			void	   *toFree2;

			/* write the block up to this one */
			datumstreamwrite_block(idesc->ds[i], &idesc->blockDirectory, i);
			if (itemCount > 0)
			{
				/*
				 * since we have written all up to the new tuple, the new
				 * blockFirstRowNum is the inserted tuple's row number
				 */
				idesc->ds[i]->blockFirstRowNum = idesc->lastSequence + 1;
			}

			Assert(idesc->ds[i]->blockFirstRowNum == idesc->lastSequence + 1);


			/* now write this new item to the new block */
			err = datumstreamwrite_put(idesc->ds[i], datum, null[i], &toFree2);
			Assert(toFree2 == NULL);
			if (err < 0)
			{
				Assert(!null[i]);
				err = datumstreamwrite_lob(idesc->ds[i],
										   datum,
										   &idesc->blockDirectory,
										   i);
				Assert(err >= 0);

				/*
				 * A lob will live by itself in the block so this assignment
				 * is for the block that contains tuples AFTER the one we are
				 * inserting
				 */
				idesc->ds[i]->blockFirstRowNum = idesc->lastSequence + 2;
			}
		}

		if (toFree1 != NULL)
			pfree(toFree1);
	}

	idesc->insertCount++;
	idesc->lastSequence++;
	if (idesc->numSequences > 0)
		(idesc->numSequences)--;

	Assert(idesc->numSequences >= 0);

	AOTupleIdInit(aoTupleId, idesc->cur_segno, idesc->lastSequence);

	/*
	 * If the allocated fast sequence numbers are used up, we request for a
	 * next list of fast sequence numbers.
	 */
	if (idesc->numSequences == 0)
	{
		int64 firstSequence PG_USED_FOR_ASSERTS_ONLY = GetFastSequences(idesc->segrelid,
																		idesc->cur_segno,
																		NUM_FAST_SEQUENCES);

		Assert(firstSequence == idesc->lastSequence + 1);
		idesc->numSequences = NUM_FAST_SEQUENCES;
	}
}

void
aocs_insert_finish(AOCSInsertDesc idesc)
{
	Relation	rel = idesc->aoi_rel;
	int			i;

	for (i = 0; i < rel->rd_att->natts; ++i)
	{
		datumstreamwrite_block(idesc->ds[i], &idesc->blockDirectory, i);
		datumstreamwrite_close_file(idesc->ds[i]);
	}

	AppendOnlyBlockDirectory_End_forInsert(&(idesc->blockDirectory));

	UpdateAOCSFileSegInfo(idesc);

	UnregisterSnapshot(idesc->appendOnlyMetaDataSnapshot);

	pfree(idesc->fsInfo);

	close_ds_write(idesc->ds, rel->rd_att->natts);
}

static void
positionFirstBlockOfRange(DatumStreamFetchDesc datumStreamFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetBeginRange(
												&datumStreamFetchDesc->currentBlock.blockDirectoryEntry,
												&datumStreamFetchDesc->scanNextFileOffset,
												&datumStreamFetchDesc->scanNextRowNum);
}

static void
positionLimitToEndOfRange(DatumStreamFetchDesc datumStreamFetchDesc)
{
	AppendOnlyBlockDirectoryEntry_GetEndRange(
											  &datumStreamFetchDesc->currentBlock.blockDirectoryEntry,
											  &datumStreamFetchDesc->scanAfterFileOffset,
											  &datumStreamFetchDesc->scanLastRowNum);
}


static void
positionSkipCurrentBlock(DatumStreamFetchDesc datumStreamFetchDesc)
{
	datumStreamFetchDesc->scanNextFileOffset =
		datumStreamFetchDesc->currentBlock.fileOffset +
		datumStreamFetchDesc->currentBlock.overallBlockLen;

	datumStreamFetchDesc->scanNextRowNum =
		datumStreamFetchDesc->currentBlock.lastRowNum + 1;
}

/*
 * Fetch the tuple's datum from the block indicated by the block directory entry
 * that covers the tuple, given the colno.
 */
static void
fetchFromCurrentBlock(AOCSFetchDesc aocsFetchDesc,
					  int64 rowNum,
					  TupleTableSlot *slot,
					  int colno)
{
	DatumStreamFetchDesc datumStreamFetchDesc =
	aocsFetchDesc->datumStreamFetchDesc[colno];
	DatumStreamRead *datumStream = datumStreamFetchDesc->datumStream;
	Datum		value;
	bool		null;
	int			rowNumInBlock = rowNum - datumStreamFetchDesc->currentBlock.firstRowNum;

	Assert(rowNumInBlock >= 0);

	/*
	 * MPP-17061: gotContents could be false in the case of aborted rows. As
	 * described in the repro in MPP-17061, if aocs_fetch is trying to fetch
	 * an invisible/aborted row, it could set the block header metadata of
	 * currentBlock to the next CO block, but without actually reading in next
	 * CO block's content.
	 */
	if (datumStreamFetchDesc->currentBlock.gotContents == false)
	{
		datumstreamread_block_content(datumStream);
		datumStreamFetchDesc->currentBlock.gotContents = true;
	}

	datumstreamread_find(datumStream, rowNumInBlock);

	if (slot != NULL)
	{
		Datum	   *values = slot->tts_values;
		bool	   *nulls = slot->tts_isnull;

		datumstreamread_get(datumStream, &(values[colno]), &(nulls[colno]));

	}
	else
	{
		datumstreamread_get(datumStream, &value, &null);
	}
}

static bool
scanToFetchValue(AOCSFetchDesc aocsFetchDesc,
				 int64 rowNum,
				 TupleTableSlot *slot,
				 int colno)
{
	DatumStreamFetchDesc 			datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colno];
	DatumStreamRead 				*datumStream = datumStreamFetchDesc->datumStream;
	AOFetchBlockMetadata 			*currentBlock = &datumStreamFetchDesc->currentBlock;
	AppendOnlyBlockDirectoryEntry 	*entry = &currentBlock->blockDirectoryEntry;
	bool							found;

	found = datumstreamread_find_block(datumStream,
									   datumStreamFetchDesc,
									   rowNum);
	if (!found)
	{
		if (AppendOnlyBlockDirectoryEntry_RangeHasRow(entry, rowNum))
		{
			/*
			 * We fell into a hole inside the resolved block directory entry
			 * we obtained from AppendOnlyBlockDirectory_GetEntry().
			 * This should not be happening for versions >= GP7. Scream
			 * appropriately. See AppendOnlyBlockDirectoryEntry for details.
			 */
			ereportif(aocsFetchDesc->relation->rd_appendonly->version >= AORelationVersion_GP7,
					  ERROR,
					  (errcode(ERRCODE_INTERNAL_ERROR),
					   errmsg("datum with row number %ld and col no %d not found in block directory entry range", rowNum, colno),
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
	else
		fetchFromCurrentBlock(aocsFetchDesc, rowNum, slot, colno);

	return found;
}

static void
closeFetchSegmentFile(DatumStreamFetchDesc datumStreamFetchDesc)
{
	Assert(datumStreamFetchDesc->currentSegmentFile.isOpen);

	datumstreamread_close_file(datumStreamFetchDesc->datumStream);
	datumStreamFetchDesc->currentSegmentFile.isOpen = false;
}

static bool
openFetchSegmentFile(AOCSFetchDesc aocsFetchDesc,
					 int openSegmentFileNum,
					 int colNo)
{
	int			i;

	AOCSFileSegInfo *fsInfo;
	int			segmentFileNum;
	int64		logicalEof;
	DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colNo];

	Assert(!datumStreamFetchDesc->currentSegmentFile.isOpen);

	i = 0;
	while (true)
	{
		if (i >= aocsFetchDesc->totalSegfiles)
			return false;
		/* Segment file not visible in catalog information. */

		fsInfo = aocsFetchDesc->segmentFileInfo[i];
		segmentFileNum = fsInfo->segno;
		if (openSegmentFileNum == segmentFileNum)
		{
			break;
		}
		i++;
	}

	/*
	 * Don't try to open a segment file when its EOF is 0, since the file may
	 * not exist. See MPP-8280. Also skip the segment file if it is awaiting a
	 * drop.
	 *
	 * Check for awaiting-drop first, before accessing the vpinfo, because
	 * vpinfo might not be valid on awaiting-drop segment after adding a column.
	 */
	if (fsInfo->state == AOSEG_STATE_AWAITING_DROP)
		return false;

	AOCSVPInfoEntry *entry = getAOCSVPEntry(fsInfo, colNo);
	logicalEof = entry->eof;
	if (logicalEof == 0)
		return false;

	open_datumstreamread_segfile(aocsFetchDesc->basepath, aocsFetchDesc->relation,
								 fsInfo,
								 datumStreamFetchDesc->datumStream,
								 colNo);

	datumStreamFetchDesc->currentSegmentFile.num = openSegmentFileNum;
	datumStreamFetchDesc->currentSegmentFile.logicalEof = logicalEof;

	datumStreamFetchDesc->currentSegmentFile.isOpen = true;

	return true;
}

/*
 * Note: we don't reset the block directory entry here. This is crucial, so we
 * can use the block directory entry later on. See comment in AOFetchBlockMetadata
 * FIXME: reset other fields here.
 */
static void
resetCurrentBlockInfo(AOFetchBlockMetadata *currentBlock)
{
	currentBlock->valid = false;
	currentBlock->firstRowNum = 0;
	currentBlock->lastRowNum = 0;
}

/*
 * Initialize the fetch descriptor.
 */
AOCSFetchDesc
aocs_fetch_init(Relation relation,
				Snapshot snapshot,
				Snapshot appendOnlyMetaDataSnapshot,
				bool *proj)
{
	AOCSFetchDesc aocsFetchDesc;
	int			colno;
	char	   *basePath = relpathbackend(relation->rd_node, relation->rd_backend, MAIN_FORKNUM);
	TupleDesc	tupleDesc = RelationGetDescr(relation);
	StdRdOptions **opts = RelationGetAttributeOptions(relation);
	int			segno;

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	aocsFetchDesc = (AOCSFetchDesc) palloc0(sizeof(AOCSFetchDescData));
	aocsFetchDesc->relation = relation;

	aocsFetchDesc->appendOnlyMetaDataSnapshot = appendOnlyMetaDataSnapshot;
	aocsFetchDesc->snapshot = snapshot;


	aocsFetchDesc->initContext = CurrentMemoryContext;

	aocsFetchDesc->segmentFileNameMaxLen = AOSegmentFilePathNameLen(relation) + 1;
	aocsFetchDesc->segmentFileName =
		(char *) palloc(aocsFetchDesc->segmentFileNameMaxLen);
	aocsFetchDesc->segmentFileName[0] = '\0';
	aocsFetchDesc->basepath = basePath;

	/* lastrownums info */
	aocsFetchDesc->attnum_to_rownum = GetAttnumToLastrownumMapping(
		RelationGetRelid(relation),RelationGetNumberOfAttributes(relation));

	Assert(proj);

    bool checksum = true;
    Oid visimaprelid;
    GetAppendOnlyEntryAuxOids(relation,
                              &aocsFetchDesc->segrelid, NULL,
                              &visimaprelid);

    GetAppendOnlyEntryAttributes(relation->rd_id,
                                 NULL,
                                 NULL,
                                 &checksum,
                                 NULL);

	aocsFetchDesc->segmentFileInfo =
		GetAllAOCSFileSegInfo(relation, appendOnlyMetaDataSnapshot, &aocsFetchDesc->totalSegfiles, NULL);

	/* 
	 * Initialize lastSequence only for segments which we got above is sufficient,
	 * rather than all AOTupleId_MultiplierSegmentFileNum ones that introducing
	 * too many unnecessary calls in most cases.
	 */
	memset(aocsFetchDesc->lastSequence, InvalidAORowNum, sizeof(aocsFetchDesc->lastSequence));
	for (int i = -1; i < aocsFetchDesc->totalSegfiles; i++)
	{
		/* always initailize segment 0 */
		segno = (i < 0 ? 0 : aocsFetchDesc->segmentFileInfo[i]->segno);
		/* set corresponding bit for target segment */
		aocsFetchDesc->lastSequence[segno] = ReadLastSequence(aocsFetchDesc->segrelid, segno);
	}

	AppendOnlyBlockDirectory_Init_forSearch(
											&aocsFetchDesc->blockDirectory,
											appendOnlyMetaDataSnapshot,
											(FileSegInfo **) aocsFetchDesc->segmentFileInfo,
											aocsFetchDesc->totalSegfiles,
											aocsFetchDesc->relation,
											relation->rd_att->natts,
											true,
											proj);

	Assert(relation->rd_att != NULL);

	aocsFetchDesc->datumStreamFetchDesc = (DatumStreamFetchDesc *)
		palloc0(relation->rd_att->natts * sizeof(DatumStreamFetchDesc));

	for (colno = 0; colno < relation->rd_att->natts; colno++)
	{

		aocsFetchDesc->datumStreamFetchDesc[colno] = NULL;
		/* always need to initialize for the anchor column */
		if (proj[colno] || colno == aocsFetchDesc->blockDirectory.proj_atts[ANCHOR_COL_IN_PROJ])
		{
			char	   *ct;
			int32		clvl;
			int32		blksz;

			StringInfoData titleBuf;

			/*
			 * We always record all the three column specific attributes for
			 * each column of a column oriented table. Note: checksum is a
			 * table level attribute.
			 */
			Assert(opts[colno]);
			ct = opts[colno]->compresstype;
			clvl = opts[colno]->compresslevel;
			blksz = opts[colno]->blocksize;

			/* UNDONE: Need to track and dispose of this storage... */
			initStringInfo(&titleBuf);
			appendStringInfo(&titleBuf, "Fetch from Append-Only Column-Oriented relation '%s', column #%d '%s'",
							 RelationGetRelationName(relation),
							 colno + 1,
							 NameStr(TupleDescAttr(tupleDesc, colno)->attname));

			aocsFetchDesc->datumStreamFetchDesc[colno] = (DatumStreamFetchDesc)
				palloc0(sizeof(DatumStreamFetchDescData));

			aocsFetchDesc->datumStreamFetchDesc[colno]->datumStream =
				create_datumstreamread(ct,
									   clvl,
									   checksum,
									   blksz,
									   TupleDescAttr(tupleDesc, colno),
									   relation->rd_rel->relname.data,
									    /* title */ titleBuf.data);

		}
		if (opts[colno])
			pfree(opts[colno]);
	}
	if (opts)
		pfree(opts);
	AppendOnlyVisimap_Init(&aocsFetchDesc->visibilityMap,
						   visimaprelid,
						   AccessShareLock,
						   appendOnlyMetaDataSnapshot);

	return aocsFetchDesc;
}

/*
 * Similar to AppendOnlyBlockDirectory_GetEntry, but taking into consideration that
 * some AOCO column might be missing values and might not have block directory
 * entry for the rowNum we are checking.
 */
static bool
aocs_blkdir_getentry(AOCSFetchDesc aocsFetchDesc,
							AOTupleId *aoTupleId,
							int columnGroupNo,
							AppendOnlyBlockDirectoryEntry *directoryEntry)
{
	int 		ret;
	int 		segmentFileNum = AOTupleIdGet_segmentFileNum(aoTupleId);
	int64 		rowNum = AOTupleIdGet_rowNum(aoTupleId); 
	bool 		valmissing = columnGroupNo != aocsFetchDesc->blockDirectory.proj_atts[ANCHOR_COL_IN_PROJ] &&
								AO_ATTR_VAL_IS_MISSING(rowNum,
											columnGroupNo,
											segmentFileNum,
											aocsFetchDesc->attnum_to_rownum);

	if (valmissing)
	{
		/*
		 * If value is missing, we know there's no blkdir entry for it, so no
		 * point checking. Instead, check the anchor column for the confirm.
		 */
		ret = blkdir_entry_exists(&aocsFetchDesc->blockDirectory, aoTupleId, aocsFetchDesc->blockDirectory.proj_atts[ANCHOR_COL_IN_PROJ]);
	}
	else
	{
		/*
		 * If value is not missing, conduct the regular blkdir entry check.
		 */
		ret = AppendOnlyBlockDirectory_GetEntry(&aocsFetchDesc->blockDirectory,
										   aoTupleId,
										   columnGroupNo,
										   directoryEntry,
										   aocsFetchDesc->attnum_to_rownum);
	}
	return ret;
}

/*
 * Fetch the tuple based on the given tuple id.
 *
 * If the 'slot' is not NULL, the tuple will be assigned to the slot.
 *
 * Return true if the tuple is found. Otherwise, return false.
 */
bool
aocs_fetch(AOCSFetchDesc aocsFetchDesc,
		   AOTupleId *aoTupleId,
		   TupleTableSlot *slot)
{
	int			segmentFileNum = AOTupleIdGet_segmentFileNum(aoTupleId);
	int64		rowNum = AOTupleIdGet_rowNum(aoTupleId);
	int			numCols = aocsFetchDesc->relation->rd_att->natts;
	int			colno = 0; /* initialize to placate compiler, but it should be overwritten later in any case */
	bool		found = true;
	bool		isSnapshotAny = (aocsFetchDesc->snapshot == SnapshotAny);
	bool 		valmissing;

	Assert(numCols > 0);

	Assert(segmentFileNum >= 0);

	if (aocsFetchDesc->lastSequence[segmentFileNum] == InvalidAORowNum)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Row No. %ld in segment file No. %d is out of scanning scope for target relfilenode %u.",
				 		rowNum, segmentFileNum, aocsFetchDesc->relation->rd_node.relNode)));

	/*
	 * skip if the rowNum is not discoverable:
	 * 1. rowNum is 0 which is invalid.
	 * 2. rowNum bigger than lastsequence of the segment.
	 */
	if (rowNum == 0 || rowNum > aocsFetchDesc->lastSequence[segmentFileNum])
	{
		if (slot != NULL)
			slot = ExecClearTuple(slot);
		return false;
	}

	/*
	 * Go through columns one by one. Check if the current block has the
	 * requested tuple. If so, fetch it. Otherwise, read the block that
	 * contains the requested tuple.
	 */
	for (int i = 0; i < aocsFetchDesc->blockDirectory.num_proj_atts; i++)
	{
		colno = aocsFetchDesc->blockDirectory.proj_atts[i];

		DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colno];

		/* If this column does not need to be fetched, skip it. */
		if (datumStreamFetchDesc == NULL)
			continue;

		elogif(Debug_appendonly_print_datumstream, LOG,
			   "aocs_fetch filePathName %s segno %u rowNum  " INT64_FORMAT
			   " firstRowNum " INT64_FORMAT " lastRowNum " INT64_FORMAT " ",
			   datumStreamFetchDesc->datumStream->ao_read.bufferedRead.filePathName,
			   datumStreamFetchDesc->currentSegmentFile.num,
			   rowNum,
			   datumStreamFetchDesc->currentBlock.firstRowNum,
			   datumStreamFetchDesc->currentBlock.lastRowNum);

		/*
		 * If the current block has the requested tuple, read it.
		 */
		if (datumStreamFetchDesc->currentSegmentFile.isOpen &&
			datumStreamFetchDesc->currentSegmentFile.num == segmentFileNum &&
			aocsFetchDesc->blockDirectory.currentSegmentFileNum == segmentFileNum &&
			datumStreamFetchDesc->currentBlock.valid)
		{
			if (rowNum >= datumStreamFetchDesc->currentBlock.firstRowNum &&
				rowNum <= datumStreamFetchDesc->currentBlock.lastRowNum)
			{
				if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aocsFetchDesc->visibilityMap, aoTupleId))
				{
					found = false;
					break;
				}

				/* Check any missing value before reading from the block. */
				if (colno != aocsFetchDesc->blockDirectory.proj_atts[ANCHOR_COL_IN_PROJ])
				{
					Assert(rowNum > 0);
					if (AO_ATTR_VAL_IS_MISSING(rowNum,
														colno,
														segmentFileNum,
														aocsFetchDesc->attnum_to_rownum))
					{
						slot->tts_values[colno] = getmissingattr(slot->tts_tupleDescriptor,
															colno + 1,
															&slot->tts_isnull[colno]);
						continue;
					}
				}

				/* not a missing value, fetch from the current block */
				fetchFromCurrentBlock(aocsFetchDesc, rowNum, slot, colno);
				continue;
			}

			/*
			 * Otherwise, fetch the right block.
			 */
			if (AppendOnlyBlockDirectoryEntry_RangeHasRow(
														  &(datumStreamFetchDesc->currentBlock.blockDirectoryEntry),
														  rowNum))
			{
				/*
				 * The tuple is covered by the current Block Directory entry,
				 * but is it before or after our current block?
				 */
				if (rowNum < datumStreamFetchDesc->currentBlock.firstRowNum)
				{
					/*
					 * Set scan range to prior block
					 */
					positionFirstBlockOfRange(datumStreamFetchDesc);

					datumStreamFetchDesc->scanAfterFileOffset =
						datumStreamFetchDesc->currentBlock.fileOffset;
					datumStreamFetchDesc->scanLastRowNum =
						datumStreamFetchDesc->currentBlock.firstRowNum - 1;
				}
				else
				{
					/*
					 * Set scan range to following blocks.
					 */
					positionSkipCurrentBlock(datumStreamFetchDesc);
					positionLimitToEndOfRange(datumStreamFetchDesc);
				}

				if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aocsFetchDesc->visibilityMap, aoTupleId))
				{
					found = false;
					break;
				}

				if (!scanToFetchValue(aocsFetchDesc, rowNum, slot, colno))
				{
					found = false;
					break;
				}

				continue;
			}
		}

		/*
		 * Open or switch open, if necessary.
		 */
		if (datumStreamFetchDesc->currentSegmentFile.isOpen &&
			segmentFileNum != datumStreamFetchDesc->currentSegmentFile.num)
		{
			closeFetchSegmentFile(datumStreamFetchDesc);

			Assert(!datumStreamFetchDesc->currentSegmentFile.isOpen);
		}

		/*
		 * Check if this row value is missing in the column.
		 * Note that, however, even if the value is missing we cannot just
		 * fetch the missing value and continue at this point. We still need
		 * to go through the the block directory and the visibilityMap checks.
		 */
		valmissing = colno != aocsFetchDesc->blockDirectory.proj_atts[ANCHOR_COL_IN_PROJ] &&
									AO_ATTR_VAL_IS_MISSING(rowNum,
												colno,
												segmentFileNum,
												aocsFetchDesc->attnum_to_rownum);

		if (!datumStreamFetchDesc->currentSegmentFile.isOpen)
		{
			/* no point opening the file if the value is missing in it */
			if (!valmissing && !openFetchSegmentFile(aocsFetchDesc,
									  segmentFileNum,
									  colno))
			{
				found = false;
				/* Segment file not in aoseg table.. */
				/* Must be aborted or deleted and reclaimed. */
				break;
			}

			/* Reset currentBlock info */
			resetCurrentBlockInfo(&(datumStreamFetchDesc->currentBlock));
		}

		/*
		 * Need to get the Block Directory entry that covers the TID.
		 */
		if (!aocs_blkdir_getentry(aocsFetchDesc, aoTupleId, colno, &datumStreamFetchDesc->currentBlock.blockDirectoryEntry))
		{
			found = false;		/* Row not represented in Block Directory. */
			/* Must be aborted or deleted and reclaimed. */
			break;
		}

		if (!isSnapshotAny && !AppendOnlyVisimap_IsVisible(&aocsFetchDesc->visibilityMap, aoTupleId))
		{
			found = false;
			break;
		}

		/*
		 * Set scan range covered by new Block Directory entry.
		 */
		positionFirstBlockOfRange(datumStreamFetchDesc);

		positionLimitToEndOfRange(datumStreamFetchDesc);

		/* fetch missing value and proceed to next column */
		if (valmissing)
		{
			slot->tts_values[colno] = getmissingattr(slot->tts_tupleDescriptor,
												colno + 1,
												&slot->tts_isnull[colno]);

			continue;
		}

		/* otherwise, fetch from the segmentfile */
		if (!scanToFetchValue(aocsFetchDesc, rowNum, slot, colno))
		{
			found = false;
			break;
		}
	}

	if (found)
	{
		if (slot != NULL)
		{
			slot->tts_nvalid = colno;
			slot->tts_tid = *(ItemPointer)(aoTupleId);
		}
	}
	else
	{
		if (slot != NULL)
			slot = ExecClearTuple(slot);
	}

	return found;
}

void
aocs_fetch_finish(AOCSFetchDesc aocsFetchDesc)
{
	int			colno;
	Relation	relation = aocsFetchDesc->relation;

	Assert(relation != NULL && relation->rd_att != NULL);

	for (colno = 0; colno < relation->rd_att->natts; colno++)
	{
		DatumStreamFetchDesc datumStreamFetchDesc = aocsFetchDesc->datumStreamFetchDesc[colno];

		if (datumStreamFetchDesc != NULL)
		{
			Assert(datumStreamFetchDesc->datumStream != NULL);
			datumstreamread_close_file(datumStreamFetchDesc->datumStream);
			destroy_datumstreamread(datumStreamFetchDesc->datumStream);
			datumStreamFetchDesc->datumStream = NULL;
			pfree(datumStreamFetchDesc);
			aocsFetchDesc->datumStreamFetchDesc[colno] = NULL;
		}
	}
	pfree(aocsFetchDesc->datumStreamFetchDesc);

	AppendOnlyBlockDirectory_End_forSearch(&aocsFetchDesc->blockDirectory);

	if (aocsFetchDesc->segmentFileInfo)
	{
		FreeAllAOCSSegFileInfo(aocsFetchDesc->segmentFileInfo, aocsFetchDesc->totalSegfiles);
		pfree(aocsFetchDesc->segmentFileInfo);
		aocsFetchDesc->segmentFileInfo = NULL;
	}

	if (aocsFetchDesc->attnum_to_rownum)
	{
		pfree(aocsFetchDesc->attnum_to_rownum);
		aocsFetchDesc->attnum_to_rownum = NULL;
	}

	RelationDecrementReferenceCount(aocsFetchDesc->relation);

	pfree(aocsFetchDesc->segmentFileName);
	pfree(aocsFetchDesc->basepath);

	AppendOnlyVisimap_Finish(&aocsFetchDesc->visibilityMap, AccessShareLock);
}

AOCSIndexOnlyDesc
aocs_index_only_init(Relation relation, Snapshot snapshot)
{
	AOCSIndexOnlyDesc indexonlydesc = (AOCSIndexOnlyDesc) palloc0(sizeof(AppendOnlyIndexOnlyDescData));

	/* initialize the block directory */
	indexonlydesc->blockDirectory = palloc0(sizeof(AppendOnlyBlockDirectory));
	AppendOnlyBlockDirectory_Init_forIndexOnlyScan(indexonlydesc->blockDirectory,
												   relation,
												   relation->rd_att->natts, /* numColGroups */
												   snapshot);

	/* initialize the visimap */
	indexonlydesc->visimap = palloc0(sizeof(AppendOnlyVisimap));
	AppendOnlyVisimap_Init_forIndexOnlyScan(indexonlydesc->visimap,
											relation,
											snapshot);
	return indexonlydesc;										
}

bool
aocs_index_only_check(AOCSIndexOnlyDesc indexonlydesc, AOTupleId *aotid, Snapshot snapshot)
{
	if (!AppendOnlyBlockDirectory_CoversTuple(indexonlydesc->blockDirectory, aotid))
		return false;

	/* check SnapshotAny for the case when gp_select_invisible is on */
	if (snapshot != SnapshotAny && !AppendOnlyVisimap_IsVisible(indexonlydesc->visimap, aotid))
		return false;
	
	return true;
}

void
aocs_index_only_finish(AOCSIndexOnlyDesc indexonlydesc)
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
AOCSDeleteDesc
aocs_delete_init(Relation rel)
{
	/*
	 * Get the pg_appendonly information
	 */
	Oid visimaprelid;
	AOCSDeleteDesc aoDeleteDesc = palloc0(sizeof(AOCSDeleteDescData));

	aoDeleteDesc->aod_rel = rel;

    Snapshot snapshot = GetCatalogSnapshot(InvalidOid);

    GetAppendOnlyEntryAuxOids(rel,
                              NULL, NULL,
                              &visimaprelid);

	AppendOnlyVisimap_Init(&aoDeleteDesc->visibilityMap,
						   visimaprelid,
						   RowExclusiveLock,
						   snapshot);

	AppendOnlyVisimapDelete_Init(&aoDeleteDesc->visiMapDelete,
								 &aoDeleteDesc->visibilityMap);

	return aoDeleteDesc;
}

void
aocs_delete_finish(AOCSDeleteDesc aoDeleteDesc)
{
	Assert(aoDeleteDesc);

	AppendOnlyVisimapDelete_Finish(&aoDeleteDesc->visiMapDelete);
	AppendOnlyVisimap_Finish(&aoDeleteDesc->visibilityMap, NoLock);

	pfree(aoDeleteDesc);
}

TM_Result
aocs_delete(AOCSDeleteDesc aoDeleteDesc,
			AOTupleId *aoTupleId)
{
	Assert(aoDeleteDesc);
	Assert(aoTupleId);

	elogif(Debug_appendonly_print_delete, LOG,
		   "AOCS delete tuple from table '%s' (AOTupleId %s)",
		   NameStr(aoDeleteDesc->aod_rel->rd_rel->relname),
		   AOTupleIdToString(aoTupleId));

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
								   "appendonly_delete",
								   DDLNotSpecified,
								   "",	/* databaseName */
								   RelationGetRelationName(aoDeleteDesc->aod_rel)); /* tableName */
#endif

	return AppendOnlyVisimapDelete_Hide(&aoDeleteDesc->visiMapDelete, aoTupleId);
}

/*
 * Initialize a scan on varblock headers in an AOCS segfile.  The
 * segfile is identified by colno.
 */
AOCSHeaderScanDesc
aocs_begin_headerscan(Relation rel, int colno)
{
	AOCSHeaderScanDesc hdesc;
	AppendOnlyStorageAttributes ao_attr;
	StdRdOptions **opts = RelationGetAttributeOptions(rel);

	Assert(opts[colno]);

    GetAppendOnlyEntryAttributes(rel->rd_id,
                                 NULL,
                                 NULL,
                                 &ao_attr.checksum,
                                 NULL);

	/*
	 * We are concerned with varblock headers only, not their content.
	 * Therefore, don't waste cycles in decompressing the content.
	 */
	ao_attr.compress = false;
	ao_attr.compressType = NULL;
	ao_attr.compressLevel = 0;
	ao_attr.overflowSize = 0;
	hdesc = palloc(sizeof(AOCSHeaderScanDescData));
	AppendOnlyStorageRead_Init(&hdesc->ao_read,
							   NULL, //current memory context
							   opts[colno]->blocksize,
							   RelationGetRelationName(rel),
							   "ALTER TABLE ADD COLUMN scan",
							   &ao_attr);
	hdesc->colno = colno;
	hdesc->relid = RelationGetRelid(rel);

	for (int i = 0; i < RelationGetNumberOfAttributes(rel); i++)
		pfree(opts[i]);
	pfree(opts);

	return hdesc;
}

/*
 * Open AOCS segfile for scanning varblock headers.
 */
static void
aocs_headerscan_opensegfile(AOCSHeaderScanDesc hdesc,
							AOCSFileSegInfo *seginfo,
							char *basepath)
{
	AOCSVPInfoEntry *vpe;
	char		fn[MAXPGPATH];
	int32		fileSegNo;

	/* Filenum for the column */
	FileNumber	filenum = GetFilenumForAttribute(hdesc->relid, hdesc->colno + 1);

	/* Close currently open segfile, if any. */
	AppendOnlyStorageRead_CloseFile(&hdesc->ao_read);
	FormatAOSegmentFileName(basepath, seginfo->segno,
							filenum, &fileSegNo, fn);
	Assert(strlen(fn) + 1 <= MAXPGPATH);
	vpe = getAOCSVPEntry(seginfo, hdesc->colno);
	AppendOnlyStorageRead_OpenFile(&hdesc->ao_read, fn, seginfo->formatversion,
								   vpe->eof);
}

static bool
aocs_get_nextheader(AOCSHeaderScanDesc hdesc)
{
	if (hdesc->ao_read.current.firstRowNum > 0)
		AppendOnlyStorageRead_SkipCurrentBlock(&hdesc->ao_read);

	return AppendOnlyStorageRead_ReadNextBlock(&hdesc->ao_read);
}

static void
aocs_end_headerscan(AOCSHeaderScanDesc hdesc)
{
	AppendOnlyStorageRead_CloseFile(&hdesc->ao_read);
	AppendOnlyStorageRead_FinishSession(&hdesc->ao_read);
	pfree(hdesc);
}

/*
 * Initialize one datum stream per column for writing new files
 * in an add col/rewrite col operation.
 */
AOCSWriteColumnDesc
aocs_writecol_init(Relation rel, List *newvals, AOCSWriteColumnOperation op)
{
	char	   *ct;
	int32		clvl;
	int32               blksz;
	AOCSWriteColumnDesc desc;
	int                 i;
	StringInfoData titleBuf;
	bool        checksum;
	ListCell    *lc, *lc2;

	desc = palloc0(sizeof(AOCSWriteColumnDescData));
	desc->op = op;

	/*
	 * We filter out the newvals which may contain both of
	 * ADD COLUMN and ALTER COLUMN newcolvals
	 * into the column descriptor which will only have filtered list
	 * corresponding to that particular operation
	 */
	foreach(lc, newvals)
	{
		NewColumnValue *newval = lfirst(lc);
		if (op == newval->op || (op == AOCSADDCOLUMN && newval->op == AOCSADDCOLUMN_MISSINGMODE))
			desc->newcolvals = lappend(desc->newcolvals, newval);
	}

	desc->num_cols_to_write = list_length(desc->newcolvals);
	desc->rel = rel;
	desc->cur_segno = -1;

	/* Get existing attribute options. */
	StdRdOptions **opts = RelationGetAttributeOptions(rel);

	desc->dsw = palloc(sizeof(DatumStreamWrite *) * desc->num_cols_to_write);

	GetAppendOnlyEntryAttributes(rel->rd_id,
							NULL,
							NULL,
							&checksum,
							NULL);

	i = 0;
	foreach(lc, desc->newcolvals)
	{
		NewColumnValue *newval = lfirst(lc);
		AttrNumber attnum = newval->attnum;
		Form_pg_attribute attr = TupleDescAttr(rel->rd_att, attnum - 1);
		char *compresstype = NULL;
		int compresslevel = -1;
		int blocksize = -1;

		initStringInfo(&titleBuf);
		if (op == AOCSADDCOLUMN)
			appendStringInfo(&titleBuf, "ALTER TABLE ADD COLUMN new segfile");
		else
			appendStringInfo(&titleBuf, "ALTER TABLE REWRITE COLUMN new segfile");

		/* check any new encoding options, use those if applicable, otherwise use existing ones */
		foreach(lc2, newval->new_encoding)
		{
			DefElem *e = lfirst(lc2);
			Assert (e->defname);

			/* we should've already transformed and validated the options in phase 2 */
			if (pg_strcasecmp("compresstype", e->defname) == 0)
				compresstype = defGetString(e);
			else if (pg_strcasecmp("compresslevel", e->defname) == 0)
				compresslevel = pg_strtoint32(defGetString(e));
			else if (pg_strcasecmp("blocksize", e->defname) == 0)
				blocksize = pg_strtoint32(defGetString(e));
			else
				/* shouldn't happen, but throw a nice error message instead of Assert */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unrecognized column encoding option \'%s\' for column \'%s\'",
								e->defname, attr->attname.data)));
		}
		Assert(opts[attnum - 1]);
		ct = compresstype == NULL ? opts[attnum - 1]->compresstype : compresstype;
		clvl = compresslevel == -1 ? opts[attnum - 1]->compresslevel : compresslevel;
		blksz = blocksize == -1 ? opts[attnum - 1]->blocksize : blocksize;
		desc->dsw[i] = create_datumstreamwrite(ct, clvl, checksum, blksz,
											   attr, RelationGetRelationName(rel),
											   titleBuf.data,
											   XLogIsNeeded() && RelationNeedsWAL(rel));
		i++;
	}

	for (i = 0; i < RelationGetNumberOfAttributes(rel); i++)
		pfree(opts[i]);
	pfree(opts);

	return desc;
}

/*
 * Close segfiles for each column being written
 * as part of an add/rewrite column operation.
 */
static void
aocs_writecol_closefiles(AOCSWriteColumnDesc desc)
{
	int      	colno;
	ListCell    *lc;
	int			i = 0;

	Assert(desc->newcolvals->length == desc->num_cols_to_write);

	foreach(lc, desc->newcolvals)
	{
		NewColumnValue *newval = lfirst(lc);
		colno = newval->attnum - 1;
		datumstreamwrite_block(desc->dsw[i], &desc->blockDirectory, colno);
		datumstreamwrite_close_file(desc->dsw[i]);
		i++;
	}
	/* Update pg_aocsseg_* with eof of each segfile we just closed. */
	if (desc->cur_segno >= 0)
		AOCSFileSegInfoWriteVpe(desc->rel,
							  desc->cur_segno,
							  desc,
							  false /* non-empty VPEntry */ );
}

/*
 * Create new physical segfiles for each column being written and initialize
 * blockDirectory for recording corresponding changes to the table
 * as part of a column add/rewrite operation.
 */
static void
aocs_writecol_newsegfiles(AOCSWriteColumnDesc desc, AOCSFileSegInfo *seginfo)
{
	int32		fileSegNo;
	int			i;
	ListCell 	*lc;
	Snapshot	appendOnlyMetaDataSnapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));
	char 		*basepath = relpathbackend(desc->rel->rd_node, desc->rel->rd_backend, MAIN_FORKNUM);
	RelFileNodeBackend rnode;

	rnode.node = desc->rel->rd_node;
	rnode.backend = desc->rel->rd_backend;

	if (desc->dsw[0]->need_close_file)
	{
		aocs_writecol_closefiles(desc);
		AppendOnlyBlockDirectory_End_writeCols(&desc->blockDirectory,
											   desc->newcolvals);
	}
	AppendOnlyBlockDirectory_Init_writeCols(&desc->blockDirectory,
											appendOnlyMetaDataSnapshot,
											(FileSegInfo *) seginfo,
											desc->rel,
											seginfo->segno,
											desc->rel->rd_att->natts,
											true /* isAOCol */ );

	i = 0;
	foreach(lc, desc->newcolvals)
	{
		char		fn[MAXPGPATH];
		int			version;
		FileNumber  filenum;

		/* New filenum for the column */
		NewColumnValue *newval = lfirst(lc);
		if (desc->op == AOCSADDCOLUMN)
			filenum = GetFilenumForAttribute(RelationGetRelid(desc->rel), newval->attnum);
		else
			filenum = GetFilenumForRewriteAttribute(RelationGetRelid(desc->rel), newval->attnum);

		/* Always write in the latest format */
		version = AOSegfileFormatVersion_GetLatest();

		FormatAOSegmentFileName(basepath, seginfo->segno, filenum,
								&fileSegNo, fn);
		Assert(strlen(fn) + 1 <= MAXPGPATH);
		datumstreamwrite_open_file(desc->dsw[i], fn,
								   0 /* eof */ , 0 /* eof_uncompressed */ ,
								   &rnode, fileSegNo,
								   version);
		desc->dsw[i]->blockFirstRowNum = 1;
		i++;
	}
	desc->cur_segno = seginfo->segno;
	UnregisterSnapshot(appendOnlyMetaDataSnapshot);
}

static void
aocs_writecol_setfirstrownum(AOCSWriteColumnDesc desc, int64 firstRowNum)
{
       int                     i;
       for (i = 0; i < desc->num_cols_to_write; ++i)
       {
               /*
                * Next block's first row number.
                */
               desc->dsw[i]->blockFirstRowNum = firstRowNum;
       }
}


/*
 * Force writing new varblock in each segfile open for insert.
 */
static void
aocs_writecol_endblock(AOCSWriteColumnDesc desc, int64 firstRowNum)
{
	int	i = 0;
	ListCell *lc;
	foreach(lc, desc->newcolvals)
	{
		NewColumnValue *newval = lfirst(lc);
		int colno = newval->attnum - 1;
		datumstreamwrite_block(desc->dsw[i], &desc->blockDirectory, colno);

		/*
		 * Next block's first row number.  In this case, the block being ended
		 * has less number of rows than its capacity.
		 */
		desc->dsw[i]->blockFirstRowNum = firstRowNum;
		i++;
	}
}

/*
 * Insert one new datum for each new column being written.  This is
 * derived from aocs_insert_values().
 */
static void
aocs_writecol_insert_datum(AOCSWriteColumnDesc desc, Datum *datums, bool *isnulls)
{
	void	   *toFree1;
	void	   *toFree2;
	ListCell    *lc;

	int i = 0;
	foreach(lc, desc->newcolvals)
	{
		NewColumnValue *newval = lfirst(lc);

		int colno = newval->attnum - 1;
		Datum datum = datums[colno];
		bool isnullcol = isnulls[colno];
		int err = datumstreamwrite_put(desc->dsw[i], datum, isnullcol, &toFree1);

		if (toFree1 != NULL)
		{
			/*
			 * Use the de-toasted and/or de-compressed as datum instead.
			 */
			datum = PointerGetDatum(toFree1);
		}
		if (err < 0)
		{
			/*
			 * We have reached max number of datums that can be accommodated
			 * in current varblock.
			 */
			int itemCount = datumstreamwrite_nth(desc->dsw[i]);
			/* write the block up to this one */
			datumstreamwrite_block(desc->dsw[i], &desc->blockDirectory, colno);
			if (itemCount > 0)
			{
				/* Next block's first row number */
				desc->dsw[i]->blockFirstRowNum += itemCount;
			}

			/* now write this new item to the new block */
			err = datumstreamwrite_put(desc->dsw[i], datum, isnullcol, &toFree2);

			Assert(toFree2 == NULL);
			if (err < 0)
			{
				Assert(!isnullcol);
				err = datumstreamwrite_lob(desc->dsw[i],
										   datum,
										   &desc->blockDirectory,
										   colno);
				Assert(err >= 0);

				/*
				 * Have written the block above with column value
				 * corresponding to a row, so now update the first row number
				 * to correctly reflect for next block.
				 */
				desc->dsw[i]->blockFirstRowNum++;
			}
		}
		if (toFree1 != NULL)
			pfree(toFree1);
		i++;
	}
}

static void
aocs_writecol_finish(AOCSWriteColumnDesc desc)
{
	int			i;
	Oid         blkdirrelid;
	GetAppendOnlyEntryAuxOids(desc->rel, NULL, &blkdirrelid, NULL);
	aocs_writecol_closefiles(desc);

	if (OidIsValid(blkdirrelid) && desc->blockDirectory.blkdirRel)
		AppendOnlyBlockDirectory_End_writeCols(&desc->blockDirectory,
											   desc->newcolvals);
	for (i = 0; i < desc->num_cols_to_write; ++i)
		destroy_datumstreamwrite(desc->dsw[i]);
	pfree(desc->dsw);
	desc->dsw = NULL;

	pfree(desc);
}

/*
 * Add empty VPEs (eof=0) to pg_aocsseg_* catalog, corresponding to
 * each new column being added.
 */
static void
aocs_addcol_emptyvpe(Relation rel,
					 AOCSFileSegInfo **segInfos, int32 nseg,
					 int num_newcols)
{
	int			i;

	for (i = 0; i < nseg; ++i)
	{
		if (Gp_role == GP_ROLE_DISPATCH || segInfos[i]->total_tupcount == 0)
		{
			/*
			 * On QD, all tuples in pg_aocsseg_* catalog have eof=0. On QE,
			 * tuples with eof=0 may exist in pg_aocsseg_* already, caused by
			 * VACUUM.  We need to add corresponding tuples with eof=0 for
			 * each newly added column on QE.
			 */
			AOCSFileSegInfoWriteVpe(rel,
								  segInfos[i]->segno,
								  NULL,
								  true /* empty VPEntry */ );
		}
	}
}

/*
 * Choose the anchor column that has the smallest segfile size so as to
 * minimize disk I/O in subsequent varblock header scan. The natts arg
 * includes only existing columns and not the ones being added. Once
 * we find a segfile with nonzero tuplecount and find the column with
 * the smallest eof to return, we continue the loop but skip over all
 * segfiles except for those in AOSEG_STATE_AWAITING_DROP state which
 * we need to append to our drop list.
 * 
 * Note that, the anchor columns has to be "complete", i.e. no missing
 * values in its data file. We consider the column "complete" column as
 * indicated by pg_attribute_encoding.lastrownums field of the column -
 * non-empty means not complete. In addition, the caller can optionally
 * provide a set of projected columns, and we will pick one from those
 * over the other columns, if a complete column is being found.
 */
int
get_anchor_col(AOCSFileSegInfo **segInfos, int nseg, int natts,
						Relation aocsrel, 
						AttrNumber *proj_atts,
						AttrNumber num_proj_atts)
{
	int scancol_all = -1;
	int scancol_proj = -1;
	int segi;
	int i;
	AOCSVPInfoEntry *vpe;
	bool *column_not_complete;
	Bitmapset *proj_bms = NULL;

	/* 
	 * Get an array in which each bool indicates whether pg_attribute_encoding.lastrownums
	 * is not NULL (i.e. exists) for each column. We translate that into meaning whether
	 * column is complete (exists == not complete).
	 */
	column_not_complete = ExistValidLastrownums(RelationGetRelid(aocsrel), natts);

	/* build a bitmapset of the projected columns */
	for (i = 0; i < num_proj_atts; ++i)
		proj_bms = bms_add_member(proj_bms, proj_atts[i]);

	for (segi = 0; segi < nseg; ++segi)
	{
		HeapTuple atttup;
		int64 min_eof_all = 0;
		int64 min_eof_proj = 0;

		/*
		 * Don't use a AOSEG_STATE_AWAITING_DROP segfile. That seems
		 * like a bad idea in general, but there's one particular problem:
		 * the 'vpinfo' of a dropped segfile might be missing information
		 * for columns that were added later.
		 */
		if (segInfos[segi]->state == AOSEG_STATE_AWAITING_DROP)
			continue;

		/*
		 * Skip over appendonly segments with no tuples (caused by VACUUM)
		 */
		if (segInfos[segi]->total_tupcount > 0)
		{
			for (i = 0; i < natts; ++i)
			{
				/* skip non-complete (i.e. missing or partially missing column) */
				if (column_not_complete[i])
					continue;

				atttup = SearchSysCacheAttNum(RelationGetRelid(aocsrel), i + 1);
				/* skip dropped column */
				if (!atttup)
					continue;
				ReleaseSysCache(atttup);

				/* 
				 * skip those do not have aoseg entry yet - this can happen
				 * when we are in the process of adding a new column
				 */
				if (i >= segInfos[segi]->vpinfo.nEntry)
					continue;

				vpe = getAOCSVPEntry(segInfos[segi], i);
				if (vpe->eof <= 0)
					continue;

				if (!min_eof_all || vpe->eof < min_eof_all)
				{
					min_eof_all = vpe->eof;
					scancol_all = i;
				}
				if (bms_is_member(i, proj_bms) && (!min_eof_proj || vpe->eof < min_eof_proj))
				{
					min_eof_proj = vpe->eof;
					scancol_proj = i;
				}
			}
		}

		/* stop if we've found our candidate */
		if (scancol_proj >= 0 || scancol_all >= 0)
			break;
	}

	/* Use projected column if found. If there's no column found (e.g. no data in table at all), just return 0 */
	return scancol_proj != -1 ? scancol_proj : (scancol_all == -1 ? 0 : scancol_all);
}

/* 
 * A helper for aocs_writecol_writesegfiles(). It evaluates expression stored in 
 * the 'exprstate' field of the 'newvals', and store the values in 'slot'. In
 * addition, it performs NULL and check constraint checks of the values.
 *
 * The function is used only for ADD COLUMN so it expects one of the AOCSADDCOLUMN
 * and AOCSADDCOLUMN_MISSINGMODE. And it only performs the above for the indicated
 * operation.
 */
static void eval_expr_and_check_constr(List *newvals, List *constraints, ExprContext *econtext, TupleTableSlot *slot, TupleDesc tupdesc, AOCSWriteColumnOperation op)
{
	NewColumnValue 		*newval;
	Form_pg_attribute 	attr;
	Datum 			*values = slot->tts_values;
	bool 			*isnull = slot->tts_isnull;
	ListCell 		*l;

	Assert(op == AOCSADDCOLUMN || op == AOCSADDCOLUMN_MISSINGMODE);

	foreach (l, newvals)
	{
		newval = lfirst(l);
		if (newval->op != op)
			continue;

		values[newval->attnum-1] =
			ExecEvalExprSwitchContext(newval->exprstate,
									  econtext,
									  &isnull[newval->attnum-1]);
		/*
		 * Ensure that NOT NULL constraint for the newly
		 * added columns is not being violated.  This
		 * covers the case when explicit "CHECK()"
		 * constraint is not specified but only "NOT NULL"
		 * is specified in the new column's definition.
		 */
		attr = TupleDescAttr(tupdesc, newval->attnum - 1);
		if (attr->attnotnull &&	isnull[newval->attnum-1])
		{
			ereport(ERROR,
					(errcode(ERRCODE_NOT_NULL_VIOLATION),
						errmsg("column \"%s\" contains null values",
							   NameStr(attr->attname))));
		}
	}

	foreach (l, constraints)
	{
		NewConstraint *con = lfirst(l);
		switch(con->contype)
		{
			case CONSTR_CHECK:
				if(!ExecCheck(con->qualstate, econtext))
					ereport(ERROR,
							(errcode(ERRCODE_CHECK_VIOLATION),
								errmsg("check constraint \"%s\" is violated by some row",
									   con->name)));
				break;
			case CONSTR_FOREIGN:
				/* Nothing to do */
				break;
			default:
				elog(ERROR, "Unrecognized constraint type: %d",
					 (int) con->contype);
		}
	}
}

/*
 * A helper for aocs_writecols_add(). It scans an existing column for
 * varblock headers. Write one new segfile each for new columns.
 */
static void
aocs_writecol_writesegfiles(
	AOCSWriteColumnDesc idesc, AOCSHeaderScanDesc sdesc,
	List *constraints, ExprContext *econtext, TupleTableSlot *slot)
{
	NewColumnValue *newval;
	TupleDesc tupdesc = RelationGetDescr(idesc->rel);
	Datum *values = slot->tts_values;
	bool *isnull = slot->tts_isnull;
	int64 expectedFRN = -1; /* expected firstRowNum of the next varblock */
	ListCell *l;
	int i;
	bool missing_mode_only = true;

	/*
	 * Although we do not need to fill the slot for missing-mode columns, we
	 * still need to verify constraints for them.
	 */
	eval_expr_and_check_constr(idesc->newcolvals, constraints, econtext, slot, tupdesc, AOCSADDCOLUMN_MISSINGMODE);
	ResetExprContext(econtext);

	/*
	 * Also check if there's just missing-mode column to add. If so, we don't
	 * need the rest of the function.
	 */
	foreach (l, idesc->newcolvals)
	{
		newval = lfirst(l);
		if (newval->op == AOCSADDCOLUMN)
		{
			missing_mode_only = false;
			break;
		}
	}
	if (missing_mode_only)
		return;

	/* Now we deal with non-missing columns */

	/* Loop over each varblock in an appendonly segno. */
	while (aocs_get_nextheader(sdesc))
	{
		if (sdesc->ao_read.current.hasFirstRowNum)
		{
			if (expectedFRN == -1)
			{
				/*
				 * Initialize expected firstRowNum for each appendonly
				 * segment.  Initializing it to 1 may not always be
				 * good.  E.g. if the first insert into an appendonly
				 * segment is aborted.  A subsequent successful insert
				 * creates the first varblock having firstRowNum
				 * greater than 1.
				 */
				expectedFRN = sdesc->ao_read.current.firstRowNum;
				aocs_writecol_setfirstrownum(idesc, expectedFRN);
			}
			else
			{
				Assert(expectedFRN <= sdesc->ao_read.current.firstRowNum);
				if (expectedFRN < sdesc->ao_read.current.firstRowNum)
				{
					elogif(Debug_appendonly_print_storage_headers, LOG,
						   "hole in %s: exp FRN: " INT64_FORMAT ", actual FRN: "
							   INT64_FORMAT, sdesc->ao_read.segmentFileName,
						   expectedFRN, sdesc->ao_read.current.firstRowNum);
					/*
					 * We encountered a break in sequence of row
					 * numbers (hole), replicate it in the new
					 * segfiles.
					 */
					aocs_writecol_endblock(
						idesc, sdesc->ao_read.current.firstRowNum);
				}
			}
			for (i = 0; i < sdesc->ao_read.current.rowCount; ++i)
			{
				eval_expr_and_check_constr(idesc->newcolvals, constraints, econtext, slot, tupdesc, AOCSADDCOLUMN);
				aocs_writecol_insert_datum(idesc,
										   values,
										   isnull);
				ResetExprContext(econtext);
				CHECK_FOR_INTERRUPTS();
			}
			expectedFRN = sdesc->ao_read.current.firstRowNum +
				sdesc->ao_read.current.rowCount;
		}
	}
}

/*
 * Optimization for AT ADD COLUMN for AOCO tables for writing new columns
 * without requiring a full table rewrite
 *
 * This involves scanning header blocks in each segfile for one of the columns
 * and replicating that block structure into new files for the new columns, with
 * the default value for each row
 */
void
aocs_writecol_add(Oid relid, List *newvals, List *constraints, TupleDesc oldDesc)
{
	AOCSFileSegInfo **segInfos;
	AOCSHeaderScanDesc  sdesc;
	AOCSWriteColumnDesc idesc;
	NewColumnValue      *newval;
	NewConstraint *con;
	TupleTableSlot *slot;
	EState *estate;
	ExprContext *econtext;
	Relation rel; /* Relation being altered */
	int32 nseg;
	int32 segi;
	char *basepath;
	int32 scancol; /* chosen column number to scan from */
	ListCell *l;
	Snapshot snapshot;
	int numaddcols;

	snapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

	estate = CreateExecutorState();
	foreach(l, constraints)
	{
		con = lfirst(l);
		switch (con->contype)
		{
			case CONSTR_CHECK:
				con->qualstate = ExecPrepareExpr((Expr *) con->qual, estate);
				break;
			case CONSTR_FOREIGN:
				/* Nothing to do here */
				break;
			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 (int) con->contype);
		}
	}
	Assert(newvals);

	foreach(l, newvals)
	{
		newval = lfirst(l);
		if (newval->op == AOCSREWRITECOLUMN)
			continue;
		newval->exprstate = ExecPrepareExpr((Expr *) newval->expr, estate);
	}

	rel = heap_open(relid, NoLock);
	Assert(RelationIsAoCols(rel));

	/*
	 * There might be AWAITING_DROP segments occupying spaces for failing
	 * to drop at VACUUM in the case of cleaning up happened concurrently
	 * with earlier readers which was accessing the dead segment files.
	 *
	 * We used to call AppendOptimizedRecycleDeadSegments() (current name is
	 * ao_vacuum_rel_recycle_dead_segments) to recycle those segfiles to save
	 * spaces in this scenario. But it didn't do corresponding index tuples
	 * cleanup for unknown reason.
	 *
	 * After optimizing VACUUM AO strategy, we did refactor for
	 * AppendOptimizedRecycleDeadSegments() a little bit and combine
	 * dead segfiles cleanup with corresponding indexes cleanup together.
	 * While it seems to be impossible to pass index vacuuming parameter in
	 * this scenario, so we removed AppendOptimizedRecycleDeadSegments() out
	 * of this function and dedicated it to be called only in VACUUM scenario.
	 *
	 * We are supposed to be fine without recycling spaces here, or find
	 * another way to fix it if that does become a real problem.
	 */

	segInfos = GetAllAOCSFileSegInfo(rel, snapshot, &nseg, NULL);
	basepath = relpathbackend(rel->rd_node, rel->rd_backend, MAIN_FORKNUM);
	numaddcols = RelationGetDescr(rel)->natts - oldDesc->natts;
	if (nseg > 0)
		aocs_addcol_emptyvpe(rel, segInfos, nseg, numaddcols);

	scancol = get_anchor_col(segInfos, nseg, oldDesc->natts, rel, NULL, 0);
	elogif(Debug_appendonly_print_storage_headers, LOG,
		   "using column %d of relation %s for alter table scan",
		   scancol, RelationGetRelationName(rel));

	/*
	 * Continue only if a non-empty existing segfile was found above.
	 */
	if (Gp_role != GP_ROLE_DISPATCH && scancol != -1)
	{
		slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsVirtual);

		/*
		 * Initialize expression context for evaluating values and
		 * constraints of the newly added columns.
		 */
		econtext = GetPerTupleExprContext(estate);
		/*
		 * The slot's data will be populated for each newly added
		 * column by ExecEvalExpr().
		 */
		econtext->ecxt_scantuple = slot;

		/*
		 * Mark all attributes including newly added columns as valid.
		 * Used for per tuple constraint evaluation.
		 */
		ExecStoreAllNullTuple(slot);

		sdesc = aocs_begin_headerscan(rel, scancol);
		/*
		 * Protect against potential negative number here.
		 * Note that natts is not decremented to reflect dropped columns,
		 * so this should be safe
		 */
		Assert(numaddcols > 0);
		idesc = aocs_writecol_init(rel, newvals, AOCSADDCOLUMN);

		/* Loop over all appendonly segments */
		for (segi = 0; segi < nseg; ++segi)
		{
			if (segInfos[segi]->total_tupcount <= 0 ||
				segInfos[segi]->state == AOSEG_STATE_AWAITING_DROP)
			{
				/*
				 * VACUUM may cause appendonly segments with eof=0.
				 * We only need to add new rows in pg_aocsseg_* in
				 * this case for each newly added column.  This is
				 * accomplished by aocs_addcol_emptyvpe() above.
				 *
				 * Compaction leaves redundant segments in
				 * AOSEG_STATE_AWAITING_DROP.  We skip over them too.
				 */
				elogif(Debug_appendonly_print_storage_headers, LOG,
					   "Skipping over empty segno %d relation %s",
					   segInfos[segi]->segno, RelationGetRelationName(rel));
				continue;
			}
			/*
			 * Open aocs segfile for chosen column for current
			 * appendonly segment.
			 */
			aocs_headerscan_opensegfile(sdesc, segInfos[segi], basepath);

			/*
			 * Create new segfiles for new columns for current
			 * appendonly segment.
			 */
			aocs_writecol_newsegfiles(idesc, segInfos[segi]);

			aocs_writecol_writesegfiles(idesc, sdesc, constraints, econtext, slot);
		}
		aocs_end_headerscan(sdesc);
		aocs_writecol_finish(idesc);
		ExecDropSingleTupleTableSlot(slot);
	}

	FreeExecutorState(estate);
	heap_close(rel, NoLock);
	UnregisterSnapshot(snapshot);
}

/*
 * A helper for aocs_writecol_rewrite(). It scans an existing column's segfiles
 * for all rows including deleted rows. Write one new segfile each for each column
 *
 * Note: this is similar to ATAocsWriteNewColumns() with one important difference:
 * we don't directly iterate over varblocks, but over the tuples (with projection applied).
 * It also borrows code from ATRewriteTable().
 */
static void
aocs_writecol_rewritesegfiles(
	AOCSWriteColumnDesc idesc, AOCSScanDesc scanDesc,
	ExprContext *econtext, TupleTableSlot *oldslot,
	TupleTableSlot *newslot, int colno)
{
	ListCell *l;
	/* expected first row number of the next varblock */
	int64 expectedFRN = -1;
	Assert(list_length(idesc->newcolvals) > 0);

	/* Loop over each row in the segment. */
	while (aocs_getnext(scanDesc, ForwardScanDirection, oldslot))
	{
		if (scanDesc->columnScanInfo.ds[colno]->ao_read.current.hasFirstRowNum)
		{
			if (expectedFRN == -1)
			{
				/* first time init */
				expectedFRN =
					scanDesc->columnScanInfo.ds[colno]->ao_read.current.firstRowNum;
				aocs_writecol_setfirstrownum(idesc, expectedFRN);
			}
			else
			{
				/* row number grows monotonically */
				/* we have switched to the next block, so end the current block */
				if (expectedFRN <
					scanDesc->columnScanInfo.ds[colno]->ao_read.current.firstRowNum)
				{
					elogif(Debug_appendonly_print_storage_headers, LOG,
						   "hole in %s: exp FRN: " INT64_FORMAT ", actual FRN: "
							   INT64_FORMAT, scanDesc->columnScanInfo.ds[colno]->ao_read.segmentFileName,
						   expectedFRN, scanDesc->columnScanInfo.ds[colno]->ao_read.current.firstRowNum);
					/*
					 * We encountered a break in sequence of row
					 * numbers (hole), replicate it in the new
					 * segfiles.
					 */
					expectedFRN =
						scanDesc->columnScanInfo.ds[colno]->ao_read.current.firstRowNum;
					aocs_writecol_endblock(idesc,
										   scanDesc->columnScanInfo.ds[colno]->ao_read.current.firstRowNum);
					expectedFRN = scanDesc->columnScanInfo.ds[colno]->ao_read.current.firstRowNum + scanDesc->columnScanInfo.ds[colno]->ao_read.current.rowCount;
				}
			}
		}
		ExecClearTuple(newslot);

		memcpy(newslot->tts_values, oldslot->tts_values,
			   sizeof(Datum) * oldslot->tts_nvalid);
		memcpy(newslot->tts_isnull, oldslot->tts_isnull,
			   sizeof(bool) * oldslot->tts_nvalid);


		/*
		 * Process supplied expressions to replace selected columns.
		 *
		 * First, evaluate expressions whose inputs come from the old
		 * tuple.
		 */
		econtext->ecxt_scantuple = oldslot;

		foreach(l, idesc->newcolvals)
		{
			NewColumnValue *ex = lfirst(l);
			if (ex->is_generated)
				continue;

			newslot->tts_values[ex->attnum - 1]
				= ExecEvalExprSwitchContext(ex->exprstate,
										  econtext,
										  &newslot->tts_isnull[ex->attnum-1]);
		}

		ExecStoreVirtualTuple(newslot);

		/*
		 * Now, evaluate any expressions whose inputs come from the
		 * new tuple.  We assume these columns won't reference each
		 * other, so that there's no ordering dependency.
		 */
		econtext->ecxt_scantuple = newslot;

		foreach(l, idesc->newcolvals)
		{
			NewColumnValue *ex = lfirst(l);
			if (!ex->is_generated)
				continue;

			newslot->tts_values[ex->attnum - 1]
				= ExecEvalExprSwitchContext(ex->exprstate,
											econtext,
											&newslot->tts_isnull[ex->attnum-1]);
		}

		aocs_writecol_insert_datum(idesc,
								   newslot->tts_values,
								   newslot->tts_isnull);
		ResetExprContext(econtext);
		CHECK_FOR_INTERRUPTS();
		expectedFRN++;
	}
}

/*
 * Recreate indexes that depend on the columns rewritten during
 * ALTER TABLE for AOCO tables.
 */
static void
aocs_writecol_reindex(Oid relid, List *newvals)
{
	ListCell       *lcindex;
	ListCell       *lc;
	NewColumnValue *newval;
	Relation       OldHeap = relation_open(relid, NoLock);
	char           oldRelPersistence = OldHeap->rd_rel->relpersistence;
	List           *indexoidlist = RelationGetIndexList(OldHeap);

	heap_close(OldHeap, NoLock);
	foreach(lcindex, indexoidlist)
	{
		bool      reindex  = false;
		Oid       indexoid = lfirst_oid(lcindex);
		HeapTuple indexTuple;

		indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))	/* should not happen */
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		int       indnatts = ((Form_pg_index) GETSTRUCT(indexTuple))->indnatts;
		foreach (lc, newvals)
		{
			newval = lfirst(lc);
			if (newval->op == AOCSADDCOLUMN || newval->op == AOCSADDCOLUMN_MISSINGMODE)
				continue;
			AttrNumber rewrittenattnum = newval->attnum;
			for (int i = 0; i < indnatts; ++i)
			{
				if (rewrittenattnum == ((Form_pg_index) GETSTRUCT(indexTuple))->indkey.values[i])
				{
					reindex = true;
					break;
				}
			}
			if (reindex)
				break;
		}
		ReleaseSysCache(indexTuple);
		if (reindex)
			reindex_index(indexoid, false, oldRelPersistence, 0);
	}

	list_free(indexoidlist);
}

/*
 * Rewrite the segfiles of columns involved in a rewrite operation into new
 * segfiles using the pg_attribute_encoding.filenum pair
 * (i, i+MaxHeapAttributeNumber), where i is the attnum of a given column.
 *
 * To do this, we scan the old column files segfiles (using projection), including deleted rows,
 * evaluate new altered datums (eg. ALTER COLUMN TYPE would change the type of
 * the datums). Then we write the altered datums into a new segfile, determined
 * by the filenum pair: (i, i+MaxHeapAttributeNumber) where i is the attnum of a
 * given column and i <= MaxHeapAttributeNumber.
 */
void
aocs_writecol_rewrite(Oid relid, List *newvals, TupleDesc oldDesc)
{
	Relation rel = relation_open(relid, NoLock);
	Snapshot snapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));
	ListCell *l;
	EState *estate;
	AOCSFileSegInfo **segInfos;
	ExprContext *econtext;
	TupleTableSlot *oldslot;
	TupleTableSlot *newslot;
	int32 nseg;
	AOCSWriteColumnDesc idesc;
	FileNumber newfilenum;

	estate = CreateExecutorState();
	Assert(newvals);
	segInfos = GetAllAOCSFileSegInfo(rel, snapshot, &nseg, NULL);

	if (Gp_role != GP_ROLE_DISPATCH && nseg >= 1)
	{
		idesc = aocs_writecol_init(rel, newvals, AOCSREWRITECOLUMN);
		foreach(l, idesc->newcolvals)
		{
			NewColumnValue *ex = lfirst(l);
			/* expr already planned */
			ex->exprstate = ExecInitExpr((Expr *) ex->expr, NULL);
		}
		oldslot = MakeSingleTupleTableSlot(oldDesc, &TTSOpsVirtual);
		newslot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsVirtual);

		/*
		 * GENERATED expressions might reference the
		 * tableoid column, so fill tts_tableOid with the desired
		 * value. It is sufficient to do it once here, instead of once
		 * per tuple, as we don't modify this field during the rewrite
		 * process.
		 */
		newslot->tts_tableOid = relid;

		/*
		 * Initialize expression context for evaluating values
		 * of the columns being rewritten
		 */
		econtext = GetPerTupleExprContext(estate);

		for (int segi = 0; segi < nseg; ++segi)
		{
			AOCSScanDesc scanDesc;

			if (segInfos[segi]->total_tupcount <= 0 ||
				segInfos[segi]->state == AOSEG_STATE_AWAITING_DROP)
			{
				/*
				 * VACUUM may cause appendonly segments with eof=0.
				 *
				 * Compaction leaves redundant segments in
				 * AOSEG_STATE_AWAITING_DROP.
				 *
				 * Both of these cases don't require a rewrite.
				 */
				elogif(Debug_appendonly_print_storage_headers, LOG,
					   "Skipping over empty segno %d relation %s total tupcount %ld state %u",
					   segInfos[segi]->segno, RelationGetRelationName(rel), segInfos[segi]->total_tupcount, segInfos[segi]->state);
				continue;
			}

			/*
			 * We are using SnapshotAny to scan the column here.
			 * We can't exclude datums from dead rows, unlike a full table rewrite.
			 * as we need to replicate block structure like other columns to maintain
			 * tuple id for every row
			 */
			scanDesc = aocs_beginrangescan(rel,
										   SnapshotAny, NULL,
										   &(segInfos[segi]->segno), 1, NULL);

			aocs_writecol_newsegfiles(idesc, segInfos[segi]);
			if (idesc->blockDirectory.blkdirRel != NULL)
			{
				/*
				 * Delete the existing block directory entries for the given col and segno.
				 * New entries for this col and segno will be added during the course of
				 * the rewrite process.
				 */
				ListCell *lc;
				foreach(lc, idesc->newcolvals)
				{
					NewColumnValue *newval = lfirst(lc);
					AttrNumber     colno   = newval->attnum - 1;
					AppendOnlyBlockDirectory_DeleteSegmentFile(&idesc->blockDirectory,
															   colno,
															   segInfos[segi]->segno,
															   snapshot);
				}
			}

			/* take any column that we are altering, for reading the header info */
			int colno = get_anchor_col(segInfos, nseg, rel->rd_att->natts, rel, NULL, 0);

			/*
			 * Rewrite segfiles for columns for current appendonly segment.
			 */
			aocs_writecol_rewritesegfiles(idesc,
										scanDesc,
										econtext,
										oldslot,
										newslot,
										colno);
			aocs_endscan(scanDesc);
		}

		aocs_writecol_finish(idesc);
		FreeExecutorState(estate);
		ExecDropSingleTupleTableSlot(oldslot);
		ExecDropSingleTupleTableSlot(newslot);
	}

	/* Update the filenum, lastrownums, any encoding options in pg_attribute_encoding */
	foreach(l, newvals)
	{
		Datum newattoptions = (Datum)0;
		NewColumnValue *ex = lfirst(l);

		if (ex->op == AOCSADDCOLUMN || ex->op == AOCSADDCOLUMN_MISSINGMODE)
			continue;

		newfilenum = GetFilenumForRewriteAttribute(RelationGetRelid(rel), ex->attnum);
		/* get the attoptions string to be stored in pg_attribute_encoding */
		if (ex->new_encoding)
		{
			newattoptions = transformRelOptions(PointerGetDatum(NULL),
											ex->new_encoding,
											NULL,
											NULL,
											true,
											false);
		}

		update_attribute_encoding_entry(RelationGetRelid(rel), 
									ex->attnum,
									newfilenum,
									0,
									newattoptions);
		/*
		 * lastrownums is not useful after rewriting the column, so clear them.
		 * As we just update the catalog we need to increment command counter.
		 * XXX: change update_attribute_encoding_entry so we can nullify
		 * lastrownums in one call, or use something like NullableDatum.
		 */
		CommandCounterIncrement();
		ClearAttributeEncodingLastrownumsByAttnum(RelationGetRelid(rel), ex->attnum);
	}

	/* Re-index the ones that contain the columns we just rewrote */
	aocs_writecol_reindex(relid, newvals);

	heap_close(rel, NoLock);
	UnregisterSnapshot(snapshot);
}

