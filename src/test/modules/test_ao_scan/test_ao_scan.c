/*--------------------------------------------------------------------------
 *
 * test_ao_scan.c
 *		Read append-only tables from an extension: by blocks, without a
 *		tuple slot and a call for every row.
 *
 * ao_next_batch and aocs_next_batch give up to BATCH_ROWS rows of a block
 * at once, column by column, into arrays of the attributes asked for, with
 * a mask of the rows the snapshot sees. A value passed by reference points
 * into its block, which stays as it is until the next batch is read: a
 * batch ends where a block ends, so that it never needs two blocks of one
 * column.
 *
 * test_ao_scan_compare reads a table this way and through tuple slots and
 * checks that the two agree on every value, on each segment.
 *
 * IDENTIFICATION
 *		src/test/modules/test_ao_scan/test_ao_scan.c
 *
 *--------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "pgstat.h"
#include "port/pg_bitutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

/* The rows the mask of a batch has room for. */
#define BATCH_ROWS 64

typedef struct BatchScan
{
	Relation	rel;
	AOCSScanDesc aocs;			/* of an ao_column table, or */
	AppendOnlyScanDesc ao;		/* of an ao_row table */
	bool		in_segment;		/* aocs is inside a segment file */

	/* The attributes asked for, zero-based. */
	int			nattrs;
	AttrNumber *attnos;

	/* The last batch: its rows, indexed by attno, and which of them are seen. */
	Datum	  **values;
	bool	  **isnull;
	int			segno;
	int64		rownums[BATCH_ROWS];
	uint64		visible;
} BatchScan;

/*
 * Set the mask of the batch's nrows rows. One entry of the visibility map
 * usually covers them all, and with no row deleted in it they are all seen.
 */
static void
set_visible(BatchScan *scan, AppendOnlyVisimap *visimap, bool any_row, int nrows)
{
	AOTupleId	first;
	AOTupleId	last;
	PgStat_TableStatus *stats = scan->rel->pgstat_info;

	scan->visible = nrows == BATCH_ROWS ? ~UINT64CONST(0) : (UINT64CONST(1) << nrows) - 1;
	if (!any_row)
	{
		AOTupleIdInit(&first, scan->segno, scan->rownums[0]);
		AOTupleIdInit(&last, scan->segno, scan->rownums[nrows - 1]);
		if (!AppendOnlyVisimap_IsVisible(visimap, &first) ||
			!AppendOnlyVisimapEntry_CoversTuple(&visimap->visimapEntry, &last) ||
			!bms_is_empty(visimap->visimapEntry.bitmap))
		{
			for (int row = 0; row < nrows; row++)
			{
				AOTupleId	tid;

				AOTupleIdInit(&tid, scan->segno, scan->rownums[row]);
				if (!AppendOnlyVisimap_IsVisible(visimap, &tid))
					scan->visible &= ~(UINT64CONST(1) << row);
			}
		}
	}
	if (stats != NULL)
		stats->t_counts.t_tuples_returned += pg_popcount((const char *) &scan->visible,
														 sizeof(scan->visible));
}

/*
 * The next rows of an ao_row scan. A memtuple gives any attribute at once, so
 * only those asked for are read, and an attribute added after the row was
 * written has its missing value. Returns the number of rows, 0 at the end of
 * the scan.
 */
static int
ao_next_batch(BatchScan *scan)
{
	AppendOnlyScanDesc ao = scan->ao;
	AppendOnlyExecutorReadBlock *block = &ao->executorReadBlock;
	TupleDesc	tupdesc = RelationGetDescr(scan->rel);

	for (;;)
	{
		int			nrows = 0;

		if (ao->needNextBuffer && !appendonly_scan_next_block(ao))
			return 0;
		while (nrows < BATCH_ROWS)
		{
			MemTuple	tuple;
			int32		tuple_len;

			if (!AppendOnlyExecutorReadBlock_NextTuple(block, &tuple, &scan->rownums[nrows],
													   &tuple_len))
			{
				ao->needNextBuffer = true;
				break;
			}
			AppendOnlyExecutorReadBlock_BindingInit(block, tupdesc, scan->rownums[nrows]);
			for (int i = 0; i < scan->nattrs; i++)
			{
				int			attno = scan->attnos[i];

				if (attno < block->mt_bind->natts)
					scan->values[attno][nrows] =
						memtuple_getattr_inline(tuple, block->mt_bind, attno + 1,
												&scan->isnull[attno][nrows]);
				else
					scan->values[attno][nrows] =
						getmissingattr(tupdesc, attno + 1, &scan->isnull[attno][nrows]);
			}
			nrows++;
		}
		if (nrows == 0)
			continue;
		scan->segno = block->segmentFileNum;
		set_visible(scan, &ao->visibilityMap, ao->snapshot == SnapshotAny, nrows);
		return nrows;
	}
}

/* The rows of a column's current block not read yet. */
static inline int
rows_left_in_block(DatumStreamRead *ds)
{
	if (ds->largeObjectState == DatumStreamLargeObjectState_None)
		return Max(ds->blockRead.logical_row_count - (ds->blockRead.nth + 1), 0);
	/* A large value fills its blocks alone: one row, until read. */
	return ds->largeObjectState == DatumStreamLargeObjectState_HaveAoContent ? 1 : 0;
}

/*
 * Make a column's current block hold a row not read yet, reading its next
 * block when the current one is done. False when the segment file has no
 * more blocks of the column.
 */
static bool
column_has_row(AOCSScanDesc aocs, AttrNumber attno)
{
	DatumStreamRead *ds = aocs->columnScanInfo.ds[attno];

	if (rows_left_in_block(ds) > 0)
		return true;
	(void) datumstreamread_advance(ds);
	if (datumstreamread_block(ds, aocs->blockDirectory, attno) < 0)
		return false;
	AOCSScanDesc_UpdateTotalBytesRead(aocs, attno);
	pgstat_count_buffer_read_ao(aocs->rs_base.rs_rd,
								RelationGuessNumberOfBlocksFromSize(aocs->totalBytesRead));
	return true;
}

/*
 * The next nrows values of a column's block, when it keeps them one after
 * another: a fixed width passed by value, no NULL, no RLE or delta encoding,
 * no large object. Loads them as fetch_att does, and leaves the reader where
 * advancing and getting row by row would, at the last value read. False,
 * nothing read, otherwise; a block of a version not known here is among them.
 */
static bool
read_dense(DatumStreamRead *ds, int nrows, Datum *values, bool *isnull)
{
	DatumStreamBlockRead *dsr = &ds->blockRead;
	int			len = dsr->typeInfo.datumlen;
	uint8	   *first;

	if (ds->largeObjectState != DatumStreamLargeObjectState_None ||
		!dsr->typeInfo.byval || dsr->has_null ||
		(len != 1 && len != 2 && len != 4 && len != 8) ||
		dsr->nth + nrows >= dsr->logical_row_count)
		return false;
	switch (dsr->datumStreamVersion)
	{
		case DatumStreamVersion_Original:
			break;
		case DatumStreamVersion_Dense:
		case DatumStreamVersion_Dense_Enhanced:
			if (dsr->rle_block_was_compressed || dsr->delta_block_was_compressed)
				return false;
			break;
		default:
			return false;
	}

	/*
	 * The block's first value is where the read left the pointer, the others
	 * past the current one.
	 */
	first = dsr->datump + (dsr->physical_datum_index >= 0 ? len : 0);
	switch (len)
	{
		case 1:
			for (int row = 0; row < nrows; row++)
				values[row] = CharGetDatum(((char *) first)[row]);
			break;
		case 2:
			for (int row = 0; row < nrows; row++)
				values[row] = Int16GetDatum(((int16 *) first)[row]);
			break;
		case 4:
			for (int row = 0; row < nrows; row++)
				values[row] = Int32GetDatum(((int32 *) first)[row]);
			break;
		default:
			for (int row = 0; row < nrows; row++)
				values[row] = ((Datum *) first)[row];
			break;
	}
	memset(isnull, 0, sizeof(bool) * nrows);
	dsr->datump = first + (nrows - 1) * len;
	dsr->physical_datum_index += nrows;
	dsr->nth += nrows;
	return true;
}

/*
 * The next rows of an ao_column scan. The scan reads the columns of its
 * projection, and the anchor column, which gives the rows' numbers, even when
 * nobody asked for it: that column has no arrays here and only moves on.
 * Returns the number of rows, 0 at the end of the scan.
 */
static int
aocs_next_batch(BatchScan *scan)
{
	AOCSScanDesc aocs = scan->aocs;

	for (;;)
	{
		AttrNumber	anchor;
		DatumStreamRead *anchor_ds;
		int			segno;
		bool		missing[MaxHeapAttributeNumber];
		int64		first_row;
		int			nrows = BATCH_ROWS;
		bool		segment_done = false;

		if (!scan->in_segment)
		{
			if (!aocs_scan_next_seg(aocs))
				return 0;
			scan->in_segment = true;
		}
		anchor = aocs->columnScanInfo.proj_atts[ANCHOR_COL_IN_PROJ];
		anchor_ds = aocs->columnScanInfo.ds[anchor];
		segno = aocs->seginfo[aocs->cur_seg]->segno;

		if (!column_has_row(aocs, anchor))
		{
			scan->in_segment = false;
			continue;
		}
		/* A large value, alone in its blocks, is its block's first row. */
		if (anchor_ds->blockFirstRowNum != InvalidAORowNum)
			first_row = anchor_ds->blockFirstRowNum +
				(anchor_ds->largeObjectState == DatumStreamLargeObjectState_None ?
				 anchor_ds->blockRead.nth + 1 : 0);
		else
			first_row = aocs->segrowsprocessed + 1;
		nrows = Min(nrows, rows_left_in_block(anchor_ds));

		/*
		 * The batch ends where a column's block ends or a column added later
		 * starts.
		 */
		for (AttrNumber i = 0; i < aocs->columnScanInfo.num_proj_atts; i++)
		{
			AttrNumber	attno = aocs->columnScanInfo.proj_atts[i];
			int64		last_missing;

			missing[attno] = false;
			if (attno == anchor)
				continue;
			last_missing = aocs->columnScanInfo.attnum_to_rownum[attno * MAX_AOREL_CONCURRENCY + segno];
			if (anchor_ds->blockFirstRowNum != InvalidAORowNum && first_row <= last_missing)
			{
				missing[attno] = true;
				nrows = (int) Min((int64) nrows, last_missing - first_row + 1);
				continue;
			}
			if (!column_has_row(aocs, attno))
			{
				segment_done = true;
				break;
			}
			nrows = Min(nrows, rows_left_in_block(aocs->columnScanInfo.ds[attno]));
		}
		if (segment_done)
		{
			scan->in_segment = false;
			continue;
		}

		for (AttrNumber i = 0; i < aocs->columnScanInfo.num_proj_atts; i++)
		{
			AttrNumber	attno = aocs->columnScanInfo.proj_atts[i];
			DatumStreamRead *ds = aocs->columnScanInfo.ds[attno];
			Datum	   *values = scan->values[attno];
			bool	   *isnull = scan->isnull[attno];

			if (values == NULL)
			{
				for (int row = 0; row < nrows; row++)
					(void) datumstreamread_advance(ds);
			}
			else if (missing[attno])
			{
				bool		value_null;
				Datum		value = getmissingattr(RelationGetDescr(scan->rel), attno + 1,
												   &value_null);

				for (int row = 0; row < nrows; row++)
				{
					values[row] = value;
					isnull[row] = value_null;
				}
			}
			else if (!read_dense(ds, nrows, values, isnull))
			{
				for (int row = 0; row < nrows; row++)
				{
					(void) datumstreamread_advance(ds);
					/* The reader writes no Datum for a NULL. */
					values[row] = (Datum) 0;
					datumstreamread_get(ds, &values[row], &isnull[row]);
				}
			}
		}

		for (int row = 0; row < nrows; row++)
			scan->rownums[row] = first_row + row;
		scan->segno = segno;
		set_visible(scan, &aocs->visibilityMap, aocs->rs_base.rs_snapshot == SnapshotAny, nrows);
		aocs->segrowsprocessed += nrows;
		return nrows;
	}
}

/* Begin a scan of the attributes attnums names, or of all when NULL. */
static void
batch_scan_begin(BatchScan *scan, Relation rel, ArrayType *attnums)
{
	int			natts = RelationGetNumberOfAttributes(rel);
	bool	   *proj = palloc0(sizeof(bool) * Max(natts, 1));
	uint32		flags = SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;

	memset(scan, 0, sizeof(BatchScan));
	scan->rel = rel;
	scan->attnos = palloc(sizeof(AttrNumber) * Max(natts, 1));
	if (attnums != NULL)
	{
		int			nattnums = ArrayGetNItems(ARR_NDIM(attnums), ARR_DIMS(attnums));
		int		   *numbers = (int *) ARR_DATA_PTR(attnums);

		if (array_contains_nulls(attnums))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("attnums must not contain NULLs")));
		for (int i = 0; i < nattnums; i++)
		{
			if (numbers[i] < 1 || numbers[i] > natts ||
				TupleDescAttr(RelationGetDescr(rel), numbers[i] - 1)->attisdropped ||
				proj[numbers[i] - 1])
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid attribute number %d", numbers[i])));
			proj[numbers[i] - 1] = true;
			scan->attnos[scan->nattrs++] = numbers[i] - 1;
		}
	}
	else
	{
		for (int attno = 0; attno < natts; attno++)
		{
			if (TupleDescAttr(RelationGetDescr(rel), attno)->attisdropped)
				continue;
			proj[attno] = true;
			scan->attnos[scan->nattrs++] = attno;
		}
	}

	/* Arrays of the attributes asked for only. */
	scan->values = palloc0(sizeof(Datum *) * Max(natts, 1));
	scan->isnull = palloc0(sizeof(bool *) * Max(natts, 1));
	for (int i = 0; i < scan->nattrs; i++)
	{
		scan->values[scan->attnos[i]] = palloc0(sizeof(Datum) * BATCH_ROWS);
		scan->isnull[scan->attnos[i]] = palloc0(sizeof(bool) * BATCH_ROWS);
	}

	if (rel->rd_rel->relam == AO_COLUMN_TABLE_AM_OID)
		scan->aocs = aocs_beginscan(rel, GetActiveSnapshot(),
									scan->nattrs > 0 ? proj : NULL,
									scan->nattrs > 0 ? AOCS_PROJ_SOME : AOCS_PROJ_ANY,
									flags);
	else
		scan->ao = (AppendOnlyScanDesc) appendonly_beginscan(rel, GetActiveSnapshot(),
															 0, NULL, NULL, flags);
	pfree(proj);
}

static void
batch_scan_rescan(BatchScan *scan)
{
	if (scan->aocs != NULL)
	{
		aocs_rescan(scan->aocs);
		scan->in_segment = false;
	}
	else
		appendonly_rescan((TableScanDesc) scan->ao, NULL, false, false, false, false);
}

static void
batch_scan_end(BatchScan *scan)
{
	if (scan->aocs != NULL)
		aocs_endscan(scan->aocs);
	else
		appendonly_endscan((TableScanDesc) scan->ao);
}

/* A value narrower than a Datum must fill it as fetch_att does. */
static void
check_datum_form(Form_pg_attribute att, Datum value)
{
	Datum		expected = value;

	if (!att->attbyval)
		return;
	if (att->attlen == 1)
		expected = CharGetDatum(DatumGetChar(value));
	else if (att->attlen == 2)
		expected = Int16GetDatum(DatumGetInt16(value));
	else if (att->attlen == 4)
		expected = Int32GetDatum(DatumGetInt32(value));
	if (value != expected)
		elog(ERROR, "attribute \"%s\" came as Datum %016llx, not %016llx",
			 NameStr(att->attname), (unsigned long long) value, (unsigned long long) expected);
}

/* Check one visible row of the batch against the slot of the same row. */
static void
compare_row(BatchScan *scan, int row, TupleTableSlot *slot)
{
	TupleDesc	tupdesc = RelationGetDescr(scan->rel);
	AOTupleId  *tid = (AOTupleId *) &slot->tts_tid;

	if (AOTupleIdGet_segmentFileNum(tid) != scan->segno ||
		(int64) AOTupleIdGet_rowNum(tid) != scan->rownums[row])
		elog(ERROR, "batch at segment file %d row " INT64_FORMAT ", slot at segment file %d row " INT64_FORMAT,
			 scan->segno, scan->rownums[row], (int) AOTupleIdGet_segmentFileNum(tid),
			 (int64) AOTupleIdGet_rowNum(tid));
	slot_getallattrs(slot);
	for (int i = 0; i < scan->nattrs; i++)
	{
		int			attno = scan->attnos[i];
		Form_pg_attribute att = TupleDescAttr(tupdesc, attno);
		Datum		value = scan->values[attno][row];
		bool		isnull = scan->isnull[attno][row];

		if (isnull != slot->tts_isnull[attno] ||
			(!isnull && !datumIsEqual(value, slot->tts_values[attno], att->attbyval, att->attlen)))
			elog(ERROR, "attribute \"%s\" differs at segment file %d row " INT64_FORMAT,
				 NameStr(att->attname), scan->segno, scan->rownums[row]);
		if (!isnull)
			check_datum_form(att, value);
	}
}

/* Read the table both ways, and again after a rescan. Returns the rows compared. */
static int64
compare_scans(Oid relid, ArrayType *attnums)
{
	Relation	rel = table_open(relid, AccessShareLock);
	TableScanDesc slot_scan;
	TupleTableSlot *slot;
	BatchScan	scan;
	int64		rows = 0;

	if (rel->rd_rel->relam != AO_COLUMN_TABLE_AM_OID &&
		rel->rd_rel->relam != AO_ROW_TABLE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an append-only table", RelationGetRelationName(rel))));

	slot_scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	slot = table_slot_create(rel, NULL);
	batch_scan_begin(&scan, rel, attnums);

	for (int pass = 0; pass < 2; pass++)
	{
		int			nrows;

		while ((nrows = scan.aocs != NULL ? aocs_next_batch(&scan) : ao_next_batch(&scan)) > 0)
		{
			CHECK_FOR_INTERRUPTS();
			for (int row = 0; row < nrows; row++)
			{
				if ((scan.visible & (UINT64CONST(1) << row)) == 0)
					continue;
				if (!table_scan_getnextslot(slot_scan, ForwardScanDirection, slot))
					elog(ERROR, "the slot scan ended before the batches");
				compare_row(&scan, row, slot);
				rows++;
			}
		}
		if (table_scan_getnextslot(slot_scan, ForwardScanDirection, slot))
			elog(ERROR, "the batches ended before the slot scan");
		batch_scan_rescan(&scan);
		table_rescan(slot_scan, NULL);
	}

	batch_scan_end(&scan);
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(slot_scan);
	table_close(rel, AccessShareLock);
	return rows / 2;
}

PG_FUNCTION_INFO_V1(test_ao_scan_compare);

Datum
test_ao_scan_compare(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
		SRF_FIRSTCALL_INIT();
	funcctx = SRF_PERCALL_SETUP();
	if (funcctx->call_cntr == 0)
	{
		ArrayType  *attnums = PG_ARGISNULL(1) ? NULL : PG_GETARG_ARRAYTYPE_P(1);

		SRF_RETURN_NEXT(funcctx, Int64GetDatum(compare_scans(PG_GETARG_OID(0), attnums)));
	}
	SRF_RETURN_DONE(funcctx);
}
