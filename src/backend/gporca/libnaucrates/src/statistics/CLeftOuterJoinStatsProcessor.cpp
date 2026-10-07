//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLeftOuterJoinStatsProcessor.cpp
//
//	@doc:
//		Statistics helper routines for processing Left Outer Joins
//---------------------------------------------------------------------------

#include "naucrates/statistics/CLeftOuterJoinStatsProcessor.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpmd;

// return statistics object after performing LOJ operation with another statistics structure
CStatistics *
CLeftOuterJoinStatsProcessor::CalcLOJoinStatsStatic(
	CMemoryPool *mp, const IStatistics *outer_side_stats,
	const IStatistics *inner_side_stats, CStatsPredJoinArray *join_preds_stats)
{
	GPOS_ASSERT(nullptr != outer_side_stats);
	GPOS_ASSERT(nullptr != inner_side_stats);
	GPOS_ASSERT(nullptr != join_preds_stats);

	const CStatistics *result_stats_outer_side =
		dynamic_cast<const CStatistics *>(outer_side_stats);
	const CStatistics *result_stats_inner_side =
		dynamic_cast<const CStatistics *>(inner_side_stats);

	CStatistics *inner_join_stats =
		CStatistics::CastStats(result_stats_outer_side->CalcInnerJoinStats(
			mp, inner_side_stats, join_preds_stats));
	CDouble num_rows_inner_join = inner_join_stats->Rows();
	CDouble num_rows_LASJ(1.0);

	// create a new hash map of histograms, for each column from the outer child
	// add the buckets that do not contribute to the inner join
	UlongToHistogramMap *LOJ_histograms =
		CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
			mp, result_stats_outer_side, result_stats_inner_side,
			inner_join_stats, join_preds_stats, num_rows_inner_join,
			&num_rows_LASJ, &num_rows_inner_join);

	// cardinality of LOJ is at least the cardinality of the outer child.
	// num_rows_inner_join may have been bounded above (by MakeLOJHistogram)
	// to stay consistent with num_rows_LASJ, so use its returned value here
	// rather than the original, pre-call estimate.
	CDouble num_rows_LOJ =
		std::max(outer_side_stats->Rows(), num_rows_inner_join + num_rows_LASJ);

	// create an output stats object
	CStatistics *result_stats_LOJ = GPOS_NEW(mp) CStatistics(
		mp, LOJ_histograms, inner_join_stats->CopyWidths(mp), num_rows_LOJ,
		outer_side_stats->IsEmpty(), outer_side_stats->GetNumberOfPredicates());

	inner_join_stats->Release();

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated join cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, result_stats_outer_side, result_stats_LOJ, num_rows_LOJ,
		CStatistics::EcbmMin /* card_bounding_method */);
	CStatisticsUtils::ComputeCardUpperBounds(
		mp, result_stats_inner_side, result_stats_LOJ, num_rows_LOJ,
		CStatistics::EcbmMin /* card_bounding_method */);

	return result_stats_LOJ;
}

// create a new hash map of histograms for LOJ from the histograms
// of the outer child and the histograms of the inner join
UlongToHistogramMap *
CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
	CMemoryPool *mp, const CStatistics *outer_side_stats,
	const CStatistics *inner_side_stats, CStatistics *inner_join_stats,
	CStatsPredJoinArray *join_preds_stats, CDouble num_rows_inner_join,
	CDouble *result_rows_LASJ, CDouble *result_rows_inner_join)
{
	GPOS_ASSERT(nullptr != outer_side_stats);
	GPOS_ASSERT(nullptr != inner_side_stats);
	GPOS_ASSERT(nullptr != join_preds_stats);
	GPOS_ASSERT(nullptr != inner_join_stats);

	// build a bitset with all outer child columns contributing to the join
	CBitSet *outer_side_join_cols = GPOS_NEW(mp) CBitSet(mp);
	for (ULONG j = 0; j < join_preds_stats->Size(); j++)
	{
		CStatsPredJoin *join_stats = (*join_preds_stats)[j];
		if (join_stats->HasValidColIdOuter())
		{
			(void) outer_side_join_cols->ExchangeSet(join_stats->ColIdOuter());
		}
	}

	// for the columns in the outer child, compute the buckets that do not contribute to the inner join
	CStatistics *LASJ_stats =
		CStatistics::CastStats(outer_side_stats->CalcLASJoinStats(
			mp, inner_side_stats, join_preds_stats,
			false /* DoIgnoreLASJHistComputation */
			));
	CDouble num_rows_LASJ(0.0);
	if (!LASJ_stats->IsEmpty())
	{
		num_rows_LASJ = LASJ_stats->Rows();
	}

	// if a join column's inner-side histogram was marked, num_rows_inner_join
	// was computed from that same un-narrowed-but-scaled histogram without
	// knowing that num_rows_LASJ (just computed above, from the same marked
	// histogram's dedicated coverage check) says some outer rows don't match
	// at all - so adding the two as-is double-counts those rows.
	// num_rows_LASJ is the more trustworthy of the two here: it comes from a
	// coverage check that deliberately falls back to a conservative answer
	// once it knows the histogram's bucket content can't be trusted, while
	// num_rows_inner_join's generic equi-join computation never consults the
	// mark at all - so rather than adding num_rows_LASJ's unmatched rows on
	// top of num_rows_inner_join's own total, credit num_rows_LASJ out of
	// that same total instead, trusting it as already present within
	// num_rows_inner_join's own count rather than additional to it. This
	// still adds back up to the original, uncorrected num_rows_inner_join
	// once num_rows_LASJ is added to the credited amount below, so it never
	// discards a one-to-many join's genuine fan-out the way an absolute cap
	// at the outer row count would. Only correct it when, taken together
	// with num_rows_LASJ, it would already overcount past the outer side's
	// own row count - num_rows_inner_join's generic computation may already
	// be reporting fewer matches than that, in which case it's not in
	// conflict with num_rows_LASJ's own finding and doesn't need adjusting.
	// Do this before num_rows_inner_join is used to weight any of this
	// LOJ's per-column histograms below, so every histogram this function
	// builds, and the row count the caller derives from it, stay
	// consistent.
	BOOL any_join_col_marked = false;
	for (ULONG j = 0; j < join_preds_stats->Size() && !any_join_col_marked; j++)
	{
		CStatsPredJoin *join_stats = (*join_preds_stats)[j];
		if (join_stats->HasValidColIdInner())
		{
			const CHistogram *inner_histogram =
				inner_side_stats->GetHistogram(join_stats->ColIdInner());
			if (nullptr != inner_histogram &&
				inner_histogram->IsUnsupportedPredDerived())
			{
				any_join_col_marked = true;
			}
		}
	}
	if (any_join_col_marked)
	{
		CDouble outer_rows = outer_side_stats->Rows();

		if (num_rows_inner_join + num_rows_LASJ > outer_rows)
		{
			num_rows_inner_join = std::max(CStatistics::MinRows,
										   num_rows_inner_join - num_rows_LASJ);
		}
	}

	UlongToHistogramMap *LOJ_histograms = GPOS_NEW(mp) UlongToHistogramMap(mp);

	ULongPtrArray *outer_colids_with_stats =
		outer_side_stats->GetColIdsWithStats(mp);
	const ULONG num_outer_cols = outer_colids_with_stats->Size();

	for (ULONG i = 0; i < num_outer_cols; i++)
	{
		ULONG colid = *(*outer_colids_with_stats)[i];
		const CHistogram *inner_join_histogram =
			inner_join_stats->GetHistogram(colid);
		GPOS_ASSERT(nullptr != inner_join_histogram);

		if (outer_side_join_cols->Get(colid))
		{
			// the outer side's own column is preserved in full by a LOJ, so
			// its value range is exact regardless of any imprecision on the
			// inner side - the only mark that can legitimately apply here is
			// one this column already carried coming in
			const CHistogram *outer_own_histogram =
				outer_side_stats->GetHistogram(colid);
			GPOS_ASSERT(nullptr != outer_own_histogram);
			BOOL outer_is_marked =
				outer_own_histogram->IsUnsupportedPredDerived();

			// add buckets from the outer histogram that do not contribute to the inner join
			const CHistogram *LASJ_histogram = LASJ_stats->GetHistogram(colid);
			GPOS_ASSERT(nullptr != LASJ_histogram);

			if (LASJ_histogram->IsWellDefined() && !LASJ_histogram->IsEmpty())
			{
				// union the buckets from the inner join and LASJ to get the LOJ buckets
				CHistogram *LOJ_histogram =
					LASJ_histogram->MakeUnionAllHistogramNormalize(
						num_rows_LASJ, inner_join_histogram,
						num_rows_inner_join);
				if (outer_is_marked)
				{
					LOJ_histogram->SetUnsupportedPredDerived();
				}
				CStatisticsUtils::AddHistogram(mp, colid, LOJ_histogram,
											   LOJ_histograms);
				GPOS_DELETE(LOJ_histogram);
			}
			else
			{
				CHistogram *result_histogram =
					inner_join_histogram->CopyHistogram();
				if (outer_is_marked)
				{
					result_histogram->SetUnsupportedPredDerived();
				}
				else
				{
					// inner_join_histogram may have picked up a mark from
					// the inner side alone, which says nothing about this
					// (preserved) outer column's own value range
					result_histogram->ResetUnsupportedPredDerived();
				}
				CStatisticsUtils::AddHistogram(mp, colid, result_histogram,
											   LOJ_histograms);
				GPOS_DELETE(result_histogram);
			}
		}
		else
		{
			// if column from the outer side that is not a join then just add it
			CStatisticsUtils::AddHistogram(mp, colid, inner_join_histogram,
										   LOJ_histograms);
		}
	}

	LASJ_stats->Release();

	// extract all columns from the inner child of the join
	ULongPtrArray *inner_colids_with_stats =
		inner_side_stats->GetColIdsWithStats(mp);

	// add its corresponding statistics
	AddHistogramsLOJInner(mp, inner_join_stats, inner_colids_with_stats,
						  num_rows_LASJ, num_rows_inner_join, LOJ_histograms);

	*result_rows_LASJ = num_rows_LASJ;
	*result_rows_inner_join = num_rows_inner_join;

	// clean up
	inner_colids_with_stats->Release();
	outer_colids_with_stats->Release();
	outer_side_join_cols->Release();

	return LOJ_histograms;
}


// helper function to add histograms of the inner side of a LOJ
void
CLeftOuterJoinStatsProcessor::AddHistogramsLOJInner(
	CMemoryPool *mp, const CStatistics *inner_join_stats,
	ULongPtrArray *inner_colids_with_stats, CDouble num_rows_LASJ,
	CDouble num_rows_inner_join, UlongToHistogramMap *LOJ_histograms)
{
	GPOS_ASSERT(nullptr != inner_join_stats);
	GPOS_ASSERT(nullptr != inner_colids_with_stats);
	GPOS_ASSERT(nullptr != LOJ_histograms);

	const ULONG num_inner_cols = inner_colids_with_stats->Size();

	for (ULONG ul = 0; ul < num_inner_cols; ul++)
	{
		ULONG colid = *(*inner_colids_with_stats)[ul];

		const CHistogram *inner_join_histogram =
			inner_join_stats->GetHistogram(colid);
		GPOS_ASSERT(nullptr != inner_join_histogram);

		// the number of nulls added to the inner side should be the number of rows of the LASJ on the outer side.
		CHistogram *null_histogram = GPOS_NEW(mp) CHistogram(
			mp, GPOS_NEW(mp) CBucketArray(mp), true /*is_well_defined*/,
			1.0 /*null_freq*/, CHistogram::DefaultNDVRemain,
			CHistogram::DefaultNDVFreqRemain, true /*is_col_stats_missing*/);
		CHistogram *LOJ_histogram =
			inner_join_histogram->MakeUnionAllHistogramNormalize(
				num_rows_inner_join, null_histogram, num_rows_LASJ);
		// this union can't undo an earlier unsupported predicate's
		// unreliable row-count guess on this column; the synthetic
		// null_histogram standing in for unmatched outer rows is never
		// itself marked
		if (inner_join_histogram->IsUnsupportedPredDerived())
		{
			LOJ_histogram->SetUnsupportedPredDerived();
		}
		CStatisticsUtils::AddHistogram(mp, colid, LOJ_histogram,
									   LOJ_histograms);

		GPOS_DELETE(null_histogram);
		GPOS_DELETE(LOJ_histogram);
	}
}

// EOF
