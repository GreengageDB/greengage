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

#include "naucrates/statistics/CFilterStatsProcessor.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpmd;

// return statistics object after performing LOJ operation with another statistics structure
CStatistics *
CLeftOuterJoinStatsProcessor::CalcLOJoinStatsStatic(
	CMemoryPool *mp, const IStatistics *outer_side_stats,
	const IStatistics *inner_side_stats, CStatsPredJoinArray *join_preds_stats,
	CStatsPred *unsupported_pred_stats)
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
	CDouble num_rows_inner_join_unfiltered = inner_join_stats->Rows();

	if (nullptr != unsupported_pred_stats)
	{
		// Join predicates that cannot be modeled by the join histograms only
		// apply to the matched pairs of the join, so filter the inner join
		// result with them. The outer rows that lose all their matches this
		// way are added back as null-extended rows in MakeLOJHistogram(),
		// so that Card(LOJ) = Card(matched) + Card(null-extended) still holds
		// and the null fraction of the inner columns reflects those rows.
		// TODO: we currently only cap NDVs for filters immediately on top of tables.
		CStatistics *inner_join_stats_filtered =
			CFilterStatsProcessor::MakeStatsFilter(mp, inner_join_stats,
												   unsupported_pred_stats,
												   false /* do_cap_NDVs */);
		inner_join_stats->Release();
		inner_join_stats = inner_join_stats_filtered;
	}
	CDouble num_rows_inner_join = inner_join_stats->Rows();
	CDouble num_rows_LASJ(1.0);

	// create a new hash map of histograms, for each column from the outer child
	// add the buckets that do not contribute to the inner join
	UlongToHistogramMap *LOJ_histograms =
		CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
			mp, result_stats_outer_side, result_stats_inner_side,
			inner_join_stats, join_preds_stats, num_rows_inner_join,
			num_rows_inner_join_unfiltered, &num_rows_LASJ);

	// cardinality of LOJ is at least the cardinality of the outer child
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

// number of outer rows that lose all their matches to join predicates that
// could not be modeled by the join histograms (see CalcLOJoinStatsStatic);
// the caller has already estimated
//   num_rows_LASJ: outer rows without a match under the supported predicates,
//   num_rows_inner_join_unfiltered: matched pairs under the supported predicates,
//   num_rows_inner_join: matched pairs that also survive the unsupported predicates.
//
// With k matches per matched outer row and a fraction s of the matched pairs
// surviving the unsupported predicates, an outer row keeps at least one
// match with probability 1 - (1 - s)^k. This assumes attribute value
// independence, as the rest of the cardinality model does: the unsupported
// predicates do not favor the rows the supported ones matched, and they act
// independently on the k matches of a row. Under positive correlation
// between the predicates (e.g. an unsupported predicate that depends on the
// outer row only, so it succeeds or fails for all k matches at once) the
// true number of unmatched rows lies between the value computed here and
// num_rows_matched * (1 - s), the perfectly correlated case; for k = 1 the
// two coincide.
CDouble
CLeftOuterJoinStatsProcessor::NumRowsUnmatchedByUnsupportedPreds(
	CDouble num_rows_outer, CDouble num_rows_LASJ,
	CDouble num_rows_inner_join_unfiltered, CDouble num_rows_inner_join)
{
	if (num_rows_inner_join_unfiltered < CStatistics::Epsilon ||
		num_rows_inner_join >= num_rows_inner_join_unfiltered)
	{
		// nothing was filtered out
		return CDouble(0.0);
	}

	// outer rows that have at least one match under the supported predicates;
	// this can come out negative when the LASJ estimate was clamped to
	// CStatistics::MinRows on a tiny input, which the check below covers
	CDouble num_rows_matched = num_rows_outer - num_rows_LASJ;
	if (num_rows_matched < CStatistics::Epsilon)
	{
		return CDouble(0.0);
	}

	// fraction of the matched pairs that survive the unsupported predicates
	CDouble selectivity = num_rows_inner_join / num_rows_inner_join_unfiltered;

	// average number of matches of a matched outer row; assuming the
	// unsupported predicates act independently on each of these matches,
	// an outer row keeps at least one match with probability 1 - (1 - s)^k
	CDouble matches_per_outer_row =
		std::max(CDouble(1.0),
				 CDouble(num_rows_inner_join_unfiltered / num_rows_matched));
	CDouble prob_keeps_match =
		CDouble(1.0) - (CDouble(1.0) - selectivity).Pow(matches_per_outer_row);

	return num_rows_matched * (CDouble(1.0) - prob_keeps_match);
}

// create a new hash map of histograms for LOJ from the histograms
// of the outer child and the histograms of the inner join
UlongToHistogramMap *
CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
	CMemoryPool *mp, const CStatistics *outer_side_stats,
	const CStatistics *inner_side_stats, CStatistics *inner_join_stats,
	CStatsPredJoinArray *join_preds_stats, CDouble num_rows_inner_join,
	CDouble num_rows_inner_join_unfiltered, CDouble *result_rows_LASJ)
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

	// outer rows that had a match under the supported join predicates but
	// lost all their matches to the unsupported ones; we do not know which
	// outer values these are, so they are assumed to be spread like the
	// values of the outer child
	CDouble num_rows_LASJ_unsupported = NumRowsUnmatchedByUnsupportedPreds(
		outer_side_stats->Rows(), num_rows_LASJ, num_rows_inner_join_unfiltered,
		num_rows_inner_join);
	CDouble num_rows_LASJ_total = num_rows_LASJ + num_rows_LASJ_unsupported;

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

		// histogram of the column after the LOJ and the number of rows it describes
		CHistogram *LOJ_histogram = nullptr;
		CDouble num_rows_LOJ_histogram = num_rows_inner_join;

		if (outer_side_join_cols->Get(colid))
		{
			// add buckets from the outer histogram that do not contribute to the inner join
			const CHistogram *LASJ_histogram = LASJ_stats->GetHistogram(colid);
			GPOS_ASSERT(nullptr != LASJ_histogram);

			if (LASJ_histogram->IsWellDefined() && !LASJ_histogram->IsEmpty())
			{
				// union the buckets from the inner join and LASJ to get the LOJ buckets
				LOJ_histogram = LASJ_histogram->MakeUnionAllHistogramNormalize(
					num_rows_LASJ, inner_join_histogram, num_rows_inner_join);
				num_rows_LOJ_histogram = num_rows_LASJ + num_rows_inner_join;
			}
		}

		if (nullptr == LOJ_histogram)
		{
			// column from the outer side that is not a join column, or a join
			// column whose LASJ histogram is empty: use the inner join histogram
			LOJ_histogram = inner_join_histogram->CopyHistogram();
		}

		if (num_rows_LASJ_unsupported > CStatistics::Epsilon)
		{
			const CHistogram *outer_histogram =
				outer_side_stats->GetHistogram(colid);
			GPOS_ASSERT(nullptr != outer_histogram);

			if (outer_histogram->IsWellDefined() &&
				LOJ_histogram->IsWellDefined())
			{
				// add the outer rows that lost their matches to the
				// unsupported predicates, spread like the outer child
				CHistogram *LOJ_histogram_with_unmatched =
					LOJ_histogram->MakeUnionAllHistogramNormalize(
						num_rows_LOJ_histogram, outer_histogram,
						num_rows_LASJ_unsupported);
				GPOS_DELETE(LOJ_histogram);
				LOJ_histogram = LOJ_histogram_with_unmatched;
			}
		}

		CStatisticsUtils::AddHistogram(mp, colid, LOJ_histogram,
									   LOJ_histograms);
		GPOS_DELETE(LOJ_histogram);
	}
	LASJ_stats->Release();

	// extract all columns from the inner child of the join
	ULongPtrArray *inner_colids_with_stats =
		inner_side_stats->GetColIdsWithStats(mp);

	// add its corresponding statistics
	AddHistogramsLOJInner(mp, inner_join_stats, inner_colids_with_stats,
						  num_rows_LASJ_total, num_rows_inner_join,
						  LOJ_histograms);

	*result_rows_LASJ = num_rows_LASJ_total;

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
		CStatisticsUtils::AddHistogram(mp, colid, LOJ_histogram,
									   LOJ_histograms);

		GPOS_DELETE(null_histogram);
		GPOS_DELETE(LOJ_histogram);
	}
}

// EOF
