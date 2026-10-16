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
	CDouble num_rows_inner_join = inner_join_stats->Rows();
	CDouble num_rows_LASJ(1.0);

	// create a new hash map of histograms, for each column from the outer child
	// add the buckets that do not contribute to the inner join
	UlongToHistogramMap *LOJ_histograms =
		CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
			mp, result_stats_outer_side, result_stats_inner_side,
			inner_join_stats, join_preds_stats, unsupported_pred_stats,
			num_rows_inner_join, &num_rows_LASJ, &num_rows_inner_join);

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

// number of outer rows that lose all their matches to join predicates that
// could not be modeled by the join histograms (see MakeLOJHistogram);
// the caller has already estimated
//   num_rows_LASJ: outer rows without a match under the supported predicates,
//   num_rows_inner_join_unfiltered: matched pairs under the supported predicates,
//   num_rows_inner_join: matched pairs that also survive the unsupported predicates.
//
// The unsupported predicates are applied with a default selectivity s to
// the matched pairs (see CStatsPredUnsupported). We take that same fraction
// to apply to the matched outer rows: a fraction 1 - s of them loses all
// its matches and becomes null-extended. This treats the predicates as
// deciding per outer row rather than per matched pair, i.e. as perfectly
// correlated across the matches of one row. The alternative, per-pair
// independence, gives (1 - s)^k for k matches per outer row; with the
// default s = 0.4 that is below 1% for k >= 9, so for any key-to-foreign-key
// left join it would predict no null-extended rows at all, which is the
// estimate this function exists to correct. The per-row form is bounded by
// the outer cardinality, and for one-to-one joins both forms coincide.
CDouble
CLeftOuterJoinStatsProcessor::NumRowsUnmatchedByUnsupportedPreds(
	CDouble num_rows_outer, CDouble num_rows_LASJ,
	CDouble num_rows_inner_join_unfiltered, CDouble num_rows_inner_join)
{
	// the unsupported predicates are applied as a filter on the inner join,
	// and a filter can only reduce the row count or clamp it to MinRows
	GPOS_ASSERT(num_rows_inner_join <=
				num_rows_inner_join_unfiltered + CStatistics::Epsilon);

	if (num_rows_inner_join_unfiltered < CStatistics::Epsilon ||
		num_rows_inner_join_unfiltered - num_rows_inner_join <
			CStatistics::Epsilon)
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

	return num_rows_matched * (CDouble(1.0) - selectivity);
}

// add the num_rows_LASJ_unsupported outer rows that lost all their matches to
// the unsupported join predicates to LOJ_histogram, which describes
// num_rows_LOJ_histogram rows of an outer column; we do not know which outer
// values these rows have, so they are assumed to be spread like the values of
// the outer child. Takes ownership of LOJ_histogram and returns the histogram
// to use in its place.
CHistogram *
CLeftOuterJoinStatsProcessor::AddUnmatchedOuterRows(
	CHistogram *LOJ_histogram, CDouble num_rows_LOJ_histogram,
	const CHistogram *outer_histogram, CDouble num_rows_LASJ_unsupported)
{
	GPOS_ASSERT(nullptr != LOJ_histogram);
	GPOS_ASSERT(nullptr != outer_histogram);

	if (num_rows_LASJ_unsupported < CStatistics::Epsilon ||
		!outer_histogram->IsWellDefined() || !LOJ_histogram->IsWellDefined())
	{
		return LOJ_histogram;
	}

	CHistogram *result_histogram =
		LOJ_histogram->MakeUnionAllHistogramNormalize(
			num_rows_LOJ_histogram, outer_histogram, num_rows_LASJ_unsupported);
	// the union builds a fresh histogram, keep the mark of the one it replaces
	if (LOJ_histogram->IsUnsupportedPredDerived())
	{
		result_histogram->SetUnsupportedPredDerived();
	}
	GPOS_DELETE(LOJ_histogram);

	return result_histogram;
}

// create a new hash map of histograms for LOJ from the histograms
// of the outer child and the histograms of the inner join
UlongToHistogramMap *
CLeftOuterJoinStatsProcessor::MakeLOJHistogram(
	CMemoryPool *mp, const CStatistics *outer_side_stats,
	const CStatistics *inner_side_stats, CStatistics *inner_join_stats,
	CStatsPredJoinArray *join_preds_stats, CStatsPred *unsupported_pred_stats,
	CDouble num_rows_inner_join, CDouble *result_rows_LASJ,
	CDouble *result_rows_inner_join)
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

	// Join predicates that cannot be modeled by the join histograms only apply
	// to the matched pairs of the join, so filter the inner join with them; the
	// outer rows that lose all their matches this way are added back below as
	// null-extended rows, so that Card(LOJ) = Card(matched) + Card(null-extended)
	// still holds and the null fraction of the inner columns reflects those
	// rows. The filter starts from the corrected num_rows_inner_join, so the
	// inner join statistics are scaled to that count first if it changed.
	// TODO: we currently only cap NDVs for filters immediately on top of tables.
	CDouble num_rows_inner_join_unfiltered = num_rows_inner_join;
	CStatistics *matched_stats = inner_join_stats;
	matched_stats->AddRef();
	if (nullptr != unsupported_pred_stats)
	{
		if (inner_join_stats->Rows() > CStatistics::Epsilon &&
			num_rows_inner_join != inner_join_stats->Rows())
		{
			matched_stats->Release();
			matched_stats = CStatistics::CastStats(inner_join_stats->ScaleStats(
				mp, num_rows_inner_join / inner_join_stats->Rows()));
		}
		CStatistics *filtered_stats = CFilterStatsProcessor::MakeStatsFilter(
			mp, matched_stats, unsupported_pred_stats, false /* do_cap_NDVs */);
		matched_stats->Release();
		matched_stats = filtered_stats;
		num_rows_inner_join = matched_stats->Rows();
	}

	// outer rows that had a match under the supported join predicates but
	// lost all their matches to the unsupported ones
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
			matched_stats->GetHistogram(colid);
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
				LOJ_histogram = AddUnmatchedOuterRows(
					LOJ_histogram, num_rows_LASJ + num_rows_inner_join,
					outer_own_histogram, num_rows_LASJ_unsupported);
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
				result_histogram = AddUnmatchedOuterRows(
					result_histogram, num_rows_inner_join, outer_own_histogram,
					num_rows_LASJ_unsupported);
				CStatisticsUtils::AddHistogram(mp, colid, result_histogram,
											   LOJ_histograms);
				GPOS_DELETE(result_histogram);
			}
		}
		else
		{
			// if column from the outer side that is not a join then just add it
			const CHistogram *outer_histogram =
				outer_side_stats->GetHistogram(colid);
			GPOS_ASSERT(nullptr != outer_histogram);
			CHistogram *LOJ_histogram = AddUnmatchedOuterRows(
				inner_join_histogram->CopyHistogram(), num_rows_inner_join,
				outer_histogram, num_rows_LASJ_unsupported);
			CStatisticsUtils::AddHistogram(mp, colid, LOJ_histogram,
										   LOJ_histograms);
			GPOS_DELETE(LOJ_histogram);
		}
	}

	LASJ_stats->Release();

	// extract all columns from the inner child of the join
	ULongPtrArray *inner_colids_with_stats =
		inner_side_stats->GetColIdsWithStats(mp);

	// add its corresponding statistics
	AddHistogramsLOJInner(mp, matched_stats, inner_colids_with_stats,
						  num_rows_LASJ_total, num_rows_inner_join,
						  LOJ_histograms);

	*result_rows_LASJ = num_rows_LASJ_total;
	*result_rows_inner_join = num_rows_inner_join;

	// clean up
	matched_stats->Release();
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
