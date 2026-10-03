//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CLeftOuterJoinStatsProcessor.h
//
//	@doc:
//		Processor for computing statistics for Left Outer Join
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CLeftOuterJoinStatsProcessor_H
#define GPNAUCRATES_CLeftOuterJoinStatsProcessor_H

#include "naucrates/statistics/CJoinStatsProcessor.h"

namespace gpnaucrates
{
class CLeftOuterJoinStatsProcessor : public CJoinStatsProcessor
{
private:
	// create a new hash map of histograms from the results of the inner join and the histograms of the outer child
	static UlongToHistogramMap *MakeLOJHistogram(
		CMemoryPool *mp, const CStatistics *outer_stats,
		const CStatistics *inner_side_stats, CStatistics *inner_join_stats,
		CStatsPredJoinArray *join_preds_stats, CDouble num_rows_inner_join,
		CDouble num_rows_inner_join_unfiltered, CDouble *result_rows_LASJ);

	// helper method to add histograms of the inner side of a LOJ
	static void AddHistogramsLOJInner(CMemoryPool *mp,
									  const CStatistics *inner_join_stats,
									  ULongPtrArray *inner_colids_with_stats,
									  CDouble num_rows_LASJ,
									  CDouble num_rows_inner_join,
									  UlongToHistogramMap *LOJ_histograms);

	// number of outer rows that lose all their matches to join predicates
	// that could not be modeled by the join histograms
	static CDouble NumRowsUnmatchedByUnsupportedPreds(
		CDouble num_rows_outer, CDouble num_rows_LASJ,
		CDouble num_rows_inner_join_unfiltered, CDouble num_rows_inner_join);

public:
	// return statistics object after performing LOJ operation with another statistics structure;
	// unsupported_pred_stats (optional) holds the join predicates that cannot be
	// modeled by the join histograms, they are applied to the matched part of the
	// join and the outer rows that lose all their matches become null-extended rows
	static CStatistics *CalcLOJoinStatsStatic(
		CMemoryPool *mp, const IStatistics *outer_stats,
		const IStatistics *inner_side_stats,
		CStatsPredJoinArray *join_preds_stats,
		CStatsPred *unsupported_pred_stats = nullptr);
};
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CLeftOuterJoinStatsProcessor_H

// EOF
