//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CScaleFactorUtils.cpp
//
//	@doc:
//		Helper routines to compute scale factors / damping factors
//---------------------------------------------------------------------------

#include "naucrates/statistics/CScaleFactorUtils.h"

#include "gpos/base.h"

#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionUtils.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;
using namespace gpmd;


// default scaling factor of a non-equality (<, >, <=, >=) join predicates
const CDouble CScaleFactorUtils::DefaultInequalityJoinPredScaleFactor(3.0);

// default scaling factor of join predicates
const CDouble CScaleFactorUtils::DefaultJoinPredScaleFactor(100.0);

// default scaling factor of LIKE predicate
const CDouble CScaleFactorUtils::DDefaultScaleFactorLike(150.0);

// invalid scale factor
const CDouble CScaleFactorUtils::InvalidScaleFactor(0.0);

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::GenerateScaleFactorMap
//
//	@doc:
//		Generate the hashmap of scale factors grouped by pred tables, also
//		produces array of complex (i.e. more than 2 tables involved in the predicate)
//		join preds
//
//---------------------------------------------------------------------------
CScaleFactorUtils::OIDPairToScaleFactorArrayMap *
CScaleFactorUtils::GenerateScaleFactorMap(
	CMemoryPool *mp, SJoinConditionArray *join_conds_scale_factors,
	CDoubleArray *independent_join_preds)
{
	GPOS_ASSERT(join_conds_scale_factors != NULL);

	// create a hashmap of size 7 as we don't anticipate many join conditions here. Creating a larger map
	// would be wasted memory.
	CScaleFactorUtils::OIDPairToScaleFactorArrayMap *scale_factor_hashmap =
		GPOS_NEW(mp) OIDPairToScaleFactorArrayMap(mp, 7);

	// If a dist col = dist col predicate exists, it needs to be the first element in the scale factor array
	// so that the predicate does not get damped, and any following predicate will be damped accordingly.
	// If more than one dist col = dist col predicate exists (in the case of joins on multi-distkey tables)
	// any additional dist col = dist col predicate are treated as independent
	BOOL contains_dist_pred = false;
	// iterate over joins to find predicates on same tables
	for (ULONG ul = 0; ul < join_conds_scale_factors->Size(); ul++)
	{
		CDouble local_scale_factor =
			(*(*join_conds_scale_factors)[ul]).m_scale_factor;
		IMdIdArray *oid_pair = (*(*join_conds_scale_factors)[ul]).m_oid_pair;
		BOOL both_dist_keys = (*(*join_conds_scale_factors)[ul]).m_dist_keys;

		if (oid_pair != NULL && oid_pair->Size() == 2)
		{
			// the array of scale factors in the order of damping
			// i.e. the scale_factor_array[0] is not damped, and any subsequent
			// element in the array is damped by the nth_root.
			CDoubleArray *scale_factor_array =
				scale_factor_hashmap->Find(oid_pair);

			// no predicates have been added, so create the scale factor array
			if (!scale_factor_array)
			{
				scale_factor_array = GPOS_NEW(mp) CDoubleArray(mp);
				scale_factor_array->Append(GPOS_NEW(mp)
											   CDouble(local_scale_factor));
				oid_pair->AddRef();
				scale_factor_hashmap->Insert(oid_pair, scale_factor_array);
				if (both_dist_keys)
				{
					contains_dist_pred = true;
				}
			}
			// if it's a dist key pred, it should not be damped, so handle accordingly
			else if (both_dist_keys)
			{
				if (!contains_dist_pred)
				{
					contains_dist_pred = true;
					// it is a dist key pred and none exist yet in the scale_factor array
					// so add it here as the first element in the scale factor array
					GPOS_ASSERT(scale_factor_array);
					CDoubleArray *new_scale_factor_array =
						GPOS_NEW(mp) CDoubleArray(mp);
					// add the dist key pred as the first predicate
					new_scale_factor_array->Append(
						GPOS_NEW(mp) CDouble(local_scale_factor));
					// append the rest of the predicates after
					for (ULONG i = 0; i < scale_factor_array->Size(); i++)
					{
						CDouble scale_factor = (*(*scale_factor_array)[i]);
						new_scale_factor_array->Append(
							GPOS_NEW(mp) CDouble(scale_factor));
					}
					scale_factor_hashmap->Replace(oid_pair,
												  new_scale_factor_array);
				}
				else
				{
					// a dist key predicate was already added to the scale_factor_array and any additional
					// dist key pred needs to be treated as independent, so add it to the correct array
					independent_join_preds->Append(
						GPOS_NEW(mp) CDouble(local_scale_factor));
				}
			}
			// not a dist key pred so just append as necessary
			else
			{
				GPOS_ASSERT(scale_factor_array);
				// otherwise add to scale factor array so that the predicate gets damped accordingly
				scale_factor_array->Append(GPOS_NEW(mp)
											   CDouble(local_scale_factor));
			}
		}
		else
		{
			independent_join_preds->Append(GPOS_NEW(mp)
											   CDouble(local_scale_factor));
		}
	}

	return scale_factor_hashmap;
}

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::CalcCumulativeScaleFactorSqrtAlg
//
//	@doc:
//		Generate a cumulative scale factor using a modified sqrt algorithm to
//		moderately decrease the impact of subsequent predicates to account for
//		correlated columns.
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::CalcCumulativeScaleFactorSqrtAlg(
	OIDPairToScaleFactorArrayMap *scale_factor_hashmap,
	CDoubleArray *independent_join_preds)
{
	CDouble cumulative_scale_factor(1.0);

	CScaleFactorUtils::OIDPairToScaleFactorArrayMapIter iter(
		scale_factor_hashmap);
	// calculate damping using new sqrt algorithm
	while (iter.Advance())
	{
		const CDoubleArray *scale_factor_array = iter.Value();

		// damp the join preds if they are on the same tables (ex: t1.a = t2.a AND t1.b = t2.b)
		for (ULONG ul = 0; ul < scale_factor_array->Size(); ul++)
		{
			CDouble local_scale_factor = *(*scale_factor_array)[ul];
			CDouble fp(2);
			CDouble nth_root = fp.Pow(ul);
			cumulative_scale_factor =
				cumulative_scale_factor *
				std::max(CStatistics::MinRows.Get(),
						 local_scale_factor.Pow(CDouble(1) / nth_root).Get());
		}
	}

	// independent_join_preds are either dist_key = dist_key preds or
	// more complex predicates, such as t1.a = t2.a + t3.a;
	for (ULONG ul = 0; ul < independent_join_preds->Size(); ul++)
	{
		CDouble local_scale_factor = *(*independent_join_preds)[ul];
		cumulative_scale_factor = cumulative_scale_factor * local_scale_factor;
	}

	return cumulative_scale_factor;
}

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::CumulativeJoinScaleFactor
//
//	@doc:
//		Calculate the cumulative join scaling factor
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::CumulativeJoinScaleFactor(
	CMemoryPool *mp, const CStatisticsConfig *stats_config,
	SJoinConditionArray *join_conds_scale_factors,
	CDouble limit_for_result_scale_factor)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != join_conds_scale_factors);

	const ULONG num_join_conds = join_conds_scale_factors->Size();
	if (1 < num_join_conds)
	{
		// sort (in desc order) the scaling factor of the join conditions
		join_conds_scale_factors->Sort(
			CScaleFactorUtils::DescendingOrderCmpJoinFunc);
	}

	// We have two methods to calculate the cumulative scale factor:
	// 1. When optimizer_damping_factor_join is greater than 0, use the legacy damping method
	//    Note: The default value (.01) severely overestimates cardinalities for non-correlated columns
	//
	// 2. Otherwise, use a damping method to moderately decrease the impact of subsequent predicates to
	//    account for correlated columns. This damping only occurs on sorted predicates of the same table,
	//    otherwise we assume independence.
	//
	//    For example, given ANDed predicates (t1.a = t2.a AND t1.b = t2.b AND t2.b = t3.a) with the given selectivities:
	//			(S1) t1.a = t2.a has selectivity .3
	//			(S2) t1.b = t2.b has selectivity .5
	//			(S3) t2.b = t3.a has selectivity .1
	//	  S1 and S2 would use the sqrt algorithm, and S3 is independent. Additionally,
	//	  S2 has a larger selectivity so it comes first.
	//	  The cumulative selectivity would be as follows:
	//      S = ( S2 * sqrt(S1) ) * S3
	//    .03 = .5 * sqrt(.3) * .1
	//    For scale factors, this is equivalent to ( SF2 * sqrt(SF1) ) * SF3
	//
	//    Note: This will underestimate the cardinality of highly correlated columns and overestimate the
	//    cardinality of highly independent columns, but seems to be a good middle ground in the absence
	//    of correlated column statistics
	//
	//    However, if both sides of the predicate are distribution columns, we assume that this predicate
	//    is not correlated with any other predicate. This assumption comes from the idea that distribution
	//    cols are ideally unique for each record to gain the best possible performance. This is a best
	//    guess since we do not have a way to support correlated columns at this time.
	//

	CDouble cumulative_scale_factor(1.0);
	if (stats_config->DDampingFactorJoin() > 0)
	{
		for (ULONG ul = 0; ul < num_join_conds; ul++)
		{
			CDouble local_scale_factor =
				(*(*join_conds_scale_factors)[ul]).m_scale_factor;
			cumulative_scale_factor =
				cumulative_scale_factor *
				std::max(CStatistics::MinRows.Get(),
						 (local_scale_factor *
						  DampedJoinScaleFactor(stats_config, ul + 1))
							 .Get());
		}
	}
	else
	{
		// save the join preds that are not simple equalities in a different array
		CDoubleArray *independent_join_preds = GPOS_NEW(mp) CDoubleArray(mp);
		// create the map of sorted join preds
		CScaleFactorUtils::OIDPairToScaleFactorArrayMap *scale_factor_hashmap =
			CScaleFactorUtils::GenerateScaleFactorMap(
				mp, join_conds_scale_factors, independent_join_preds);

		cumulative_scale_factor =
			CScaleFactorUtils::CalcCumulativeScaleFactorSqrtAlg(
				scale_factor_hashmap, independent_join_preds);

		// Limit the scale factor, usually to the cardinality of the larger of the
		// joined tables. This causes the resulting join cardinality to be at least
		// the size of the smaller table. The reason for this is that we want to
		// assume a referential integrity constraint between the two joined tables,
		// so a row in one table will match with at least one row in the other
		// table. This makes multi-predicate joins more similar to single
		// predicates, where we make the same assumption. This assumption is
		// baked in the formula itself: When we divide the cartesian product
		// by the max of the NDVs that means that every one of these NDVs will
		// have a match in the other table. Another way to look at it is that
		// 'cumulative_scale_factor' represents the NDV of the combined equi-join
		// columns (ignore non-equi joins for a moment). We know that this NDV
		// cannot exceed the cardinality of the larger of the tables.
		cumulative_scale_factor = std::min(cumulative_scale_factor.Get(),
										   limit_for_result_scale_factor.Get());

		independent_join_preds->Release();
		scale_factor_hashmap->Release();
	}

	return cumulative_scale_factor;
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DampedJoinScaleFactor
//
//	@doc:
//		Return scaling factor of the join predicate after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DampedJoinScaleFactor(const CStatisticsConfig *stats_config,
										 ULONG num_columns)
{
	if (1 >= num_columns)
	{
		return CDouble(1.0);
	}

	return stats_config->DDampingFactorJoin().Pow(CDouble(num_columns));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DampedFilterScaleFactor
//
//	@doc:
//		Return scaling factor of the filter after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DampedFilterScaleFactor(
	const CStatisticsConfig *stats_config, ULONG num_columns)
{
	GPOS_ASSERT(NULL != stats_config);

	if (1 >= num_columns)
	{
		return CDouble(1.0);
	}

	return stats_config->DDampingFactorFilter().Pow(CDouble(num_columns));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DampedGroupByScaleFactor
//
//	@doc:
//		Return scaling factor of the group by predicate after apply damping
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::DampedGroupByScaleFactor(
	const CStatisticsConfig *stats_config, ULONG num_columns)
{
	GPOS_ASSERT(NULL != stats_config);

	if (1 > num_columns)
	{
		return CDouble(1.0);
	}

	return stats_config->DDampingFactorGroupBy().Pow(CDouble(num_columns + 1));
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::SortScalingFactor
//
//	@doc:
//		Sort the array of scaling factor
//
//---------------------------------------------------------------------------
void
CScaleFactorUtils::SortScalingFactor(CDoubleArray *scale_factors,
									 BOOL is_descending)
{
	GPOS_ASSERT(NULL != scale_factors);
	const ULONG num_cols = scale_factors->Size();
	if (1 < num_cols)
	{
		if (is_descending)
		{
			// sort (in desc order) the scaling factor based on the selectivity of each column
			scale_factors->Sort(CScaleFactorUtils::DescendingOrderCmpFunc);
		}
		else
		{
			// sort (in ascending order) the scaling factor based on the selectivity of each column
			scale_factors->Sort(CScaleFactorUtils::AscendingOrderCmpFunc);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DescendingOrderCmpFunc
//
//	@doc:
//		Comparison function for sorting double
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::DescendingOrderCmpFunc(const void *val1, const void *val2)
{
	GPOS_ASSERT(NULL != val1 && NULL != val2);
	const CDouble *double_val1 = *(const CDouble **) val1;
	const CDouble *double_val2 = *(const CDouble **) val2;

	return DoubleCmpFunc(double_val1, double_val2, true /*is_descending*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DescendingOrderCmpJoinFunc
//
//	@doc:
//		Comparison function for sorting SJoinCondition
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::DescendingOrderCmpJoinFunc(const void *val1,
											  const void *val2)
{
	GPOS_ASSERT(NULL != val1 && NULL != val2);
	const CDouble double_val1 =
		(*(const SJoinCondition **) val1)->m_scale_factor;
	const CDouble double_val2 =
		(*(const SJoinCondition **) val2)->m_scale_factor;

	return DoubleCmpFunc(&double_val1, &double_val2, true /*is_descending*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::AscendingOrderCmpFunc
//
//	@doc:
//		Comparison function for sorting double
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::AscendingOrderCmpFunc(const void *val1, const void *val2)
{
	GPOS_ASSERT(NULL != val1 && NULL != val2);
	const CDouble *double_val1 = *(const CDouble **) val1;
	const CDouble *double_val2 = *(const CDouble **) val2;

	return DoubleCmpFunc(double_val1, double_val2, false /*is_descending*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::DoubleCmpFunc
//
//	@doc:
//		Helper function for double comparison
//
//---------------------------------------------------------------------------
INT
CScaleFactorUtils::DoubleCmpFunc(const CDouble *double_val1,
								 const CDouble *double_val2, BOOL is_descending)
{
	GPOS_ASSERT(NULL != double_val1);
	GPOS_ASSERT(NULL != double_val2);

	if (double_val1->Get() == double_val2->Get())
	{
		return 0;
	}

	if (double_val1->Get() < double_val2->Get() && is_descending)
	{
		return 1;
	}

	if (double_val1->Get() > double_val2->Get() && !is_descending)
	{
		return 1;
	}

	return -1;
}

//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::CalcScaleFactorCumulativeConj
//
//	@doc:
//		Calculate the cumulative scaling factor for conjunction of filters
//		after applying damping multiplier
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::CalcScaleFactorCumulativeConj(
	const CStatisticsConfig *stats_config, CDoubleArray *scale_factors)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != scale_factors);

	const ULONG num_cols = scale_factors->Size();
	CDouble scale_factor(1.0);
	if (1 < num_cols)
	{
		// sort (in desc order) the scaling factor based on the selectivity of each column
		scale_factors->Sort(CScaleFactorUtils::DescendingOrderCmpFunc);
	}

	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		// apply damping factor
		CDouble local_scale_factor = *(*scale_factors)[ul];
		scale_factor =
			scale_factor * std::max(CStatistics::MinRows.Get(),
									(local_scale_factor *
									 CScaleFactorUtils::DampedFilterScaleFactor(
										 stats_config, ul + 1))
										.Get());
	}

	return scale_factor;
}


//---------------------------------------------------------------------------
//	@function:
//		CScaleFactorUtils::CalcScaleFactorCumulativeDisj
//
//	@doc:
//		Calculate the cumulative scaling factor for disjunction of filters
//		after applying damping multiplier
//
//---------------------------------------------------------------------------
CDouble
CScaleFactorUtils::CalcScaleFactorCumulativeDisj(
	const CStatisticsConfig *stats_config, CDoubleArray *scale_factors,
	CDouble total_rows)
{
	GPOS_ASSERT(NULL != stats_config);
	GPOS_ASSERT(NULL != scale_factors);

	const ULONG num_cols = scale_factors->Size();
	GPOS_ASSERT(0 < num_cols);

	if (1 == num_cols)
	{
		return *(*scale_factors)[0];
	}

	// sort (in ascending order) the scaling factor based on the selectivity of each column
	scale_factors->Sort(CScaleFactorUtils::AscendingOrderCmpFunc);

	// accumulate row estimates of different predicates after applying damping
	// rows = rows0 + rows1 * 0.75 + rows2 *(0.75)^2 + ...

	CDouble rows(0.0);
	for (ULONG ul = 0; ul < num_cols; ul++)
	{
		CDouble local_scale_factor = *(*scale_factors)[ul];
		GPOS_ASSERT(InvalidScaleFactor < local_scale_factor);

		// get a row estimate based on current scale factor
		CDouble local_rows = total_rows / local_scale_factor;

		// accumulate row estimates after damping
		rows =
			rows +
			std::max(CStatistics::MinRows.Get(),
					 (local_rows * CScaleFactorUtils::DampedFilterScaleFactor(
									   stats_config, ul + 1))
						 .Get());

		// cap accumulated row estimate with total number of rows
		rows = std::min(rows.Get(), total_rows.Get());
	}

	// return an accumulated scale factor based on accumulated row estimate
	return CDouble(total_rows / rows);
}


// EOF
