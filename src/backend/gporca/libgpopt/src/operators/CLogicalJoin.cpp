//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLogicalJoin.cpp
//
//	@doc:
//		Implementation of logical join class
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::CLogicalJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
//CLogicalJoin::CLogicalJoin(CMemoryPool *mp)
//	: CLogical(mp), m_join_order_origin_xform(CXform::ExfSentinel)
//{
//	GPOS_ASSERT(NULL != mp);
//}

CLogicalJoin::CLogicalJoin(CMemoryPool *mp, CXform::EXformId origin_xform)
	: CLogical(mp), m_origin_xform(origin_xform)
{
	GPOS_ASSERT(NULL != mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalJoin::Matches(COperator *pop) const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalJoin::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   IStatisticsArray *stats_ctxt) const
{
	return CJoinStatsProcessor::DeriveJoinStats(mp, exprhdl, stats_ctxt);
}

// EOF
