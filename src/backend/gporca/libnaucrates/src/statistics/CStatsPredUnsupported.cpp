//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CStatsPredUnsupported.cpp
//
//	@doc:
//		Implementation of unsupported statistics predicate
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatsPredUnsupported.h"

#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUnsupported::CStatsPredUnsupported
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredUnsupported::CStatsPredUnsupported(
	ULONG colid, CStatsPred::EStatsCmpType stats_cmp_type,
	ULongPtrArray *used_colids)
	: CStatsPred(colid),
	  m_stats_cmp_type(stats_cmp_type),
	  m_default_scale_factor(0.0),
	  m_used_colids(used_colids)
{
	m_default_scale_factor = InitScaleFactor();
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUnsupported::CStatsPredUnsupported
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredUnsupported::CStatsPredUnsupported(
	ULONG colid, CStatsPred::EStatsCmpType stats_cmp_type,
	CDouble default_scale_factor, ULongPtrArray *used_colids)
	: CStatsPred(colid),
	  m_stats_cmp_type(stats_cmp_type),
	  m_default_scale_factor(default_scale_factor),
	  m_used_colids(used_colids)
{
	GPOS_ASSERT(CStatistics::Epsilon < default_scale_factor);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUnsupported::~CStatsPredUnsupported
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CStatsPredUnsupported::~CStatsPredUnsupported()
{
	CRefCount::SafeRelease(m_used_colids);
}


//---------------------------------------------------------------------------
//		CStatsPredUnsupported::InitScaleFactor
//
//	@doc:
//		Initialize the scale factor of the unknown predicate
//---------------------------------------------------------------------------
CDouble
CStatsPredUnsupported::InitScaleFactor()
{
	return (1 / CHistogram::DefaultSelectivity).Get();
}

// EOF
