//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CStatsPredUnsupported.h
//
//	@doc:
//		Class representing unsupported statistics filter
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CStatsPredUnsupported_H
#define GPNAUCRATES_CStatsPredUnsupported_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"

#include "naucrates/statistics/CStatsPred.h"

namespace gpnaucrates
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CStatsPredUnsupported
//
//	@doc:
//		Class representing unsupported statistics filter
//---------------------------------------------------------------------------
class CStatsPredUnsupported : public CStatsPred
{
private:
	// predicate comparison type
	CStatsPred::EStatsCmpType m_stats_cmp_type;

	// scale factor of the predicate
	CDouble m_default_scale_factor;

	// columns actually referenced by the original predicate expression, for
	// predicates that don't reduce to a single "the" column (GetColId() ==
	// ulong_max). Null when not tracked. Owned by this object.
	ULongPtrArray *m_used_colids;

	// initialize the scale factor of the predicate
	CDouble InitScaleFactor();

	// private copy ctor
	CStatsPredUnsupported(const CStatsPredUnsupported &);

public:
	// ctors
	CStatsPredUnsupported(ULONG colid,
						  CStatsPred::EStatsCmpType stats_pred_type,
						  ULongPtrArray *used_colids = NULL);
	CStatsPredUnsupported(ULONG colid,
						  CStatsPred::EStatsCmpType stats_pred_type,
						  CDouble default_scale_factor,
						  ULongPtrArray *used_colids = NULL);

	virtual ~CStatsPredUnsupported();

	// filter type id
	virtual CStatsPred::EStatsPredType
	GetPredStatsType() const
	{
		return CStatsPred::EsptUnsupported;
	}

	// columns actually referenced by the original predicate expression
	// (may be null, or empty, when not tracked / not applicable)
	const ULongPtrArray *
	GetUsedColIds() const
	{
		return m_used_colids;
	}

	// comparison types for stats computation
	virtual CStatsPred::EStatsCmpType
	GetStatsCmpType() const
	{
		return m_stats_cmp_type;
	}

	CDouble
	ScaleFactor() const
	{
		return m_default_scale_factor;
	}

	// conversion function
	static CStatsPredUnsupported *
	ConvertPredStats(CStatsPred *pred_stats)
	{
		GPOS_ASSERT(NULL != pred_stats);
		GPOS_ASSERT(CStatsPred::EsptUnsupported ==
					pred_stats->GetPredStatsType());

		return dynamic_cast<CStatsPredUnsupported *>(pred_stats);
	}

};	// class CStatsPredUnsupported
}  // namespace gpnaucrates

#endif	// !GPNAUCRATES_CStatsPredUnsupported_H

// EOF
