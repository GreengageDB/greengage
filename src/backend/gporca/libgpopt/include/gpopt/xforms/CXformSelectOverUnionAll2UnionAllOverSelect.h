//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformSelectOverUnionAll2UnionAllOverSelect.h
//
//	@doc:
//		Transform select over union all created at exploration phase into a
//		union all of selections from union's children.
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformSelectOverUnionAll2UnionAllOverSelect_H
#define GPOPT_CXformSelectOverUnionAll2UnionAllOverSelect_H

#include "gpos/base.h"

#include "gpopt/xforms/CXformExploration.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CXformSelectOverUnionAll2UnionAllOverSelect
//
//	@doc:
//		Transform select over union all created at exploration phase into a
//		union all of selections from union's children.
//---------------------------------------------------------------------------
class CXformSelectOverUnionAll2UnionAllOverSelect : public CXformExploration
{
private:
public:
	CXformSelectOverUnionAll2UnionAllOverSelect(
		const CXformSelectOverUnionAll2UnionAllOverSelect &) = delete;

	// ctor
	explicit CXformSelectOverUnionAll2UnionAllOverSelect(CMemoryPool *mp);

	// dtor
	~CXformSelectOverUnionAll2UnionAllOverSelect() override = default;

	// identifier
	EXformId
	Exfid() const override
	{
		return ExfSelectOverUnionAll2UnionAllOverSelect;
	}

	// xform name
	const CHAR *
	SzId() const override
	{
		return "CXformSelectOverUnionAll2UnionAllOverSelect";
	}

	// compute xform promise for a given expression handle
	EXformPromise Exfp(CExpressionHandle &exprhdl) const override;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const override;

};	// class CXformSelectOverUnionAll2UnionAllOverSelect
}  // namespace gpopt

#endif	// !GPOPT_CXformSelectOverUnionAll2UnionAllOverSelect_H

// EOF
