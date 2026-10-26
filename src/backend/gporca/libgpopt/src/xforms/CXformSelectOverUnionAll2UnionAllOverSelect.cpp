//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformSelectOverUnionAll2UnionAllOverSelect.cpp
//
//	@doc:
//		Transform select over union all created at exploration phase into a
//		union all of selections from union's children.
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSelectOverUnionAll2UnionAllOverSelect.h"

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/xforms/CXformUtils.h"

using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelectOverUnionAll2UnionAllOverSelect::CXformSelectOverUnionAll2UnionAllOverSelect
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelectOverUnionAll2UnionAllOverSelect::
	CXformSelectOverUnionAll2UnionAllOverSelect(CMemoryPool *mp)
	: CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalUnionAll(mp),  // Union All
			  GPOS_NEW(mp) CExpression(
				  mp, GPOS_NEW(mp) CPatternMultiLeaf(mp))),	 // children
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))))	// predicate
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelectOverUnionAll2UnionAllOverSelect::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelectOverUnionAll2UnionAllOverSelect::Exfp(
	CExpressionHandle &exprhndl) const
{
	if (exprhndl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelectOverUnionAll2UnionAllOverSelect::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelectOverUnionAll2UnionAllOverSelect::Transform(CXformContext *pxfctxt,
													   CXformResult *pxfres,
													   CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));
	GPOS_ASSERT(nullptr != pxfres);

	CMemoryPool *mp = pxfctxt->Pmp();

	CExpression *pexprUnionAll = (*pexpr)[0];
	CExpression *pexprPred = (*pexpr)[1];

	CLogicalUnionAll *popUnionAll =
		CLogicalUnionAll::PopConvert(pexprUnionAll->Pop());

	// output columns of Union All.
	CColRefArray *pdrgpcrOutput = popUnionAll->PdrgpcrOutput();
	CColRefSet *pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, pdrgpcrOutput);

	CColRef2dArray *pdrgpdrgpcrInput = popUnionAll->PdrgpdrgpcrInput();

	// invariant: length of input array of arrays is equal to union arity
	GPOS_ASSERT(pexprUnionAll->Arity() == pdrgpdrgpcrInput->Size());
	GPOS_ASSERT(nullptr != (*pexprUnionAll)[0]->Pgexpr());

	// Bail out if the predicate references columns the union does not
	// produce (outer references, e.g. inside the inner side of a correlated
	// apply). The remap below copies the predicate with must_exist = true,
	// which would invent fresh column refs for such columns, and no union
	// child could satisfy the resulting select
	BOOL fPushable = pcrsOutput->ContainsAll(pexprPred->DeriveUsedColumns());
	pcrsOutput->Release();
	if (!fPushable)
	{
		return;
	}

	if (CUtils::FScalarConstTrue(pexprPred) ||
		CUtils::FScalarConstFalse(pexprPred))
	{
		return;
	}

	// Replicates the logic from CNormalizer::PushThruSetOp(). We need
	// to remap the predicate's column references to the children input
	// columns. In this case pushdown will be legit in terms of output columns.
	CExpressionArray *pdrgpexprSubSelects = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG arity = pexprUnionAll->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = (*pexprUnionAll)[ul];
		CColRefArray *pdrgpcrChild = (*pdrgpdrgpcrInput)[ul];

		pexprPred->AddRef();
		CExpression *pexprRemappedPred = pexprPred;

		// shortcut similar to CNormalizer::PushThruSetOp()
		if (!pdrgpcrChild->Equals(pdrgpcrOutput))
		{
			UlongToColRefMap *colref_mapping =
				CUtils::PhmulcrMapping(mp, pdrgpcrOutput, pdrgpcrChild);
			pexprRemappedPred->Release();
			pexprRemappedPred = pexprPred->PexprCopyWithRemappedColumns(
				mp, colref_mapping, true /*must_exist*/);
			colref_mapping->Release();
		}

		GPOS_ASSERT(pexprChild->DeriveOutputColumns()->ContainsAll(
			pexprRemappedPred->DeriveUsedColumns()));

		pexprChild->AddRef();
		CExpression *pexprChildSelect =
			CUtils::PexprLogicalSelect(mp, pexprChild, pexprRemappedPred);
		pdrgpexprSubSelects->Append(pexprChildSelect);
	}

	pdrgpcrOutput->AddRef();
	pdrgpdrgpcrInput->AddRef();
	CLogicalUnionAll *popNewUnionAll =
		GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgpdrgpcrInput);
	CExpression *pexprResult =
		GPOS_NEW(mp) CExpression(mp, popNewUnionAll, pdrgpexprSubSelects);
	pxfres->Add(pexprResult);
}

// EOF
