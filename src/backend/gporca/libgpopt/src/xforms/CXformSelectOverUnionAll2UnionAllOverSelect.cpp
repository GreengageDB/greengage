//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2026 Greengage Community
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
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternMultiLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/operators/CPredicateUtils.h"

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

	// Pushing a constant predicate into the branches only enlarges the
	// plan space: a constant true select is dropped by the normalizer and
	// a constant false one would be copied into every branch.
	if (CUtils::FScalarConstTrue(pexprPred) ||
		CUtils::FScalarConstFalse(pexprPred))
	{
		return;
	}

	// Bail out unless at least one conjunct can be pushed, i.e. references
	// only columns the union produces. Conjuncts with outer references
	// (e.g. inside the inner side of a correlated apply) are kept on top by
	// the normalizer; if none is pushable, the normalizer would return an
	// expression identical to the input.
	CColRefSet *pcrsOutput = pexprUnionAll->DeriveOutputColumns();
	CExpressionArray *pdrgpexprConjuncts =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprPred);
	BOOL fPushable = false;
	const ULONG ulConjuncts = pdrgpexprConjuncts->Size();
	for (ULONG ul = 0; ul < ulConjuncts && !fPushable; ul++)
	{
		fPushable = pcrsOutput->ContainsAll(
			(*pdrgpexprConjuncts)[ul]->DeriveUsedColumns());
	}
	pdrgpexprConjuncts->Release();
	if (!fPushable)
	{
		return;
	}

	// The normalizer pushes the predicate through the set operation
	// (CNormalizer::PushThruSetOp) remapping the union's output columns to
	// the input columns of each branch, and stops at the union's children
	// because they are leaf patterns extracted from the memo.
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(mp, pexpr);
	GPOS_ASSERT(COperator::EopLogicalUnionAll ==
					pexprNormalized->Pop()->Eopid() ||
				COperator::EopLogicalSelect == pexprNormalized->Pop()->Eopid());

	pxfres->Add(pexprNormalized);
}

// EOF
