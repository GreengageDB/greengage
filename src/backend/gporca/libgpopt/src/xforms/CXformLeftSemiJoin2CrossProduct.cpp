//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformLeftSemiJoin2CrossProduct.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformLeftSemiJoin2CrossProduct.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalInnerJoin.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"
#include "gpopt/operators/CLogicalLimit.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPatternLeaf.h"
#include "gpopt/operators/CPatternTree.h"
#include "gpopt/xforms/CXformUtils.h"



using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2CrossProduct::CXformLeftSemiJoin2CrossProduct
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CXformLeftSemiJoin2CrossProduct::CXformLeftSemiJoin2CrossProduct(
	CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalLeftSemiJoin(mp),
		  GPOS_NEW(mp) CExpression(
			  mp,
			  GPOS_NEW(mp) CPatternTree(
				  mp)),	 // left child is a tree since we may need to push predicates down
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp)),  // right child
		  GPOS_NEW(mp) CExpression(
			  mp,
			  GPOS_NEW(mp) CPatternTree(
				  mp))	// predicate is a tree since we may need to do clean-up of scalar expression
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2CrossProduct::Exfp
//
//	@doc:
//		 Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformLeftSemiJoin2CrossProduct::Exfp(CExpressionHandle &exprhdl) const
{
	return CXformUtils::ExfpSemiJoin2CrossProduct(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformLeftSemiJoin2CrossProduct::Transform
//
//	@doc:
//		Semi join whose join predicate does not use columns from join's inner
//		child is equivalent to a Cross Product between outer child (after
//		pushing join predicate), and one tuple from inner child
//
//---------------------------------------------------------------------------
void
CXformLeftSemiJoin2CrossProduct::Transform(CXformContext *pxfctxt,
										   CXformResult *pxfres,
										   CExpression *pexpr) const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];
	pexprOuter->AddRef();
	pexprInner->AddRef();
	pexprScalar->AddRef();

	// create a (limit 1) on top of inner child
	CExpression *pexprLimitOffset = CUtils::PexprScalarConstInt8(mp, 0 /*val*/);
	CExpression *pexprLimitCount = CUtils::PexprScalarConstInt8(mp, 1 /*val*/);
	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	CLogicalLimit *popLimit = GPOS_NEW(mp)
		CLogicalLimit(mp, pos, true /*fGlobal*/, true /*fHasCount*/,
					  false /*fNonRemovableLimit*/);
	CExpression *pexprLimit = GPOS_NEW(mp) CExpression(
		mp, popLimit, pexprInner, pexprLimitOffset, pexprLimitCount);

	// create cross product
	CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(
		mp, pexprOuter, pexprLimit, pexprScalar);
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(mp, pexprJoin);
	pexprJoin->Release();

	pxfres->Add(pexprNormalized);
}


// EOF
