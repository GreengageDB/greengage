//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSelect2DynamicIndexGet.cpp
//
//	@doc:
//		Implementation of select over a partitioned table to a dynamic index get
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformSelect2DynamicIndexGet.h"

#include "gpos/base.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalDynamicGet.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/IMDPartConstraint.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicIndexGet::CXformSelect2DynamicIndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2DynamicIndexGet::CXformSelect2DynamicIndexGet(CMemoryPool *mp)
	:  // pattern
	  CXformExploration(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalSelect(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CLogicalDynamicGet(mp)),	 // relational child
		  GPOS_NEW(mp)
			  CExpression(mp, GPOS_NEW(mp) CPatternTree(mp))  // predicate tree
		  ))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicIndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2DynamicIndexGet::Exfp(CExpressionHandle &exprhdl) const
{
	if (exprhdl.DeriveHasSubquery(1))
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicIndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2DynamicIndexGet::Transform(CXformContext *pxfctxt,
										CXformResult *pxfres,
										CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CLogicalDynamicGet *popGet =
		CLogicalDynamicGet::PopConvert((*pexpr)[0]->Pop());
	// Do not run if contains foreign partitions, instead run CXformExpandDynamicGetWithForeignPartitions
	if (popGet->ContainsForeignParts())
	{
		return;
	}

	// We need to early exit when the relation contains security quals
	// because we are adding the security quals when translating from DXL to
	// Planned Statement as a filter. If we don't early exit then it may happen
	// that we generate a plan where the index condition contains non-leakproof
	// expressions. This can lead to data leak as we always want our security
	// quals to be executed first.
	if (popGet->HasSecurityQuals())
	{
		return;
	}

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// get the indexes on this relation
	CLogicalDynamicGet *popDynamicGet =
		CLogicalDynamicGet::PopConvert(pexprRelational->Pop());
	const ULONG ulIndices = popDynamicGet->Ptabdesc()->IndexCount();
	if (0 == ulIndices)
	{
		return;
	}

	// array of expressions in the scalar expression
	CExpressionArray *pdrgpexpr =
		CPredicateUtils::PdrgpexprConjuncts(mp, pexprScalar);
	GPOS_ASSERT(0 < pdrgpexpr->Size());

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsScalarExpr = pexprScalar->DeriveUsedColumns();

	// find the indexes whose included columns meet the required columns
	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel =
		md_accessor->RetrieveRel(popDynamicGet->Ptabdesc()->MDId());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->IndexMDidAt(ul);
		const IMDIndex *pmdindex = md_accessor->RetrieveIndex(pmdidIndex);
		// We consider ForwardScan here because, because ORCA currently doesn't
		// support BackwardScan on partition tables. Moreover, BackwardScan is
		// only supported in the case where we have Order by clause in the
		// query, but this xform handles scenario of a filter on top of a
		// partitioned table.
		CExpression *pexprDynamicIndexGet =
			CXformUtils::PexprBuildBtreeIndexPlan(
				mp, md_accessor, pexprRelational, pexpr->Pop()->UlOpId(),
				pdrgpexpr, pcrsScalarExpr, nullptr /*outer_refs*/, pmdindex,
				pmdrel, EForwardScan /*indexScanDirection*/,
				false /*indexForOrderBy*/, false /*indexonly*/);
		if (nullptr != pexprDynamicIndexGet)
		{
			// create a redundant SELECT on top of DynamicIndexGet to be able to use predicate in partition elimination

			CExpression *pexprRedundantSelect =
				CXformUtils::PexprRedundantSelectForDynamicIndex(
					mp, pexprDynamicIndexGet);
			pexprDynamicIndexGet->Release();
			pxfres->Add(pexprRedundantSelect);
		}
	}

	pdrgpexpr->Release();
}

// EOF
