//	Greengage Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/base/CDistributionSpecHashedNoOp.h"

#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotionHashDistribute.h"

using namespace gpopt;

CDistributionSpecHashedNoOp::CDistributionSpecHashedNoOp(
	CExpressionArray *pdrgpexpr, IMdIdArray *opfamilies)
	: CDistributionSpecHashed(pdrgpexpr, true, opfamilies, true)
{
}

CDistributionSpec::EDistributionType
CDistributionSpecHashedNoOp::Edt() const
{
	return CDistributionSpec::EdtHashedNoOp;
}

BOOL
CDistributionSpecHashedNoOp::Matches(const CDistributionSpec *pds) const
{
	return pds->Edt() == Edt();
}

void
CDistributionSpecHashedNoOp::AppendEnforcers(CMemoryPool *mp,
											 CExpressionHandle &exprhdl,
											 CReqdPropPlan *,
											 CExpressionArray *pdrgpexpr,
											 CExpression *pexpr)
{
	CDrvdProp *pdp = exprhdl.Pdp();
	CDistributionSpec *pdsChild = CDrvdPropPlan::Pdpplan(pdp)->Pds();
	CDistributionSpecHashed *pdsChildHashed =
		dynamic_cast<CDistributionSpecHashed *>(pdsChild);

	if (NULL == pdsChildHashed)
	{
		return;
	}

	CExpressionArray *pdrgpexprNoOpRedistributionColumns =
		pdsChildHashed->Pdrgpexpr();
	pdrgpexprNoOpRedistributionColumns->AddRef();

	IMdIdArray *opfamilies = pdsChildHashed->Opfamilies();

	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		if (NULL == opfamilies)
			GPOS_RAISE(
				gpopt::ExmaGPOPT, gpdxl::ExmiUnexpectedOp,
				GPOS_WSZ_LIT(": opfamily must exist for each hash expr"));
		opfamilies->AddRef();
	}

	CDistributionSpecHashedNoOp *pdsNoOp =
		GPOS_NEW(mp) CDistributionSpecHashedNoOp(
			pdrgpexprNoOpRedistributionColumns, opfamilies);
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CPhysicalMotionHashDistribute(mp, pdsNoOp), pexpr);
	pdrgpexpr->Append(pexprMotion);
}
