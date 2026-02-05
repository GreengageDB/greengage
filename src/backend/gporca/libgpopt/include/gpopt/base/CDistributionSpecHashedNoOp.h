//	Greengage Database
//	Copyright (C) 2016 VMware, Inc. or its affiliates.


#ifndef GPOPT_CDistributionSpecHashedNoOp_H
#define GPOPT_CDistributionSpecHashedNoOp_H

#include "gpopt/base/CDistributionSpecHashed.h"

namespace gpopt
{
class CDistributionSpecHashedNoOp : public CDistributionSpecHashed
{
public:
	// explicitly pass opfamilies or NULL, since the default ones are not
	// populated by parent ctor for NoOp cases.
	CDistributionSpecHashedNoOp(CExpressionArray *pdrgpexr,
								IMdIdArray *opfamilies);

	EDistributionType Edt() const override;

	BOOL Matches(const CDistributionSpec *pds) const override;

	const CHAR *
	SzId() const override
	{
		return "HASHED NO-OP";
	}

	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;
};
}  // namespace gpopt

#endif
