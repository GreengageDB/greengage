//	Greengage Database
//	Copyright (C) 2016 Pivotal Software, Inc.


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

	virtual EDistributionType Edt() const;

	virtual BOOL Matches(const CDistributionSpec *pds) const;

	virtual const CHAR *
	SzId() const
	{
		return "HASHED NO-OP";
	}

	virtual void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CReqdPropPlan *prpp,
								 CExpressionArray *pdrgpexpr,
								 CExpression *pexpr);
};
}  // namespace gpopt

#endif
