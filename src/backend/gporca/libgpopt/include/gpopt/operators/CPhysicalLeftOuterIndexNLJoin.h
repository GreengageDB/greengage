//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Left outer index nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalLeftOuterIndexNLJoin_H
#define GPOPT_CPhysicalLeftOuterIndexNLJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalLeftOuterNLJoin.h"

namespace gpopt
{
class CPhysicalLeftOuterIndexNLJoin : public CPhysicalLeftOuterNLJoin
{
private:
	// columns from outer child used for index lookup in inner child
	CColRefArray *m_pdrgpcrOuterRefs;

	// a copy of the original join predicate that has been pushed down to the inner side
	CExpression *m_origJoinPred;

	// private copy ctor
	CPhysicalLeftOuterIndexNLJoin(const CPhysicalLeftOuterIndexNLJoin &);

public:
	// ctor
	CPhysicalLeftOuterIndexNLJoin(CMemoryPool *mp, CColRefArray *colref_array,
								  CExpression *origJoinPred);

	// dtor
	virtual ~CPhysicalLeftOuterIndexNLJoin();

	CEnfdProp::EPropEnforcingType EpetDistribution(
		CExpressionHandle &exprhdl, const CEnfdDistribution *ped) const;

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalLeftOuterIndexNLJoin;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalLeftOuterIndexNLJoin";
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// outer column references accessor
	CColRefArray *
	PdrgPcrOuterRefs() const
	{
		return m_pdrgpcrOuterRefs;
	}

	// compute required distribution of the n-th child
	virtual CDistributionSpec *PdsRequired(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *pdsRequired,
										   ULONG child_index,
										   CDrvdPropArray *pdrgpdpCtxt,
										   ULONG ulOptReq) const;

	virtual CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CReqdPropPlan *prppInput, ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt,
								   ULONG ulDistrReq);

	// execution order of children
	virtual EChildExecOrder
	Eceo() const
	{
		// we optimize inner (right) child first to be able to match child hashed distributions
		return EceoRightToLeft;
	}

	// conversion function
	static CPhysicalLeftOuterIndexNLJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalLeftOuterIndexNLJoin == pop->Eopid());

		return dynamic_cast<CPhysicalLeftOuterIndexNLJoin *>(pop);
	}

	CExpression *
	OrigJoinPred()
	{
		return m_origJoinPred;
	}

};	// class CPhysicalLeftOuterIndexNLJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalLeftOuterIndexNLJoin_H

// EOF
