//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CPhysicalDynamicBitmapTableScan.h
//
//	@doc:
//		Dynamic bitmap table scan physical operator
//
//	@owner:
//
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CPhysicalDynamicBitmapTableScan_H
#define GPOPT_CPhysicalDynamicBitmapTableScan_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalDynamicScan.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CName;
class CPartConstraint;

//---------------------------------------------------------------------------
//	@class:
//		CPhysicalDynamicBitmapTableScan
//
//	@doc:
//		Dynamic bitmap table scan physical operator
//
//---------------------------------------------------------------------------
class CPhysicalDynamicBitmapTableScan : public CPhysicalDynamicScan
{
private:
	// disable copy ctor
	CPhysicalDynamicBitmapTableScan(const CPhysicalDynamicBitmapTableScan &);

public:
	// ctor
	CPhysicalDynamicBitmapTableScan(CMemoryPool *mp, BOOL is_partial,
									CTableDescriptor *ptabdesc,
									ULONG ulOriginOpId, const CName *pnameAlias,
									ULONG scan_id, CColRefArray *pdrgpcrOutput,
									CColRef2dArray *pdrgpdrgpcrParts,
									ULONG ulSecondaryScanId,
									CPartConstraint *ppartcnstr,
									CPartConstraint *ppartcnstrRel);

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalDynamicBitmapTableScan;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalDynamicBitmapTableScan";
	}

	// match function
	virtual BOOL Matches(COperator *) const;

	// statistics derivation during costing
	virtual IStatistics *PstatsDerive(CMemoryPool *mp,
									  CExpressionHandle &exprhdl,
									  CReqdPropPlan *prpplan,
									  IStatisticsArray *stats_ctxt) const;

	// conversion function
	static CPhysicalDynamicBitmapTableScan *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalDynamicBitmapTableScan == pop->Eopid());

		return dynamic_cast<CPhysicalDynamicBitmapTableScan *>(pop);
	}
};
}  // namespace gpopt

#endif	// !GPOPT_CPhysicalDynamicBitmapTableScan_H

// EOF
