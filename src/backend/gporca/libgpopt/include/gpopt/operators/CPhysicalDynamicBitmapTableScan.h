//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
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
#include "gpos/common/CBitSet.h"

#include "gpopt/operators/CPhysicalDynamicScan.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CName;

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
	// positions in the partition list that belong to this operator; null
	// when the operator covers every partition of the list
	CBitSet *m_selected_parts;

public:
	CPhysicalDynamicBitmapTableScan(const CPhysicalDynamicBitmapTableScan &) =
		delete;

	// ctor
	CPhysicalDynamicBitmapTableScan(
		CMemoryPool *mp, CTableDescriptor *ptabdesc, ULONG ulOriginOpId,
		const CName *pnameAlias, ULONG scan_id, CColRefArray *pdrgpcrOutput,
		CColRef2dArray *pdrgpdrgpcrParts, IMdIdArray *partition_mdids,
		CBitSet *selected_parts,
		ColRefToUlongMapArray *root_col_mapping_per_part);

	// dtor
	~CPhysicalDynamicBitmapTableScan() override;

	CBitSet *
	GetSelectedParts() const
	{
		return m_selected_parts;
	}

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalDynamicBitmapTableScan;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalDynamicBitmapTableScan";
	}

	// match function
	BOOL Matches(COperator *) const override;

	// statistics derivation during costing
	IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  CReqdPropPlan *prpplan,
							  IStatisticsArray *stats_ctxt) const override;

	// conversion function
	static CPhysicalDynamicBitmapTableScan *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopPhysicalDynamicBitmapTableScan == pop->Eopid());

		return dynamic_cast<CPhysicalDynamicBitmapTableScan *>(pop);
	}
};
}  // namespace gpopt

#endif	// !GPOPT_CPhysicalDynamicBitmapTableScan_H

// EOF
