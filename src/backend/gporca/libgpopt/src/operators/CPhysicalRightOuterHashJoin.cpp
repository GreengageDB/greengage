//---------------------------------------------------------------------------
//	Greenplum Database
//  Copyright (c) 2020 VMware, Inc.
//
//	@filename:
//		CPhysicalRightOuterHashJoin.cpp
//
//	@doc:
//		Implementation of right outer hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalRightOuterHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightOuterHashJoin::CPhysicalRightOuterHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalRightOuterHashJoin::CPhysicalRightOuterHashJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys, IMdIdArray *hash_opfamilies,
	BOOL is_null_aware, CXform::EXformId origin_xform)
	: CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys,
						hash_opfamilies, is_null_aware, origin_xform)
{
	ULONG ulDistrReqs = 1 + NumDistrReq();
	SetDistrRequests(ulDistrReqs);
	SetPartPropagateRequests(2);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightOuterHashJoin::~CPhysicalRightOuterHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalRightOuterHashJoin::~CPhysicalRightOuterHashJoin() = default;

CDistributionSpec *
CPhysicalRightOuterHashJoin::PdsDerive(CMemoryPool *mp,
									   CExpressionHandle &exprhdl) const
{
	return PdsDeriveForOuterJoin(mp, exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalRightOuterHashJoin::Ped
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CEnfdDistribution *
CPhysicalRightOuterHashJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CReqdPropPlan *prppInput, ULONG child_index,
								 CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	return PedRightOrFullJoin(mp, exprhdl, prppInput, child_index, pdrgpdpCtxt,
							  ulOptReq);
}

void
CPhysicalRightOuterHashJoin::CreateOptRequests(CMemoryPool *mp)
{
	CreateHashRedistributeRequests(mp);

	// given an optimization context, Right Hash Join creates 2 optimization requests
	// to enforce distribution of its children:
	// Req(1 to N) (redistribute, redistribute), where we request the first hash join child
	//              to be distributed on single hash join keys separately, as well as the set
	//              of all hash join keys, the second hash join child is always required to
	// 				match the distribution returned by first child
	// Req(N + 1) (singleton, singleton)
	ULONG ulDistrReqs = 1 + NumDistrReq();
	SetDistrRequests(ulDistrReqs);

	SetPartPropagateRequests(2);
}

CPartitionPropagationSpec *
CPhysicalRightOuterHashJoin::PppsRequired(
	CMemoryPool *mp, CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired, ULONG child_index,
	CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const
{
	return PppsRequiredForJoins(mp, exprhdl, pppsRequired, child_index,
								pdrgpdpCtxt, ulOptReq);
}

// In the following function, we are generating the Derived property :
// "Partition Propagation Spec" of Right Outer Hash join.
CPartitionPropagationSpec *
CPhysicalRightOuterHashJoin::PppsDerive(CMemoryPool *mp,
										CExpressionHandle &exprhdl) const
{
	return PppsDeriveForJoins(mp, exprhdl);
}
// EOF
