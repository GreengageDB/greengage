// Greengage Database
// Copyright (C) 2012 EMC Corp.
//
// Assert operator for runtime checking of constraints. Assert operators have a list
// of constraints to be checked, and corresponding error messages to print in the
// event of constraint violation.
//
// For example:
// clang-format off
//
// +--CPhysicalAssert (Error code: 23514)   
//    |--CPhysicalAssert (Error code: 23502)  
//    |  |--CPhysical [...]
//    |  +--CScalarAssertConstraintList
//    |     +--CScalarAssertConstraint (ErrorMsg: Not null constraint for column b of table r violated)
//    |        +--CScalarBoolOp (EboolopNot)
//    |           +--CScalarNullTest
//    |              +--CScalarIdent "b" (2) 
//    +--CScalarAssertConstraintList
//       |--CScalarAssertConstraint (ErrorMsg: Check constraint r_check for table r violated)
//       |  +--CScalarIsDistinctFrom (=)
//       |     |--CScalarCmp (<)
//       |     |  |--CScalarIdent "d" (4)
//       |     |  +--CScalarIdent "c" (3)
//       |     +--CScalarConst (0)
//       +--CScalarAssertConstraint (ErrorMsg: Check constraint r_c_check for table r violated)
//          +--CScalarIsDistinctFrom (=)
//             |--CScalarCmp (>)
//             |  |--CScalarIdent "c" (3) 
//             |  +--CScalarConst (0)
//             +--CScalarConst (0)
// clang-format on
#ifndef GPOPT_CPhysicalAssert_H
#define GPOPT_CPhysicalAssert_H

#include "gpos/base.h"

#include "naucrates/dxl/errorcodes.h"

#include "gpopt/operators/CPhysical.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalAssert
//
//	@doc:
//		Assert operator
//
//---------------------------------------------------------------------------
class CPhysicalAssert : public CPhysical
{
private:
	// exception
	CException *m_pexc;


	// private copy ctor
	CPhysicalAssert(const CPhysicalAssert &);

public:
	// ctor
	CPhysicalAssert(CMemoryPool *mp, CException *pexc);

	// dtor
	virtual ~CPhysicalAssert();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalAssert;
	}

	// operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalAssert";
	}

	// exception
	CException *
	Pexc() const
	{
		return m_pexc;
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return true;
	}

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	virtual CColRefSet *PcrsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl, CColRefSet *pcrsRequired,
		ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	// compute required ctes of the n-th child
	virtual CCTEReq *PcteRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								  CCTEReq *pcter, ULONG child_index,
								  CDrvdPropArray *pdrgpdpCtxt,
								  ULONG ulOptReq) const;

	// compute required sort order of the n-th child
	virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									COrderSpec *posRequired, ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const;

	// compute required distribution of the n-th child
	virtual CDistributionSpec *PdsRequired(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *pdsRequired,
										   ULONG child_index,
										   CDrvdPropArray *pdrgpdpCtxt,
										   ULONG ulOptReq) const;

	// compute required rewindability of the n-th child
	virtual CRewindabilitySpec *PrsRequired(CMemoryPool *mp,
											CExpressionHandle &exprhdl,
											CRewindabilitySpec *prsRequired,
											ULONG child_index,
											CDrvdPropArray *pdrgpdpCtxt,
											ULONG ulOptReq) const;

	// compute required partition propagation of the n-th child
	virtual CPartitionPropagationSpec *PppsRequired(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	// check if required columns are included in output columns
	virtual BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired,
								   ULONG ulOptReq) const;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	virtual COrderSpec *PosDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const;

	// derive distribution
	virtual CDistributionSpec *PdsDerive(CMemoryPool *mp,
										 CExpressionHandle &exprhdl) const;

	// derive rewindability
	virtual CRewindabilitySpec *PrsDerive(CMemoryPool *mp,
										  CExpressionHandle &exprhdl) const;

	// derive partition index map
	virtual CPartIndexMap *
	PpimDerive(CMemoryPool *,  // mp
			   CExpressionHandle &exprhdl,
			   CDrvdPropCtxt *	//pdpctxt
	) const
	{
		return PpimPassThruOuter(exprhdl);
	}

	// derive partition filter map
	virtual CPartFilterMap *
	PpfmDerive(CMemoryPool *,  // mp
			   CExpressionHandle &exprhdl) const
	{
		return PpfmPassThruOuter(exprhdl);
	}

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

	// return rewindability property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetRewindability(
		CExpressionHandle &exprhdl, const CEnfdRewindability *per) const;

	// return true if operator passes through stats obtained from children,
	// this is used when computing stats during costing
	virtual BOOL
	FPassThruStats() const
	{
		return true;
	}

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CPhysicalAssert *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalAssert == pop->Eopid());

		return reinterpret_cast<CPhysicalAssert *>(pop);
	}

	// debug print
	IOstream &OsPrint(IOstream &os) const;

};	// class CPhysicalAssert

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalAssert_H

// EOF
