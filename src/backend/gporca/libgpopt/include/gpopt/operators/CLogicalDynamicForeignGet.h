//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2023 VMware Inc.
//
//	@filename:
//		CLogicalDynamicForeignGet.h
//
//	@doc:
//  	Logical foreign get operator for partitioned table with only foreign tables
//      with the same foreign server
//---------------------------------------------------------------------------
#ifndef GPOPT_CLogicalDynamicForeignGet_H
#define GPOPT_CLogicalDynamicForeignGet_H

#include "gpos/base.h"

#include "gpopt/operators/CLogicalDynamicGetBase.h"

namespace gpopt
{
// fwd declarations
class CTableDescriptor;
class CName;
class CColRefSet;

class CLogicalDynamicForeignGet : public CLogicalDynamicGetBase
{
private:
	OID m_foreign_server_oid;

	IMDRelation::Ereldistrpolicy m_exec_location;

	CBitSet *m_selected_parts{nullptr};

	// Disjunction of selected child partition's constraints after static pruning
	CConstraint *m_partition_cnstrs_disj{nullptr};

	// Has done static pruning
	BOOL m_static_pruned{false};

public:
	CLogicalDynamicForeignGet(const CLogicalDynamicForeignGet &) = delete;

	// ctors
	explicit CLogicalDynamicForeignGet(CMemoryPool *mp);

	CLogicalDynamicForeignGet(CMemoryPool *mp, const CName *pnameAlias,
							  CTableDescriptor *ptabdesc, ULONG ulPartIndex,
							  CColRefArray *pdrgpcrOutput,
							  CColRef2dArray *pdrgpdrgpcrPart,
							  IMdIdArray *partition_mdids,
							  CConstraint *partition_cnstrs_disj,
							  BOOL static_pruned,
							  CBitSet *selected_parts, OID foreign_server_oid,
							  IMDRelation::Ereldistrpolicy exec_location);

	~CLogicalDynamicForeignGet() override;

	// ident accessors

	EOperatorId
	Eopid() const override
	{
		return EopLogicalDynamicForeignGet;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CLogicalDynamicForeignGet";
	}

	// match function
	BOOL Matches(COperator *pop) const override;

	// return a copy of the operator with remapped columns
	COperator *PopCopyWithRemappedColumns(CMemoryPool *mp,
										  UlongToColRefMap *colref_mapping,
										  BOOL must_exist) override;

	OID
	GetForeignServerOid() const
	{
		return m_foreign_server_oid;
	}

	// return disjunctive constraint of selected partitions
	CConstraint *
	GetPartitionConstraintsDisj() const
	{
		return m_partition_cnstrs_disj;
	}

	// return whether static pruning is performed
	BOOL
	FStaticPruned() const
	{
		return m_static_pruned;
	}

	IMDRelation::Ereldistrpolicy
	DistributionType() const
	{
		return m_exec_location;
	}

	CBitSet *
	GetSelectedParts() const
	{
		return m_selected_parts;
	}

	//-------------------------------------------------------------------------------------
	// Required Relational Properties
	//-------------------------------------------------------------------------------------

	// compute required stat columns of the n-th child
	CColRefSet *
	PcrsStat(CMemoryPool *,		   // mp,
			 CExpressionHandle &,  // exprhdl
			 CColRefSet *,		   // pcrsInput
			 ULONG				   // child_index
	) const override
	{
		GPOS_ASSERT(!"CLogicalDynamicForeignGet has no children");
		return nullptr;
	}

	// sensitivity to order of inputs
	BOOL
	FInputOrderSensitive() const override
	{
		GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
		return false;
	}

	// derive statistics
	IStatistics *PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  IStatisticsArray *stats_ctxt) const override;

	// derive stats from base table using filters on partition and/or index columns
	IStatistics *PstatsDeriveFilter(CMemoryPool *mp, CExpressionHandle &exprhdl,
									CExpression *pexprFilter) const;

	// stat promise
	EStatPromise
	Esp(CExpressionHandle &) const override
	{
		return CLogical::EspHigh;
	}

	//-------------------------------------------------------------------------------------
	// Transformations
	//-------------------------------------------------------------------------------------

	// candidate set of xforms
	CXformSet *PxfsCandidates(CMemoryPool *mp) const override;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// conversion function
	static CLogicalDynamicForeignGet *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(nullptr != pop);
		GPOS_ASSERT(EopLogicalDynamicForeignGet == pop->Eopid());

		return dynamic_cast<CLogicalDynamicForeignGet *>(pop);
	}

};	// class CLogicalDynamicForeignGet
}  // namespace gpopt

#endif	// !GPOPT_CLogicalDynamicForeignGet_H

// EOF
