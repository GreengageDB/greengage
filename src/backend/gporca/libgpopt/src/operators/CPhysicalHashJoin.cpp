//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalHashJoin.cpp
//
//	@doc:
//		Implementation of hash join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalHashJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonReplicated.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarIdent.h"

using namespace gpopt;

// number of non-redistribute requests created by hash join
#define GPOPT_NON_HASH_DIST_REQUESTS 3

// maximum number of redistribute requests on single hash join keys
#define GPOPT_MAX_HASH_DIST_REQUESTS 6

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CPhysicalHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalHashJoin::CPhysicalHashJoin(CMemoryPool *mp,
									 CExpressionArray *pdrgpexprOuterKeys,
									 CExpressionArray *pdrgpexprInnerKeys,
									 IMdIdArray *hash_opfamilies,
									 BOOL is_null_aware,
									 CXform::EXformId origin_xform)
	: CPhysicalJoin(mp, origin_xform),
	  m_pdrgpexprOuterKeys(pdrgpexprOuterKeys),
	  m_pdrgpexprInnerKeys(pdrgpexprInnerKeys),
	  m_hash_opfamilies(nullptr),
	  m_is_null_aware(is_null_aware),
	  m_pdrgpdsRedistributeRequests(nullptr)
{
	GPOS_ASSERT(nullptr != mp);
	GPOS_ASSERT(nullptr != pdrgpexprOuterKeys);
	GPOS_ASSERT(nullptr != pdrgpexprInnerKeys);
	GPOS_ASSERT(pdrgpexprOuterKeys->Size() == pdrgpexprInnerKeys->Size());

	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		GPOS_ASSERT(nullptr != hash_opfamilies);
		m_hash_opfamilies = hash_opfamilies;
		GPOS_ASSERT(pdrgpexprOuterKeys->Size() == m_hash_opfamilies->Size());
	}

	CreateOptRequests(mp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::~CPhysicalHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalHashJoin::~CPhysicalHashJoin()
{
	m_pdrgpexprOuterKeys->Release();
	m_pdrgpexprInnerKeys->Release();
	CRefCount::SafeRelease(m_hash_opfamilies);
	CRefCount::SafeRelease(m_pdrgpdsRedistributeRequests);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::CreateHashRedistributeRequests
//
//	@doc:
//		Create the set of redistribute requests to send to first
//		hash join child
//
//---------------------------------------------------------------------------
void
CPhysicalHashJoin::CreateHashRedistributeRequests(CMemoryPool *mp)
{
	GPOS_ASSERT(nullptr == m_pdrgpdsRedistributeRequests);
	GPOS_ASSERT(nullptr != m_pdrgpexprOuterKeys);
	GPOS_ASSERT(nullptr != m_pdrgpexprInnerKeys);

	CExpressionArray *pdrgpexpr = nullptr;
	if (EceoRightToLeft == Eceo())
	{
		pdrgpexpr = m_pdrgpexprInnerKeys;
	}
	else
	{
		pdrgpexpr = m_pdrgpexprOuterKeys;
	}

	m_pdrgpdsRedistributeRequests = GPOS_NEW(mp) CDistributionSpecArray(mp);
	const ULONG ulExprs =
		std::min((ULONG) GPOPT_MAX_HASH_DIST_REQUESTS, pdrgpexpr->Size());
	if (1 < ulExprs)
	{
		for (ULONG ul = 0; ul < ulExprs; ul++)
		{
			CExpressionArray *pdrgpexprCurrent =
				GPOS_NEW(mp) CExpressionArray(mp);
			CExpression *pexpr = (*pdrgpexpr)[ul];
			pexpr->AddRef();
			pdrgpexprCurrent->Append(pexpr);

			IMdIdArray *opfamilies = nullptr;
			if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
			{
				GPOS_ASSERT(nullptr != m_hash_opfamilies);
				opfamilies = GPOS_NEW(mp) IMdIdArray(mp);
				IMDId *opfamily = (*m_hash_opfamilies)[ul];
				opfamily->AddRef();
				opfamilies->Append(opfamily);
			}

			// add a separate request for each hash join key

			CDistributionSpecHashed *pdshashedCurrent =
				GPOS_NEW(mp) CDistributionSpecHashed(
					pdrgpexprCurrent, true /* fNullsCollocated */, opfamilies);
			m_pdrgpdsRedistributeRequests->Append(pdshashedCurrent);
		}
	}
	// add a request that contains all hash join keys
	pdrgpexpr->AddRef();
	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		GPOS_ASSERT(nullptr != m_hash_opfamilies);
		m_hash_opfamilies->AddRef();
	}
	CDistributionSpecHashed *pdshashed = GPOS_NEW(mp) CDistributionSpecHashed(
		pdrgpexpr, true /* fNullsCollocated */, m_hash_opfamilies);
	m_pdrgpdsRedistributeRequests->Append(pdshashed);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalHashJoin::PosRequired(CMemoryPool *mp,
							   CExpressionHandle &,	 //exprhdl
							   COrderSpec *,		 // posInput,
							   ULONG
#ifdef GPOS_DEBUG
								   child_index
#endif	// GPOS_DEBUG
							   ,
							   CDrvdPropArray *,  // pdrgpdpCtxt
							   ULONG			  // ulOptReq
) const
{
	GPOS_ASSERT(
		child_index < 2 &&
		"Required sort order can be computed on the relational child only");

	// hash join does not have order requirements to both children, and it
	// does not preserve any sort order
	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalHashJoin::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							   CRewindabilitySpec *prsRequired,
							   ULONG child_index,
							   CDrvdPropArray *,  // pdrgpdpCtxt
							   ULONG			  // ulOptReq
) const
{
	GPOS_ASSERT(
		child_index < 2 &&
		"Required rewindability can be computed on the relational child only");

	if (1 == child_index)
	{
		// If the inner child contains outer references, and the required
		// rewindability is not ErtNone, we must ensure that the inner subtree is
		// at least rescannable, even though a Hash op on the inner side
		// materialized the subtree results.
		if (exprhdl.HasOuterRefs(1) &&
			(prsRequired->Ert() == CRewindabilitySpec::ErtRescannable ||
			 prsRequired->Ert() == CRewindabilitySpec::ErtRewindable))
		{
			return GPOS_NEW(mp) CRewindabilitySpec(
				CRewindabilitySpec::ErtRescannable, prsRequired->Emht());
		}
		// Otherwise, the inner Hash op will take care of materializing the
		// subtree, so no rewindability type is required
		else
		{
			return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
												   prsRequired->Emht());
		}
	}

	// pass through requirements to outer child
	return PrsPassThru(mp, exprhdl, prsRequired, 0 /*child_index*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsMatch
//
//	@doc:
//		Compute a distribution matching the distribution delivered by
//		given child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsMatch(CMemoryPool *mp, CDistributionSpec *pds,
							ULONG ulSourceChildIndex) const
{
	GPOS_ASSERT(nullptr != pds);

	EChildExecOrder eceo = Eceo();

	// check the type of distribution delivered by first (inner) child
	switch (pds->Edt())
	{
		case CDistributionSpec::EdtUniversal:
			// One child is universal
			//
			// If the outer child is universal (join outputs all tuples from the universal
			// side with or without match in the other child), request the other child to
			// be a singleton. This way the join occurs on one segment or the coordinator,
			// eliminating the duplicate risk. This can happen in outer, anti semi and
			// full joins. Note, full join has two outer children (both left and right).
			//
			// If the inner child is universal (join only outputs tuples from the universal
			// side in case of a match in the other child), request the other child to
			// be non duplicated, i.e. non-replicated. This is the case with inner and semi
			// joins.

			if ((EceoRightToLeft == eceo &&
				 EopPhysicalRightOuterHashJoin == this->Eopid()) ||
				(EceoLeftToRight == eceo &&
				 (EopPhysicalLeftOuterHashJoin == this->Eopid() ||
				  EopPhysicalLeftAntiSemiHashJoin == this->Eopid())) ||
				EopPhysicalFullHashJoin == this->Eopid())
			{
				return GPOS_NEW(mp) CDistributionSpecSingleton();
			}
			else
			{
				return GPOS_NEW(mp) CDistributionSpecNonReplicated();
			}

		case CDistributionSpec::EdtSingleton:
		case CDistributionSpec::EdtStrictSingleton:
			// require second child to provide a matching singleton distribution
			return PdssMatching(mp,
								CDistributionSpecSingleton::PdssConvert(pds));

		case CDistributionSpec::EdtHashed:
			// require second child to provide a matching hashed distribution
			return PdshashedMatching(mp,
									 CDistributionSpecHashed::PdsConvert(pds),
									 ulSourceChildIndex, true);

		default:
			GPOS_ASSERT(CDistributionSpec::EdtStrictReplicated == pds->Edt() ||
						CDistributionSpec::EdtTaintedReplicated == pds->Edt());

			// Full join has two outer children (full join outputs all the tuples of either
			// child with or without match in the other child), if one child is replicated,
			// we request the other child to be replicated as well.
			if (EopPhysicalFullHashJoin == this->Eopid())
			{
				return GPOS_NEW(mp) CDistributionSpecReplicated(
					CDistributionSpec::EdtStrictReplicated);
			}

			if (EceoRightToLeft == eceo)
			{
				GPOS_ASSERT(1 == ulSourceChildIndex);

				// inner child is replicated, for ROJ outer must be executed on a single (non-coordinator) segment to avoid duplicates
				if (this->Eopid() == EopPhysicalRightOuterHashJoin)
				{
					return GPOS_NEW(mp) CDistributionSpecSingleton(
						CDistributionSpecSingleton::EstSegment);
				}
				// inner child is replicated, request outer child to have non-singleton distribution
				return GPOS_NEW(mp) CDistributionSpecNonSingleton();
			}

			GPOS_ASSERT(0 == ulSourceChildIndex);

			// outer child is replicated, replicate inner child too in order to preserve correctness of semi-join
			return GPOS_NEW(mp) CDistributionSpecReplicated(
				CDistributionSpec::EdtStrictReplicated);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		FIdenticalExpression
//
//	@doc:
//		Check whether the expressions match based on column name, instead of
//		column identifier. This is to accommodate the case of self-joins where
//		column names match, but the column identifier may not.
//---------------------------------------------------------------------------
static BOOL
FIdenticalExpression(CExpression *left, CExpression *right)
{
	if (left->Pop()->Eopid() == COperator::EopScalarIdent &&
		right->Pop()->Eopid() == COperator::EopScalarIdent)
	{
		// skip colid check. just make sure that names are same.
		return CWStringConst::Equals(
			CScalarIdent::PopConvert(left->Pop())->Pcr()->Name().Pstr(),
			CScalarIdent::PopConvert(right->Pop())->Pcr()->Name().Pstr());
	}
	else if (!left->Pop()->Matches(right->Pop()) ||
			 left->Arity() != right->Arity())
	{
		return false;
	}
	else
	{
		for (ULONG ul = 0; ul < left->Arity(); ul++)
		{
			if (!FIdenticalExpression((*left)[ul], (*right)[ul]))
			{
				return false;
			}
		}
		return true;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		FIdenticalExpressionArrays
//
//	@doc:
//		Check whether the expressions in input arrays match, *not* including
//		colid.
//---------------------------------------------------------------------------
static BOOL
FIdenticalExpressionArrays(const CExpressionArray *outer,
						   const CExpressionArray *inner)
{
	GPOS_ASSERT(outer->Size() == inner->Size());

	for (ULONG ul = 0; ul < outer->Size(); ul++)
	{
		if (!FIdenticalExpression((*outer)[ul], (*inner)[ul]))
		{
			return false;
		}
	}
	return true;
}

BOOL
CPhysicalHashJoin::FSelfJoinWithMatchingJoinKeys(
	CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	// There may be duplicate mdids because the hash key is unique on a
	// combination of mdid and alias. Here we do not care about duplicate
	// aliases because joining the same table with different alias is still a
	// self-join.
	CTableDescriptorHashSet *outertabs = CUtils::RemoveDuplicateMdids(
		mp, exprhdl.DeriveTableDescriptor(0 /*child_index*/));
	CTableDescriptorHashSet *innertabs = CUtils::RemoveDuplicateMdids(
		mp, exprhdl.DeriveTableDescriptor(1 /*child_index*/));

	BOOL result = false;

	// Check that this is a self join. Size() of 1 means that there is 1 unique
	// table on each side of the join. Check whether it is the same table.
	if (outertabs->Size() == 1 && innertabs->Size() == 1 &&
		outertabs->First()->MDId()->Equals(innertabs->First()->MDId()))
	{
		// Check that the join keys are identical
		if (FIdenticalExpressionArrays(PdrgpexprInnerKeys(),
									   PdrgpexprOuterKeys()))
		{
			result = true;
		}
	}
	outertabs->Release();
	innertabs->Release();

	return result;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsDeriveFromHashedChildren
//
//	@doc:
//		Derive hash join distribution from hashed children;
//		return NULL if derivation failed
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsDeriveFromHashedChildren(
	CMemoryPool *mp, CExpressionHandle &exprhdl, CDistributionSpec *pdsOuter,
	CDistributionSpec *pdsInner) const
{
	GPOS_ASSERT(nullptr != pdsOuter);
	GPOS_ASSERT(nullptr != pdsInner);

	CDistributionSpecHashed *pdshashedOuter =
		CDistributionSpecHashed::PdsConvert(pdsOuter);
	CDistributionSpecHashed *pdshashedInner =
		CDistributionSpecHashed::PdsConvert(pdsInner);

	if (FSelfJoinWithMatchingJoinKeys(mp, exprhdl))
	{
		// A self join on distributed spec columns will preserve the colocated
		// nulls.
		CDistributionSpecHashed *combined_hashed_spec =
			pdshashedOuter->Combine(mp, pdshashedInner);
		return combined_hashed_spec;
	}

	if (pdshashedOuter->IsCoveredBy(PdrgpexprOuterKeys()) &&
		pdshashedInner->IsCoveredBy(PdrgpexprInnerKeys()))
	{
		// if both sides are hashed on subsets of hash join keys, join's output can be
		// seen as distributed on outer spec or (equivalently) on inner spec,
		// so create a new spec and mark outer and inner as equivalent

		CDistributionSpecHashed *pdshashedInnerCopy =
			pdshashedInner->Copy(mp, false);
		CDistributionSpecHashed *combined_hashed_spec =
			pdshashedOuter->Combine(mp, pdshashedInnerCopy);
		pdshashedInnerCopy->Release();
		return combined_hashed_spec;
	}

	return nullptr;
}

CDistributionSpec *
CPhysicalHashJoin::PdsDeriveForOuterJoin(CMemoryPool *mp,
										 CExpressionHandle &exprhdl) const
{
	GPOS_ASSERT(EopPhysicalLeftOuterHashJoin == Eopid() ||
				EopPhysicalRightOuterHashJoin == Eopid());

	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0 /*child_index*/)->Pds();
	CDistributionSpec *pdsInner = exprhdl.Pdpplan(1 /*child_index*/)->Pds();

	// We must use the non-nullable side for the distribution spec for outer joins.
	// For right join, the hash side is the non-nullable side, so we swap the inner/outer
	// distribution specs for the logic below
	if (exprhdl.Pop()->Eopid() == EopPhysicalRightOuterHashJoin)
	{
		pdsOuter = exprhdl.Pdpplan(1 /*child_index*/)->Pds();
		pdsInner = exprhdl.Pdpplan(0 /*child_index*/)->Pds();
	}

	if (CDistributionSpec::EdtHashed == pdsOuter->Edt() &&
		CDistributionSpec::EdtHashed == pdsInner->Edt())
	{
		CDistributionSpec *pdsDerived =
			PdsDeriveFromHashedChildren(mp, exprhdl, pdsOuter, pdsInner);
		if (nullptr != pdsDerived)
		{
			return pdsDerived;
		}
	}

	CDistributionSpec *pds;
	if (CDistributionSpec::EdtStrictReplicated == pdsOuter->Edt() ||
		CDistributionSpec::EdtUniversal == pdsOuter->Edt())
	{
		// if outer is replicated/universal, return inner distribution
		pds = pdsInner;
	}
	else
	{
		// otherwise, return outer distribution
		pds = pdsOuter;
	}

	if (CDistributionSpec::EdtHashed == pds->Edt())
	{
		CDistributionSpecHashed *pdsHashed =
			CDistributionSpecHashed::PdsConvert(pds);

		// Clean up any incomplete distribution specs since they can no longer be completed above
		// Note that, since this is done at the lowest join, no relevant equivalent specs are lost.
		if (!pdsHashed->HasCompleteEquivSpec(mp))
		{
			CExpressionArray *pdrgpexpr = pdsHashed->Pdrgpexpr();
			IMdIdArray *opfamilies = pdsHashed->Opfamilies();

			if (nullptr != opfamilies)
			{
				opfamilies->AddRef();
			}
			pdrgpexpr->AddRef();
			return GPOS_NEW(mp) CDistributionSpecHashed(
				pdrgpexpr, pdsHashed->FNullsColocated(), opfamilies);
		}
	}

	pds->AddRef();
	return pds;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdshashedMatching
//
//	@doc:
//		Compute a hashed distribution matching the given distribution
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalHashJoin::PdshashedMatching(
	CMemoryPool *mp, CDistributionSpecHashed *pdshashed,
	ULONG
		ulSourceChild,	// index of child that delivered the given hashed distribution
	// indicates whether function is called within the distribution request (true)
	// or within property derivation (false) from PdsDeriveFromHashedOuter/PdsDeriveFromReplicatedOuter
	BOOL isPdsReq) const
{
	GPOS_ASSERT(2 > ulSourceChild);

	CExpressionArray *pdrgpexprSource = m_pdrgpexprOuterKeys;
	CExpressionArray *pdrgpexprTarget = m_pdrgpexprInnerKeys;
	if (1 == ulSourceChild)
	{
		pdrgpexprSource = m_pdrgpexprInnerKeys;
		pdrgpexprTarget = m_pdrgpexprOuterKeys;
	}

	const CExpressionArray *pdrgpexprDist = pdshashed->Pdrgpexpr();
	const ULONG ulDlvrdSize = pdrgpexprDist->Size();
	const ULONG ulSourceSize = pdrgpexprSource->Size();

	// construct an array of target key expressions matching source key expressions
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArrays *all_equiv_exprs = pdshashed->HashSpecEquivExprs();
	IMdIdArray *opfamilies = nullptr;

	if (GPOS_FTRACE(EopttraceConsiderOpfamiliesForDistribution))
	{
		opfamilies = GPOS_NEW(mp) IMdIdArray(mp);
	}

	for (ULONG ulDlvrdIdx = 0; ulDlvrdIdx < ulDlvrdSize; ulDlvrdIdx++)
	{
		CExpression *pexprDlvrd = (*pdrgpexprDist)[ulDlvrdIdx];
		CExpressionArray *equiv_distribution_exprs = nullptr;
		if (nullptr != all_equiv_exprs && all_equiv_exprs->Size() > 0)
		{
			equiv_distribution_exprs = (*all_equiv_exprs)[ulDlvrdIdx];
		}
		for (ULONG idx = 0; idx < ulSourceSize; idx++)
		{
			BOOL fSuccess = false;
			CExpression *source_expr = (*pdrgpexprSource)[idx];
			fSuccess = CUtils::Equals(pexprDlvrd, source_expr);
			if (!fSuccess)
			{
				// if failed to find a equal match in the source distribution expr
				// array, check the equivalent exprs to find a match
				fSuccess =
					CUtils::Contains(equiv_distribution_exprs, source_expr);
			}
			if (fSuccess)
			{
				// TODO: 02/21/2012 - ; source column may be mapped to multiple
				// target columns (e.g. i=j and i=k);
				// in this case, we need to generate multiple optimization requests to the target child
				CExpression *pexprTarget = (*pdrgpexprTarget)[idx];
				pexprTarget->AddRef();
				pdrgpexpr->Append(pexprTarget);

				if (nullptr != opfamilies)
				{
					GPOS_ASSERT(nullptr != m_hash_opfamilies);
					IMDId *opfamily = (*m_hash_opfamilies)[idx];
					opfamily->AddRef();
					opfamilies->Append(opfamily);
				}
				break;
			}
		}
	}
	// As of now, we cannot set nulls colocation to false for inner joins, because
	// this logic is used by PdsDeriveFromHashedOuter and PdsDeriveFromReplicatedOuter,
	// where the property delivered by the inner relation is calculated based on the
	// property delivered by the outer relation in inner joins.
	//
	// For outer joins, this logic is only used for distribution requests, where we
	// can safely waive the request for nulls colocation, as far as the join condition
	// isn't null aware (not district from).
	BOOL fNullsColocated = true;

	if (!m_is_null_aware &&
		(COperator::EopPhysicalLeftOuterHashJoin == Eopid() ||
		 COperator::EopPhysicalRightOuterHashJoin == Eopid() ||
		 COperator::EopPhysicalFullHashJoin == Eopid()))
	{
		fNullsColocated = false;
	}

	// check if we failed to compute required distribution
	// We could fail to find enough key expressions matching the source
	// distribution, or we need the matching distribution have colocated nulls
	// but input distribution's nulls are not colocated (only for distribution requests).
	if (pdrgpexpr->Size() != ulDlvrdSize ||
		(isPdsReq && fNullsColocated && !pdshashed->FNullsColocated()))
	{
		pdrgpexpr->Release();
		if (nullptr != pdshashed->PdshashedEquiv())
		{
			CRefCount::SafeRelease(opfamilies);
			// try again using the equivalent hashed distribution
			return PdshashedMatching(mp, pdshashed->PdshashedEquiv(),
									 ulSourceChild, isPdsReq);
		}
		// it should never happen, but instead of creating wrong spec, raise an exception
		GPOS_RAISE(
			CException::ExmaInvalid, CException::ExmiInvalid,
			GPOS_WSZ_LIT("Unable to create matching hashed distribution."));
	}

	return GPOS_NEW(mp)
		CDistributionSpecHashed(pdrgpexpr, fNullsColocated, opfamilies);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequiredSingleton
//
//	@doc:
//		Create (singleton, singleton) optimization request
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequiredSingleton(CMemoryPool *mp,
										CExpressionHandle &,  // exprhdl
										CDistributionSpec *,  // pdsInput
										ULONG child_index,
										CDrvdPropArray *pdrgpdpCtxt) const
{
	if (FFirstChildToOptimize(child_index))
	{
		// require first child to be singleton
		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	// require a matching distribution from second child
	GPOS_ASSERT(nullptr != pdrgpdpCtxt);
	CDistributionSpec *pdsFirst =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(nullptr != pdsFirst);

	if (CDistributionSpec::EdtUniversal == pdsFirst->Edt() ||
		CDistributionSpec::EdtTaintedReplicated == pdsFirst->Edt())
	{
		// first child is universal, request second child to execute on a single host to avoid duplicates
		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	if (COptCtxt::PoctxtFromTLS()->OptimizeDMLQueryWithSingletonSegment() &&
		CDistributionSpec::EdtStrictReplicated == pdsFirst->Edt())
	{
		// For a DML query that can be optimized by enforcing a non-coordinator gather motion,
		// we request singleton-segment distribution on the outer child. If the outer child
		// is replicated, no enforcer gets added; in which case pdsFirst is EdtStrictReplicated.
		// Hence handle this scenario here and require a singleton-segment on the
		// inner child to produce a singleton execution alternavtive for the HJ.
		return GPOS_NEW(mp)
			CDistributionSpecSingleton(CDistributionSpecSingleton::EstSegment);
	}

	GPOS_ASSERT(CDistributionSpec::EdtSingleton == pdsFirst->Edt() ||
				CDistributionSpec::EdtStrictSingleton == pdsFirst->Edt());

	// require second child to have matching singleton distribution (coordinator or segment)
	return CPhysical::PdssMatching(
		mp, CDistributionSpecSingleton::PdssConvert(pdsFirst));
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequiredReplicate
//
//	@doc:
//		Create (hashed/non-singleton, broadcast) optimization request
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequiredReplicate(
	CMemoryPool *mp, CExpressionHandle &exprhdl, CDistributionSpec *pdsInput,
	ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq,
	CReqdPropPlan *prppInput)
{
	EChildExecOrder eceo = Eceo();
	if (EceoLeftToRight == eceo)
	{
		// if optimization order is left to right, fall-back to implementation of parent Join operator
		CEnfdDistribution *ped = CPhysicalJoin::Ped(
			mp, exprhdl, prppInput, child_index, pdrgpdpCtxt, ulOptReq);
		CDistributionSpec *pds = ped->PdsRequired();
		pds->AddRef();
		SafeRelease(ped);
		return pds;
	}
	GPOS_ASSERT(EceoRightToLeft == eceo);

	if (1 == child_index)
	{
		// require inner child to be replicated
		return GPOS_NEW(mp)
			CDistributionSpecReplicated(CDistributionSpec::EdtReplicated);
	}
	GPOS_ASSERT(0 == child_index);

	// require a matching distribution from outer child
	CDistributionSpec *pdsInner =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(nullptr != pdsInner);

	if (CDistributionSpec::EdtUniversal == pdsInner->Edt())
	{
		// Inner child is universal, which satisfies the requested
		// replicated spec.
		// If the join outputs the inner side, request the outer child
		// to be a singleton. This way the join output ends up on one
		// segment or the coordinator, so the data isn't duplicated.
		// This only occurs in right outer join, cause we always
		// broadcast child index 1.
		// If the join outputs the outer side, request the outer child
		// to be non duplicated, i.e. non-replicated.

		if (EopPhysicalRightOuterHashJoin == this->Eopid())
		{
			return GPOS_NEW(mp) CDistributionSpecSingleton();
		}
		else
		{
			return GPOS_NEW(mp) CDistributionSpecNonReplicated();
		}
	}

	if (ulOptReq == m_pdrgpdsRedistributeRequests->Size() &&
		CDistributionSpec::EdtHashed == pdsInput->Edt())
	{
		// attempt to propagate hashed request to child
		CDistributionSpecHashed *pdshashed = PdshashedPassThru(
			mp, exprhdl, CDistributionSpecHashed::PdsConvert(pdsInput),
			child_index, pdrgpdpCtxt, ulOptReq);
		if (nullptr != pdshashed)
		{
			return pdshashed;
		}
	}

	// otherwise, require second child to deliver non-singleton distribution
	GPOS_ASSERT(CDistributionSpec::EdtStrictReplicated == pdsInner->Edt() ||
				CDistributionSpec::EdtTaintedReplicated == pdsInner->Edt());
	return GPOS_NEW(mp) CDistributionSpecNonSingleton();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdshashedPassThru
//
//	@doc:
//		Create a child hashed distribution request based on input hashed
//		distribution,
//		return NULL if no such request can be created
//
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalHashJoin::PdshashedPassThru(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 CDistributionSpecHashed *pdshashedInput,
									 ULONG,				// child_index
									 CDrvdPropArray *,	// pdrgpdpCtxt
									 ULONG ulOptReq GPOS_UNUSED)
{
	GPOS_ASSERT(nullptr != pdshashedInput);

	if (!GPOS_FTRACE(EopttraceEnableRedistributeBroadcastHashJoin))
	{
		// this option is disabled
		return nullptr;
	}

	// since incoming request is hashed, we attempt here to propagate this request to outer child
	CColRefSet *pcrsOuterOutput =
		exprhdl.DeriveOutputColumns(0 /*child_index*/);
	CExpressionArray *pdrgpexprIncomingRequest = pdshashedInput->Pdrgpexpr();
	CColRefSet *pcrsAllUsed =
		CUtils::PcrsExtractColumns(mp, pdrgpexprIncomingRequest);
	BOOL fSubset = pcrsOuterOutput->ContainsAll(pcrsAllUsed);
	BOOL fDisjoint = pcrsOuterOutput->IsDisjoint(pcrsAllUsed);
	pcrsAllUsed->Release();
	if (fSubset)
	{
		// incoming request uses columns from outer child only, pass it through
		// but create a copy
		CDistributionSpecHashed *pdsHashedRequired = pdshashedInput->Copy(mp);
		return pdsHashedRequired;
	}

	if (!fDisjoint)
	{
		// incoming request intersects with columns from outer child,
		// we restrict the request to outer child columns only, then we pass it through
		CExpressionArray *pdrgpexprChildRequest =
			GPOS_NEW(mp) CExpressionArray(mp);
		const ULONG size = pdrgpexprIncomingRequest->Size();
		for (ULONG ul = 0; ul < size; ul++)
		{
			CExpression *pexpr = (*pdrgpexprIncomingRequest)[ul];
			CColRefSet *pcrsUsed = pexpr->DeriveUsedColumns();
			if (pcrsOuterOutput->ContainsAll(pcrsUsed))
			{
				// hashed expression uses columns from outer child only, add it to request
				pexpr->AddRef();
				pdrgpexprChildRequest->Append(pexpr);
			}
		}
		GPOS_ASSERT(0 < pdrgpexprChildRequest->Size());

		CDistributionSpecHashed *pdshashed =
			GPOS_NEW(mp) CDistributionSpecHashed(
				pdrgpexprChildRequest, pdshashedInput->FNullsColocated());

		// since the other child of the join is replicated, we need to enforce hashed-distribution across segments here
		pdshashed->MarkUnsatisfiableBySingleton();

		return pdshashed;
	}

	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequiredRedistribute
//
//	@doc:
//		Compute (redistribute, redistribute) optimization request
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequiredRedistribute(CMemoryPool *mp,
										   CExpressionHandle &exprhdl,
										   CDistributionSpec *,	 // pdsInput
										   ULONG child_index,
										   CDrvdPropArray *pdrgpdpCtxt,
										   ULONG ulOptReq) const
{
	if (FFirstChildToOptimize(child_index))
	{
		// require first child to provide a hashed distribution,
		return PdshashedRequired(mp, child_index, ulOptReq);
	}

	// find the distribution delivered by first child
	CDistributionSpec *pdsFirst =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	GPOS_ASSERT(nullptr != pdsFirst);

	CDistributionSpec *pdsInputForMatch = nullptr;
	if (pdsFirst->Edt() == CDistributionSpec::EdtHashed)
	{
		// we need to create a matching required spec based on the derived distribution spec from
		// the first child. Since that does not contain the m_equiv_hash_exprs (as they are not populated
		// for derived specs), so compute that here.
		CDistributionSpecHashed *pdsHashedFirstChild =
			CDistributionSpecHashed::PdsConvert(pdsFirst);
		CDistributionSpecHashed *pdsHashed = pdsHashedFirstChild->Copy(mp);
		pdsHashed->ComputeEquivHashExprs(mp, exprhdl);
		pdsInputForMatch = pdsHashed;
	}
	else
	{
		pdsInputForMatch = pdsFirst;
	}

	// find the index of the first child
	ULONG ulFirstChild = 0;
	if (EceoRightToLeft == Eceo())
	{
		ulFirstChild = 1;
	}

	// return a matching distribution request for the second child
	CDistributionSpec *pdsMatch = PdsMatch(mp, pdsInputForMatch, ulFirstChild);
	if (pdsFirst->Edt() == CDistributionSpec::EdtHashed)
	{
		// if the input spec was created as a copy, release it
		pdsInputForMatch->Release();
	}
	return pdsMatch;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//		this function creates three optimization requests to join children:
//		Req(1 to N) (redistribute, redistribute), where we request the first hash join child
//			to be distributed on single hash join keys separately, as well as the set
//			of all hash join keys,
//			the second hash join child is always required to match the distribution returned
//			by first child
// 		Req(N + 1) (hashed, broadcast)
// 		Req(N + 2) (non-singleton, broadcast)
// 		Req(N + 3) (singleton, singleton)
//
//		we always check the distribution delivered by the first child (as
//		given by child optimization order), and then match the delivered
//		distribution on the second child
//
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalHashJoin::PdsRequired(
	CMemoryPool *mp GPOS_UNUSED, CExpressionHandle &exprhdl GPOS_UNUSED,
	CDistributionSpec *pdsInput GPOS_UNUSED, ULONG child_index GPOS_UNUSED,
	CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
	ULONG ulOptReq
		GPOS_UNUSED	 // identifies which optimization request should be created
) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT("PdsRequired should not be called for CPhysicalHashJoin"));
	return nullptr;
}

CEnfdDistribution *
CPhysicalHashJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
					   CReqdPropPlan *prppInput, ULONG child_index,
					   CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	GPOS_ASSERT(2 > child_index);
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulOptReq);
	CDistributionSpec *const pdsInput = prppInput->Ped()->PdsRequired();

	// if expression has to execute on a single host then we need a gather
	if (exprhdl.NeedsSingletonExecution())
	{
		return GPOS_NEW(mp) CEnfdDistribution(
			PdsRequireSingleton(mp, exprhdl, pdsInput, child_index), dmatch);
	}

	if (exprhdl.HasOuterRefs())
	{
		if (CDistributionSpec::EdtSingleton == pdsInput->Edt() ||
			CDistributionSpec::EdtStrictReplicated == pdsInput->Edt())
		{
			return GPOS_NEW(mp) CEnfdDistribution(
				PdsPassThru(mp, exprhdl, pdsInput, child_index), dmatch);
		}
		return GPOS_NEW(mp)
			CEnfdDistribution(GPOS_NEW(mp) CDistributionSpecReplicated(
								  CDistributionSpec::EdtStrictReplicated),
							  dmatch);
	}

	const ULONG ulHashDistributeRequests =
		m_pdrgpdsRedistributeRequests->Size();
	if (ulOptReq < ulHashDistributeRequests)
	{
		// requests 1 .. N are (redistribute, redistribute)
		CDistributionSpec *pds = PdsRequiredRedistribute(
			mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);
		if (CDistributionSpec::EdtHashed == pds->Edt())
		{
			CDistributionSpecHashed *pdsHashed =
				CDistributionSpecHashed::PdsConvert(pds);
			pdsHashed->ComputeEquivHashExprs(mp, exprhdl);
		}
		return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
	}

	if (ulOptReq == ulHashDistributeRequests ||
		ulOptReq == ulHashDistributeRequests + 1)
	{
		// requests N+1, N+2 are (hashed/non-singleton, replicate)

		CDistributionSpec *pds =
			PdsRequiredReplicate(mp, exprhdl, pdsInput, child_index,
								 pdrgpdpCtxt, ulOptReq, prppInput);
		if (CDistributionSpec::EdtHashed == pds->Edt())
		{
			CDistributionSpecHashed *pdsHashed =
				CDistributionSpecHashed::PdsConvert(pds);
			pdsHashed->ComputeEquivHashExprs(mp, exprhdl);
		}
		return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
	}

	GPOS_ASSERT(ulOptReq == ulHashDistributeRequests + 2);

	// requests N+3 is (singleton, singleton)

	return GPOS_NEW(mp) CEnfdDistribution(
		PdsRequiredSingleton(mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt),
		dmatch);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PedRightOrFullJoin
//
//	@doc:
//		Compute required distribution of the n-th child
//		for right outer join and full outer join
//
//---------------------------------------------------------------------------
CEnfdDistribution *
CPhysicalHashJoin::PedRightOrFullJoin(
	CMemoryPool *mp, CExpressionHandle &exprhdl, CReqdPropPlan *prppInput,
	ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	// create the following requests:
	// 1) hash-hash (provided by CPhysicalHashJoin::Ped)
	// 2) singleton-singleton
	//
	// We also could create a replicated-hashed and replicated-non-singleton
	// request, but that isn't a promising alternative as we would be
	// broadcasting the outer side. In that case, an LOJ would be better.

	CDistributionSpec *const pdsInput = prppInput->Ped()->PdsRequired();
	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulOptReq);

	if (exprhdl.NeedsSingletonExecution() || exprhdl.HasOuterRefs())
	{
		return GPOS_NEW(mp) CEnfdDistribution(
			PdsRequireSingleton(mp, exprhdl, pdsInput, child_index), dmatch);
	}

	if (ulOptReq < NumDistrReq())
	{
		// requests 1 .. N are (redistribute, redistribute)
		CDistributionSpec *pds = PdsRequiredRedistribute(
			mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt, ulOptReq);
		if (CDistributionSpec::EdtHashed == pds->Edt())
		{
			CDistributionSpecHashed *pdsHashed =
				CDistributionSpecHashed::PdsConvert(pds);
			pdsHashed->ComputeEquivHashExprs(mp, exprhdl);
		}
		return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
	}
	GPOS_ASSERT(ulOptReq == NumDistrReq());
	return GPOS_NEW(mp) CEnfdDistribution(
		PdsRequiredSingleton(mp, exprhdl, pdsInput, child_index, pdrgpdpCtxt),
		dmatch);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::PdshashedRequired
//
//	@doc:
//		Compute required hashed distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalHashJoin::PdshashedRequired(CMemoryPool *,	 // mp
									 ULONG,			 // child_index
									 ULONG ulReqIndex) const
{
	GPOS_ASSERT(ulReqIndex < m_pdrgpdsRedistributeRequests->Size());
	CDistributionSpec *pds = (*m_pdrgpdsRedistributeRequests)[ulReqIndex];

	pds->AddRef();
	return CDistributionSpecHashed::PdsConvert(pds);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator;
//
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalHashJoin::EpetOrder(CExpressionHandle &,  // exprhdl
							 const CEnfdOrder *
#ifdef GPOS_DEBUG
								 peo
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(nullptr != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	// hash join is not order-preserving;
	// any order requirements have to be enforced on top
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::FNullableInnerHashKeys
//
//	@doc:
//		Check whether the hash keys from one child are nullable. pcrsNotNull must
//		be all the "not null" columns coming from that child
//
//---------------------------------------------------------------------------
BOOL
CPhysicalHashJoin::FNullableHashKeys(CColRefSet *pcrsNotNull, BOOL fInner) const
{
	ULONG ulHashKeys = 0;
	if (fInner)
	{
		ulHashKeys = m_pdrgpexprInnerKeys->Size();
	}
	else
	{
		ulHashKeys = m_pdrgpexprOuterKeys->Size();
	}

	for (ULONG ul = 0; ul < ulHashKeys; ul++)
	{
		if (FNullableHashKey(ul, pcrsNotNull, fInner))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalHashJoin::FNullableHashKey
//
//	@doc:
//		Check whether a hash key is nullable
//
//---------------------------------------------------------------------------
BOOL
CPhysicalHashJoin::FNullableHashKey(ULONG ulKey, CColRefSet *pcrsNotNull,
									BOOL fInner) const
{
	COperator *pop = nullptr;
	if (fInner)
	{
		pop = (*m_pdrgpexprInnerKeys)[ulKey]->Pop();
	}
	else
	{
		pop = (*m_pdrgpexprOuterKeys)[ulKey]->Pop();
	}
	EOperatorId op_id = pop->Eopid();

	if (COperator::EopScalarIdent == op_id)
	{
		const CColRef *colref = CScalarIdent::PopConvert(pop)->Pcr();
		return (!pcrsNotNull->FMember(colref));
	}

	if (COperator::EopScalarConst == op_id)
	{
		return CScalarConst::PopConvert(pop)->GetDatum()->IsNull();
	}

	// be conservative for all other scalar expressions where we cannot easily
	// determine nullability
	return true;
}

void
CPhysicalHashJoin::CreateOptRequests(CMemoryPool *mp)
{
	CreateHashRedistributeRequests(mp);

	// given an optimization context, HJN creates three optimization requests
	// to enforce distribution of its children:
	// Req(1 to N) (redistribute, redistribute), where we request the first hash join child
	//		to be distributed on single hash join keys separately, as well as the set
	//		of all hash join keys,
	//		the second hash join child is always required to match the distribution returned
	//		by first child
	// Req(N + 1) (hashed, broadcast)
	// Req(N + 2) (non-singleton, broadcast)
	// Req(N + 3) (singleton, singleton)

	ULONG ulDistrReqs = GPOPT_NON_HASH_DIST_REQUESTS + NumDistrReq();
	SetDistrRequests(ulDistrReqs);

	// With DP enabled, there are several (max 10 controlled by macro)
	// alternatives generated for a join tree and during optimization of those
	// alternatives expressions PS is inserted in almost all the groups possibly.
	// However, if DP is turned off, i.e in query or greedy join order,
	// PS must be inserted in the group with DTS else in some cases HJ plan
	// cannot be created. So, to ensure pushing PS without DPE 2 partition
	// propagation request are required if DP is disabled.
	//    Req 0 => Push PS with considering DPE possibility
	//    Req 1 => Push PS without considering DPE possibility
	// Ex case: select * from non_part_tbl1 t1, part_tbl t2, non_part_tbl2 t3
	// where t1.b = t2.b and t2.b = t3.b;
	// Note: b is the partitioned column for part_tbl. If DP is turned off, HJ
	// will not be created for the above query if we send only 1 request.
	// Also, increasing the number of request increases the optimization time, so
	// set 2 only when needed.
	//
	// There are also cases where greedy does generate a better plan
	// without DPE. This adds some overhead (<10%)to optimization time in
	// some cases, but can create better alternatives to DPE, so
	// we also generate this additional request for expressions that originated
	// from CXformExpandNAryJoinGreedy.
	CPhysicalJoin *physical_join = dynamic_cast<CPhysicalJoin *>(this);
	if ((GPOPT_FDISABLED_XFORM(CXform::ExfExpandNAryJoinDP) &&
		 GPOPT_FDISABLED_XFORM(CXform::ExfExpandNAryJoinDPv2)) ||
		physical_join->OriginXform() == CXform::ExfExpandNAryJoinGreedy)
	{
		SetPartPropagateRequests(2);
	}
	else
	{
		SetPartPropagateRequests(1);
	}
}

CExpression *
CPhysicalHashJoin::PexprJoinPredOnPartKeys(CMemoryPool *mp,
										   CExpression *pexprScalar,
										   CPartKeysArray *pdrgppartkeys,
										   CColRefSet *pcrsAllowedRefs) const
{
	GPOS_ASSERT(nullptr != pcrsAllowedRefs);

	CExpression *pexprPred = nullptr;
	for (ULONG ulKey = 0; nullptr == pexprPred && ulKey < pdrgppartkeys->Size();
		 ulKey++)
	{
		// get partition key
		CColRef2dArray *pdrgpdrgpcrPartKeys =
			(*pdrgppartkeys)[ulKey]->Pdrgpdrgpcr();

		// try to generate a request with dynamic partition selection
		pexprPred = CPredicateUtils::PexprExtractPredicatesOnPartKeys(
			mp, pexprScalar, pdrgpdrgpcrPartKeys, pcrsAllowedRefs,
			true  // fUseConstraints
		);
	}

	return pexprPred;
}

CPartitionPropagationSpec *
CPhysicalHashJoin::PppsRequiredForJoins(CMemoryPool *mp,
										CExpressionHandle &exprhdl,
										CPartitionPropagationSpec *pppsRequired,
										ULONG child_index,
										CDrvdPropArray *pdrgpdpCtxt,
										ULONG ulOptReq) const
{
	GPOS_ASSERT(nullptr != pppsRequired);
	GPOS_ASSERT(nullptr != pdrgpdpCtxt);

	CExpression *pexprScalar = exprhdl.PexprScalarExactChild(2 /*child_index*/);

	CColRefSet *pcrsOutputInner = exprhdl.DeriveOutputColumns(1);

	CPartitionPropagationSpec *pps_result;
	if (ulOptReq == 0)
	{
		// DPE: create a new request
		pps_result = GPOS_NEW(mp) CPartitionPropagationSpec(mp);

		// Extract the partition info of the outer child.
		// Info in CPartInfo is added at the logical level. We add information
		// about consumers during that stage. During the physical implementation,
		// for every consumer, we check if we have to insert a consumer/propagator
		// in PppsRequired()
		CPartInfo *part_info_outer = exprhdl.DerivePartitionInfo(0);

		// Extracting the information of existing partition table consumers.
		// For every consumer(Dynamic Table Scan, identified by scan-id),
		// if PppsRequired() is called for inner child, we can add a propagator.
		// if PppsRequired() is called for outer child, we can add a consumer.
		for (ULONG ul = 0; ul < part_info_outer->UlConsumers(); ++ul)
		{
			ULONG scan_id = part_info_outer->ScanId(ul);
			IMDId *rel_mdid = part_info_outer->GetRelMdId(ul);
			CPartKeysArray *part_keys_array =
				part_info_outer->Pdrgppartkeys(ul);

			CExpression *pexprCmp =
				PexprJoinPredOnPartKeys(mp, pexprScalar, part_keys_array,
										pcrsOutputInner /* pcrsAllowedRefs*/);

			// If we don't have predicate on partition keys, then partition
			// elimination won't work, so we don't add a Consumer or Propagator
			if (pexprCmp == nullptr)
			{
				continue;
			}

			// For outer child(index=0), we insert Consumer, if a Partition
			// Selector exist in the inner child for a scan-id.
			if (child_index == 0)
			{
				// For the inner child, we extract the derived PPS.
				CPartitionPropagationSpec *pps_inner =
					CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Ppps();

				// In the derived plan properties of the inner child,
				// we check if a partition selector exist for the given scan-id
				// If found, we insert a corresponding 'Consumer' in the outer child
				CBitSet *selector_ids =
					GPOS_NEW(mp) CBitSet(mp, *pps_inner->SelectorIds(scan_id));

				// For the identified 'partition selector' we insert a consumer.
				// This will a form part of our required properties, i.e. for the
				// given 'partition selector', we require a 'Consumer'
				pps_result->Insert(scan_id,
								   CPartitionPropagationSpec::EpptConsumer,
								   rel_mdid, selector_ids, nullptr /* expr */);
				selector_ids->Release();
			}
			else
			{
				// For inner child (index=1), we insert a propagator given that
				// we have predicate on the partition keys
				GPOS_ASSERT(child_index == 1);
				pps_result->Insert(scan_id,
								   CPartitionPropagationSpec::EpptPropagator,
								   rel_mdid, nullptr, pexprCmp);
			}
			pexprCmp->Release();
		}

		// Now for the input 'pppsRequired' & 'child_index' we check if any
		// other consumer is required to be added in the pps_result.
		// 1. We prepare a list of allowed scan-ids for the input child. These scan-ids
		// we defined at the logical level.
		// 2. For each of the scan-ids, that exist in pppsRequired, we check if
		// they exist in 'allowed list' and are of type Consumer.
		// 3. For all such scan-ids, we update our computed required props in pps_result
		// Thus based we have computed the required properties for the operator based
		// on the input from higher level(using input pppsRequired) and our own
		// operator specific rules (as in the for loop above)
		CBitSet *allowed_scan_ids = GPOS_NEW(mp) CBitSet(mp);
		CPartInfo *part_info = exprhdl.DerivePartitionInfo(child_index);
		for (ULONG ul = 0; ul < part_info->UlConsumers(); ++ul)
		{
			ULONG scan_id = part_info->ScanId(ul);
			allowed_scan_ids->ExchangeSet(scan_id);
		}
		pps_result->InsertAllowedConsumers(pppsRequired, allowed_scan_ids);
		allowed_scan_ids->Release();
	}
	else
	{
		// No DPE: pass through requests
		pps_result = CPhysical::PppsRequired(
			mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, ulOptReq);
	}
	return pps_result;
}

// In the following function, we are generating the Derived property :
// "Partition Propagation Spec" of the join.
// Since Property derivation takes place in a bottom-up fashion, this operator
// derives the information from its child and passes it up. In this function
// we are focussing only on the "Partition Propagation Spec" of the children
CPartitionPropagationSpec *
CPhysicalHashJoin::PppsDeriveForJoins(CMemoryPool *mp,
									  CExpressionHandle &exprhdl) const
{
	CPartitionPropagationSpec *pps_outer = exprhdl.Pdpplan(0)->Ppps();
	CPartitionPropagationSpec *pps_inner = exprhdl.Pdpplan(1)->Ppps();

	CPartitionPropagationSpec *pps_result =
		GPOS_NEW(mp) CPartitionPropagationSpec(mp);
	pps_result->InsertAll(pps_outer);
	pps_result->InsertAllResolve(pps_inner);

	return pps_result;
}
// EOF
